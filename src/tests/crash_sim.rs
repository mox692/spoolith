//! Crash simulation (section 17, Step 2).
//!
//! Randomly interleaves append / flush / I/O error / restart / power loss, and checks the following each time.
//! - Invariants I1 through I5
//! - In memory, `[mem_low, write)` matches the written records
//! - On disk, `[tail, head)` pointed to by the header matches the written records (neither frames nor crcs are corrupt)
//! - After a restart or power loss, head is at least the head of the last successful flush and at most write
//! - Recovery does not fail even after a power loss

use proptest::prelude::*;

use super::{
    MAX_PAYLOAD, Records, S, SimRing, payload, records_in, scan_hot, small_config, summary,
};
use crate::config::{ChecksumMode, Config};
use crate::core::AppendError;
use crate::frame::frame_len;
use crate::io::sim::{SimDisk, SimRng};

#[derive(Debug, Clone)]
enum Op {
    Append(usize),
    Flush,
    /// Make the next write / sync fail once
    FailNext,
    /// Process restart (not-yet-synced writes survive)
    Restart,
    /// Power loss
    Crash,
    /// Power loss just before the n-th write / sync of a flush
    CrashDuringFlush(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        12 => payload_len().prop_map(Op::Append),
        4 => Just(Op::Flush),
        1 => Just(Op::FailNext),
        1 => Just(Op::Restart),
        1 => Just(Op::Crash),
        1 => (0u64..6).prop_map(Op::CrashDuringFlush),
    ]
}

fn payload_len() -> impl Strategy<Value = usize> {
    prop_oneof![
        4 => 0usize..64,
        2 => 0usize..=MAX_PAYLOAD,
        1 => Just(MAX_PAYLOAD),
        1 => (MAX_PAYLOAD - 64)..=MAX_PAYLOAD,
    ]
}

/// M in {2, 4, 8}, 1 <= R <= M - 1, D in [M + R, M + R + 5] (not necessarily a power of two)
fn config() -> impl Strategy<Value = Config> {
    (1u32..4)
        .prop_flat_map(|m_log| {
            let m = 1 << m_log;
            (Just(m), 1..m)
        })
        .prop_flat_map(|(m, r)| (Just(m), Just(r), 0u32..6, 1..=r, any::<bool>(), 0..m))
        .prop_map(|(m, r, extra, flush_segs, per_record, warm)| {
            let mut cfg = small_config(m, m + r + extra, r);
            cfg.max_flush_bytes = u64::from(flush_segs * S);
            cfg.checksum = if per_record {
                ChecksumMode::PerRecord
            } else {
                ChecksumMode::None
            };
            cfg.warm_segments = warm;
            cfg
        })
}

struct Sim {
    ring: SimRing,
    rng: SimRng,
    /// Written records (in position order). Ranges lost on restart are removed.
    oracle: Records,
    next_id: u64,
}

impl Sim {
    fn run(&mut self, op: Op) {
        match op {
            Op::Append(len) => self.append(len),
            Op::Flush => self.flush(),
            Op::FailNext => self.ring.disk.fail_next(),
            Op::Restart => {
                let disk = self.ring.disk.clone();
                self.reopen(disk);
            }
            Op::Crash => {
                let disk = self.ring.disk.crash(&mut self.rng);
                self.reopen(disk);
            }
            Op::CrashDuringFlush(ops) => {
                self.ring.disk.crash_after(ops);
                let _ = self.ring.flush();
                let disk = self.ring.disk.crash(&mut self.rng);
                self.reopen(disk);
            }
        }
        self.ring.core.check_invariants();
    }

    fn flush(&mut self) {
        if let Some(Ok(_)) = self.ring.flush() {
            self.check_disk();
        }
    }

    fn append(&mut self, len: usize) {
        let p = payload(self.next_id, len);
        self.next_id += 1;
        let before = self.ring.core.write_pos();
        // on a stall, flush up to 2 times (give up if an I/O error prevents progress)
        let mut flushes = 0;
        let pos = loop {
            match self.ring.core.try_append(&p) {
                Ok(pos) => break pos,
                Err(AppendError::Stall(_)) if flushes < 2 => {
                    flushes += 1;
                    assert!(
                        self.ring.core.write_pos() > self.ring.core.durable(),
                        "stalled without anything to flush"
                    );
                    self.flush();
                }
                Err(AppendError::Stall(_)) => return,
                Err(e) => panic!("{e:?}"),
            }
        };
        // if it fits, place it at write; otherwise place it at the start of the next segment. It never spans segments
        let geo = &self.ring.geo;
        let end = pos + frame_len(len);
        assert!(
            pos == before
                || (pos == geo.seg_start(pos) && pos > before && pos - before < geo.seg_size())
        );
        assert_eq!(geo.seg(pos), geo.seg(end - 1));
        assert_eq!(self.ring.core.write_pos(), end);
        self.oracle.push((pos, p));
    }

    fn reopen(&mut self, disk: SimDisk) {
        let (acked, write) = (self.ring.core.durable(), self.ring.core.write_pos());
        let cfg = self.ring.cfg.clone();
        // the old RingCore is dropped here
        self.ring = SimRing::open(cfg, disk).expect("recovery failed");
        let head = self.ring.core.durable();
        assert!(
            acked <= head && head <= write,
            "acked={acked} head={head} write={write}"
        );
        self.oracle.retain(|r| r.0 < head);
        self.check_disk();
    }

    /// Compares the range pointed to by the on-disk header and the in-memory range against the oracle
    fn check_disk(&mut self) {
        let records = self.ring.disk_records();
        let expected = records_in(
            &self.oracle,
            self.ring.core.disk_tail(),
            self.ring.core.durable(),
        );
        assert_eq!(summary(&records), summary(&expected), "disk");
        self.check_hot();
    }

    fn check_hot(&self) {
        let expected = records_in(&self.oracle, self.ring.core.mem_low(), u64::MAX);
        assert_eq!(
            summary(&scan_hot(&self.ring.core)),
            summary(&expected),
            "hot"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn crash_sim(cfg in config(), ops in prop::collection::vec(op(), 1..300), seed in any::<u64>()) {
        let mut sim = Sim { ring: SimRing::create(cfg), rng: SimRng::new(seed), oracle: Vec::new(), next_id: 0 };
        for op in ops {
            sim.run(op);
        }
        sim.check_hot();
        // finally persist everything so that reopening reads the same data (fails once if a FailNext is still pending)
        let write = sim.ring.core.write_pos();
        while let Some(res) = sim.ring.flush() {
            let _ = res;
        }
        prop_assert_eq!(sim.ring.core.durable(), write);
        let disk = sim.ring.disk.clone();
        sim.reopen(disk);
        prop_assert_eq!(sim.ring.core.durable(), write);
    }
}
