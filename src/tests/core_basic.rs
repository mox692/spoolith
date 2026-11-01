//! Individual cases for RingCore and recovery.

use std::io;

use super::{MAX_PAYLOAD, Records, S, SimRing, payload, records_in, scan_hot, small_config};
use crate::config::ChecksumMode;
use crate::core::{AppendError, HotRead, Stall};
use crate::io::sim::SimDisk;
use crate::pos::Geometry;

const SEG: u64 = S as u64;

fn append_all(ring: &mut SimRing, oracle: &mut Records, lens: impl IntoIterator<Item = usize>) {
    for len in lens {
        let p = payload(oracle.len() as u64, len);
        let pos = ring.append(&p).unwrap();
        oracle.push((pos, p));
    }
}

#[test]
fn append_and_read_hot() {
    let mut ring = SimRing::create(small_config(4, 7, 2));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [0, 1, 7, 8, 9, 100]);
    let expected: Vec<u64> = vec![0, 8, 24, 40, 56, 80];
    assert_eq!(oracle.iter().map(|r| r.0).collect::<Vec<_>>(), expected);
    assert_eq!(ring.core.write_pos(), 80 + 8 + 100 + 4);
    assert_eq!(scan_hot(&ring.core), oracle);
}

/// A record that exactly fills a segment advances to the next segment without an end marker
#[test]
fn exact_segment_fill() {
    let mut ring = SimRing::create(small_config(4, 7, 3));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [MAX_PAYLOAD; 5]);
    let positions: Vec<u64> = oracle.iter().map(|r| r.0).collect();
    assert_eq!(positions, [0, SEG, 2 * SEG, 3 * SEG, 4 * SEG]);
    assert_eq!(
        scan_hot(&ring.core),
        records_in(&oracle, ring.core.mem_low(), u64::MAX)
    );

    // reopen with head still at a segment boundary
    ring.flush_all();
    assert_eq!(ring.core.durable(), 5 * SEG);
    assert_eq!(
        ring.disk_records(),
        records_in(&oracle, ring.core.disk_tail(), u64::MAX)
    );
    let mut ring = ring.restart();
    assert_eq!(ring.core.write_pos(), 5 * SEG);
    append_all(&mut ring, &mut oracle, [10]);
    assert_eq!(oracle.last().unwrap().0, 5 * SEG);
    assert_eq!(
        scan_hot(&ring.core),
        records_in(&oracle, ring.core.mem_low(), u64::MAX)
    );
}

/// A record that does not fit in the remainder writes an end marker and goes in the next segment
#[test]
fn record_skips_to_next_segment() {
    let mut ring = SimRing::create(small_config(4, 7, 3));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [3000, 3000]);
    assert_eq!(oracle[1].0, SEG);
    let end = ring.core.read_hot(3008, |_| ()).unwrap();
    assert_eq!(end, HotRead::SegmentEnd { next: SEG });
    assert_eq!(scan_hot(&ring.core), oracle);
}

#[test]
fn too_large() {
    let ring = SimRing::create(small_config(4, 7, 3));
    assert_eq!(
        ring.core.try_append(&[0; MAX_PAYLOAD + 1]),
        Err(AppendError::TooLarge)
    );
    assert_eq!(ring.core.write_pos(), 0);
}

/// Once the runway (R*S) is used up, it stalls until the flush completes (I2)
#[test]
fn stalls_on_runway() {
    let mut ring = SimRing::create(small_config(4, 7, 2));
    for i in 0..2 {
        ring.core.try_append(&payload(i, MAX_PAYLOAD)).unwrap();
    }
    assert_eq!(
        ring.core.try_append(&[1]),
        Err(AppendError::Stall(Stall::Durability))
    );
    // a range on loan does not count as durable until it completes
    let job = ring.core.prepare_flush().unwrap();
    assert!(ring.core.flush_in_flight());
    assert!(
        ring.core.prepare_flush().is_none(),
        "I5: only one flush in flight"
    );
    assert_eq!(
        ring.core.try_append(&[1]),
        Err(AppendError::Stall(Stall::Durability))
    );
    let mut buf = Box::new([0u8; crate::header::HEADER_SIZE]);
    let res = crate::io::run_flush(&mut ring.storage, ring.core.memory(), job, &mut buf);
    assert_eq!(ring.core.complete_flush(res).unwrap(), 2 * SEG);
    assert!(!ring.core.flush_in_flight());
    ring.core.try_append(&[1]).unwrap();
    ring.core.check_invariants();
}

/// A single flush is bounded by max_flush_bytes
#[test]
fn flush_is_capped() {
    let mut cfg = small_config(8, 12, 4);
    cfg.max_flush_bytes = SEG;
    let mut ring = SimRing::create(cfg);
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [1000; 10]);
    let mut heads = Vec::new();
    while let Some(res) = ring.flush() {
        heads.push(res.unwrap());
    }
    // 10 records of 1008B. 4 fit in one segment
    assert_eq!(heads, [SEG, 2 * SEG, 2 * SEG + 2 * 1008]);
    assert_eq!(ring.disk_records(), oracle);
}

/// A failed flush does not advance durable; the next flush rewrites from the same range (section 13)
#[test]
fn failed_flush_is_retried() {
    let mut ring = SimRing::create(small_config(4, 7, 2));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [100, 200]);
    ring.disk.fail_next();
    assert!(ring.flush().unwrap().is_err());
    assert_eq!((ring.core.durable(), ring.core.flush_failures()), (0, 1));
    append_all(&mut ring, &mut oracle, [300]);
    assert_eq!(ring.flush().unwrap().unwrap(), ring.core.write_pos());
    assert_eq!(ring.core.flush_failures(), 0);
    assert_eq!(ring.disk_records(), oracle);
}

#[test]
fn no_valid_header() {
    let cfg = small_config(4, 7, 2);
    let disk = SimDisk::new(cfg.geometry().unwrap().file_len());
    let err = SimRing::open(cfg, disk).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn rejects_incompatible_config() {
    let ring = SimRing::create(small_config(4, 8, 2));
    let disk = ring.disk.clone();
    drop(ring);

    let mut cfg = small_config(4, 8, 2);
    cfg.checksum = ChecksumMode::PerRecord;
    let mut other_s = small_config(4, 8, 2);
    other_s.segment_size = 2 * S;
    other_s.max_flush_bytes = 2 * SEG;
    for cfg in [cfg, small_config(4, 9, 2), other_s] {
        let err = SimRing::open(cfg, disk.clone()).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
    // M and R can be changed
    SimRing::open(small_config(2, 8, 1), disk.clone()).unwrap();
}

/// If range_crc does not match, roll back to the head of the previous flush (section 8.3, item 2)
#[test]
fn falls_back_on_range_crc_mismatch() {
    let mut ring = SimRing::create(small_config(4, 7, 2));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [100, 200]);
    ring.flush_all();
    let h1 = ring.core.durable();
    append_all(&mut ring, &mut oracle, [300, 400]);
    ring.flush_all();
    let h2 = ring.core.durable();

    // flush 2's header arrived, but its data did not
    let geo = ring.geo;
    crate::io::Storage::write_at(&mut ring.storage, &[0xff; 16], geo.disk_off(h1 + 8)).unwrap();
    let mut ring = ring.restart();
    assert_eq!(ring.core.durable(), h1);
    assert_eq!(ring.core.recovery_info().lost_bytes, h2 - h1);
    oracle.retain(|r| r.0 < h1);
    assert_eq!(scan_hot(&ring.core), oracle);
    assert_eq!(ring.disk_records(), oracle);

    // the lost range gets overwritten
    append_all(&mut ring, &mut oracle, [50]);
    ring.flush_all();
    let mut ring = ring.restart();
    assert_eq!(ring.core.recovery_info().lost_bytes, 0);
    assert_eq!(ring.disk_records(), oracle);
}

/// After restarting with a larger R, write only the header before any data (section 5.2)
#[test]
fn runway_increase_writes_header_first() {
    let mut ring = SimRing::create(small_config(4, 8, 1));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [100, 200]);
    ring.flush_all();
    let head = ring.core.durable();

    let mut ring = ring.reopen(small_config(4, 8, 3)).unwrap();
    append_all(&mut ring, &mut oracle, [MAX_PAYLOAD, MAX_PAYLOAD]);
    let job = ring.core.prepare_flush().unwrap();
    assert!(job.pieces.is_empty());
    assert_eq!(
        (
            job.header.prev_head,
            job.header.head,
            job.header.runway_segments
        ),
        (head, head, 3)
    );
    let mut buf = Box::new([0u8; crate::header::HEADER_SIZE]);
    let res = crate::io::run_flush(&mut ring.storage, ring.core.memory(), job, &mut buf);
    ring.core.complete_flush(res).unwrap();

    let job = ring.core.prepare_flush().unwrap();
    assert_eq!(job.header.head, ring.core.write_pos());
    drop(job);
}

/// Can open even with M smaller than the previous R (range_crc is verified in a temporary buffer)
#[test]
fn memory_smaller_than_old_runway() {
    let mut ring = SimRing::create(small_config(8, 16, 5));
    let mut oracle = Vec::new();
    append_all(
        &mut ring,
        &mut oracle,
        [MAX_PAYLOAD, MAX_PAYLOAD, MAX_PAYLOAD, MAX_PAYLOAD, 1000],
    );
    assert_eq!(ring.flush().unwrap().unwrap(), 4 * SEG + 1008);
    assert!(ring.flush().is_none());

    let mut cfg = small_config(2, 16, 1);
    cfg.max_flush_bytes = SEG;
    let mut ring = ring.reopen(cfg).unwrap();
    assert_eq!(ring.core.durable(), 4 * SEG + 1008);
    assert_eq!(ring.core.recovery_info().lost_bytes, 0);
    assert_eq!(ring.disk_records(), oracle);
    assert_eq!(scan_hot(&ring.core), records_in(&oracle, 4 * SEG, u64::MAX));
}

/// Loads warm_segments segments before head into memory as well
#[test]
fn warm_segments_are_loaded() {
    let mut ring = SimRing::create(small_config(8, 12, 2));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, (0..40).map(|i| 300 + i * 37));
    ring.flush_all();
    let head = ring.core.durable();
    let geo = ring.geo;

    for warm in [0, 3, 7] {
        let mut cfg = small_config(8, 12, 2);
        cfg.warm_segments = warm;
        ring = ring.reopen(cfg).unwrap();
        let expected_low = geo.seg_start(head) - u64::from(warm) * SEG;
        assert_eq!(ring.core.mem_low(), expected_low.max(ring.core.disk_tail()));
        assert_eq!(
            scan_hot(&ring.core),
            records_in(&oracle, ring.core.mem_low(), u64::MAX)
        );
    }
}

/// The per-record checksum detects in-memory corruption
#[test]
fn per_record_checksum_detects_corruption() {
    let mut cfg = small_config(4, 7, 2);
    cfg.checksum = ChecksumMode::PerRecord;
    let mut ring = SimRing::create(cfg.clone());
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [100, 200]);
    ring.flush_all();
    let mut ring = ring.reopen(cfg).unwrap();
    assert_eq!(scan_hot(&ring.core), oracle);
    assert_eq!(ring.disk_records(), oracle);

    // SAFETY: only the test thread is using the ring
    unsafe { ring.core.memory().slice_mut(ring.geo.mem_off(112 + 8), 1)[0] ^= 1 };
    assert!(ring.core.read_hot(0, |_| ()).is_ok());
    assert!(ring.core.read_hot(112, |_| ()).is_err());
}

/// Even when D is not a power of two and M != D, the readable range stays intact across any number of disk laps
#[test]
fn wraps_disk_many_times() {
    let mut cfg = small_config(4, 7, 3);
    cfg.max_flush_bytes = 2 * SEG;
    let mut ring = SimRing::create(cfg);
    let geo: Geometry = ring.geo;
    let mut oracle = Vec::new();
    for round in 0..40 {
        append_all(
            &mut ring,
            &mut oracle,
            (0..7).map(|i| (round * 131 + i * 557) % (MAX_PAYLOAD + 1)),
        );
        ring.flush_all();
        assert_eq!(
            ring.disk_records(),
            records_in(&oracle, ring.core.disk_tail(), u64::MAX)
        );
        assert_eq!(
            scan_hot(&ring.core),
            records_in(&oracle, ring.core.mem_low(), u64::MAX)
        );
        // the header's tail guarantees H*S of history on disk
        let last = geo.last_seg_start(ring.core.durable());
        assert_eq!(
            ring.core.disk_tail(),
            last.saturating_sub(geo.history_bytes())
        );
    }
    assert!(ring.core.durable() > 5 * geo.disk_segs() * SEG);
}

/// With R = 1 and write == durable, a record that does not fit in the remainder still makes progress
#[test]
fn runway_one_does_not_deadlock() {
    let mut ring = SimRing::create(small_config(2, 3, 1));
    let mut oracle = Vec::new();
    append_all(&mut ring, &mut oracle, [100]);
    ring.flush_all();
    // it goes in the next segment, but does not fit until the end marker is flushed
    let big = payload(1, MAX_PAYLOAD);
    assert_eq!(
        ring.core.try_append(&big),
        Err(AppendError::Stall(Stall::Durability))
    );
    assert_eq!(ring.core.write_pos(), SEG);
    assert_eq!(ring.flush().unwrap().unwrap(), SEG);
    oracle.push((ring.core.try_append(&big).unwrap(), big));
    assert_eq!(oracle[1].0, SEG);
    ring.flush_all();
    assert_eq!(ring.disk_records(), oracle);
}
