//! Integration tests (section 17, Steps 1-2). They use crate-internal types, so they live here rather than in `tests/`.

mod core_basic;
mod crash_sim;
mod file_backend;
mod ring;

use std::io;

use crate::config::{ChecksumMode, Config};
use crate::core::{AppendError, HotRead, RingCore};
use crate::frame::{Frame, decode, frame_len, record_crc};
use crate::header::{FLAG_CHECKSUM_PER_RECORD, HEADER_SIZE, HeaderBuf};
use crate::io::sim::{SimDisk, SimStorage};
use crate::io::{Storage, run_flush};
use crate::pos::Geometry;

pub(crate) const S: u32 = 4096;
/// The payload length that exactly fills one segment
pub(crate) const MAX_PAYLOAD: usize = S as usize - 8;

pub(crate) type Records = Vec<(u64, Vec<u8>)>;

pub(crate) fn small_config(m: u32, d: u32, r: u32) -> Config {
    let mut cfg = Config::new("/nonexistent/ring", d);
    cfg.segment_size = S;
    cfg.memory_segments = m;
    cfg.runway_segments = r;
    cfg.max_flush_bytes = u64::from(r * S);
    cfg
}

/// A payload whose contents differ per record
pub(crate) fn payload(id: u64, len: usize) -> Vec<u8> {
    let seed = id.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes();
    (0..len)
        .map(|i| seed[i % 8].wrapping_add((i / 8) as u8))
        .collect()
}

/// Enumerates the records in `[from, to)` on disk. Panics if a frame is corrupt
pub(crate) fn scan_disk(
    storage: &mut impl Storage,
    geo: &Geometry,
    checksum: ChecksumMode,
    from: u64,
    to: u64,
) -> Records {
    let s = geo.seg_size();
    let mut seg = vec![0u8; s as usize];
    let mut out = Vec::new();
    let mut pos = from;
    while pos < to {
        let start = geo.seg_start(pos);
        storage.read_at(&mut seg, geo.disk_off(start)).unwrap();
        while pos < to && pos < start + s {
            let frame = decode(&seg[(pos - start) as usize..])
                .unwrap_or_else(|_| panic!("corrupt frame at {pos}"));
            match frame {
                Frame::End => pos = start + s,
                Frame::Record { payload, crc } => {
                    if checksum == ChecksumMode::PerRecord {
                        assert_eq!(crc, record_crc(pos, payload), "crc mismatch at {pos}");
                    }
                    out.push((pos, payload.to_vec()));
                    pos += frame_len(payload.len());
                }
            }
        }
    }
    assert_eq!(pos, to, "{to} is not a record boundary");
    out
}

/// Enumerates the records in `[mem_low, write)` in memory
pub(crate) fn scan_hot(core: &RingCore) -> Records {
    let mut out = Vec::new();
    let mut pos = core.mem_low();
    while pos < core.write_pos() {
        match core
            .read_hot(pos, |d| d.to_vec())
            .unwrap_or_else(|_| panic!("corrupt frame at {pos}"))
        {
            HotRead::Record { next, out: data } => {
                out.push((pos, data));
                pos = next;
            }
            HotRead::SegmentEnd { next } => pos = next,
        }
    }
    assert_eq!(pos, core.write_pos());
    out
}

/// The records in the oracle that fall in `[from, to)`
pub(crate) fn records_in(oracle: &Records, from: u64, to: u64) -> Records {
    oracle
        .iter()
        .filter(|(p, _)| (from..to).contains(p))
        .cloned()
        .collect()
}

/// Summarizes records as (position, length, crc) for display on failure
pub(crate) fn summary(records: &Records) -> Vec<(u64, usize, u32)> {
    records
        .iter()
        .map(|(p, d)| (*p, d.len(), crc32c::crc32c(d)))
        .collect()
}

/// A ring on a SimDisk. Flushes run synchronously on the caller's thread
pub(crate) struct SimRing {
    pub cfg: Config,
    pub geo: Geometry,
    pub disk: SimDisk,
    pub storage: SimStorage,
    pub core: RingCore,
    hdr_buf: Box<HeaderBuf>,
}

impl SimRing {
    pub fn create(cfg: Config) -> SimRing {
        let geo = cfg.geometry().unwrap();
        let flags = match cfg.checksum {
            ChecksumMode::None => 0,
            ChecksumMode::PerRecord => FLAG_CHECKSUM_PER_RECORD,
        };
        SimRing::open(cfg, SimDisk::formatted(&geo, flags)).unwrap()
    }

    pub fn open(cfg: Config, disk: SimDisk) -> io::Result<SimRing> {
        let mut storage = disk.storage();
        let core = RingCore::open(&mut storage, &cfg)?;
        let geo = *core.geometry();
        core.check_invariants();
        Ok(SimRing {
            cfg,
            geo,
            disk,
            storage,
            core,
            hdr_buf: Box::new([0; HEADER_SIZE]),
        })
    }

    /// Reopens with the same disk (process restart; not-yet-synced writes are visible too)
    pub fn reopen(self, cfg: Config) -> io::Result<SimRing> {
        let disk = self.disk.clone();
        drop(self);
        SimRing::open(cfg, disk)
    }

    /// Reopens with the same config
    pub fn restart(self) -> SimRing {
        let cfg = self.cfg.clone();
        self.reopen(cfg).unwrap()
    }

    /// Runs one flush. Returns None if there is nothing to write
    pub fn flush(&mut self) -> Option<io::Result<u64>> {
        let job = self.core.prepare_flush()?;
        let res = run_flush(
            &mut self.storage,
            self.core.memory(),
            job,
            &mut self.hdr_buf,
        );
        let res = self.core.complete_flush(res);
        self.core.check_invariants();
        Some(res)
    }

    /// Persists everything that was written
    pub fn flush_all(&mut self) {
        while let Some(res) = self.flush() {
            res.unwrap();
        }
        assert_eq!(self.core.durable(), self.core.write_pos());
    }

    /// On a stall, flushes and retries
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, AppendError> {
        loop {
            match self.core.try_append(payload) {
                Err(AppendError::Stall(_)) => {
                    self.flush()
                        .expect("stalled without anything to flush")
                        .expect("flush failed");
                }
                res => {
                    self.core.check_invariants();
                    return res;
                }
            }
        }
    }

    /// The records in `[tail, head)` pointed to by the on-disk header
    pub fn disk_records(&mut self) -> Records {
        let (from, to) = (self.core.disk_tail(), self.core.durable());
        scan_disk(&mut self.storage, &self.geo, self.cfg.checksum, from, to)
    }
}
