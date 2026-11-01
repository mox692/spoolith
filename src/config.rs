//! Configuration (section 14).

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use crate::pos::Geometry;

#[derive(Clone, Debug)]
pub struct Config {
    /// The ring file.
    pub path: PathBuf,
    /// S: power of two, at least 4 KiB (only at creation).
    pub segment_size: u32,
    /// D: arbitrary (only at creation). `D >= M + R` is required.
    pub disk_segments: u32,
    /// M: power of two, at least 2 (changeable on restart).
    pub memory_segments: u32,
    /// R: `1 <= R <= M - 1`. Upper bound on unpersisted data (in segments).
    pub runway_segments: u32,
    /// Upper bound written per flush. A multiple of S and `S <= max_flush_bytes <= R*S`.
    pub max_flush_bytes: u64,
    /// Data is flushed at most this long after it was appended (section 7.1).
    pub flush_interval: Duration,
    /// Flush right away once this many bytes are waiting, without waiting for `flush_interval`.
    /// 0 flushes as soon as anything is appended (back-to-back group commit). Should be well below
    /// `R*S`, otherwise appends stall on the runway before a flush starts.
    pub flush_threshold: u64,
    /// After this many consecutive failed flushes the ring is `Failed` (section 13). A failed flush
    /// is retried after `flush_interval`.
    pub max_flush_retries: u32,
    pub sync: SyncMode,
    pub io: IoConfig,
    /// Only at creation. open errors out if it differs from the existing file.
    pub checksum: ChecksumMode,
    /// Number of segments before head to load into memory at startup (`<= M - 1`).
    pub warm_segments: u32,
}

impl Config {
    /// Use the example from section 14 (S = 1 MiB, M = 1024, R = 64) as the defaults.
    pub fn new(path: impl Into<PathBuf>, disk_segments: u32) -> Self {
        Config {
            path: path.into(),
            segment_size: 1 << 20,
            disk_segments,
            memory_segments: 1024,
            runway_segments: 64,
            max_flush_bytes: 16 << 20,
            flush_interval: Duration::from_millis(10),
            flush_threshold: 4 << 20,
            max_flush_retries: 5,
            sync: SyncMode::Data,
            io: IoConfig::default(),
            checksum: ChecksumMode::None,
            warm_segments: 0,
        }
    }

    pub(crate) fn geometry(&self) -> io::Result<Geometry> {
        let geo = Geometry::new(
            self.segment_size,
            self.memory_segments,
            self.disk_segments,
            self.runway_segments,
        )?;
        let s = geo.seg_size();
        if self.max_flush_bytes < s
            || !self.max_flush_bytes.is_multiple_of(s)
            || self.max_flush_bytes > geo.runway_bytes()
        {
            return Err(invalid(
                "max_flush_bytes must be a multiple of S within [S, R*S]",
            ));
        }
        if u64::from(self.warm_segments) >= geo.mem_segs() {
            return Err(invalid("warm_segments must be <= M - 1"));
        }
        Ok(geo)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Write only. Survives a process crash, but not an OS crash / power loss.
    None,
    /// fdatasync (recommended default). On macOS, std uses F_FULLFSYNC.
    Data,
    /// fsync
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChecksumMode {
    None,
    /// Attach a crc32c per record (section 8.4).
    PerRecord,
}

#[derive(Clone, Debug, Default)]
pub struct IoConfig {
    pub preallocate: Preallocate,
}

/// The preallocation method at creation (section 5.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Preallocate {
    /// Actually write zeros.
    #[default]
    ZeroFill,
    /// fallocate only (F_PREALLOCATE on macOS). The first pass around makes sync heavier.
    Fallocate,
}

pub(crate) fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}
