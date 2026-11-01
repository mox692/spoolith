//! I/O backend (section 7.3).
//!
//! - `Storage`: positioned read / write and sync. There is a regular file (`FileStorage`) and
//!   a crash-simulating disk for tests (`sim::SimStorage`).
//! - `IoBackend`: receives a FlushJob, runs it outside the ring thread, and signals completion.

pub(crate) mod prealloc;
#[cfg(test)]
pub(crate) mod sim;
pub(crate) mod thread;

use std::io;

use tokio::sync::oneshot;

use crate::header::{Header, HeaderBuf, slot_offset};
use crate::memory::RingMemory;
use crate::pos::Pieces;

pub(crate) trait Storage: Send + 'static {
    fn write_at(&mut self, buf: &[u8], off: u64) -> io::Result<()>;
    fn read_at(&mut self, buf: &mut [u8], off: u64) -> io::Result<()>;
    /// Persists according to the configured SyncMode.
    fn sync(&mut self) -> io::Result<()>;
    /// Called after an I/O error. Linux may treat a dirty page that failed fsync as clean, so
    /// we do not trust a retry on the same fd; we reopen and rewrite (section 13).
    fn reopen(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A loan of the ring-memory range `[prev_head, head)` and a snapshot of the header (section 7.2).
pub(crate) struct FlushJob {
    pub pieces: Pieces,
    /// The I/O backend fills in `range_crc` as it computes it while writing data.
    pub header: Header,
}

/// On completion, returns the persisted head.
pub(crate) type FlushDone = oneshot::Receiver<io::Result<u64>>;

pub(crate) trait IoBackend {
    /// Does not block.
    fn submit_flush(&self, job: FlushJob) -> FlushDone;
}

/// Steps of flush n (section 8.1): write data -> write header -> sync once.
pub(crate) fn run_flush<S: Storage>(
    storage: &mut S,
    mem: &RingMemory,
    mut job: FlushJob,
    hdr_buf: &mut HeaderBuf,
) -> io::Result<u64> {
    let mut crc = 0u32;
    for p in job.pieces.iter() {
        // SAFETY: [durable, inflight_end) is on loan and the writer does not touch it (I2).
        // References ring memory directly (no copy).
        let src = unsafe { mem.slice(p.mem_off, p.len) };
        storage.write_at(src, p.disk_off)?;
        crc = crc32c::crc32c_append(crc, src);
    }
    job.header.range_crc = crc;
    job.header.encode_into(hdr_buf);
    storage.write_at(hdr_buf, slot_offset(job.header.seq))?;
    storage.sync()?;
    Ok(job.header.head)
}

/// Reads the two slots in `[0x0000, 0x2000)`.
pub(crate) fn read_header_slots<S: Storage>(storage: &mut S) -> io::Result<Box<[HeaderBuf; 2]>> {
    let mut slots = Box::new([[0u8; crate::header::HEADER_SIZE]; 2]);
    storage.read_at(&mut slots[0], slot_offset(0))?;
    storage.read_at(&mut slots[1], slot_offset(1))?;
    Ok(slots)
}
