//! Startup recovery (section 8.3). It reads only the most recent flush range and the current
//! segment (+ warm_segments), so the time taken does not depend on disk capacity.

use std::io;

use crate::config::{ChecksumMode, Config, invalid};
use crate::header::{FLAG_CHECKSUM_PER_RECORD, Header, select};
use crate::io::{Storage, read_header_slots};
use crate::memory::RingMemory;
use crate::pos::Geometry;

/// Amount read at once when verifying range_crc.
const VERIFY_CHUNK: u64 = 1 << 20;

pub(crate) struct Recovered {
    /// The header that was selected.
    pub header: Header,
    pub head: u64,
    pub tail: u64,
    /// The lower bound of the range loaded into memory. `[mem_valid_low, head)` is in memory.
    pub mem_valid_low: u64,
    /// Number of bytes lost when range_crc did not match and we rolled back to prev_head.
    pub lost_bytes: u64,
}

/// # Safety
///
/// `mem` must not yet be referenced by any other thread.
pub(crate) unsafe fn recover<S: Storage>(
    storage: &mut S,
    cfg: &Config,
    geo: &Geometry,
    mem: &RingMemory,
) -> io::Result<Recovered> {
    // 1. From both slots, select the valid one with the largest seq.
    let slots = read_header_slots(storage)?;
    let header = select(&slots[0], &slots[1]).ok_or_else(|| corrupt("no valid header"))?;
    check_compat(&header, cfg, geo)?;
    if header.prev_head > header.head
        || header.head - header.prev_head > u64::from(header.runway_segments) * geo.seg_size()
    {
        return Err(corrupt("header has an invalid flush range"));
    }

    // 2. Verify the range_crc of data[prev_head, head). On mismatch, roll back to prev_head.
    let mut head = header.head;
    let mut lost_bytes = 0;
    if range_crc(storage, geo, header.prev_head, header.head)? != header.range_crc {
        lost_bytes = header.head - header.prev_head;
        head = header.prev_head;
    }

    // 3.
    let tail = header.tail.min(head);

    // 4. Load into memory [seg_start(head), head) of the segment containing head, plus warm_segments segments before it.
    let warm = u64::from(cfg.warm_segments) * geo.seg_size();
    let mem_valid_low = tail.max(geo.seg_start(head).saturating_sub(warm));
    for p in geo.pieces(mem_valid_low, head).iter() {
        // SAFETY: the caller guarantees mem is not referenced from anywhere else.
        storage.read_at(unsafe { mem.slice_mut(p.mem_off, p.len) }, p.disk_off)?;
    }

    Ok(Recovered {
        header,
        head,
        tail,
        mem_valid_low,
        lost_bytes,
    })
}

fn check_compat(header: &Header, cfg: &Config, geo: &Geometry) -> io::Result<()> {
    if u64::from(header.segment_size) != geo.seg_size()
        || u64::from(header.disk_segments) != geo.disk_segs()
    {
        return Err(invalid(
            "segment_size / disk_segments differ from the existing file",
        ));
    }
    let per_record = header.flags & FLAG_CHECKSUM_PER_RECORD != 0;
    if per_record != (cfg.checksum == ChecksumMode::PerRecord) {
        return Err(invalid("checksum mode differs from the existing file"));
    }
    Ok(())
}

/// The crc32c of `[start, end)` on disk. Read in chunks split by disk wrap and VERIFY_CHUNK.
fn range_crc<S: Storage>(storage: &mut S, geo: &Geometry, start: u64, end: u64) -> io::Result<u32> {
    let mut buf = vec![0u8; (end - start).min(VERIFY_CHUNK) as usize];
    let mut crc = 0;
    let mut cur = start;
    while cur < end {
        let len = (end - cur).min(geo.disk_room(cur)).min(VERIFY_CHUNK) as usize;
        storage.read_at(&mut buf[..len], geo.disk_off(cur))?;
        crc = crc32c::crc32c_append(crc, &buf[..len]);
        cur += len as u64;
    }
    Ok(crc)
}

fn corrupt(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
