//! Position arithmetic (section 3, 5.1). All pure functions.
//!
//! A position is a monotonically increasing logical byte offset. The in-memory position is
//! `pos & (M*S - 1)`, and the on-disk position is `DATA_OFFSET + (seg(pos) mod D)*S + (pos & (S - 1))`.

use std::io;
use std::ops::Deref;

use crate::config::invalid;

/// The start position of the data region (after headers A / B).
pub(crate) const DATA_OFFSET: u64 = 0x2000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Geometry {
    seg_shift: u32,
    seg_size: u64,
    mem_segs: u64,
    disk_segs: u64,
    runway: u64,
}

impl Geometry {
    pub fn new(
        segment_size: u32,
        memory_segments: u32,
        disk_segments: u32,
        runway_segments: u32,
    ) -> io::Result<Self> {
        if !segment_size.is_power_of_two() || !(4096..=1 << 31).contains(&segment_size) {
            return Err(invalid(
                "segment_size must be a power of two in [4 KiB, 2 GiB]",
            ));
        }
        if !memory_segments.is_power_of_two() || memory_segments < 2 {
            return Err(invalid("memory_segments must be a power of two >= 2"));
        }
        if runway_segments < 1 || runway_segments >= memory_segments {
            return Err(invalid("runway_segments must satisfy 1 <= R <= M - 1"));
        }
        // H = D - 1 - R >= M - 1
        if u64::from(disk_segments) < u64::from(memory_segments) + u64::from(runway_segments) {
            return Err(invalid("disk_segments must satisfy D >= M + R"));
        }
        let mem_len = u64::from(segment_size) * u64::from(memory_segments);
        if usize::try_from(mem_len).map_or(true, |l| l > isize::MAX as usize) {
            return Err(invalid("M*S does not fit in memory"));
        }
        Ok(Geometry {
            seg_shift: segment_size.trailing_zeros(),
            seg_size: u64::from(segment_size),
            mem_segs: u64::from(memory_segments),
            disk_segs: u64::from(disk_segments),
            runway: u64::from(runway_segments),
        })
    }

    pub fn seg_size(&self) -> u64 {
        self.seg_size
    }

    pub fn mem_segs(&self) -> u64 {
        self.mem_segs
    }

    pub fn disk_segs(&self) -> u64 {
        self.disk_segs
    }

    pub fn runway_segs(&self) -> u64 {
        self.runway
    }

    /// R*S
    pub fn runway_bytes(&self) -> u64 {
        self.runway * self.seg_size
    }

    /// H*S (H = D - 1 - R). The amount of history guaranteed to be readable on disk.
    pub fn history_bytes(&self) -> u64 {
        (self.disk_segs - 1 - self.runway) * self.seg_size
    }

    /// M*S
    pub fn mem_len(&self) -> u64 {
        self.mem_segs * self.seg_size
    }

    /// The length of the whole file (2 header slots + D*S).
    pub fn file_len(&self) -> u64 {
        DATA_OFFSET + self.disk_segs * self.seg_size
    }

    pub fn seg(&self, pos: u64) -> u64 {
        pos >> self.seg_shift
    }

    pub fn seg_start(&self, pos: u64) -> u64 {
        pos & !(self.seg_size - 1)
    }

    pub fn seg_end(&self, pos: u64) -> u64 {
        self.seg_start(pos) + self.seg_size
    }

    /// The start of the segment containing the byte just before `e`.
    /// If `e` is on a segment boundary, this is the previous segment (0 if `e = 0`).
    pub fn last_seg_start(&self, e: u64) -> u64 {
        self.seg_start(e.saturating_sub(1))
    }

    pub fn mem_off(&self, pos: u64) -> usize {
        (pos & (self.mem_len() - 1)) as usize
    }

    pub fn disk_off(&self, pos: u64) -> u64 {
        DATA_OFFSET + (self.seg(pos) % self.disk_segs) * self.seg_size + (pos & (self.seg_size - 1))
    }

    /// Number of bytes from `pos` to the end of the data region (the on-disk wrap).
    pub fn disk_room(&self, pos: u64) -> u64 {
        (self.disk_segs - self.seg(pos) % self.disk_segs) * self.seg_size
            - (pos & (self.seg_size - 1))
    }

    /// `tail = max(floor, seg_start - H*S)` (section 3.4). `seg_start` must be a segment boundary.
    pub fn tail(&self, floor: u64, seg_start: u64) -> u64 {
        debug_assert_eq!(seg_start, self.seg_start(seg_start));
        floor.max(seg_start.saturating_sub(self.history_bytes()))
    }

    /// `mem_low = max(valid_low, seg_start - (M - 1)*S)` (section 3.4).
    pub fn mem_low(&self, valid_low: u64, seg_start: u64) -> u64 {
        debug_assert_eq!(seg_start, self.seg_start(seg_start));
        valid_low.max(seg_start.saturating_sub(self.mem_len() - self.seg_size))
    }

    /// Split `[start, end)` into intervals that are contiguous both in memory and on disk (section 7.2).
    /// If the length is at most M*S, memory and disk (D >= M) each wrap at most once, so at most 3 intervals.
    pub fn pieces(&self, start: u64, end: u64) -> Pieces {
        assert!(
            start <= end && end - start <= self.mem_len(),
            "range too long: [{start}, {end})"
        );
        let mut out = Pieces::default();
        let mut cur = start;
        while cur < end {
            let mem_room = self.mem_len() - self.mem_off(cur) as u64;
            let len = (end - cur).min(mem_room).min(self.disk_room(cur));
            out.push(Piece {
                mem_off: self.mem_off(cur),
                disk_off: self.disk_off(cur),
                len: len as usize,
            });
            cur += len;
        }
        out
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Piece {
    pub mem_off: usize,
    pub disk_off: u64,
    pub len: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Pieces {
    buf: [Piece; 3],
    n: usize,
}

impl Pieces {
    fn push(&mut self, p: Piece) {
        self.buf[self.n] = p;
        self.n += 1;
    }
}

impl Deref for Pieces {
    type Target = [Piece];
    fn deref(&self) -> &[Piece] {
        &self.buf[..self.n]
    }
}

pub(crate) fn align8(n: u64) -> u64 {
    (n + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn geometry() -> impl Strategy<Value = Geometry> {
        (0u32..3, 1u32..4)
            .prop_flat_map(|(s_log, m_log)| {
                let m = 1u32 << m_log;
                (Just(s_log), Just(m), 1..m, 0u32..10)
            })
            .prop_map(|(s_log, m, r, extra)| {
                Geometry::new(4096 << s_log, m, m + r + extra, r).unwrap()
            })
    }

    proptest! {
        /// Each interval is contiguous both in memory and on disk, and the per-byte mapping matches mem_off / disk_off.
        #[test]
        fn pieces_cover_range(geo in geometry(), start in 0u64..1 << 24, len_frac in 0.0f64..=1.0) {
            let len = (geo.mem_len() as f64 * len_frac) as u64;
            let pieces = geo.pieces(start, start + len);
            prop_assert!(pieces.len() <= 3);
            let mut cur = start;
            for p in pieces.iter() {
                prop_assert!(p.len > 0);
                prop_assert_eq!(p.mem_off, geo.mem_off(cur));
                prop_assert_eq!(p.disk_off, geo.disk_off(cur));
                let last = cur + p.len as u64 - 1;
                prop_assert_eq!(geo.mem_off(last), p.mem_off + p.len - 1);
                prop_assert_eq!(geo.disk_off(last), p.disk_off + p.len as u64 - 1);
                prop_assert!(p.disk_off + p.len as u64 <= geo.file_len());
                cur += p.len as u64;
            }
            prop_assert_eq!(cur, start + len);
        }

        #[test]
        fn tail_and_mem_low(geo in geometry(), seg in 0u64..10_000) {
            let s = geo.seg_start(seg * geo.seg_size());
            // Since H >= M - 1, tail <= mem_low.
            prop_assert!(geo.tail(0, s) <= geo.mem_low(0, s));
            prop_assert!(geo.mem_low(0, s) <= s);
        }
    }

    #[test]
    fn last_seg_start() {
        let geo = Geometry::new(4096, 4, 7, 2).unwrap();
        assert_eq!(geo.last_seg_start(0), 0);
        assert_eq!(geo.last_seg_start(1), 0);
        assert_eq!(geo.last_seg_start(4096), 0);
        assert_eq!(geo.last_seg_start(4097), 4096);
    }

    #[test]
    fn rejects_invalid() {
        assert!(Geometry::new(4096, 4, 6, 3).is_err()); // D < M + R
        assert!(Geometry::new(4096, 4, 7, 4).is_err()); // R > M - 1
        assert!(Geometry::new(4096, 3, 7, 1).is_err()); // M is not a power of two
        assert!(Geometry::new(2048, 4, 7, 1).is_err()); // S < 4 KiB
        assert!(Geometry::new(4096, 4, 7, 3).is_ok());
    }
}
