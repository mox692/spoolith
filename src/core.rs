//! RingCore (section 6.3, 7.2): the ring state. Owned solely by the ring thread
//! (`Cell`, !Send), and position updates use neither locks nor atomic RMW.

use std::cell::{Cell, RefCell};
use std::io;
use std::sync::Arc;

use crate::config::{ChecksumMode, Config};
use crate::frame::{CorruptFrame, Frame, decode, frame_len, record_crc};
use crate::header::{FLAG_CHECKSUM_PER_RECORD, FLAG_CLEAN_SHUTDOWN, Header, ReaderSlot};
use crate::io::{FlushJob, Storage};
use crate::memory::RingMemory;
use crate::pos::Geometry;
use crate::recovery::{self, Recovered};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendError {
    Stall(Stall),
    /// The frame does not fit in a segment (payload is at most S - 8).
    TooLarge,
    /// Appends are blocked (`set_appends_blocked`).
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stall {
    /// The runway is full (I2). Wait for a flush to complete.
    Durability,
}

impl From<Stall> for AppendError {
    fn from(s: Stall) -> Self {
        AppendError::Stall(s)
    }
}

/// The result of a hot read.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HotRead<R> {
    Record {
        next: u64,
        out: R,
    },
    /// Segment-end marker. `next` is the start of the next segment.
    SegmentEnd {
        next: u64,
    },
}

/// What recovery found when the ring was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryInfo {
    /// Number of bytes lost because the range_crc of the last flush did not match and recovery
    /// rolled back to the previous head (the header reached the disk but the data did not).
    pub lost_bytes: u64,
    /// The previous run ended with a clean shutdown (everything was durable).
    pub clean_shutdown: bool,
}

#[derive(Clone, Copy)]
struct Inflight {
    end: u64,
    tail: u64,
    /// This is the header-only flush that sets CLEAN_SHUTDOWN.
    clean: bool,
}

pub(crate) struct RingCore {
    geo: Geometry,
    mem: Arc<RingMemory>,
    checksum: bool,
    max_flush_bytes: u64,
    instance_id: [u8; 16],
    /// Flags written to the header (does not include CLEAN_SHUTDOWN).
    flags: u32,
    recovery_info: RecoveryInfo,

    /// The next position to write.
    write: Cell<u64>,
    /// The end of the segment the writer is in. `write == seg_end` means "this segment
    /// is used up, but we have not yet entered the next segment"; the next append takes
    /// the slow path.
    seg_end: Cell<u64>,
    /// Upper bound the fast path may write to: `min(seg_end, durable + R*S)`.
    limit: Cell<u64>,
    /// The position synced so far (= the head of the on-disk header).
    durable: Cell<u64>,
    /// The range lent out to the I/O backend (I5: at most one).
    inflight: Cell<Option<Inflight>>,
    /// The oldest readable position.
    tail: Cell<u64>,
    tail_floor: u64,
    /// The lower bound of the memory window.
    mem_low: Cell<u64>,
    mem_valid_low: u64,
    /// The tail of the header that most recently became durable.
    disk_tail: Cell<u64>,
    next_seq: Cell<u64>,
    /// The header must be written even if there is no data.
    header_dirty: Cell<bool>,
    /// If we restart with a larger R, perform one header-only flush before writing data (section 5.2).
    header_only_first: Cell<bool>,
    readers: RefCell<Vec<ReaderSlot>>,
    flush_failures: Cell<u32>,
    appends_blocked: Cell<bool>,
    /// Once everything is durable, write a header-only flush with CLEAN_SHUTDOWN (section 13).
    clean_requested: Cell<bool>,
    clean_written: Cell<bool>,
}

impl RingCore {
    /// Recover by reading the header. storage must already be created with an initial header written.
    pub fn open<S: Storage>(storage: &mut S, cfg: &Config) -> io::Result<RingCore> {
        let geo = cfg.geometry()?;
        let mem = Arc::new(RingMemory::new(geo.mem_len() as usize));
        // SAFETY: mem was just created and has not been shared with any other thread.
        let rec = unsafe { recovery::recover(storage, cfg, &geo, &mem)? };
        Ok(RingCore::from_recovered(cfg, geo, mem, rec))
    }

    fn from_recovered(
        cfg: &Config,
        geo: Geometry,
        mem: Arc<RingMemory>,
        rec: Recovered,
    ) -> RingCore {
        let Recovered {
            header,
            head,
            tail: tail_floor,
            mem_valid_low,
            lost_bytes,
        } = rec;
        // If head is on a segment boundary, the writer starts having used up the previous segment.
        let seg_end = if head == geo.seg_start(head) {
            head
        } else {
            geo.seg_end(head)
        };
        let cur_seg = seg_end.saturating_sub(geo.seg_size());
        let tail = geo.tail(tail_floor, cur_seg);
        let readers = header
            .readers
            .iter()
            .map(|r| ReaderSlot {
                name: r.name,
                committed: r.committed.clamp(tail, head),
            })
            .collect();
        let core = RingCore {
            geo,
            mem,
            checksum: cfg.checksum == ChecksumMode::PerRecord,
            max_flush_bytes: cfg.max_flush_bytes,
            instance_id: header.instance_id,
            flags: header.flags & FLAG_CHECKSUM_PER_RECORD,
            recovery_info: RecoveryInfo {
                lost_bytes,
                clean_shutdown: header.flags & FLAG_CLEAN_SHUTDOWN != 0,
            },
            write: Cell::new(head),
            seg_end: Cell::new(seg_end),
            limit: Cell::new(0),
            durable: Cell::new(head),
            inflight: Cell::new(None),
            tail: Cell::new(tail),
            tail_floor,
            mem_low: Cell::new(geo.mem_low(mem_valid_low, cur_seg)),
            mem_valid_low,
            disk_tail: Cell::new(tail_floor),
            next_seq: Cell::new(header.seq + 1),
            header_dirty: Cell::new(false),
            header_only_first: Cell::new(u64::from(header.runway_segments) < geo.runway_segs()),
            readers: RefCell::new(readers),
            flush_failures: Cell::new(0),
            appends_blocked: Cell::new(false),
            clean_requested: Cell::new(false),
            clean_written: Cell::new(false),
        };
        core.recompute_limit();
        core
    }

    /// Append one record and return its position. Per-record work is just one comparison + an 8B header + memcpy.
    #[inline]
    pub fn try_append(&self, payload: &[u8]) -> Result<u64, AppendError> {
        let need = frame_len(payload.len());
        if self.write.get() + need > self.limit.get() {
            self.slow_path(need)?;
        }
        let pos = self.write.get();
        let crc = if self.checksum {
            record_crc(pos, payload)
        } else {
            0
        };
        // SAFETY: [pos, pos + need) lies within the writer's segment at or after write, and is
        // referenced by neither the I/O backend (which reads only [durable, inflight_end)) nor any reader.
        unsafe { self.mem.write_frame(self.geo.mem_off(pos), crc, payload) };
        self.write.set(pos + need);
        Ok(pos)
    }

    /// Called at most a few times per segment (section 6.3).
    #[cold]
    fn slow_path(&self, need: u64) -> Result<(), AppendError> {
        if self.appends_blocked.get() {
            return Err(AppendError::Blocked);
        }
        if need > self.geo.seg_size() {
            return Err(AppendError::TooLarge);
        }
        self.recompute_limit();
        let w = self.write.get();
        if w + need <= self.limit.get() {
            return Ok(());
        }
        let seg_end = self.seg_end.get();
        if w + need <= seg_end {
            // It fits in the current segment, but the runway is insufficient.
            return Err(Stall::Durability.into());
        }
        // Check whether we can enter the next segment before moving (judge I2 at the post-move position).
        let budget = self.durable.get() + self.geo.runway_bytes();
        if seg_end > budget {
            return Err(Stall::Durability.into());
        }
        if w < seg_end {
            // SAFETY: [w, w + 8) is at or after write and within the current segment (by alignment the remainder is 0 or >= 8).
            unsafe { self.mem.write_end_marker(self.geo.mem_off(w)) };
        }
        self.write.set(seg_end);
        self.seg_end.set(seg_end + self.geo.seg_size());
        self.tail.set(self.geo.tail(self.tail_floor, seg_end));
        self.mem_low
            .set(self.geo.mem_low(self.mem_valid_low, seg_end));
        self.recompute_limit();
        if seg_end + need > budget {
            // We moved but the record still does not fit (happens with R = 1). Stalling without
            // moving would get stuck: when write == durable there is nothing to flush, so no progress.
            // Flushing up through the end marker makes `durable = seg_end`, and then it fits.
            return Err(Stall::Durability.into());
        }
        Ok(())
    }

    fn recompute_limit(&self) {
        self.limit.set(if self.appends_blocked.get() {
            0
        } else {
            self.seg_end
                .get()
                .min(self.durable.get() + self.geo.runway_bytes())
        });
    }

    /// While blocked, `try_append` fails with `AppendError::Blocked`. The fast path is unchanged:
    /// `limit` is pinned to 0, so every append takes the slow path, which checks the flag.
    pub fn set_appends_blocked(&self, blocked: bool) {
        self.appends_blocked.set(blocked);
        self.recompute_limit();
    }

    /// Once everything up to `write` is durable, the next flush is header-only and sets
    /// CLEAN_SHUTDOWN. Appends must be blocked first, so that nothing follows the flag.
    pub fn request_clean_shutdown(&self) {
        debug_assert!(self.appends_blocked.get());
        self.clean_requested.set(true);
    }

    /// The CLEAN_SHUTDOWN header is durable.
    pub fn clean_shutdown_written(&self) -> bool {
        self.clean_written.get()
    }

    /// Build a FlushJob that lends out `[durable, end)`. None if a flush is in flight or there is nothing to write.
    pub fn prepare_flush(&self) -> Option<FlushJob> {
        if self.inflight.get().is_some() {
            return None;
        }
        let start = self.durable.get();
        // We cut at write or at a segment boundary, so either way it is a record boundary (I4).
        let end = if self.header_only_first.get() {
            start
        } else {
            self.write
                .get()
                .min(self.geo.seg_start(start) + self.max_flush_bytes)
        };
        // set the flag only on a header-only flush after all data is durable (section 13)
        let clean = self.clean_requested.get()
            && !self.header_only_first.get()
            && start == self.write.get();
        if end == start
            && !self.header_dirty.get()
            && !self.header_only_first.get()
            && !(clean && !self.clean_written.get())
        {
            return None;
        }
        // Determine tail from the segment containing the byte just before end. It is at most the writer's tail.
        let tail = self
            .geo
            .tail(self.disk_tail.get(), self.geo.last_seg_start(end));
        let header = Header {
            instance_id: self.instance_id,
            seq: self.next_seq.get(),
            segment_size: self.geo.seg_size() as u32,
            disk_segments: self.geo.disk_segs() as u32,
            runway_segments: self.geo.runway_segs() as u32,
            flags: self.flags | if clean { FLAG_CLEAN_SHUTDOWN } else { 0 },
            head: end,
            prev_head: start,
            tail,
            range_crc: 0,
            readers: self.readers.borrow().clone(),
        };
        self.inflight.set(Some(Inflight { end, tail, clean }));
        self.header_dirty.set(false);
        Some(FlushJob {
            pieces: self.geo.pieces(start, end),
            header,
        })
    }

    /// Apply a completion notification from the I/O backend. On failure, do not advance durable; rewrite on the next flush (section 13).
    pub fn complete_flush(&self, res: io::Result<u64>) -> io::Result<u64> {
        let inflight = self
            .inflight
            .take()
            .expect("complete_flush without a flush in flight");
        match res {
            Ok(head) => {
                assert_eq!(
                    head, inflight.end,
                    "flush completed with an unexpected head"
                );
                self.durable.set(head);
                self.disk_tail.set(inflight.tail);
                self.next_seq.set(self.next_seq.get() + 1);
                self.header_only_first.set(false);
                self.clean_written.set(inflight.clean);
                self.flush_failures.set(0);
                self.recompute_limit();
                Ok(head)
            }
            Err(e) => {
                self.header_dirty.set(true);
                self.flush_failures.set(self.flush_failures.get() + 1);
                Err(e)
            }
        }
    }

    /// Read the frame at `pos` (`mem_low <= pos < write`, a record boundary) from memory.
    /// `f` receives a borrow of the payload. Do not append inside `f`.
    pub fn read_hot<R>(
        &self,
        pos: u64,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<HotRead<R>, CorruptFrame> {
        let write = self.write.get();
        assert!(
            self.mem_low.get() <= pos && pos < write,
            "pos {pos} is outside the hot range"
        );
        let seg_end = self.geo.seg_end(pos);
        let len = (seg_end.min(write) - pos) as usize;
        // SAFETY: [pos, min(seg_end, write)) is already written, and a slot is reused only
        // when the writer moves segments (which does not happen inside f).
        let buf = unsafe { self.mem.slice(self.geo.mem_off(pos), len) };
        match decode(buf)? {
            Frame::End => Ok(HotRead::SegmentEnd { next: seg_end }),
            Frame::Record { payload, crc } => {
                if self.checksum && crc != record_crc(pos, payload) {
                    return Err(CorruptFrame);
                }
                Ok(HotRead::Record {
                    next: pos + frame_len(payload.len()),
                    out: f(payload),
                })
            }
        }
    }

    pub fn geometry(&self) -> &Geometry {
        &self.geo
    }

    pub fn memory(&self) -> &Arc<RingMemory> {
        &self.mem
    }

    pub fn write_pos(&self) -> u64 {
        self.write.get()
    }

    pub fn durable(&self) -> u64 {
        self.durable.get()
    }

    pub fn tail(&self) -> u64 {
        self.tail.get()
    }

    pub fn mem_low(&self) -> u64 {
        self.mem_low.get()
    }

    #[cfg(test)]
    pub fn flush_in_flight(&self) -> bool {
        self.inflight.get().is_some()
    }

    pub fn flush_failures(&self) -> u32 {
        self.flush_failures.get()
    }

    pub fn recovery_info(&self) -> RecoveryInfo {
        self.recovery_info
    }

    /// The tail of the on-disk header (`[disk_tail, durable)` is readable on disk).
    #[cfg(test)]
    pub fn disk_tail(&self) -> u64 {
        self.disk_tail.get()
    }

    /// Check the invariants from section 3.5.
    #[cfg(test)]
    pub fn check_invariants(&self) {
        let s = self.geo.seg_size();
        let (tail, mem_low, durable, write) = (
            self.tail(),
            self.mem_low(),
            self.durable(),
            self.write_pos(),
        );
        let inflight_end = self.inflight.get().map_or(durable, |i| i.end);
        // I1
        assert!(
            tail <= mem_low
                && mem_low <= durable
                && durable <= inflight_end
                && inflight_end <= write,
            "I1: tail={tail} mem_low={mem_low} durable={durable} inflight_end={inflight_end} write={write}"
        );
        // I2
        assert!(
            write - durable <= self.geo.runway_bytes(),
            "I2: write={write} durable={durable}"
        );
        let seg_end = self.seg_end.get();
        assert!(seg_end.is_multiple_of(s) && write <= seg_end && write + s >= seg_end);
        assert!(tail.is_multiple_of(s) && mem_low.is_multiple_of(s) && write.is_multiple_of(8));
        let limit = if self.appends_blocked.get() {
            0
        } else {
            seg_end.min(durable + self.geo.runway_bytes())
        };
        assert_eq!(self.limit.get(), limit);
    }
}
