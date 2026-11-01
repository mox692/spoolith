//! RingMemory (section 4.1). An aligned region of M*S bytes.
//!
//! Shared between the ring thread, the I/O backend, and holders of SharedSlice. We never create a
//! `&mut [u8]` spanning the whole buffer (another thread references part of it as `&[u8]`, so that would be aliasing UB).
//! Reads and writes handle only the target range through a raw pointer.

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::ptr::NonNull;

use crate::frame::{END_MARKER, FRAME_HDR, header_word};

const PAGE: usize = 4096;
const HUGE_PAGE: usize = 2 << 20;

pub(crate) struct RingMemory {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: Relies on the write range and the range referenced by other threads being disjoint (the invariant in section 3.5).
unsafe impl Send for RingMemory {}
unsafe impl Sync for RingMemory {}

impl RingMemory {
    /// Allocates `len` bytes and touches every page to prefault it (so no page fault happens on the hot path).
    pub fn new(len: usize) -> Self {
        assert!(len > 0 && len.is_multiple_of(PAGE));
        let align = if len >= HUGE_PAGE { HUGE_PAGE } else { PAGE };
        let layout = Layout::from_size_align(len, align).expect("invalid RingMemory layout");
        // SAFETY: the layout size is not 0
        let ptr = unsafe { alloc_zeroed(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout)
        };
        for off in (0..len).step_by(PAGE) {
            // SAFETY: off < len
            unsafe { ptr.add(off).write_volatile(0) };
        }
        RingMemory { ptr, layout }
    }

    pub fn len(&self) -> usize {
        self.layout.size()
    }

    /// Writes a frame (8B header + payload).
    ///
    /// # Safety
    /// `off` is a multiple of 8, `[off, off + 8 + payload.len())` is within the region, and is not referenced elsewhere.
    #[inline]
    pub unsafe fn write_frame(&self, off: usize, crc: u32, payload: &[u8]) {
        debug_assert!(
            off.is_multiple_of(8) && off + FRAME_HDR as usize + payload.len() <= self.len()
        );
        unsafe {
            let p = self.ptr.as_ptr().add(off);
            p.cast::<u64>()
                .write(header_word(payload.len() as u32, crc));
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                p.add(FRAME_HDR as usize),
                payload.len(),
            );
        }
    }

    /// Writes a segment-end marker.
    ///
    /// # Safety
    /// `off` is a multiple of 8, `[off, off + 8)` is within the region, and is not referenced elsewhere.
    pub unsafe fn write_end_marker(&self, off: usize) {
        debug_assert!(off.is_multiple_of(8) && off + FRAME_HDR as usize <= self.len());
        unsafe {
            self.ptr
                .as_ptr()
                .add(off)
                .cast::<u64>()
                .write(header_word(END_MARKER, 0))
        };
    }

    /// # Safety
    /// `[off, off + len)` is within the region and is not written elsewhere while it is borrowed.
    pub unsafe fn slice(&self, off: usize, len: usize) -> &[u8] {
        assert!(off <= self.len() && len <= self.len() - off);
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(off), len) }
    }

    /// # Safety
    /// `[off, off + len)` is within the region and is not read or written elsewhere while it is borrowed.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn slice_mut(&self, off: usize, len: usize) -> &mut [u8] {
        assert!(off <= self.len() && len <= self.len() - off);
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(off), len) }
    }
}

impl Drop for RingMemory {
    fn drop(&mut self) {
        // SAFETY: allocated with the same layout in new
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Frame, decode};

    #[test]
    fn frames() {
        let mem = RingMemory::new(PAGE);
        unsafe {
            mem.write_frame(0, 5, b"hello");
            mem.write_end_marker(16);
            assert_eq!(
                decode(mem.slice(0, PAGE)),
                Ok(Frame::Record {
                    payload: b"hello",
                    crc: 5
                })
            );
            assert_eq!(decode(mem.slice(16, PAGE - 16)), Ok(Frame::End));
        }
    }

    #[test]
    fn aligned() {
        let mem = RingMemory::new(HUGE_PAGE);
        assert_eq!(mem.ptr.as_ptr() as usize % HUGE_PAGE, 0);
    }
}
