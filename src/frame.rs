//! frame encode / decode (section 4.2).
//!
//! ```text
//!  0         4         8                         8+len     align 8
//!  +---------+---------+-------------------------+--------+
//!  | len u32 | crc u32 | payload (len bytes)     | pad    |
//!  +---------+---------+-------------------------+--------+
//! ```
//!
//! `len == u32::MAX` is the segment-end marker. The format is the same in memory and on disk.

use crate::pos::align8;

pub(crate) const FRAME_HDR: u64 = 8;
pub(crate) const END_MARKER: u32 = u32::MAX;

pub(crate) fn frame_len(payload_len: usize) -> u64 {
    align8(FRAME_HDR + payload_len as u64)
}

/// The 8 bytes of the frame header (little-endian `len`, `crc`) expressed as a native u64.
pub(crate) fn header_word(len: u32, crc: u32) -> u64 {
    (u64::from(len) | (u64::from(crc) << 32)).to_le()
}

/// per-record CRC (section 8.4). Mixes the logical position into the initial value so that leftovers from a previous lap are not mistaken for a valid record.
pub(crate) fn record_crc(pos: u64, payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&pos.to_le_bytes()), payload)
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Frame<'a> {
    Record {
        payload: &'a [u8],
        crc: u32,
    },
    /// Segment end. Advance to the start of the next segment.
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CorruptFrame;

/// `buf` is the byte range from a record boundary to the segment end.
pub(crate) fn decode(buf: &[u8]) -> Result<Frame<'_>, CorruptFrame> {
    let Some((hdr, rest)) = buf.split_first_chunk::<8>() else {
        return Err(CorruptFrame);
    };
    let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let crc = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    if len == END_MARKER {
        return Ok(Frame::End);
    }
    match rest.get(..len as usize) {
        Some(payload) => Ok(Frame::Record { payload, crc }),
        None => Err(CorruptFrame),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf = vec![0u8; 64];
        buf[..8].copy_from_slice(&header_word(3, 7).to_ne_bytes());
        buf[8..11].copy_from_slice(b"abc");
        assert_eq!(
            decode(&buf),
            Ok(Frame::Record {
                payload: b"abc",
                crc: 7
            })
        );
        assert_eq!(frame_len(3), 16);
        assert_eq!(frame_len(0), 8);

        buf[..8].copy_from_slice(&header_word(END_MARKER, 0).to_ne_bytes());
        assert_eq!(decode(&buf), Ok(Frame::End));
    }

    #[test]
    fn rejects_overrun() {
        let mut buf = vec![0u8; 16];
        buf[..8].copy_from_slice(&header_word(9, 0).to_ne_bytes());
        assert_eq!(decode(&buf), Err(CorruptFrame));
        assert_eq!(decode(&buf[..4]), Err(CorruptFrame));
    }

    #[test]
    fn crc_depends_on_position() {
        assert_ne!(record_crc(0, b"x"), record_crc(8, b"x"));
    }
}
