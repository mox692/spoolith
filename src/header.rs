//! Header encode / decode and A/B slot selection (section 5.2).
//!
//! Instead of transmuting the in-memory representation, explicitly encode / decode in little endian.

use crate::pos::Geometry;

pub(crate) const HEADER_SIZE: usize = 4096;
pub(crate) const MAGIC: [u8; 8] = *b"RBUFV2\0\0";
pub(crate) const VERSION: u32 = 1;
pub(crate) const MAX_READERS: usize = 64;
pub(crate) const READER_NAME_LEN: usize = 32;

pub(crate) const FLAG_CLEAN_SHUTDOWN: u32 = 1 << 0;
pub(crate) const FLAG_CHECKSUM_PER_RECORD: u32 = 1 << 1;

// Field offsets
const OFF_READERS: usize = 88;
const READER_SLOT_SIZE: usize = READER_NAME_LEN + 8;
const OFF_CRC: usize = HEADER_SIZE - 4;
const _: () = assert!(OFF_READERS + MAX_READERS * READER_SLOT_SIZE <= OFF_CRC);

pub(crate) type HeaderBuf = [u8; HEADER_SIZE];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReaderSlot {
    pub name: [u8; READER_NAME_LEN],
    pub committed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    /// Chosen randomly at creation time. Used to detect a mixed-up file.
    pub instance_id: [u8; 16],
    /// Header generation. The write target is slot = seq % 2.
    pub seq: u64,
    pub segment_size: u32,
    pub disk_segments: u32,
    pub runway_segments: u32,
    pub flags: u32,
    /// Durable write position (a record boundary).
    pub head: u64,
    /// Start position of this flush (= head of the previous header).
    pub prev_head: u64,
    /// Oldest valid position (monotonically increasing).
    pub tail: u64,
    /// crc32c of data[prev_head, head).
    pub range_crc: u32,
    pub readers: Vec<ReaderSlot>,
}

impl Header {
    /// Header for a freshly created ring (seq 0, empty ring).
    pub fn initial(geo: &Geometry, flags: u32, instance_id: [u8; 16]) -> Header {
        Header {
            instance_id,
            seq: 0,
            segment_size: geo.seg_size() as u32,
            disk_segments: geo.disk_segs() as u32,
            runway_segments: geo.runway_segs() as u32,
            flags,
            head: 0,
            prev_head: 0,
            tail: 0,
            range_crc: 0,
            readers: Vec::new(),
        }
    }

    pub fn encode_into(&self, buf: &mut HeaderBuf) {
        assert!(self.readers.len() <= MAX_READERS);
        buf.fill(0);
        let mut w = Writer { buf, off: 0 };
        w.bytes(&MAGIC);
        w.u32(VERSION);
        w.u32(HEADER_SIZE as u32);
        w.bytes(&self.instance_id);
        w.u64(self.seq);
        w.u32(self.segment_size);
        w.u32(self.disk_segments);
        w.u32(self.runway_segments);
        w.u32(self.flags);
        w.u64(self.head);
        w.u64(self.prev_head);
        w.u64(self.tail);
        w.u32(self.range_crc);
        w.u32(self.readers.len() as u32);
        debug_assert_eq!(w.off, OFF_READERS);
        for r in &self.readers {
            w.bytes(&r.name);
            w.u64(r.committed);
        }
        let crc = crc32c::crc32c(&buf[..OFF_CRC]);
        buf[OFF_CRC..].copy_from_slice(&crc.to_le_bytes());
    }

    /// Returns None if magic / version / header_crc are not correct.
    pub fn decode(buf: &HeaderBuf) -> Option<Header> {
        let crc = u32::from_le_bytes(buf[OFF_CRC..].try_into().unwrap());
        if crc32c::crc32c(&buf[..OFF_CRC]) != crc {
            return None;
        }
        let mut r = Reader { buf, off: 0 };
        if r.bytes::<8>() != MAGIC || r.u32() != VERSION || r.u32() != HEADER_SIZE as u32 {
            return None;
        }
        let mut h = Header {
            instance_id: r.bytes(),
            seq: r.u64(),
            segment_size: r.u32(),
            disk_segments: r.u32(),
            runway_segments: r.u32(),
            flags: r.u32(),
            head: r.u64(),
            prev_head: r.u64(),
            tail: r.u64(),
            range_crc: r.u32(),
            readers: Vec::new(),
        };
        let reader_count = r.u32() as usize;
        if reader_count > MAX_READERS {
            return None;
        }
        h.readers = (0..reader_count)
            .map(|_| ReaderSlot {
                name: r.bytes(),
                committed: r.u64(),
            })
            .collect();
        Some(h)
    }
}

/// A 128-bit value for mix-up detection. Cryptographic strength is not needed, so it is built from the key of std's RandomState.
pub(crate) fn new_instance_id() -> [u8; 16] {
    use std::hash::{BuildHasher, Hasher};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mut id = [0u8; 16];
    for half in id.chunks_mut(8) {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(now.as_nanos());
        h.write_u32(std::process::id());
        half.copy_from_slice(&h.finish().to_le_bytes());
    }
    id
}

/// In-file offset of the slot where header seq is written.
pub(crate) fn slot_offset(seq: u64) -> u64 {
    (seq % 2) * HEADER_SIZE as u64
}

/// Of the two slots, among those that decode correctly and sit in their own slot (seq % 2),
/// pick the one with the largest seq (section 8.3, item 1).
pub(crate) fn select(slot_a: &HeaderBuf, slot_b: &HeaderBuf) -> Option<Header> {
    let a = Header::decode(slot_a).filter(|h| h.seq % 2 == 0);
    let b = Header::decode(slot_b).filter(|h| h.seq % 2 == 1);
    match (a, b) {
        (Some(a), Some(b)) => Some(if a.seq > b.seq { a } else { b }),
        (a, b) => a.or(b),
    }
}

struct Writer<'a> {
    buf: &'a mut HeaderBuf,
    off: usize,
}

impl Writer<'_> {
    fn bytes(&mut self, b: &[u8]) {
        self.buf[self.off..self.off + b.len()].copy_from_slice(b);
        self.off += b.len();
    }
    fn u32(&mut self, v: u32) {
        self.bytes(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
}

struct Reader<'a> {
    buf: &'a HeaderBuf,
    off: usize,
}

impl Reader<'_> {
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let b = self.buf[self.off..self.off + N].try_into().unwrap();
        self.off += N;
        b
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.bytes())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(seq: u64) -> Header {
        let mut name = [0u8; READER_NAME_LEN];
        name[..7].copy_from_slice(b"indexer");
        Header {
            instance_id: [7; 16],
            seq,
            segment_size: 4096,
            disk_segments: 9,
            runway_segments: 2,
            flags: FLAG_CHECKSUM_PER_RECORD,
            head: 12345 * 8,
            prev_head: 12000 * 8,
            tail: 4096,
            range_crc: 0xdead_beef,
            readers: vec![
                ReaderSlot {
                    name,
                    committed: 99
                };
                MAX_READERS
            ],
        }
    }

    fn encode(h: &Header) -> Box<HeaderBuf> {
        let mut buf = Box::new([0u8; HEADER_SIZE]);
        h.encode_into(&mut buf);
        buf
    }

    #[test]
    fn roundtrip() {
        let h = sample(3);
        assert_eq!(Header::decode(&encode(&h)), Some(h));
    }

    #[test]
    fn detects_corruption() {
        let mut buf = encode(&sample(3));
        buf[100] ^= 1;
        assert_eq!(Header::decode(&buf), None);
        assert_eq!(Header::decode(&[0u8; HEADER_SIZE]), None);
    }

    #[test]
    fn select_latest_valid() {
        let a = encode(&sample(4));
        let b = encode(&sample(5));
        assert_eq!(select(&a, &b).unwrap().seq, 5);
        assert_eq!(select(&encode(&sample(6)), &b).unwrap().seq, 6);

        let mut torn = encode(&sample(7));
        torn[2000] ^= 0xff;
        assert_eq!(select(&a, &torn).unwrap().seq, 4);
        // Do not accept one whose slot and seq parity do not match
        assert_eq!(select(&b, &a), None);
    }
}
