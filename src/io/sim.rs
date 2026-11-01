//! A disk for tests (the crash simulation in section 17).
//!
//! Holds a "durable image" and a "list of not-yet-synced writes". `crash` applies a random subset
//! of the not-yet-synced writes to the image, including sector-granular tearing.

use std::io;
use std::sync::{Arc, Mutex};

use super::Storage;
use crate::header::{HEADER_SIZE, Header, slot_offset};
use crate::pos::Geometry;

const SECTOR: u64 = 512;

#[derive(Clone)]
pub(crate) struct SimDisk {
    state: Arc<Mutex<State>>,
}

struct State {
    durable: Vec<u8>,
    /// durable with pending applied in order (reads observe this).
    current: Vec<u8>,
    pending: Vec<(u64, Vec<u8>)>,
    /// Some(n): crash after n more operations (every operation afterwards fails).
    ops_until_crash: Option<u64>,
    /// Make the next operation fail with an I/O error exactly once.
    fail_next: bool,
    /// Make every write / sync fail until cleared.
    fail_always: bool,
}

impl SimDisk {
    pub fn new(len: u64) -> SimDisk {
        let image = vec![0u8; len as usize];
        SimDisk {
            state: Arc::new(Mutex::new(State {
                durable: image.clone(),
                current: image,
                pending: Vec::new(),
                ops_until_crash: None,
                fail_next: false,
                fail_always: false,
            })),
        }
    }

    /// A disk with the initial header written and synced.
    pub fn formatted(geo: &Geometry, flags: u32) -> SimDisk {
        let disk = SimDisk::new(geo.file_len());
        let mut buf = Box::new([0u8; HEADER_SIZE]);
        Header::initial(geo, flags, [1; 16]).encode_into(&mut buf);
        let mut st = disk.storage();
        st.write_at(&buf[..], slot_offset(0)).unwrap();
        st.sync().unwrap();
        disk
    }

    pub fn storage(&self) -> SimStorage {
        SimStorage { disk: self.clone() }
    }

    pub fn crash_after(&self, ops: u64) {
        self.state.lock().unwrap().ops_until_crash = Some(ops);
    }

    pub fn fail_next(&self) {
        self.state.lock().unwrap().fail_next = true;
    }

    pub fn fail_always(&self, fail: bool) {
        self.state.lock().unwrap().fail_always = fail;
    }

    /// Simulates a power loss. For each not-yet-synced write, chooses one of: drop it / apply all of
    /// it / apply it randomly per sector. Returns a new disk holding the resulting image.
    pub fn crash(&self, rng: &mut SimRng) -> SimDisk {
        let st = self.state.lock().unwrap();
        let mut image = st.durable.clone();
        for (off, data) in &st.pending {
            let mode = rng.below(3);
            let end = off + data.len() as u64;
            let mut sector = off - off % SECTOR;
            while sector < end {
                let lo = sector.max(*off);
                let hi = (sector + SECTOR).min(end);
                let apply = match mode {
                    0 => false,
                    1 => true,
                    _ => rng.below(2) == 0,
                };
                if apply {
                    image[lo as usize..hi as usize]
                        .copy_from_slice(&data[(lo - off) as usize..(hi - off) as usize]);
                }
                sector += SECTOR;
            }
        }
        let disk = SimDisk::new(0);
        {
            let mut new = disk.state.lock().unwrap();
            new.durable = image.clone();
            new.current = image;
        }
        disk
    }
}

pub(crate) struct SimStorage {
    disk: SimDisk,
}

impl SimStorage {
    fn begin_op(st: &mut State) -> io::Result<()> {
        if std::mem::take(&mut st.fail_next) || st.fail_always {
            return Err(io::Error::other("injected I/O error"));
        }
        match &mut st.ops_until_crash {
            Some(0) => Err(io::Error::other("simulated crash")),
            Some(n) => {
                *n -= 1;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

impl Storage for SimStorage {
    fn write_at(&mut self, buf: &[u8], off: u64) -> io::Result<()> {
        let mut st = self.disk.state.lock().unwrap();
        Self::begin_op(&mut st)?;
        let range = off as usize..off as usize + buf.len();
        st.current[range].copy_from_slice(buf);
        st.pending.push((off, buf.to_vec()));
        Ok(())
    }

    fn read_at(&mut self, buf: &mut [u8], off: u64) -> io::Result<()> {
        let st = self.disk.state.lock().unwrap();
        buf.copy_from_slice(&st.current[off as usize..off as usize + buf.len()]);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        let mut st = self.disk.state.lock().unwrap();
        Self::begin_op(&mut st)?;
        let st = &mut *st;
        for (off, data) in st.pending.drain(..) {
            st.durable[off as usize..off as usize + data.len()].copy_from_slice(&data);
        }
        Ok(())
    }
}

/// SplitMix64.
pub(crate) struct SimRng(u64);

impl SimRng {
    pub fn new(seed: u64) -> SimRng {
        SimRng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}
