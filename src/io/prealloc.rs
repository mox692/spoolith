//! Preallocation at creation time (section 5.1).

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

use rustix::fs::FallocateFlags;

use crate::config::Preallocate;

const ZERO_CHUNK: u64 = 8 << 20;

pub(crate) fn preallocate(file: &File, len: u64, mode: Preallocate) -> io::Result<()> {
    match mode {
        Preallocate::ZeroFill => zero_fill(file, len),
        // on macOS, rustix implements this with F_PREALLOCATE + ftruncate
        Preallocate::Fallocate => {
            match rustix::fs::fallocate(file, FallocateFlags::empty(), 0, len) {
                Ok(()) => Ok(()),
                // on filesystems that do not support fallocate, make a sparse file
                Err(e) if io::Error::from(e).kind() == io::ErrorKind::Unsupported => {
                    file.set_len(len)
                }
                Err(e) => Err(e.into()),
            }
        }
    }
}

/// Actually writes zeros. From the first lap onward, every write overwrites an already-written extent.
fn zero_fill(file: &File, len: u64) -> io::Result<()> {
    let zeros = vec![0u8; ZERO_CHUNK.min(len) as usize];
    let mut off = 0;
    while off < len {
        let n = (len - off).min(zeros.len() as u64) as usize;
        file.write_all_at(&zeros[..n], off)?;
        off += n as u64;
    }
    Ok(())
}
