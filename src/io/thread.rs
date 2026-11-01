//! ThreadBackend (section 7.3): performs pwrite / fdatasync on a dedicated `std::thread`. Works on all OSes.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use super::{FlushDone, FlushJob, IoBackend, Storage, prealloc, run_flush};
use crate::config::{ChecksumMode, Config, SyncMode};
use crate::header::{FLAG_CHECKSUM_PER_RECORD, HEADER_SIZE, Header, new_instance_id, slot_offset};
use crate::memory::RingMemory;
use crate::pos::Geometry;

pub(crate) struct FileStorage {
    file: File,
    path: PathBuf,
    sync: SyncMode,
}

impl FileStorage {
    /// Opens the existing file. Creates it if it does not exist.
    pub fn open_or_create(cfg: &Config, geo: &Geometry) -> io::Result<FileStorage> {
        match fs::metadata(&cfg.path) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => create(cfg, geo)?,
            Err(e) => return Err(e),
        }
        let file = OpenOptions::new().read(true).write(true).open(&cfg.path)?;
        let len = file.metadata()?.len();
        if len != geo.file_len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "file size {len} does not match the configured geometry ({})",
                    geo.file_len()
                ),
            ));
        }
        Ok(FileStorage {
            file,
            path: cfg.path.clone(),
            sync: cfg.sync,
        })
    }
}

/// Preallocates and writes the initial header to a temporary file, syncs, then renames it.
/// If path exists, creation is already complete.
fn create(cfg: &Config, geo: &Geometry) -> io::Result<()> {
    let mut tmp = cfg.path.clone().into_os_string();
    tmp.push(".init");
    let tmp = PathBuf::from(tmp);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    prealloc::preallocate(&file, geo.file_len(), cfg.io.preallocate)?;

    let flags = match cfg.checksum {
        ChecksumMode::None => 0,
        ChecksumMode::PerRecord => FLAG_CHECKSUM_PER_RECORD,
    };
    let mut buf = Box::new([0u8; HEADER_SIZE]);
    Header::initial(geo, flags, new_instance_id()).encode_into(&mut buf);
    file.write_all_at(&buf[..], slot_offset(0))?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp, &cfg.path)?;
    sync_parent_dir(&cfg.path)
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    // Miri cannot fsync a directory fd
    if cfg!(miri) {
        return Ok(());
    }
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => File::open(dir)?.sync_all(),
        _ => File::open(".")?.sync_all(),
    }
}

impl Storage for FileStorage {
    fn write_at(&mut self, buf: &[u8], off: u64) -> io::Result<()> {
        self.file.write_all_at(buf, off)
    }

    fn read_at(&mut self, buf: &mut [u8], off: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, off)
    }

    fn sync(&mut self) -> io::Result<()> {
        match self.sync {
            SyncMode::None => Ok(()),
            SyncMode::Data => self.file.sync_data(),
            SyncMode::Full => self.file.sync_all(),
        }
    }

    fn reopen(&mut self) -> io::Result<()> {
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        Ok(())
    }
}

type Request = (FlushJob, oneshot::Sender<io::Result<u64>>);

pub(crate) struct ThreadBackend {
    tx: Option<mpsc::Sender<Request>>,
    thread: Option<JoinHandle<()>>,
}

impl ThreadBackend {
    pub fn spawn<S: Storage>(storage: S, mem: Arc<RingMemory>) -> io::Result<ThreadBackend> {
        let (tx, rx) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("ring-flush".into())
            .spawn(move || flush_thread(storage, mem, rx))?;
        Ok(ThreadBackend {
            tx: Some(tx),
            thread: Some(thread),
        })
    }
}

impl IoBackend for ThreadBackend {
    fn submit_flush(&self, job: FlushJob) -> FlushDone {
        let (done_tx, done_rx) = oneshot::channel();
        // if the thread has exited, done_tx is dropped and the receiver gets an error
        let _ = self
            .tx
            .as_ref()
            .expect("backend closed")
            .send((job, done_tx));
        done_rx
    }
}

impl Drop for ThreadBackend {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn flush_thread<S: Storage>(mut storage: S, mem: Arc<RingMemory>, rx: mpsc::Receiver<Request>) {
    let mut hdr_buf = Box::new([0u8; HEADER_SIZE]); // reused
    let mut failed = false;
    while let Ok((job, done)) = rx.recv() {
        let res = if failed { storage.reopen() } else { Ok(()) }
            .and_then(|()| run_flush(&mut storage, &mem, job, &mut hdr_buf));
        failed = res.is_err();
        let _ = done.send(res);
    }
}
