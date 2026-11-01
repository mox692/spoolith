//! Writes to a real file using FileStorage + ThreadBackend.

use std::fs;
use std::io;

use super::{Records, payload, records_in, scan_disk, scan_hot, small_config};
use crate::config::{ChecksumMode, Config, Preallocate};
use crate::core::{AppendError, RingCore};
use crate::io::IoBackend;
use crate::io::thread::{FileStorage, ThreadBackend};

fn file_config(dir: &tempfile::TempDir) -> Config {
    let mut cfg = small_config(4, 11, 2);
    cfg.path = dir.path().join("ring");
    cfg.checksum = ChecksumMode::PerRecord;
    cfg
}

fn flush(core: &RingCore, backend: &ThreadBackend) -> Option<io::Result<u64>> {
    let job = core.prepare_flush()?;
    let res = backend
        .submit_flush(job)
        .blocking_recv()
        .expect("flush thread exited");
    Some(core.complete_flush(res))
}

/// Writes, closes, reopens, and checks that the same data can be read back
fn write_and_reopen(cfg: &Config, records: u64) -> Records {
    let geo = cfg.geometry().unwrap();
    let mut oracle = Vec::new();
    {
        let mut storage = FileStorage::open_or_create(cfg, &geo).unwrap();
        let core = RingCore::open(&mut storage, cfg).unwrap();
        let backend = ThreadBackend::spawn(storage, core.memory().clone()).unwrap();
        for id in 0..records {
            let p = payload(id, (id as usize * 37) % 1500);
            let pos = loop {
                match core.try_append(&p) {
                    Ok(pos) => break pos,
                    Err(AppendError::Stall(_)) => flush(&core, &backend).unwrap().unwrap(),
                    Err(e) => panic!("{e:?}"),
                };
            };
            oracle.push((pos, p));
            core.check_invariants();
        }
        while let Some(res) = flush(&core, &backend) {
            res.unwrap();
        }
        assert_eq!(core.durable(), core.write_pos());
    }

    let mut storage = FileStorage::open_or_create(cfg, &geo).unwrap();
    let core = RingCore::open(&mut storage, cfg).unwrap();
    let head = oracle
        .last()
        .map_or(0, |(p, d)| p + crate::frame::frame_len(d.len()));
    assert_eq!(core.durable(), head);
    assert_eq!(core.recovery_info().lost_bytes, 0);
    let disk = scan_disk(&mut storage, &geo, cfg.checksum, core.disk_tail(), head);
    assert_eq!(disk, records_in(&oracle, core.disk_tail(), u64::MAX));
    assert_eq!(
        scan_hot(&core),
        records_in(&oracle, core.mem_low(), u64::MAX)
    );
    oracle
}

#[test]
fn roundtrip_through_file() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = file_config(&dir);
    // lap around the disk a few times
    write_and_reopen(&cfg, 500);
    // append to the existing file
    write_and_reopen(&cfg, 50);
    assert!(!dir.path().join("ring.init").exists());
}

#[test]
fn fallocate() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = file_config(&dir);
    cfg.io.preallocate = Preallocate::Fallocate;
    write_and_reopen(&cfg, 100);
    let len = fs::metadata(&cfg.path).unwrap().len();
    assert_eq!(len, cfg.geometry().unwrap().file_len());
}

/// A temporary file left behind by a crash mid-creation is rebuilt
#[test]
fn leftover_init_file() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = file_config(&dir);
    fs::write(dir.path().join("ring.init"), b"garbage").unwrap();
    write_and_reopen(&cfg, 10);
}

#[test]
fn rejects_wrong_file_size() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = file_config(&dir);
    write_and_reopen(&cfg, 10);
    let mut other = file_config(&dir);
    other.disk_segments = 12;
    let err = FileStorage::open_or_create(&other, &other.geometry().unwrap())
        .err()
        .unwrap();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
