//! A ring on a tokio `LocalRuntime`, fed from another thread:
//!
//! ```text
//!  "producer" (std thread) ──mpsc──▶ "ring" (LocalRuntime, 1 worker): Ring + RingDriver ──▶ "ring-flush" (I/O)
//! ```
//!
//! The ring thread owns the `!Send` `Ring`; the I/O thread is started by `Ring::open`.

use std::io;
use std::path::Path;
use std::thread;
use std::time::Duration;

use spoolith::{AppendError, Config, Preallocate, ReadError, Ring};
use tokio::runtime::{Builder, LocalOptions, LocalRuntime};
use tokio::sync::mpsc;
use tokio::task::spawn_local;

const SEG: u64 = 64 << 10;

fn config(path: &Path) -> Config {
    // 4 MiB on disk, 1 MiB in memory
    let mut cfg = Config::new(path, 64);
    cfg.segment_size = SEG as u32;
    cfg.memory_segments = 16;
    cfg.runway_segments = 4;
    cfg.max_flush_bytes = 2 * SEG;
    cfg.flush_threshold = SEG;
    cfg.flush_interval = Duration::from_millis(5);
    // Miri does not implement fallocate / F_PREALLOCATE
    cfg.io.preallocate = if cfg!(miri) {
        Preallocate::ZeroFill
    } else {
        Preallocate::Fallocate
    };
    cfg.warm_segments = 15;
    cfg
}

fn runtime() -> LocalRuntime {
    Builder::new_current_thread()
        .enable_time()
        .build_local(LocalOptions::default())
        .unwrap()
}

fn message(id: u64) -> Vec<u8> {
    format!("message {id:08} {}", "x".repeat((id % 32) as usize)).into_bytes()
}

fn message_id(data: &[u8]) -> u64 {
    let text = std::str::from_utf8(data).unwrap();
    let id = text.strip_prefix("message ").unwrap()[..8].parse().unwrap();
    assert_eq!(data, message(id), "corrupt message");
    id
}

/// Sends `count` messages from a std thread, as fast as the channel takes them.
fn spawn_producer(first: u64, count: u64) -> (mpsc::Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1024);
    let producer = thread::Builder::new()
        .name("producer".into())
        .spawn(move || {
            for id in first..first + count {
                tx.blocking_send(message(id)).unwrap();
            }
        })
        .unwrap();
    (rx, producer)
}

struct RunStats {
    /// Position of the first and the last message appended in this run.
    first_pos: u64,
    last_pos: u64,
    write: u64,
    clean_at_open: bool,
}

/// The ring thread: owns the Ring, runs the driver and an ingest loop on a LocalRuntime.
fn run_ring(cfg: Config, mut rx: mpsc::Receiver<Vec<u8>>) -> io::Result<RunStats> {
    let rt = runtime();
    // Ring::open blocks on file I/O, so it runs before the runtime starts serving
    let (ring, driver) = Ring::open(cfg)?;
    let clean_at_open = ring.recovery_info().clean_shutdown;
    rt.block_on(async move {
        let driver = spawn_local(driver.run());

        // another local task sharing the ring: a flush barrier in the middle of the ingest
        let barrier = spawn_local({
            let ring = ring.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let target = ring.flush().await?;
                assert!(ring.durable_pos() >= target);
                io::Result::Ok(target)
            }
        });

        let mut batch = Vec::with_capacity(256);
        let mut positions = Vec::new();
        while rx.recv_many(&mut batch, 256).await > 0 {
            for msg in batch.drain(..) {
                positions.push(ring.append(&msg).await.map_err(io::Error::other)?);
            }
        }
        // the producer is done (the channel closed)
        barrier.await.unwrap()?;
        let write = ring.write_pos();
        ring.shutdown().await?;
        driver.await.unwrap()?;

        assert_eq!(ring.durable_pos(), write);
        assert_eq!(ring.try_append(b"late"), Err(AppendError::Closed));
        assert!(positions.is_sorted() && positions.len() > 1);
        Ok(RunStats {
            first_pos: positions[0],
            last_pos: *positions.last().unwrap(),
            write,
            clean_at_open,
        })
    })
}

fn run_in_ring_thread(cfg: Config, first: u64, count: u64) -> RunStats {
    let (rx, producer) = spawn_producer(first, count);
    let ring = thread::Builder::new()
        .name("ring".into())
        .spawn(move || run_ring(cfg, rx))
        .unwrap();
    producer.join().unwrap();
    ring.join().unwrap().unwrap()
}

/// Reopens the file and returns the ids of the messages still in the memory window.
fn reopen_and_read(cfg: Config) -> (Ring, Vec<(u64, u64)>) {
    let (ring, driver) = Ring::open(cfg).unwrap();
    // reading only: without a driver the ring is Failed (no appends), but reads still work
    drop(driver);
    let mut out = Vec::new();
    let mut pos = ring.mem_low();
    while let Some(next) = ring
        .read_hot(pos, |r| {
            out.push((r.pos, message_id(r.data)));
            r.next_pos()
        })
        .unwrap()
    {
        pos = next;
    }
    (ring, out)
}

#[test]
fn producer_ring_and_io_threads() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&dir.path().join("ring"));
    // ~200 KiB: does not wrap the disk
    const N: u64 = 5_000;
    let run = run_in_ring_thread(cfg.clone(), 0, N);
    assert!(!run.clean_at_open);
    assert_eq!(run.first_pos, 0);

    let (ring, records) = reopen_and_read(cfg.clone());
    let info = ring.recovery_info();
    assert!(info.clean_shutdown);
    assert_eq!(info.lost_bytes, 0);
    assert_eq!(ring.durable_pos(), run.write);
    assert_eq!(ring.tail(), 0);
    // everything fits in the warm window
    let ids: Vec<u64> = records.iter().map(|r| r.1).collect();
    assert_eq!(ids, (0..N).collect::<Vec<_>>());
    assert_eq!(records.last().unwrap().0, run.last_pos);
    drop(ring);

    // a second run appends after the first one
    let second = run_in_ring_thread(cfg.clone(), N, N);
    assert!(second.clean_at_open);
    assert_eq!(second.first_pos, run.write);
    let (_, records) = reopen_and_read(cfg);
    let ids: Vec<u64> = records.iter().map(|r| r.1).collect();
    assert_eq!(ids, (0..2 * N).collect::<Vec<_>>());
}

/// Several laps around the disk: only the newest data survives, and the window is still intact.
#[test]
fn wraps_the_disk() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&dir.path().join("ring"));
    // ~12 MiB through a 4 MiB disk
    const N: u64 = 300_000;
    let run = run_in_ring_thread(cfg.clone(), 0, N);

    let (ring, records) = reopen_and_read(cfg);
    let info = ring.recovery_info();
    assert!(info.clean_shutdown);
    assert_eq!(info.lost_bytes, 0);
    assert_eq!(ring.durable_pos(), run.write);
    assert!(ring.tail() > 0);
    assert_eq!(
        ring.read_hot(ring.mem_low() - 8, |_| ()).err(),
        Some(ReadError::NotInMemory {
            mem_low: ring.mem_low()
        })
    );
    // the newest 15 segments + the head segment, contiguous up to the last message
    assert!(records.len() > 10_000);
    let ids: Vec<u64> = records.iter().map(|r| r.1).collect();
    let first = ids[0];
    assert_eq!(ids, (first..N).collect::<Vec<_>>());
    assert_eq!(records.last().unwrap().0, run.last_pos);
}
