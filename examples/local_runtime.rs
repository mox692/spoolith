//! CAN-like frames from a producer thread into a ring running on a tokio `LocalRuntime`. The
//! driver flushes them to disk once a second (`flush_interval`).
//!
//! ```text
//!  "can-rx" (std thread) ──mpsc──▶ "ring" (LocalRuntime, 1 worker) ──FlushJob──▶ "ring-flush" (I/O)
//!                                   ingest task + monitor task + RingDriver
//! ```
//!
//! `cargo run --release --example local_runtime -- [PATH] [SECONDS] [FRAMES_PER_SEC]`
//!
//! The ring file is kept (default: `$TMPDIR/spoolith_example.ring`, 64 MiB). Run it again to
//! see the recovery of the previous run and its last frame.

use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use spoolith::{Config, Preallocate, Ring};
use tokio::runtime::{Builder, LocalOptions};
use tokio::sync::mpsc;
use tokio::task::spawn_local;

/// A classic CAN frame with a receive timestamp.
#[derive(Clone, Copy, Debug)]
struct CanFrame {
    timestamp_us: u64,
    id: u32,
    dlc: u8,
    data: [u8; 8],
}

impl CanFrame {
    const LEN: usize = 21;

    fn encode(&self) -> [u8; Self::LEN] {
        let mut buf = [0; Self::LEN];
        buf[..8].copy_from_slice(&self.timestamp_us.to_le_bytes());
        buf[8..12].copy_from_slice(&self.id.to_le_bytes());
        buf[12] = self.dlc;
        buf[13..].copy_from_slice(&self.data);
        buf
    }

    fn decode(buf: &[u8]) -> Option<CanFrame> {
        let buf: &[u8; Self::LEN] = buf.try_into().ok()?;
        Some(CanFrame {
            timestamp_us: u64::from_le_bytes(buf[..8].try_into().unwrap()),
            id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            dlc: buf[12],
            data: buf[13..].try_into().unwrap(),
        })
    }
}

fn config(path: PathBuf) -> Config {
    let seg = 256 << 10;
    // 64 MiB on disk, 16 MiB in memory, at most 4 MiB not yet on disk
    let mut cfg = Config::new(path, 256);
    cfg.segment_size = seg;
    cfg.memory_segments = 64;
    cfg.runway_segments = 16;
    cfg.max_flush_bytes = 8 * u64::from(seg);
    // the driver flushes once a second; the threshold is a safety net for bursts (half the
    // runway), so appends do not stall on a full runway
    cfg.flush_interval = Duration::from_secs(1);
    cfg.flush_threshold = 8 * u64::from(seg);
    cfg.io.preallocate = Preallocate::Fallocate;
    cfg.warm_segments = 1;
    cfg
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().map_or_else(
        || std::env::temp_dir().join("spoolith_example.ring"),
        PathBuf::from,
    );
    let mut num = |default| match args.next() {
        Some(n) => n.parse().map_err(io::Error::other),
        None => Ok(default),
    };
    let seconds = num(5)?;
    // a busy 1 Mbit/s CAN bus
    let rate = num(8_000)?;

    let (tx, rx) = mpsc::channel(4096);
    let producer = thread::Builder::new()
        .name("can-rx".into())
        .spawn(move || produce(tx, Duration::from_secs(seconds), rate))?;
    let ring = thread::Builder::new()
        .name("ring".into())
        .spawn(move || ring_thread(config(path), rx))?;
    producer.join().unwrap();
    ring.join().unwrap()
}

/// Thread 1: a fake CAN bus, receiving `rate` frames per second for `duration`.
fn produce(tx: mpsc::Sender<CanFrame>, duration: Duration, rate: u64) {
    const IDS: [u32; 4] = [0x0c0, 0x1a0, 0x2f0, 0x7e8];
    let start = Instant::now();
    let mut n = 0u64;
    while start.elapsed() < duration {
        let due = (start.elapsed().as_secs_f64() * rate as f64) as u64;
        for n in n..due {
            let frame = CanFrame {
                timestamp_us: start.elapsed().as_micros() as u64,
                id: IDS[n as usize % IDS.len()],
                dlc: 8,
                data: n.to_le_bytes(),
            };
            if tx.blocking_send(frame).is_err() {
                // the ring thread is gone; it reports the error
                return;
            }
        }
        n = n.max(due);
        thread::sleep(Duration::from_millis(1));
    }
}

/// Thread 2: owns the `!Send` Ring. The I/O thread (thread 3) is started by `Ring::open`.
fn ring_thread(cfg: Config, mut rx: mpsc::Receiver<CanFrame>) -> io::Result<()> {
    let rt = Builder::new_current_thread()
        .enable_time()
        .build_local(LocalOptions::default())?;
    // blocking file I/O (create / recover), before the runtime starts serving
    let t = Instant::now();
    let path = cfg.path.clone();
    let (ring, driver) = Ring::open(cfg)?;
    println!("opened {} in {:.1?}", path.display(), t.elapsed());
    report_previous_run(&ring);

    rt.block_on(async {
        let driver = spawn_local(driver.run());
        let monitor = spawn_local(monitor(ring.clone()));

        let start = Instant::now();
        let first = ring.write_pos();
        let mut batch = Vec::with_capacity(512);
        let mut count = 0u64;
        while rx.recv_many(&mut batch, 512).await > 0 {
            count += batch.len() as u64;
            for frame in batch.drain(..) {
                // waits only when the runway is full (the disk is behind)
                ring.append(&frame.encode())
                    .await
                    .map_err(io::Error::other)?;
            }
        }
        let ingest = start.elapsed();
        let unflushed = ring.write_pos() - ring.durable_pos();
        ring.shutdown().await?;
        let total = start.elapsed();
        monitor.abort();
        driver.await.map_err(io::Error::other)??;

        let bytes = ring.write_pos() - first;
        println!(
            "appended {count} frames ({:.1} MiB in the ring) in {ingest:.2?}: {:.2} M frames/s",
            bytes as f64 / (1 << 20) as f64,
            count as f64 / ingest.as_secs_f64() / 1e6,
        );
        println!(
            "shutdown flushed the last {:.1} KiB in {:.2?}; durable = write = {}",
            unflushed as f64 / 1024.0,
            total - ingest,
            ring.durable_pos(),
        );
        Ok(())
    })
}

/// Another local task sharing the ring: the flushes happen in the background.
async fn monitor(ring: Ring) {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tick.tick().await;
        println!(
            "  write={:>10} durable={:>10} (not yet on disk: {:>5.0} KiB)",
            ring.write_pos(),
            ring.durable_pos(),
            (ring.write_pos() - ring.durable_pos()) as f64 / 1024.0,
        );
    }
}

fn report_previous_run(ring: &Ring) {
    let info = ring.recovery_info();
    if ring.write_pos() == 0 {
        println!("new ring");
        return;
    }
    println!(
        "recovered: clean_shutdown={} lost_bytes={} tail={} head={}",
        info.clean_shutdown,
        info.lost_bytes,
        ring.tail(),
        ring.write_pos(),
    );
    // warm_segments keeps the newest data in memory
    let mut last = None;
    let mut pos = ring.mem_low();
    while let Ok(Some(next)) = ring.read_hot(pos, |r| {
        last = CanFrame::decode(r.data);
        r.next_pos()
    }) {
        pos = next;
    }
    if let Some(f) = last {
        println!(
            "last frame of the previous run: t={}us id={:#05x} data={:02x?}",
            f.timestamp_us, f.id, f.data
        );
    }
}
