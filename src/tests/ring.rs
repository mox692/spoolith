//! Ring + RingDriver on a tokio LocalRuntime, over a SimDisk.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use tokio::runtime::{Builder, LocalOptions, LocalRuntime};
use tokio::task::spawn_local;

use super::{MAX_PAYLOAD, Records, S, SimRing, payload, records_in, small_config};
use crate::config::{ChecksumMode, Config};
use crate::header::FLAG_CHECKSUM_PER_RECORD;
use crate::io::sim::SimDisk;
use crate::ring::{AppendError, ReadError, Ring, RingDriver};

const SEG: u64 = S as u64;

fn runtime() -> LocalRuntime {
    Builder::new_current_thread()
        .enable_time()
        .build_local(LocalOptions::default())
        .unwrap()
}

/// Flushes only on barriers, stalls and shutdown: the interval and threshold never fire.
fn config(m: u32, d: u32, r: u32) -> Config {
    let mut cfg = small_config(m, d, r);
    cfg.flush_interval = Duration::from_secs(3600);
    cfg.flush_threshold = u64::MAX;
    cfg.max_flush_retries = 2;
    cfg
}

fn new_disk(cfg: &Config) -> SimDisk {
    let flags = match cfg.checksum {
        ChecksumMode::None => 0,
        ChecksumMode::PerRecord => FLAG_CHECKSUM_PER_RECORD,
    };
    SimDisk::formatted(&cfg.geometry().unwrap(), flags)
}

fn open(cfg: &Config, disk: &SimDisk) -> (Ring, RingDriver) {
    Ring::with_storage(cfg, disk.storage()).unwrap()
}

/// Opens the disk with the core alone and checks that the durable records match the oracle
fn check_disk(cfg: &Config, disk: &SimDisk, oracle: &Records) -> SimRing {
    let mut ring = SimRing::open(cfg.clone(), disk.clone()).unwrap();
    let expected = records_in(oracle, ring.core.disk_tail(), ring.core.durable());
    assert_eq!(ring.disk_records(), expected);
    ring
}

/// All records in the memory window
fn read_all_hot(ring: &Ring) -> Records {
    let mut out = Vec::new();
    let mut pos = ring.mem_low();
    while let Some(next) = ring
        .read_hot(pos, |r| {
            out.push((r.pos, r.data.to_vec()));
            r.next_pos()
        })
        .unwrap()
    {
        pos = next;
    }
    out
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    let wait = async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("timed out");
}

#[test]
fn flushes_after_interval() {
    let mut cfg = config(4, 11, 2);
    cfg.flush_interval = Duration::from_millis(5);
    let disk = new_disk(&cfg);
    let oracle = runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        let mut oracle = Vec::new();
        for i in 0..10 {
            let p = payload(i, 100);
            oracle.push((ring.append(&p).await.unwrap(), p));
        }
        // append does not yield unless it stalls, so the driver has not run yet
        assert_eq!(ring.durable_pos(), 0);
        wait_until(|| ring.durable_pos() == ring.write_pos()).await;
        ring.shutdown().await.unwrap();
        driver.await.unwrap().unwrap();
        oracle
    });
    check_disk(&cfg, &disk, &oracle);
}

#[test]
fn flushes_at_threshold() {
    let mut cfg = config(4, 11, 2);
    cfg.flush_threshold = 1000;
    let disk = new_disk(&cfg);
    runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        while ring.write_pos() < 1000 {
            ring.append(&[7; 100]).await.unwrap();
        }
        wait_until(|| ring.durable_pos() == ring.write_pos()).await;

        // below the threshold, nothing is flushed until the (hour-long) interval
        let durable = ring.durable_pos();
        ring.append(&[8; 100]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(ring.durable_pos(), durable);
        drop(ring);
        driver.await.unwrap().unwrap();
    });
}

#[test]
fn flush_barrier() {
    let cfg = config(4, 11, 2);
    let disk = new_disk(&cfg);
    let oracle = runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let _driver = spawn_local(driver.run());
        let mut oracle = Vec::new();
        for i in 0..5 {
            let p = payload(i, 300);
            oracle.push((ring.append(&p).await.unwrap(), p));
        }
        // the barrier position is taken at the call, not at the first poll
        let barrier = ring.flush();
        let p = payload(99, 300);
        oracle.push((ring.append(&p).await.unwrap(), p));
        let target = barrier.await.unwrap();
        assert_eq!(target, 5 * 312);
        assert!(ring.durable_pos() >= target);
        oracle
    });
    let ring = check_disk(&cfg, &disk, &oracle);
    assert!(ring.core.durable() >= 5 * 312);
}

#[test]
fn try_append_reports_full() {
    let cfg = config(2, 3, 1);
    let disk = new_disk(&cfg);
    runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let _driver = spawn_local(driver.run());
        let big = payload(0, MAX_PAYLOAD);
        assert_eq!(ring.try_append(&big), Ok(0));
        assert_eq!(ring.try_append(&big), Err(AppendError::Full));
        // Full requested a flush
        wait_until(|| ring.durable_pos() == SEG).await;
        assert_eq!(ring.try_append(&big), Ok(SEG));
        assert_eq!(
            ring.try_append(&[0; MAX_PAYLOAD + 1]),
            Err(AppendError::TooLarge)
        );
        assert_eq!(ring.max_payload(), MAX_PAYLOAD);
    });
}

/// With R = 1 every segment needs a flush; append waits for the driver each time
#[test]
fn append_waits_for_runway() {
    let cfg = config(2, 3, 1);
    let disk = new_disk(&cfg);
    let oracle = runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        let mut oracle = Vec::new();
        for i in 0..40 {
            let p = payload(i, [MAX_PAYLOAD, 3000, 10][i as usize % 3]);
            oracle.push((ring.append(&p).await.unwrap(), p));
        }
        ring.shutdown().await.unwrap();
        driver.await.unwrap().unwrap();
        oracle
    });
    check_disk(&cfg, &disk, &oracle);
}

/// Several local tasks append concurrently and wait on the same flushes
#[test]
fn concurrent_appenders() {
    let cfg = config(2, 5, 1);
    let disk = new_disk(&cfg);
    let oracle = runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        let oracle = Rc::new(RefCell::new(Vec::new()));
        let tasks: Vec<_> = (0..4u64)
            .map(|t| {
                let (ring, oracle) = (ring.clone(), oracle.clone());
                spawn_local(async move {
                    for i in 0..50 {
                        let p = payload(t * 1000 + i, ((t * 977 + i * 1213) % 2500) as usize);
                        let pos = ring.append(&p).await.unwrap();
                        oracle.borrow_mut().push((pos, p));
                        if i % 7 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        // dropping the last handle shuts the ring down
        drop(ring);
        driver.await.unwrap().unwrap();
        let mut oracle = oracle.take();
        oracle.sort_by_key(|r| r.0);
        oracle
    });
    assert_eq!(oracle.len(), 200);
    check_disk(&cfg, &disk, &oracle);
}

#[test]
fn shutdown_marks_clean() {
    let cfg = config(4, 11, 2);
    let disk = new_disk(&cfg);
    let rt = runtime();
    let oracle = rt.block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        assert!(!ring.recovery_info().clean_shutdown);
        let mut oracle = Vec::new();
        for i in 0..20 {
            let p = payload(i, 500);
            oracle.push((ring.append(&p).await.unwrap(), p));
        }
        let shutdown = ring.shutdown();
        // refused as soon as shutdown is called
        assert_eq!(ring.try_append(&[1]), Err(AppendError::Closed));
        shutdown.await.unwrap();
        driver.await.unwrap().unwrap();
        assert_eq!(ring.append(&[1]).await, Err(AppendError::Closed));
        assert_eq!(ring.durable_pos(), ring.write_pos());
        assert_eq!(ring.flush().await.unwrap(), ring.write_pos());
        // reads still work
        assert_eq!(
            read_all_hot(&ring),
            records_in(&oracle, ring.mem_low(), u64::MAX)
        );
        oracle
    });
    let ring = check_disk(&cfg, &disk, &oracle);
    let info = ring.core.recovery_info();
    assert!(info.clean_shutdown && info.lost_bytes == 0);
    drop(ring);

    // the first flush after a restart clears the flag. Stop without shutdown (the runtime goes
    // away with the driver still running)
    let mut oracle = oracle;
    let ring = rt.block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        spawn_local(driver.run());
        assert!(ring.recovery_info().clean_shutdown);
        let p = payload(100, 10);
        oracle.push((ring.append(&p).await.unwrap(), p));
        ring.flush().await.unwrap();
        ring
    });
    drop(rt);
    assert_eq!(ring.try_append(&[1]), Err(AppendError::Failed));
    drop(ring);
    let ring = check_disk(&cfg, &disk, &oracle);
    assert!(!ring.core.recovery_info().clean_shutdown);
}

/// A failed flush is retried after flush_interval; the barrier waits through it
#[test]
fn retries_failed_flush() {
    let mut cfg = config(4, 11, 2);
    cfg.flush_interval = Duration::from_millis(2);
    let disk = new_disk(&cfg);
    let oracle = runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        let p = payload(0, 1000);
        let oracle = vec![(ring.append(&p).await.unwrap(), p)];
        disk.fail_next();
        assert_eq!(ring.flush().await.unwrap(), 1008);
        ring.shutdown().await.unwrap();
        driver.await.unwrap().unwrap();
        oracle
    });
    check_disk(&cfg, &disk, &oracle);
}

#[test]
fn fails_after_retry_limit() {
    let mut cfg = config(4, 11, 2);
    cfg.flush_interval = Duration::from_millis(2);
    let disk = new_disk(&cfg);
    runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let driver = spawn_local(driver.run());
        ring.append(&[1; 100]).await.unwrap();
        disk.fail_always(true);
        let err = ring.flush().await.unwrap_err();
        assert!(err.to_string().contains("injected"), "{err}");
        assert!(driver.await.unwrap().is_err());
        assert_eq!(ring.try_append(&[1]), Err(AppendError::Failed));
        assert!(ring.shutdown().await.is_err());
    });
    disk.fail_always(false);
    let ring = check_disk(&cfg, &disk, &Vec::new());
    assert_eq!(ring.core.durable(), 0);
}

/// A driver that is dropped without running fails the ring instead of hanging its waiters
#[test]
fn dropped_driver_fails_the_ring() {
    let cfg = config(4, 11, 2);
    let disk = new_disk(&cfg);
    runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        ring.append(&[1; 100]).await.unwrap();
        drop(driver);
        assert_eq!(ring.try_append(&[1]), Err(AppendError::Failed));
        assert!(ring.flush().await.is_err());
    });
}

#[test]
fn read_hot() {
    let mut cfg = config(4, 11, 2);
    cfg.checksum = ChecksumMode::PerRecord;
    let disk = new_disk(&cfg);
    runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let _driver = spawn_local(driver.run());
        let mut oracle = Vec::new();
        // wraps the memory window (M*S = 16 KiB) a few times
        for i in 0..60 {
            let p = payload(i, (i as usize * 389) % 2000);
            oracle.push((ring.append(&p).await.unwrap(), p));
        }
        assert!(ring.mem_low() > 0);
        assert_eq!(
            read_all_hot(&ring),
            records_in(&oracle, ring.mem_low(), u64::MAX)
        );

        let mem_low = ring.mem_low();
        assert_eq!(
            ring.read_hot(mem_low - 8, |_| ()),
            Err(ReadError::NotInMemory { mem_low })
        );
        assert_eq!(ring.read_hot(ring.write_pos(), |_| ()), Ok(None));
        assert_eq!(
            ring.read_hot(ring.write_pos() + 8, |_| ()),
            Err(ReadError::InvalidPosition)
        );
        assert_eq!(
            ring.read_hot(mem_low + 4, |_| ()),
            Err(ReadError::InvalidPosition)
        );

        // a position at a segment end reads the first record of the next segment
        let (gap, next) = oracle
            .windows(2)
            .map(|w| (w[0].0 + crate::frame::frame_len(w[0].1.len()), w[1].0))
            .find(|&(end, next)| end != next && end >= mem_low)
            .expect("no record skipped to the next segment");
        assert_eq!(ring.read_hot(gap, |r| r.pos), Ok(Some(next)));
    });
}

#[test]
#[should_panic(expected = "inside a read_hot closure")]
fn append_inside_read_hot_panics() {
    let cfg = config(4, 11, 2);
    let disk = new_disk(&cfg);
    runtime().block_on(async {
        let (ring, driver) = open(&cfg, &disk);
        let _driver = spawn_local(driver.run());
        ring.append(&[1; 10]).await.unwrap();
        let _ = ring.read_hot(0, |_| ring.try_append(&[2; 10]));
    });
}
