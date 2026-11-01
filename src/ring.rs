//! The public handle (section 6, 7, 13).
//!
//! `Ring::open` recovers the file and starts the I/O thread ("ring-flush"). `Ring` is !Send: it
//! lives on one thread, typically inside a tokio `LocalRuntime` (or a current_thread runtime +
//! `LocalSet`). Integrating it into the runtime is up to the user: spawn `RingDriver::run` as a
//! local task and it flushes in the background, while other local tasks append and read.
//!
//! ```text
//!  producer threads ──(any channel)──▶ ring thread: Ring + RingDriver ──FlushJob──▶ "ring-flush"
//! ```

use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::time::Instant;

use crate::config::Config;
use crate::core::{self, HotRead, RecoveryInfo, RingCore};
use crate::frame::{FRAME_HDR, frame_len};
use crate::io::thread::{FileStorage, ThreadBackend};
use crate::io::{IoBackend, Storage};

/// A handle to the ring. Cloning is cheap and all clones share one ring. !Send.
///
/// When the last handle is dropped, the ring shuts down as if `shutdown` had been called: the
/// driver flushes what is left, writes the clean-shutdown header, and exits.
pub struct Ring {
    inner: Rc<Inner>,
}

/// Runs the flushes of a ring. Must be polled, typically with
/// `tokio::task::spawn_local(driver.run())`; otherwise appends stall once the runway is full and
/// `flush` / `shutdown` never complete. Dropping it before the ring is closed fails the ring.
#[must_use = "the driver must be run (e.g. `tokio::task::spawn_local(driver.run())`) for flushes to happen"]
pub struct RingDriver {
    inner: Rc<Inner>,
}

/// A record borrowed from the ring memory.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    /// Position of the record.
    pub pos: u64,
    pub data: &'a [u8],
}

impl Record<'_> {
    /// Position of the next record (it may be the end of the segment; `read_hot` skips that).
    pub fn next_pos(&self) -> u64 {
        self.pos + frame_len(self.data.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendError {
    /// The runway is full until a flush completes. Only `try_append` returns this; `append` waits.
    Full,
    /// The payload is longer than `Ring::max_payload`.
    TooLarge,
    /// `shutdown` was called, or every `Ring` handle was dropped.
    Closed,
    /// Flushes kept failing (`Config::max_flush_retries`) or the driver stopped. The cause is
    /// returned by `RingDriver::run`, `flush` and `shutdown`.
    Failed,
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AppendError::Full => "the runway is full until a flush completes",
            AppendError::TooLarge => "the record does not fit in a segment",
            AppendError::Closed => "the ring is closed",
            AppendError::Failed => "the ring failed",
        })
    }
}

impl std::error::Error for AppendError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// The position is older than the memory window (`Ring::mem_low`). Reading from disk (cold
    /// read, section 9.4) is not implemented yet.
    NotInMemory { mem_low: u64 },
    /// The position is past the write position or not 8-byte aligned.
    InvalidPosition,
    /// The frame at `pos` is corrupt (a bad length, or a per-record crc mismatch).
    Corrupt { pos: u64 },
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::NotInMemory { mem_low } => {
                write!(f, "position is older than the memory window ({mem_low})")
            }
            ReadError::InvalidPosition => f.write_str("invalid position"),
            ReadError::Corrupt { pos } => write!(f, "corrupt record at {pos}"),
        }
    }
}

impl std::error::Error for ReadError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Open,
    /// Appends are refused; the driver flushes everything and writes the CLEAN_SHUTDOWN header.
    Closing,
    /// Everything is durable and the I/O thread has exited.
    Closed,
    Failed,
}

struct Inner {
    core: RingCore,
    /// Dropped (and the I/O thread joined) when the driver exits.
    backend: RefCell<Option<ThreadBackend>>,
    flush_interval: Duration,
    flush_threshold: u64,
    max_flush_retries: u32,
    state: Cell<State>,
    /// Why the ring failed.
    failure: RefCell<Option<Arc<io::Error>>>,
    /// Number of `Ring` handles. The ring closes when it drops to 0.
    handles: Cell<usize>,
    /// Nesting depth of `read_hot` closures. Appends are blocked while it is > 0.
    reading: Cell<u32>,
    /// An append that moves `write` to this kicks the driver (set by the driver before it waits).
    kick_at: Cell<u64>,
    kicked: Cell<bool>,
    driver_waker: Cell<Option<Waker>>,
    /// Flush without waiting for the threshold or interval until `durable` reaches this
    /// (barriers, stalled appends).
    flush_target: Cell<u64>,
    /// Woken when a flush completes or the state changes.
    waiters: WakerList,
}

impl Ring {
    /// Opens the ring file, creating it if it does not exist, and recovers it (section 8.3).
    /// Starts the I/O thread. Blocks on file I/O, so call it before the ring thread starts serving
    /// (creating a large file with `Preallocate::ZeroFill` takes a while).
    pub fn open(cfg: Config) -> io::Result<(Ring, RingDriver)> {
        let geo = cfg.geometry()?;
        let storage = FileStorage::open_or_create(&cfg, &geo)?;
        Ring::with_storage(&cfg, storage)
    }

    pub(crate) fn with_storage<S: Storage>(
        cfg: &Config,
        mut storage: S,
    ) -> io::Result<(Ring, RingDriver)> {
        let core = RingCore::open(&mut storage, cfg)?;
        let backend = ThreadBackend::spawn(storage, core.memory().clone())?;
        let durable = core.durable();
        let inner = Rc::new(Inner {
            core,
            backend: RefCell::new(Some(backend)),
            flush_interval: cfg.flush_interval,
            flush_threshold: cfg.flush_threshold,
            max_flush_retries: cfg.max_flush_retries,
            state: Cell::new(State::Open),
            failure: RefCell::new(None),
            handles: Cell::new(1),
            reading: Cell::new(0),
            kick_at: Cell::new(durable + 1),
            kicked: Cell::new(false),
            driver_waker: Cell::new(None),
            flush_target: Cell::new(0),
            waiters: WakerList::default(),
        });
        Ok((
            Ring {
                inner: inner.clone(),
            },
            RingDriver { inner },
        ))
    }

    /// Appends a record and returns its position, or `Full` if the runway is full (a flush is
    /// requested). Does not wait.
    #[inline]
    pub fn try_append(&self, payload: &[u8]) -> Result<u64, AppendError> {
        let inner = &*self.inner;
        match inner.core.try_append(payload) {
            Ok(pos) => {
                if inner.core.write_pos() >= inner.kick_at.get() {
                    inner.kick();
                }
                Ok(pos)
            }
            Err(e) => Err(self.append_error(e)),
        }
    }

    #[cold]
    fn append_error(&self, e: core::AppendError) -> AppendError {
        let inner = &*self.inner;
        match e {
            core::AppendError::Stall(_) => {
                inner.request_flush(inner.core.write_pos());
                AppendError::Full
            }
            core::AppendError::TooLarge => AppendError::TooLarge,
            core::AppendError::Blocked => {
                // appending could reuse the memory the closure is borrowing
                assert!(
                    inner.reading.get() == 0,
                    "cannot append to a Ring inside a read_hot closure"
                );
                match inner.state.get() {
                    State::Failed => AppendError::Failed,
                    _ => AppendError::Closed,
                }
            }
        }
    }

    /// Appends a record and returns its position. If the runway is full, waits for a flush.
    pub async fn append(&self, payload: &[u8]) -> Result<u64, AppendError> {
        loop {
            match self.try_append(payload) {
                Err(AppendError::Full) => {}
                res => return res,
            }
            let durable = self.inner.core.durable();
            Wait::new(self.inner.clone(), move |inner| {
                (inner.core.durable() != durable || inner.state.get() != State::Open).then_some(())
            })
            .await;
        }
    }

    /// Flush barrier: resolves to the current write position once everything before it is
    /// durable. The position is taken when `flush` is called (not when the future is first
    /// polled), and the flush starts right away.
    pub fn flush(&self) -> impl Future<Output = io::Result<u64>> + 'static {
        let target = self.inner.core.write_pos();
        self.inner.request_flush(target);
        Wait::new(self.inner.clone(), move |inner| {
            if inner.core.durable() >= target {
                Some(Ok(target))
            } else if inner.state.get() == State::Failed {
                Some(Err(inner.failure()))
            } else {
                None
            }
        })
    }

    /// Refuses further appends, flushes everything, and marks the file as cleanly shut down
    /// (section 13). Resolves when the driver has finished and the I/O thread has exited. Reads
    /// still work afterwards.
    pub fn shutdown(&self) -> impl Future<Output = io::Result<()>> + 'static {
        self.inner.begin_close();
        Wait::new(self.inner.clone(), |inner| match inner.state.get() {
            State::Closed => Some(Ok(())),
            State::Failed => Some(Err(inner.failure())),
            State::Open | State::Closing => None,
        })
    }

    /// Reads the record at `pos` from the memory window and passes it to `f`. `pos` must be a
    /// record boundary: a position returned by `append`, a `Record::next_pos`, or `mem_low`.
    /// Returns `Ok(None)` at the write position (nothing more to read yet).
    ///
    /// Data that is not durable yet is visible too. `f` must not append to the ring (it panics).
    pub fn read_hot<R>(
        &self,
        pos: u64,
        f: impl FnOnce(Record<'_>) -> R,
    ) -> Result<Option<R>, ReadError> {
        let core = &self.inner.core;
        let write = core.write_pos();
        if pos > write || !pos.is_multiple_of(FRAME_HDR) {
            return Err(ReadError::InvalidPosition);
        }
        if pos < core.mem_low() {
            return Err(ReadError::NotInMemory {
                mem_low: core.mem_low(),
            });
        }
        let _guard = ReadGuard::new(&self.inner);
        let mut f = Some(f);
        let mut pos = pos;
        while pos < write {
            let at = pos;
            let read = core.read_hot(at, |data| {
                f.take().expect("read twice")(Record { pos: at, data })
            });
            match read {
                Ok(HotRead::Record { out, .. }) => return Ok(Some(out)),
                Ok(HotRead::SegmentEnd { next }) => pos = next,
                Err(_) => return Err(ReadError::Corrupt { pos: at }),
            }
        }
        Ok(None)
    }

    /// The next record is written here.
    pub fn write_pos(&self) -> u64 {
        self.inner.core.write_pos()
    }

    /// Everything before this is on disk.
    pub fn durable_pos(&self) -> u64 {
        self.inner.core.durable()
    }

    /// Oldest position in the memory window: `[mem_low, write_pos)` can be read with `read_hot`.
    pub fn mem_low(&self) -> u64 {
        self.inner.core.mem_low()
    }

    /// Oldest valid position. Older data has been overwritten on disk.
    pub fn tail(&self) -> u64 {
        self.inner.core.tail()
    }

    /// The largest payload a record can have (segment size − 8).
    pub fn max_payload(&self) -> usize {
        (self.inner.core.geometry().seg_size() - FRAME_HDR) as usize
    }

    pub fn recovery_info(&self) -> RecoveryInfo {
        self.inner.core.recovery_info()
    }
}

impl Clone for Ring {
    fn clone(&self) -> Ring {
        self.inner.handles.set(self.inner.handles.get() + 1);
        Ring {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;
        self.inner.handles.set(handles);
        if handles == 0 {
            self.inner.begin_close();
        }
    }
}

impl fmt::Debug for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let core = &self.inner.core;
        f.debug_struct("Ring")
            .field("state", &self.inner.state.get())
            .field("tail", &core.tail())
            .field("mem_low", &core.mem_low())
            .field("durable", &core.durable())
            .field("write", &core.write_pos())
            .finish()
    }
}

impl RingDriver {
    /// Flushes until the ring is closed (`Ok`) or fails (`Err`). Needs a tokio runtime with the
    /// time driver enabled (`LocalRuntime::new()`, or `Builder::enable_time`).
    ///
    /// Data is flushed `flush_interval` after it was appended, or right away once
    /// `flush_threshold` bytes are waiting, a `flush` barrier waits, or an append stalls (section 7.1).
    pub async fn run(self) -> io::Result<()> {
        let inner = &*self.inner;
        let core = &inner.core;
        let mut sleep = Box::pin(tokio::time::sleep(Duration::ZERO));
        // the oldest data waiting to be flushed must be flushed by this time
        let mut deadline: Option<Instant> = None;
        // a failed flush is retried no earlier than this
        let mut retry_at: Option<Instant> = None;
        loop {
            // everything below is evaluated afresh, so earlier kicks are accounted for
            inner.kicked.set(false);
            let state = inner.state.get();
            match state {
                State::Failed => return Err(inner.failure()),
                State::Closing if core.clean_shutdown_written() => {
                    inner.finish_close();
                    return Ok(());
                }
                _ => {}
            }
            let now = Instant::now();
            let pending = core.write_pos() - core.durable();
            if pending == 0 {
                deadline = None;
            } else if deadline.is_none() {
                deadline = Some(after(now, inner.flush_interval));
            }
            let due = state == State::Closing
                || inner.flush_target.get() > core.durable()
                || pending > 0
                    && (pending >= inner.flush_threshold || deadline.is_some_and(|d| d <= now));
            let backoff = retry_at.is_some_and(|t| now < t);
            if due && !backoff {
                if let Some(job) = core.prepare_flush() {
                    inner.kick_at.set(u64::MAX);
                    let done = inner
                        .backend
                        .borrow()
                        .as_ref()
                        .expect("I/O backend is gone")
                        .submit_flush(job);
                    let res = done
                        .await
                        .unwrap_or_else(|_| Err(io::Error::other("the I/O thread exited")));
                    match core.complete_flush(res) {
                        Ok(_) => {
                            retry_at = None;
                            // data appended during the flush arrived after `now`
                            deadline = Some(after(now, inner.flush_interval));
                        }
                        Err(e) if core.flush_failures() > inner.max_flush_retries => {
                            inner.fail(e);
                            drop(inner.backend.take());
                            return Err(inner.failure());
                        }
                        Err(_) => retry_at = Some(after(Instant::now(), inner.flush_interval)),
                    }
                    inner.waiters.wake_all();
                    continue;
                }
                debug_assert!(state != State::Closing, "closing with nothing to flush");
            }

            // wait for a kick (an append crossing kick_at, a barrier, a stall, shutdown) or a timer
            let wake_at = if backoff { retry_at } else { deadline };
            inner.kick_at.set(match (backoff, pending) {
                (true, _) => u64::MAX,
                // the first append starts the deadline
                (false, 0) => core.durable() + 1,
                (false, _) => core.durable().saturating_add(inner.flush_threshold),
            });
            if let Some(t) = wake_at {
                sleep.as_mut().reset(t);
            }
            poll_fn(|cx| {
                if inner.kicked.get() {
                    return Poll::Ready(());
                }
                if wake_at.is_some() && sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
                inner.set_driver_waker(cx.waker());
                Poll::Pending
            })
            .await;
        }
    }
}

/// `t + d`, saturating far in the future (a huge `flush_interval` means "never on its own").
fn after(t: Instant, d: Duration) -> Instant {
    t.checked_add(d)
        .unwrap_or_else(|| t + Duration::from_secs(30 * 365 * 86400))
}

impl Drop for RingDriver {
    fn drop(&mut self) {
        let inner = &self.inner;
        inner.driver_waker.take();
        if matches!(inner.state.get(), State::Open | State::Closing) {
            inner.fail(io::Error::other("the ring driver stopped"));
        }
    }
}

impl Inner {
    fn kick(&self) {
        self.kick_at.set(u64::MAX);
        self.kicked.set(true);
        if let Some(w) = self.driver_waker.take() {
            w.wake();
        }
    }

    fn set_driver_waker(&self, waker: &Waker) {
        let w = match self.driver_waker.take() {
            Some(w) if w.will_wake(waker) => w,
            _ => waker.clone(),
        };
        self.driver_waker.set(Some(w));
    }

    /// Asks the driver to flush everything before `target` right away.
    fn request_flush(&self, target: u64) {
        if target > self.flush_target.get().max(self.core.durable()) {
            self.flush_target.set(target);
            self.kick();
        }
    }

    fn refresh_blocked(&self) {
        self.core
            .set_appends_blocked(self.state.get() != State::Open || self.reading.get() > 0);
    }

    fn begin_close(&self) {
        if self.state.get() == State::Open {
            self.state.set(State::Closing);
            self.refresh_blocked();
            self.core.request_clean_shutdown();
            self.kick();
            self.waiters.wake_all();
        }
    }

    fn finish_close(&self) {
        self.state.set(State::Closed);
        // no flush is in flight, so the I/O thread exits right away
        drop(self.backend.take());
        self.waiters.wake_all();
    }

    fn fail(&self, err: io::Error) {
        self.failure.replace(Some(Arc::new(err)));
        self.state.set(State::Failed);
        self.refresh_blocked();
        self.waiters.wake_all();
    }

    fn failure(&self) -> io::Error {
        let err = self
            .failure
            .borrow()
            .clone()
            .expect("failed without an error");
        io::Error::new(err.kind(), err)
    }
}

/// Blocks appends while a `read_hot` closure borrows the ring memory.
struct ReadGuard<'a>(&'a Inner);

impl<'a> ReadGuard<'a> {
    fn new(inner: &'a Inner) -> Self {
        inner.reading.set(inner.reading.get() + 1);
        inner.refresh_blocked();
        ReadGuard(inner)
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.0.reading.set(self.0.reading.get() - 1);
        self.0.refresh_blocked();
    }
}

/// Wakers of the tasks waiting in `Wait`. Single-threaded, so no lock (section 7.4).
#[derive(Default)]
struct WakerList {
    slots: RefCell<Vec<Option<Waker>>>,
    free: RefCell<Vec<usize>>,
}

impl WakerList {
    fn register(&self, key: &mut Option<usize>, waker: &Waker) {
        let k = *key.get_or_insert_with(|| {
            self.free.borrow_mut().pop().unwrap_or_else(|| {
                let mut slots = self.slots.borrow_mut();
                slots.push(None);
                slots.len() - 1
            })
        });
        match &mut self.slots.borrow_mut()[k] {
            Some(w) if w.will_wake(waker) => {}
            slot => *slot = Some(waker.clone()),
        }
    }

    fn remove(&self, key: usize) {
        self.slots.borrow_mut()[key] = None;
        self.free.borrow_mut().push(key);
    }

    fn wake_all(&self) {
        // take them first: waking must not happen while the list is borrowed
        let wakers: Vec<Waker> = self
            .slots
            .borrow_mut()
            .iter_mut()
            .filter_map(Option::take)
            .collect();
        for w in wakers {
            w.wake();
        }
    }
}

/// Resolves once `ready` returns `Some`. Checked again whenever a flush completes or the state
/// changes.
struct Wait<F> {
    inner: Rc<Inner>,
    key: Option<usize>,
    ready: F,
}

impl<F> Wait<F> {
    fn new<T>(inner: Rc<Inner>, ready: F) -> Self
    where
        F: FnMut(&Inner) -> Option<T>,
    {
        Wait {
            inner,
            key: None,
            ready,
        }
    }
}

impl<T, F: FnMut(&Inner) -> Option<T> + Unpin> Future for Wait<F> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        if let Some(v) = (this.ready)(&this.inner) {
            if let Some(k) = this.key.take() {
                this.inner.waiters.remove(k);
            }
            return Poll::Ready(v);
        }
        this.inner.waiters.register(&mut this.key, cx.waker());
        Poll::Pending
    }
}

impl<F> Drop for Wait<F> {
    fn drop(&mut self) {
        if let Some(k) = self.key.take() {
            self.inner.waiters.remove(k);
        }
    }
}
