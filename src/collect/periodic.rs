use crate::collect::{self, BoxedDynObserver};
use crate::model::NameIdentity;
use crate::observe::{MetricSource, Mode, SyncObserver};
use crate::{Error, atomic, dto};

use super::timer::{CollectorTime, GridTimer};
use log::{error, warn};
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Instant;
use tokio::sync::mpsc as tmpsc;
use tokio::time::Duration;

/// Default capacity for the internal export batch channel when
/// [`Config::batch_capacity`] is not specified.
pub const DEFAULT_BATCH_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Egress sink for metric batches produced by a [`Collector`].
pub trait Sender<W>: Send + 'static {
    /// Ship one export batch.
    fn send(&mut self, batch: Vec<W>) -> impl Future<Output = ()> + Send;
}

impl<W> Sender<W> for tmpsc::UnboundedSender<Vec<W>>
where
    W: Send + 'static,
{
    fn send(&mut self, batch: Vec<W>) -> impl Future<Output = ()> + Send {
        if let Err(err) = tmpsc::UnboundedSender::send(self, batch) {
            error!("failed to send metrics batch: {err}");
        }
        std::future::ready(())
    }
}

// We could theoretically not collect anything instead of not sending anything, but pausing
// the collector will break the grid, and we'll need to reset all observers. It's still doable,
// though it's doable in addition to the gate already implemented here; I mean a gated collector
// is orthogonal to a gated exporter.
// Mainly, it exists to make it easier to use debug collectors like console collector in production:
// one doesn't need conditional compilation for collector collection and can just turn off console
// collector when they don't need it.
/// A [`Sender`] wrapper whose forwarding can be toggled at runtime through a shared
/// [`Arc<AtomicBool>`]. While disabled, batches are dropped instead of being handed to the inner
/// sender, so nothing leaves the process.
pub struct Gated<S> {
    inner: S,
    enabled: Arc<AtomicBool>,
}

impl<S> fmt::Debug for Gated<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gated")
            .field("enabled", &self.enabled.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<S> Gated<S> {
    pub fn new(inner: S) -> Self {
        Self::with_enabled(inner, Arc::new(AtomicBool::new(true)))
    }

    pub const fn with_enabled(inner: S, enabled: Arc<AtomicBool>) -> Self {
        Self { inner, enabled }
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn enabled_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.enabled)
    }
}

impl<W, S> Sender<W> for Gated<S>
where
    W: Send + 'static,
    S: Sender<W>,
{
    async fn send(&mut self, batch: Vec<W>) {
        if self.enabled.load(Ordering::Relaxed) {
            self.inner.send(batch).await;
        }
    }
}

/// Configuration for a periodic [`Collector`].
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Config {
    /// Call [`crate::observe::DynObserver::observe`] on all observers each `poll_period` duration.
    pub poll_period: Duration,

    /// Call [`crate::observe::DynObserver::export`] on accumulated metrics each `export_period`
    /// duration.
    pub export_period: Duration,

    /// Use the observers' start time and `seq_id` to generate perfect timestamps (i.e., for data
    /// compressibility).
    pub align_timestamps: bool,

    /// Capacity of the internal channel buffering export batches awaiting dispatch by the sender
    /// task. When full, newly produced batches are discarded. Defaults to
    /// [`DEFAULT_BATCH_CAPACITY`] when `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub batch_capacity: Option<NonZeroUsize>,
}

/// A reusable collector that polls registered observers on a fixed grid and sends non-empty export
/// batches through `S`.
pub struct Collector<W: Send + 'static> {
    inner: Arc<Inner<W>>,
}

impl<W: Send + 'static> Clone for Collector<W> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<W: Send + 'static> fmt::Debug for Collector<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collector").finish_non_exhaustive()
    }
}

struct Inner<W: Send + 'static> {
    cmd_tx: mpsc::Sender<CollectorCmd<W>>,
    handle: Option<JoinHandle<()>>,
    sender_task: tokio::task::JoinHandle<()>,
    /// Identities already registered with [`Mode::Destructive`]. A destructive observer resets the
    /// source buckets between observations, so allowing two of them for the same identity would
    /// make them race against each other.
    destructive_ids: Mutex<HashSet<Arc<NameIdentity>>>,
}

impl<W: Send + 'static> fmt::Debug for Inner<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner").finish_non_exhaustive()
    }
}

enum CollectorCmd<W: Send + 'static> {
    Add(BoxedDynObserver<W>),
    Shutdown,
}

impl<W: Send + 'static> fmt::Debug for CollectorCmd<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Add(_) => f.debug_tuple("Add").finish(),
            Self::Shutdown => f.write_str("Shutdown"),
        }
    }
}

impl<W: Send + 'static> Collector<W> {
    /// - `sender` - sender implementation used to send accumulated metric batches.
    pub fn new<S>(sender: S, config: Config) -> Result<Self, Error>
    where
        S: Sender<W>,
    {
        let Config {
            poll_period,
            export_period,
            align_timestamps,
            batch_capacity,
        } = config;

        if poll_period.is_zero() {
            return Err(Error::ZeroPollPeriod);
        }
        if export_period.is_zero() {
            return Err(Error::ZeroExportPeriod);
        }
        if poll_period > export_period {
            return Err(Error::PollPeriodExceedsExportPeriod);
        }

        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (batch_tx, batch_rx) =
            tmpsc::channel(batch_capacity.unwrap_or(DEFAULT_BATCH_CAPACITY).get());

        let sender_task = tokio::spawn(process_send(sender, batch_rx));

        let start = CollectorTime::now();
        let handle = std::thread::spawn(move || {
            Worker {
                cmd_rx,
                batch_tx,
                observers: Vec::new(),
                start,
                observe_timer: GridTimer::new(start.instant, poll_period),
                export_timer: GridTimer::new(start.instant, export_period),
                poll_period,
                align_timestamps,
            }
            .block();
        });

        Ok(Self {
            inner: Arc::new(Inner {
                cmd_tx,
                handle: Some(handle),
                sender_task,
                destructive_ids: Mutex::new(HashSet::new()),
            }),
        })
    }

    /// Add a metric source and check whether it has been registered previously in
    /// [`Mode::Destructive`]. If it was, it would fail.
    pub fn add<Src, T, S, A>(&self, source: Src, mode: Mode) -> Result<Src, Error>
    where
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<W, Error = Error>,
    {
        let observer = SyncObserver::new(&source, mode)?;
        if mode == Mode::Destructive {
            let mut destructive_ids = self
                .inner
                .destructive_ids
                .lock()
                .expect("destructive ids poisoned");
            if !destructive_ids.insert(Arc::clone(source.id())) {
                return Err(Error::DuplicateDestructive);
            }
        }
        self.add_observer(observer);
        Ok(source)
    }

    /// Adds a new raw observer. Unline [`Collector::add`], this method doesn't check for duplicate
    /// registrations of the same identity in [`Mode::Destructive`] mode, so you have to maintain
    /// invariants yourself.
    pub fn add_observer<T, S, A>(&self, observer: SyncObserver<T, S, A>)
    where
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<W, Error = Error>,
    {
        self.add_boxed_observer(Box::new(observer));
    }

    pub fn add_boxed_observer(&self, observer: BoxedDynObserver<W>) {
        self.inner
            .cmd_tx
            .send(CollectorCmd::Add(observer))
            .expect("collector worker stopped");
    }
}

impl<W, Src, T, S, A> collect::AddSource<Src> for Collector<W>
where
    W: Send + 'static,
    Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
    T: atomic::Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: atomic::Record<T>,
    dto::Series<A::Snapshot, S>: dto::IntoWire<W, Error = Error>,
{
    fn add_source(&self, source: &Src, mode: Mode) -> Result<(), Error> {
        self.add_boxed_observer(Box::new(SyncObserver::new(source, mode)?));
        Ok(())
    }
}

impl<W: Send + 'static> Drop for Inner<W> {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(CollectorCmd::Shutdown);

        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            error!("collector worker thread panicked");
        }

        self.sender_task.abort();
    }
}

struct Worker<W: Send + 'static> {
    cmd_rx: mpsc::Receiver<CollectorCmd<W>>,
    batch_tx: tmpsc::Sender<Vec<W>>,
    observers: Vec<BoxedDynObserver<W>>,
    start: CollectorTime,
    observe_timer: GridTimer,
    export_timer: GridTimer,
    poll_period: Duration,
    align_timestamps: bool,
}

impl<W: Send + 'static> Worker<W> {
    fn block(&mut self) {
        loop {
            let now = Instant::now();
            self.maybe_observe(now);
            self.maybe_export(now);

            let now = Instant::now();
            let deadline = self
                .observe_timer
                .deadline()
                .min(self.export_timer.deadline());
            let timeout = deadline.saturating_duration_since(now);

            let mut cmd = match self.cmd_rx.recv_timeout(timeout) {
                Ok(cmd) => Some(cmd),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            };

            while let Some(next) = cmd.take().or_else(|| self.cmd_rx.try_recv().ok()) {
                if !self.handle(next) {
                    return;
                }
            }
        }
    }

    fn maybe_observe(&mut self, now: Instant) {
        if !self.observe_timer.due(now) {
            return;
        }

        let ts = self.start.at(now).system;
        for observer in &mut self.observers {
            observer.observe(ts);
        }
        self.observe_timer.advance(Instant::now());
    }

    fn maybe_export(&mut self, now: Instant) {
        if !self.export_timer.due(now) {
            return;
        }
        let align = self.align_timestamps.then_some(self.poll_period);
        let mut batch = Vec::with_capacity(self.observers.len());

        for observer in &mut self.observers {
            match observer.export(align) {
                Ok(Some(metric)) => batch.push(metric),
                Ok(None) => {}
                Err(err) => error!("failed to export metric to wire format: {err}"),
            }
        }

        if !batch.is_empty() {
            match self.batch_tx.try_send(batch) {
                Ok(()) => {}
                Err(tmpsc::error::TrySendError::Full(_)) => {
                    warn!("batch channel full; discarding metrics batch");
                }
                Err(tmpsc::error::TrySendError::Closed(_)) => {
                    warn!("sender task is gone; discarding metrics batch");
                }
            }
        }

        self.export_timer.advance(Instant::now());
    }

    fn handle(&mut self, cmd: CollectorCmd<W>) -> bool {
        match cmd {
            CollectorCmd::Add(mut observer) => {
                // Reset a newly added observer to the next grid tick.
                let next_tick = self.observe_timer.deadline();
                observer.reset(self.start.at(next_tick).system);
                self.observers.push(observer);
                true
            }
            CollectorCmd::Shutdown => {
                self.maybe_export(Instant::now());
                false
            }
        }
    }
}

async fn process_send<W, S>(mut sender: S, mut batch_rx: tmpsc::Receiver<Vec<W>>)
where
    W: Send + 'static,
    S: Sender<W>,
{
    while let Some(batch) = batch_rx.recv().await {
        sender.send(batch).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::Counter;
    use crate::metric::tests::ID;
    use testresult::TestResult;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestWire {
        name: String,
    }

    impl<T, S> dto::IntoWire<TestWire> for dto::Series<T, S>
    where
        T: atomic::Measure,
        S: Clone,
    {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<TestWire>, Error> {
            if self.series.is_empty() {
                return Ok(None);
            }

            Ok(Some(TestWire {
                name: self.id.name.to_string(),
            }))
        }
    }

    #[tokio::test]
    async fn pushes_batches() -> TestResult {
        let (sender, mut rx) = tmpsc::unbounded_channel::<Vec<TestWire>>();

        let collector = Collector::new(
            sender,
            Config {
                poll_period: Duration::from_millis(10),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let counter = Counter::<u64>::new(ID);
        counter.add(3, &[]);
        let _ = collector.add(counter, Mode::Direct)?;

        let batch = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await?
            .expect("sender task stopped");

        assert_eq!(
            batch,
            vec![TestWire {
                name: ID.name.to_string()
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_duplicate_destructive() -> TestResult {
        let (sender, _rx) = tmpsc::unbounded_channel::<Vec<TestWire>>();

        let collector = Collector::new(
            sender,
            Config {
                poll_period: Duration::from_millis(10),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let first = Counter::<u64>::new(ID);
        let _ = collector.add(first, Mode::Destructive)?;

        let second = Counter::<u64>::new(ID);
        let err = collector
            .add(second, Mode::Destructive)
            .expect_err("expected duplicate destructive to be rejected");
        assert!(
            matches!(err, Error::DuplicateDestructive),
            "expected DuplicateDestructive, got {err:?}",
        );
        Ok(())
    }

    #[tokio::test]
    async fn gated_drops() -> TestResult {
        let (sender, mut rx) = tmpsc::unbounded_channel::<Vec<TestWire>>();
        let enabled = Arc::new(AtomicBool::new(true));
        let gated = Gated::with_enabled(sender, Arc::clone(&enabled));

        let collector = Collector::new(
            gated,
            Config {
                poll_period: Duration::from_millis(10),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let counter = Counter::<u64>::new(ID);
        counter.add(3, &[]);
        let _ = collector.add(counter, Mode::Direct)?;

        let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await?
            .expect("sender task stopped");

        // Disable, let an in-flight batch settle, then drain whatever already arrived.
        enabled.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(50)).await;
        while rx.try_recv().is_ok() {}

        // Nothing new is forwarded while disabled.
        let got = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(got.is_err(), "disabled gated sender forwarded a batch");

        // Re-enabling resumes forwarding without any further intervention.
        enabled.store(true, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await?
            .expect("sender task stopped after re-enable");

        Ok(())
    }

    #[tokio::test]
    async fn no_empty_exports() -> TestResult {
        let (sender, mut rx) = tmpsc::unbounded_channel::<Vec<TestWire>>();

        let _collector = Collector::new(
            sender,
            Config {
                poll_period: Duration::from_millis(10),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let got = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(got.is_err(), "empty collector dispatched a batch");
        Ok(())
    }
}
