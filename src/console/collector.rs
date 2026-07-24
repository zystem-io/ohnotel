use crate::collect;
use crate::collect::periodic;
use crate::console::dto::Metric;
use crate::console::sender;
use crate::observe::{MetricSource, Mode, SyncObserver};
use crate::{Error, atomic, dto};
use std::hash::BuildHasher;

pub use periodic::Config;

type BoxedConsoleObserver = collect::BoxedDynObserver<Metric>;

#[derive(Clone, Debug)]
pub struct Collector {
    inner: periodic::Collector<Metric>,
}

impl Collector {
    /// Builds a console collector that writes metric batches to standard output.
    pub fn new(config: Config) -> Result<Self, Error> {
        Self::with_sender(sender::Stdout, config)
    }

    /// Builds a console collector with a custom sender. This is primarily useful for tests or for
    /// redirecting console-formatted metrics to another terminal-like sink.
    pub fn with_sender<S>(sender: S, config: Config) -> Result<Self, Error>
    where
        S: periodic::Sender<Metric>,
    {
        Ok(Self {
            inner: periodic::Collector::new(sender, config)?,
        })
    }

    pub fn add<Src, T, S, A>(&self, source: Src, mode: Mode) -> Result<Src, Error>
    where
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<Metric, Error = Error>,
    {
        self.inner.add(source, mode)
    }

    pub fn add_observer<T, S, A>(&self, observer: SyncObserver<T, S, A>)
    where
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<Metric, Error = Error>,
    {
        self.inner.add_observer(observer);
    }

    pub fn add_boxed_observer(&self, observer: BoxedConsoleObserver) {
        self.inner.add_boxed_observer(observer);
    }
}

impl<Src, T, S, A> collect::AddSource<Src> for Collector
where
    Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
    T: atomic::Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: atomic::Record<T>,
    dto::Series<A::Snapshot, S>: dto::IntoWire<Metric, Error = Error>,
{
    fn add_source(&self, source: &Src, mode: Mode) -> Result<(), Error> {
        collect::AddSource::add_source(&self.inner, source, mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::tests::ID;
    use crate::metric::{Counter, Gauge, Histogram};
    use crate::model::{KeyValue, NameIdentity, Str};
    use ordered_float::OrderedFloat;
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::time::Duration;
    use testresult::TestResult;
    use tokio::sync::mpsc as tmpsc;

    #[tokio::test]
    async fn pushes_console_batches() -> TestResult {
        let (sender, mut rx) = tmpsc::unbounded_channel::<Vec<Metric>>();

        let collector = Collector::with_sender(
            sender,
            Config {
                poll_period: Duration::from_millis(20),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let counter = Counter::<u64>::new(ID);
        counter.add(5, &[]);
        let _ = collector.add(counter, Mode::Direct)?;

        let batch = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await?
            .expect("sender task stopped");

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id.as_ref(), &ID);
        assert_eq!(batch[0].data_points.len(), 1);
        assert_eq!(
            batch[0].data_points[0].value,
            crate::console::dto::DataPointValue::Number(OrderedFloat(5.0))
        );
        Ok(())
    }

    fn id(name: &'static str) -> NameIdentity {
        NameIdentity {
            name: Str::Cow(Cow::Borrowed(name)),
            description: Str::Cow(Cow::Borrowed("")),
            unit: Str::Cow(Cow::Borrowed("")),
        }
    }

    /// Registers a counter / gauge / histogram on `collector`, spawns a
    /// worker thread that updates them at ~50Hz across a few attribute
    /// combinations, lets `runtime` elapse, then joins the worker.
    async fn run_demo_workload(collector: &Collector, runtime: Duration) -> TestResult {
        let counter = Counter::<u64>::new(id("demo.requests"));
        let gauge = Gauge::<i64>::new(id("demo.queue_depth"));
        let histogram = Histogram::<f64>::new(
            id("demo.latency_ms"),
            [1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0],
        )?;

        let counter = collector.add(counter, Mode::Delta)?;
        let gauge = collector.add(gauge, Mode::Direct)?;
        let histogram = collector.add(histogram, Mode::Delta)?;

        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let routes = ["/a", "/b", "/c"];
                let mut tick: usize = 0;
                while !stop.load(AtomicOrdering::Relaxed) {
                    let route = routes[tick % routes.len()];
                    let attrs = [
                        KeyValue::new("route", route),
                        KeyValue::new("status", 200i32),
                    ];

                    counter.add(1, &attrs);
                    gauge.set(i64::try_from(tick % 17).unwrap_or(0), &attrs);
                    let latency = f64::from(u32::try_from((tick * 7) % 300).unwrap_or(0)) + 0.5;
                    histogram.add(latency, &attrs);

                    tick = tick.wrapping_add(1);
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };

        tokio::time::sleep(runtime).await;
        stop.store(true, AtomicOrdering::Relaxed);
        worker.join().expect("worker thread panicked");
        Ok(())
    }

    /// ```bash
    /// cargo test --all-features -p ohnotel stdout_demo -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "prints to stdout"]
    async fn stdout_demo() -> TestResult {
        let collector = Collector::with_sender(
            sender::Stdout,
            Config {
                poll_period: Duration::from_millis(500),
                export_period: Duration::from_millis(1000),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;
        run_demo_workload(&collector, Duration::from_millis(5000)).await
    }
}
