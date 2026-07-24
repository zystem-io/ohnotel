#[cfg(feature = "periodic")]
pub mod periodic;
#[cfg(feature = "periodic")]
mod timer;

use crate::Error;
use crate::model::NameIdentity;
use crate::observe::{DynObserver, MetricSource, Mode};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type BoxedDynObserver<W> = Box<dyn DynObserver<W, Error> + Send>;

/// A collector that can take part in a [`Collection`].
///
/// [`Collection::add`] hands every collector in the tuple the source together
/// with the observation mode requested at registration. Push-based collectors
/// build an observer reporting in that mode; pull-based collectors (e.g.
/// Prometheus) keep a handle to the source itself and read its current state
/// on demand -- the mode only describes how push observers report, so they
/// ignore it.
pub trait AddSource<Src>: Send + Sync + 'static {
    fn add_source(&self, source: &Src, mode: Mode) -> Result<(), Error>;
}

pub trait CollectorTuple {
    const LEN: usize;
}

pub trait RegisterAll<Src> {
    fn register_all(&self, source: &Src, mode: Mode) -> Result<(), Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegistryKey {
    id: Arc<NameIdentity>,
    type_id: TypeId,
}

#[derive(Debug)]
struct RegistryEntry {
    mode: Mode,
    source: Arc<dyn Any + Send + Sync>,
}

type Registry = Mutex<HashMap<RegistryKey, RegistryEntry>>;

#[derive(Debug)]
pub struct Collection<C> {
    collectors: Arc<C>,
    registry: Arc<Registry>,
}

impl<C> Clone for Collection<C> {
    fn clone(&self) -> Self {
        Self {
            collectors: Arc::clone(&self.collectors),
            registry: Arc::clone(&self.registry),
        }
    }
}

impl<C> Collection<C> {
    pub fn new(collectors: C) -> Self {
        Self {
            collectors: Arc::new(collectors),
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn collectors(&self) -> &C {
        &self.collectors
    }

    /// Adds a new collector. If one has already been added before, the clone of the previous will
    /// be returned. Adding the same source with different mode will return an error.
    pub fn add<Src>(&self, source: Src, mode: Mode) -> Result<Src, Error>
    where
        C: CollectorTuple + RegisterAll<Src>,
        Src: MetricSource + Clone + Send + Sync + 'static,
    {
        if mode == Mode::Destructive && C::LEN != 1 {
            return Err(Error::DestructiveWithMultipleCollectors);
        }

        let key = RegistryKey {
            id: Arc::clone(source.id()),
            type_id: TypeId::of::<Src>(),
        };
        let mut registry = self.registry.lock().expect("registry poisoned");

        if let Some(entry) = registry.get(&key) {
            if entry.mode != mode {
                return Err(Error::ModeMismatch);
            }
            if let Some(existing) = entry.source.downcast_ref::<Src>() {
                return Ok(existing.clone());
            }
        }

        self.collectors.register_all(&source, mode)?;
        let _ = registry.insert(
            key,
            RegistryEntry {
                mode,
                source: Arc::new(source.clone()),
            },
        );
        Ok(source)
    }
}

macro_rules! impl_collector_tuple {
    ($($name:ident . $idx:tt),+ $(,)?) => {
        impl<$($name),+> CollectorTuple for ($($name,)+) {
            const LEN: usize = [$(stringify!($name)),+].len();
        }

        impl<Src, $($name),+> RegisterAll<Src> for ($($name,)+)
        where
            $($name: AddSource<Src>,)+
        {
            fn register_all(&self, source: &Src, mode: Mode) -> Result<(), Error> {
                $(self.$idx.add_source(source, mode)?;)+
                Ok(())
            }
        }
    };
}

impl_collector_tuple!(A.0);
impl_collector_tuple!(A.0, B.1);
impl_collector_tuple!(A.0, B.1, C.2);
impl_collector_tuple!(A.0, B.1, C.2, D.3);
impl_collector_tuple!(A.0, B.1, C.2, D.3, E.4);
impl_collector_tuple!(A.0, B.1, C.2, D.3, E.4, F.5);
impl_collector_tuple!(A.0, B.1, C.2, D.3, E.4, F.5, G.6);
impl_collector_tuple!(A.0, B.1, C.2, D.3, E.4, F.5, G.6, H.7);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic;
    use crate::atomic::histogram::Snapshot;
    use crate::dto;
    use crate::metric::tests::ID;
    use crate::metric::{Counter, Gauge, Histogram};
    use crate::model::KeyValue;
    use crate::observe::SyncObserver;
    use std::fmt::Write as _;
    use std::hash::BuildHasher;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    #[derive(Default, Debug, Clone, PartialEq, Eq)]
    struct UnsignedWire {
        total: u64,
    }

    #[derive(Default, Debug, Clone, PartialEq, Eq)]
    struct TextWire {
        text: String,
    }

    #[derive(Default, Debug, Clone, PartialEq, Eq)]
    struct HistogramWire {
        count: u64,
    }

    impl<S: Clone> dto::IntoWire<UnsignedWire> for dto::Series<u64, S> {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<UnsignedWire>, Error> {
            let mut total = 0u64;
            for snaps in self.series.values() {
                for snap in snaps {
                    total += snap.value;
                }
            }
            Ok(Some(UnsignedWire { total }))
        }
    }

    impl<S: Clone> dto::IntoWire<TextWire> for dto::Series<u64, S> {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<TextWire>, Error> {
            let mut text = String::new();
            for snaps in self.series.values() {
                for snap in snaps {
                    let _ = write!(text, "{}={} ", self.id.name, snap.value);
                }
            }
            Ok(Some(TextWire { text }))
        }
    }

    impl<S: Clone> dto::IntoWire<HistogramWire> for dto::Series<Snapshot<u64>, S> {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<HistogramWire>, Error> {
            let mut count = 0u64;
            for snaps in self.series.values() {
                for snap in snaps {
                    count += snap.value.count;
                }
            }
            Ok(Some(HistogramWire { count }))
        }
    }

    impl<S: Clone> dto::IntoWire<HistogramWire> for dto::Series<u64, S> {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<HistogramWire>, Error> {
            let count = self
                .series
                .values()
                .map(|s| u64::try_from(s.len()).expect("snapshot count fits in u64"))
                .sum();
            Ok(Some(HistogramWire { count }))
        }
    }

    impl<S: Clone> dto::IntoWire<UnsignedWire> for dto::Series<Snapshot<u64>, S> {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<UnsignedWire>, Error> {
            let mut total = 0u64;
            for snaps in self.series.values() {
                for snap in snaps {
                    total += snap.value.count;
                }
            }
            Ok(Some(UnsignedWire { total }))
        }
    }

    impl<S: Clone> dto::IntoWire<TextWire> for dto::Series<Snapshot<u64>, S> {
        type Error = Error;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<TextWire>, Error> {
            let mut text = String::new();
            for snaps in self.series.values() {
                for snap in snaps {
                    let _ = write!(text, "{}#{} ", self.id.name, snap.value.count);
                }
            }
            Ok(Some(TextWire { text }))
        }
    }

    struct InMemoryCollector<W: Send + 'static> {
        observers: Arc<Mutex<Vec<BoxedDynObserver<W>>>>,
    }

    impl<W: Send + 'static> InMemoryCollector<W> {
        fn new() -> Self {
            Self {
                observers: Arc::new(Mutex::new(vec![])),
            }
        }

        fn drain_export(&self, ts: SystemTime) -> Vec<W> {
            let mut observers = self.observers.lock().expect("poisoned");
            let mut out = vec![];
            for observer in observers.iter_mut() {
                observer.observe(ts);
                if let Some(batch) = observer.export(None).expect("export failed") {
                    out.push(batch);
                }
            }
            out
        }

        fn len(&self) -> usize {
            self.observers.lock().expect("poisoned").len()
        }
    }

    impl<W, Src, T, S, A> AddSource<Src> for InMemoryCollector<W>
    where
        W: Send + 'static,
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<W, Error = Error>,
    {
        fn add_source(&self, source: &Src, mode: Mode) -> Result<(), Error> {
            self.observers
                .lock()
                .expect("poisoned")
                .push(Box::new(SyncObserver::new(source, mode)?));
            Ok(())
        }
    }

    fn collection() -> Collection<(
        InMemoryCollector<UnsignedWire>,
        InMemoryCollector<TextWire>,
        InMemoryCollector<HistogramWire>,
    )> {
        Collection::new((
            InMemoryCollector::<UnsignedWire>::new(),
            InMemoryCollector::<TextWire>::new(),
            InMemoryCollector::<HistogramWire>::new(),
        ))
    }

    #[test]
    fn add_distributes_across_collectors() {
        let collectors = collection();

        let counter = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("counter registration failed");
        let gauge = collectors
            .add(Gauge::<u64>::new(ID), Mode::Direct)
            .expect("gauge registration failed");
        let histogram = collectors
            .add(
                Histogram::<u64>::new(ID, vec![1, 10, 100]).expect("histogram setup failed"),
                Mode::Direct,
            )
            .expect("histogram registration failed");

        counter.add(7, &[KeyValue::new("k", 1)]);
        gauge.add(3, &[]);
        histogram.add(50, &[]);
        histogram.add(2, &[]);

        let ts = SystemTime::now();
        let (unsigned, text, hist) = collectors.collectors();

        assert_eq!(unsigned.len(), 3, "unsigned sink missing observers");
        assert_eq!(text.len(), 3, "text sink missing observers");
        assert_eq!(hist.len(), 3, "hist sink missing observers");

        let unsigned_batches = unsigned.drain_export(ts);
        // counter=7, gauge=3, histogram count=2 (sum of bucket counts).
        let unsigned_total: u64 = unsigned_batches.iter().map(|w| w.total).sum();
        assert_eq!(
            unsigned_total,
            7 + 3 + 2,
            "unsigned wire totals incorrect: {unsigned_batches:?}",
        );

        let text_batches = text.drain_export(ts);
        assert_eq!(
            text_batches.len(),
            3,
            "text sink should produce 3 batches: {text_batches:?}",
        );

        let hist_batches = hist.drain_export(ts);
        let hist_total: u64 = hist_batches.iter().map(|w| w.count).sum();
        // counter contributes 1 datapoint, gauge 1, histogram 2 (50 and 2). The counter never
        // wrote to its no-attr bucket, so that pristine bucket must not be reported.
        assert_eq!(
            hist_total,
            1 + 1 + 2,
            "histogram wire saw {hist_total} events: {hist_batches:?}",
        );
    }

    #[test]
    fn add_rejects_destructive_with_multiple_collectors() {
        let collectors = collection();
        let err = collectors
            .add(Counter::<u64>::new(ID), Mode::Destructive)
            .expect_err("destructive registration must fail with >1 collectors");
        assert!(
            matches!(err, Error::DestructiveWithMultipleCollectors),
            "expected DestructiveWithMultipleCollectors, got {err:?}",
        );
    }

    #[test]
    fn add_allows_destructive_with_single_collector() {
        let collectors = Collection::new((InMemoryCollector::<UnsignedWire>::new(),));
        let _counter = collectors
            .add(Counter::<u64>::new(ID), Mode::Destructive)
            .expect("single-collector destructive should succeed");
        assert_eq!(collectors.collectors().0.len(), 1);
    }

    #[test]
    fn same_instance() {
        let collectors = collection();
        let c1 = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("adding counter failed");
        let c2 = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("adding counter failed");

        c1.add(1, &[]);
        c2.add(1, &[]);

        let res = c1.get(&[]).expect("get counter failed");
        assert_eq!(res, 2);
    }

    #[test]
    fn add_rejects_mode_mismatch() {
        let collectors = collection();
        let _counter = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("adding counter failed");
        let err = collectors
            .add(Counter::<u64>::new(ID), Mode::Delta)
            .expect_err("re-adding under a different mode must fail");
        assert!(
            matches!(err, Error::ModeMismatch),
            "expected ModeMismatch, got {err:?}",
        );
    }

    #[test]
    fn same_instance_different_types() {
        let collectors = collection();
        let g1 = collectors
            .add(Gauge::<u64>::new(ID), Mode::Direct)
            .expect("adding gauge failed");
        let c1 = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("adding counter failed");

        g1.set(42, &[]);
        c1.add(1, &[]);

        let res = g1.get(&[]).expect("get gauge failed");
        assert_eq!(res, 42);

        let res = c1.get(&[]).expect("get counter failed");
        assert_eq!(res, 1);
    }

    #[test]
    fn no_clash() {
        let collectors = collection();

        let gauge1 = collectors
            .add(Gauge::<u64>::new(ID), Mode::Direct)
            .expect("adding gauge failed");
        let counter1 = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("adding counter failed");

        gauge1.set(42, &[]);
        counter1.add(1, &[]);

        let gauge2 = collectors
            .add(Gauge::<u64>::new(ID), Mode::Direct)
            .expect("re-adding gauge failed");
        let counter2 = collectors
            .add(Counter::<u64>::new(ID), Mode::Direct)
            .expect("re-adding counter failed");

        gauge2.set(7, &[]);
        counter2.add(1, &[]);

        assert_eq!(
            gauge1.get(&[]).expect("get gauge failed"),
            7,
            "re-added gauge must alias the original instance",
        );
        assert_eq!(
            counter1.get(&[]).expect("get counter failed"),
            2,
            "re-added counter must alias the original instance",
        );

        let (unsigned, text, hist) = collectors.collectors();
        assert_eq!(unsigned.len(), 2, "unsigned sink should hold 2 observers");
        assert_eq!(text.len(), 2, "text sink should hold 2 observers");
        assert_eq!(hist.len(), 2, "hist sink should hold 2 observers");
    }
}
