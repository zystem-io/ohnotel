#[cfg(feature = "periodic")]
pub mod periodic;
#[cfg(feature = "periodic")]
mod timer;

use crate::model::NameIdentity;
use crate::observe::{DynObserver, MetricSource, Mode, SyncObserver};
use crate::{Error, atomic, dto};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};

pub type BoxedDynObserver<W> = Box<dyn DynObserver<W, Error> + Send>;

pub trait Collector: Send + Sync + 'static {
    type Wire: Send + 'static;

    fn register(&self, observer: BoxedDynObserver<Self::Wire>);
}

pub trait ObserverFactory<C: Collector> {
    fn build(&self, collector: &C) -> Result<BoxedDynObserver<C::Wire>, Error>;
}

#[derive(Debug)]
pub struct SyncObserverFactory<'a, Src> {
    source: &'a Src,
    mode: Mode,
}

impl<'a, Src> SyncObserverFactory<'a, Src> {
    pub const fn new(source: &'a Src, mode: Mode) -> Self {
        Self { source, mode }
    }
}

impl<C, Src, T, S, A> ObserverFactory<C> for SyncObserverFactory<'_, Src>
where
    C: Collector,
    Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
    T: atomic::Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: atomic::Record<T>,
    dto::Series<A::Snapshot, S>: dto::IntoWire<C::Wire, Error = Error>,
{
    fn build(&self, _collector: &C) -> Result<BoxedDynObserver<C::Wire>, Error> {
        Ok(Box::new(SyncObserver::new(self.source, self.mode)?))
    }
}

pub trait CollectorTuple {
    const LEN: usize;
}

pub trait RegisterAll<F> {
    fn register_all(&self, factory: &F) -> Result<(), Error>;
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

    pub fn register<F>(&self, factory: &F) -> Result<(), Error>
    where
        C: RegisterAll<F>,
    {
        self.collectors.register_all(factory)
    }

    /// Adds a new collector. If one has already been added before, the clone of the previous will
    /// be returned. Adding the same source with different mode will return an error.
    pub fn add<Src>(&self, source: Src, mode: Mode) -> Result<Src, Error>
    where
        C: CollectorTuple,
        Src: MetricSource + Clone + Send + Sync + 'static,
        for<'a> C: RegisterAll<SyncObserverFactory<'a, Src>>,
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

        self.register(&SyncObserverFactory::new(&source, mode))?;
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
        impl<$($name),+> CollectorTuple for ($($name,)+)
        where
            $($name: Collector,)+
        {
            const LEN: usize = [$(stringify!($name)),+].len();
        }

        impl<Fact, $($name),+> RegisterAll<Fact> for ($($name,)+)
        where
            $($name: Collector,)+
            $(Fact: ObserverFactory<$name>,)+
        {
            fn register_all(&self, factory: &Fact) -> Result<(), Error> {
                $(
                    let observer = <Fact as ObserverFactory<$name>>::build(
                        factory,
                        &self.$idx,
                    )?;
                    self.$idx.register(observer);
                )+
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
    use crate::atomic::histogram::Snapshot;
    use crate::metric::tests::ID;
    use crate::metric::{Counter, Gauge, Histogram};
    use crate::model::KeyValue;
    use std::fmt::Write as _;
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

    impl<W: Send + 'static> Collector for InMemoryCollector<W> {
        type Wire = W;

        fn register(&self, observer: BoxedDynObserver<W>) {
            self.observers.lock().expect("poisoned").push(observer);
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

    struct ConstObserver<W> {
        wire: W,
    }

    impl<W: Clone + Send + Sync + 'static> DynObserver<W, Error> for ConstObserver<W> {
        fn observe(&mut self, _ts: SystemTime) {}
        fn reset(&mut self, _start_time: SystemTime) {}
        fn export(&mut self, _align: Option<Duration>) -> Result<Option<W>, Error> {
            Ok(Some(self.wire.clone()))
        }
    }

    struct ConstFactory;

    impl ObserverFactory<InMemoryCollector<UnsignedWire>> for ConstFactory {
        fn build(
            &self,
            _collector: &InMemoryCollector<UnsignedWire>,
        ) -> Result<BoxedDynObserver<UnsignedWire>, Error> {
            Ok(Box::new(ConstObserver {
                wire: UnsignedWire { total: 42 },
            }))
        }
    }

    impl ObserverFactory<InMemoryCollector<TextWire>> for ConstFactory {
        fn build(
            &self,
            _collector: &InMemoryCollector<TextWire>,
        ) -> Result<BoxedDynObserver<TextWire>, Error> {
            Ok(Box::new(ConstObserver {
                wire: TextWire {
                    text: "constant".to_owned(),
                },
            }))
        }
    }

    impl ObserverFactory<InMemoryCollector<HistogramWire>> for ConstFactory {
        fn build(
            &self,
            _collector: &InMemoryCollector<HistogramWire>,
        ) -> Result<BoxedDynObserver<HistogramWire>, Error> {
            Ok(Box::new(ConstObserver {
                wire: HistogramWire { count: 7 },
            }))
        }
    }

    #[test]
    fn register_custom_factory_per_wire() {
        let collectors = collection();
        collectors
            .register(&ConstFactory)
            .expect("custom factory registration failed");

        let ts = SystemTime::now();
        let (unsigned, text, hist) = collectors.collectors();

        assert_eq!(unsigned.drain_export(ts), vec![UnsignedWire { total: 42 }]);
        assert_eq!(
            text.drain_export(ts),
            vec![TextWire {
                text: "constant".to_owned(),
            }],
        );
        assert_eq!(hist.drain_export(ts), vec![HistogramWire { count: 7 }]);
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
