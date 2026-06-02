use crate::atomic::{IsInitial as _, Measure, Record};
use crate::bucket_map::BucketMap;
use crate::model::{KeyValue, NameIdentity};
use crate::{Error, dto};
use hashbrown::hash_map::Entry;
use hashbrown::{DefaultHashBuilder, HashMap};

use crate::dto::IntoWire as _;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub type SeriesMap<V, S = DefaultHashBuilder> = HashMap<Arc<[KeyValue]>, Vec<dto::Snapshot<V>>, S>;
pub type LastValueMap<V, S = DefaultHashBuilder> = HashMap<Arc<[KeyValue]>, V, S>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Mode {
    /// Report values as is.
    Direct,

    /// Track previous values and send out deltas. Observer stores the previous values, and it does
    /// not affect the metric itself in any way.
    Delta,

    /// Reset the source buckets between observations. If a metric is being observed by multiple
    /// observers, they will be in a race against each other.
    Destructive,
}

pub trait MetricSource {
    type Measure: Measure;
    type Cell: Record<Self::Measure>;
    type Hasher: BuildHasher + Clone;

    fn id(&self) -> &Arc<NameIdentity>;

    fn buckets(&self) -> &Arc<BucketMap<Self::Measure, Self::Hasher, Self::Cell>>;

    fn kind(&self) -> dto::Kind;
}

pub trait DynObserver<W, E>: Send + Sync + 'static {
    /// Snapshot the observed source at `ts`. Time is passed externally because we want to produce
    /// the same time for all observed snapshots from a single collector.
    fn observe(&mut self, ts: SystemTime);

    /// Reset the observer to a fresh state at `start_time`. All side effects should be cleared by
    /// the implementation.
    ///
    /// This method will be called once by the collector at the moment of adding the observer to
    /// the list of observers.
    ///
    /// For example, if someone creates an observer, uses it, corrupts the state, then adds it to
    /// a collector, the collector should have a way to ensure that this observer can be trusted.
    fn reset(&mut self, start_time: SystemTime);

    fn export(&mut self, align: Option<Duration>) -> Result<Option<W>, E>;
}

// SyncObserver deliberately does NOT implement Clone because we don't want it to be mutated from
// multiple sites. Another way to ensure is using `&mut` for all methods. Consumers of observers
// should never take Arc's of them to avoid incorrect usage.
pub struct SyncObserver<T, S = DefaultHashBuilder, A = <T as Measure>::Type>
where
    T: Measure,
    S: BuildHasher + Clone,
    A: Record<T>,
{
    /// What was the time when this observer was initialized.
    start_time: SystemTime,

    /// A sequence number that gets incremented each time observe is called. Will be added to the
    /// snapshot.
    seq: u64,

    /// A metric identity passed with every snapshot series.
    id: Arc<NameIdentity>,

    /// A bucket map borrowed from a metric (Counter/Gauge/etc).
    buckets: Arc<BucketMap<T, S, A>>,

    /// A map of `[attrs]` -> `[snapshot1, snapshot2, ..., snapshotN]`.
    series: SeriesMap<A::Snapshot, S>,

    /// An optional map tracking last value for delta counters. We could use 'take' semantics on
    /// counters, i.e., reset them every collection and then not have a duplicate keymap here.
    /// However, then it will be intrusive, and the app code will not be able to have the normal
    /// total count and will have to deal with it, i.e., by creating another one tracking counter.
    /// IMO, it's more stupid than having a duplicate hashmap.
    last_values: Option<LastValueMap<A::Snapshot, S>>,
    observe_mode: Mode,
    kind: dto::Kind,
}

impl<T, S, A> fmt::Debug for SyncObserver<T, S, A>
where
    T: Measure,
    S: BuildHasher + Clone,
    A: Record<T>,
    A::Snapshot: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncObserver")
            .field("measurements", &self.series)
            .finish_non_exhaustive()
    }
}

impl<T, S, A> SyncObserver<T, S, A>
where
    T: Measure,
    S: BuildHasher + Clone,
    A: Record<T>,
{
    pub fn new<Src>(source: &Src, observe_mode: Mode) -> Result<Self, Error>
    where
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
    {
        let kind = source.kind();
        // Delta for gauge doesn't make sense
        if observe_mode == Mode::Delta && kind == dto::Kind::Gauge {
            return Err(Error::DeltaForGauge);
        }

        let hasher = source.buckets().hasher().clone();
        Ok(Self {
            id: Arc::clone(source.id()),
            buckets: Arc::clone(source.buckets()),
            series: HashMap::with_hasher(hasher.clone()),
            seq: 0,
            start_time: SystemTime::now(),
            last_values: match observe_mode {
                Mode::Direct | Mode::Destructive => None,
                Mode::Delta => Some(HashMap::with_hasher(hasher)),
            },
            observe_mode,
            kind,
        })
    }

    /// Reset the observer to a fresh state. See [`DynObserver::reset`].
    pub fn reset(&mut self, start_time: SystemTime) {
        self.start_time = start_time;
        self.seq = 0;
        self.series.clear();
        if let Some(last_values) = self.last_values.as_mut() {
            last_values.clear();
        }
    }

    fn observe_impl(&mut self, ts: SystemTime) {
        if self.observe_mode == Mode::Destructive {
            self.observe_destructive(ts);
            return;
        }

        self.buckets.visit_bucket(|bucket_entry| {
            let current = bucket_entry.bucket.current();

            let recorded = match self.last_values.as_mut() {
                Some(last_values) => match last_values.entry(Arc::clone(&bucket_entry.attrs)) {
                    Entry::Occupied(mut last_value_entry) => {
                        // It is tempting to drop here unchanged Gauge buckets, but it will break
                        // other observers.
                        if last_value_entry.get() == &current {
                            return true;
                        }
                        let prev = std::mem::replace(last_value_entry.get_mut(), current.clone());
                        if prev < current {
                            current - prev
                        } else {
                            current
                        }
                    }
                    Entry::Vacant(entry) => {
                        // `VacantEntry::insert` returns `&mut V` for chaining; we just need the
                        // side effect of populating the slot.
                        let _ = entry.insert(current.clone());
                        current
                    }
                },
                None => current,
            };

            // We intentionally do NOT skip `recorded.is_initial()` here. The only 'no change' case
            // is a zero delta, when prev equals to curr, which is already handled above.

            self.series
                .entry(Arc::clone(&bucket_entry.attrs))
                .or_default()
                .push(dto::Snapshot {
                    ts,
                    seq_id: self.seq,
                    value: recorded,
                });

            true
        });

        self.seq += 1;
    }

    fn observe_destructive(&mut self, ts: SystemTime) {
        let (buckets, no_attr_val) = self.buckets.take();
        for entry in buckets {
            let current = entry.bucket.current();
            if current.is_initial() {
                continue;
            }
            self.series
                .entry(Arc::clone(&entry.attrs))
                .or_default()
                .push(dto::Snapshot {
                    ts,
                    seq_id: self.seq,
                    value: current,
                });
        }

        if let Some(no_attr_val) = no_attr_val {
            self.series
                .entry(Arc::from([]))
                .or_default()
                .push(dto::Snapshot {
                    ts,
                    seq_id: self.seq,
                    value: no_attr_val,
                });
        }

        self.seq += 1;
    }

    fn take(&mut self) -> dto::Series<A::Snapshot, S> {
        let hasher = self.series.hasher().clone();
        dto::Series {
            start_time: self.start_time,
            id: Arc::clone(&self.id),
            series: std::mem::replace(&mut self.series, HashMap::with_hasher(hasher)),
            observe_mode: self.observe_mode,
            kind: self.kind,
        }
    }
}

impl<T, S, A, W, E> DynObserver<W, E> for SyncObserver<T, S, A>
where
    T: Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: Record<T>,
    dto::Series<A::Snapshot, S>: dto::IntoWire<W, Error = E>,
{
    fn observe(&mut self, ts: SystemTime) {
        self.observe_impl(ts);
    }

    fn reset(&mut self, start_time: SystemTime) {
        self.reset(start_time);
    }

    fn export(&mut self, align: Option<Duration>) -> Result<Option<W>, E> {
        self.take().into_wire(align)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic::histogram::Snapshot;
    use crate::metric::tests::ID;
    use crate::metric::{Counter, Gauge, Histogram};
    use crate::model::KeyValue;
    use std::convert::Infallible;

    #[derive(Default)]
    struct TestWire {
        u64_total: u64,
        i64_total: i64,
        histogram_count_total: u64,
    }

    impl TestWire {
        fn merge(&mut self, other: Option<&TestWire>) {
            let Some(other) = other else {
                return;
            };

            self.u64_total += other.u64_total;
            self.i64_total += other.i64_total;
            self.histogram_count_total += other.histogram_count_total;
        }
    }

    impl<S: Clone> dto::IntoWire<TestWire> for dto::Series<u64, S> {
        type Error = Infallible;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<TestWire>, Infallible> {
            let mut out = TestWire::default();
            for snaps in self.series.values() {
                for s in snaps {
                    out.u64_total += s.value;
                }
            }
            Ok(Some(out))
        }
    }

    impl<S: Clone> dto::IntoWire<TestWire> for dto::Series<i64, S> {
        type Error = Infallible;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<TestWire>, Infallible> {
            let mut out = TestWire::default();
            for snaps in self.series.values() {
                for s in snaps {
                    out.i64_total += s.value;
                }
            }
            Ok(Some(out))
        }
    }

    impl<S: Clone> dto::IntoWire<TestWire> for dto::Series<Snapshot<u64>, S> {
        type Error = Infallible;

        fn into_wire(self, _align: Option<Duration>) -> Result<Option<TestWire>, Infallible> {
            let mut out = TestWire::default();
            for snaps in self.series.values() {
                for s in snaps {
                    out.histogram_count_total += s.value.count;
                }
            }
            Ok(Some(out))
        }
    }

    fn attrs(pairs: &[(&'static str, i64)]) -> Vec<KeyValue> {
        pairs.iter().map(|(k, v)| KeyValue::new(*k, *v)).collect()
    }

    fn values<T: Clone>(snapshots: &[dto::Snapshot<T>]) -> Vec<T> {
        snapshots.iter().map(|m| m.value.clone()).collect()
    }

    #[test]
    fn counter_cumulative() {
        let c = Counter::<u64>::new(ID);
        let mut obs = SyncObserver::new(&c, Mode::Direct).expect("direct mode is supported");

        c.add(2, &[]);
        obs.observe_impl(SystemTime::now());
        c.add(3, &[]);
        obs.observe_impl(SystemTime::now());

        let empty = Arc::from([]);
        let m = obs.take();
        let series = m.series.get(&empty).expect("series");

        assert_eq!(series[0].seq_id, 0);
        assert_eq!(series[0].value, 2);

        assert_eq!(series[1].seq_id, 1);
        assert_eq!(series[1].value, 5);
    }

    #[test]
    fn observer_buckets() {
        let c = Counter::<u64>::new(ID);
        let a = attrs(&[("route", 1)]);
        let b = attrs(&[("route", 2)]);

        c.add(1, &a);
        c.add(2, &b);
        c.add(7, &[]);

        let mut obs = SyncObserver::new(&c, Mode::Direct).expect("direct mode is supported");
        obs.observe_impl(SystemTime::now());

        let m = obs.take();
        assert_eq!(m.series.len(), 3);

        let key_a = Arc::from(a.clone());
        let key_b = Arc::from(b.clone());
        let key_empty = Arc::from([]);

        assert_eq!(values(m.series.get(&key_a).expect("series a exists")), [1]);
        assert_eq!(values(m.series.get(&key_b).expect("series b exists")), [2]);
        assert_eq!(
            values(m.series.get(&key_empty).expect("empty series exists")),
            [7]
        );
    }

    #[test]
    fn gauge_current() {
        let g = Gauge::<i64>::new(ID);
        let mut obs = SyncObserver::new(&g, Mode::Direct).expect("direct mode is supported");

        g.add(10, &[]);
        obs.observe_impl(SystemTime::now());
        g.sub(3, &[]);
        obs.observe_impl(SystemTime::now());

        let key = Arc::from([]);
        let m = obs.take();

        assert_eq!(values(m.series.get(&key).expect("series exists")), [10, 7]);
    }

    #[test]
    fn counter_diffs() {
        let c = Counter::<u64>::new(ID);
        let mut obs = SyncObserver::new(&c, Mode::Delta).expect("delta mode is supported");

        c.add(2, &[]);
        obs.observe_impl(SystemTime::now());
        c.add(3, &[]);
        obs.observe_impl(SystemTime::now());
        c.add(10, &[]);
        obs.observe_impl(SystemTime::now());

        let empty = Arc::from([]);
        let m = obs.take();
        let series = m.series.get(&empty).expect("series");

        assert_eq!(series[0].value, 2);
        assert_eq!(series[1].value, 3);
        assert_eq!(series[2].value, 10);
    }

    #[test]
    fn no_delta_gauge() {
        let g = Gauge::<i64>::new(ID);
        assert!(matches!(
            SyncObserver::new(&g, Mode::Delta),
            Err(Error::DeltaForGauge),
        ));
    }

    #[test]
    fn dyn_sample() {
        let counter = Counter::<u64>::new(ID);
        let gauge = Gauge::<i64>::new(ID);
        let hist = Histogram::<u64>::new(ID, [1, 2, 3]).expect("valid boundaries");

        counter.add(7, &[]);
        gauge.add(-3, &[]);
        hist.add(2, &[]);
        hist.add(10, &[]);

        let mut registry: Vec<Box<dyn DynObserver<TestWire, Infallible>>> = vec![
            Box::new(SyncObserver::new(&counter, Mode::Direct).expect("direct mode is supported")),
            Box::new(SyncObserver::new(&gauge, Mode::Direct).expect("direct mode is supported")),
            Box::new(SyncObserver::new(&hist, Mode::Direct).expect("direct mode is supported")),
        ];

        let now = SystemTime::now();
        for obs in &mut registry {
            obs.observe(now);
        }

        let mut w = TestWire::default();
        for obs in &mut registry {
            match obs.export(None) {
                Ok(part) => w.merge(part.as_ref()),
                Err(never) => match never {},
            }
        }

        assert_eq!(w.u64_total, 7);
        assert_eq!(w.i64_total, -3);
        assert_eq!(w.histogram_count_total, 2);
    }

    #[test]
    fn histogram_delta_diffs() {
        let h = Histogram::<u64>::new(ID, [1, 2, 3]).expect("valid boundaries");
        let mut obs = SyncObserver::new(&h, Mode::Delta).expect("delta mode is supported");

        h.add(1, &[]);
        h.add(2, &[]);
        obs.observe_impl(SystemTime::now());

        h.add(3, &[]);
        h.add(10, &[]);
        obs.observe_impl(SystemTime::now());

        let empty = Arc::from([]);
        let m = obs.take();
        let series = m.series.get(&empty).expect("series");

        assert_eq!(series[0].value.count, 2);
        assert_eq!(series[0].value.sum, 3);
        assert_eq!(series[0].value.bucket_counts, [1, 1, 0, 0]);

        assert_eq!(series[1].value.count, 2);
        assert_eq!(series[1].value.sum, 13);
        assert_eq!(series[1].value.bucket_counts, [0, 0, 1, 1]);
    }

    #[test]
    fn destructive_seq_bump() {
        // Destructive mode must reset the source buckets between observations, and it should still
        // advance seq_id because we want alignment and for that we need correct seq_id.
        let c = Counter::<u64>::new(ID);
        let a = attrs(&[("route", 1)]);
        let mut obs =
            SyncObserver::new(&c, Mode::Destructive).expect("destructive mode is supported");

        c.add(2, &[]);
        c.add(4, &a);
        obs.observe_impl(SystemTime::now());

        c.add(3, &[]);
        c.add(1, &a);
        obs.observe_impl(SystemTime::now());

        let empty = Arc::from([]);
        let key_a = Arc::from(a.clone());
        let m = obs.take();

        let empty_series = m.series.get(&empty).expect("no-attr series");
        assert_eq!(values(empty_series), [2, 3]);
        assert_eq!(empty_series[0].seq_id, 0);
        assert_eq!(empty_series[1].seq_id, 1, "seq_id must advance");

        let a_series = m.series.get(&key_a).expect("attr series");
        assert_eq!(values(a_series), [4, 1], "attr bucket must be drained");
        assert_eq!(a_series[0].seq_id, 0);
        assert_eq!(a_series[1].seq_id, 1, "seq_id must advance");
    }

    #[test]
    fn empty() {
        let c = Counter::<u64>::new(ID);
        let mut obs = SyncObserver::new(&c, Mode::Direct).expect("direct mode is supported");

        c.add(1, &[]);
        obs.observe_impl(SystemTime::now());

        let taken = obs.take();
        assert_eq!(taken.series.len(), 1);

        let m = obs.take();
        assert!(m.series.is_empty());
    }

    #[test]
    fn decr_count_on_evict() {
        let g = Gauge::<i64>::new(ID);
        let a = attrs(&[("route", 1)]);

        g.add(7, &a);
        assert_eq!(g.inner.len(), 1);

        g.inner.visit_bucket(|_| false);

        assert_eq!(
            g.inner.len(),
            0,
            "count must be decremented when visit_bucket evicts entries"
        );
    }
}
