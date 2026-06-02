use crate::bucket_map::BucketMap;
use crate::model::{KeyValue, NameIdentity};
use crate::observe::MetricSource;
use crate::{Error, atomic, dto};
use hashbrown::DefaultHashBuilder;
use std::hash::BuildHasher;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Gauge<T, S = DefaultHashBuilder>
where
    T: atomic::Measure,
    S: BuildHasher + Clone,
{
    pub(crate) inner: Arc<BucketMap<T, S>>,
    pub(crate) id: Arc<NameIdentity>,
}

impl<T> Gauge<T, DefaultHashBuilder>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
{
    pub fn new(id: NameIdentity) -> Self {
        Self::with_hasher(id, DefaultHashBuilder::default())
    }
}

impl<T, S> Gauge<T, S>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
    S: BuildHasher + Clone,
{
    pub fn with_hasher(id: NameIdentity, hasher: S) -> Self {
        Self {
            inner: Arc::new(BucketMap::with_hasher(hasher)),
            id: Arc::new(id),
        }
    }

    /// Wraps on overflow rather than panicking or saturating; past `MAX` it wraps back
    /// to `MIN` (for `Gauge<u64>`, `MAX + 1 == 0`). Use `Gauge<i64>` if the gauge can go negative.
    #[inline(always)]
    pub fn add(&self, value: T, attrs: &[KeyValue]) {
        self.inner.add(value, attrs);
    }

    /// Wraps on underflow rather than panicking or saturating; below `MIN` it wraps back
    /// to `MAX` (for `Gauge<u64>`, `0 - 1 == MAX`). Use `Gauge<i64>` if the gauge can go negative.
    #[inline(always)]
    pub fn sub(&self, value: T, attrs: &[KeyValue]) {
        self.inner.sub(value, attrs);
    }

    #[inline(always)]
    pub fn get(&self, attrs: &[KeyValue]) -> Option<T> {
        self.inner.get(attrs)
    }

    #[inline(always)]
    pub fn set(&self, value: T, attrs: &[KeyValue]) {
        self.inner.set(value, attrs);
    }

    #[inline(always)]
    #[must_use = "This is the only remaining copy of the data."]
    pub fn reset(&self, attrs: &[KeyValue]) -> T {
        self.inner.reset(attrs)
    }
}

impl<T, S> MetricSource for Gauge<T, S>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
    S: BuildHasher + Clone,
{
    type Measure = T;
    type Cell = T::Type;
    type Hasher = S;

    #[inline]
    fn id(&self) -> &Arc<NameIdentity> {
        &self.id
    }

    #[inline]
    fn buckets(&self) -> &Arc<BucketMap<T, S>> {
        &self.inner
    }

    fn kind(&self) -> dto::Kind {
        dto::Kind::Gauge
    }
}

#[derive(Debug, Clone)]
pub struct Counter<T, S = DefaultHashBuilder>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
    S: BuildHasher + Clone,
{
    pub(crate) inner: Arc<BucketMap<T, S>>,
    pub(crate) id: Arc<NameIdentity>,
}

impl<T> Counter<T, DefaultHashBuilder>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
{
    pub fn new(id: NameIdentity) -> Self {
        Self::with_hasher(id, DefaultHashBuilder::default())
    }
}

impl<T, S> Counter<T, S>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
    S: BuildHasher + Clone,
{
    pub fn with_hasher(id: NameIdentity, hasher: S) -> Self {
        Self {
            inner: Arc::new(BucketMap::with_hasher(hasher)),
            id: Arc::new(id),
        }
    }
    #[inline(always)]
    pub fn add(&self, value: T, attrs: &[KeyValue]) {
        self.inner.add(value, attrs);
    }

    #[inline(always)]
    pub fn get(&self, attrs: &[KeyValue]) -> Option<T> {
        self.inner.get(attrs)
    }

    #[inline(always)]
    #[must_use = "This is the only remaining copy of the data."]
    pub fn reset(&self, attrs: &[KeyValue]) -> T {
        self.inner.reset(attrs)
    }
}

impl<T, S> MetricSource for Counter<T, S>
where
    T: atomic::Measure,
    T::Type: atomic::Record<T>,
    S: BuildHasher + Clone,
{
    type Measure = T;
    type Cell = T::Type;
    type Hasher = S;

    #[inline]
    fn id(&self) -> &Arc<NameIdentity> {
        &self.id
    }

    #[inline]
    fn buckets(&self) -> &Arc<BucketMap<T, S>> {
        &self.inner
    }

    fn kind(&self) -> dto::Kind {
        dto::Kind::Counter
    }
}

#[derive(Debug, Clone)]
pub struct Histogram<T, S = DefaultHashBuilder>
where
    T: atomic::Measure + Send + Sync + 'static,
    T::Type: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub(crate) inner: Arc<BucketMap<T, S, atomic::histogram::Bucket<T>>>,
    pub(crate) id: Arc<NameIdentity>,
}

impl<T> Histogram<T>
where
    T: atomic::Measure + Send + Sync + 'static,
    T::Type: Send + Sync + 'static,
{
    #[inline]
    pub fn new(id: NameIdentity, boundaries: impl Into<Vec<T>>) -> Result<Self, Error> {
        Self::with_hasher(id, boundaries, DefaultHashBuilder::default())
    }
}

impl<T, S> Histogram<T, S>
where
    T: atomic::Measure + Send + Sync + 'static,
    T::Type: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    #[inline]
    pub fn with_hasher(
        id: NameIdentity,
        boundaries: impl Into<Vec<T>>,
        hasher: S,
    ) -> Result<Self, Error> {
        let boundaries = atomic::histogram::Bucket::<T>::checked_boundaries(boundaries)?;

        Ok(Self {
            inner: Arc::new(BucketMap::with_storage(hasher, move || {
                atomic::histogram::Bucket::from_checked_boundaries(Arc::clone(&boundaries))
            })),
            id: Arc::new(id),
        })
    }

    #[inline(always)]
    pub fn add(&self, value: T, attrs: &[KeyValue]) {
        self.inner.add(value, attrs);
    }

    #[inline(always)]
    pub fn snapshot(&self, attrs: &[KeyValue]) -> Option<atomic::histogram::Snapshot<T>> {
        self.inner
            .get_bucket(attrs)
            .ok()
            .map(|bucket| bucket.snapshot())
    }

    #[inline(always)]
    #[must_use = "This is the only remaining copy of the data."]
    pub fn take(&self, attrs: &[KeyValue]) -> Option<atomic::histogram::Snapshot<T>> {
        self.inner
            .get_bucket(attrs)
            .ok()
            .map(|bucket| bucket.take())
    }

    #[inline(always)]
    pub fn clear(&self) {
        self.inner.clear();
    }
}

impl<T, S> MetricSource for Histogram<T, S>
where
    T: atomic::Measure + Send + Sync + 'static,
    T::Type: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    type Measure = T;
    type Cell = atomic::histogram::Bucket<T>;
    type Hasher = S;

    #[inline]
    fn id(&self) -> &Arc<NameIdentity> {
        &self.id
    }

    #[inline]
    fn buckets(&self) -> &Arc<BucketMap<T, S, atomic::histogram::Bucket<T>>> {
        &self.inner
    }

    fn kind(&self) -> dto::Kind {
        dto::Kind::Histogram
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::model::{KeyValue, Str};
    use std::borrow::Cow;

    pub const ID: NameIdentity = NameIdentity {
        name: Str::Cow(Cow::Borrowed("name")),
        description: Str::Cow(Cow::Borrowed("description")),
        unit: Str::Cow(Cow::Borrowed("unit")),
    };

    #[test]
    fn check_compiles() {
        let c = Counter::<u64>::new(ID);
        let h = {
            let c = c.clone();
            std::thread::spawn(move || {
                c.add(1, &[]);
                let c2 = c.clone();
                std::thread::spawn(move || {
                    c2.add(1, &[KeyValue::no_val("qwe")]);
                })
            })
        };

        h.join()
            .expect("outer thread panicked")
            .join()
            .expect("inner thread panicked");

        assert_eq!(c.get(&[KeyValue::no_val("qwe")]), Some(1));
        assert_eq!(c.get(&[]), Some(1));
    }

    #[test]
    fn gauge_set_no_attr() {
        let g = Gauge::<i64>::new(ID);

        assert_eq!(g.get(&[]), Some(0));
        g.set(42, &[]);
        assert_eq!(g.get(&[]), Some(42));

        g.set(7, &[]);
        assert_eq!(g.get(&[]), Some(7));

        g.add(100, &[]);
        assert_eq!(g.get(&[]), Some(107));
        g.set(-3, &[]);
        assert_eq!(g.get(&[]), Some(-3));
        g.sub(2, &[]);
        assert_eq!(g.get(&[]), Some(-5));
    }

    #[test]
    fn gauge_set_attr() {
        let g = Gauge::<i64>::new(ID);
        let a = [KeyValue::new("route", "/a")];
        let b = [KeyValue::new("route", "/b")];

        g.set(10, &a);
        g.set(20, &b);

        assert_eq!(g.get(&a), Some(10));
        assert_eq!(g.get(&b), Some(20));
        assert_eq!(g.get(&[]), Some(0));

        g.set(11, &a);
        assert_eq!(g.get(&a), Some(11));
        assert_eq!(g.get(&b), Some(20));
    }

    #[test]
    fn gauge_set_attr_shuffle() {
        let g = Gauge::<i64>::new(ID);
        let sorted = [KeyValue::new("a", 1), KeyValue::new("b", 2)];
        let shuffled = [KeyValue::new("b", 2), KeyValue::new("a", 1)];

        g.set(5, &sorted);
        assert_eq!(g.get(&sorted), Some(5));

        g.set(9, &shuffled);
        assert_eq!(g.get(&sorted), Some(9));
    }

    #[test]
    fn histogram_wrapper() {
        let h = Histogram::<u64>::new(ID, [1, 2, 3]).expect("valid boundaries");

        h.add(0, &[]);
        h.add(1, &[]);
        h.add(2, &[]);
        h.add(3, &[]);
        h.add(10, &[]);

        let snap = h.snapshot(&[]).expect("empty-attr snapshot exists");

        assert_eq!(snap.count, 5);
        assert_eq!(snap.sum, 16);
        assert_eq!(snap.bucket_counts, [2, 1, 1, 1]);

        let attrs = [KeyValue::new("route", "/api")];
        h.add(10, &attrs);

        let snap = h.snapshot(&attrs).expect("attr snapshot exists");

        assert_eq!(snap.count, 1);
        assert_eq!(snap.sum, 10);
        assert_eq!(snap.bucket_counts, [0, 0, 0, 1]);

        let old = h.take(&attrs).expect("attr snapshot exists");
        assert_eq!(old.count, 1);
        assert_eq!(old.sum, 10);

        let after = h.snapshot(&attrs).expect("attr snapshot exists");
        assert_eq!(after.count, 0);
        assert_eq!(after.sum, 0);
        assert_eq!(after.bucket_counts, [0, 0, 0, 0]);
    }
}
