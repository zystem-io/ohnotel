use crate::Error;
use crate::atomic;
use crate::atomic::Scalar as _;
use crate::lock::SeqLock;
use arc_swap::ArcSwap;
use std::cmp::Ordering as CmpOrdering;
use std::fmt::Debug;
use std::ops::Sub;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Point-in-time view of a [`Bucket`].
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot<T> {
    pub boundaries: Arc<[T]>,
    pub bucket_counts: Vec<u64>,
    pub count: u64,
    pub sum: T,
    pub min: Option<T>,
    pub max: Option<T>,
}

impl<T> PartialOrd for Snapshot<T>
where
    T: PartialEq,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        self.count.partial_cmp(&other.count)
    }
}

impl<T> atomic::IsInitial for Snapshot<T> {
    #[inline]
    fn is_initial(&self) -> bool {
        self.count == 0
    }
}

/// Per-bucket delta between two snapshots for converting cumulative readings into rate.
///
/// During subtraction, counts and `sum` saturate at zero.
///
/// `min`/`max` are unrecoverable across a delta and are always `None`.
impl<T> Sub for Snapshot<T>
where
    T: Sub<Output = T>,
{
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        debug_assert_eq!(
            self.bucket_counts.len(),
            rhs.bucket_counts.len(),
            "histogram snapshots must share boundaries to be subtracted"
        );

        let bucket_counts = self
            .bucket_counts
            .iter()
            .zip(rhs.bucket_counts.iter())
            .map(|(a, b)| a.saturating_sub(*b))
            .collect();

        Self {
            boundaries: self.boundaries,
            bucket_counts,
            count: self.count.saturating_sub(rhs.count),
            sum: self.sum - rhs.sum,
            // Since we don't want to break histogram user-side code by take/reset, min/max,
            // I think, can't be meaningfully implemented, so it has to be set to None.
            min: None,
            max: None,
        }
    }
}

struct State<T>
where
    T: atomic::Measure,
{
    bucket_counts: Box<[AtomicU64]>,
    sum: T::Type,
    min: T::Type,
    max: T::Type,
    count: AtomicU64,
}

impl<T> State<T>
where
    T: atomic::Measure,
{
    fn new(bucket_count: usize) -> Self {
        Self {
            bucket_counts: (0..bucket_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            sum: T::Type::from(T::default()),
            min: T::Type::from(T::min_identity()),
            max: T::Type::from(T::max_identity()),
            count: AtomicU64::new(0),
        }
    }

    fn snapshot(&self, boundaries: &Arc<[T]>) -> Snapshot<T> {
        let count = self.count.load(Ordering::Relaxed);

        let bucket_counts: Vec<u64> = self
            .bucket_counts
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();

        let sum = self.sum.get();

        let (min, max) = if count == 0 {
            (None, None)
        } else {
            (Some(self.min.get()), Some(self.max.get()))
        };

        Snapshot {
            boundaries: Arc::clone(boundaries),
            bucket_counts,
            count,
            sum,
            min,
            max,
        }
    }
}

pub struct Bucket<T>
where
    T: atomic::Measure,
{
    boundaries: Arc<[T]>,
    state: ArcSwap<SeqLock<State<T>>>,
}

impl<T> Debug for Bucket<T>
where
    T: atomic::Measure,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();

        f.debug_struct("AtomicHistogram")
            .field("boundaries", &snapshot.boundaries)
            .field("bucket_counts", &snapshot.bucket_counts)
            .field("count", &snapshot.count)
            .field("sum", &snapshot.sum)
            .field("min", &snapshot.min)
            .field("max", &snapshot.max)
            .finish()
    }
}

impl<T> Bucket<T>
where
    T: atomic::Measure,
{
    /// Builds a histogram with the given bucket boundaries.
    ///
    /// Boundaries must be in the strictly increasing order.
    ///
    /// ```
    /// # use ohnotel::atomic::histogram::Bucket;
    /// # use ohnotel::atomic::Record;
    /// let h = Bucket::new([1u64, 2, 3]).unwrap();
    /// h.add(2); // bucket 1: (1, 2]
    /// h.add(9); // overflow bucket 3
    /// assert!(Bucket::new([3u64, 1]).is_err());
    /// ```
    pub fn new(boundaries: impl Into<Vec<T>>) -> Result<Self, Error> {
        Self::checked_boundaries(boundaries).map(Self::from_checked_boundaries)
    }

    pub(crate) fn checked_boundaries(boundaries: impl Into<Vec<T>>) -> Result<Arc<[T]>, Error> {
        let boundaries = boundaries.into();

        if boundaries.is_empty()
            || !boundaries
                .windows(2)
                .all(|w| matches!(w[0].partial_cmp(&w[1]), Some(std::cmp::Ordering::Less)))
        {
            return Err(Error::InvalidHistogramBoundaries);
        }

        Ok(boundaries.into())
    }

    pub(crate) fn from_checked_boundaries(boundaries: Arc<[T]>) -> Self {
        let bucket_count = boundaries.len() + 1;

        Self {
            boundaries,
            state: ArcSwap::from_pointee(SeqLock::new(State::<T>::new(bucket_count))),
        }
    }

    #[inline]
    fn bucket_index(&self, value: &T) -> usize {
        self.boundaries.partition_point(|boundary| boundary < value)
    }

    /// Returns a consistent snapshot of the current cumulative state.
    ///
    /// ```
    /// # use ohnotel::atomic::histogram::Bucket;
    /// # use ohnotel::atomic::Record as _;
    /// let h = Bucket::new([10u64, 20]).unwrap();
    /// h.add(5);
    /// h.add(25);
    /// let s = h.snapshot();
    /// assert_eq!(s.count, 2);
    /// assert_eq!(s.bucket_counts, [1, 0, 1]);
    /// ```
    pub fn snapshot(&self) -> Snapshot<T> {
        self.state.load().read(|s| s.snapshot(&self.boundaries))
    }

    /// Returns an atomic snapshot consistent at some point in time. Resets the state afterward.
    ///
    /// Updates from writers that loaded the old state *before* the swap will likely be lost.
    ///
    /// ```
    /// # use ohnotel::atomic::histogram::Bucket;
    /// # use ohnotel::atomic::Record as _;
    /// let h = Bucket::new([10u64, 20]).unwrap();
    /// h.add(5);
    /// let prev = h.take();
    /// assert_eq!(prev.count, 1);
    /// assert_eq!(h.snapshot().count, 0);
    /// ```
    pub fn take(&self) -> Snapshot<T> {
        let bucket_count = self.boundaries.len() + 1;
        let old = self
            .state
            .swap(Arc::new(SeqLock::new(State::<T>::new(bucket_count))));
        old.read(|s| s.snapshot(&self.boundaries))
    }
}

impl<T> atomic::Record<T> for Bucket<T>
where
    T: atomic::Measure,
{
    type Snapshot = Snapshot<T>;

    /// Records `value`. Counters and `sum` wrap on overflow; use `i64` for negative sums.
    #[inline]
    fn add(&self, value: T) {
        // NaN != NaN
        if value.partial_cmp(&value).is_none() {
            return;
        }
        let index = self.bucket_index(&value);
        let state = self.state.load();

        state.write(|s| {
            s.sum.add(value.clone());
            s.min.fetch_min(value.clone());
            s.max.fetch_max(value);
            let _ = s.bucket_counts[index].fetch_add(1, Ordering::Relaxed);
            let _ = s.count.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Undoes a matching `add`. Counters and `sum` wrap on underflow, so
    /// subtracting an unrecorded value corrupts the snapshot. Use `i64` for negative sums.
    #[inline]
    fn sub(&self, value: T) {
        if value.partial_cmp(&value).is_none() {
            return;
        }

        let index = self.bucket_index(&value);
        let state = self.state.load();

        state.write(|s| {
            s.sum.sub(value);
            let _ = s.bucket_counts[index].fetch_sub(1, Ordering::Relaxed);
            let _ = s.count.fetch_sub(1, Ordering::Relaxed);
        });
    }

    #[inline]
    fn clear(&self) {
        let _ = self.take();
    }

    #[inline]
    fn current(&self) -> Snapshot<T> {
        self.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic::Record as _;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    #[test]
    fn bucket() {
        let a = Bucket::new([1u64, 2, 3]).expect("valid boundaries");
        a.add(0u64); // bucket 0 (x <= 1)
        a.add(1u64); // bucket 0 (x <= 1)
        a.add(2u64); // bucket 1 (1 < x <= 2)
        a.add(3u64); // bucket 2 (2 < x <= 3)
        a.add(10u64); // bucket 3 (x > 3, overflow)

        let snap = a.snapshot();
        assert_eq!(snap.count, 5);
        assert_eq!(snap.bucket_counts, [2, 1, 1, 1]);
        assert_eq!(snap.sum, 16);
        assert_eq!(snap.min, Some(0));
        assert_eq!(snap.max, Some(10));
    }

    #[test]
    fn reset() {
        let h = Bucket::new([10u64, 20, 30]).expect("valid boundaries");
        h.add(5);
        h.add(15);
        h.add(25);
        h.add(100);

        let snap = h.snapshot();
        assert_eq!(snap.sum, 145);
        assert_eq!(snap.count, 4);
        assert_eq!(snap.min, Some(5));
        assert_eq!(snap.max, Some(100));

        let prev = h.take();
        assert_eq!(prev.sum, 145);
        assert_eq!(prev.min, Some(5));
        assert_eq!(prev.max, Some(100));

        let after = h.snapshot();
        assert_eq!(after.bucket_counts, [0, 0, 0, 0]);
        assert_eq!(after.count, 0);
        assert_eq!(after.sum, 0);
        assert_eq!(after.min, None);
        assert_eq!(after.max, None);
    }

    #[test]
    fn min_max_f64() {
        let h = Bucket::<f64>::new([1.0, 2.0]).expect("valid boundaries");
        h.add(0.5);
        h.add(1.5);
        h.add(3.0);
        h.add(-2.0);

        let snap = h.snapshot();
        assert_eq!(snap.count, 4);
        assert_eq!(snap.min, Some(-2.0));
        assert_eq!(snap.max, Some(3.0));
    }

    #[test]
    fn broken_boundaries() {
        assert!(matches!(
            Bucket::new([3u64, 2, 1]),
            Err(Error::InvalidHistogramBoundaries),
        ));
        assert!(matches!(
            Bucket::<u64>::new([]),
            Err(Error::InvalidHistogramBoundaries),
        ));
    }

    #[test]
    fn no_nan_poison() {
        let h = Bucket::<f64>::new([1.0, 2.0]).expect("valid boundaries");
        h.add(0.5);
        h.add(f64::NAN); // should be ignored for count, buckets, and sum
        h.add(1.5);

        let snap = h.snapshot();
        assert!(!snap.sum.is_nan(), "poisoned with nan: {}", snap.sum);
        assert_eq!(snap.sum, 2.0);
        assert_eq!(snap.count, 2, "NaN was counted: {:?}", snap);
        assert_eq!(
            snap.bucket_counts,
            [1, 1, 0],
            "NaN landed in a bucket: {:?}",
            snap
        );
        assert_eq!(snap.min, Some(0.5));
        assert_eq!(snap.max, Some(1.5));
    }

    #[test]
    fn consistent_snapshots() {
        // blink once in ci if you're broken lol

        const WRITERS: usize = 8;
        const WRITES_PER_THREAD: u64 = 50_000;

        let h = Arc::new(Bucket::new([1u64, 2, 3, 4, 5]).expect("valid boundaries"));
        let stop = Arc::new(AtomicBool::new(false));

        let reader = {
            let h = Arc::clone(&h);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut snapshots = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let s = h.snapshot();
                    let bucket_total: u64 = s.bucket_counts.iter().sum();
                    assert_eq!(
                        s.count, bucket_total,
                        "torn snapshot: count={} bucket_total={} buckets={:?}",
                        s.count, bucket_total, s.bucket_counts,
                    );
                    assert_eq!(
                        s.sum, s.count,
                        "torn snapshot: sum={} count={} (each add records value 1)",
                        s.sum, s.count,
                    );
                    snapshots += 1;
                }
                snapshots
            })
        };

        let writers: Vec<_> = (0..WRITERS)
            .map(|_| {
                let h = Arc::clone(&h);
                std::thread::spawn(move || {
                    for _ in 0..WRITES_PER_THREAD {
                        h.add(1u64);
                    }
                })
            })
            .collect();

        for w in writers {
            w.join().expect("w join")
        }
        stop.store(true, Ordering::Relaxed);
        let snapshots = reader.join().expect("r join");

        let expected = WRITERS as u64 * WRITES_PER_THREAD;
        let final_snap = h.snapshot();
        assert_eq!(final_snap.count, expected);
        assert_eq!(final_snap.sum, expected);
        assert_eq!(final_snap.bucket_counts.iter().sum::<u64>(), expected);
        assert!(
            snapshots > 0,
            "reader never completed a snapshot ({} writers)",
            WRITERS,
        );
    }

    #[test]
    fn atomic_reset() {
        let h = Arc::new(Bucket::new([1u64, 2, 3, 4, 5]).expect("valid boundaries"));
        let stop = Arc::new(AtomicBool::new(false));

        let reader = {
            let h = Arc::clone(&h);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let s = h.snapshot();
                    let bucket_total: u64 = s.bucket_counts.iter().sum();
                    assert_eq!(
                        s.count, bucket_total,
                        "torn snapshot during reset stress: count={} buckets={:?}",
                        s.count, s.bucket_counts,
                    );
                    assert_eq!(s.sum, s.count, "torn sum: sum={} count={}", s.sum, s.count);
                }
            })
        };

        let writer = {
            let h = Arc::clone(&h);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..1000 {
                        h.add(1u64);
                    }
                }
            })
        };

        let resetter = {
            let h = Arc::clone(&h);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let prev = h.take();
                    let bucket_total: u64 = prev.bucket_counts.iter().sum();
                    assert_eq!(
                        prev.count, bucket_total,
                        "torn snapshot_and_reset: count={} buckets={:?}",
                        prev.count, prev.bucket_counts,
                    );
                    assert_eq!(
                        prev.sum, prev.count,
                        "torn reset sum: sum={} count={}",
                        prev.sum, prev.count,
                    );
                }
            })
        };

        std::thread::sleep(Duration::from_millis(250));

        stop.store(true, Ordering::Relaxed);
        reader.join().expect("r join");
        writer.join().expect("w join");
        resetter.join().expect("reset join");
    }
}
