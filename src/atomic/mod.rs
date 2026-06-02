mod float;
pub mod histogram;
pub mod integers;

use std::fmt::Debug;
use std::ops::Sub;

pub trait Measure:
    Sized + Debug + Default + PartialOrd + Sub<Output = Self> + Clone + Send + Sync + 'static
{
    type Type: Scalar<Self> + From<Self>;

    fn min_identity() -> Self;
    fn max_identity() -> Self;
}

pub trait Record<T>: Debug + Send + Sync + 'static {
    type Snapshot: Clone
        + PartialOrd
        + Sub<Output = Self::Snapshot>
        + IsInitial
        + Send
        + Sync
        + 'static;

    /// Wraps on overflow for unsigned `T`; use `i64` if values can go negative.
    fn add(&self, value: T);

    /// Wraps on underflow for unsigned `T`; use `i64` if values can go negative.
    fn sub(&self, value: T);

    fn clear(&self);

    fn current(&self) -> Self::Snapshot;
}

pub trait IsInitial {
    fn is_initial(&self) -> bool;
}

impl IsInitial for u64 {
    #[inline(always)]
    fn is_initial(&self) -> bool {
        *self == 0
    }
}

impl IsInitial for i64 {
    #[inline(always)]
    fn is_initial(&self) -> bool {
        *self == 0
    }
}

impl IsInitial for f64 {
    #[inline(always)]
    fn is_initial(&self) -> bool {
        *self == 0.0
    }
}

pub trait Scalar<T>: Record<T> {
    fn fetch_min(&self, value: T);
    fn fetch_max(&self, value: T);
    fn swap(&self, value: T) -> T;
    fn load(&self) -> T;
    fn store(&self, value: T);
    #[must_use = "This is the only remaining copy of the data."]
    fn reset(&self) -> T;
    #[inline]
    fn get(&self) -> T {
        self.load()
    }
}
