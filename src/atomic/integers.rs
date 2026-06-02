use crate::atomic;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

macro_rules! impl_atomic_integer {
    ($atomic:ty, $prim:ty) => {
        impl atomic::Record<$prim> for $atomic {
            type Snapshot = $prim;

            /// Wraps on overflow rather than panicking or saturating; past `MAX` it
            /// wraps back to `MIN` (for `u64`, `MAX + 1 == 0`). Use `i64` if values can go negative.
            #[inline(always)]
            fn add(&self, value: $prim) {
                let _ = self.fetch_add(value, Ordering::Relaxed);
            }

            /// Wraps on underflow rather than panicking or saturating; below `MIN` it
            /// wraps back to `MAX` (for `u64`, `0 - 1 == MAX`). Use `i64` if values can go negative.
            #[inline(always)]
            fn sub(&self, value: $prim) {
                let _ = self.fetch_sub(value, Ordering::Relaxed);
            }

            #[inline(always)]
            fn clear(&self) {
                <$atomic>::store(self, 0, Ordering::Relaxed);
            }

            #[inline(always)]
            fn current(&self) -> $prim {
                <$atomic>::load(self, Ordering::Relaxed)
            }
        }

        impl atomic::Scalar<$prim> for $atomic {
            #[inline(always)]
            fn fetch_min(&self, value: $prim) {
                let _ = <$atomic>::fetch_min(self, value, Ordering::Relaxed);
            }

            #[inline(always)]
            fn fetch_max(&self, value: $prim) {
                let _ = <$atomic>::fetch_max(self, value, Ordering::Relaxed);
            }

            #[inline(always)]
            fn swap(&self, value: $prim) -> $prim {
                <$atomic>::swap(self, value, Ordering::Relaxed)
            }

            #[inline(always)]
            fn load(&self) -> $prim {
                <$atomic>::load(self, Ordering::Relaxed)
            }

            #[inline(always)]
            fn store(&self, value: $prim) {
                <$atomic>::store(self, value, Ordering::Relaxed);
            }

            #[inline(always)]
            fn reset(&self) -> $prim {
                <$atomic>::swap(self, 0, Ordering::Relaxed)
            }
        }

        impl atomic::Measure for $prim {
            type Type = $atomic;

            fn min_identity() -> Self {
                <$prim>::MAX
            }

            fn max_identity() -> Self {
                <$prim>::MIN
            }
        }
    };
}

impl_atomic_integer!(AtomicU64, u64);
impl_atomic_integer!(AtomicI64, i64);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic::Record as _;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn bucket() {
        let a = AtomicU64::new(0);
        a.add(1u64);
        a.add(4u64);
        assert_eq!(atomic::Scalar::get(&a), 5);
        assert_eq!(atomic::Scalar::reset(&a), 5);
        assert_eq!(atomic::Scalar::get(&a), 0);

        let a = AtomicU64::from(0);
        a.add(1u64);
        assert_eq!(atomic::Scalar::get(&a), 1);
    }

    #[test]
    fn sample() {
        let a = AtomicU64::new(100);
        atomic::Scalar::fetch_min(&a, 50);
        assert_eq!(atomic::Scalar::get(&a), 50);
        atomic::Scalar::fetch_min(&a, 80);
        assert_eq!(atomic::Scalar::get(&a), 50);
        atomic::Scalar::fetch_max(&a, 70);
        assert_eq!(atomic::Scalar::get(&a), 70);
        atomic::Scalar::fetch_max(&a, 200);
        assert_eq!(atomic::Scalar::get(&a), 200);
    }
}
