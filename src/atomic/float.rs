use crate::atomic;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

/// An implementation of atomic f64.
pub struct AtomicF64 {
    bits: AtomicU64,
}

impl Debug for AtomicF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicF64")
            .field("val", &atomic::Scalar::get(self))
            .finish()
    }
}

impl AtomicF64 {
    #[inline]
    pub const fn new(value: f64) -> Self {
        debug_assert!(!value.is_nan(), "tried to instantiate AtomicF64 with NaN");
        Self {
            bits: AtomicU64::new(value.to_bits()),
        }
    }

    #[inline]
    fn update_with<F: Fn(f64) -> f64>(&self, f: F) {
        let mut old = self.bits.load(Ordering::Relaxed);
        loop {
            let cur = f64::from_bits(old);
            let new_bits = f(cur).to_bits();
            if new_bits == old {
                return;
            }
            match self.bits.compare_exchange_weak(
                old,
                new_bits,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(next) => old = next,
            }
        }
    }
}

impl From<f64> for AtomicF64 {
    #[inline]
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}

impl atomic::Record<f64> for AtomicF64 {
    type Snapshot = f64;

    #[inline(always)]
    fn add(&self, value: f64) {
        if value.is_nan() {
            return;
        }
        self.update_with(|cur| cur + value);
    }

    #[inline(always)]
    fn sub(&self, value: f64) {
        self.add(-value);
    }

    #[inline(always)]
    fn clear(&self) {
        self.bits.store(0.0f64.to_bits(), Ordering::Relaxed);
    }

    #[inline(always)]
    fn current(&self) -> f64 {
        atomic::Scalar::load(self)
    }
}

impl atomic::Scalar<f64> for AtomicF64 {
    #[inline]
    fn fetch_min(&self, value: f64) {
        // ignore NaNs
        if value.is_nan() {
            return;
        }

        self.update_with(|cur| {
            if cur.is_nan() || value < cur {
                value
            } else {
                cur
            }
        });
    }

    #[inline]
    fn fetch_max(&self, value: f64) {
        if value.is_nan() {
            return;
        }

        self.update_with(|cur| {
            if cur.is_nan() || value > cur {
                value
            } else {
                cur
            }
        });
    }

    #[inline]
    fn swap(&self, value: f64) -> f64 {
        // swap currently is called from reset only; AtomicF64 is private
        debug_assert!(!value.is_nan(), "tried to swap with NaN");
        f64::from_bits(self.bits.swap(value.to_bits(), Ordering::Relaxed))
    }

    #[inline]
    fn load(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }

    #[inline]
    fn store(&self, value: f64) {
        if value.is_nan() {
            return;
        }

        self.bits.store(value.to_bits(), Ordering::Relaxed);
    }

    #[inline]
    fn reset(&self) -> f64 {
        self.swap(0.0)
    }
}

impl atomic::Measure for f64 {
    type Type = AtomicF64;

    fn min_identity() -> Self {
        f64::INFINITY
    }

    fn max_identity() -> Self {
        f64::NEG_INFINITY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_sample() {
        let a = AtomicF64::new(0.0);
        atomic::Scalar::fetch_max(&a, std::f64::consts::PI);
        atomic::Scalar::fetch_max(&a, 2.71);
        atomic::Scalar::fetch_max(&a, f64::NAN); // ignored
        assert!((atomic::Scalar::get(&a) - std::f64::consts::PI).abs() < 1e-12);

        atomic::Scalar::fetch_min(&a, -1.0);
        atomic::Scalar::fetch_min(&a, f64::NAN); // ignored
        assert!((atomic::Scalar::get(&a) - (-1.0)).abs() < 1e-12);
    }
}
