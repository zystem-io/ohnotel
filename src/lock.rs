use std::sync::atomic::{AtomicU64, Ordering, fence};

/// A sequence lock returning a consistent snapshot of `inner` under multiple writers with a HUGE
///
/// # !!!DISCLAIMER!!! #
///
/// It DOES NOT serialize writers against each other. If two writers run concurrently, their
/// effects on `inner` are interleaved arbitrarily and the final state may or may not be a mix of
/// both writers' work. But this is fine as long as ALL operations writers do on `inner` are
/// commutative, so that any interleaving produces the same end state.
///
/// It is intended to be used ONLY with add/sub semantics within the histogram bucket.
///
/// If you use a plain `store` op inside, it WILL break.
pub struct SeqLock<T> {
    start: AtomicU64,
    end: AtomicU64,
    inner: T,
}

impl<T> SeqLock<T> {
    pub const fn new(inner: T) -> Self {
        Self {
            start: AtomicU64::new(0),
            end: AtomicU64::new(0),
            inner,
        }
    }

    /// USE NO OPERATION HERE FOR WHICH ORDERING MATTERS.
    #[inline(always)]
    pub fn write<R>(&self, cb: impl FnOnce(&T) -> R) -> R {
        let _ = self.start.fetch_add(1, Ordering::AcqRel);
        let r = cb(&self.inner);
        let _ = self.end.fetch_add(1, Ordering::Release);
        r
    }

    #[inline(always)]
    pub fn read<R>(&self, cb: impl Fn(&T) -> R) -> R {
        loop {
            let e1 = self.end.load(Ordering::Acquire);
            let s1 = self.start.load(Ordering::Acquire);

            if s1 != e1 {
                std::hint::spin_loop();
                continue;
            }

            let r = cb(&self.inner);

            // Sync on let _ = self.end.fetch_add(1, Ordering::Release) in write
            fence(Ordering::Acquire);

            let s2 = self.start.load(Ordering::Relaxed);

            if s1 == s2 {
                return r;
            }

            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::lock::SeqLock;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    };
    use std::thread;

    struct Pair {
        x: AtomicU64,
        y: AtomicU64,
    }

    impl Pair {
        fn new(x: u64, y: u64) -> Self {
            Self {
                x: AtomicU64::new(x),
                y: AtomicU64::new(y),
            }
        }

        fn read(&self) -> (u64, u64) {
            (
                self.x.load(Ordering::Relaxed),
                self.y.load(Ordering::Relaxed),
            )
        }
    }

    #[test]
    fn misuse() {
        let lock = Arc::new(SeqLock::new(Pair::new(0, 0)));

        let (w1_wrote_x_tx, w1_wrote_x_rx) = mpsc::channel();
        let (w2_wrote_y_tx, w2_wrote_y_rx) = mpsc::channel();

        let lock_w1 = Arc::clone(&lock);
        let w1 = thread::spawn(move || {
            lock_w1.write(|pair| {
                // Writer 1's intended value is (0, 0).
                pair.x.store(0, Ordering::Relaxed);

                // Let writer 2 run after writer 1 has written only x.
                w1_wrote_x_tx.send(()).expect("w1_wrote_x_tx");

                // Wait until writer 2 has written both x and y.
                w2_wrote_y_rx.recv().expect("w2_wrote_y_rx");

                // Writer 1 resumes and overwrites only y back to 0.
                pair.y.store(0, Ordering::Relaxed);
            });
        });

        let lock_w2 = Arc::clone(&lock);
        let w2 = thread::spawn(move || {
            // Start writer 2 after writer 1 has partially written.
            w1_wrote_x_rx.recv().expect("w1_wrote_x_rx");

            lock_w2.write(|pair| {
                // Writer 2's intended value is (1, 1).
                pair.x.store(1, Ordering::Relaxed);
                pair.y.store(1, Ordering::Relaxed);

                // Let writer 1 resume and overwrite y.
                w2_wrote_y_tx.send(()).expect("w2_wrote_y_tx");
            });
        });

        w1.join().expect("w1 join");
        w2.join().expect("w2 join");

        let (x, y) = lock.read(Pair::read);

        assert_eq!((x, y), (1, 0), "expected the documented torn state");
    }
}
