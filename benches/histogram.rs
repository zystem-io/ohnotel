use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ohnotel::metric::Histogram;
use ohnotel::model::{KeyValue, NameIdentity, Str};
use opentelemetry::KeyValue as OtKv;
use opentelemetry::metrics::{Histogram as OtHistogram, MeterProvider as _};
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use std::borrow::Cow;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

const CARDINALITY: usize = 128;
const ATTR_COUNTS: [usize; 5] = [1, 2, 4, 8, 16];
const BG_WRITERS: [usize; 6] = [0, 1, 2, 4, 8, 16];
const BOUNDARIES: [u64; 9] = [1, 5, 10, 25, 50, 100, 250, 500, 1000];
const BOUNDARIES_F: [f64; 9] = [1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

/// Adds executed per criterion iteration. Amortizes the timer overhead so each
/// sample measures the cost of the hot path itself, not the harness.
const BATCH: u64 = 1024;

fn ohnotel_histogram() -> Histogram<u64> {
    let id = NameIdentity {
        name: Str::Cow(Cow::Borrowed("bench")),
        description: Str::Cow(Cow::Borrowed("")),
        unit: Str::Cow(Cow::Borrowed("")),
    };
    Histogram::<u64>::new(id, BOUNDARIES.to_vec()).expect("valid boundaries")
}

/// Returns `(provider, histogram)`. The provider must outlive the histogram.
fn otel_histogram() -> (SdkMeterProvider, OtHistogram<u64>) {
    let reader = ManualReader::builder().build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("bench");
    let h = meter
        .u64_histogram("bench_h")
        .with_boundaries(BOUNDARIES_F.to_vec())
        .build();
    (provider, h)
}

const SHUFFLE_SEED: u64 = 0x5151_5151_5151_5151;

fn ohnotel_pool(n: usize, sort: bool) -> Arc<[Vec<KeyValue>]> {
    let mut rng = StdRng::seed_from_u64(SHUFFLE_SEED);
    let pool: Vec<Vec<KeyValue>> = (0..CARDINALITY)
        .map(|i| {
            let mut kvs: Vec<KeyValue> = (0..n)
                .map(|k| KeyValue::new(format!("attr_{k}"), format!("attr_{k}_value_{i}")))
                .collect();
            if !sort {
                kvs.shuffle(&mut rng);
            }
            kvs
        })
        .collect();
    Arc::from(pool)
}

fn otel_pool(n: usize, sort: bool) -> Arc<[Vec<OtKv>]> {
    let mut rng = StdRng::seed_from_u64(SHUFFLE_SEED);
    let pool: Vec<Vec<OtKv>> = (0..CARDINALITY)
        .map(|i| {
            let mut kvs: Vec<OtKv> = (0..n)
                .map(|k| OtKv::new(format!("attr_{k}"), format!("attr_{k}_value_{i}")))
                .collect();
            if !sort {
                kvs.shuffle(&mut rng);
            }
            kvs
        })
        .collect();
    Arc::from(pool)
}

/// Owns the background writer fleet for one bench parameter. Signals shutdown
/// on drop so a panicking bench cannot leak threads.
struct Workers {
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl Workers {
    fn join(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles.drain(..) {
            h.join().expect("worker thread panicked");
        }
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

fn spawn_ohnotel_writers(
    t: usize,
    h: &Arc<Histogram<u64>>,
    pool: &Arc<[Vec<KeyValue>]>,
) -> Workers {
    let stop = Arc::new(AtomicBool::new(false));
    let cursor = Arc::new(AtomicUsize::new(0));
    let handles = (0..t)
        .map(|_| {
            let h = Arc::clone(h);
            let pool = Arc::clone(pool);
            let stop = Arc::clone(&stop);
            let cursor = Arc::clone(&cursor);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let i = cursor.fetch_add(1, Ordering::Relaxed) % CARDINALITY;
                    h.add(1, &pool[i]);
                }
            })
        })
        .collect();
    Workers { stop, handles }
}

fn spawn_otel_writers(t: usize, h: &Arc<OtHistogram<u64>>, pool: &Arc<[Vec<OtKv>]>) -> Workers {
    let stop = Arc::new(AtomicBool::new(false));
    let cursor = Arc::new(AtomicUsize::new(0));
    let handles = (0..t)
        .map(|_| {
            let h = Arc::clone(h);
            let pool = Arc::clone(pool);
            let stop = Arc::clone(&stop);
            let cursor = Arc::clone(&cursor);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let i = cursor.fetch_add(1, Ordering::Relaxed) % CARDINALITY;
                    h.record(1, &pool[i]);
                }
            })
        })
        .collect();
    Workers { stop, handles }
}

fn bench_no_attrs(c: &mut Criterion) {
    let mut g = c.benchmark_group("no_attrs");
    let _ = g.throughput(Throughput::Elements(BATCH));

    let h = ohnotel_histogram();
    let _ = g.bench_function("ohnotel", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                h.add(black_box(1), &[]);
            }
        });
    });

    let (_provider, h) = otel_histogram();
    let _ = g.bench_function("otel", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                h.record(black_box(1), &[]);
            }
        });
    });

    g.finish();
}

fn bench_attrs(c: &mut Criterion, group: &str, sort: bool) {
    let mut g = c.benchmark_group(group);
    let _ = g.throughput(Throughput::Elements(BATCH));

    for &n in &ATTR_COUNTS {
        let pool = ohnotel_pool(n, sort);
        let h = ohnotel_histogram();
        let _ = g.bench_function(BenchmarkId::new("ohnotel", n), |b| {
            let mut i: usize = 0;
            b.iter(|| {
                for _ in 0..BATCH {
                    h.add(black_box(1), &pool[i % CARDINALITY]);
                    i = i.wrapping_add(1);
                }
            });
        });

        let pool = otel_pool(n, sort);
        let (_provider, h) = otel_histogram();
        let _ = g.bench_function(BenchmarkId::new("otel", n), |b| {
            let mut i: usize = 0;
            b.iter(|| {
                for _ in 0..BATCH {
                    h.record(black_box(1), &pool[i % CARDINALITY]);
                    i = i.wrapping_add(1);
                }
            });
        });
    }

    g.finish();
}

/// Per-bench-thread `add` throughput while `t` background writers hammer the
/// same histogram and attribute pool. `t == 0` is the uncontended baseline.
fn bench_contended_add(c: &mut Criterion) {
    const N: usize = 4;

    let mut g = c.benchmark_group("contended_add");
    let _ = g.throughput(Throughput::Elements(BATCH));

    for &t in &BG_WRITERS {
        // ohnotel.
        let pool = ohnotel_pool(N, true);
        let h = Arc::new(ohnotel_histogram());
        let workers = spawn_ohnotel_writers(t, &h, &pool);
        let _ = g.bench_function(BenchmarkId::new("ohnotel", t), |b| {
            let mut i: usize = 0;
            b.iter(|| {
                for _ in 0..BATCH {
                    h.add(black_box(1), &pool[i % CARDINALITY]);
                    i = i.wrapping_add(1);
                }
            });
        });
        workers.join();

        // otel.
        let pool = otel_pool(N, true);
        let (_provider, h) = otel_histogram();
        let h = Arc::new(h);
        let workers = spawn_otel_writers(t, &h, &pool);
        let _ = g.bench_function(BenchmarkId::new("otel", t), |b| {
            let mut i: usize = 0;
            b.iter(|| {
                for _ in 0..BATCH {
                    h.record(black_box(1), &pool[i % CARDINALITY]);
                    i = i.wrapping_add(1);
                }
            });
        });
        workers.join();
    }

    g.finish();
}

/// `snapshot` cost on the bench thread while `t` background writers contend on
/// the same series. Each iteration snapshots every entry in the pool once.
fn bench_snapshot_under_load(c: &mut Criterion) {
    const N: usize = 4;

    let mut g = c.benchmark_group("snapshot_under_load");
    let _ = g.throughput(Throughput::Elements(CARDINALITY as u64));

    let pool = ohnotel_pool(N, true);
    let h = Arc::new(ohnotel_histogram());

    // Pre-populate every series so snapshot has work to do.
    for kvs in pool.iter() {
        h.add(1, kvs);
    }

    for &t in &BG_WRITERS {
        let workers = spawn_ohnotel_writers(t, &h, &pool);
        let _ = g.bench_function(BenchmarkId::new("ohnotel", t), |b| {
            b.iter(|| {
                for kvs in pool.iter() {
                    let _snap = black_box(h.snapshot(kvs).expect("series exists"));
                }
            });
        });
        workers.join();
    }

    g.finish();
}

fn benches(c: &mut Criterion) {
    bench_no_attrs(c);
    bench_attrs(c, "attrs_sorted", true);
    bench_attrs(c, "attrs_unsorted", false);
    bench_contended_add(c);
    bench_snapshot_under_load(c);
}

criterion_group! {
    name = histogram;
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(3));
    targets = benches
}
criterion_main!(histogram);
