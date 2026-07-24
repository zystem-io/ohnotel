use crate::bucket_map::BucketMap;
use crate::collect;
use crate::dto::Kind;
use crate::model::NameIdentity;
use crate::observe::{MetricSource, Mode};
use crate::prometheus::Error as PrometheusError;
use crate::prometheus::dto::{DataPoint, IntoDataPointValue, Metric};
use crate::{Error, atomic};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};

/// A registered metric source that a scrape can read.
trait Source: Send + Sync + 'static {
    /// Reads the source's current state as a renderable metric family, or
    /// `None` when nothing has been recorded yet.
    fn scrape(&self) -> Result<Option<Metric>, Error>;
}

/// The identity of a metric source plus a shared handle to its live buckets.
struct SourceHandle<T, S, A>
where
    T: atomic::Measure,
    S: BuildHasher + Clone,
    A: atomic::Record<T>,
{
    id: Arc<NameIdentity>,
    buckets: Arc<BucketMap<T, S, A>>,
    kind: Kind,
}

impl<T, S, A> Source for SourceHandle<T, S, A>
where
    T: atomic::Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: atomic::Record<T>,
    A::Snapshot: IntoDataPointValue,
{
    fn scrape(&self) -> Result<Option<Metric>, Error> {
        let mut points = Vec::new();
        let mut failure = None;
        self.buckets.visit_bucket(|entry| {
            if failure.is_none() {
                match entry.bucket.current().into_data_point_value(&self.id) {
                    Ok(value) => points.push(DataPoint {
                        attrs: Arc::clone(&entry.attrs),
                        value,
                    }),
                    Err(err) => failure = Some(err),
                }
            }
            // Returning `false` would evict the bucket; a scrape must never
            // mutate the source.
            true
        });
        if let Some(err) = failure {
            return Err(err);
        }
        if points.is_empty() {
            return Ok(None);
        }
        Ok(Some(Metric {
            id: Arc::clone(&self.id),
            kind: self.kind,
            points,
        }))
    }
}

/// A pull-based collector for Prometheus scrapes.
///
/// Instead of pushing batches on a timer, it holds handles to the registered
/// sources until [`Collector::render`] is called and then reports their
/// current state in the text exposition format. There are no observers and no
/// observation mode involved: a scrape reads the metric values as they are,
/// which is exactly the cumulative view Prometheus expects.
#[derive(Clone)]
pub struct Collector {
    inner: Arc<Inner>,
}

struct Inner {
    sources: Mutex<Vec<Box<dyn Source>>>,
}

impl Collector {
    /// Builds an empty collector. Sources are added through
    /// [`Collector::add`] or a [`collect::Collection`].
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                sources: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Adds a metric source. Every scrape reports the source's current state
    /// as-is, so unlike the push-based collectors there is no `mode`
    /// parameter.
    pub fn add<Src, T, S, A>(&self, source: Src) -> Src
    where
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        A::Snapshot: IntoDataPointValue,
    {
        self.add_handle(&source);
        source
    }

    fn add_handle<Src, T, S, A>(&self, source: &Src)
    where
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        A::Snapshot: IntoDataPointValue,
    {
        self.inner
            .sources
            .lock()
            .expect("sources poisoned")
            .push(Box::new(SourceHandle {
                id: Arc::clone(source.id()),
                buckets: Arc::clone(source.buckets()),
                kind: source.kind(),
            }));
    }

    /// Reads every registered source and renders the result in the
    /// Prometheus text exposition format (version 0.0.4).
    ///
    /// Metrics with the same rendered name are merged only when their identity
    /// and kind match. Any other collision, including histogram suffixes,
    /// returns [`PrometheusError::FamilyCollision`].
    ///
    /// Families are sorted by name, so the output is deterministic. An empty
    /// registry renders an empty string.
    ///
    /// # Errors
    ///
    /// Returns a scrape error, or an error if the registered metrics cannot
    /// be represented as a valid Prometheus scrape.
    pub fn render(&self) -> Result<String, Error> {
        let mut families: BTreeMap<String, Metric> = BTreeMap::new();

        let sources = self.inner.sources.lock().expect("sources poisoned");
        for source in &*sources {
            let Some(metric) = source.scrape()? else {
                continue;
            };

            match families.entry(metric.family_name()?) {
                Entry::Occupied(mut entry) => {
                    let family = entry.get();
                    if family.id != metric.id || family.kind != metric.kind {
                        return Err(PrometheusError::FamilyCollision(entry.key().clone()).into());
                    }
                    entry.get_mut().points.extend(metric.points);
                }
                Entry::Vacant(entry) => {
                    let _ = entry.insert(metric);
                }
            }
        }
        drop(sources);

        let mut emitted: HashSet<String> = HashSet::new();
        for (name, family) in &families {
            let suffixes: &[&str] = match family.kind {
                Kind::Histogram => &["", "_bucket", "_sum", "_count"],
                Kind::Counter | Kind::Gauge => &[""],
            };

            for suffix in suffixes {
                let sample = format!("{name}{suffix}");
                if emitted.contains(&sample) {
                    return Err(PrometheusError::FamilyCollision(sample).into());
                }
                let _ = emitted.insert(sample);
            }
        }

        let mut out = String::new();
        for family in families.values() {
            family.render_into(&mut out)?;
        }

        Ok(out)
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collector").finish_non_exhaustive()
    }
}

/// Membership in a [`collect::Collection`]: scrapes report the source's
/// current state as-is, so the observation mode -- which only describes how
/// push observers report -- is ignored.
impl<Src, T, S, A> collect::AddSource<Src> for Collector
where
    Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
    T: atomic::Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: atomic::Record<T>,
    A::Snapshot: IntoDataPointValue,
{
    fn add_source(&self, source: &Src, _mode: Mode) -> Result<(), Error> {
        self.add_handle(source);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::Collection;
    use crate::metric::{Counter, Gauge, Histogram};
    use crate::model::{KeyValue, NameIdentity, Str};
    use std::borrow::Cow;
    use testresult::TestResult;

    fn id(name: &'static str) -> NameIdentity {
        NameIdentity {
            name: Str::Cow(Cow::Borrowed(name)),
            description: Str::Cow(Cow::Borrowed("")),
            unit: Str::Cow(Cow::Borrowed("")),
        }
    }

    #[test]
    fn renders_cumulative_totals_across_scrapes() -> TestResult {
        let collector = Collector::new();
        let counter = collector.add(Counter::<u64>::new(id("http.server.requests")));

        counter.add(5, &[]);
        let first = collector.render()?;
        assert!(first.contains("http_server_requests_total 5\n"), "{first}");

        counter.add(3, &[]);
        let second = collector.render()?;
        assert!(
            second.contains("http_server_requests_total 8\n"),
            "{second}"
        );
        Ok(())
    }

    #[test]
    fn collection_scrapes_report_state_regardless_of_requested_mode() -> TestResult {
        let collection = Collection::new((Collector::new(),));
        let counter =
            collection.add(Counter::<u64>::new(id("http.server.requests")), Mode::Delta)?;
        let (prometheus,) = collection.collectors();

        counter.add(5, &[]);
        let first = prometheus.render()?;
        assert!(first.contains("http_server_requests_total 5\n"), "{first}");

        counter.add(3, &[]);
        let second = prometheus.render()?;
        assert!(
            second.contains("http_server_requests_total 8\n"),
            "{second}"
        );
        Ok(())
    }

    #[test]
    fn untouched_source_renders_nothing() -> TestResult {
        let collector = Collector::new();
        let _counter = collector.add(Counter::<u64>::new(id("http.server.requests")));
        assert_eq!(collector.render()?, "");
        Ok(())
    }

    #[test]
    fn gauge_reports_current_value() -> TestResult {
        let collector = Collector::new();
        let gauge = collector.add(Gauge::<i64>::new(id("queue.depth")));

        gauge.set(7, &[]);
        assert!(collector.render()?.contains("queue_depth 7\n"));

        gauge.set(3, &[]);
        let out = collector.render()?;
        assert!(out.contains("queue_depth 3\n"), "{out}");
        assert!(!out.contains("queue_depth 7\n"), "{out}");
        Ok(())
    }

    #[test]
    fn renders_histograms_end_to_end() -> TestResult {
        let collector = Collector::new();
        let histogram = collector.add(Histogram::<u64>::new(
            id("http.server.request.duration"),
            [10, 20],
        )?);
        histogram.add(5, &[]);
        histogram.add(25, &[]);

        let out = collector.render()?;
        assert!(
            out.contains("# TYPE http_server_request_duration histogram\n"),
            "{out}"
        );
        assert!(
            out.contains("http_server_request_duration_bucket{le=\"10\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("http_server_request_duration_bucket{le=\"+Inf\"} 2\n"),
            "{out}"
        );
        assert!(
            out.contains("http_server_request_duration_sum 30\n"),
            "{out}"
        );
        assert!(
            out.contains("http_server_request_duration_count 2\n"),
            "{out}"
        );
        Ok(())
    }

    #[test]
    fn orders_families_deterministically() -> TestResult {
        let collector = Collector::new();
        let second = collector.add(Counter::<u64>::new(id("b.metric")));
        let first = collector.add(Counter::<u64>::new(id("a.metric")));
        second.add(1, &[]);
        first.add(1, &[]);

        let out = collector.render()?;
        let first_pos = out.find("a_metric_total").expect("a family");
        let second_pos = out.find("b_metric_total").expect("b family");
        assert!(first_pos < second_pos, "{out}");
        Ok(())
    }

    #[test]
    fn orders_series_and_labels_deterministically() -> TestResult {
        let collector = Collector::new();
        let counter = collector.add(Counter::<u64>::new(id("requests")));
        counter.add(
            2,
            &[KeyValue::new("zone", "z"), KeyValue::new("method", "GET")],
        );
        counter.add(
            1,
            &[KeyValue::new("zone", "a"), KeyValue::new("method", "GET")],
        );

        let out = collector.render()?;
        assert_eq!(
            out,
            concat!(
                "# TYPE requests_total counter\n",
                "requests_total{method=\"GET\",zone=\"a\"} 1\n",
                "requests_total{method=\"GET\",zone=\"z\"} 2\n",
            ),
        );
        Ok(())
    }

    #[test]
    fn merges_families_with_equal_identity_and_kind() -> TestResult {
        let collector = Collector::new();
        let first = collector.add(Counter::<u64>::new(id("app.requests")));
        let second = collector.add(Counter::<u64>::new(id("app.requests")));
        first.add(1, &[KeyValue::new("route", "/a")]);
        second.add(2, &[KeyValue::new("route", "/b")]);

        let out = collector.render()?;
        assert_eq!(
            out.matches("# TYPE app_requests_total counter").count(),
            1,
            "{out}"
        );
        assert!(
            out.contains("app_requests_total{route=\"/a\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("app_requests_total{route=\"/b\"} 2\n"),
            "{out}"
        );
        Ok(())
    }

    #[test]
    fn rejects_distinct_metrics_colliding_on_family_name() -> TestResult {
        let collector = Collector::new();
        let first = collector.add(Counter::<u64>::new(id("app.requests")));
        let second = collector.add(Counter::<u64>::new(id("app/requests")));
        first.add(1, &[]);
        second.add(2, &[]);

        let err = collector.render().expect_err("family collision");
        assert!(
            matches!(&err, Error::Prometheus(PrometheusError::FamilyCollision(name)) if name == "app_requests_total"),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_kind_mismatch_colliding_on_family_name() -> TestResult {
        let collector = Collector::new();
        let counter = collector.add(Counter::<u64>::new(id("app.requests")));
        let gauge = collector.add(Gauge::<i64>::new(id("app.requests.total")));
        counter.add(1, &[]);
        gauge.set(2, &[]);

        let err = collector.render().expect_err("family collision");
        assert!(
            matches!(&err, Error::Prometheus(PrometheusError::FamilyCollision(name)) if name == "app_requests_total"),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_label_collision_within_a_sample() -> TestResult {
        let collector = Collector::new();
        let counter = collector.add(Counter::<u64>::new(id("app.requests")));
        counter.add(
            1,
            &[
                KeyValue::new("http.route", "/a"),
                KeyValue::new("http/route", "/b"),
            ],
        );

        let err = collector.render().expect_err("label collision");
        assert!(
            matches!(
                &err,
                Error::Prometheus(PrometheusError::LabelCollision(label, name))
                    if label == "http_route" && name == "app.requests"
            ),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_duplicate_series_across_merged_sources() -> TestResult {
        let collector = Collector::new();
        let first = collector.add(Counter::<u64>::new(id("app.requests")));
        let second = collector.add(Counter::<u64>::new(id("app.requests")));
        first.add(1, &[]);
        second.add(2, &[]);

        let err = collector.render().expect_err("duplicate series");
        assert!(
            matches!(
                &err,
                Error::Prometheus(PrometheusError::DuplicateSeries(labels, name))
                    if labels.is_empty() && name == "app.requests"
            ),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_attr_sets_sanitizing_to_equal_labels() -> TestResult {
        let collector = Collector::new();
        let counter = collector.add(Counter::<u64>::new(id("app.requests")));
        counter.add(1, &[KeyValue::new("a.b", "1")]);
        counter.add(2, &[KeyValue::new("a/b", "1")]);

        let err = collector.render().expect_err("duplicate series");
        assert!(
            matches!(
                &err,
                Error::Prometheus(PrometheusError::DuplicateSeries(labels, name))
                    if labels == "a_b=\"1\"" && name == "app.requests"
            ),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_duplicate_series_with_differently_ordered_sanitized_labels() -> TestResult {
        let collector = Collector::new();
        let counter = collector.add(Counter::<u64>::new(id("app.requests")));
        counter.add(1, &[KeyValue::new("a.", "x"), KeyValue::new("a0", "y")]);
        counter.add(2, &[KeyValue::new("a~", "x"), KeyValue::new("a0", "y")]);

        let err = collector.render().expect_err("duplicate series");
        assert!(
            matches!(
                &err,
                Error::Prometheus(PrometheusError::DuplicateSeries(labels, name))
                    if labels == "a0=\"y\",a_=\"x\"" && name == "app.requests"
            ),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn empty_registry_renders_empty_string() -> TestResult {
        assert_eq!(Collector::new().render()?, "");
        Ok(())
    }

    #[test]
    fn rejects_empty_metric_names() -> TestResult {
        let collector = Collector::new();
        let gauge = collector.add(Gauge::<u64>::new(id("")));
        gauge.set(1, &[]);

        let err = collector.render().expect_err("empty metric name");
        assert!(
            matches!(err, Error::Prometheus(PrometheusError::EmptyMetricName)),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_empty_label_names() -> TestResult {
        let collector = Collector::new();
        let gauge = collector.add(Gauge::<u64>::new(id("m")));
        gauge.set(1, &[KeyValue::new("", "empty")]);

        let err = collector.render().expect_err("empty label name");
        assert!(
            matches!(&err, Error::Prometheus(PrometheusError::EmptyLabelName(name)) if name == "m"),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn remaps_reserved_label_names_and_skips_valueless_attributes() -> TestResult {
        let collector = Collector::new();
        let gauge = collector.add(Gauge::<u64>::new(id("m")));
        gauge.set(
            1,
            &[KeyValue::no_val(""), KeyValue::new("__name__", "reserved")],
        );

        assert_eq!(
            collector.render()?,
            concat!("# TYPE m gauge\n", "m{label__name__=\"reserved\"} 1\n",),
        );
        Ok(())
    }

    #[test]
    fn rejects_histogram_suffix_colliding_with_scalar_family() -> TestResult {
        let collector = Collector::new();
        let histogram = collector.add(Histogram::<u64>::new(id("request.duration"), [10, 20])?);
        let gauge = collector.add(Gauge::<i64>::new(id("request.duration.count")));
        histogram.add(5, &[]);
        gauge.set(2, &[]);

        let err = collector.render().expect_err("emitted-name collision");
        assert!(
            matches!(
                &err,
                Error::Prometheus(PrometheusError::FamilyCollision(name)) if name == "request_duration_count"
            ),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn rejects_scalar_family_colliding_with_histogram_suffix() -> TestResult {
        let collector = Collector::new();
        let gauge = collector.add(Gauge::<i64>::new(id("request.duration.bucket")));
        let histogram = collector.add(Histogram::<u64>::new(id("request.duration"), [10, 20])?);
        gauge.set(1, &[]);
        histogram.add(5, &[]);

        let err = collector.render().expect_err("emitted-name collision");
        assert!(
            matches!(
                &err,
                Error::Prometheus(PrometheusError::FamilyCollision(name)) if name == "request_duration_bucket"
            ),
            "unexpected error: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn non_overlapping_families_render_together() -> TestResult {
        let collector = Collector::new();
        let histogram = collector.add(Histogram::<u64>::new(id("request.duration"), [10, 20])?);
        let gauge = collector.add(Gauge::<i64>::new(id("queue.depth")));
        histogram.add(5, &[]);
        gauge.set(7, &[]);

        let out = collector.render()?;
        assert!(out.contains("# TYPE request_duration histogram\n"), "{out}");
        assert!(out.contains("request_duration_count 1\n"), "{out}");
        assert!(out.contains("queue_depth 7\n"), "{out}");
        Ok(())
    }
}
