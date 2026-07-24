use crate::Error;
use crate::atomic;
use crate::atomic::histogram;
use crate::dto::Kind;
use crate::model::{KeyValue, NameIdentity, Value};
use crate::prometheus::Error as PrometheusError;
use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use std::fmt::Write as _;
use std::sync::Arc;

/// Prometheus-renderable metric family.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    pub id: Arc<NameIdentity>,
    pub kind: Kind,
    pub points: Vec<DataPoint>,
}

/// One Prometheus-renderable sample.
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub attrs: Arc<[KeyValue]>,
    pub value: DataPointValue,
}

/// Prometheus-renderable sample value.
#[derive(Debug, Clone, PartialEq)]
pub enum DataPointValue {
    Number(OrderedFloat<f64>),
    Histogram(HistogramValue),
}

/// Prometheus-renderable histogram value. `bucket_counts` holds per-bucket
/// (non-cumulative) counts; the cumulative `le` series required by the
/// exposition format is computed while rendering.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramValue {
    pub boundaries: Vec<f64>,
    pub bucket_counts: Vec<u64>,
    pub count: u64,
    pub sum: OrderedFloat<f64>,
}

impl Metric {
    /// Returns the final sample-family name: the metric name sanitized to the
    /// Prometheus alphabet, with a `_total` suffix appended for counters that
    /// do not already carry one.
    ///
    /// # Errors
    ///
    /// Returns [`PrometheusError::EmptyMetricName`] if the original name is
    /// empty.
    pub fn family_name(&self) -> Result<String, PrometheusError> {
        if self.id.name.as_str().is_empty() {
            return Err(PrometheusError::EmptyMetricName);
        }

        let mut name = sanitize_metric_name(self.id.name.as_str());
        if self.kind == Kind::Counter && !name.ends_with("_total") {
            name.push_str("_total");
        }
        Ok(name)
    }

    /// Renders this family into `out` in the text exposition format.
    ///
    /// # Errors
    ///
    /// Returns an error if the metric has an empty name, an emitted attribute
    /// has an empty key, or sanitization would produce colliding labels or
    /// duplicate series. Validation completes before `out` is modified.
    pub fn render_into(&self, out: &mut String) -> Result<(), PrometheusError> {
        let name = self.family_name()?;

        let mut points: Vec<(String, &DataPoint)> = Vec::with_capacity(self.points.len());
        for point in &self.points {
            // `le` is generated on `<name>_bucket` lines, so it is reserved
            // for histogram points only; scalar points may use it freely.
            let reserved: &[&str] = match &point.value {
                DataPointValue::Number(_) => &[],
                DataPointValue::Histogram(_) => &["le"],
            };
            let labels = render_labels(self.id.name.as_str(), &point.attrs, reserved)?;
            points.push((labels, point));
        }

        points.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        if let Some(duplicate) = points
            .windows(2)
            .find(|pair| pair[0].0.as_str() == pair[1].0.as_str())
        {
            return Err(PrometheusError::DuplicateSeries(
                duplicate[1].0.clone(),
                self.id.name.as_str().to_owned(),
            ));
        }

        let description = self.id.description.as_str();
        if !description.is_empty() {
            let _ = write!(out, "# HELP {name} ");
            escape_help_into(description, out);
            out.push('\n');
        }

        let _ = writeln!(out, "# TYPE {name} {}", kind_str(self.kind));

        for (labels, point) in points {
            match &point.value {
                DataPointValue::Number(value) => {
                    out.push_str(&name);
                    if !labels.is_empty() {
                        let _ = write!(out, "{{{labels}}}");
                    }
                    out.push(' ');
                    write_f64(out, value.0);
                    out.push('\n');
                }
                DataPointValue::Histogram(value) => value.render_into(&name, &labels, out),
            }
        }
        Ok(())
    }
}

impl HistogramValue {
    /// Emits the cumulative `<name>_bucket{le="..."}` series, the `le="+Inf"`
    /// bucket carrying the total count, and the `<name>_sum` / `<name>_count`
    /// samples.
    fn render_into(&self, name: &str, labels: &str, out: &mut String) {
        let mut cumulative: u64 = 0;
        for (idx, boundary) in self.boundaries.iter().enumerate() {
            cumulative =
                cumulative.saturating_add(self.bucket_counts.get(idx).copied().unwrap_or(0));
            out.push_str(name);
            out.push_str("_bucket{");
            if !labels.is_empty() {
                out.push_str(labels);
                out.push(',');
            }
            out.push_str("le=\"");
            write_f64(out, *boundary);
            let _ = writeln!(out, "\"}} {cumulative}");
        }

        out.push_str(name);
        out.push_str("_bucket{");
        if !labels.is_empty() {
            out.push_str(labels);
            out.push(',');
        }
        let _ = writeln!(out, "le=\"+Inf\"}} {}", self.count);

        out.push_str(name);
        out.push_str("_sum");
        if !labels.is_empty() {
            let _ = write!(out, "{{{labels}}}");
        }
        out.push(' ');
        write_f64(out, self.sum.0);
        out.push('\n');

        out.push_str(name);
        out.push_str("_count");
        if !labels.is_empty() {
            let _ = write!(out, "{{{labels}}}");
        }
        let _ = writeln!(out, " {}", self.count);
    }
}

/// Conversion from a bucket snapshot into a Prometheus-renderable sample
/// value. `metric` is the metric identity, used only for error messages.
pub trait IntoDataPointValue {
    fn into_data_point_value(self, metric: &NameIdentity) -> Result<DataPointValue, Error>;
}

impl<T> IntoDataPointValue for T
where
    T: atomic::Measure + ToPrimitive,
{
    fn into_data_point_value(self, _metric: &NameIdentity) -> Result<DataPointValue, Error> {
        Ok(DataPointValue::Number(to_number(&self)?))
    }
}

impl<T> IntoDataPointValue for histogram::Snapshot<T>
where
    T: atomic::Measure + ToPrimitive,
{
    fn into_data_point_value(self, metric: &NameIdentity) -> Result<DataPointValue, Error> {
        let boundaries = self
            .boundaries
            .iter()
            .map(|boundary| {
                let boundary = to_number(boundary)?.0;
                if !boundary.is_finite() {
                    return Err(Error::Prometheus(PrometheusError::NonFiniteBoundary(
                        metric.name.as_str().to_owned(),
                    )));
                }
                Ok(boundary)
            })
            .collect::<Result<Vec<f64>, Error>>()?;

        Ok(DataPointValue::Histogram(HistogramValue {
            boundaries,
            bucket_counts: self.bucket_counts,
            count: self.count,
            sum: to_number(&self.sum)?,
        }))
    }
}

const fn kind_str(kind: Kind) -> &'static str {
    match kind {
        Kind::Counter => "counter",
        Kind::Gauge => "gauge",
        Kind::Histogram => "histogram",
    }
}

/// Maps a metric name onto the Prometheus alphabet.
///
/// Characters outside `[a-zA-Z0-9_:]` become `_`, and a leading digit is
/// prefixed with `_`.
fn sanitize_metric_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 1);
    for (idx, c) in name.chars().enumerate() {
        if idx == 0 && c.is_ascii_digit() {
            out.push('_');
        }
        if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

/// Like [`sanitize_metric_name`], but `:` is reserved and rewritten too.
///
/// Label names only allow `[a-zA-Z0-9_]`. Names beginning with Prometheus's
/// reserved `__` prefix receive a `label` prefix.
fn sanitize_label_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 6);
    for (idx, c) in name.chars().enumerate() {
        if idx == 0 && c.is_ascii_digit() {
            out.push('_');
        }
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.starts_with("__") {
        out.insert_str(0, "label");
    }
    out
}

/// Renders `attrs` as a comma-separated `name="value"` list without the
/// surrounding braces.
///
/// Attributes without a value are skipped, non-string values are rendered
/// through their `Display` implementation.
///
/// An attribute with a value and an empty key fails with
/// [`PrometheusError::EmptyLabelName`].
///
/// Two attribute keys (distinct or repeated) sanitizing to the same label
/// name -- or to a name listed in `reserved` (labels the caller generates
/// itself, e.g. `le` for histogram bucket lines) -- would duplicate a label
/// within one sample, which is invalid exposition: this fails with
/// [`PrometheusError::LabelCollision`] instead. `metric` is the original
/// metric name, used only for the error message.
fn render_labels(
    metric: &str,
    attrs: &[KeyValue],
    reserved: &[&str],
) -> Result<String, PrometheusError> {
    let mut sorted: Vec<(String, &Value)> = Vec::with_capacity(attrs.len());
    for attr in attrs {
        let Some(value) = &attr.value else {
            continue;
        };
        if attr.key.as_str().is_empty() {
            return Err(PrometheusError::EmptyLabelName(metric.to_owned()));
        }

        let name = sanitize_label_name(attr.key.as_str());
        if reserved.contains(&name.as_str()) {
            return Err(PrometheusError::LabelCollision(name, metric.to_owned()));
        }
        sorted.push((name, value));
    }

    sorted.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    if let Some(collision) = sorted
        .windows(2)
        .find(|pair| pair[0].0.as_str() == pair[1].0.as_str())
    {
        return Err(PrometheusError::LabelCollision(
            collision[1].0.clone(),
            metric.to_owned(),
        ));
    }

    let mut labels = String::new();
    for (name, value) in sorted {
        if !labels.is_empty() {
            labels.push(',');
        }
        labels.push_str(&name);
        labels.push_str("=\"");
        match value {
            Value::String(value) => escape_label_value_into(value.as_str(), &mut labels),
            value => escape_label_value_into(&value.to_string(), &mut labels),
        }
        labels.push('"');
    }
    Ok(labels)
}

fn escape_help_into(text: &str, out: &mut String) {
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

fn escape_label_value_into(text: &str, out: &mut String) {
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

/// Writes a sample or `le` value with plain `f64` `Display` formatting;
/// non-finite values use the `+Inf`/`-Inf`/`NaN` spellings from the
/// exposition format.
fn write_f64(out: &mut String, value: f64) {
    if value.is_nan() {
        out.push_str("NaN");
    } else if value.is_infinite() {
        out.push_str(if value.is_sign_positive() {
            "+Inf"
        } else {
            "-Inf"
        });
    } else {
        let _ = write!(out, "{value}");
    }
}

fn to_number<T: ToPrimitive>(value: &T) -> Result<OrderedFloat<f64>, Error> {
    value.to_f64().map(OrderedFloat).ok_or(Error::ValueToF64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic::Record as _;
    use crate::metric::Histogram as HistogramMetric;
    use crate::model::Str;
    use crate::observe::MetricSource as _;
    use std::borrow::Cow;
    use testresult::TestResult;

    fn id(name: &'static str) -> NameIdentity {
        NameIdentity {
            name: Str::Cow(Cow::Borrowed(name)),
            description: Str::Cow(Cow::Borrowed("")),
            unit: Str::Cow(Cow::Borrowed("")),
        }
    }

    fn number_metric(identity: NameIdentity, kind: Kind, value: f64) -> Metric {
        Metric {
            id: Arc::new(identity),
            kind,
            points: vec![DataPoint {
                attrs: Arc::from([]),
                value: DataPointValue::Number(OrderedFloat(value)),
            }],
        }
    }

    #[test]
    fn sanitizes_names() {
        assert_eq!(
            sanitize_metric_name("http.server.request.duration"),
            "http_server_request_duration"
        );
        assert_eq!(sanitize_metric_name("9lives"), "_9lives");
        assert_eq!(sanitize_metric_name("app:requests"), "app:requests");
        assert_eq!(sanitize_label_name("app:route"), "app_route");
        assert_eq!(sanitize_label_name("http.route"), "http_route");
        assert_eq!(sanitize_label_name("2xx"), "_2xx");
        assert_eq!(sanitize_label_name("__name__"), "label__name__");
    }

    #[test]
    fn suffixes_counters_with_total() -> TestResult {
        let metric = number_metric(id("http.requests"), Kind::Counter, 1.0);
        assert_eq!(metric.family_name()?, "http_requests_total");

        let metric = number_metric(id("http.requests.total"), Kind::Counter, 1.0);
        assert_eq!(metric.family_name()?, "http_requests_total");

        let metric = number_metric(id("queue.depth"), Kind::Gauge, 1.0);
        assert_eq!(metric.family_name()?, "queue_depth");

        let metric = number_metric(id(""), Kind::Gauge, 1.0);
        assert!(matches!(
            metric.family_name(),
            Err(PrometheusError::EmptyMetricName),
        ));
        Ok(())
    }

    #[test]
    fn escapes_label_values() -> TestResult {
        let attrs: Arc<[KeyValue]> = Arc::from([
            KeyValue::new("path", "C:\\temp\n\"x\""),
            KeyValue::new("code", 200_i32),
            KeyValue::no_val("ignored"),
        ]);
        let metric = Metric {
            id: Arc::new(id("m")),
            kind: Kind::Gauge,
            points: vec![DataPoint {
                attrs,
                value: DataPointValue::Number(OrderedFloat(1.0)),
            }],
        };

        let mut out = String::new();
        metric.render_into(&mut out)?;
        assert!(
            out.contains(r#"m{code="200",path="C:\\temp\n\"x\""} 1"#),
            "{out}"
        );
        Ok(())
    }

    #[test]
    fn escapes_help_text() -> TestResult {
        let mut identity = id("m");
        identity.description = Str::Cow(Cow::Borrowed("line one\nback\\slash"));
        let metric = number_metric(identity, Kind::Gauge, 1.0);

        let mut out = String::new();
        metric.render_into(&mut out)?;
        assert!(out.contains("# HELP m line one\\nback\\\\slash\n"), "{out}");
        Ok(())
    }

    #[test]
    fn skips_empty_description() -> TestResult {
        let metric = number_metric(id("m"), Kind::Gauge, 1.0);

        let mut out = String::new();
        metric.render_into(&mut out)?;
        assert!(!out.contains("# HELP"), "{out}");
        assert!(out.contains("# TYPE m gauge\n"), "{out}");
        Ok(())
    }

    #[test]
    fn rejects_label_collisions_within_a_data_point() {
        let attrs: Arc<[KeyValue]> = Arc::from([
            KeyValue::new("http.route", "/a"),
            KeyValue::new("http/route", "/b"),
        ]);
        let metric = Metric {
            id: Arc::new(id("m")),
            kind: Kind::Gauge,
            points: vec![DataPoint {
                attrs,
                value: DataPointValue::Number(OrderedFloat(1.0)),
            }],
        };

        let mut out = "prefix".to_owned();
        let err = metric.render_into(&mut out).expect_err("label collision");
        assert!(
            matches!(
                &err,
                PrometheusError::LabelCollision(label, name)
                    if label == "http_route" && name == "m"
            ),
            "unexpected error: {err:?}",
        );
        assert_eq!(out, "prefix");
    }

    #[test]
    fn rejects_le_attribute_on_histogram_points() {
        let metric = Metric {
            id: Arc::new(id("m")),
            kind: Kind::Histogram,
            points: vec![DataPoint {
                attrs: Arc::from([KeyValue::new("le", "user")]),
                value: DataPointValue::Histogram(HistogramValue {
                    boundaries: vec![1.0],
                    bucket_counts: vec![1],
                    count: 1,
                    sum: OrderedFloat(0.5),
                }),
            }],
        };

        let mut out = String::new();
        let err = metric.render_into(&mut out).expect_err("le collision");
        assert!(
            matches!(
                &err,
                PrometheusError::LabelCollision(label, name)
                    if label == "le" && name == "m"
            ),
            "unexpected error: {err:?}",
        );
    }

    #[test]
    fn allows_le_attribute_on_scalar_points() -> TestResult {
        let metric = Metric {
            id: Arc::new(id("m")),
            kind: Kind::Gauge,
            points: vec![DataPoint {
                attrs: Arc::from([KeyValue::new("le", "user")]),
                value: DataPointValue::Number(OrderedFloat(1.0)),
            }],
        };

        let mut out = String::new();
        metric.render_into(&mut out)?;
        assert!(out.contains("m{le=\"user\"} 1\n"), "{out}");
        Ok(())
    }

    #[test]
    fn renders_histogram_with_cumulative_buckets() -> TestResult {
        let identity = id("request.duration");
        let histogram = HistogramMetric::<u64>::new(identity.clone(), [10, 20])?;
        histogram.add(5, &[]);
        histogram.add(25, &[]);

        let mut snapshot = None;
        histogram.buckets().visit_bucket(|entry| {
            snapshot = Some(entry.bucket.current());
            true
        });
        let value = snapshot
            .expect("recorded bucket")
            .into_data_point_value(&identity)?;
        let metric = Metric {
            id: Arc::new(identity),
            kind: Kind::Histogram,
            points: vec![DataPoint {
                attrs: Arc::from([]),
                value,
            }],
        };
        let mut out = String::new();
        metric.render_into(&mut out)?;

        assert!(out.contains("# TYPE request_duration histogram\n"), "{out}");
        assert!(
            out.contains("request_duration_bucket{le=\"10\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("request_duration_bucket{le=\"20\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("request_duration_bucket{le=\"+Inf\"} 2\n"),
            "{out}"
        );
        assert!(out.contains("request_duration_sum 30\n"), "{out}");
        assert!(out.contains("request_duration_count 2\n"), "{out}");
        Ok(())
    }

    /// Builds a histogram snapshot directly, bypassing `Histogram::new`
    /// validation, so non-finite boundaries can reach the conversion path.
    /// The finite-boundary path is covered by
    /// [`renders_histogram_with_cumulative_buckets`].
    #[test]
    fn rejects_non_finite_histogram_boundaries() {
        for boundaries in [&[f64::NAN][..], &[1.0, f64::INFINITY][..]] {
            let snapshot = histogram::Snapshot {
                boundaries: Arc::from(boundaries),
                bucket_counts: vec![1; boundaries.len()],
                count: 1,
                sum: 0.5,
                min: None,
                max: None,
            };
            let err = snapshot
                .into_data_point_value(&id("m"))
                .expect_err("non-finite boundary");
            assert!(
                matches!(&err, Error::Prometheus(PrometheusError::NonFiniteBoundary(name)) if name == "m"),
                "unexpected error: {err:?}",
            );
        }
    }

    #[test]
    fn converts_scalar_snapshots_to_numbers() -> TestResult {
        assert_eq!(
            7_u64.into_data_point_value(&id("m"))?,
            DataPointValue::Number(OrderedFloat(7.0)),
        );
        assert_eq!(
            (-3_i64).into_data_point_value(&id("m"))?,
            DataPointValue::Number(OrderedFloat(-3.0)),
        );
        Ok(())
    }
}
