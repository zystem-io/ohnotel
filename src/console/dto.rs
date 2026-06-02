use crate::Error;
use crate::atomic;
use crate::atomic::histogram;
use crate::dto::{IntoWire, Kind, Series};
use crate::model::{KeyValue, NameIdentity, Temporality};
use crate::observe::Mode;
use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONTINUATION_INDENT: usize = 2 + 12 + 2;

/// Console-renderable metric payload.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Metric {
    pub id: Arc<NameIdentity>,
    pub kind: Kind,
    pub temporality: Temporality,
    pub data_points: Vec<DataPoint>,
}

/// One console-renderable metric data point.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DataPoint {
    pub attributes: Vec<KeyValue>,
    pub start_time: SystemTime,
    pub time: SystemTime,
    pub value: DataPointValue,
}

/// Console-renderable data point value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DataPointValue {
    Number(OrderedFloat<f64>),
    Histogram(Histogram),
}

/// Console-renderable histogram value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Histogram {
    pub count: u64,
    pub sum: OrderedFloat<f64>,
    pub buckets: Vec<Bucket>,
    pub min: Option<OrderedFloat<f64>>,
    pub max: Option<OrderedFloat<f64>>,
}

/// Console-renderable histogram bucket.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Bucket {
    pub upper_bound: Option<OrderedFloat<f64>>,
    pub count: u64,
}

impl Metric {
    pub fn render(&self) -> String {
        let mut out = self.header();
        if !self.id.unit.as_str().is_empty() {
            let _ = write!(out, " unit={}", self.id.unit);
        }
        if !self.id.description.as_str().is_empty() {
            let _ = write!(out, " description={:?}", self.id.description.as_str());
        }

        let points = self.sorted_points();
        let widths = self.histogram_column_widths();
        let mut last_time: Option<SystemTime> = None;

        for dp in &points {
            let time_col = if last_time == Some(dp.time) {
                " ".repeat(CONTINUATION_INDENT)
            } else {
                last_time = Some(dp.time);
                format!("  {}  ", dp.render_time())
            };
            let attrs_col = if dp.attributes.is_empty() {
                String::new()
            } else {
                format!("{} ", dp.render_attrs())
            };
            let value_col = match (&dp.value, widths.as_deref()) {
                (DataPointValue::Histogram(h), w) => h.render(w),
                (DataPointValue::Number(n), _) => n.0.to_string(),
            };
            let _ = write!(out, "\n{time_col}{attrs_col}{value_col}");
        }
        out
    }

    fn header(&self) -> String {
        match self.kind {
            Kind::Gauge => format!("{:?} {}", self.kind, self.id.name),
            Kind::Counter | Kind::Histogram => {
                format!("{:?}{:?} {}", self.temporality, self.kind, self.id.name)
            }
        }
    }

    fn sorted_points(&self) -> Vec<&DataPoint> {
        let mut points: Vec<&DataPoint> = self.data_points.iter().collect();
        points.sort_by(|a, b| {
            a.time
                .cmp(&b.time)
                .then_with(|| a.attributes.cmp(&b.attributes))
        });
        points
    }

    fn histogram_column_widths(&self) -> Option<Vec<usize>> {
        let first = self.data_points.first()?.as_histogram()?;
        let mut widths = vec![0_usize; first.buckets.len()];
        for dp in &self.data_points {
            let h = dp.as_histogram()?;
            if h.buckets.len() != widths.len() {
                return None;
            }
            for (i, b) in h.buckets.iter().enumerate() {
                widths[i] = widths[i].max(b.render().chars().count());
            }
        }
        Some(widths)
    }
}

impl DataPoint {
    fn render_time(&self) -> String {
        let Ok(d) = self.time.duration_since(UNIX_EPOCH) else {
            return "??:??:??.???".to_owned();
        };
        let sec_of_day = d.as_secs() % 86_400;
        let h = sec_of_day / 3_600;
        let m = (sec_of_day / 60) % 60;
        let s = sec_of_day % 60;
        let ms = d.subsec_millis();
        format!("{h:02}:{m:02}:{s:02}.{ms:03}")
    }

    fn render_attrs(&self) -> String {
        let mut s = String::from("{");
        for (i, kv) in self.attributes.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            let _ = write!(s, "{kv}");
        }
        s.push('}');
        s
    }

    const fn as_histogram(&self) -> Option<&Histogram> {
        match &self.value {
            DataPointValue::Histogram(h) => Some(h),
            DataPointValue::Number(_) => None,
        }
    }
}

impl Histogram {
    fn render(&self, widths: Option<&[usize]>) -> String {
        let mut s = String::from("[");
        for (i, b) in self.buckets.iter().enumerate() {
            if i > 0 {
                s.push(' ');
            }
            let cell = b.render();
            s.push_str(&cell);
            if let Some(w) = widths.and_then(|w| w.get(i).copied()) {
                for _ in 0..w.saturating_sub(cell.chars().count()) {
                    s.push(' ');
                }
            }
        }
        let _ = write!(s, "]  count={} sum={}", self.count, self.sum);
        if let Some(min) = &self.min {
            let _ = write!(s, " min={min}");
        }
        if let Some(max) = &self.max {
            let _ = write!(s, " max={max}");
        }
        s
    }
}

impl Bucket {
    fn render(&self) -> String {
        match &self.upper_bound {
            Some(ub) => format!("{ub}:{}", self.count),
            None => format!("\u{221e}:{}", self.count),
        }
    }
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

impl fmt::Display for DataPointValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(v) => write!(f, "{}", v.0),
            Self::Histogram(h) => f.write_str(&h.render(None)),
        }
    }
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render(None))
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

impl<T, S> IntoWire<Metric> for Series<histogram::Snapshot<T>, S>
where
    T: atomic::Measure + ToPrimitive,
    S: Clone,
{
    type Error = Error;

    fn into_wire(self, align: Option<Duration>) -> Result<Option<Metric>, Error> {
        if self.series.is_empty() {
            return Ok(None);
        }

        let Series {
            start_time,
            id,
            series,
            observe_mode,
            kind,
        } = self;
        let temporality = map_temporality(observe_mode);
        let mut data_points = Vec::new();

        for (attributes, snapshots) in series {
            for snapshot in snapshots {
                let time = snapshot.align_ts(start_time, align);
                let mut buckets = Vec::with_capacity(snapshot.value.bucket_counts.len());
                for (idx, count) in snapshot.value.bucket_counts.into_iter().enumerate() {
                    let upper_bound = snapshot
                        .value
                        .boundaries
                        .get(idx)
                        .map(|value| to_number(value))
                        .transpose()?;
                    buckets.push(Bucket { upper_bound, count });
                }

                data_points.push(DataPoint {
                    attributes: attributes.to_vec(),
                    start_time,
                    time,
                    value: DataPointValue::Histogram(Histogram {
                        count: snapshot.value.count,
                        sum: to_number(&snapshot.value.sum)?,
                        buckets,
                        min: snapshot.value.min.as_ref().map(to_number).transpose()?,
                        max: snapshot.value.max.as_ref().map(to_number).transpose()?,
                    }),
                });
            }
        }

        if data_points.is_empty() {
            return Ok(None);
        }

        Ok(Some(Metric {
            id,
            kind,
            temporality,
            data_points,
        }))
    }
}

impl<T, S> IntoWire<Metric> for Series<T, S>
where
    T: atomic::Measure + ToPrimitive,
    S: Clone,
{
    type Error = Error;

    fn into_wire(self, align: Option<Duration>) -> Result<Option<Metric>, Error> {
        if self.series.is_empty() {
            return Ok(None);
        }

        let Series {
            start_time,
            id,
            series,
            observe_mode,
            kind,
        } = self;
        let temporality = map_temporality(observe_mode);
        let mut data_points = Vec::new();

        for (attributes, snapshots) in series {
            for snapshot in snapshots {
                data_points.push(DataPoint {
                    attributes: attributes.to_vec(),
                    start_time,
                    time: snapshot.align_ts(start_time, align),
                    value: DataPointValue::Number(to_number(&snapshot.value)?),
                });
            }
        }

        if data_points.is_empty() {
            return Ok(None);
        }

        Ok(Some(Metric {
            id,
            kind,
            temporality,
            data_points,
        }))
    }
}

const fn map_temporality(mode: Mode) -> Temporality {
    match mode {
        Mode::Direct => Temporality::Cumulative,
        Mode::Delta | Mode::Destructive => Temporality::Delta,
    }
}

fn to_number<T: ToPrimitive>(value: &T) -> Result<OrderedFloat<f64>, Error> {
    value.to_f64().map(OrderedFloat).ok_or(Error::ValueToF64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::tests::ID;
    use crate::metric::{Counter, Histogram as HistogramMetric};
    use crate::model::KeyValue;
    use crate::observe::{DynObserver, Mode, SyncObserver};
    use std::time::SystemTime;

    #[test]
    fn renders_counter() {
        let counter = Counter::<u64>::new(ID);
        counter.add(7, &[KeyValue::new("route", "/")]);

        let mut observer: Box<dyn DynObserver<Metric, Error>> =
            Box::new(SyncObserver::new(&counter, Mode::Direct).expect("counter observer"));
        observer.observe(SystemTime::now());

        let metric = observer.export(None).expect("export").expect("metric");
        let rendered = metric.to_string();

        assert!(rendered.contains(&format!("CumulativeCounter {}", ID.name)));
        assert!(rendered.contains("{route=\"/\"} 7"));
    }

    #[test]
    fn renders_histogram() {
        let histogram = HistogramMetric::<u64>::new(ID, [10, 20]).expect("histogram");
        histogram.add(5, &[]);
        histogram.add(25, &[]);

        let mut observer: Box<dyn DynObserver<Metric, Error>> =
            Box::new(SyncObserver::new(&histogram, Mode::Direct).expect("histogram observer"));
        observer.observe(SystemTime::now());

        let metric = observer.export(None).expect("export").expect("metric");
        let rendered = metric.to_string();

        assert!(rendered.contains("count=2 sum=30"));
        assert!(rendered.contains("10:1"));
        assert!(rendered.contains("20:0"));
        assert!(rendered.contains("\u{221e}:1"));
    }
}
