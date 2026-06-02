use num_traits::ToPrimitive;
use opentelemetry_proto::tonic::common::v1 as pb;
use opentelemetry_proto::tonic::metrics::v1 as proto;
use opentelemetry_proto::tonic::resource::v1 as res;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::atomic::Measure;
use crate::atomic::histogram::Snapshot;
use crate::dto::{IntoWire, Kind, Series};
use crate::model::{KeyValue, Str, Value};
use crate::observe::Mode;

const OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";

impl From<&Value> for pb::any_value::Value {
    fn from(v: &Value) -> Self {
        match v {
            Value::String(s) => pb::any_value::Value::StringValue(s.to_string()),
            &Value::Bool(b) => pb::any_value::Value::BoolValue(b),
            &Value::Int(i) => pb::any_value::Value::IntValue(i),
            Value::Double(d) => pb::any_value::Value::DoubleValue(d.0),
            Value::ArrayAny(arr) => pb::any_value::Value::ArrayValue(pb::ArrayValue {
                values: arr
                    .iter()
                    .map(|v| pb::AnyValue {
                        value: Some(v.into()),
                    })
                    .collect(),
            }),
            Value::ArrayKv(kvs) => pb::any_value::Value::KvlistValue(pb::KeyValueList {
                values: kvs.iter().map(Into::into).collect(),
            }),
            Value::Bytes(b) => pb::any_value::Value::BytesValue(b.clone()),
        }
    }
}

impl From<&Value> for pb::AnyValue {
    fn from(v: &Value) -> Self {
        pb::AnyValue {
            value: Some(v.into()),
        }
    }
}

impl From<&KeyValue> for pb::KeyValue {
    fn from(kv: &KeyValue) -> Self {
        pb::KeyValue {
            key: kv.key.to_string(),
            value: kv.value.as_ref().map(Into::into),
            key_strindex: 0,
        }
    }
}

impl From<Resource> for res::Resource {
    fn from(resource: Resource) -> Self {
        res::Resource {
            attributes: map_attrs(&resource.into_attributes()),
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }
    }
}

impl From<Scope> for pb::InstrumentationScope {
    fn from(scope: Scope) -> Self {
        pb::InstrumentationScope {
            name: scope.name.to_string(),
            version: scope.version.to_string(),
            attributes: map_attrs(&scope.attributes),
            dropped_attributes_count: 0,
        }
    }
}

impl<T, S> IntoWire<proto::Metric> for Series<Snapshot<T>, S>
where
    T: Measure + ToPrimitive,
    S: Clone,
{
    type Error = crate::Error;

    fn into_wire(self, align: Option<Duration>) -> Result<Option<proto::Metric>, Self::Error> {
        if self.series.is_empty() {
            return Ok(None);
        }

        let temporality = match self.observe_mode {
            Mode::Direct => proto::AggregationTemporality::Cumulative,
            Mode::Delta | Mode::Destructive => proto::AggregationTemporality::Delta,
        };

        let start_time = self.start_time;
        let start_time_unix_nano = map_time(start_time)?;

        let mut data_points = vec![];

        for (attrs, snapshots) in self.series {
            let attrs = map_attrs(&attrs);
            for snapshot in snapshots {
                let time_unix_nano = map_time(snapshot.align_ts(start_time, align))?;

                let sum = snapshot.value.sum.to_f64().ok_or(Self::Error::ValueToF64)?;

                let mut explicit_bounds = Vec::with_capacity(snapshot.value.boundaries.len());
                for b in snapshot.value.boundaries.iter() {
                    explicit_bounds.push(b.to_f64().ok_or(Self::Error::BoundaryToF64)?);
                }

                let dp = proto::HistogramDataPoint {
                    attributes: attrs.clone(),
                    start_time_unix_nano,
                    time_unix_nano,
                    count: snapshot.value.count,
                    sum: Some(sum),
                    bucket_counts: snapshot.value.bucket_counts,
                    explicit_bounds,
                    exemplars: vec![],
                    flags: 0,
                    min: snapshot
                        .value
                        .min
                        .map(|v| v.to_f64().ok_or(Self::Error::ValueToF64))
                        .transpose()?,
                    max: snapshot
                        .value
                        .max
                        .map(|v| v.to_f64().ok_or(Self::Error::ValueToF64))
                        .transpose()?,
                };

                data_points.push(dp);
            }
        }

        if data_points.is_empty() {
            return Ok(None);
        }

        Ok(Some(proto::Metric {
            name: self.id.name.to_string(),
            description: self.id.description.to_string(),
            unit: self.id.unit.to_string(),
            metadata: vec![],
            data: Some(proto::metric::Data::Histogram(proto::Histogram {
                data_points,
                aggregation_temporality: temporality as i32,
            })),
        }))
    }
}

impl<T, S> IntoWire<proto::Metric> for Series<T, S>
where
    T: Measure + IntoNumberDataPointValue,
    S: Clone,
{
    type Error = crate::Error;

    fn into_wire(self, align: Option<Duration>) -> Result<Option<proto::Metric>, Self::Error> {
        if self.series.is_empty() {
            return Ok(None);
        }

        let temporality = match self.observe_mode {
            Mode::Direct => proto::AggregationTemporality::Cumulative,
            Mode::Delta | Mode::Destructive => proto::AggregationTemporality::Delta,
        };

        let start_time = self.start_time;
        let start_time_unix_nano = map_time(start_time)?;
        let mut data_points = vec![];

        for (attrs, snapshots) in self.series {
            let attrs = map_attrs(&attrs);

            for snapshot in snapshots {
                let time_unix_nano = map_time(snapshot.align_ts(start_time, align))?;

                let dp = proto::NumberDataPoint {
                    attributes: attrs.clone(),
                    start_time_unix_nano,
                    time_unix_nano,
                    exemplars: vec![],
                    flags: 0,
                    value: Some(snapshot.value.into_number_value()?),
                };

                data_points.push(dp);
            }
        }

        if data_points.is_empty() {
            return Ok(None);
        }

        let data = match self.kind {
            Kind::Counter => proto::metric::Data::Sum(proto::Sum {
                data_points,
                aggregation_temporality: temporality as i32,
                // `is_monotonic` describes the metric kind (Counter vs UpDownCounter),
                // not its temporality: a delta Counter is still monotonic.
                is_monotonic: true,
            }),
            Kind::Gauge => proto::metric::Data::Gauge(proto::Gauge { data_points }),
            Kind::Histogram => unreachable!("bug: can't be"),
        };

        Ok(Some(proto::Metric {
            name: self.id.name.to_string(),
            description: self.id.description.to_string(),
            unit: self.id.unit.to_string(),
            metadata: vec![],
            data: Some(data),
        }))
    }
}

pub trait IntoNumberDataPointValue {
    fn into_number_value(self) -> Result<proto::number_data_point::Value, crate::Error>;
}

impl IntoNumberDataPointValue for i64 {
    #[inline(always)]
    fn into_number_value(self) -> Result<proto::number_data_point::Value, crate::Error> {
        Ok(proto::number_data_point::Value::AsInt(self))
    }
}

impl IntoNumberDataPointValue for u64 {
    #[inline(always)]
    fn into_number_value(self) -> Result<proto::number_data_point::Value, crate::Error> {
        let value = i64::try_from(self).map_err(|_| crate::Error::ValueToI64)?;
        Ok(proto::number_data_point::Value::AsInt(value))
    }
}

impl IntoNumberDataPointValue for f64 {
    #[inline(always)]
    fn into_number_value(self) -> Result<proto::number_data_point::Value, crate::Error> {
        Ok(proto::number_data_point::Value::AsDouble(self))
    }
}

#[inline]
fn map_time(t: SystemTime) -> Result<u64, crate::Error> {
    let nanos = t.duration_since(UNIX_EPOCH)?.as_nanos();
    u64::try_from(nanos).map_err(|_| crate::Error::TimeOverflowsU64)
}

#[inline]
fn map_attrs(attrs: &[KeyValue]) -> Vec<pb::KeyValue> {
    attrs.iter().map(From::from).collect()
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Resource {
    pub service_name: Option<Str>,
    pub service_namespace: Option<Str>,
    pub service_version: Option<Str>,
    pub service_instance_id: Option<Str>,
    pub telemetry_sdk_name: Option<Str>,
    pub telemetry_sdk_language: Option<Str>,
    pub telemetry_sdk_version: Option<Str>,
    pub host_name: Option<Str>,
    pub host_arch: Option<Str>,
    pub os_type: Option<Str>,
    pub process_executable_name: Option<Str>,
    pub attributes: Vec<KeyValue>,
}

impl Default for Resource {
    fn default() -> Self {
        Self {
            service_name: env_var(OTEL_SERVICE_NAME).map(Str::from),
            service_namespace: None,
            service_version: None,
            service_instance_id: None,

            telemetry_sdk_name: Some(Str::from(env!("CARGO_PKG_NAME"))),
            telemetry_sdk_language: Some(Str::from("rust")),
            telemetry_sdk_version: Some(Str::from(env!("CARGO_PKG_VERSION"))),

            host_name: Some(host_name().into()),
            host_arch: Some(Str::from(host_arch())),
            os_type: Some(Str::from(os_type())),
            process_executable_name: exe_name(),

            attributes: vec![],
        }
    }
}

impl Resource {
    pub const fn empty() -> Self {
        Self {
            service_name: None,
            service_namespace: None,
            service_version: None,
            service_instance_id: None,
            telemetry_sdk_name: None,
            telemetry_sdk_language: None,
            telemetry_sdk_version: None,
            host_name: None,
            host_arch: None,
            os_type: None,
            process_executable_name: None,
            attributes: vec![],
        }
    }

    pub fn into_attributes(self) -> Vec<KeyValue> {
        let mut attrs = Vec::new();

        for kv in self.attributes {
            upsert_key_value(&mut attrs, kv);
        }

        macro_rules! push_attrs {
            (
                $( $key:expr => $val:expr ),* $(,)?
            ) => {
                $(
                    if let Some(value) = $val {
                        upsert_key_value(&mut attrs, KeyValue::new($key, value));
                    }
                )*
            };
        }

        push_attrs!(
            "service.name" => self.service_name,
            "service.namespace" => self.service_namespace,
            "service.version" => self.service_version,
            "service.instance.id" => self.service_instance_id,

            "telemetry.sdk.name" => self.telemetry_sdk_name,
            "telemetry.sdk.language" => self.telemetry_sdk_language,
            "telemetry.sdk.version" => self.telemetry_sdk_version,

            "host.name" => self.host_name,
            "host.arch" => self.host_arch,

            "os.type" => self.os_type,

            "process.executable.name" => self.process_executable_name,
        );

        attrs
    }
}

fn upsert_key_value(attrs: &mut Vec<KeyValue>, kv: KeyValue) {
    if kv.key.as_str().is_empty() {
        return;
    }

    if let Some(pos) = attrs
        .iter()
        .position(|existing| existing.key.as_str() == kv.key.as_str())
    {
        attrs[pos] = kv;
    } else {
        attrs.push(kv);
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn exe_name() -> Option<Str> {
    std::env::current_exe()
        .ok()?
        .file_name()
        .map(|n| n.to_string_lossy().into_owned().into())
}

fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "x86",
        "arm" => "arm32",
        "powerpc64" => "ppc64",
        other => other,
    }
}

fn os_type() -> &'static str {
    match std::env::consts::OS {
        "macos" | "ios" => "darwin",
        other => other,
    }
}

fn host_name() -> String {
    let mut buf = [0u8; 256];
    let _ignored = unsafe { libc::gethostname(buf.as_mut_ptr().cast::<libc::c_char>(), buf.len()) };
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

/// Identifies the instrumentation scope, mirroring the OTLP `InstrumentationScope`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Scope {
    pub name: Str,
    pub version: Str,
    pub attributes: Vec<KeyValue>,
}

impl Default for Scope {
    fn default() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME").into(),
            version: env!("CARGO_PKG_VERSION").into(),
            attributes: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sane_host() {
        let host_name = host_name();
        println!("{host_name}");
        assert!(!host_name.is_empty());
    }

    #[test]
    fn defaults_are_populated() {
        let attrs = Resource::default().into_attributes();

        let has = |key: &str| attrs.iter().any(|kv| kv.key.as_str() == key);
        assert!(has("telemetry.sdk.language"));
        assert!(has("telemetry.sdk.name"));
        assert!(has("telemetry.sdk.version"));
    }

    #[test]
    fn overrides_replace_without_erasing_defaults() {
        let resource = Resource {
            service_name: Some(Str::from("my-service")),
            attributes: vec![KeyValue::new("deployment.environment", "prod")],
            ..Resource::default()
        };
        let attrs = resource.into_attributes();

        let value_of = |key: &str| {
            attrs
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .and_then(|kv| kv.value.clone())
        };

        // The override wins, and only one `service.name` entry exists.
        assert_eq!(
            attrs
                .iter()
                .filter(|kv| kv.key.as_str() == "service.name")
                .count(),
            1
        );
        assert_eq!(value_of("service.name"), Some(Value::from("my-service")));

        // The free-form attribute is emitted.
        assert_eq!(
            value_of("deployment.environment"),
            Some(Value::from("prod"))
        );

        // Untouched defaults survive.
        assert!(
            attrs
                .iter()
                .any(|kv| kv.key.as_str() == "telemetry.sdk.language")
        );
    }
}
