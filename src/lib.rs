#![cfg_attr(
    test,
    allow(
        clippy::allow_attributes,
        clippy::dbg_macro,
        clippy::todo,
        clippy::unimplemented,
        clippy::print_stdout,
        clippy::print_stderr,
        unreachable_pub,
    )
)]

use std::time::SystemTimeError;
use thiserror::Error;

pub mod atomic;
pub(crate) mod bucket_map;
pub mod collect;
#[cfg(feature = "console")]
pub mod console;
pub mod dto;
pub(crate) mod lock;
pub mod metric;
pub mod model;
pub mod observe;
#[cfg(feature = "otel")]
pub mod otel;

#[derive(Debug, Error)]
pub enum Error {
    #[error("delta temporality is not supported for gauges")]
    DeltaForGauge,

    #[error("histogram boundaries must be non-empty and strictly increasing")]
    InvalidHistogramBoundaries,

    #[error("value out of f64 range")]
    ValueToF64,

    #[error("value out of i64 range")]
    ValueToI64,

    #[error("histogram boundary out of f64 range")]
    BoundaryToF64,

    #[error("timestamp is before the unix epoch")]
    TimeBeforeEpoch(#[from] SystemTimeError),

    #[error("timestamp nanos do not fit in u64")]
    TimeOverflowsU64,

    #[error("poll period must not be zero")]
    ZeroPollPeriod,

    #[error("export period must not be zero")]
    ZeroExportPeriod,

    #[error("poll period must not be greater than export period")]
    PollPeriodExceedsExportPeriod,

    #[error("Mode::Destructive cannot be used with more than one collector")]
    DestructiveWithMultipleCollectors,

    #[error("the same identity cannot be observed under more than one mode")]
    ModeMismatch,

    #[error("Mode::Destructive cannot be used more than once for the same identity")]
    DuplicateDestructive,

    #[cfg(feature = "otel")]
    #[error("OTLP transport error: {0}")]
    OtlpTransport(#[from] tonic::Status),

    #[cfg(feature = "otel")]
    #[error("OTLP collector rejected {rejected_data_points} data points: {error_message}")]
    OtlpPartialExport {
        rejected_data_points: i64,
        error_message: String,
    },

    #[error("custom error: {0}")]
    Custom(String),
}
