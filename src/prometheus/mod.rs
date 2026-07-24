//! Pull-based Prometheus exporter.
//!
//! Unlike the push-based backends, this module runs no periodic export loop:
//! collection is driven by scrapes. [`Collector::render`](collector::Collector::render)
//! observes every registered source on demand and returns the current
//! cumulative state in the Prometheus text exposition format (version 0.0.4).
//!
//! The library deliberately ships no HTTP server. Serve the rendered string
//! from the transport of your choice with the content type
//! `text/plain; version=0.0.4; charset=utf-8`:
//!
//! ```ignore
//! let body = collector.render()?;
//! respond(200, "text/plain; version=0.0.4; charset=utf-8", body);
//! ```

pub mod collector;
pub mod dto;

use thiserror::Error;

/// Errors produced while mapping metrics onto the Prometheus exposition
/// format.
#[derive(Debug, Error)]
pub enum Error {
    #[error("prometheus metric name must not be empty")]
    EmptyMetricName,

    #[error("metric '{0}': prometheus label name must not be empty")]
    EmptyLabelName(String),

    #[error("distinct metrics collide on prometheus family name '{0}'")]
    FamilyCollision(String),

    #[error("metric '{1}': attribute keys collide on prometheus label name '{0}'")]
    LabelCollision(String, String),

    #[error("metric '{1}': duplicate time series with labels '{0}'")]
    DuplicateSeries(String, String),

    #[error("metric '{0}': non-finite prometheus histogram boundary")]
    NonFiniteBoundary(String),
}
