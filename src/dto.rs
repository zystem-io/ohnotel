use crate::model::NameIdentity;
use crate::observe::{Mode, SeriesMap};
use hashbrown::DefaultHashBuilder;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Implements wire serialization for [`Series`].
pub trait IntoWire<W> {
    type Error;

    /// Consumes `self` and produces a wire payload, or `Ok(None)` when there is
    /// nothing to emit (e.g. an empty series).
    ///
    /// `align` overrides per-snapshot timestamps with a synthetic clock derived
    /// from the series `start_time` and each snapshot's `seq_id` (see
    /// [`Snapshot::align_ts`]). `None` keeps the wall-clock `ts` recorded at
    /// observation time.
    ///
    /// ```ignore
    /// let wire = series.into_wire(Some(Duration::from_secs(10)))?;
    /// ```
    fn into_wire(self, align: Option<Duration>) -> Result<Option<W>, Self::Error>;
}

/// Represents the original metric type.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
}

/// A single observed value tagged with its wall-clock time and a monotonic `seq_id`, where `seq_id`
/// is the poll index since the series started.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Snapshot<T> {
    pub ts: SystemTime,
    pub seq_id: u64,
    pub value: T,
}

impl<T> Snapshot<T> {
    /// Reconstructs this snapshot's timestamp as `start_ts + poll_period * seq_id`.
    /// Should produce a uniform grid that hides scheduling jitter and can improve storage
    /// compression (i.e., using Delta codec).
    ///
    /// Falls back to the recorded `self.ts` when `poll_period` is `None` or when `seq_id` overflows
    /// `u32`.
    ///
    /// ```
    /// # use ohnotel::dto::Snapshot;
    /// # use std::time::{Duration, SystemTime};
    /// # let start_ts = SystemTime::now();
    /// # let snap = Snapshot { ts: start_ts, seq_id: 3, value: 0u64 };
    /// // seq_id = 3, 10s period => start_ts + 30s
    /// let t = snap.align_ts(start_ts, Some(Duration::from_secs(10)));
    /// ```
    pub fn align_ts(&self, start_ts: SystemTime, poll_period: Option<Duration>) -> SystemTime {
        let Some(poll_period) = poll_period else {
            return self.ts;
        };

        let Ok(seq_id) = u32::try_from(self.seq_id) else {
            return self.ts;
        };

        start_ts + poll_period * seq_id
    }
}

/// All snapshots for one metric since `start_time` by attribute set.
pub struct Series<V: Clone, S: Clone = DefaultHashBuilder> {
    pub start_time: SystemTime,
    pub id: Arc<NameIdentity>,
    pub series: SeriesMap<V, S>,
    pub observe_mode: Mode,
    pub kind: Kind,
}

impl<V: Clone, S: Clone> Clone for Series<V, S> {
    fn clone(&self) -> Self {
        Self {
            start_time: self.start_time,
            id: Arc::clone(&self.id),
            series: self.series.clone(),
            observe_mode: self.observe_mode,
            kind: self.kind,
        }
    }
}

impl<V: fmt::Debug + Clone, S: fmt::Debug + Clone> fmt::Debug for Series<V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Series")
            .field("start_time", &self.start_time)
            .field("id", &self.id)
            .field("series", &self.series)
            .field("observe_mode", &self.observe_mode)
            .finish()
    }
}
