use std::time::{Duration, Instant, SystemTime};

/// Captures the monotonic + wall clock time. Wall clock timestamp we report is derived:
/// we add monotonic deltas, so timestamps should stay consistent if the system clock jumps.
/// (I think (tm)).
#[derive(Debug, Clone, Copy)]
pub struct CollectorTime {
    pub instant: Instant,
    pub system: SystemTime,
}

impl CollectorTime {
    pub fn now() -> Self {
        Self {
            instant: Instant::now(),
            system: SystemTime::now(),
        }
    }

    pub fn at(&self, at: Instant) -> Self {
        // TODO: if we notice that the time is broken at the moment of collection, should we restart
        //  and reset all observed entities instead?
        let system = if at >= self.instant {
            self.system + (at - self.instant)
        } else {
            self.system - (self.instant - at)
        };
        Self {
            instant: at,
            system,
        }
    }
}

pub struct GridTimer {
    start: Instant,
    period: Duration,
    deadline: Instant,
}

impl GridTimer {
    pub const fn new(start: Instant, period: Duration) -> Self {
        Self {
            start,
            period,
            deadline: start,
        }
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn due(&self, now: Instant) -> bool {
        now >= self.deadline
    }

    pub fn advance(&mut self, now: Instant) {
        self.deadline = self.next(now);
    }

    fn next(&self, now: Instant) -> Instant {
        let next_tick =
            now.saturating_duration_since(self.start).as_nanos() / self.period.as_nanos() + 1;

        let Some(offset) = self.mul(next_tick) else {
            return now.checked_add(self.period).unwrap_or(now);
        };

        self.start
            .checked_add(offset)
            .unwrap_or_else(|| now.checked_add(self.period).unwrap_or(now))
    }

    fn mul(&self, n: u128) -> Option<Duration> {
        let nanos = u128::from(self.period.subsec_nanos()) * n;

        let secs = u128::from(self.period.as_secs())
            .checked_mul(n)?
            .checked_add(nanos / 1_000_000_000)?;

        let secs = u64::try_from(secs).ok()?;

        Some(Duration::new(secs, (nanos % 1_000_000_000) as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: Duration = Duration::from_secs(1);

    fn timer() -> (Instant, GridTimer) {
        let start = Instant::now();
        (start, GridTimer::new(start, PERIOD))
    }

    #[test]
    fn due_on_start() {
        let (start, t) = timer();
        assert_eq!(t.deadline(), start);
        assert!(t.due(start));
    }

    #[test]
    fn due_threshold() {
        let (start, mut t) = timer();
        t.advance(start);
        let deadline = t.deadline();
        assert!(!t.due(deadline - Duration::from_nanos(1)));
        assert!(t.due(deadline));
        assert!(t.due(deadline + Duration::from_nanos(1)));
    }

    #[test]
    fn advance_from_start() {
        let (start, mut t) = timer();
        t.advance(start);
        assert_eq!(t.deadline(), start + PERIOD);
    }

    #[test]
    fn advance_mid_period() {
        let (start, mut t) = timer();
        t.advance(start + Duration::from_millis(250));
        assert_eq!(t.deadline(), start + PERIOD);
    }

    #[test]
    fn advance_on_grid_jumps_next() {
        let (start, mut t) = timer();
        t.advance(start + 2 * PERIOD);
        assert_eq!(t.deadline(), start + 3 * PERIOD);
    }

    #[test]
    fn skip_missed() {
        let (start, mut t) = timer();
        t.advance(start + Duration::from_millis(3_500));
        assert_eq!(t.deadline(), start + 4 * PERIOD);
    }

    #[test]
    fn advance_past() {
        let now = Instant::now();
        let mut t = GridTimer::new(now + 10 * PERIOD, PERIOD);
        t.advance(now);
        assert_eq!(t.deadline(), now + 11 * PERIOD);
    }

    #[test]
    fn monotonic() {
        let (start, mut t) = timer();
        let mut prev = start;
        for _ in 0..5 {
            t.advance(t.deadline());
            println!("{:?}, {:?}", prev, t.deadline);
            assert!(t.deadline() > prev);
            prev = t.deadline();
        }
    }
}
