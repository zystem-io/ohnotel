pub mod collector;
pub mod dto;
pub mod sender;

pub use opentelemetry_proto::tonic::common::v1 as pb;
pub use opentelemetry_proto::tonic::metrics::v1 as proto;
pub use opentelemetry_proto::tonic::resource::v1 as res;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::Counter;
    use crate::metric::tests::ID;
    use crate::observe::{DynObserver, Mode, SyncObserver};
    use std::time::SystemTime;

    #[test]
    fn monotonic_counters() {
        let c = Counter::<u64>::new(ID);
        let mut obs: Box<dyn DynObserver<proto::Metric, crate::Error>> =
            Box::new(SyncObserver::new(&c, Mode::Delta).expect("delta mode is supported"));
        c.add(7, &[]);
        obs.observe(SystemTime::now());
        let metric = obs
            .export(None)
            .expect("convert")
            .expect("metric was produced");
        match metric.data.expect("data") {
            proto::metric::Data::Sum(sum) => {
                assert_eq!(
                    sum.aggregation_temporality,
                    proto::AggregationTemporality::Delta as i32
                );
                assert!(
                    sum.is_monotonic,
                    "a counter must be monotonic regardless of temporality"
                );
            }
            other => panic!("expected sum, got {:?}", other),
        }
    }
}
