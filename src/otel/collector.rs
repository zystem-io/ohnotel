use crate::collect;
use crate::collect::periodic;
use crate::{Error, atomic, dto};

use crate::observe::{MetricSource, Mode, SyncObserver};
use opentelemetry_proto::tonic::metrics::v1 as proto;
use std::hash::BuildHasher;

pub use periodic::Config;

type BoxedOtelProtoObserver = collect::BoxedDynObserver<proto::Metric>;

#[derive(Clone, Debug)]
pub struct Collector {
    inner: periodic::Collector<proto::Metric>,
}

impl Collector {
    /// - `sender` - sender implementation used to send accumulated metric batches.
    pub fn new<S>(sender: S, config: Config) -> Result<Self, Error>
    where
        S: periodic::Sender<proto::Metric>,
    {
        Ok(Self {
            inner: periodic::Collector::new(sender, config)?,
        })
    }

    pub fn add<Src, T, S, A>(&self, source: Src, mode: Mode) -> Result<Src, Error>
    where
        Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<proto::Metric, Error = Error>,
    {
        self.inner.add(source, mode)
    }

    pub fn add_observer<T, S, A>(&self, observer: SyncObserver<T, S, A>)
    where
        T: atomic::Measure + Send + Sync + 'static,
        S: BuildHasher + Clone + Send + Sync + 'static,
        A: atomic::Record<T>,
        dto::Series<A::Snapshot, S>: dto::IntoWire<proto::Metric, Error = Error>,
    {
        self.inner.add_observer(observer);
    }

    pub fn add_boxed_observer(&self, observer: BoxedOtelProtoObserver) {
        self.inner.add_boxed_observer(observer);
    }
}

impl<Src, T, S, A> collect::AddSource<Src> for Collector
where
    Src: MetricSource<Measure = T, Hasher = S, Cell = A>,
    T: atomic::Measure + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
    A: atomic::Record<T>,
    dto::Series<A::Snapshot, S>: dto::IntoWire<proto::Metric, Error = Error>,
{
    fn add_source(&self, source: &Src, mode: Mode) -> Result<(), Error> {
        collect::AddSource::add_source(&self.inner, source, mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::Counter;
    use crate::metric::tests::ID;
    use crate::otel::sender;
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
        MetricsService, MetricsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    };
    use std::time::Duration;
    use testresult::TestResult;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc as tmpsc;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Endpoint, Server};
    use tonic::{Request, Response, Status};

    #[tokio::test]
    async fn pushes_batches() -> TestResult {
        let (sender, mut rx) = tmpsc::unbounded_channel::<Vec<proto::Metric>>();

        let collector = Collector::new(
            sender,
            Config {
                poll_period: Duration::from_millis(10),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let counter = Counter::<u64>::new(ID);
        counter.add(3, &[]);
        let _ = collector.add(counter, Mode::Direct)?;

        let batch = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await?
            .expect("sender task stopped");

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].name, ID.name.to_string());
        Ok(())
    }

    #[derive(Clone)]
    struct StubMetricsService {
        tx: tmpsc::UnboundedSender<ExportMetricsServiceRequest>,
    }

    #[tonic::async_trait]
    impl MetricsService for StubMetricsService {
        async fn export(
            &self,
            request: Request<ExportMetricsServiceRequest>,
        ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
            let _ = self.tx.send(request.into_inner());
            Ok(Response::new(ExportMetricsServiceResponse::default()))
        }
    }

    async fn spawn_stub_server() -> TestResult<(
        String,
        tmpsc::UnboundedReceiver<ExportMetricsServiceRequest>,
        tokio::task::JoinHandle<()>,
    )> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (tx, rx) = tmpsc::unbounded_channel();
        let service = StubMetricsService { tx };
        let stream = TcpListenerStream::new(listener);

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(MetricsServiceServer::new(service))
                .serve_with_incoming(stream)
                .await
                .expect("stub server failed");
        });

        Ok((format!("http://{addr}"), rx, handle))
    }

    #[tokio::test]
    async fn pushes_batches_tonic() -> TestResult {
        let (uri, mut rx, server) = spawn_stub_server().await?;

        let channel = Endpoint::from_shared(uri)?.connect_lazy();

        let collector = Collector::new(
            sender::Tonic::new(channel),
            Config {
                poll_period: Duration::from_millis(10),
                export_period: Duration::from_millis(20),
                align_timestamps: false,
                batch_capacity: None,
            },
        )?;

        let counter = Counter::<u64>::new(ID);
        counter.add(42, &[]);
        let _ = collector.add(counter, Mode::Direct)?;

        let req = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await?
            .expect("server channel closed");

        let resource_metrics = &req.resource_metrics;
        assert_eq!(resource_metrics.len(), 1);
        assert_eq!(resource_metrics[0].scope_metrics.len(), 1);
        assert_eq!(resource_metrics[0].scope_metrics[0].metrics.len(), 1);
        assert_eq!(
            resource_metrics[0].scope_metrics[0].metrics[0].name,
            ID.name.to_string()
        );

        drop(collector);
        server.abort();
        Ok(())
    }
}
