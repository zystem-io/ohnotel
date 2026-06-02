use crate::Error;
use crate::collect::periodic;
use crate::otel::dto::{Resource, Scope};
use log::{error, warn};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::common::v1 as pb;
use opentelemetry_proto::tonic::metrics::v1 as proto;
use opentelemetry_proto::tonic::resource::v1 as res;
use tonic::transport::Channel;

#[derive(Debug)]
pub struct Tonic {
    client: MetricsServiceClient<Channel>,
    resource: Option<res::Resource>,
    scope: Option<pb::InstrumentationScope>,
    schema_url: String,
    silent: bool,
}

impl Tonic {
    pub fn new(channel: Channel) -> Self {
        let resource = Resource::default();
        let scope = Scope::default();

        Self {
            client: MetricsServiceClient::new(channel),
            resource: Some(resource.into()),
            scope: Some(scope.into()),
            schema_url: String::new(),
            silent: false,
        }
    }

    #[must_use]
    pub fn with_resource(mut self, resource: Resource) -> Self {
        self.resource = Some(resource.into());
        self
    }

    #[must_use]
    pub fn with_scope(mut self, scope: Scope) -> Self {
        self.scope = Some(scope.into());
        self
    }

    #[must_use]
    pub fn with_schema_url(mut self, url: impl Into<String>) -> Self {
        self.schema_url = url.into();
        self
    }

    #[must_use]
    pub const fn with_silent(mut self, silent: bool) -> Self {
        self.silent = silent;
        self
    }

    fn build_request(&self, metrics: Vec<proto::Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![proto::ResourceMetrics {
                resource: self.resource.clone(),
                scope_metrics: vec![proto::ScopeMetrics {
                    scope: self.scope.clone(),
                    metrics,
                    schema_url: self.schema_url.clone(),
                }],
                schema_url: self.schema_url.clone(),
            }],
        }
    }

    async fn export(&mut self, metrics: Vec<proto::Metric>) -> Result<(), Error> {
        let request = self.build_request(metrics);
        let response = self.client.export(request).await?.into_inner();

        let Some(partial_success) = response.partial_success else {
            return Ok(());
        };

        // rejected_data_points > 0 means the collector dropped data.
        // rejected_data_points == 0 with a non-empty error_message is a warning.
        if partial_success.rejected_data_points != 0 {
            return Err(Error::OtlpPartialExport {
                rejected_data_points: partial_success.rejected_data_points,
                error_message: partial_success.error_message,
            });
        }

        if !partial_success.error_message.is_empty() && !self.silent {
            warn!(
                "OTLP collector partial-success warning: {}",
                partial_success.error_message
            );
        }

        Ok(())
    }
}

impl periodic::Sender<proto::Metric> for Tonic {
    async fn send(&mut self, metrics: Vec<proto::Metric>) {
        if let Err(err) = self.export(metrics).await {
            error!("failed to export metrics batch: {err}");
        }
    }
}
