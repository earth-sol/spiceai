/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::net::SocketAddr;
use std::sync::Arc;

use flight_client::Credentials;
use flight_client::FlightClient;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use otel_arrow::proto_to_sdk;
use otel_arrow::OtelToArrowConverter;
use runtime_auth::layer::grpc::make_interceptor;
use runtime_auth::GrpcAuth;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::health_server::HealthServer;

use crate::tls::TlsConfig;

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to serve: {source}"))]
    UnableToServe { source: tonic::transport::Error },

    #[snafu(display("Unable to configure TLS on the Flight server: {source}"))]
    UnableToConfigureTls { source: tonic::transport::Error },

    #[snafu(display("Unable to initialize telemetry exporter: {source}"))]
    UnableToInitializeTelemetryExporter { source: flight_client::Error },
}

pub struct Service {
    flight_client: FlightClient,
}

#[async_trait]
impl MetricsService for Service {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> std::result::Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut flight_client = self.flight_client.clone();
        let request = request.into_inner();
        for resource_metric in request.resource_metrics {
            let mut converter = OtelToArrowConverter::new(resource_metric.scope_metrics.len());

            let sdk_resource_metric = proto_to_sdk(resource_metric)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let batch = converter
                .convert(&sdk_resource_metric)
                .map_err(|e| Status::internal(e.to_string()))?;
            let name = sdk_resource_metric
                .resource
                .iter()
                .find(|attr| *attr.0 == opentelemetry::Key::from_static_str("name"))
                .map(|attr| attr.1.as_str());
            if let Some(name) = name {
                flight_client
                    .publish(&name, vec![batch])
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

async fn create_health_service() -> HealthServer<impl Health> {
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<MetricsServiceServer<Service>>()
        .await;
    health_service
}

pub async fn start(
    bind_address: SocketAddr,
    export_url: Option<Arc<str>>,
    export_credentials: Option<Credentials>,
    tls_config: Option<Arc<TlsConfig>>,
    grpc_auth: Option<Arc<dyn GrpcAuth + Send + Sync>>,
) -> Result<()> {
    let flight_client = match FlightClient::try_new(
        export_url.unwrap_or("https://telemetry.spiceai.io".into()),
        export_credentials.unwrap_or(Credentials::anonymous()),
        None,
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::trace!("Unable to initialize anonymous telemetry: {e}");
            return Err(Error::UnableToInitializeTelemetryExporter { source: e });
        }
    };
    let service = Service { flight_client };
    let svc = MetricsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip);

    tracing::info!("Spice Runtime OpenTelemetry listening on {bind_address}");

    let mut server = Server::builder();

    if let Some(ref tls_config) = tls_config {
        let server_tls_config = ServerTlsConfig::new().identity(Identity::from_pem(
            tls_config.cert.expose_secret(),
            tls_config.key.expose_secret(),
        ));
        server = server
            .tls_config(server_tls_config)
            .context(UnableToConfigureTlsSnafu)?;
    }

    server
        .layer(interceptor(make_interceptor(grpc_auth)))
        .add_service(create_health_service().await)
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToServeSnafu)?;

    Ok(())
}
