mod cli;
mod evaluator;
mod metrics;
mod payload;
mod stats;

use anyhow::Context;
use clap::Parser;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::cli::Cli;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Built before the Tokio runtime: the exporter's blocking HTTP client and the
    // batch processor run on their own threads.
    let logger_provider = cli
        .otel_logs
        .then(|| -> anyhow::Result<SdkLoggerProvider> {
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .build()
                .context("building OTLP log exporter")?;
            Ok(SdkLoggerProvider::builder()
                .with_resource(
                    Resource::builder()
                        .with_service_name("fibre-evaluator")
                        .build(),
                )
                .with_batch_exporter(exporter)
                .build())
        })
        .transpose()?;

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(
            logger_provider
                .as_ref()
                .map(OpenTelemetryTracingBridge::new),
        )
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.tokio_worker_threads)
        .enable_all()
        .build()
        .context("building shared Tokio runtime")?;
    let result = runtime.block_on(evaluator::run(cli));

    // Flushes the final report before exit.
    if let Some(provider) = logger_provider
        && let Err(error) = provider.shutdown()
    {
        eprintln!("OTLP log exporter shutdown failed: {error}");
    }
    result
}
