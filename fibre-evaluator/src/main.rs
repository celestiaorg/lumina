mod cli;
mod evaluator;
mod metrics;
mod payload;
mod stats;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.tokio_worker_threads)
        .enable_all()
        .build()
        .context("building shared Tokio runtime")?
        .block_on(evaluator::run(cli))
}
