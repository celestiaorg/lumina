mod adapters;
mod application;
mod domain;
mod interface;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

#[tokio::main]
/// Entrypoint: delegates to interface layer so `main` stays thin and testable.
async fn main() -> Result<()> {
    init_tracing();
    interface::main::run().await
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_names(true)
        .try_init();
}
