mod adapters;
mod application;
mod domain;
mod interface;

use anyhow::Result;

#[tokio::main]
/// Entrypoint: delegates to interface layer so `main` stays thin and testable.
async fn main() -> Result<()> {
    interface::main::run().await
}
