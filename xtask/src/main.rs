mod adapters;
mod application;
mod domain;
mod interface;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    interface::main::run().await
}
