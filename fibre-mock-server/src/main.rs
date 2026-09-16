//! Mock validator network binary for fibre load testing.

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fibre_mock_server::cli::run().await
}

// Placeholder to allow compilation
#[cfg(target_arch = "wasm32")]
fn main() {}
