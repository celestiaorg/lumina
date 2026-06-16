#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub mod block_ranges;
pub mod blockstore;
mod daser;
pub mod events;
pub mod network;
pub mod node;
mod p2p;
mod peer_tracker;
mod pruner;
pub mod store;
mod syncer;
#[cfg(any(test, feature = "test-utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test-utils")))]
pub mod test_utils;
mod utils;

#[cfg(all(target_arch = "wasm32", test))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

/// Install a tracing subscriber once for the (native) unit-test binary.
///
/// Runs automatically before any test via `#[ctor]`. Logs are written to a file (never
/// to the test output) so they can be inspected or uploaded as a CI artifact. The path
/// comes from `LUMINA_TEST_LOG` (default: `lumina-test.log`); the file is appended to.
/// Defaults to `lumina_node=debug`; override with `RUST_LOG=lumina_node=trace`.
#[cfg(all(test, not(target_arch = "wasm32")))]
#[ctor::ctor]
fn init_unit_test_logs() {
    use std::sync::Mutex;

    use tracing_subscriber::EnvFilter;

    let path = std::env::var("LUMINA_TEST_LOG").unwrap_or_else(|_| "lumina-test.log".to_string());
    let Ok(file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    else {
        return;
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("lumina_node=debug"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(Mutex::new(file))
        .try_init();
}

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[doc(inline)]
pub use crate::node::{Node, NodeBuilder, NodeError, Result};
