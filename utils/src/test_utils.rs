use std::future::Future;
use std::time::Duration;

use crate::time::{sleep, timeout};

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::test as async_test;
#[cfg(target_arch = "wasm32")]
pub use wasm_bindgen_test::wasm_bindgen_test as async_test;

/// Polls `f` every 100 ms until it returns `Some`, for at most 10 seconds.
///
/// Meant for tests that observe state which becomes visible asynchronously,
/// e.g. querying a node right after a transaction was reported as committed:
/// the transaction index and the latest application state can lag behind
/// the confirmation for a moment.
///
/// # Panics
///
/// Panics with `what` in the message when the deadline passes.
pub async fn wait_until<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(value) = f().await {
                return value;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}
