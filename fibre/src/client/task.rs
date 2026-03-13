//! Shared task spawning utilities.
//!
//! Provides a platform-agnostic way to spawn async tasks and track their
//! results in a [`FuturesUnordered`] collection. Works on both native and
//! wasm32 targets by using [`lumina_utils::executor::spawn`] instead of
//! `tokio::task::JoinSet` (which requires the `rt` feature).

use std::future::Future;

use futures::stream::FuturesUnordered;

use lumina_utils::cond_send::{BoxFuture, CondSend, into_boxed};

/// Spawn a task via [`lumina_utils::executor::spawn`] and track its result in a
/// [`FuturesUnordered`] collection via a oneshot channel.
///
/// This avoids depending on `tokio::task::JoinSet` (which requires the `rt`
/// feature) and works on both native and wasm32 targets.
pub(crate) fn spawn_task<T>(
    unordered: &mut FuturesUnordered<BoxFuture<'static, Option<T>>>,
    fut: impl Future<Output = T> + CondSend + 'static,
) where
    T: CondSend + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    lumina_utils::executor::spawn(async move {
        let _ = tx.send(fut.await);
    });
    unordered.push(into_boxed(async move { rx.await.ok() }));
}
