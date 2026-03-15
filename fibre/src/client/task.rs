//! Shared task spawning utilities.
//!
//! Provides a platform-agnostic way to spawn async tasks and track their
//! results in a [`FuturesUnordered`] collection. Works on both native and
//! wasm32 targets by using [`lumina_utils::executor::spawn`] instead of
//! `tokio::task::JoinSet` (which requires the `rt` feature).

use std::future::Future;

use futures::stream::FuturesUnordered;

use lumina_utils::cond_send::{BoxFuture, CondSend, into_boxed};

/// Spawn a task via [`lumina_utils::executor::spawn`] and track its tagged
/// result in a [`FuturesUnordered`] collection via a oneshot channel.
///
/// The `tag` is returned alongside the task result, allowing callers to
/// identify which task produced each result (e.g. a validator index).
///
/// This avoids depending on `tokio::task::JoinSet` (which requires the `rt`
/// feature) and works on both native and wasm32 targets.
pub(crate) fn spawn_task<K, T>(
    unordered: &mut FuturesUnordered<BoxFuture<'static, (K, Option<T>)>>,
    tag: K,
    fut: impl Future<Output = T> + CondSend + 'static,
) where
    K: CondSend + 'static,
    T: CondSend + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    lumina_utils::executor::spawn(async move {
        let _ = tx.send(fut.await);
    });
    unordered.push(into_boxed(async move { (tag, rx.await.ok()) }));
}
