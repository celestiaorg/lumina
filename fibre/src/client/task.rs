//! Shared task spawning utilities.

use std::future::Future;

use futures::stream::FuturesUnordered;

use lumina_utils::cond_send::{BoxFuture, CondSend, into_boxed};

/// Spawn a task and track its tagged result in a [`FuturesUnordered`] collection.
///
/// The `tag` is returned alongside the task result, allowing callers to
/// identify which task produced each result (e.g. a validator index).
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
