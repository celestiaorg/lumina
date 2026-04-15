//! Conditional `Send` bound and boxed future abstractions.
//!
//! On native targets futures must be `Send` (for `tokio::spawn`).
//! On wasm32 there is only one thread, so `Send` is unnecessary and some
//! transports (e.g. `tonic-web-wasm-client`) return `!Send` futures.
//!
//! This module provides:
//! - `CondSend` — equals `Send` on native, no-op on wasm32.
//! - `BoxFuture` — a boxed future that is `Send` on native, `!Send` on wasm32.
//! - `into_boxed` — box a future into a `BoxFuture`.

use std::future::Future;
use std::pin::Pin;

/// A conditionally compiled trait indirection for `Send` bounds.
/// On native targets this requires `Send`.
#[cfg(not(target_arch = "wasm32"))]
pub trait CondSend: Send {}

/// A conditionally compiled trait indirection for `Send` bounds.
/// On wasm32 this is a no-op (no marker traits required).
#[cfg(target_arch = "wasm32")]
pub trait CondSend {}

#[cfg(not(target_arch = "wasm32"))]
impl<S: Send> CondSend for S {}

#[cfg(target_arch = "wasm32")]
impl<S> CondSend for S {}

/// A boxed future that is `Send` on native and `!Send` on wasm32.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed future that is `Send` on native and `!Send` on wasm32.
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Box a future into a `BoxFuture`.
#[cfg(not(target_arch = "wasm32"))]
pub fn into_boxed<F, T>(fut: F) -> BoxFuture<'static, T>
where
    F: Future<Output = T> + Send + 'static,
{
    Box::pin(fut)
}

/// Box a future into a [`BoxFuture`] (wasm32 variant).
#[cfg(target_arch = "wasm32")]
pub fn into_boxed<F, T>(fut: F) -> BoxFuture<'static, T>
where
    F: Future<Output = T> + 'static,
{
    Box::pin(fut)
}
