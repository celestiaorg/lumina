//! Utilities providing platform abstraction used across lumina project

/// Conditional `Send` bound and boxed future abstractions for wasm32 compatibility.
pub mod cond_send;
/// async executor platform independent utilities
#[cfg(feature = "executor")]
pub mod executor;
/// JS object utils
#[cfg(all(target_arch = "wasm32", feature = "make-object"))]
mod object;
/// test utilities
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
/// platform independent timers
#[cfg(feature = "time")]
pub mod time;
/// platform independent cancellation token
#[cfg(feature = "token")]
pub mod token;
