//! Native heap-allocation profiler for the blob submission path.

#[cfg(not(target_arch = "wasm32"))]
#[path = "submit_blob_allocations/native.rs"]
mod native;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> native::Result<()> {
    native::run()
}

// Keep `cargo clippy --all-targets --target wasm32-unknown-unknown` working.
#[cfg(target_arch = "wasm32")]
fn main() {}
