//! Wall-time benchmarks for building and validating blobs.

#[cfg(not(target_arch = "wasm32"))]
#[path = "blob/native.rs"]
mod native;

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    native::main()
}

// Keep `cargo clippy --all-targets --target wasm32-unknown-unknown` working.
#[cfg(target_arch = "wasm32")]
fn main() {}
