//! Wall-time latency of the gRPC blob submission path against a local devnet.

#[cfg(not(target_arch = "wasm32"))]
#[path = "submit_blob_latency/native.rs"]
mod native;

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    native::main()
}

// Keep `cargo clippy --all-targets --target wasm32-unknown-unknown` working.
#[cfg(target_arch = "wasm32")]
fn main() {}
