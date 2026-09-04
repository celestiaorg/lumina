//! Latency of the client-side blob submission path, measured against Lumina's
//! development network (`docker compose -f ci/docker-compose.yml up -d --wait`).
//!
//! Two groups, each for a 1 MiB and a 7 MiB payload:
//!
//! - `broadcast_tx`: `GrpcClient::broadcast_tx` with a payload-sized byte
//!   string that is not a valid transaction. The node rejects it right after
//!   decoding, so the timing covers only the client's request encoding and the
//!   HTTP/2 transfer of the payload. Nothing enters the mempool.
//! - `broadcast_blobs`: `GrpcClient::broadcast_blobs`, the full client-side
//!   submission: blob validation, gas estimation, signing, transaction
//!   encoding and broadcast. It returns as soon as the node accepted the
//!   transaction into its mempool and does not wait for block inclusion.
//!   Iterations are paced with an untimed pause of at least one devnet block
//!   (500 ms) so every blob is included before the next one is sent; the
//!   reported time is only the `broadcast_blobs` call.
//!
//! Because of that pacing, `broadcast_blobs` runs take several times longer
//! than the configured measurement time.
//!
//! Endpoint and key default to the devnet values and can be overridden with
//! `LUMINA_ALLOC_GRPC_URL` and `LUMINA_ALLOC_PRIVATE_KEY`, the same variables
//! the `submit_blob_allocations` bench uses.
//!
//! To compare two revisions:
//!
//! ```sh
//! cargo bench -p celestia-client --bench submit_blob_latency -- --save-baseline main
//! # ...apply the change...
//! cargo bench -p celestia-client --bench submit_blob_latency -- --baseline main
//! ```
//!
//! A fast sanity pass (each case runs once) is `-- --test`.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use celestia_client::tx::TxConfig;
use celestia_grpc::GrpcClient;
use celestia_grpc::grpc::BroadcastMode;
use celestia_types::Blob;
use celestia_types::nmt::Namespace;
use celestia_types::state::AccAddress;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, measurement::WallTime,
};
use tokio::runtime::Runtime;

const DEFAULT_GRPC_URL: &str = "http://localhost:19090";
const DEFAULT_PRIVATE_KEY: &str =
    "393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839";
const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const WARMUP_BLOB_SIZE: usize = KIB;
/// Devnet block time is 500 ms; leave a margin so the mempool is empty again.
const BLOCK_DRAIN_PAUSE: Duration = Duration::from_millis(700);

const SIZES: &[(&str, usize)] = &[("1MiB", MIB), ("7MiB", 7 * MIB)];

fn grpc_url() -> String {
    env::var("LUMINA_ALLOC_GRPC_URL").unwrap_or_else(|_| DEFAULT_GRPC_URL.to_owned())
}

fn private_key() -> String {
    env::var("LUMINA_ALLOC_PRIVATE_KEY").unwrap_or_else(|_| DEFAULT_PRIVATE_KEY.to_owned())
}

fn build_client() -> GrpcClient {
    GrpcClient::builder()
        .url(grpc_url())
        .private_key_hex(&private_key())
        .build()
        .expect("gRPC client builds")
}

fn namespace() -> Namespace {
    Namespace::new_v0(b"latbench").expect("static namespace is valid")
}

fn make_blob(size: usize, address: AccAddress) -> Blob {
    Blob::new(namespace(), vec![0xA5; size], Some(address)).expect("blob is valid")
}

fn configure(group: &mut BenchmarkGroup<'_, WallTime>, size: usize) {
    group
        .throughput(Throughput::Bytes(size as u64))
        .sampling_mode(SamplingMode::Flat)
        .sample_size(10)
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10));
}

fn bench_broadcast_tx(c: &mut Criterion, runtime: &Runtime, client: &GrpcClient) {
    let mut group = c.benchmark_group("broadcast_tx");

    for &(name, size) in SIZES {
        configure(&mut group, size);

        group.bench_with_input(BenchmarkId::from_parameter(name), &size, |b, &size| {
            b.iter_custom(|iters| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        // Not a valid transaction: the node rejects it after the
                        // whole request has been received, which is the point.
                        let tx_bytes = vec![0xA5; size];
                        let start = Instant::now();
                        let result = client.broadcast_tx(tx_bytes, BroadcastMode::Sync).await;
                        elapsed += start.elapsed();
                        black_box(result).ok();
                    }
                    elapsed
                })
            })
        });
    }

    group.finish();
}

fn bench_broadcast_blobs(c: &mut Criterion, runtime: &Runtime, client: &GrpcClient) {
    let mut group = c.benchmark_group("broadcast_blobs");
    let address = client
        .get_account_address()
        .expect("benchmark client has a signer");

    for &(name, size) in SIZES {
        configure(&mut group, size);

        group.bench_with_input(BenchmarkId::from_parameter(name), &size, |b, &size| {
            b.iter_custom(|iters| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let blob = make_blob(size, address);
                        let start = Instant::now();
                        let submitted = client
                            .broadcast_blobs(&[blob], TxConfig::default())
                            .await
                            .expect("blob broadcast succeeds");
                        elapsed += start.elapsed();
                        black_box(submitted);
                        tokio::time::sleep(BLOCK_DRAIN_PAUSE).await;
                    }
                    elapsed
                })
            })
        });
    }

    group.finish();
}

/// Put glibc's allocator in a steady state before measuring; see the same
/// helper in `types/benches/blob/native.rs` for the reasoning.
fn pin_allocator_state() {
    let mut block = vec![0u8; 24 * MIB];
    black_box(&mut block);
    drop(block);
}

pub(super) fn main() {
    pin_allocator_state();
    let runtime = Runtime::new().expect("tokio runtime");
    // The transport is created eagerly and needs a runtime context.
    let _runtime_context = runtime.enter();
    let client = build_client();

    // Warm the connection, account and chain state caches outside the timings.
    runtime.block_on(async {
        let address = client
            .get_account_address()
            .expect("benchmark client has a signer");
        client
            .submit_blobs(&[make_blob(WARMUP_BLOB_SIZE, address)], TxConfig::default())
            .await
            .expect("warm-up submission succeeds");
    });

    let mut criterion = Criterion::default().configure_from_args();
    bench_broadcast_tx(&mut criterion, &runtime, &client);
    bench_broadcast_blobs(&mut criterion, &runtime, &client);
    criterion.final_summary();
}
