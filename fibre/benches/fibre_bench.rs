//! Benchmarks for the CPU-bound (network-free) paths of the fibre client.
//!
//! Upload path: blob encoding, per-row proof generation, payment promise
//! signing, validator signature verification, deterministic shard assignment.
//! Download path: wire decoding. Shard verification and reconstruction are
//! benchmarked in `rsema1d/benches/codec_bench.rs` (groups
//! `verification_context`, `verification`, and `reconstruct`), where those
//! code paths are public API; the fibre layer only adds thin bookkeeping on
//! top of them.
//!
//! These benchmarks use the production v0 protocol parameters (K=4096,
//! N=12288) and only the crate's public API.
//!
//! Measurement windows are deliberately long (10s for cheap groups, 30s for
//! heavy ones) so results are stable enough for regression detection. To
//! compare two revisions, save a baseline before the change and compare after:
//!
//! ```sh
//! cargo bench -p celestia-fibre --bench fibre_bench -- --save-baseline main
//! # ...apply the change...
//! cargo bench -p celestia-fibre --bench fibre_bench -- --baseline main
//! ```
//!
//! A fast sanity pass (each case runs once) is `-- --test`. Blob encoding is
//! partially parallel via rayon; set `RAYON_NUM_THREADS=1` for
//! single-threaded numbers.
//!
//! ## Reading criterion's change verdicts
//!
//! Per-group noise thresholds below are tuned from A/A runs (same binary, no
//! code change) on a 32-core desktop: differences within the threshold are
//! reported as noise rather than a change. Residual run-to-run variance comes
//! from ASLR-dependent memory layout (dominant for sub-microsecond benches),
//! turbo/thermal clocking, and background load. For tighter comparisons:
//! run with ASLR disabled (`setarch -R cargo bench ...`), and before trusting
//! a verdict do an A/A pass (re-run the baseline once) to see the machine's
//! current noise floor.

use std::num::NonZeroU64;
use std::time::{Duration, SystemTime};

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, black_box, criterion_group,
    criterion_main,
};
use rand::Rng;
use rand::rngs::OsRng;

use celestia_fibre::transport::proto_conv;
use celestia_fibre::{
    BlobConfig, EncodedBlob, FibreClientConfig, Fraction, PaymentPromise, ValidatorInfo,
    ValidatorSet,
};
use celestia_proto::celestia::fibre::v1 as proto;
use celestia_types::nmt::Namespace;

/// Blob payload sizes exercised by the encode/decode benchmarks. The largest
/// entry is the protocol maximum (128 MiB minus the blob header).
fn blob_sizes() -> Vec<(&'static str, usize)> {
    vec![
        ("128KB", 128 << 10),
        ("1MB", 1 << 20),
        ("8MB", 8 << 20),
        ("32MB", 32 << 20),
        ("128MB", BlobConfig::v0().max_data_size),
    ]
}

/// Measurement window for cheap (sub-millisecond) benchmarks.
const CHEAP_MEASUREMENT: Duration = Duration::from_secs(10);
/// Measurement window for heavy benchmarks (blob encoding).
const HEAVY_MEASUREMENT: Duration = Duration::from_secs(30);
const HEAVY_WARM_UP: Duration = Duration::from_secs(5);

/// Rows per shard, matching the protocol's min_rows_per_validator (148 for v0).
fn rows_per_shard() -> usize {
    FibreClientConfig::default().min_rows_per_validator
}

fn liveness() -> Fraction {
    Fraction::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(3).unwrap())
}

fn generate_data(len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; len];
    rng.fill(&mut data[..]);
    data
}

fn make_validators(count: usize) -> (Vec<ed25519_dalek::SigningKey>, Vec<ValidatorInfo>) {
    (0..count)
        .map(|i| {
            let mut seed = [0u8; 32];
            seed[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            let key = ed25519_dalek::SigningKey::from_bytes(&seed);
            let mut address = [0u8; 20];
            address[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            let info = ValidatorInfo {
                address,
                pubkey: key.verifying_key(),
                voting_power: (i as i64 % 10) + 1,
            };
            (key, info)
        })
        .unzip()
}

/// Upload path: full client-side encode (header write + rsema1d encode_in_place
/// over the (K+N) x row_size matrix). The dominant CPU sink of `upload()`.
fn bench_blob_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_new");
    group.sample_size(10);
    group.measurement_time(HEAVY_MEASUREMENT);
    group.warm_up_time(HEAVY_WARM_UP);
    // Flat sampling: iteration counts don't grow across samples, so the 128MB
    // case fits the measurement window without criterion warning about it.
    group.sampling_mode(SamplingMode::Flat);
    group.noise_threshold(0.02);

    for (name, len) in blob_sizes() {
        let data = generate_data(len);
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| EncodedBlob::new(black_box(data), BlobConfig::v0()).unwrap());
        });
    }

    group.finish();
}

/// Upload path: per-row inclusion proof generation, called once per row per
/// validator when building upload shards.
///
/// Measured per shard (148 rows), not per single row: a lone ~100ns row proof
/// is dominated by where that row's pages landed in memory, which varies
/// between processes (ASLR) and made single-row numbers swing >15% run to
/// run. Averaging over a shard's worth of rows removes that; divide by 148
/// for the per-row cost.
fn bench_blob_row_proofs(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_row_proofs");
    group.measurement_time(CHEAP_MEASUREMENT);
    group.noise_threshold(0.03);

    for &(name, len) in &[("1MB", 1 << 20), ("8MB", 8 << 20)] {
        let blob = EncodedBlob::new(&generate_data(len), BlobConfig::v0()).unwrap();

        let shard = rows_per_shard();
        group.bench_function(BenchmarkId::new(format!("shard_{shard}_rows"), name), |b| {
            b.iter(|| {
                for i in 0..shard {
                    black_box(blob.row(i).unwrap());
                }
            });
        });
    }

    group.finish();
}

/// Upload path: payment promise canonical serialization, secp256k1
/// sign/verify, and hashing.
fn bench_payment_promise(c: &mut Criterion) {
    let mut group = c.benchmark_group("payment_promise");
    group.measurement_time(CHEAP_MEASUREMENT);
    group.noise_threshold(0.03);

    let signing_key = k256::ecdsa::SigningKey::random(&mut OsRng);
    let mut promise = PaymentPromise {
        chain_id: "private".into(),
        height: 42,
        namespace: Namespace::from_raw(&[0u8; 29]).unwrap(),
        upload_size: BlobConfig::v0().upload_size(1 << 20) as u32,
        blob_version: 0,
        commitment: [7u8; 32],
        creation_timestamp: SystemTime::now(),
        signer_pubkey: *signing_key.verifying_key(),
        signature: None,
    };
    promise.sign(&signing_key).unwrap();

    group.bench_function("sign_bytes", |b| b.iter(|| promise.sign_bytes().unwrap()));
    group.bench_function("sign", |b| b.iter(|| promise.sign(&signing_key).unwrap()));
    group.bench_function("validate", |b| b.iter(|| promise.validate().unwrap()));
    group.bench_function("hash", |b| b.iter(|| promise.hash().unwrap()));

    group.finish();
}

/// Upload path: ed25519 signature verification of validator responses, and
/// collecting a full validator set's worth of signatures.
fn bench_signature_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("signature_set");
    // collect_100 runs ~3ms/iter; 100 samples need more than the cheap window.
    group.measurement_time(Duration::from_secs(20));
    group.noise_threshold(0.03);

    let (keys, validators) = make_validators(100);
    let payload = generate_data(200);
    let signatures: Vec<Vec<u8>> = keys
        .iter()
        .map(|k| {
            use ed25519_dalek::Signer;
            k.sign(&payload).to_bytes().to_vec()
        })
        .collect();

    let set = ValidatorSet::new(validators.clone(), 1);
    let threshold = Fraction::new(NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap());

    let signature_set = set.new_signature_set(threshold, payload.clone());
    group.bench_function("add_one", |b| {
        b.iter(|| signature_set.add(&validators[0], &signatures[0]).unwrap());
    });

    group.bench_function("collect_100", |b| {
        b.iter_batched(
            || set.new_signature_set(threshold, payload.clone()),
            |signature_set| {
                for (validator, signature) in validators.iter().zip(&signatures) {
                    signature_set.add(validator, signature).unwrap();
                }
                signature_set.signatures().unwrap()
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Deterministic stake-weighted shard assignment (upload) and validator
/// selection (download), scaling over validator count.
///
/// The `select` benches are sub-microsecond, allocation- and RNG-heavy, and
/// show up to ~10% ASLR-driven run-to-run variance on an unquiesced machine;
/// hence the wide noise threshold. Treat only large `select` deltas (or
/// deltas that survive an A/A re-run) as real. `assign` is stable (<2%).
fn bench_validator_assign(c: &mut Criterion) {
    let mut group = c.benchmark_group("validator_assign");
    group.measurement_time(CHEAP_MEASUREMENT);
    group.noise_threshold(0.10);

    let cfg = BlobConfig::v0();
    let min_rows = rows_per_shard();

    for count in [10, 50, 100] {
        let (_, validators) = make_validators(count);
        let set = ValidatorSet::new(validators, 1);

        group.bench_with_input(BenchmarkId::new("assign", count), &set, |b, set| {
            b.iter(|| {
                set.assign(
                    black_box([42u8; 32]),
                    cfg.total_rows(),
                    cfg.original_rows,
                    min_rows,
                    liveness(),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("select", count), &set, |b, set| {
            b.iter(|| set.select(cfg.original_rows, min_rows, liveness()));
        });
    }

    group.finish();
}

/// Download path: decoding a validator's wire response (proof hash conversion
/// + RLC vector parse), run once per validator response.
fn bench_parse_download_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_download_response");
    group.measurement_time(CHEAP_MEASUREMENT);
    group.noise_threshold(0.03);

    let shard = rows_per_shard();
    let blob = EncodedBlob::new(&generate_data(1 << 20), BlobConfig::v0()).unwrap();

    let rows: Vec<proto::BlobRow> = (0..shard)
        .map(|i| {
            let proof = blob.row(i).unwrap();
            proto::BlobRow {
                index: proof.index as u32,
                data: proof.row,
                proof: proof.row_proof.iter().map(|h| h.to_vec()).collect(),
            }
        })
        .collect();
    let rlcs: Vec<u8> = blob
        .rlc_coeffs()
        .iter()
        .flat_map(|rlc| rlc.to_bytes())
        .collect();
    let response = proto::DownloadShardResponse {
        shard: Some(proto::BlobShard { rows, rlcs }),
    };

    group.bench_function(format!("shard_{shard}_rows_1MB"), |b| {
        b.iter_batched(
            || response.clone(),
            |response| proto_conv::parse_download_response(response).unwrap(),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_blob_new,
    bench_blob_row_proofs,
    bench_payment_promise,
    bench_signature_set,
    bench_validator_assign,
    bench_parse_download_response
);
criterion_main!(benches);
