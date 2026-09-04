//! CPU benchmarks for Fibre's upload and download paths using v0 parameters.
//! RSEMA verification and reconstruction are benchmarked in `rsema1d`.
//!
//! ```sh
//! cargo bench -p celestia-fibre --bench fibre_bench
//! cargo bench -p celestia-fibre --bench fibre_bench --target wasm32-unknown-unknown
//! cargo bench -p celestia-fibre --bench fibre_bench --target wasm32-unknown-unknown -- --include-ignored blob_new_32mb
//! ```

use std::num::NonZeroU64;
use std::time::{Duration, SystemTime};

use rand::Rng;
use rand::rngs::OsRng;

use celestia_fibre::transport::proto_conv;
use celestia_fibre::{
    BlobConfig, EncodedBlob, FibreClientConfig, Fraction, PaymentPromise, ValidatorInfo,
    ValidatorSet,
};
use celestia_proto::celestia::fibre::v1 as proto;
use celestia_types::nmt::Namespace;

fn blob_sizes() -> Vec<(&'static str, usize)> {
    vec![
        ("128KB", 128 << 10),
        ("1MB", 1 << 20),
        ("8MB", 8 << 20),
        ("32MB", 32 << 20),
        ("128MB", BlobConfig::v0().max_data_size),
    ]
}

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

fn signed_promise() -> (k256::ecdsa::SigningKey, PaymentPromise) {
    let signing_key = k256::ecdsa::SigningKey::random(&mut OsRng);
    let mut promise = PaymentPromise {
        chain_id: "private".into(),
        height: 42,
        namespace: Namespace::from_raw(&[0u8; 29]).unwrap(),
        upload_size: BlobConfig::v0().upload_size(1 << 20) as u32,
        blob_version: 0,
        commitment: [7u8; 32],
        creation_timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        signer_pubkey: *signing_key.verifying_key(),
        signature: None,
    };
    promise.sign(&signing_key).unwrap();
    (signing_key, promise)
}

fn download_response() -> proto::DownloadShardResponse {
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
    proto::DownloadShardResponse {
        shard: Some(proto::BlobShard { rows, rlcs }),
    }
}

#[cfg(not(target_arch = "wasm32"))]
use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, black_box, criterion_group,
};

#[cfg(not(target_arch = "wasm32"))]
const CHEAP_MEASUREMENT: Duration = Duration::from_secs(10);
#[cfg(not(target_arch = "wasm32"))]
const HEAVY_MEASUREMENT: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const HEAVY_WARM_UP: Duration = Duration::from_secs(5);

#[cfg(not(target_arch = "wasm32"))]
fn bench_blob_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_new");
    group.sample_size(10);
    group.measurement_time(HEAVY_MEASUREMENT);
    group.warm_up_time(HEAVY_WARM_UP);
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

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(not(target_arch = "wasm32"))]
fn bench_payment_promise(c: &mut Criterion) {
    let mut group = c.benchmark_group("payment_promise");
    group.measurement_time(CHEAP_MEASUREMENT);
    group.noise_threshold(0.03);

    let (signing_key, mut promise) = signed_promise();

    group.bench_function("sign_bytes", |b| b.iter(|| promise.sign_bytes().unwrap()));
    group.bench_function("sign", |b| b.iter(|| promise.sign(&signing_key).unwrap()));
    group.bench_function("validate", |b| b.iter(|| promise.validate().unwrap()));
    group.bench_function("hash", |b| b.iter(|| promise.hash().unwrap()));

    group.finish();
}

#[cfg(not(target_arch = "wasm32"))]
fn bench_signature_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("signature_set");
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

    group.bench_function("add_one", |b| {
        b.iter_batched(
            || set.new_signature_set(threshold, payload.clone()),
            |signature_set| signature_set.add(&validators[0], &signatures[0]).unwrap(),
            BatchSize::SmallInput,
        );
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

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(not(target_arch = "wasm32"))]
fn bench_parse_download_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_download_response");
    group.measurement_time(CHEAP_MEASUREMENT);
    group.noise_threshold(0.03);

    let shard = rows_per_shard();
    let response = download_response();

    group.bench_function(format!("shard_{shard}_rows_1MB"), |b| {
        b.iter_batched(
            || response.clone(),
            |response| proto_conv::parse_download_response(response).unwrap(),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

#[cfg(not(target_arch = "wasm32"))]
criterion_group!(
    benches,
    bench_blob_new,
    bench_blob_row_proofs,
    bench_payment_promise,
    bench_signature_set,
    bench_validator_assign,
    bench_parse_download_response
);

#[cfg(not(target_arch = "wasm32"))]
criterion::criterion_main!(benches);

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::hint::black_box;
    use std::time::Duration;

    use wasm_bindgen_test::{Criterion, Instant, wasm_bindgen_bench};

    use super::*;

    fn profile(
        c: &mut Criterion,
        sample_size: usize,
        warm_up_secs: u64,
        measurement_secs: u64,
        noise_threshold: f64,
    ) {
        *c = std::mem::take(c)
            .sample_size(sample_size)
            .warm_up_time(Duration::from_secs(warm_up_secs))
            .measurement_time(Duration::from_secs(measurement_secs))
            .noise_threshold(noise_threshold);
    }

    fn blob_size(name: &str) -> usize {
        blob_sizes()
            .into_iter()
            .find(|(n, _)| *n == name)
            .map(|(_, len)| len)
            .unwrap_or_else(|| panic!("unknown blob size {name}"))
    }

    fn blob_new_case(c: &mut Criterion, name: &str) {
        let len = blob_size(name);
        let (sample_size, warm_up_secs, measurement_secs) = if len >= 32 << 20 {
            (10, 3, 20)
        } else if len >= 4 << 20 {
            (10, 2, 10)
        } else {
            (20, 1, 10)
        };
        profile(c, sample_size, warm_up_secs, measurement_secs, 0.02);

        let data = generate_data(len);
        c.bench_function(&format!("blob_new/{name}"), |b| {
            b.iter(|| EncodedBlob::new(black_box(&data), BlobConfig::v0()).unwrap());
        });
    }

    fn blob_row_proofs_case(c: &mut Criterion, name: &str) {
        let blob = EncodedBlob::new(&generate_data(blob_size(name)), BlobConfig::v0()).unwrap();
        let shard = rows_per_shard();

        profile(c, 50, 1, 5, 0.03);
        c.bench_function(&format!("blob_row_proofs/shard_{shard}_rows/{name}"), |b| {
            b.iter(|| {
                for i in 0..shard {
                    black_box(blob.row(i).unwrap());
                }
            });
        });
    }

    fn signature_set_setup() -> (Vec<ValidatorInfo>, Vec<Vec<u8>>, Vec<u8>) {
        let (keys, validators) = make_validators(100);
        let payload = generate_data(200);
        let signatures = keys
            .iter()
            .map(|k| {
                use ed25519_dalek::Signer;
                k.sign(&payload).to_bytes().to_vec()
            })
            .collect();
        (validators, signatures, payload)
    }

    fn threshold() -> Fraction {
        Fraction::new(NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap())
    }

    fn assign_case(c: &mut Criterion, count: usize) {
        let cfg = BlobConfig::v0();
        let min_rows = rows_per_shard();
        let (_, validators) = make_validators(count);
        let set = ValidatorSet::new(validators, 1);

        profile(c, 50, 1, 5, 0.10);
        c.bench_function(&format!("validator_assign/assign/{count}"), |b| {
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
    }

    fn select_case(c: &mut Criterion, count: usize) {
        let cfg = BlobConfig::v0();
        let min_rows = rows_per_shard();
        let (_, validators) = make_validators(count);
        let set = ValidatorSet::new(validators, 1);

        profile(c, 50, 1, 5, 0.10);
        c.bench_function(&format!("validator_assign/select/{count}"), |b| {
            b.iter(|| set.select(cfg.original_rows, min_rows, liveness()));
        });
    }

    macro_rules! cases {
        ($case:ident: $( $( #[$attr:ident] )* $id:ident = $arg:literal ),* $(,)?) => {
            $(
                #[wasm_bindgen_bench]
                $( #[$attr] )*
                fn $id(c: &mut Criterion) {
                    $case(c, $arg);
                }
            )*
        };
    }

    cases!(blob_new_case:
        blob_new_128kb = "128KB",
        blob_new_1mb = "1MB",
        blob_new_8mb = "8MB",
        #[ignore] blob_new_32mb = "32MB",
        #[ignore] blob_new_128mb = "128MB",
    );

    cases!(blob_row_proofs_case:
        blob_row_proofs_1mb = "1MB",
        blob_row_proofs_8mb = "8MB",
    );

    #[wasm_bindgen_bench]
    fn payment_promise_sign_bytes(c: &mut Criterion) {
        let (_, promise) = signed_promise();
        profile(c, 50, 1, 5, 0.03);
        c.bench_function("payment_promise/sign_bytes", |b| {
            b.iter(|| promise.sign_bytes().unwrap());
        });
    }

    #[wasm_bindgen_bench]
    fn payment_promise_sign(c: &mut Criterion) {
        let (signing_key, mut promise) = signed_promise();
        profile(c, 50, 1, 5, 0.03);
        c.bench_function("payment_promise/sign", |b| {
            b.iter(|| promise.sign(&signing_key).unwrap());
        });
    }

    #[wasm_bindgen_bench]
    fn payment_promise_validate(c: &mut Criterion) {
        let (_, promise) = signed_promise();
        profile(c, 50, 1, 5, 0.03);
        c.bench_function("payment_promise/validate", |b| {
            b.iter(|| promise.validate().unwrap());
        });
    }

    #[wasm_bindgen_bench]
    fn payment_promise_hash(c: &mut Criterion) {
        let (_, promise) = signed_promise();
        profile(c, 50, 1, 5, 0.03);
        c.bench_function("payment_promise/hash", |b| {
            b.iter(|| promise.hash().unwrap());
        });
    }

    #[wasm_bindgen_bench]
    fn signature_set_add_one(c: &mut Criterion) {
        let (validators, signatures, payload) = signature_set_setup();
        let set = ValidatorSet::new(validators.clone(), 1);
        let threshold = threshold();

        profile(c, 50, 1, 5, 0.03);
        c.bench_function("signature_set/add_one", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let signature_set = set.new_signature_set(threshold, payload.clone());
                    let start = Instant::now();
                    let result = signature_set.add(&validators[0], &signatures[0]);
                    total += start.elapsed();
                    black_box(result.unwrap());
                }
                total
            });
        });
    }

    #[wasm_bindgen_bench]
    fn signature_set_collect_100(c: &mut Criterion) {
        let (validators, signatures, payload) = signature_set_setup();
        let set = ValidatorSet::new(validators.clone(), 1);
        let threshold = threshold();

        profile(c, 30, 2, 10, 0.03);
        c.bench_function("signature_set/collect_100", |b| {
            // Exclude setup to match native `iter_batched`.
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let signature_set = set.new_signature_set(threshold, payload.clone());
                    let start = Instant::now();
                    for (validator, signature) in validators.iter().zip(&signatures) {
                        signature_set.add(validator, signature).unwrap();
                    }
                    let result = signature_set.signatures().unwrap();
                    total += start.elapsed();
                    black_box(result);
                }
                total
            });
        });
    }

    cases!(assign_case:
        validator_assign_assign_10 = 10,
        validator_assign_assign_50 = 50,
        validator_assign_assign_100 = 100,
    );

    cases!(select_case:
        validator_assign_select_10 = 10,
        validator_assign_select_50 = 50,
        validator_assign_select_100 = 100,
    );

    #[wasm_bindgen_bench]
    fn parse_download_response_1mb(c: &mut Criterion) {
        let shard = rows_per_shard();
        let response = download_response();

        profile(c, 30, 1, 5, 0.03);
        c.bench_function(
            &format!("parse_download_response/shard_{shard}_rows_1MB"),
            |b| {
                // Exclude cloning to match native `iter_batched`.
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let input = response.clone();
                        let start = Instant::now();
                        black_box(proto_conv::parse_download_response(input).unwrap());
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {}
