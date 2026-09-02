use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId,
    Criterion, SamplingMode, Throughput,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rsema1d::{
    encode, encode_in_place, reconstruct, ExtendedData, Parameters, RowMatrix, VerificationContext,
};

/// Fixed seed so every run (and every config) encodes the same bytes. The RLC
/// path skips zero symbols, so data content affects timing slightly; keeping it
/// stable removes that as a source of run-to-run variance.
const DATA_SEED: u64 = 0x5eed_5eed_5eed_5eed;

fn generate_test_data(k: usize, row_size: usize) -> Vec<u8> {
    let mut rng = ChaCha8Rng::seed_from_u64(DATA_SEED);
    let mut data = vec![0u8; k * row_size];
    rng.fill_bytes(&mut data);
    data
}

/// Sampling plan for one benchmark, chosen by how long a single iteration
/// takes. Criterion's defaults (100 samples, 5 s, linear sampling) only work
/// well for sub-millisecond routines; for the multi-millisecond and
/// multi-second routines here they either emit "unable to complete 100
/// samples" warnings or collapse to one iteration per sample.
///
/// * `Fast`  — iteration well under 1 ms (proof generation, verification).
///   Linear sampling, 100 samples, 10 s. Plenty of iterations per sample, so
///   scheduler jitter averages out.
/// * `Small` — iteration in the 1–15 ms range (≤1 MB encodes, context build).
///   Linear sampling needs 5050 iterations for 100 samples; 45 s covers that
///   for iterations up to ~9 ms and degrades gracefully above.
/// * `Medium` — iteration in the ~50–200 ms range (8 MB encodes/reconstruct).
///   Flat sampling so every sample gets the same iteration count; 60 samples
///   over 45 s gives several iterations per sample.
/// * `Large` — iteration of one to a few seconds (128 MB encodes/reconstruct).
///   Flat sampling, 30 samples over 75 s (one to three iterations per sample),
///   with a longer warm-up so the allocator and rayon pool are steady before
///   timing.
#[derive(Clone, Copy)]
enum Tier {
    Fast,
    Small,
    Medium,
    Large,
}

impl Tier {
    /// Pick a tier for the data-parallel routines (`encode`, `encode_in_place`,
    /// `reconstruct`) from the number of bytes they touch per iteration.
    fn for_bytes(bytes: usize) -> Self {
        const MIB: usize = 1024 * 1024;
        if bytes >= 64 * MIB {
            Tier::Large
        } else if bytes >= 4 * MIB {
            Tier::Medium
        } else {
            Tier::Small
        }
    }

    fn apply(self, group: &mut BenchmarkGroup<'_, WallTime>) {
        match self {
            Tier::Fast => {
                group
                    .sampling_mode(SamplingMode::Linear)
                    .sample_size(100)
                    .warm_up_time(Duration::from_secs(3))
                    .measurement_time(Duration::from_secs(10));
            }
            Tier::Small => {
                group
                    .sampling_mode(SamplingMode::Linear)
                    .sample_size(100)
                    .warm_up_time(Duration::from_secs(3))
                    .measurement_time(Duration::from_secs(45));
            }
            Tier::Medium => {
                group
                    .sampling_mode(SamplingMode::Flat)
                    .sample_size(60)
                    .warm_up_time(Duration::from_secs(3))
                    .measurement_time(Duration::from_secs(45));
            }
            Tier::Large => {
                group
                    .sampling_mode(SamplingMode::Flat)
                    .sample_size(30)
                    .warm_up_time(Duration::from_secs(5))
                    .measurement_time(Duration::from_secs(75));
            }
        }
    }
}

// Test configurations: (data_size_name, k, n, row_size)
const ENCODE_CONFIGS: &[(&str, usize, usize, usize)] = &[
    ("128KB_k1024_n3072", 1024, 3072, 128),
    ("1MB_k1024_n3072", 1024, 3072, 1024),
    ("1MB_k4096_n12288", 4096, 12288, 256),
    ("8MB_k4096_n12288", 4096, 12288, 2048),
    ("128MB_k4096_n12288", 4096, 12288, 32768),
    ("128MB_k8192_n24576", 8192, 24576, 16384),
];

const PROOF_CONFIGS: &[(&str, usize, usize, usize)] = &[
    ("1MB_k1024_n3072", 1024, 3072, 1024),
    ("8MB_k4096_n12288", 4096, 12288, 2048),
    ("128MB_k4096_n12288", 4096, 12288, 32768),
];

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for &(name, k, n, row_size) in ENCODE_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;

        // `encode` allocates and frees a fresh (k+n)*row_size buffer every
        // iteration; that allocation (and the page faults on first touch) is
        // part of what this benchmark measures. See `encode_in_place` for the
        // buffer-reusing variant.
        Tier::for_bytes(total_bytes).apply(&mut group);
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| encode(black_box(data), black_box(&params)).unwrap());
        });
    }

    group.finish();
}

fn bench_encode_in_place(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_in_place");

    // Match the `encode` benchmark matrix exactly.
    for &(name, k, n, row_size) in ENCODE_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;
        let mut prefilled = vec![0u8; (k + n) * row_size];
        prefilled[..k * row_size].copy_from_slice(data.as_row_major());
        let mut extended = Some(RowMatrix::with_shape(prefilled, k + n, row_size).unwrap());

        Tier::for_bytes(total_bytes).apply(&mut group);
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &params, |b, params| {
            b.iter(|| {
                let buffer = extended.take().expect("buffer must be available");
                let (ext_data, _commitment, _rlc_orig) =
                    encode_in_place(black_box(buffer), black_box(params)).unwrap();
                let rsema1d::ExtendedData { all_rows, .. } = ext_data;
                extended = Some(all_rows);
            });
        });
    }

    group.finish();
}

fn bench_proof_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("proof_generation");
    Tier::Fast.apply(&mut group);

    for &(name, k, n, row_size) in PROOF_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let commitment = ExtendedData::generate(&data, &params).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &commitment,
            |b, commitment| {
                b.iter(|| commitment.generate_row_proof(black_box(0)).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_verification(c: &mut Criterion) {
    let mut group = c.benchmark_group("verification");
    Tier::Fast.apply(&mut group);

    for &(name, k, n, row_size) in PROOF_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let commitment = ExtendedData::generate(&data, &params).unwrap();
        let proof = commitment.generate_row_proof(0).unwrap();
        let context = VerificationContext::new(commitment.rlc_original(), &params).unwrap();
        let commitment_bytes = commitment.commitment();

        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &(&proof, &commitment_bytes, &context),
            |b, (proof, commitment_bytes, context)| {
                b.iter(|| {
                    rsema1d::codec::verify_proof(
                        black_box(proof),
                        black_box(commitment_bytes),
                        black_box(context),
                    )
                    .unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_verification_context(c: &mut Criterion) {
    let mut group = c.benchmark_group("verification_context");
    Tier::Small.apply(&mut group);

    // The context build (RS extension of the RLC vector + Merkle build +
    // row-derived coefficients) depends only on (k, n), not on row_size.
    let configs = [
        ("k1024_n3072", 1024, 3072, 1024),
        ("k4096_n12288", 4096, 12288, 256),
        ("k8192_n24576", 8192, 24576, 128),
    ];

    for (name, k, n, row_size) in configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let commitment = ExtendedData::generate(&data, &params).unwrap();
        let rlcs = commitment.rlc_original().to_vec();

        group.bench_with_input(BenchmarkId::from_parameter(name), &rlcs, |b, rlcs| {
            b.iter(|| VerificationContext::new(black_box(rlcs), black_box(&params)).unwrap());
        });
    }

    group.finish();
}

fn bench_reconstruct(c: &mut Criterion) {
    let mut group = c.benchmark_group("reconstruct");

    for &(name, k, n, row_size) in PROOF_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let extended = ExtendedData::generate(&data, &params).unwrap();

        // Use parity rows only (indices k..2k) so reconstruction performs an
        // actual Reed-Solomon decode instead of copying originals through.
        let indices: Vec<usize> = (k..2 * k).collect();
        let rows: Vec<&[u8]> = indices.iter().map(|&i| extended.row(i).unwrap()).collect();
        let total_bytes = k * row_size;

        Tier::for_bytes(total_bytes).apply(&mut group);
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                reconstruct(black_box(&rows), black_box(&indices), black_box(&params)).unwrap()
            });
        });
    }

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        // Everything here fans out over rayon (32 threads on a 16-core box),
        // so run-to-run wobble of 1-2% is normal. Only flag a change once it
        // clears that band; the raw estimates are still reported.
        .noise_threshold(0.03)
        // Let `-- --sample-size N` / `--measurement-time S` still override the
        // *base* config. Per-tier settings above take precedence where set.
        .configure_from_args()
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_encode,
        bench_encode_in_place,
        bench_proof_generation,
        bench_verification,
        bench_verification_context,
        bench_reconstruct
}
criterion_main!(benches);
