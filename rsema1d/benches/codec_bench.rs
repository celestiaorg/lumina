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

const DATA_SEED: u64 = 0x5eed_5eed_5eed_5eed;

fn generate_test_data(k: usize, row_size: usize) -> Vec<u8> {
    let mut rng = ChaCha8Rng::seed_from_u64(DATA_SEED);
    let mut data = vec![0u8; k * row_size];
    rng.fill_bytes(&mut data);
    data
}

#[derive(Clone, Copy)]
enum SamplingProfile {
    SubMillisecond,
    Milliseconds,
    Subsecond,
    Seconds,
}

impl SamplingProfile {
    fn for_bytes(bytes: usize) -> Self {
        const MIB: usize = 1024 * 1024;
        if bytes >= 64 * MIB {
            SamplingProfile::Seconds
        } else if bytes >= 4 * MIB {
            SamplingProfile::Subsecond
        } else {
            SamplingProfile::Milliseconds
        }
    }

    fn apply(self, group: &mut BenchmarkGroup<'_, WallTime>) {
        let (sampling_mode, sample_size, warm_up_secs, measurement_secs) = match self {
            SamplingProfile::SubMillisecond => (SamplingMode::Linear, 100, 3, 10),
            SamplingProfile::Milliseconds => (SamplingMode::Linear, 100, 3, 45),
            SamplingProfile::Subsecond => (SamplingMode::Flat, 60, 3, 45),
            SamplingProfile::Seconds => (SamplingMode::Flat, 30, 5, 75),
        };

        group
            .sampling_mode(sampling_mode)
            .sample_size(sample_size)
            .warm_up_time(Duration::from_secs(warm_up_secs))
            .measurement_time(Duration::from_secs(measurement_secs));
    }
}

const ENCODE_CONFIGS: &[(&str, usize, usize, usize)] = &[
    ("128KB_k1024_n3072", 1024, 3072, 128),
    ("1MB_k1024_n3072", 1024, 3072, 1024),
    ("1MB_k4096_n12288", 4096, 12288, 256),
    ("8MB_k4096_n12288", 4096, 12288, 2048),
    ("128MB_k4096_n12288", 4096, 12288, 32768),
    ("128MB_k8192_n24576", 8192, 24576, 16384),
];

const COMMON_CONFIGS: &[(&str, usize, usize, usize)] = &[
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

        SamplingProfile::for_bytes(total_bytes).apply(&mut group);
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| encode(black_box(data), black_box(&params)).unwrap());
        });
    }

    group.finish();
}

fn bench_encode_in_place(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_in_place");

    for &(name, k, n, row_size) in ENCODE_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;
        let mut prefilled = vec![0u8; (k + n) * row_size];
        prefilled[..k * row_size].copy_from_slice(data.as_row_major());
        let mut extended = Some(RowMatrix::with_shape(prefilled, k + n, row_size).unwrap());

        SamplingProfile::for_bytes(total_bytes).apply(&mut group);
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
    SamplingProfile::SubMillisecond.apply(&mut group);

    for &(name, k, n, row_size) in COMMON_CONFIGS {
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
    SamplingProfile::SubMillisecond.apply(&mut group);

    for &(name, k, n, row_size) in COMMON_CONFIGS {
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
    SamplingProfile::Milliseconds.apply(&mut group);

    // Context construction depends on k and n, not row_size.
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

    for &(name, k, n, row_size) in COMMON_CONFIGS {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let extended = ExtendedData::generate(&data, &params).unwrap();

        // Use parity rows only (indices k..2k) so reconstruction performs an
        // actual Reed-Solomon decode instead of copying originals through.
        let indices: Vec<usize> = (k..2 * k).collect();
        let rows: Vec<&[u8]> = indices.iter().map(|&i| extended.row(i).unwrap()).collect();
        let total_bytes = k * row_size;

        SamplingProfile::for_bytes(total_bytes).apply(&mut group);
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                reconstruct(black_box(&rows), black_box(&indices), black_box(&params)).unwrap()
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.03);
    targets =
        bench_encode,
        bench_encode_in_place,
        bench_proof_generation,
        bench_verification,
        bench_verification_context,
        bench_reconstruct
}
criterion_main!(benches);
