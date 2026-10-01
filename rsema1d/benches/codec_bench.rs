mod support;

use std::{sync::Once, time::Duration};

use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId,
    Criterion, SamplingMode, Throughput,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
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
            SamplingProfile::SubMillisecond => (SamplingMode::Linear, 200, 5, 30),
            SamplingProfile::Milliseconds => (SamplingMode::Flat, 200, 5, 90),
            SamplingProfile::Subsecond => (SamplingMode::Flat, 120, 5, 90),
            SamplingProfile::Seconds => (SamplingMode::Flat, 100, 10, 120),
        };

        group
            .sampling_mode(sampling_mode)
            .sample_size(sample_size)
            .warm_up_time(Duration::from_secs(warm_up_secs))
            .measurement_time(Duration::from_secs(measurement_secs));
    }
}

const ENCODE_CONFIGS: &[(&str, usize, usize, usize)] = &[
    ("256B_k4_n4", 4, 4, 64),
    ("32KB_k64_n64", 64, 64, 512),
    ("128KB_k1024_n3072", 1024, 3072, 128),
    ("1MB_k1024_n1024", 1024, 1024, 1024),
    ("1MB_k1024_n3072", 1024, 3072, 1024),
    ("1MB_k4096_n12288", 4096, 12288, 256),
    ("8MB_k4096_n12288", 4096, 12288, 2048),
    ("32MB_k4096_n12288", 4096, 12288, 8192),
    ("128MB_k1024_n1024", 1024, 1024, 131072),
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
        let metadata = Once::new();
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            metadata.call_once(|| {
                support::print_case(
                    format_args!("encode/{name}"),
                    k,
                    n,
                    row_size,
                    rayon::current_num_threads(),
                );
            });
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
        let metadata = Once::new();
        group.bench_with_input(BenchmarkId::from_parameter(name), &params, |b, params| {
            metadata.call_once(|| {
                support::print_case(
                    format_args!("encode_in_place/{name}"),
                    k,
                    n,
                    row_size,
                    rayon::current_num_threads(),
                );
            });
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

        let metadata = Once::new();
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &commitment,
            |b, commitment| {
                metadata.call_once(|| {
                    support::print_case(
                        format_args!("proof_generation/{name}"),
                        k,
                        n,
                        row_size,
                        rayon::current_num_threads(),
                    );
                });
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
        assert!(rsema1d::codec::verify_proof(&proof, &commitment_bytes, &context).unwrap());

        let metadata = Once::new();
        group.bench_with_input(
            BenchmarkId::new("cached", name),
            &(&proof, &commitment_bytes, &context),
            |b, (proof, commitment_bytes, context)| {
                metadata.call_once(|| {
                    support::print_case(
                        format_args!("verification/cached/{name}"),
                        k,
                        n,
                        row_size,
                        rayon::current_num_threads(),
                    );
                });
                b.iter(|| {
                    assert!(rsema1d::codec::verify_proof(
                        black_box(proof),
                        black_box(commitment_bytes),
                        black_box(context),
                    )
                    .unwrap())
                });
            },
        );

        let proof = commitment.generate_standalone_proof(0).unwrap();
        assert!(rsema1d::verify_standalone(&proof, &commitment_bytes, &params).unwrap());
        let metadata = Once::new();
        group.bench_function(BenchmarkId::new("standalone", name), |b| {
            metadata.call_once(|| {
                support::print_case(
                    format_args!("verification/standalone/{name}"),
                    k,
                    n,
                    row_size,
                    rayon::current_num_threads(),
                );
            });
            b.iter(|| {
                assert!(rsema1d::verify_standalone(
                    black_box(&proof),
                    black_box(&commitment_bytes),
                    black_box(&params),
                )
                .unwrap());
            });
        });
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

        let metadata = Once::new();
        group.bench_with_input(BenchmarkId::from_parameter(name), &rlcs, |b, rlcs| {
            metadata.call_once(|| {
                support::print_case(
                    format_args!("verification_context/{name}"),
                    k,
                    n,
                    row_size,
                    rayon::current_num_threads(),
                );
            });
            b.iter(|| VerificationContext::new(black_box(rlcs), black_box(&params)).unwrap());
        });
    }

    group.finish();
}

fn bench_verification_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("verification_batch");
    SamplingProfile::Seconds.apply(&mut group);

    let (k, n, row_size) = (4096, 12288, 32768);
    let params = Parameters::new(k, n, row_size).unwrap();
    let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
    let extended = ExtendedData::generate(&data, &params).unwrap();
    let proofs: Vec<_> = (0..k)
        .map(|index| extended.generate_row_proof(index).unwrap())
        .collect();
    let context = VerificationContext::new(extended.rlc_original(), &params).unwrap();
    let commitment = extended.commitment();
    rsema1d::codec::verify_proof(&proofs[0], &commitment, &context).unwrap();

    group.throughput(Throughput::Bytes((k * row_size) as u64));
    let metadata = Once::new();
    group.bench_function("serial_k4096_32KiB", |b| {
        metadata.call_once(|| {
            support::print_case(
                "verification_batch/serial_k4096_32KiB",
                k,
                n,
                row_size,
                rayon::current_num_threads(),
            );
        });
        b.iter(|| {
            black_box(&proofs)
                .iter()
                .try_for_each(|proof| {
                    rsema1d::codec::verify_proof(proof, &commitment, &context).map(|_| ())
                })
                .unwrap()
        });
    });
    let metadata = Once::new();
    group.bench_function("rayon_148_rows_k4096_32KiB", |b| {
        metadata.call_once(|| {
            support::print_case(
                "verification_batch/rayon_148_rows_k4096_32KiB",
                k,
                n,
                row_size,
                rayon::current_num_threads(),
            );
        });
        b.iter(|| {
            black_box(&proofs)
                .par_chunks(148)
                .try_for_each(|proofs| {
                    proofs.iter().try_for_each(|proof| {
                        rsema1d::codec::verify_proof(proof, &commitment, &context).map(|_| ())
                    })
                })
                .unwrap()
        });
    });

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
        let metadata = Once::new();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            metadata.call_once(|| {
                support::print_case(
                    format_args!("reconstruct/{name}"),
                    k,
                    n,
                    row_size,
                    rayon::current_num_threads(),
                );
            });
            b.iter(|| {
                reconstruct(black_box(&rows), black_box(&indices), black_box(&params)).unwrap()
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = {
        support::print_environment(rayon::current_num_threads());
        Criterion::default().noise_threshold(0.03)
    };
    targets =
        bench_encode,
        bench_encode_in_place,
        bench_proof_generation,
        bench_verification,
        bench_verification_context,
        bench_verification_batch,
        bench_reconstruct
}
criterion_main!(benches);
