use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use rsema1d::{ExtendedData, Parameters, VerificationContext};

fn generate_test_data(k: usize, row_size: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    (0..k)
        .map(|_| (0..row_size).map(|_| rng.gen::<u8>()).collect())
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    // Test configurations: (data_size_name, k, n, row_size)
    let configs = vec![
        ("128KB_k1024_n1024", 1024, 1024, 128),
        ("1MB_k1024_n1024", 1024, 1024, 1024),
        ("1MB_k4096_n4096", 4096, 4096, 256),
        ("8MB_k4096_n4096", 4096, 4096, 2048),
        ("128MB_k4096_n4096", 4096, 4096, 32768),
        ("128MB_k8192_n8192", 8192, 8192, 16384),
    ];

    for (name, k, n, row_size) in configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = generate_test_data(k, row_size);
        let total_bytes = k * row_size;

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| ExtendedData::generate(black_box(data), black_box(&params)).unwrap());
        });
    }

    group.finish();
}

fn bench_proof_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("proof_generation");

    let configs = vec![
        ("1MB_k1024", 1024, 1024, 1024),
        ("8MB_k4096", 4096, 4096, 2048),
        ("128MB_k4096", 4096, 4096, 32768),
    ];

    for (name, k, n, row_size) in configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = generate_test_data(k, row_size);
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

    let configs = vec![
        ("1MB_k1024", 1024, 1024, 1024),
        ("8MB_k4096", 4096, 4096, 2048),
        ("128MB_k4096", 4096, 4096, 32768),
    ];

    for (name, k, n, row_size) in configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = generate_test_data(k, row_size);
        let commitment = ExtendedData::generate(&data, &params).unwrap();
        let proof = commitment.generate_row_proof(0).unwrap();
        let context = VerificationContext::new(&commitment.rlc_original(), &params).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &(&proof, &commitment.commitment(), &context),
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

criterion_group!(
    benches,
    bench_encode,
    bench_proof_generation,
    bench_verification
);
criterion_main!(benches);
