use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use rsema1d::{encode, encode_in_place, ExtendedData, Parameters, RowMatrix, VerificationContext};

fn generate_test_data(k: usize, row_size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; k * row_size];
    for b in &mut data {
        *b = rng.gen::<u8>();
    }
    data
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
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;

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
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;
        let mut prefilled = vec![0u8; (k + n) * row_size];
        prefilled[..k * row_size].copy_from_slice(data.as_row_major());
        let mut extended = Some(RowMatrix::with_shape(prefilled, k + n, row_size).unwrap());

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

    let configs = vec![
        ("1MB_k1024", 1024, 1024, 1024),
        ("8MB_k4096", 4096, 4096, 2048),
        ("128MB_k4096", 4096, 4096, 32768),
    ];

    for (name, k, n, row_size) in configs {
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

    let configs = vec![
        ("1MB_k1024", 1024, 1024, 1024),
        ("8MB_k4096", 4096, 4096, 2048),
        ("128MB_k4096", 4096, 4096, 32768),
    ];

    for (name, k, n, row_size) in configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k, row_size), k, row_size).unwrap();
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
    bench_encode_in_place,
    bench_proof_generation,
    bench_verification
);
criterion_main!(benches);
