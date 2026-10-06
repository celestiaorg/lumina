mod support;

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rsema1d::{
    codec::bench_build_row_tree,
    crypto::{bench_hash_leaf_pair, hash_leaf},
    encode_in_place, ExtendedData, Parameters, RowMatrix,
};

const DATA_SEED: u64 = 0x5eed_5eed_5eed_5eed;
const CASES: &[(&str, usize, usize, usize)] = &[
    ("128MB_k1024_n1024", 1024, 1024, 131072),
    ("128MB_k4096_n12288", 4096, 12288, 32768),
    ("128MB_k8192_n24576", 8192, 24576, 16384),
];

fn bench_leaves(c: &mut Criterion) {
    let mut group = c.benchmark_group("leaf_pair");
    for row_size in [64, 16384, 32768, 131072] {
        let mut data = vec![0; row_size * 2];
        ChaCha8Rng::seed_from_u64(DATA_SEED).fill_bytes(&mut data);
        let (a, b) = data.split_at(row_size);
        assert_eq!(bench_hash_leaf_pair(a, b), [hash_leaf(a), hash_leaf(b)]);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_function(BenchmarkId::new("sequential", row_size), |bencher| {
            bencher.iter(|| [hash_leaf(black_box(a)), hash_leaf(black_box(b))]);
        });
        group.bench_function(BenchmarkId::new("paired", row_size), |bencher| {
            bencher.iter(|| bench_hash_leaf_pair(black_box(a), black_box(b)));
        });
    }
    group.finish();
}

fn bench_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_pipeline");
    group.sampling_mode(SamplingMode::Flat);
    for &(name, k, n, row_size) in CASES {
        let params = Parameters::new(k, n, row_size).unwrap();
        let mut data = vec![0; (k + n) * row_size];
        ChaCha8Rng::seed_from_u64(DATA_SEED).fill_bytes(&mut data[..k * row_size]);
        let fixture = encode_in_place(
            RowMatrix::with_shape(data, k + n, row_size).unwrap(),
            &params,
        )
        .unwrap();
        assert_eq!(
            bench_build_row_tree(&fixture.all_rows, &params).root(),
            fixture.row_root
        );
        support::print_case(name, k, n, row_size, rayon::current_num_threads());
        group.throughput(Throughput::Bytes(((k + n) * row_size) as u64));
        group.bench_function(BenchmarkId::new("row_tree", name), |b| {
            b.iter(|| bench_build_row_tree(black_box(&fixture.all_rows), black_box(&params)));
        });

        let mut extended = Some(fixture.all_rows);
        group.throughput(Throughput::Bytes((k * row_size) as u64));
        group.bench_function(BenchmarkId::new("encode_in_place", name), |b| {
            b.iter(|| {
                let buffer = extended.take().expect("buffer must be available");
                let encoded = encode_in_place(black_box(buffer), black_box(&params)).unwrap();
                let ExtendedData { all_rows, .. } = encoded;
                extended = Some(all_rows);
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = {
        support::print_environment(rayon::current_num_threads());
        #[cfg(target_arch = "aarch64")]
        println!("SHA-2 available: {}", std::arch::is_aarch64_feature_detected!("sha2"));
        Criterion::default().noise_threshold(0.03)
    };
    targets = bench_leaves, bench_rows
}
criterion_main!(benches);
