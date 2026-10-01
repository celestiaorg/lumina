mod support;

use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::{prelude::*, ThreadPoolBuilder};
use rsema1d::{
    codec::{compute_rlc, RlcCoefficientLogs},
    crypto::derive_coefficients,
    GF128,
};

const K: usize = 1024;
const ROW_SIZE: usize = 128 * 1024;
const WORKERS: [usize; 2] = [1, 16];
const DATA_SEED: u64 = 0x5eed_5eed_5eed_5eed;

fn compute_rlcs(data: &[u8], row_size: usize, coefficients: Vec<GF128>) -> Vec<GF128> {
    let logs = RlcCoefficientLogs::new(coefficients);
    data.par_chunks_exact(row_size)
        .map(|row| logs.compute_rlc(row))
        .collect()
}

fn check_rlcs() {
    let (k, row_size) = (33, 128);
    let mut data = vec![0; k * row_size];
    ChaCha8Rng::seed_from_u64(DATA_SEED).fill_bytes(&mut data);
    let coefficients = derive_coefficients(&[0; 32], k, 31, row_size);
    let expected: Vec<_> = data
        .chunks_exact(row_size)
        .map(|row| compute_rlc(row, &coefficients))
        .collect();
    let actual = compute_rlcs(&data, row_size, coefficients);
    assert_eq!(actual.len(), k);
    assert_eq!(actual, expected);
}

fn bench_rlc_compute(c: &mut Criterion) {
    let mut group = c.benchmark_group("rlc_compute");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(100)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(120))
        .throughput(Throughput::Bytes((K * ROW_SIZE) as u64));

    let mut data = vec![0; K * ROW_SIZE];
    ChaCha8Rng::seed_from_u64(DATA_SEED).fill_bytes(&mut data);
    let mut row_root = [0; 32];
    row_root[..4].copy_from_slice(&[1, 2, 3, 4]);

    for workers in WORKERS {
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap();
        pool.install(check_rlcs);

        for n in [1024, 3072] {
            let coefficients = derive_coefficients(&row_root, K, n, ROW_SIZE);
            let name = format!("128MB_k{K}_n{n}/workers={workers}");
            group.bench_function(name, |b| {
                pool.install(|| {
                    b.iter_batched(
                        // Encoding consumes the coefficients; restore them outside timing.
                        || coefficients.clone(),
                        |coefficients| compute_rlcs(black_box(&data), ROW_SIZE, coefficients),
                        BatchSize::PerIteration,
                    );
                });
            });
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = {
        support::print_environment(format_args!("{WORKERS:?} (per case)"));
        Criterion::default().noise_threshold(0.03)
    };
    targets = bench_rlc_compute
}
criterion_main!(benches);
