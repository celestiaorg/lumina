mod support;

use std::{sync::Once, time::Duration};

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::ThreadPoolBuilder;
use rsema1d::{
    codec::{bench_compute_rlcs as compute_rlcs, compute_rlc},
    crypto::derive_coefficients,
};

const BYTES: usize = 128 * 1024 * 1024;
const SHAPES: [(usize, usize); 3] = [(1024, 1024), (4096, 12288), (8192, 24576)];
const DATA_SEED: u64 = 0x5eed_5eed_5eed_5eed;

/// One worker and every available core.
fn workers() -> [usize; 2] {
    [1, std::thread::available_parallelism().unwrap().get()]
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
        .throughput(Throughput::Bytes(BYTES as u64));

    let mut data = vec![0; BYTES];
    ChaCha8Rng::seed_from_u64(DATA_SEED).fill_bytes(&mut data);
    let mut row_root = [0; 32];
    row_root[..4].copy_from_slice(&[1, 2, 3, 4]);

    for workers in workers() {
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap();
        pool.install(check_rlcs);

        for (k, n) in SHAPES {
            let row_size = BYTES / k;
            let coefficients = derive_coefficients(&row_root, k, n, row_size);
            let name = format!("128MB_k{k}_n{n}/workers={workers}");
            let metadata = Once::new();
            group.bench_function(&name, |b| {
                metadata.call_once(|| {
                    support::print_case(
                        format_args!("rlc_compute/{name}"),
                        k,
                        n,
                        row_size,
                        workers,
                    );
                });
                pool.install(|| {
                    b.iter_batched(
                        // Encoding consumes the coefficients; restore them outside timing.
                        || coefficients.clone(),
                        |coefficients| compute_rlcs(black_box(&data), row_size, coefficients),
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
        support::print_environment(format_args!("{:?} (per case)", workers()));
        Criterion::default().noise_threshold(0.03)
    };
    targets = bench_rlc_compute
}
criterion_main!(benches);
