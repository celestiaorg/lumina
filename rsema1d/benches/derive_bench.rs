mod support;

use std::{sync::Once, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rayon::ThreadPoolBuilder;
use rsema1d::crypto::derive_coefficients;

fn bench_derive(c: &mut Criterion) {
    let mut group = c.benchmark_group("derive_coefficients");
    group
        .sample_size(50)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));

    for workers in [1, 2, 4, 8, 16, 32] {
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap();
        pool.broadcast(|_| ());
        for exponent in 5..=16 {
            let symbols = 1 << exponent;
            let row_size = symbols * 2;
            group.throughput(Throughput::Elements(symbols as u64));
            let name = format!("symbols={symbols}/workers={workers}");
            let metadata = Once::new();
            group.bench_function(&name, |b| {
                metadata.call_once(|| {
                    support::print_case(&name, 1024, 1024, row_size, workers);
                });
                pool.install(|| {
                    b.iter(|| {
                        derive_coefficients(
                            black_box(&[0xa5; 32]),
                            black_box(1024),
                            black_box(1024),
                            black_box(row_size),
                        )
                    });
                });
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = {
        support::print_environment("per case");
        Criterion::default().noise_threshold(0.03)
    };
    targets = bench_derive
}
criterion_main!(benches);
