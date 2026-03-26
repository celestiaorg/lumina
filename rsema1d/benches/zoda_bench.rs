use commonware_coding::{Config, PhasedScheme, Zoda};
use commonware_cryptography::Sha256;
use commonware_parallel::{Rayon, Sequential};
use commonware_utils::{NZU16, NZUsize};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;

type S = Zoda<Sha256>;

const DATA_SIZES: [usize; 3] = [8_388_608, 16_777_216, 134_217_728];
const CHUNKS: [u16; 3] = [50, 100, 250];
const CONCS: [usize; 2] = [4, 8];

fn generate_test_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; size];
    for b in &mut data {
        *b = rng.gen::<u8>();
    }
    data
}

fn size_label(bytes: usize) -> &'static str {
    match bytes {
        8_388_608 => "8MB",
        16_777_216 => "16MB",
        134_217_728 => "128MB",
        _ => "unknown",
    }
}

fn bench_zoda_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("zoda_encode");

    for &data_length in &DATA_SIZES {
        for &chunks in &CHUNKS {
            for &conc in &CONCS {
                let min = chunks / 3;
                let config = Config {
                    minimum_shards: NZU16!(min),
                    extra_shards: NZU16!(chunks - min),
                };
                let strategy = Rayon::new(NZUsize!(conc)).unwrap();
                let name = format!("{}_chunks{}_conc{}", size_label(data_length), chunks, conc);

                group.throughput(Throughput::Bytes(data_length as u64));
                group.bench_function(BenchmarkId::from_parameter(&name), |b| {
                    b.iter_batched(
                        || generate_test_data(data_length),
                        |data| {
                            if conc > 1 {
                                S::encode(&config, data.as_slice(), &strategy).unwrap()
                            } else {
                                S::encode(&config, data.as_slice(), &Sequential).unwrap()
                            }
                        },
                        BatchSize::LargeInput,
                    );
                });
            }
        }
    }

    group.finish();
}

fn bench_zoda_weaken(c: &mut Criterion) {
    let mut group = c.benchmark_group("zoda_weaken");

    for &data_length in &DATA_SIZES {
        for &chunks in &CHUNKS {
            let min = chunks / 3;
            let config = Config {
                minimum_shards: NZU16!(min),
                extra_shards: NZU16!(chunks - min),
            };
            let name = format!("{}_chunks{}", size_label(data_length), chunks);

            group.throughput(Throughput::Bytes(data_length as u64));
            group.bench_function(BenchmarkId::from_parameter(&name), |b| {
                b.iter_batched(
                    || {
                        let data = generate_test_data(data_length);
                        let (commitment, shards) =
                            S::encode(&config, data.as_slice(), &Sequential).unwrap();
                        (commitment, shards[0].clone())
                    },
                    |(commitment, shard)| S::weaken(&config, &commitment, 0, shard).unwrap(),
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_zoda_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("zoda_check");

    for &data_length in &DATA_SIZES {
        for &chunks in &CHUNKS {
            let min = chunks / 3;
            let config = Config {
                minimum_shards: NZU16!(min),
                extra_shards: NZU16!(chunks - min),
            };
            let name = format!("{}_chunks{}", size_label(data_length), chunks);

            group.throughput(Throughput::Bytes(data_length as u64));
            group.bench_function(BenchmarkId::from_parameter(&name), |b| {
                b.iter_batched(
                    || {
                        let data = generate_test_data(data_length);
                        let (commitment, shards) =
                            S::encode(&config, data.as_slice(), &Sequential).unwrap();
                        let (checking_data, _, _) =
                            S::weaken(&config, &commitment, 0, shards[0].clone()).unwrap();
                        let (_, _, weak_shard) =
                            S::weaken(&config, &commitment, 1, shards[1].clone()).unwrap();
                        (commitment, checking_data, weak_shard)
                    },
                    |(commitment, checking_data, weak_shard)| {
                        S::check(&config, &commitment, &checking_data, 1, weak_shard).unwrap()
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_zoda_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("zoda_decode");

    for &data_length in &DATA_SIZES {
        for &chunks in &CHUNKS {
            for &conc in &CONCS {
                let min = chunks / 3;
                let config = Config {
                    minimum_shards: NZU16!(min),
                    extra_shards: NZU16!(chunks - min),
                };
                let strategy = Rayon::new(NZUsize!(conc)).unwrap();
                let name = format!("{}_chunks{}_conc{}", size_label(data_length), chunks, conc);

                group.throughput(Throughput::Bytes(data_length as u64));
                group.bench_function(BenchmarkId::from_parameter(&name), |b| {
                    b.iter_batched(
                        || {
                            let data = generate_test_data(data_length);
                            let (commitment, mut shards) = if conc > 1 {
                                S::encode(&config, data.as_slice(), &strategy).unwrap()
                            } else {
                                S::encode(&config, data.as_slice(), &Sequential).unwrap()
                            };

                            let my_shard = shards.pop().unwrap();
                            let my_index = chunks - 1;

                            // "Best" shard selection: first min indices
                            let mut opt_shards: Vec<Option<_>> =
                                shards.into_iter().map(Some).collect();
                            let weak_shards: Vec<(u16, _)> = (0..min)
                                .map(|i| {
                                    let shard = opt_shards[i as usize].take().unwrap();
                                    let (_, _, weak_shard) =
                                        S::weaken(&config, &commitment, i, shard).unwrap();
                                    (i, weak_shard)
                                })
                                .collect();

                            let (checking_data, my_checked_shard, _) =
                                S::weaken(&config, &commitment, my_index, my_shard).unwrap();

                            (commitment, checking_data, my_checked_shard, weak_shards)
                        },
                        |(commitment, checking_data, my_checked_shard, weak_shards)| {
                            let mut checked_shards = weak_shards
                                .into_iter()
                                .map(|(idx, weak_shard)| {
                                    S::check(
                                        &config,
                                        &commitment,
                                        &checking_data,
                                        idx,
                                        weak_shard,
                                    )
                                    .unwrap()
                                })
                                .collect::<Vec<_>>();
                            checked_shards.push(my_checked_shard);

                            if conc > 1 {
                                S::decode(
                                    &config,
                                    &commitment,
                                    checking_data,
                                    checked_shards.iter(),
                                    &strategy,
                                )
                                .unwrap()
                            } else {
                                S::decode(
                                    &config,
                                    &commitment,
                                    checking_data,
                                    checked_shards.iter(),
                                    &Sequential,
                                )
                                .unwrap()
                            }
                        },
                        BatchSize::LargeInput,
                    );
                });
            }
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_zoda_encode, bench_zoda_weaken, bench_zoda_check, bench_zoda_decode
}
criterion_main!(benches);
