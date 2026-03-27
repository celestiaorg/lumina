use commonware_coding::{Config, PhasedScheme, Zoda};
use commonware_cryptography::Sha256;
use commonware_parallel::Rayon;
use commonware_utils::{NZU16, NZUsize};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;
use rsema1d::{encode, encode_in_place, reconstruct, ExtendedData, Parameters, RowMatrix};

type S = Zoda<Sha256>;

const DATA_SIZES: [usize; 2] = [8_388_608, 16_777_216];
const CHUNKS: [u16; 2] = [50, 100];
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
        _ => "unknown",
    }
}

fn bench_full_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_encode");

    // --- Zoda encode: 1/3 ratio, chunks [50, 100], conc [4, 8], sizes [8MB, 16MB] ---
    for &data_length in &DATA_SIZES {
        for &chunks in &CHUNKS {
            for &conc in &CONCS {
                let min = chunks / 3;
                let config = Config {
                    minimum_shards: NZU16!(min),
                    extra_shards: NZU16!(chunks - min),
                };
                let strategy = Rayon::new(NZUsize!(conc)).unwrap();
                let name = format!(
                    "zoda_{}_chunks{}_conc{}",
                    size_label(data_length),
                    chunks,
                    conc
                );

                group.throughput(Throughput::Bytes(data_length as u64));
                group.bench_function(BenchmarkId::from_parameter(&name), |b| {
                    b.iter_batched(
                        || generate_test_data(data_length),
                        |data| S::encode(&config, data.as_slice(), &strategy).unwrap(),
                        BatchSize::LargeInput,
                    );
                });
            }
        }
    }

    // --- rsema1d encode: n=3*k redundancy, matching data sizes ---
    // (k, n, row_size) configs that produce 8MB and 16MB of original data
    let rsema1d_configs: Vec<(&str, usize, usize, usize)> = vec![
        ("rsema1d_8MB_k4096_n12288", 4096, 12288, 2048),  // 4096*2048 = 8MB
        ("rsema1d_16MB_k4096_n12288", 4096, 12288, 4096), // 4096*4096 = 16MB
    ];

    for (name, k, n, row_size) in rsema1d_configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k * row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| encode(black_box(data), black_box(&params)).unwrap());
        });
    }

    group.finish();
}

fn bench_full_encode_in_place(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_encode_in_place");

    let rsema1d_configs: Vec<(&str, usize, usize, usize)> = vec![
        ("rsema1d_8MB_k4096_n12288", 4096, 12288, 2048),  // 4096*2048 = 8MB
        ("rsema1d_16MB_k4096_n12288", 4096, 12288, 4096), // 4096*4096 = 16MB
    ];

    for (name, k, n, row_size) in rsema1d_configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let data = RowMatrix::with_shape(generate_test_data(k * row_size), k, row_size).unwrap();
        let total_bytes = k * row_size;
        let mut prefilled = vec![0u8; (k + n) * row_size];
        prefilled[..total_bytes].copy_from_slice(data.as_row_major());
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

fn zoda_roundtrip(data_length: usize, chunks: u16, conc: usize) {
    let min = chunks / 3;
    let config = Config {
        minimum_shards: NZU16!(min),
        extra_shards: NZU16!(chunks - min),
    };
    let strategy = Rayon::new(NZUsize!(conc)).unwrap();
    let original = generate_test_data(data_length);

    let (commitment, mut shards) =
        S::encode(&config, original.as_slice(), &strategy).unwrap();

    let my_shard = shards.pop().unwrap();
    let my_index = chunks - 1;
    let mut opt_shards: Vec<Option<_>> = shards.into_iter().map(Some).collect();
    let weak_shards: Vec<(u16, _)> = (0..min)
        .map(|i| {
            let shard = opt_shards[i as usize].take().unwrap();
            let (_, _, weak_shard) = S::weaken(&config, &commitment, i, shard).unwrap();
            (i, weak_shard)
        })
        .collect();
    let (checking_data, my_checked_shard, _) =
        S::weaken(&config, &commitment, my_index, my_shard).unwrap();
    let mut checked_shards: Vec<_> = weak_shards
        .into_iter()
        .map(|(idx, weak_shard)| {
            S::check(&config, &commitment, &checking_data, idx, weak_shard).unwrap()
        })
        .collect();
    checked_shards.push(my_checked_shard);

    let decoded = S::decode(
        &config,
        &commitment,
        checking_data,
        checked_shards.iter(),
        &strategy,
    )
    .unwrap();
    assert_eq!(
        decoded.len(),
        original.len(),
        "Zoda roundtrip length mismatch: {} != {}",
        decoded.len(),
        original.len()
    );
    assert!(
        decoded == original,
        "Zoda roundtrip data mismatch (chunks={chunks}, size={data_length})"
    );
}

fn rsema1d_roundtrip(k: usize, n: usize, row_size: usize) {
    let params = Parameters::new(k, n, row_size).unwrap();
    let original_data = generate_test_data(k * row_size);
    let original_rm =
        RowMatrix::with_shape(original_data.clone(), k, row_size).unwrap();
    let ext_data = ExtendedData::generate(&original_rm, &params).unwrap();

    // Reconstruct from first k parity rows (actual recovery, not a no-op)
    let indices: Vec<usize> = (k..k + k).collect();
    let rows: Vec<&[u8]> = indices
        .iter()
        .map(|&i| ext_data.rows().row(i).unwrap())
        .collect();
    let reconstructed = reconstruct(&rows, &indices, &params).unwrap();
    assert_eq!(
        reconstructed.as_row_major(),
        original_data.as_slice(),
        "rsema1d roundtrip data mismatch (k={k}, n={n}, row_size={row_size})"
    );
}

fn bench_full_decode(c: &mut Criterion) {
    // Verify correctness before running benchmarks
    eprintln!("Verifying decode correctness...");
    zoda_roundtrip(1_048_576, 50, 4);
    rsema1d_roundtrip(256, 768, 4096);
    eprintln!("Decode correctness verified for both Zoda and rsema1d.");

    let mut group = c.benchmark_group("full_decode");

    // --- Zoda decode: 1/3 ratio, chunks [50, 100], conc [4, 8], sizes [8MB, 16MB] ---
    for &data_length in &DATA_SIZES {
        for &chunks in &CHUNKS {
            for &conc in &CONCS {
                let min = chunks / 3;
                let config = Config {
                    minimum_shards: NZU16!(min),
                    extra_shards: NZU16!(chunks - min),
                };
                let strategy = Rayon::new(NZUsize!(conc)).unwrap();
                let name = format!(
                    "zoda_{}_chunks{}_conc{}",
                    size_label(data_length),
                    chunks,
                    conc
                );

                group.throughput(Throughput::Bytes(data_length as u64));
                group.bench_function(BenchmarkId::from_parameter(&name), |b| {
                    b.iter_batched(
                        || {
                            let data = generate_test_data(data_length);
                            let (commitment, mut shards) =
                                S::encode(&config, data.as_slice(), &strategy).unwrap();

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

                            S::decode(
                                &config,
                                &commitment,
                                checking_data,
                                checked_shards.iter(),
                                &strategy,
                            )
                            .unwrap()
                        },
                        BatchSize::LargeInput,
                    );
                });
            }
        }
    }

    // --- rsema1d reconstruct: recover k original rows from k sampled rows ---
    let rsema1d_configs: Vec<(&str, usize, usize, usize)> = vec![
        ("rsema1d_8MB_k4096_n12288", 4096, 12288, 2048),
        ("rsema1d_16MB_k4096_n12288", 4096, 12288, 4096),
    ];

    for (name, k, n, row_size) in rsema1d_configs {
        let params = Parameters::new(k, n, row_size).unwrap();
        let original =
            RowMatrix::with_shape(generate_test_data(k * row_size), k, row_size).unwrap();
        let ext_data = ExtendedData::generate(&original, &params).unwrap();

        // Sample k parity rows (indices k..2k) — actual recovery, not a no-op
        let indices: Vec<usize> = (k..k + k).collect();
        let total_bytes = k * row_size;

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let rows: Vec<&[u8]> = indices
                    .iter()
                    .map(|&i| ext_data.rows().row(i).unwrap())
                    .collect();
                reconstruct(black_box(&rows), black_box(&indices), black_box(&params)).unwrap()
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_full_encode, bench_full_encode_in_place, bench_full_decode
}
criterion_main!(benches);
