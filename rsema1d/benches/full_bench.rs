use commonware_coding::{Config, PhasedScheme, Zoda};
use commonware_cryptography::Sha256;
use commonware_parallel::Rayon;
use commonware_utils::{NZU16, NZUsize};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;
use rsema1d::{encode, encode_in_place, Parameters, RowMatrix};

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

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_full_encode, bench_full_encode_in_place
}
criterion_main!(benches);
