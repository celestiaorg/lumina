use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use rsema1d::{reconstruct, reconstruct_with_work_budget, ExtendedData, Parameters, RowMatrix};

const K: usize = 4096;
const N: usize = 12288;
const ROW_SIZE: usize = 32768;
const SAMPLES: usize = 5;

fn measure<F>(operation: F) -> Duration
where
    F: FnOnce(),
{
    let started = Instant::now();
    operation();
    started.elapsed()
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn rows<'a>(input: &'a ExtendedData, indices: &[usize]) -> Vec<&'a [u8]> {
    indices
        .iter()
        .map(|&index| input.row(index).unwrap())
        .collect()
}

fn reconstruct_rows(
    rows: &[&[u8]],
    indices: &[usize],
    params: &Parameters,
    work_budget: Option<NonZeroUsize>,
) -> RowMatrix {
    match work_budget {
        Some(work_budget) => {
            reconstruct_with_work_budget(rows, indices, params, work_budget).unwrap()
        }
        None => reconstruct(rows, indices, params).unwrap(),
    }
}

fn reconstruct_once(
    input: &ExtendedData,
    indices: &[usize],
    params: &Parameters,
    work_budget: Option<NonZeroUsize>,
) -> Duration {
    let rows = rows(input, indices);
    measure(|| {
        black_box(reconstruct_rows(
            black_box(&rows),
            black_box(indices),
            black_box(params),
            work_budget,
        ));
    })
}

fn concurrent_reconstruct(
    inputs: &[&ExtendedData],
    indices: &[usize],
    params: &Parameters,
    work_budget: Option<NonZeroUsize>,
) -> Duration {
    let barrier = Barrier::new(inputs.len() + 1);
    std::thread::scope(|scope| {
        for &input in inputs {
            let barrier = &barrier;
            scope.spawn(move || {
                let rows = rows(input, indices);
                for _ in 0..=SAMPLES {
                    barrier.wait();
                    black_box(reconstruct_rows(&rows, indices, params, work_budget));
                    barrier.wait();
                }
            });
        }

        let mut samples = Vec::with_capacity(SAMPLES);
        for sample in 0..=SAMPLES {
            let elapsed = measure(|| {
                barrier.wait();
                barrier.wait();
            });
            // The first round warms the workers and is excluded from the median.
            if sample > 0 {
                samples.push(elapsed);
            }
        }
        median(samples)
    })
}

fn make_original(seed: u8) -> RowMatrix {
    let data = (0..K * ROW_SIZE)
        .map(|index| {
            (index as u8)
                .wrapping_mul(31)
                .wrapping_add((index / ROW_SIZE) as u8)
                .wrapping_add(seed)
        })
        .collect();
    RowMatrix::with_shape(data, K, ROW_SIZE).unwrap()
}

fn main() {
    let selected = std::env::args()
        .skip(1)
        .find(|arg| arg != "--bench")
        .unwrap_or_else(|| "all".into());
    assert!(
        ["all", "original", "mixed", "parity", "shared", "unique"].contains(&selected.as_str()),
        "unknown case {selected:?}"
    );
    let work_budget = std::env::var("RSEMA1D_RECONSTRUCT_WORK_BUDGET_MIB")
        .ok()
        .map(|value| {
            let mib: usize = value.parse().expect("work budget must be an integer MiB");
            NonZeroUsize::new(mib << 20).expect("work budget must be non-zero")
        });

    let params = Parameters::new(K, N, ROW_SIZE).unwrap();
    let original = make_original(0);
    let input = ExtendedData::generate(&original, &params).unwrap();

    let original_indices: Vec<_> = (0..K).collect();
    let parity_indices: Vec<_> = (K..2 * K).collect();
    let mixed_indices: Vec<_> = (0..K / 4).chain(K..K + (K * 3 / 4)).collect();

    for (name, indices) in [
        ("original", &original_indices),
        ("mixed", &mixed_indices),
        ("parity", &parity_indices),
    ] {
        if selected != "all" && selected != name {
            continue;
        }
        reconstruct_once(&input, indices, &params, work_budget);
        let samples = (0..SAMPLES)
            .map(|_| reconstruct_once(&input, indices, &params, work_budget))
            .collect();
        let label = if name == "mixed" {
            "mixed_25_percent_original"
        } else {
            name
        };
        let elapsed = median(samples);
        println!(
            "single {label} median={elapsed:?} blobs_per_second={:.3}",
            1.0 / elapsed.as_secs_f64()
        );
    }

    if selected == "all" || selected == "shared" {
        for concurrency in [1, 2, 4, 8, 16, 24, 32] {
            let inputs: Vec<_> = std::iter::repeat_n(&input, concurrency).collect();
            let elapsed = concurrent_reconstruct(&inputs, &mixed_indices, &params, work_budget);
            println!(
                "concurrent shared count={concurrency} median={elapsed:?} blobs_per_second={:.3}",
                concurrency as f64 / elapsed.as_secs_f64()
            );
        }
    }

    if selected == "all" || selected == "unique" {
        let unique_inputs: Vec<_> = (0..32)
            .map(|seed| ExtendedData::generate(&make_original(seed), &params).unwrap())
            .collect();
        let unique_input_refs: Vec<_> = unique_inputs.iter().collect();
        let elapsed =
            concurrent_reconstruct(&unique_input_refs, &mixed_indices, &params, work_budget);
        println!(
            "concurrent unique count=32 median={elapsed:?} blobs_per_second={:.3}",
            unique_inputs.len() as f64 / elapsed.as_secs_f64()
        );
    }
}
