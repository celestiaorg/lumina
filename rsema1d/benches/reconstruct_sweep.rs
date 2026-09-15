use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use rsema1d::{reconstruct, reconstruct_with_work_budget, ExtendedData, Parameters, RowMatrix};

const K: usize = 4096;
const N: usize = 12288;
const ROW_SIZE: usize = 32768;

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
    measure(|| {
        std::thread::scope(|scope| {
            for &input in inputs {
                scope.spawn(move || {
                    let rows = rows(input, indices);
                    black_box(reconstruct_rows(&rows, indices, params, work_budget));
                });
            }
        });
    })
}

fn striped_reconstruct(
    input: &ExtendedData,
    indices: &[usize],
    stripes: usize,
    work_budget: Option<NonZeroUsize>,
) -> (Duration, Vec<RowMatrix>) {
    assert!(ROW_SIZE.is_multiple_of(stripes));
    let rows = rows(input, indices);
    let stripe_size = ROW_SIZE / stripes;
    let started = Instant::now();
    let result = (0..stripes)
        .into_par_iter()
        .map(|stripe| {
            let start = stripe * stripe_size;
            let end = start + stripe_size;
            let stripe_rows: Vec<_> = rows.iter().map(|row| &row[start..end]).collect();
            let params = Parameters::new(K, N, stripe_size).unwrap();
            reconstruct_rows(&stripe_rows, indices, &params, work_budget)
        })
        .collect();
    (started.elapsed(), result)
}

fn validate_striped(result: &[RowMatrix], original: &RowMatrix, stripes: usize) {
    let stripe_size = ROW_SIZE / stripes;
    for row in 0..K {
        let original_row = original.row(row).unwrap();
        for (stripe, reconstructed) in result.iter().enumerate() {
            let start = stripe * stripe_size;
            let end = start + stripe_size;
            assert_eq!(reconstructed.row(row).unwrap(), &original_row[start..end]);
        }
    }
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
        ["all", "original", "mixed", "parity", "shared", "unique", "striped"]
            .contains(&selected.as_str()),
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
        let samples = (0..5)
            .map(|_| reconstruct_once(&input, indices, &params, work_budget))
            .collect();
        let label = if name == "mixed" {
            "mixed_25_percent_original"
        } else {
            name
        };
        println!("single {label} median={:?}", median(samples));
    }

    if selected == "all" || selected == "shared" {
        for concurrency in [1, 2, 4, 8, 16, 24, 32] {
            let inputs: Vec<_> = std::iter::repeat_n(&input, concurrency).collect();
            let elapsed = concurrent_reconstruct(&inputs, &mixed_indices, &params, work_budget);
            println!(
                "concurrent shared count={concurrency} elapsed={elapsed:?} blobs_per_second={:.3}",
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
            "concurrent unique count=32 elapsed={elapsed:?} blobs_per_second={:.3}",
            unique_inputs.len() as f64 / elapsed.as_secs_f64()
        );
    }

    if selected == "all" || selected == "striped" {
        for stripes in [1, 2, 4, 8, 16, 32] {
            let (elapsed, result) =
                striped_reconstruct(&input, &mixed_indices, stripes, work_budget);
            validate_striped(&result, &original, stripes);
            black_box(result);
            println!(
                "striped stripes={stripes} elapsed={elapsed:?} blobs_per_second={:.3}",
                1.0 / elapsed.as_secs_f64()
            );
        }
    }
}
