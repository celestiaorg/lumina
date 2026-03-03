use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateEncoder, RateEncoder};
use std::time::Instant;

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    k: usize,
    n: usize,
    row_size: usize,
    iters: usize,
}

fn make_cases(quick: bool) -> Vec<Case> {
    if quick {
        vec![
            Case {
                name: "128KB_k1024_n3072",
                k: 1024,
                n: 3072,
                row_size: 128,
                iters: 5,
            },
            Case {
                name: "1MB_k1024_n3072",
                k: 1024,
                n: 3072,
                row_size: 1024,
                iters: 3,
            },
        ]
    } else {
        vec![
            Case {
                name: "128KB_k1024_n3072",
                k: 1024,
                n: 3072,
                row_size: 128,
                iters: 200,
            },
            Case {
                name: "1MB_k1024_n3072",
                k: 1024,
                n: 3072,
                row_size: 1024,
                iters: 100,
            },
            Case {
                name: "1MB_k4096_n12288",
                k: 4096,
                n: 12288,
                row_size: 256,
                iters: 40,
            },
            Case {
                name: "8MB_k4096_n12288",
                k: 4096,
                n: 12288,
                row_size: 2048,
                iters: 10,
            },
            Case {
                name: "128MB_k4096_n12288",
                k: 4096,
                n: 12288,
                row_size: 32768,
                iters: 2,
            },
        ]
    }
}

fn make_original(k: usize, row_size: usize) -> Vec<u8> {
    let mut out = vec![0u8; k * row_size];
    for i in 0..k {
        for j in 0..row_size {
            out[i * row_size + j] = ((i + j) & 0xFF) as u8;
        }
    }
    out
}

fn run_case(case: Case) {
    let original = make_original(case.k, case.row_size);
    let mut sink = 0u8;

    for _ in 0..2 {
        let engine = DefaultEngine::new();
        let mut encoder: HighRateEncoder<DefaultEngine> =
            RateEncoder::new(case.k, case.n, case.row_size, engine, None)
                .expect("failed to create encoder");
        for row in original.chunks_exact(case.row_size) {
            encoder
                .add_original_shard(row)
                .expect("failed to add shard in warmup");
        }
        let result = encoder.encode().expect("failed to encode in warmup");
        if let Some(first_recovery) = result.recovery_iter().next() {
            sink ^= first_recovery[0];
        }
    }

    let start = Instant::now();
    for _ in 0..case.iters {
        let engine = DefaultEngine::new();
        let mut encoder: HighRateEncoder<DefaultEngine> =
            RateEncoder::new(case.k, case.n, case.row_size, engine, None)
                .expect("failed to create encoder");
        for row in original.chunks_exact(case.row_size) {
            encoder.add_original_shard(row).expect("failed to add shard");
        }
        let result = encoder.encode().expect("failed to encode");
        if let Some(first_recovery) = result.recovery_iter().next() {
            sink ^= first_recovery[0];
        }
    }
    let elapsed = start.elapsed();
    std::hint::black_box(sink);

    let avg = elapsed.as_secs_f64() / (case.iters as f64);
    let mib_per_s = (case.k * case.row_size) as f64 / avg / 1024.0 / 1024.0;
    println!(
        "RustSIMD_{:<22} avg={:>8.4}ms throughput={:>8.2} MiB/s iters={}",
        case.name,
        avg * 1000.0,
        mib_per_s,
        case.iters
    );
}

fn main() {
    let quick = std::env::var("RS_BENCH_QUICK").ok().as_deref() == Some("1");
    let cases = make_cases(quick);

    println!("# Rust reed-solomon-simd");
    println!("# mode={}", if quick { "quick" } else { "full" });
    for case in cases {
        run_case(case);
    }
}
