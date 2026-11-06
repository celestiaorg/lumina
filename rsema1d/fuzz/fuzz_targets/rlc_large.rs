//! The batched RLCs at production K (fibre's 4096 rows), which the `rlc` target's cap of 64
//! never reaches, must match the per-row reference. Run it with `-s none`: AddressSanitizer
//! slows it ~25x, and the `rlc` target already runs the same kernels under it.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rsema1d::codec::{bench_compute_rlcs, compute_rlc};
use rsema1d::crypto::derive_coefficients;
use rsema1d_fuzz::{pool, xorshift};

#[derive(Debug, Arbitrary)]
struct Input {
    k: u16,
    chunks: u8,
    threads: u8,
    row_root: [u8; 32],
    seed: u64,
    prefix: Vec<u8>,
}

fuzz_target!(|input: Input| {
    let k = usize::from(input.k) % 4096 + 1;
    // Up to 9 chunks, so chunks can outnumber the 8 threads and split unevenly.
    let row_size = (usize::from(input.chunks) % 9 + 1) * 64;
    let mut x = input.seed | 1;
    let mut data: Vec<u8> = (0..k * row_size).map(|_| xorshift(&mut x) as u8).collect();
    let prefix = input.prefix.len().min(data.len());
    data[..prefix].copy_from_slice(&input.prefix[..prefix]);

    let pool = pool(input.threads);
    let coefficients = pool.install(|| derive_coefficients(&input.row_root, k, 3 * k, row_size));
    let rlcs = pool.install(|| bench_compute_rlcs(&data, row_size, coefficients.clone()));
    assert_eq!(rlcs.len(), k);
    for (i, (rlc, row)) in rlcs.iter().zip(data.chunks_exact(row_size)).enumerate() {
        assert_eq!(*rlc, compute_rlc(row, &coefficients), "row {i}");
    }
});
