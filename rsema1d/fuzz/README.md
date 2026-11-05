# rsema1d fuzzing

Fuzz targets for `rsema1d`. Each input encodes a small square (K and N up to 64, rows up to 2560 bytes) inside a Rayon pool of 1 to 8 threads, with an optional work budget, so that thread and stripe splits get exercised (`src/lib.rs`). An empty input is a valid one, so the targets need no seeds.

| Target | Checks |
| --- | --- |
| `rlc` | The encoder's batched RLCs match `compute_rlc` row by row, and `commitment_from_original_rows` gives the encoder's commitment. On aarch64 this compares the platform kernels against the scalar reference. |
| `verify` | Row, standalone and row-inclusion proofs, mutated, verified against a context built from mutated RLCs. Never panics, accepts only the encoded row at the claimed index, and accepts every unmutated proof. |
| `reconstruct` | Rows with arbitrary indices, bit flips and lengths. Never panics, and returns the original data whenever given at least K distinct genuine rows. |

## Running

Requires a nightly toolchain and [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (`cargo install cargo-fuzz`). From the repository root:

```sh
cargo +nightly fuzz list --fuzz-dir rsema1d/fuzz
cargo +nightly fuzz run --fuzz-dir rsema1d/fuzz -O rlc
```

Crashing inputs land in `rsema1d/fuzz/artifacts/<target>/`; reproduce one with `cargo +nightly fuzz run --fuzz-dir rsema1d/fuzz -O <target> <path>`, or print it with `cargo +nightly fuzz fmt --fuzz-dir rsema1d/fuzz <target> <path>`.
