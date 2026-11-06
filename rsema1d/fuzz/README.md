# rsema1d fuzzing

Fuzz targets for `rsema1d`. Each input encodes a small square (K and N up to 64, rows up to 2560 bytes) inside a Rayon pool of 1 to 8 threads, with an optional work budget, so that thread and stripe splits get exercised. It encodes either from the original rows or in place over parity rows holding stale bytes, which is how fibre encodes (`src/lib.rs`). An empty input is a valid one, so the targets need no seeds.

| Target | Checks |
| --- | --- |
| `rlc` | The encoder's batched RLCs match `compute_rlc` row by row, and `commitment_from_original_rows` gives the encoder's commitment. On aarch64 this compares the platform kernels against the scalar reference. |
| `rlc_large` | The batched RLCs at K up to 4096, fibre's production row count, match `compute_rlc` row by row. Skips encoding, so it reaches block counts and partial blocks the `rlc` target's K cap of 64 never does. Run it with `-s none`; AddressSanitizer slows it ~25x. |
| `verify` | Row, standalone and row-inclusion proofs, mutated, verified against a context built from mutated RLCs. Never panics, accepts only the encoded row at the claimed index, and accepts every unmutated proof. |
| `reconstruct` | A seeded random set of K or more genuine rows, then rows dropped, duplicated, re-indexed, bit-flipped and resized. Never panics, and returns the original data whenever given at least K distinct genuine rows. |

## Running

Requires a nightly toolchain and [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (`cargo install cargo-fuzz`). From the repository root:

```sh
cargo +nightly fuzz list --fuzz-dir rsema1d/fuzz
cargo +nightly fuzz run --fuzz-dir rsema1d/fuzz -O rlc
```

Crashing inputs land in `rsema1d/fuzz/artifacts/<target>/`; reproduce one with `cargo +nightly fuzz run --fuzz-dir rsema1d/fuzz -O <target> <path>`, or print it with `cargo +nightly fuzz fmt --fuzz-dir rsema1d/fuzz <target> <path>`.

The `Fuzz` workflow (`.github/workflows/fuzz.yml`) runs every target for 20 minutes nightly on x86_64, where reed-solomon-simd uses its AVX2 engine, and keeps each corpus in the Actions cache. Crashes are uploaded as workflow artifacts.
