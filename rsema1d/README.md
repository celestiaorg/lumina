# rsema1d (Rust)

Rust implementation of the `rsema1d` codec:

- Reed-Solomon row extension (`k` original rows + `n` parity rows)
- Merkle commitment over rows
- RLC-based row verification
- Reconstruction of original rows from any `k` available rows

## Quick Start

```bash
# unit + integration tests
cargo test

# 128 MiB benchmark runner (Rust and Go)
./scripts/run_benchmarks.sh

# regenerate Go fuzzy vectors and run Rust Go-compat test
./scripts/run_go_compat.sh
```

## Encoder and RLC benchmarks

Each Rust benchmark executable prints the OS, architecture, package version, CPU model, and Rayon worker count before its results. The RLC benchmark reports its per-case worker counts. CPU model detection supports Linux and macOS and falls back to `unknown` when unavailable.

Run the encoder comparisons sequentially from the workspace root:

```bash
RAYON_NUM_THREADS=16 cargo bench -p rsema1d --bench codec_bench -- '^encode_in_place/128MB_' --quick --noplot
(cd rsema1d/go && GOMAXPROCS=16 go run ./cmd/bench/bench_runner.go)
```

The shared `rsema1d/scripts/run_benchmarks.sh` wrapper also filters Rust cases to `128MB_` and runs the same Go runner. Running `cargo bench --bench codec_bench` directly without a filter still selects every size.

Compare Rust `encode_in_place/<case>` with Go `Encode_<case>`. The Go runner selects only 128 MiB of original data, with (K, N) equal to (1024, 1024), (4096, 12288), or (8192, 24576); the Rust filter above selects the same shapes. Change both worker settings to `1` for a single-worker comparison. The Go runner also measures proof generation and verification for its 128 MiB K=4096 case; its encoder uses 10 warmups and 100 measured calls per case. Remove Rust's `--quick` for longer sampling.

`encode` includes allocation of the extended matrix and copying original rows; `encode_in_place` and the Go runner reuse their row buffers. Go clears parity inside timing and reuses its coder. Rust also extends the original RLC vector to K+N and returns a copy of the original RLCs. These are API throughput comparisons, not identical amounts of work. Rust uses deterministic pseudorandom input; the Go runner uses a repeating byte pattern.

The standalone RLC benchmark matches the dimensions of celestia-app's `rlc.BenchmarkCompute`: 128 MiB of original data, K=1024, N=1024 or 3072, and 128 KiB rows. It uses explicit 1- and 16-worker pools, independent of `RAYON_NUM_THREADS`:

```bash
cargo bench -p rsema1d --features bench-internals --bench rlc_bench -- --quick --noplot
# Select one case:
cargo bench -p rsema1d --features bench-internals --bench rlc_bench -- \
  '^rlc_compute/128MB_k1024_n3072/workers=16$' --quick --noplot
```

Run the corresponding Go RLC case from a celestia-app checkout:

```bash
GOMAXPROCS=16 go test ./pkg/rsema1d/rlc -run '^$' \
  -bench '^BenchmarkCompute$/^size=128MB$/^k=1024$/^n=3072$/^workers=16$' \
  -benchmem -benchtime=3s -count=5
```

Input generation, coefficient derivation, and restoring the owned coefficient input happen outside timing. Each timed call prepares coefficient lookup tables using the encoder's implementation, computes the K original RLCs, and allocates the output vector. RS extension, Merkle trees, and commitment construction are excluded. Both N cases process the same number of rows; N changes the derived coefficients. A small scalar-reference check runs in each worker pool before measurement.

Throughput uses original input bytes. Criterion uses binary units and the bundled Go runner reports MiB/s; Go's standard `go test -bench` reports decimal MB/s. Record the commit, CPU, toolchain, CPU compilation settings, and worker counts when comparing results. `bench-internals` only exposes the internal RLC helper for benchmarking and is disabled by default.

Without `--quick`, the Rust benchmarks use these sampling profiles:

| Cases | Sampling | Samples | Warmup | Measurement target |
|---|---|---:|---:|---:|
| Proof generation and individual verification | Linear | 200 | 5 s | 30 s |
| Encode/reconstruct below 4 MiB; verification context | Flat | 200 | 5 s | 90 s |
| Encode/reconstruct from 4 MiB to below 64 MiB | Flat | 120 | 5 s | 90 s |
| Encode/reconstruct at least 64 MiB; batch verification; RLC | Flat | 100 | 10 s | 120 s |

Flat sampling gives every sample the same iteration count, avoiding the quadratic minimum iteration count of linear sampling for slower cases. Measurement times are targets, not hard limits. Longer runs and more samples can improve estimate precision, but need not reduce the percentage classified as outliers. Assess confidence-interval width and repeatability on an otherwise idle machine with fixed worker counts. Criterion retains outliers in its analysis; these profiles keep its default 95% confidence level and existing outlier rules. `--quick` bypasses the full sampling schedule and is only for rough estimates.

## Production benchmarks

Run from the workspace root:

```bash
RAYON_NUM_THREADS=8 cargo bench -p rsema1d --bench reconstruct_sweep -- mixed
RAYON_NUM_THREADS=8 cargo bench -p rsema1d --bench codec_bench -- verification_batch
```

The reconstruction sweep uses 4096 original rows, 12288 parity rows, and 32 KiB per row (128 MiB of original data). Select `original`, `mixed` (25% original rows), or `parity` for one reconstruction, `shared` for 1–32 concurrent reconstructions of the same input, or `unique` for 32 concurrent reconstructions of distinct inputs. The default, `all`, runs every case. Each result reports the median of five samples after one warmup and throughput in blobs per second. The `unique` case holds at least 16 GiB of encoded row data; concurrent reconstruction also needs output and decoder buffers.

The Criterion `verification_batch` group compares serial verification with Rayon batches of 148 rows for the same dimensions. Keep the machine, thread count, and command fixed when comparing branches, and record the commit and CPU alongside results.

Set `RSEMA1D_RECONSTRUCT_WORK_BUDGET_MIB` to a positive integer to override the reconstruction work-buffer budget in the sweep, for example `RSEMA1D_RECONSTRUCT_WORK_BUDGET_MIB=32 RAYON_NUM_THREADS=8 cargo bench -p rsema1d --bench reconstruct_sweep -- mixed`. Leave it unset to measure the default budget.

## End-to-End Flow (Library API)

1. Build `Parameters(k, n, row_size)`.
2. Put original data into a contiguous `RowMatrix` (`k * row_size` bytes).
3. Call `encode` to produce:
   - `ExtendedData` (all `k+n` rows)
   - `Commitment` (`[u8; 32]`)
   - original `RLC` values
4. Build a `VerificationContext` from original RLC values.
5. Generate and verify row proofs.
6. Sample any `k` rows and call `reconstruct` to recover original rows.

## Example

```rust
use rsema1d::{
    create_verification_context, encode, reconstruct, verify_row_with_context, Parameters,
    RowMatrix,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let params = Parameters::new(4, 12, 64)?;

    // 1) Build contiguous original rows.
    let mut bytes = vec![0u8; params.k * params.row_size];
    for i in 0..params.k {
        bytes[i * params.row_size] = (i as u8) + 1;
    }
    let original = RowMatrix::with_shape(bytes, params.k, params.row_size)?;

    // 2) Encode.
    let (extended, commitment, rlc_orig) = encode(&original, &params)?;

    // 3) Create verification context and verify a row proof.
    let (ctx, _rlc_root) = create_verification_context(&rlc_orig, &params)?;
    let proof = extended.generate_row_proof(0)?;
    verify_row_with_context(&proof, &commitment, &ctx)?;

    // 4) Reconstruct from any k rows.
    let indices = vec![0usize, 1, params.k, params.k + 1];
    let sampled = extended.rows().sample(&indices)?;
    let reconstructed = reconstruct(&sampled, &indices, &params)?;

    assert_eq!(reconstructed.as_row_major(), original.as_row_major());
    Ok(())
}
```

## Whole-Flow Tests

Primary end-to-end test (encode -> verify -> reconstruct):

- `basic_encode_verify_reconstruct` in [`src/lib.rs`](/Users/mikhailrakhmanov/repos/mcrakhman-lumina/rsema1d/src/lib.rs)

Cross-language compatibility flow (Go vectors -> Rust encode/verify/reconstruct):

- `go_fuzzy_vectors_match_rust` in [`tests/go_fuzzy_compat.rs`](/Users/mikhailrakhmanov/repos/mcrakhman-lumina/rsema1d/tests/go_fuzzy_compat.rs)

Run:

```bash
# local whole-flow test
cargo test basic_encode_verify_reconstruct

# cross-language whole-flow test
./scripts/run_go_compat.sh
```
