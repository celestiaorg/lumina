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

# benchmark runner (Rust and Go)
./scripts/run_benchmarks.sh

# regenerate Go fuzzy vectors and run Rust Go-compat test
./scripts/run_go_compat.sh
```

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
