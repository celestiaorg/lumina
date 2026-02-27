# rsema1d

Rust implementation of rsema1d - Reed-Solomon erasure coding with Merkle commitments and Random Linear Combinations.

## Quick Start

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Usage

```rust
use rsema1d::{Parameters, ExtendedData};

// Create parameters (k original, n parity, row_size bytes)
let params = Parameters::new(4, 4, 64)?;

// Generate commitment
let data = vec![vec![0u8; 64]; 4];
let commitment = ExtendedData::generate(&data, &params)?;

// Generate proof for row 0
let proof = commitment.generate_row_proof(0)?;

// Verify proof with context
let (ctx, _rlc_root) = rsema1d::create_verification_context(&commitment.rlc_original(), &params)?;
rsema1d::verify_row_with_context(&proof, &commitment.commitment(), &ctx)?;

// Reconstruct from any k rows
let rows = vec![...];  // Any k rows from the k+n total
let indices = vec![0, 1, 2, 3];
let reconstructed = rsema1d::codec::reconstruct_data(&rows, &indices, 4, 4)?;
```

## Performance

Benchmarks run on **MacBook Pro M4 Pro** (14-core, 48 GB RAM):

### Encoding

| Data Size | Rust | Go | Speedup |
|-----------|------|-----|---------|
| 128KB (k=1024) | 1.41 ms (88 MiB/s) | 3.47 ms (36 MiB/s) | **2.5x** |
| 1MB (k=1024) | 2.41 ms (402 MiB/s) | 16.6 ms (60 MiB/s) | **6.9x** |
| 1MB (k=4096) | 3.92 ms (252 MiB/s) | 23.8 ms (42 MiB/s) | **6.1x** |
| 8MB (k=4096) | 15.9 ms (497 MiB/s) | 133 ms (60 MiB/s) | **8.4x** |
| 128MB (k=4096) | 206 ms (619 MiB/s) | 1938 ms (66 MiB/s) | **9.4x** |
| 128MB (k=8192) | 209 ms (612 MiB/s) | 2003 ms (64 MiB/s) | **9.6x** |

### Proof Generation & Verification

| Operation | Data Size | Rust | Go | Speedup |
|-----------|-----------|------|-----|---------|
| Proof Generation | 1MB | 0.12 µs | 0.12 µs | 1.0x |
| Proof Generation | 8MB | 0.14 µs | 0.14 µs | 1.0x |
| Proof Generation | 128MB | 0.14 µs | 0.14 µs | 1.0x |
| Verification | 1MB | 17 µs | 56 µs | **3.3x** |
| Verification | 8MB | 33 µs | 110 µs | **3.4x** |
| Verification | 128MB | 497 µs | 1682 µs | **3.4x** |

## Dependencies

- `reed-solomon-simd` - Optimized Reed-Solomon encoding/decoding
- `sha2` - SHA256 hashing for Merkle trees
- `criterion` - Statistical benchmarking
- `rayon` - Parallel computation

## Validation

This implementation is validated against the Go reference implementation at [github.com/celestiaorg/rsema1d](https://github.com/celestiaorg/rsema1d).

## License

Same license as the Go reference implementation.
