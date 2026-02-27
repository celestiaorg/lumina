# Go Cross-Validation Tools

This directory contains Go programs used to generate vectors and compatibility fixtures for the Rust `rsema1d` crate.

All commands should be run from this directory:

```bash
cd /Users/mikhailrakhmanov/repos/mcrakhman-lumina/rsema1d/go
```

## Commands

```bash
# Deterministic coverage vectors (fixed cases)
go run ./cmd/generate_test_vectors/main.go

# Randomized coverage vectors
# args: [num_tests] [max_total_rows]
go run ./cmd/generate_fuzzy_vectors/main.go 100 4096

# reed-solomon-simd compatibility vectors
# arg: output_dir
go run ./cmd/generate_rs_compat_vectors/main.go ../rs_compat_test_vectors
```

## Generated Files

- `test_vectors.json`
- `fuzzy_test_vectors.json`
- `../rs_compat_test_vectors/rs_compat_vectors.json`

The module dependency on `github.com/celestiaorg/rsema1d` is intentionally set to a published version (no local `replace`).
