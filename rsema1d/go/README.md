# Go Cross-Validation Tools

This directory contains Go programs used for Rust `rsema1d` compatibility vectors and benchmarks.

All commands should be run from this directory:

```bash
cd /Users/mikhailrakhmanov/repos/mcrakhman-lumina/rsema1d/go
```

## Commands

```bash
# Randomized coverage vectors
# args: [num_tests] [max_total_rows]
# always includes fixed high-scale cases for k=4096, n=12288 (1:3)
go run ./cmd/generate_fuzzy_vectors/main.go 250 4096

# Go benchmark runner
go run ./cmd/bench/bench_runner.go
```

## Generated Files

- `fuzzy_test_vectors.json`

The module dependency on `github.com/celestiaorg/rsema1d` is intentionally set to a published version (no local `replace`).
