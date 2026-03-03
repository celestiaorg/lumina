#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
GO_DIR="${REPO_ROOT}/go"

print_help() {
  cat <<'EOF'
Compare raw Reed-Solomon encoding performance:
  1) Go klauspost/reedsolomon
  2) Rust reed-solomon-simd

Usage:
  ./scripts/compare_rs_impls.sh
  RS_BENCH_QUICK=1 ./scripts/compare_rs_impls.sh

Environment variables:
  RS_BENCH_QUICK   1 for short smoke run, 0 for full run (default: 0)
  CARGO_TARGET_DIR Cargo target directory (default: monorepo ../target)
  GO_CACHE_DIR     Go build cache directory (default: /tmp/rsema1d-go-build-cache)
EOF
}

case "${1:-}" in
  -h|--help)
    print_help
    exit 0
    ;;
  "")
    ;;
  *)
    echo "error: unexpected arguments: $*" >&2
    print_help >&2
    exit 1
    ;;
esac

RS_BENCH_QUICK="${RS_BENCH_QUICK:-0}"
GO_CACHE_DIR="${GO_CACHE_DIR:-/tmp/rsema1d-go-build-cache}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${WORKSPACE_ROOT}/target}"

case "${RS_BENCH_QUICK}" in
  0|1) ;;
  *)
    echo "error: RS_BENCH_QUICK must be 0 or 1 (got '${RS_BENCH_QUICK}')" >&2
    exit 1
    ;;
esac

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not installed or not on PATH" >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "error: go is not installed or not on PATH" >&2
  exit 1
fi

mkdir -p "${GO_CACHE_DIR}"
mkdir -p "${CARGO_TARGET_DIR}" 2>/dev/null || true

echo "RS comparison configuration:"
echo "  RS_BENCH_QUICK=${RS_BENCH_QUICK}"
echo "  CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
echo "  GO_CACHE_DIR=${GO_CACHE_DIR}"

echo "[1/2] Running Rust reed-solomon-simd benchmark..."
(
  cd "${REPO_ROOT}"
  RS_BENCH_QUICK="${RS_BENCH_QUICK}" CARGO_TARGET_DIR="${CARGO_TARGET_DIR}" cargo run --release --bin bench_rs_simd
)

echo "[2/2] Running Go klauspost/reedsolomon benchmark..."
(
  cd "${GO_DIR}"
  RS_BENCH_QUICK="${RS_BENCH_QUICK}" GOCACHE="${GO_CACHE_DIR}" go run ./cmd/bench_klauspost/main.go
)

echo "done: raw RS comparison complete."

