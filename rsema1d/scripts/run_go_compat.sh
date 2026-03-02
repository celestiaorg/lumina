#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
GO_DIR="${REPO_ROOT}/go"

print_help() {
  cat <<'EOF'
Generate Go fuzzy vectors and run Rust compatibility checks.

Usage:
  ./scripts/run_go_compat.sh
  RUN_FULL_TEST_SUITE=1 ./scripts/run_go_compat.sh

Environment variables:
  FUZZ_TESTS          Random fuzzy vectors to generate (default: 250)
  MAX_TOTAL_ROWS      Max k+n for random fuzzy vectors (default: 4096)
  RUN_FULL_TEST_SUITE 1 to run full cargo test before compat test (default: 0)
  CARGO_TARGET_DIR    Cargo target directory (default: monorepo ../target)
  GO_CACHE_DIR        Go build cache directory (default: /tmp/rsema1d-go-build-cache)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_help
  exit 0
fi

if [[ $# -gt 0 ]]; then
  echo "error: unexpected arguments: $*" >&2
  print_help >&2
  exit 1
fi

FUZZ_TESTS="${FUZZ_TESTS:-250}"
MAX_TOTAL_ROWS="${MAX_TOTAL_ROWS:-4096}"
GO_CACHE_DIR="${GO_CACHE_DIR:-/tmp/rsema1d-go-build-cache}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${WORKSPACE_ROOT}/target}"
RUN_FULL_TEST_SUITE="${RUN_FULL_TEST_SUITE:-0}"

case "${RUN_FULL_TEST_SUITE}" in
  0|1) ;;
  *)
    echo "error: RUN_FULL_TEST_SUITE must be 0 or 1 (got '${RUN_FULL_TEST_SUITE}')" >&2
    exit 1
    ;;
esac

mkdir -p "${GO_CACHE_DIR}"
mkdir -p "${CARGO_TARGET_DIR}" 2>/dev/null || true

echo "Compatibility configuration:"
echo "  FUZZ_TESTS=${FUZZ_TESTS}"
echo "  MAX_TOTAL_ROWS=${MAX_TOTAL_ROWS}"
echo "  RUN_FULL_TEST_SUITE=${RUN_FULL_TEST_SUITE}"
echo "  CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
echo "  GO_CACHE_DIR=${GO_CACHE_DIR}"

if ! command -v go >/dev/null 2>&1; then
  echo "error: go is not installed or not on PATH" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not installed or not on PATH" >&2
  exit 1
fi

echo "[1/3] Generating Go fuzzy vectors (num_tests=${FUZZ_TESTS}, max_total_rows=${MAX_TOTAL_ROWS})..."
(
  cd "${GO_DIR}"
  GOCACHE="${GO_CACHE_DIR}" go run ./cmd/generate_fuzzy_vectors/main.go "${FUZZ_TESTS}" "${MAX_TOTAL_ROWS}"
)

if [[ "${RUN_FULL_TEST_SUITE}" == "1" ]]; then
  echo "[2/3] Running full Rust test suite..."
  (
    cd "${REPO_ROOT}"
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR}" cargo test
  )
else
  echo "[2/3] Skipping full Rust suite (set RUN_FULL_TEST_SUITE=1 to enable)."
fi

echo "[3/3] Running Rust Go-compat integration test..."
(
  cd "${REPO_ROOT}"
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR}" cargo test --test go_fuzzy_compat -- --ignored
)

echo "done: Go fuzzy vectors regenerated and Rust compatibility check passed."
