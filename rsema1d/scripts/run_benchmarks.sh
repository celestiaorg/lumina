#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
GO_DIR="${REPO_ROOT}/go"

print_help() {
  cat <<'EOF'
Run rsema1d benchmarks (Rust native, Rust wasm32, and/or Go).

Usage:
  ./scripts/run_benchmarks.sh
  RUN_RUST_BENCH=0 ./scripts/run_benchmarks.sh   # Go only
  RUN_GO_BENCH=0 ./scripts/run_benchmarks.sh     # Rust only
  RUN_RUST_BENCH=0 RUN_GO_BENCH=0 RUN_WASM_BENCH=1 ./scripts/run_benchmarks.sh   # wasm only

Environment variables:
  RUN_RUST_BENCH   1 to run Rust Criterion bench, 0 to skip (default: 1)
  RUN_GO_BENCH     1 to run Go bench runner, 0 to skip (default: 1)
  RUN_WASM_BENCH   1 to run the Rust bench as wasm32 in Node, 0 to skip (default: 0);
                   needs wasm-bindgen-test-runner (wasm-bindgen-cli at the
                   wasm-bindgen version in Cargo.lock) and node on PATH
  WASM_BENCH_ARGS  extra runner arguments for the wasm bench, e.g. a name
                   filter or "--ignored" for the 128 MiB cases (default: none)
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

RUN_RUST_BENCH="${RUN_RUST_BENCH:-1}"
RUN_GO_BENCH="${RUN_GO_BENCH:-1}"
RUN_WASM_BENCH="${RUN_WASM_BENCH:-0}"
WASM_BENCH_ARGS="${WASM_BENCH_ARGS:-}"
GO_CACHE_DIR="${GO_CACHE_DIR:-/tmp/rsema1d-go-build-cache}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${WORKSPACE_ROOT}/target}"

validate_binary_flag() {
  name="$1"
  value="$2"
  case "${value}" in
    0|1) ;;
    *)
      echo "error: ${name} must be 0 or 1 (got '${value}')" >&2
      exit 1
      ;;
  esac
}

validate_binary_flag "RUN_RUST_BENCH" "${RUN_RUST_BENCH}"
validate_binary_flag "RUN_GO_BENCH" "${RUN_GO_BENCH}"
validate_binary_flag "RUN_WASM_BENCH" "${RUN_WASM_BENCH}"

mkdir -p "${GO_CACHE_DIR}"
mkdir -p "${CARGO_TARGET_DIR}" 2>/dev/null || true

echo "Benchmark configuration:"
echo "  RUN_RUST_BENCH=${RUN_RUST_BENCH} (set RUN_GO_BENCH=0 for Rust-only)"
echo "  RUN_GO_BENCH=${RUN_GO_BENCH} (set RUN_RUST_BENCH=0 for Go-only)"
echo "  RUN_WASM_BENCH=${RUN_WASM_BENCH} (wasm32 in Node; WASM_BENCH_ARGS='${WASM_BENCH_ARGS}')"
echo "  CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
echo "  GO_CACHE_DIR=${GO_CACHE_DIR}"

if [ "${RUN_RUST_BENCH}" = "1" ] || [ "${RUN_WASM_BENCH}" = "1" ]; then
  if ! command -v cargo >/dev/null 2>&1; then
    echo "error: cargo is not installed or not on PATH" >&2
    exit 1
  fi
fi

if [ "${RUN_WASM_BENCH}" = "1" ]; then
  for tool in wasm-bindgen-test-runner node; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
      echo "error: ${tool} is not installed or not on PATH (required for RUN_WASM_BENCH=1)" >&2
      exit 1
    fi
  done
fi

if [ "${RUN_GO_BENCH}" = "1" ]; then
  if ! command -v go >/dev/null 2>&1; then
    echo "error: go is not installed or not on PATH" >&2
    exit 1
  fi
fi

if [ "${RUN_RUST_BENCH}" != "1" ] && [ "${RUN_GO_BENCH}" != "1" ] && [ "${RUN_WASM_BENCH}" != "1" ]; then
  echo "error: nothing to run (set RUN_RUST_BENCH=1, RUN_GO_BENCH=1 and/or RUN_WASM_BENCH=1)" >&2
  exit 1
fi

step=1
if [ "${RUN_RUST_BENCH}" = "1" ]; then
  echo "[${step}] Running Rust Criterion benchmarks..."
  (
    cd "${REPO_ROOT}"
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR}" cargo bench --bench codec_bench
  )
  step=$((step + 1))
else
  echo "[${step}] Skipping Rust benchmarks (RUN_RUST_BENCH=0)."
  step=$((step + 1))
fi

if [ "${RUN_WASM_BENCH}" = "1" ]; then
  echo "[${step}] Running Rust benchmarks as wasm32 in Node..."
  (
    cd "${REPO_ROOT}"
    # The runner's default baseline path is relative to the current directory;
    # keep it next to the other build products instead.
    # shellcheck disable=SC2086
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR}" \
      WASM_BINDGEN_BENCH_RESULT="${CARGO_TARGET_DIR}/wbg_benchmark_rsema1d.json" \
      cargo bench --target wasm32-unknown-unknown --bench codec_bench -- ${WASM_BENCH_ARGS}
  )
  step=$((step + 1))
else
  echo "[${step}] Skipping wasm32 benchmarks (RUN_WASM_BENCH=0)."
  step=$((step + 1))
fi

if [ "${RUN_GO_BENCH}" = "1" ]; then
  echo "[${step}] Running Go benchmarks..."
  (
    cd "${GO_DIR}"
    GOCACHE="${GO_CACHE_DIR}" go run ./cmd/bench/bench_runner.go
  )
else
  echo "[${step}] Skipping Go benchmarks (RUN_GO_BENCH=0)."
fi

echo "done: benchmark run complete."
