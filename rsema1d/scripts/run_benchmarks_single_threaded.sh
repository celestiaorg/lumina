#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export RAYON_NUM_THREADS=1
export GOMAXPROCS=1

exec "${SCRIPT_DIR}/run_benchmarks.sh" "$@"
