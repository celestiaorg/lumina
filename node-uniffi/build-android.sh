#!/usr/bin/env bash

set -euxo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

so_ext="so"
if [ "$(uname)" == "Darwin" ]; then
  so_ext="dylib"
fi

cargo build

cargo ndk \
  -o ./app/src/main/jniLibs \
  --manifest-path ./Cargo.toml \
  -t armeabi-v7a \
  -t arm64-v8a \
  -t x86 \
  -t x86_64 \
  build --release

cargo run --features bindgen --bin uniffi-bindgen \
  generate \
  --library ../target/debug/liblumina_node_uniffi.$so_ext \
  --language kotlin \
  --out-dir ./app/src/main/java/tech/forgen/lumina_node_uniffi/rust
