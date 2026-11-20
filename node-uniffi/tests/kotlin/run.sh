#!/usr/bin/env bash
set -xeuo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

cd ../../
cargo build

rm -rf ../target/kotlin-uniffi-bindings
mkdir -p ../target/kotlin-uniffi-bindings

cargo run --features bindgen --bin uniffi-bindgen \
  generate \
  --library ../target/debug/liblumina_node_uniffi.so \
  --language kotlin \
  --out-dir ../target/kotlin-uniffi-bindings

cd tests/kotlin

rm -rf lib/src/main/kotlin/uniffi
rm -rf lib/src/main/resources/*.so

mkdir -p lib/src/main/kotlin
mkdir -p lib/src/main/resources

cp ../../../target/debug/liblumina_node_uniffi.so lib/src/main/resources
cp -r ../../../target/kotlin-uniffi-bindings/uniffi lib/src/main/kotlin

# if user doesn't belong to docker group, run docker with sudo
SUDO=""
if ! id -nG | grep -q docker; then
  SUDO=sudo
fi

$SUDO docker run \
  --rm \
  --user "$(id -u):$(id -g)" \
  --volume "$PWD":/app \
  --workdir /app \
  gradle:8.12-jdk8 \
  gradle test
