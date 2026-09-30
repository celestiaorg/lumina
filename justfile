
# Default recipe to display help
default:
    just --list

# Install development dependencies
install-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Installing development dependencies..."
    cargo install wasm-pack
    cargo install cargo-udeps
    cargo install cargo-msrv
    cargo install cargo-minimal-versions
    cargo install cargo-hack
    cargo install cargo-ndk
    rustup target add wasm32-unknown-unknown
    rustup target add aarch64-apple-ios aarch64-apple-ios-sim
    rustup target add aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android
    rustup component add clippy rustfmt

# Format all code
fmt:
    cargo fmt --all

# Check formatting without making changes
fmt-check:
    cargo fmt --all -- --check

# Run clippy for a specific crate on native target
clippy CRATE:
    cargo clippy --all-targets --all-features -p {{CRATE}} -- -D warnings --no-deps

# Run clippy for a specific crate on wasm32 target
clippy-wasm CRATE:
    cargo clippy --all-targets --all-features --target=wasm32-unknown-unknown -p {{CRATE}} -- -D warnings --no-deps

# Run clippy for all crates on all platforms
clippy-all: clippy-all-native clippy-all-wasm

# Run clippy for all crates on native target
clippy-all-native:
    cargo clippy --all-targets --all-features -- -D warnings --no-deps

# Run clippy for all crates on wasm32 target
clippy-all-wasm:
    cargo clippy --all-targets --all-features --target=wasm32-unknown-unknown -- -D warnings --no-deps

# Run tests for a specific crate
test CRATE:
    cargo test --all-features -p {{CRATE}}

# Run all tests
test-all:
    cargo test --all-features

# Run wasm tests for a specific crate
test-wasm CRATE_DIR FLAGS="":
    wasm-pack test --headless --chrome {{CRATE_DIR}} {{FLAGS}}

# Build documentation
docs:
    cargo doc --workspace --all-features --no-deps

# Build documentation and open in browser
docs-open:
    cargo doc --workspace --all-features --no-deps --open

# Check documentation
docs-check:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Checking missing docs for native..."
    cargo clippy --workspace --all-features -- -D missing-docs
    echo "Checking missing docs for wasm32..."
    cargo clippy --workspace --all-features --target=wasm32-unknown-unknown -- -D missing-docs
    echo "Running rustdoc check..."
    RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --no-deps

# Check for unused dependencies
check-unused-deps:
    cargo +nightly udeps --all-targets --all-features

# Check minimal versions of dependencies
check-minimal-versions:
    cargo minimal-versions check --direct --all-features
    cargo minimal-versions check --direct --all-features --target wasm32-unknown-unknown

# Verify MSRV for a specific crate
msrv CRATE_DIR:
    cargo msrv verify --manifest-path ./{{CRATE_DIR}}/Cargo.toml --all-features
    cargo msrv verify --manifest-path ./{{CRATE_DIR}}/Cargo.toml --all-features --target wasm32-unknown-unknown

# Run all checks (format, clippy, tests, docs)
check-all: fmt-check clippy-all test-all docs-check

# Build and pack wasm node
build-wasm:
    wasm-pack build node-wasm
    wasm-pack pack node-wasm

# Build demo webpage
build-demo-web:
    cd cli/js && npm ci && npm run build

# Test lumina in browser
run-demo-web: build-demo-web
    cargo run --package lumina-cli --features browser-node -- browser

build-wasm-full: build-wasm build-demo-web

# Build iOS bindings
build-ios:
    ./node-uniffi/build-ios.sh
    ./examples/ios/LuminaDemo/copy_bindings.sh

# Build Android bindings
build-android:
    ./node-uniffi/build-android.sh
    ./examples/android/LuminaDemo/copy_bindings.sh

docker-up:
    sudo docker compose -f ci/docker-compose.yml up --build --force-recreate -d

docker-down:
    docker compose -f ci/docker-compose.yml down

docker-logs SERVICE="":
    #!/usr/bin/env bash
    if [ -z "{{SERVICE}}" ]; then
        docker compose -f ci/docker-compose.yml logs -f
    else
        docker compose -f ci/docker-compose.yml logs -f {{SERVICE}}
    fi

docker-restart: docker-down docker-up

docker-auth-token:
    docker compose -f ci/docker-compose.yml exec node-1 celestia bridge auth admin --p2p.network private

docker-exec SERVICE CMD:
    docker compose -f ci/docker-compose.yml exec {{SERVICE}} {{CMD}}

docker-clean:
    docker compose -f ci/docker-compose.yml down -v
    rm -rf ci/credentials/*

docker-ps:
    docker compose -f ci/docker-compose.yml ps

gen-auth-tokens:
    ./tools/gen_auth_tokens.sh

test-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Generating auth tokens..."
    ./tools/gen_auth_tokens.sh
    echo "Running integration tests..."
    cargo test --all-features

# Complete integration test workflow
test-integration-full: docker-restart gen-auth-tokens test-all docker-down

# Build CLI with browser support
build-cli-browser:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Building wasm node..."
    wasm-pack build node-wasm
    echo "Building demo webpage..."
    cd cli/js && npm ci && npm run build && cd -
    echo "Installing CLI with browser support..."
    cargo install --path cli --features browser-node

# Clean build artifacts
clean:
    cargo clean
    rm -rf node-wasm/pkg
    rm -rf cli/js/node_modules
    rm -rf cli/js/dist

# Full clean including docker volumes
clean-all: clean docker-clean

# Run pre-commit checks
checklist: fmt clippy-all-native test-all

# Quick check (format + clippy native only)
checklist-quick: fmt clippy-all-native

# CI simulation - run all checks that CI runs
checklist-full: fmt-check clippy-all test-all docs-check check-unused-deps check-minimal-versions
