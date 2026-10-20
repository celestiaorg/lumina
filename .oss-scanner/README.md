# Private Celestia devnet

The Dockerfile contains only the runtime needed for a private Celestia devnet: celestia-app, celestia-node, Fibre, the Bash launcher and disposable accounts. It uses the [documented mainnet releases](https://docs.celestia.org/operate/networks/overview/), celestia-app/Fibre `v10.4.0` and celestia-node `v0.34.3`, on Ubuntu 24.04. These binaries run a fresh local chain, without connecting to mainnet. See the [threat model](threat_model.md) for audit guidance.

The image has no Rust toolchain, Lumina source, Cargo cache or compiled tests. Run Lumina tests separately from a prepared checkout. Existing CI Dockerfiles retain their own release pins.

The enrollment's `project.yaml` should reference these files in Lumina:

```yaml
repo: https://github.com/celestiaorg/lumina
dockerfile: .oss-scanner/Dockerfile
threat_model: .oss-scanner/threat_model.md
```

The enrollment also needs the project's security contact. No enrollment is submitted by the commands below.

## Build and run

From the repository root, build the image and start the offline devnet:

```sh
docker build --platform linux/amd64 -f .oss-scanner/Dockerfile -t lumina-devnet .
docker run --rm --init --platform linux/amd64 --network none --cpus 2 \
  --memory 8g --memory-swap 8g lumina-devnet
```

The container starts `devnet.sh` automatically, waits for readiness and stays running until stopped. To check startup and exit instead, override the default command:

```sh
docker run --rm --init --platform linux/amd64 --network none lumina-devnet \
  bash .oss-scanner/devnet.sh --smoke
```

The Docker build installs the runtime without starting services or running tests. To open a shell without starting services, override the command with `docker run --rm -it --platform linux/amd64 --network none lumina-devnet bash`. The OSS Scanner harness also replaces the default command with a shell; start `bash .oss-scanner/devnet.sh` explicitly there. Rebuild after changing the launcher, credentials or release pins. AMD64 emulation on Apple Silicon checks functionality; use native x86-64 Linux to assess performance. Before enrollment, run the scanner repository's configuration validator and `tools/check lumina` as well; this devnet-only image does not prepare Lumina for offline compilation.

## Private devnet

`devnet.sh` runs one private chain independently: a celestia-app validator, Fibre, two bridge nodes and a light node as local processes. It reuses `ci/credentials` as disposable fixtures and creates fresh temporary state. All service connections use loopback; no nested Docker daemon is needed.

| Service | Loopback endpoints |
| --- | --- |
| Main validator | RPC 26657, P2P 26656, app gRPC 19090, CometBFT gRPC 9098, signing 26669 |
| Fibre | TLS 7980 |
| Bridge 0, no authentication | RPC 26658, P2P 2121 |
| Bridge 1, authentication enabled | RPC 36658, P2P 3121 |
| Light node, no authentication | RPC 46658, P2P 4121 |

The launcher waits for blocks, Fibre registration and synchronized headers, then passes bridge tokens to the child command. Failures and interruptions stop owned process groups and remove temporary state. Logs remain in `target/oss-scanner/lumina-devnet-*/`; `--keep-data` preserves state. `--timeout SECONDS` bounds each startup step (default 180). `LUMINA_TEST_TIMEOUT` bounds each test package (default 1800). Suites run serially.

The scripts also run directly on macOS/Linux with matching Celestia binaries, Bash 3.2+, curl, jq and dasel v2 installed. `bash .oss-scanner/devnet.sh --help` lists options. Container services bind to container loopback; running Lumina tests on the host uses a separate host-native devnet.

## Native tests from a local checkout

With the matching Celestia binaries, the repository's Rust toolchain and dependencies already cached, run from the repository root:

```sh
bash .oss-scanner/test.sh check                # Launcher lifecycle checks; no Celestia binaries needed.
bash .oss-scanner/test.sh unit                 # Independent package suites; no services.
bash .oss-scanner/test.sh                      # All eleven selected native suites, with a fresh devnet.
bash .oss-scanner/test.sh test celestia-fibre   # One package, with a fresh devnet.

# RPC, gRPC and transaction-client suites share one fresh devnet.
bash .oss-scanner/test.sh test celestia-rpc celestia-grpc celestia-client

# Lumina node networking, synchronization and storage tests.
bash .oss-scanner/test.sh test lumina-node

# A targeted Fibre reproducer.
bash .oss-scanner/test.sh run cargo test --locked --offline -p celestia-fibre put_then_download
```

`test.sh` with no arguments runs all eleven native suites. Integration tests start and clean up their own fresh host-native devnet, independent of the Docker container, so each run has clean state and receives its own authentication tokens. `test.sh check` checks the launcher itself. Both test modes live in the same script; only `devnet.sh` is copied into the runtime image.

The runner uses Cargo's offline mode. It explicitly skips the gRPC `client::tests::retransmit_evicted` test, which requires the specialized eviction chain in the existing CI setup. Browser/wasm tests, deprecated UniFFI bindings and the separate Go compatibility suite are excluded.
