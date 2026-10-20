# Lumina threat model

Lumina implements Rust libraries for headers, shares, blobs, RPC, gRPC, transactions and the Fibre protocol. Prioritize production Rust paths that consume untrusted messages, node responses, proofs and serialized data. Check header and commitment verification, invalid-input handling, resource bounds, storage consistency, signing intent and transaction lifecycle behavior.

## Assets and security goals

- Verified headers, shares and blobs must agree with their trusted chain state and commitments. Invalid proofs or inconsistent data must not become trusted through decoding, synchronization, caching or storage.
- A transaction's signed content must match the requested operation. Protect signer material and preserve correct sequence, retry and confirmation handling.
- Untrusted input must not cause a remotely reachable panic, corrupt persistent state, or consume resources without a practical bound.
- Fibre uploads must bind payment promises to the intended chain, namespace, blob commitment and signer, and verify validator signatures and the required voting power before reporting success; downloads must reconstruct data matching the requested blob commitment.

## Trust boundaries and attack surfaces

Treat remote P2P messages, RPC/gRPC responses, streamed events, headers, proofs, shares and Fibre responses as untrusted. A peer or endpoint can send malformed, inconsistent, stale or strategically delayed data, or disconnect mid-operation. Use existing mocks to exercise those cases as well as the real private devnet to check interoperability.

Review the path from parsing through validation to state mutation, including retries and concurrent tasks. Pay particular attention to accepting data before verification, mixing chain or height contexts, retaining invalid cached state, and trusting a successful submission as proof of confirmed application state.

The configured chain and trust anchor, caller-provided signing authority, and the host running Lumina are trusted configuration. State explicitly when a finding requires changing them or compromising those authorities. Distinguish an attacker who controls one peer or endpoint from one who controls the chain's consensus; demonstrate the capabilities required for the claimed impact.

The Celestia services in this image are dependencies for exercising Lumina. Distinguish a flaw in Lumina's use of them from a flaw wholly in a dependency. Test helpers are available to reproduce production issues; a bug confined to a test helper is not evidence of a vulnerability in the shipped libraries.

## Prepared environment

`.oss-scanner/Dockerfile` prepares only a private Celestia devnet runtime in `/src`, using mainnet release binaries on a fresh local chain. It contains celestia-app, celestia-node, Fibre, the launcher and disposable credentials. It has no Rust toolchain, Lumina source, cached Cargo dependencies or compiled tests. A separately prepared checkout is required to compile and test Lumina offline.

The audit scope is native Rust. Wasm/browser builds and tests, deprecated UniFFI/mobile bindings and the separate Go compatibility suite are not prepared. State any resulting validation limits in reports.

## Running tests and local services

The container's default command starts the private devnet and keeps it running. The OSS Scanner harness overrides that command with a shell, so services must be started explicitly in that context. From `/src`, use:

```sh
# Keep the local network running until Ctrl-C.
bash .oss-scanner/devnet.sh

# Check startup, then stop.
bash .oss-scanner/devnet.sh --smoke
```

From a local Lumina checkout with Rust dependencies cached and matching Celestia binaries on PATH, use `test.sh` below. It starts and cleans up its own fresh host-native devnet for integration tests, independent of the Docker container, and passes the matching authentication tokens to the test process. With no arguments it runs all eleven native suites; `test.sh check` runs the launcher's lifecycle checks without Celestia binaries. The devnet-only Docker image does not run Cargo tests.

The runner explicitly skips `client::tests::retransmit_evicted` in the gRPC suite because it requires a separate chain with special mempool and block-size settings. The test remains in the repository for the existing CI setup; this devnet runs only the normal private chain.

```sh
# All eleven selected native package suites, with a fresh devnet.
bash .oss-scanner/test.sh

# Independent package suites; no Celestia services.
bash .oss-scanner/test.sh unit

# RPC, gRPC and transaction-client suites, sharing one fresh devnet.
bash .oss-scanner/test.sh test celestia-rpc celestia-grpc celestia-client

# Fibre protocol tests, including upload and download.
bash .oss-scanner/test.sh test celestia-fibre

# Run a targeted reproducer with the devnet and its authentication environment.
bash .oss-scanner/test.sh run cargo test --locked --offline -p celestia-fibre put_then_download
```

Use fresh temporary state and the disposable accounts in `ci/credentials`. These published test keys and local development authentication settings are intentional. Do not use real accounts or connect to public Celestia networks. Prefer targeted, bounded reproductions. See [the environment guide](README.md) for service startup, endpoints, logs and timeouts. Mocks are useful for adversarial inputs; the real devnet checks interoperability and transaction/state timing.

## Reporting

Give the revision, attacker-controlled input, affected production entry point, required configuration, expected and actual behavior, and a runnable offline reproducer. Explain the concrete security impact and any unverified assumptions. For resource exhaustion, measure the input cost and resulting resource use. Prefer a focused regression test and minimal patch, and group variants of the same root cause in one report.
