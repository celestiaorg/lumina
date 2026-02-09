# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.3](https://github.com/celestiaorg/lumina/compare/lumina-node-uniffi-v0.5.2...lumina-node-uniffi-v0.5.3) - 2026-01-27

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

## [0.5.2](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.5.1...lumina-node-uniffi-v0.5.2) - 2026-01-08

### Other

- updated the following local packages: celestia-types, celestia-grpc, lumina-node

## [0.5.1](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.5.0...lumina-node-uniffi-v0.5.1) - 2026-01-05

### Added

- *(node,node-wasm,node-uniffi)* Add header, blob, and namespace subscriptions ([#772](https://github.com/eigerco/lumina/pull/772))

## [0.5.0](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.4.0...lumina-node-uniffi-v0.5.0) - 2025-11-19

### Other

- *(node)* [**breaking**] Upgrade MSRV for `lumina-node` to 1.88 ([#814](https://github.com/eigerco/lumina/pull/814))
- *(uniffi)* build only lib for ios simulators ([#812](https://github.com/eigerco/lumina/pull/812))
- *(node-uniffi)* fix simulator hanging and optimize ios build, workflow ([#790](https://github.com/eigerco/lumina/pull/790))
- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.4.0](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.3.3...lumina-node-uniffi-v0.4.0) - 2025-09-25

### Added

- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(grpc)* [**breaking**] Merge TxClient and GrpcClient, add builder ([#712](https://github.com/eigerco/lumina/pull/712))

## [0.3.3](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.3.2...lumina-node-uniffi-v0.3.3) - 2025-09-08

### Other

- updated the following local packages: celestia-types, celestia-grpc, lumina-node

## [0.3.2](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.3.1...lumina-node-uniffi-v0.3.2) - 2025-08-19

### Other

- updated the following local packages: celestia-proto, celestia-types, lumina-node, celestia-grpc

## [0.3.1](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.3.0...lumina-node-uniffi-v0.3.1) - 2025-08-13

### Other

- update Cargo.toml dependencies

## [0.3.0](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.2.0...lumina-node-uniffi-v0.3.0) - 2025-07-29

### Added

- *(node)* [**breaking**] enforce 7 days sampling window - CIP-36 ([#698](https://github.com/eigerco/lumina/pull/698))

## [0.2.0](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.1.4...lumina-node-uniffi-v0.2.0) - 2025-07-02

### Added

- *(node)* [**breaking**] Implement adaptive backward syncing/sampling ([#606](https://github.com/eigerco/lumina/pull/606))

### Other

- Fix uninlined-format-args clippy ([#671](https://github.com/eigerco/lumina/pull/671))

## [0.1.4](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.1.3...lumina-node-uniffi-v0.1.4) - 2025-06-20

### Other

- updated the following local packages: lumina-node, celestia-grpc

## [0.1.3](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.1.2...lumina-node-uniffi-v0.1.3) - 2025-06-09

### Added

- *(node-uniffi)* Add grpc types and client for uniffi ([#627](https://github.com/eigerco/lumina/pull/627))

## [0.1.2](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.1.1...lumina-node-uniffi-v0.1.2) - 2025-05-26

### Other

- update Cargo.toml dependencies

## [0.1.1](https://github.com/eigerco/lumina/compare/lumina-node-uniffi-v0.1.0...lumina-node-uniffi-v0.1.1) - 2025-04-02

### Other

- *(node-uniffi)* Add minimal test case in Kotlin and Swift ([#535](https://github.com/eigerco/lumina/pull/535))

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/lumina-node-uniffi-v0.1.0) - 2025-02-24

### Added

- *(node-uniffi)* [**breaking**] Use in-memory stores if base path is not set (#530)
- adding uniffi bindings to support android and ios (#473)

### Fixed

- *(node-uniffi)* [**breaking**] Serialize correctly ExtendedHeader to json (#531)

### Other

- *(ci)* add releasing for node-uniffi (#545)
