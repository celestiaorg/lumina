# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0](https://github.com/celestiaorg/lumina/compare/celestia-grpc-macros-v0.6.0...celestia-grpc-macros-v0.7.0) - 2026-01-27

### Added

- *(grpc)* [**breaking**] add per-endpoint configuration for metadata and timeouts ([#875](https://github.com/celestiaorg/lumina/pull/875))

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

## [0.6.0](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.5.0...celestia-grpc-macros-v0.6.0) - 2026-01-05

### Added

- *(rpc,grpc,client)* [**breaking**] Add request, connection timeouts for gRPC, jRPC, celestia-client ([#819](https://github.com/eigerco/lumina/pull/819))
- *(grpc)* use multiple endpoints, fallback in case of errors ([#836](https://github.com/eigerco/lumina/pull/836))

## [0.5.0](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.4.0...celestia-grpc-macros-v0.5.0) - 2025-11-19

### Other

- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.4.0](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.3.1...celestia-grpc-macros-v0.4.0) - 2025-09-25

### Added

- *(grpc)* [**breaking**] Add support for attaching metadata to requests ([#748](https://github.com/eigerco/lumina/pull/748))
- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(grpc)* [**breaking**] Merge TxClient and GrpcClient, add builder ([#712](https://github.com/eigerco/lumina/pull/712))

## [0.3.1](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.3.0...celestia-grpc-macros-v0.3.1) - 2025-09-08

### Added

- *(client,grpc)* make sure all returned futures are Send ([#729](https://github.com/eigerco/lumina/pull/729))

## [0.3.0](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.2.1...celestia-grpc-macros-v0.3.0) - 2025-07-29

### Added

- *(grpc)* [**breaking**] Add support for Gas Estimation Service ([#680](https://github.com/eigerco/lumina/pull/680))

## [0.2.1](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.2.0...celestia-grpc-macros-v0.2.1) - 2025-04-02

### Added

- *(grpc)* increase max message size to handle big celestia blocks ([#559](https://github.com/eigerco/lumina/pull/559))

## [0.2.0](https://github.com/eigerco/lumina/compare/celestia-grpc-macros-v0.1.0...celestia-grpc-macros-v0.2.0) - 2025-01-28

### Added

- *(grpc)* [**breaking**] add wasm support and transaction client (#474)

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/celestia-grpc-macros-v0.1.0) - 2024-12-02

### Added

- *(grpc, types, proto)* [**breaking**] Add tonic gRPC ([#454](https://github.com/eigerco/lumina/pull/454))
