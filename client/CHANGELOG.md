# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0](https://github.com/celestiaorg/lumina/compare/celestia-client-v0.4.1...celestia-client-v0.5.0) - 2026-01-27

### Added

- *(grpc)* [**breaking**] add per-endpoint configuration for metadata and timeouts ([#875](https://github.com/celestiaorg/lumina/pull/875))

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

## [0.4.1](https://github.com/eigerco/lumina/compare/celestia-client-v0.4.0...celestia-client-v0.4.1) - 2026-01-08

### Fixed

- *(client)* Fix CI errors, adding all possible error codes ([#866](https://github.com/eigerco/lumina/pull/866))

## [0.4.0](https://github.com/eigerco/lumina/compare/celestia-client-v0.3.0...celestia-client-v0.4.0) - 2026-01-05

### Added

- *(rpc,grpc,client)* [**breaking**] Add request, connection timeouts for gRPC, jRPC, celestia-client ([#819](https://github.com/eigerco/lumina/pull/819))
- *(proto,types,node)* [**breaking**] Implement shrex data availability protocol ([#857](https://github.com/eigerco/lumina/pull/857))
- *(client,rpc)* [**breaking**] don't require headers for share rpc calls ([#848](https://github.com/eigerco/lumina/pull/848))
- *(types,grpc)* [**breaking**] switch from Height to u64, infallible app_version() ([#846](https://github.com/eigerco/lumina/pull/846))
- *(grpc)* use multiple endpoints, fallback in case of errors ([#836](https://github.com/eigerco/lumina/pull/836))

### Other

- adds AppVersion()::latest() in doc-comments ([#855](https://github.com/eigerco/lumina/pull/855))

## [0.3.0](https://github.com/eigerco/lumina/compare/celestia-client-v0.2.0...celestia-client-v0.3.0) - 2025-11-19

### Added

- *(client,rpc)* [**breaking**] support auth in wasm rpc client ([#780](https://github.com/eigerco/lumina/pull/780))
- *(client,grpc)* [**breaking**] expose more tls config options, error if tls is not supported ([#796](https://github.com/eigerco/lumina/pull/796))
- *(grpc)* resigning and resubmission of transactions ([#768](https://github.com/eigerco/lumina/pull/768))

### Fixed

- *(client)* [**breaking**] use AsyncGrpcCall also in BlobApi::submit ([#760](https://github.com/eigerco/lumina/pull/760))

### Other

- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.2.0](https://github.com/eigerco/lumina/compare/celestia-client-v0.1.2...celestia-client-v0.2.0) - 2025-09-25

### Added

- *(grpc)* [**breaking**] Add support for attaching metadata to requests ([#748](https://github.com/eigerco/lumina/pull/748))
- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(grpc,client)* Allow creating celestia-client with read-only grpc ([#755](https://github.com/eigerco/lumina/pull/755))
- *(grpc)* [**breaking**] Merge TxClient and GrpcClient, add builder ([#712](https://github.com/eigerco/lumina/pull/712))
- *(types)* [**breaking**] singular `Blob::new` constructor ([#719](https://github.com/eigerco/lumina/pull/719))

## [0.1.2](https://github.com/eigerco/lumina/compare/celestia-client-v0.1.1...celestia-client-v0.1.2) - 2025-09-08

### Added

- *(client,grpc)* make sure all returned futures are Send ([#729](https://github.com/eigerco/lumina/pull/729))

### Other

- *(client,grpc,types)* Make all the celestia-client types types Serialisable ([#734](https://github.com/eigerco/lumina/pull/734))

## [0.1.1](https://github.com/eigerco/lumina/compare/celestia-client-v0.1.0...celestia-client-v0.1.1) - 2025-08-19

### Other

- updated the following local packages: celestia-proto, celestia-types, celestia-types, celestia-rpc, celestia-rpc, celestia-grpc, celestia-grpc

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/celestia-client-v0.1.0) - 2025-08-13

### Added

- *(proto,types,rpc)* [**breaking**] upgrade to celestia-node v0.25 ([#720](https://github.com/eigerco/lumina/pull/720))
- [**breaking**] Implement `celestia-client` crate ([#682](https://github.com/eigerco/lumina/pull/682))
