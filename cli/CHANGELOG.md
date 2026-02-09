# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.13.0](https://github.com/celestiaorg/lumina/compare/lumina-cli-v0.12.1...lumina-cli-v0.13.0) - 2026-01-27

### Added

- *(grpc)* [**breaking**] add per-endpoint configuration for metadata and timeouts ([#875](https://github.com/celestiaorg/lumina/pull/875))

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

## [0.12.1](https://github.com/eigerco/lumina/compare/lumina-cli-v0.12.0...lumina-cli-v0.12.1) - 2026-01-08

### Other

- updated the following local packages: celestia-types, celestia-rpc, lumina-node

## [0.12.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.11.0...lumina-cli-v0.12.0) - 2026-01-05

### Added

- *(rpc,grpc,client)* [**breaking**] Add request, connection timeouts for gRPC, jRPC, celestia-client ([#819](https://github.com/eigerco/lumina/pull/819))
- *(proto,types,node)* [**breaking**] Implement shrex data availability protocol ([#857](https://github.com/eigerco/lumina/pull/857))
- *(node,node-wasm,node-uniffi)* Add header, blob, and namespace subscriptions ([#772](https://github.com/eigerco/lumina/pull/772))

## [0.11.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.10.0...lumina-cli-v0.11.0) - 2025-11-19

### Added

- *(client,rpc)* [**breaking**] support auth in wasm rpc client ([#780](https://github.com/eigerco/lumina/pull/780))

### Other

- *(node)* [**breaking**] Upgrade MSRV for `lumina-node` to 1.88 ([#814](https://github.com/eigerco/lumina/pull/814))
- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.10.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.9.3...lumina-cli-v0.10.0) - 2025-09-25

### Added

- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(grpc)* [**breaking**] Merge TxClient and GrpcClient, add builder ([#712](https://github.com/eigerco/lumina/pull/712))

## [0.9.3](https://github.com/eigerco/lumina/compare/lumina-cli-v0.9.2...lumina-cli-v0.9.3) - 2025-09-08

### Other

- updated the following local packages: celestia-types, celestia-rpc, lumina-node

## [0.9.2](https://github.com/eigerco/lumina/compare/lumina-cli-v0.9.1...lumina-cli-v0.9.2) - 2025-08-19

### Other

- updated the following local packages: celestia-types, celestia-rpc, lumina-node

## [0.9.1](https://github.com/eigerco/lumina/compare/lumina-cli-v0.9.0...lumina-cli-v0.9.1) - 2025-08-13

### Fixed

- *(doc)* Fix incorrectly concatenated changelogs ([#713](https://github.com/eigerco/lumina/pull/713))

## [0.9.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.8.0...lumina-cli-v0.9.0) - 2025-07-29

### Added

- *(node)* [**breaking**] enforce 7 days sampling window - CIP-36 ([#698](https://github.com/eigerco/lumina/pull/698))
- *(grpc)* [**breaking**] Add support for Gas Estimation Service ([#680](https://github.com/eigerco/lumina/pull/680))
- *(grpc,types,node)* [**breaking**] Wasm grpc client ([#654](https://github.com/eigerco/lumina/pull/654))
- *(grpc)* Streamline TxClient creation API, add docs with example ([#673](https://github.com/eigerco/lumina/pull/673))

## [0.8.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.7.0...lumina-cli-v0.8.0) - 2025-07-02

### Added

- *(node)* [**breaking**] Implement adaptive backward syncing/sampling ([#606](https://github.com/eigerco/lumina/pull/606))

## [0.7.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.6.4...lumina-cli-v0.7.0) - 2025-06-20

### Added

- *(grpc)* [**breaking**] add memo field to TxConfig ([#659](https://github.com/eigerco/lumina/pull/659))

## [0.6.4](https://github.com/eigerco/lumina/compare/lumina-cli-v0.6.3...lumina-cli-v0.6.4) - 2025-06-09

### Other

- update Cargo.lock dependencies

## [0.6.3](https://github.com/eigerco/lumina/compare/lumina-cli-v0.6.2...lumina-cli-v0.6.3) - 2025-05-26

### Other

- update Cargo.lock dependencies

## [0.6.2](https://github.com/eigerco/lumina/compare/lumina-cli-v0.6.1...lumina-cli-v0.6.2) - 2025-04-02

### Added

- *(node-wasm)* Multiplex message channel connection to worker ([#563](https://github.com/eigerco/lumina/pull/563))
- *(ci)* allow other node types than bridge ([#562](https://github.com/eigerco/lumina/pull/562))

## [0.6.1](https://github.com/eigerco/lumina/compare/lumina-cli-v0.6.0...lumina-cli-v0.6.1) - 2025-02-24

### Other

- updated the following local packages: celestia-types

## [0.6.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.5.2...lumina-cli-v0.6.0) - 2025-01-28

### Added

- *(grpc,node-wasm)* add javascript bindings for tx client (#510)
- Add remaining node types for wasm (#476)
- *(cli)* Add `in-memory-store` and `pruning-delay` parameters (#490)
- *(node)* [**breaking**] Implement `NodeBuilder` and remove `NodeConfig` (#472)

### Fixed

- *(cli)* align with dah javascript breaking changes (#501)

### Other

- *(ci)* migrate toolchain action, parallelize (#503)
- *(node,node-wasm)* [**breaking**] Rename `syncing_window` to `sampling_window` (#477)

## [0.5.2](https://github.com/eigerco/lumina/compare/lumina-cli-v0.5.1...lumina-cli-v0.5.2) - 2024-12-02

### Added

- *(grpc, types, proto)* [**breaking**] Add tonic gRPC ([#454](https://github.com/eigerco/lumina/pull/454))

### Other

- *(node-wasm)* Add integration tests for node-wasm ([#420](https://github.com/eigerco/lumina/pull/420))

## [0.5.1](https://github.com/eigerco/lumina/compare/lumina-cli-v0.5.0...lumina-cli-v0.5.1) - 2024-11-07

### Other

- updated the following local packages: celestia-types, lumina-node

## [0.5.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.4.1...lumina-cli-v0.5.0) - 2024-10-25

### Added

- *(node,node-wasm)* [**breaking**] Allow customising syncing window size ([#442](https://github.com/eigerco/lumina/pull/442))

## [0.4.1](https://github.com/eigerco/lumina/compare/lumina-cli-v0.4.0...lumina-cli-v0.4.1) - 2024-10-11

### Added

- setup local demo page with webpack ([#388](https://github.com/eigerco/lumina/pull/388))

## [0.4.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.3.1...lumina-cli-v0.4.0) - 2024-10-03

### Added

- *(node,node-wasm)* [**breaking**] Integrate graceful shutdown in WASM ([#396](https://github.com/eigerco/lumina/pull/396))

## [0.3.1](https://github.com/eigerco/lumina/compare/lumina-cli-v0.3.0...lumina-cli-v0.3.1) - 2024-09-24

### Other

- update Cargo.lock dependencies

## [0.3.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.2.0...lumina-cli-v0.3.0) - 2024-08-13

### Added
- feat!(node): make syncer batch sizes configurable ([#327](https://github.com/eigerco/lumina/pull/327))
- add support for dnsaddr resolving in browser ([#319](https://github.com/eigerco/lumina/pull/319))
- *(node)* Generate syncing related events ([#312](https://github.com/eigerco/lumina/pull/312))
- *(wasm)* Run Lumina in a Shared Worker ([#265](https://github.com/eigerco/lumina/pull/265))
- *(node/syncer)* [**breaking**] Implement backwards header sync ([#279](https://github.com/eigerco/lumina/pull/279))
- *(node)* Generate events for data sampling that can be used by front-end ([#276](https://github.com/eigerco/lumina/pull/276))

### Fixed
- *(node-wasm)* require serving and providing worker script ([#313](https://github.com/eigerco/lumina/pull/313))

### Other
- remove genesis hash from node configuration ([#316](https://github.com/eigerco/lumina/pull/316))
- [**breaking**] Upgrade dependencies but exclude the ones that are patched by risc0 ([#292](https://github.com/eigerco/lumina/pull/292))

## [0.2.0](https://github.com/eigerco/lumina/compare/lumina-cli-v0.1.0...lumina-cli-v0.2.0) - 2024-04-18

### Added
- *(cli)* [**breaking**] Replace sled stores with redb stores ([#267](https://github.com/eigerco/lumina/pull/267))
- feat!(node): use generic blockstore in node ([#218](https://github.com/eigerco/lumina/pull/218))

### Other
- Add note about WebTransport requiring Secure Context ([#211](https://github.com/eigerco/lumina/pull/211))

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/lumina-cli-v0.1.0) - 2024-01-12

### Other
- add missing metadata to the toml files ([#170](https://github.com/eigerco/lumina/pull/170))
- document public api ([#161](https://github.com/eigerco/lumina/pull/161))
- error message for missing token and cleanups ([#168](https://github.com/eigerco/lumina/pull/168))
- rename the node implementation to Lumina ([#156](https://github.com/eigerco/lumina/pull/156))
