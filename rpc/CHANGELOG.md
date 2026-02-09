# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.16.2](https://github.com/celestiaorg/lumina/compare/celestia-rpc-v0.16.1...celestia-rpc-v0.16.2) - 2026-01-27

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

## [0.16.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.16.0...celestia-rpc-v0.16.1) - 2026-01-08

### Added

- reconnect on RestartNeeded ([#863](https://github.com/eigerco/lumina/pull/863))

### Other

- moves non re-usable types from types to rpc ([#856](https://github.com/eigerco/lumina/pull/856))

## [0.16.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.15.0...celestia-rpc-v0.16.0) - 2026-01-05

### Added

- *(rpc,grpc,client)* [**breaking**] Add request, connection timeouts for gRPC, jRPC, celestia-client ([#819](https://github.com/eigerco/lumina/pull/819))
- *(proto,types,node)* [**breaking**] Implement shrex data availability protocol ([#857](https://github.com/eigerco/lumina/pull/857))
- *(client,rpc)* [**breaking**] don't require headers for share rpc calls ([#848](https://github.com/eigerco/lumina/pull/848))
- *(types,grpc)* [**breaking**] switch from Height to u64, infallible app_version() ([#846](https://github.com/eigerco/lumina/pull/846))

### Other

- adds AppVersion()::latest() in doc-comments ([#855](https://github.com/eigerco/lumina/pull/855))
- *(ci)* use compose healthchecks to ensure docker setup is ready ([#818](https://github.com/eigerco/lumina/pull/818))

## [0.15.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.14.0...celestia-rpc-v0.15.0) - 2025-11-19

### Added

- *(client,rpc)* [**breaking**] support auth in wasm rpc client ([#780](https://github.com/eigerco/lumina/pull/780))

### Other

- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.14.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.13.0...celestia-rpc-v0.14.0) - 2025-09-25

### Added

- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(types)* [**breaking**] singular `Blob::new` constructor ([#719](https://github.com/eigerco/lumina/pull/719))

## [0.13.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.12.1...celestia-rpc-v0.13.0) - 2025-09-08

### Added

- *(types)* [**breaking**] Add support for app v6 ([#733](https://github.com/eigerco/lumina/pull/733))

### Other

- *(client,grpc,types)* Make all the celestia-client types types Serialisable ([#734](https://github.com/eigerco/lumina/pull/734))

## [0.12.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.12.0...celestia-rpc-v0.12.1) - 2025-08-19

### Other

- updated the following local packages: celestia-proto, celestia-types

## [0.12.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.11.3...celestia-rpc-v0.12.0) - 2025-08-13

### Added

- *(proto,types,rpc)* [**breaking**] upgrade to celestia-node v0.25 ([#720](https://github.com/eigerco/lumina/pull/720))
- [**breaking**] Implement `celestia-client` crate ([#682](https://github.com/eigerco/lumina/pull/682))

### Fixed

- *(doc)* Fix incorrectly concatenated changelogs ([#713](https://github.com/eigerco/lumina/pull/713))

## [0.11.3](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.11.2...celestia-rpc-v0.11.3) - 2025-07-29

### Other

- *(rpc)* improve clarity of share get range ignoring parity test ([#681](https://github.com/eigerco/lumina/pull/681))

## [0.11.2](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.11.1...celestia-rpc-v0.11.2) - 2025-07-02

### Other

- updated the following local packages: celestia-types

## [0.11.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.11.0...celestia-rpc-v0.11.1) - 2025-06-09

### Other

- updated the following local packages: celestia-types

## [0.11.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.10.0...celestia-rpc-v0.11.0) - 2025-05-26

### Added

- *(rpc)* [**breaking**] switch to passing self and add new tx config fields ([#629](https://github.com/eigerco/lumina/pull/629))
- *(rpc)* [**breaking**] update jsonrpsee to version 0.25.1 and refactor RPC methods ([#620](https://github.com/eigerco/lumina/pull/620))
- *(rpc)* support the share.GetRow RPC endpoint ([#583](https://github.com/eigerco/lumina/pull/583))
- *(rpc)* Add Blobstream API ([#543](https://github.com/eigerco/lumina/pull/543))

### Other

- *(rpc)* Use `AuthLevel::Skip` and provide `new_test_client_with_url` ([#582](https://github.com/eigerco/lumina/pull/582))

## [0.10.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.9.1...celestia-rpc-v0.10.0) - 2025-04-02

### Added

- *(types)* [**breaking**] add support for share v1 and signer in blob ([#574](https://github.com/eigerco/lumina/pull/574))
- *(rpc)* add DAS module with sampling stats and catch-up methods ([#557](https://github.com/eigerco/lumina/pull/557))

## [0.9.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.9.0...celestia-rpc-v0.9.1) - 2025-02-24

### Other

- updated the following local packages: celestia-types

## [0.9.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.8.0...celestia-rpc-v0.9.0) - 2025-01-28

### Added

- *(node-wasm, types)* [**breaking**] Add method to get blobs for wasm (#468)
- *(types,rpc)* [**breaking**] move TxConfig to celestia-rpc (#485)

### Other

- *(ci)* migrate toolchain action, parallelize (#503)

## [0.8.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.7.1...celestia-rpc-v0.8.0) - 2024-12-02

### Added

- *(proto,types,rpc)* [**breaking**] celestia node v0.20.4 upgrade ([#469](https://github.com/eigerco/lumina/pull/469))
- *(grpc, types, proto)* [**breaking**] Add tonic gRPC ([#454](https://github.com/eigerco/lumina/pull/454))

### Other

- *(proto,types,node,grpc)* [**breaking**] Use `tendermint-rs` instead of `celestia-tendermint-rs` fork ([#463](https://github.com/eigerco/lumina/pull/463))
- *(node-wasm)* Add integration tests for node-wasm ([#420](https://github.com/eigerco/lumina/pull/420))

## [0.7.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.7.0...celestia-rpc-v0.7.1) - 2024-11-07

### Other

- updated the following local packages: celestia-types

## [0.7.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.6.0...celestia-rpc-v0.7.0) - 2024-10-25

### Added

- *(types)* [**breaking**] Add versioned consts ([#412](https://github.com/eigerco/lumina/pull/412))
- *(types)* [**breaking**] add blob reconstruction from shares ([#450](https://github.com/eigerco/lumina/pull/450))
- *(types,rpc,node)* [**breaking**] refactor Share to work for parity and data ([#443](https://github.com/eigerco/lumina/pull/443))

## [0.6.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.5.0...celestia-rpc-v0.6.0) - 2024-10-11

### Fixed

- *(rpc)* [**breaking**] use correct blob type in state submit pay for blob ([#418](https://github.com/eigerco/lumina/pull/418))

### Other

- *(rpc)* Remove a workaround for go-jsonrpc not returning null ok status correctly ([#422](https://github.com/eigerco/lumina/pull/422))

## [0.5.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.4.1...celestia-rpc-v0.5.0) - 2024-10-03

### Added

- [**breaking**] shwap protocol updates ([#369](https://github.com/eigerco/lumina/pull/369))

## [0.4.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.4.0...celestia-rpc-v0.4.1) - 2024-09-24

### Added

- feat!(types,rpc): Add share, row, merkle proofs and share.GetRange ([#375](https://github.com/eigerco/lumina/pull/375))

## [0.4.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.3.0...celestia-rpc-v0.4.0) - 2024-08-22

### Added
- updating API for parity with celestia-node v0.15.0 ([#340](https://github.com/eigerco/lumina/pull/340))

## [0.3.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.2.0...celestia-rpc-v0.3.0) - 2024-08-13

### Added
- Add `#[track_caller]` on test utils and spawn utils ([#305](https://github.com/eigerco/lumina/pull/305))
- *(node/syncer)* [**breaking**] Implement backwards header sync ([#279](https://github.com/eigerco/lumina/pull/279))
- *(rpc)* Implement WASM Client ([#210](https://github.com/eigerco/lumina/pull/210))
- feat!(types): Add Blob::index field introduced in celestia 0.13 ([#274](https://github.com/eigerco/lumina/pull/274))

### Fixed
- *(rpc)* Increase max response size ([#336](https://github.com/eigerco/lumina/pull/336))
- new lints coming with 1.78 and 1.80-nightly ([#275](https://github.com/eigerco/lumina/pull/275))

### Other
- Upgrade jsonprsee to 0.24.2 ([#360](https://github.com/eigerco/lumina/pull/360))
- [**breaking**] Upgrade to nmt-rs 0.2.0 ([#322](https://github.com/eigerco/lumina/pull/322))
- [**breaking**] Upgrade dependencies but exclude the ones that are patched by risc0 ([#292](https://github.com/eigerco/lumina/pull/292))

## [0.2.0](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.1.1...celestia-rpc-v0.2.0) - 2024-04-18

### Added
- [**breaking**] Refactor RowId/SampleId/NamespacedDataId related API ([#236](https://github.com/eigerco/lumina/pull/236))
- feat!(node): Implement DASer ([#223](https://github.com/eigerco/lumina/pull/223))

### Other
- chore!(types,rpc): compatibility with celestia-node 0.13.1 ([#239](https://github.com/eigerco/lumina/pull/239))

## [0.1.1](https://github.com/eigerco/lumina/compare/celestia-rpc-v0.1.0...celestia-rpc-v0.1.1) - 2024-01-15

### Other
- add authors and homepage ([#180](https://github.com/eigerco/lumina/pull/180))

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/celestia-rpc-v0.1.0) - 2024-01-12

### Added
- *(types)* Add `wasm-bindgen` feature flag ([#143](https://github.com/eigerco/lumina/pull/143))
- *(node)* Implement running node in browser ([#112](https://github.com/eigerco/lumina/pull/112))
- *(rpc)* align to celestia-node 0.12.0 ([#125](https://github.com/eigerco/lumina/pull/125))
- *(rpc)* create wrappers for jsonrpsee clients ([#114](https://github.com/eigerco/lumina/pull/114))
- *(node)* Implement Syncer ([#94](https://github.com/eigerco/lumina/pull/94))
- Improve verification and implement verification in Exchange client ([#85](https://github.com/eigerco/lumina/pull/85))
- add RPC calls for p2p module and tests for them ([#52](https://github.com/eigerco/lumina/pull/52))
- Implement initial architecture of node crate ([#42](https://github.com/eigerco/lumina/pull/42))
- *(fraud)* Add fraud proof trait and byzantine encoding fraud ([#32](https://github.com/eigerco/lumina/pull/32))
- Add State RPC and types ([#31](https://github.com/eigerco/lumina/pull/31))
- *(rpc)* Add all calls for Blob, Share, and Header ([#24](https://github.com/eigerco/lumina/pull/24))
- align namespaced shares deserialization with latest celestia  ([#20](https://github.com/eigerco/lumina/pull/20))
- serialize proof and commitment ([#19](https://github.com/eigerco/lumina/pull/19))
- *(rpc)* Create celestia-rpc crate and add integration tests ([#17](https://github.com/eigerco/lumina/pull/17))

### Fixed
- *(types)* fix the json representation of SubmitOptions ([#66](https://github.com/eigerco/lumina/pull/66))
- make celestia-rpc to compile in wasm32 target ([#46](https://github.com/eigerco/lumina/pull/46))

### Other
- add missing metadata to the toml files ([#170](https://github.com/eigerco/lumina/pull/170))
- document public api ([#161](https://github.com/eigerco/lumina/pull/161))
- add validation of EDS in tests ([#165](https://github.com/eigerco/lumina/pull/165))
- Make sure we run clippy for wasm and fix wasm build/lints ([#115](https://github.com/eigerco/lumina/pull/115))
- Migrate to nmt-rs of crates.io ([#144](https://github.com/eigerco/lumina/pull/144))
- Upgrade libp2p to v0.53.0 ([#126](https://github.com/eigerco/lumina/pull/126))
- update celestia node to 0.11.0-rc15 ([#101](https://github.com/eigerco/lumina/pull/101))
- trim the features of workspace dependencies ([#99](https://github.com/eigerco/lumina/pull/99))
- align to celestia node v0.11-rc ([#65](https://github.com/eigerco/lumina/pull/65))
- Migrate from `log` to `tracing` ([#55](https://github.com/eigerco/lumina/pull/55))
- update jsonrpsee to 0.20 ([#41](https://github.com/eigerco/lumina/pull/41))
