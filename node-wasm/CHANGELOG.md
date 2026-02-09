# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.13.2](https://github.com/celestiaorg/lumina/compare/lumina-node-wasm-v0.13.1...lumina-node-wasm-v0.13.2) - 2026-01-27

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

### Other

- *(node-wasm)* fix flaky head headers comparison ([#878](https://github.com/celestiaorg/lumina/pull/878))

## [0.13.1](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.13.0...lumina-node-wasm-v0.13.1) - 2026-01-08

### Other

- moves non re-usable types from types to rpc ([#856](https://github.com/eigerco/lumina/pull/856))

## [0.13.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.12.0...lumina-node-wasm-v0.13.0) - 2026-01-05

### Added

- *(rpc,grpc,client)* [**breaking**] Add request, connection timeouts for gRPC, jRPC, celestia-client ([#819](https://github.com/eigerco/lumina/pull/819))
- *(grpc)* AsyncGrpcCall: Send ([#844](https://github.com/eigerco/lumina/pull/844))
- *(node,node-wasm,node-uniffi)* Add header, blob, and namespace subscriptions ([#772](https://github.com/eigerco/lumina/pull/772))

### Other

- *(node-wasm)* add proper tsconfig.json ([#839](https://github.com/eigerco/lumina/pull/839))
- *(node-wasm)* fix flaky request_network_head_header ([#837](https://github.com/eigerco/lumina/pull/837))
- fix tsc not finding iterators on release ([#838](https://github.com/eigerco/lumina/pull/838))

## [0.12.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.11.0...lumina-node-wasm-v0.12.0) - 2025-11-19

### Added

- *(client,rpc)* [**breaking**] support auth in wasm rpc client ([#780](https://github.com/eigerco/lumina/pull/780))
- *(node)* Node identity retention for native,ios,android ([#751](https://github.com/eigerco/lumina/pull/751))

### Fixed

- *(ci)* Workaround `discover_network_peers` test of `lumina-node-wasm` ([#801](https://github.com/eigerco/lumina/pull/801))

### Other

- *(node)* [**breaking**] Upgrade MSRV for `lumina-node` to 1.88 ([#814](https://github.com/eigerco/lumina/pull/814))
- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.11.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.10.3...lumina-node-wasm-v0.11.0) - 2025-09-25

### Added

- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(grpc)* [**breaking**] Merge TxClient and GrpcClient, add builder ([#712](https://github.com/eigerco/lumina/pull/712))
- *(types)* [**breaking**] singular `Blob::new` constructor ([#719](https://github.com/eigerco/lumina/pull/719))
- *(node-wasm)* Identity persistance in browser ([#723](https://github.com/eigerco/lumina/pull/723))

## [0.10.3](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.10.2...lumina-node-wasm-v0.10.3) - 2025-09-08

### Other

- updated the following local packages: celestia-types, celestia-rpc, celestia-grpc, lumina-node

## [0.10.2](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.10.1...lumina-node-wasm-v0.10.2) - 2025-08-19

### Other

- updated the following local packages: celestia-types, celestia-rpc, lumina-node, celestia-grpc

## [0.10.1](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.10.0...lumina-node-wasm-v0.10.1) - 2025-08-13

### Fixed

- *(doc)* Fix incorrectly concatenated changelogs ([#713](https://github.com/eigerco/lumina/pull/713))

## [0.10.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.9.0...lumina-node-wasm-v0.10.0) - 2025-07-29

### Added

- *(node)* [**breaking**] enforce 7 days sampling window - CIP-36 ([#698](https://github.com/eigerco/lumina/pull/698))

## [0.9.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.8.5...lumina-node-wasm-v0.9.0) - 2025-07-02

### Added

- *(node)* [**breaking**] Use `block_height` for `Node::request_all_blobs` and return error on missing header ([#669](https://github.com/eigerco/lumina/pull/669))
- *(node)* [**breaking**] Implement adaptive backward syncing/sampling ([#606](https://github.com/eigerco/lumina/pull/606))

## [0.8.5](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.8.4...lumina-node-wasm-v0.8.5) - 2025-06-20

### Other

- updated the following local packages: lumina-node, celestia-grpc

## [0.8.4](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.8.3...lumina-node-wasm-v0.8.4) - 2025-06-09

### Other

- updated the following local packages: celestia-types, lumina-node, celestia-grpc, celestia-rpc

## [0.8.3](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.8.2...lumina-node-wasm-v0.8.3) - 2025-05-26

### Other

- update Cargo.toml dependencies

## [0.8.2](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.8.1...lumina-node-wasm-v0.8.2) - 2025-04-02

### Added

- *(node-wasm)* Multiplex message channel connection to worker ([#563](https://github.com/eigerco/lumina/pull/563))
- *(node)* Resolve dnsaddr in lumina-node ([#577](https://github.com/eigerco/lumina/pull/577))
- lumina-utils crate ([#564](https://github.com/eigerco/lumina/pull/564))
- *(ci)* allow other node types than bridge ([#562](https://github.com/eigerco/lumina/pull/562))

## [0.8.1](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.8.0...lumina-node-wasm-v0.8.1) - 2025-02-24

### Other

- *(ci)* fix outdated lockfiles when releasing to npm (#544)
- remove outdated shwap note and link to changelog for js (#536)

## [0.8.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.7.0...lumina-node-wasm-v0.8.0) - 2025-01-28

Summary:

- New options on `NodeConfig`, disabling usage of indexeddb with `usePersistentMemory` and pruning configuration with `customPruningDelaySecs`.
- Added `AppVersion`, `Blob`, `Namespace` types and support for fetching blobs from network using `NodeClient.requestAllBlobs`.
- Added grpc `TxClient` for blob and cosmos messages submission to the network, as well as basic queries for `auth` and `bank`.
- `ExtendedHeader` and `DataAvailabilityHeader` are now exposed as classes from wasm instead of being used as jsons.
  This means they now have methods and proper typescript support.

For migration:

- `header.dah.columnRoot()` and `header.dah.rowRoot()` are now methods that return `Uint8Array` instead of being
  properties returning base64 `string`s
- `NodeConfig.custom_syncing_window_secs` was renamed to `NodeConfig.customSamplingWindowSecs`

### Added

- *(grpc,node-wasm)* add javascript bindings for tx client (#510)
- *(node-wasm, types)* [**breaking**] Add method to get blobs for wasm (#468)
- Add remaining node types for wasm (#476)
- *(node-wasm)* Add more configuration options in `NodeConfig` (#487)
- *(node)* [**breaking**] Implement `NodeBuilder` and remove `NodeConfig` (#472)

### Fixed

- *(node-wasm)* [**breaking**] use camelCase in config fields (#528)

### Other

- *(ci)* migrate toolchain action, parallelize (#503)
- *(node-wasm)* Update js build dependencies, commit package lock (#478)
- *(node,node-wasm)* [**breaking**] Rename `syncing_window` to `sampling_window` (#477)

## [0.7.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.6.1...lumina-node-wasm-v0.7.0) - 2024-12-02

### Added

- *(grpc, types, proto)* [**breaking**] Add tonic gRPC ([#454](https://github.com/eigerco/lumina/pull/454))

### Other

- *(proto,types,node,grpc)* [**breaking**] Use `tendermint-rs` instead of `celestia-tendermint-rs` fork ([#463](https://github.com/eigerco/lumina/pull/463))
- *(node-wasm)* Add integration tests for node-wasm ([#420](https://github.com/eigerco/lumina/pull/420))

## [0.6.1](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.6.0...lumina-node-wasm-v0.6.1) - 2024-11-07

### Other

- update lumina-node npm package

## [0.6.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.5.2...lumina-node-wasm-v0.6.0) - 2024-10-25

### Added

- *(node,node-wasm)* [**breaking**] Allow customising syncing window size ([#442](https://github.com/eigerco/lumina/pull/442))

## [0.5.2](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.5.1...lumina-node-wasm-v0.5.2) - 2024-10-21

### Fixed

- *(node-wasm)* workaround to clean extra message from worker ([#444](https://github.com/eigerco/lumina/pull/444))

## [0.5.1](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.5.0...lumina-node-wasm-v0.5.1) - 2024-10-11

### Added

- setup local demo page with webpack ([#388](https://github.com/eigerco/lumina/pull/388))

## [0.5.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.3.0...lumina-node-wasm-v0.5.0) - 2024-10-03

### Added

- *(node,node-wasm)* [**breaking**] Integrate graceful shutdown in WASM ([#396](https://github.com/eigerco/lumina/pull/396))

### Other

- *(node-wasm)* add automatic generation of the types file and README ([#408](https://github.com/eigerco/lumina/pull/408))
- Include API in readme  ([#409](https://github.com/eigerco/lumina/pull/409))
- *(node-wasm)* Switch to bundler target for wasm-pack for manifest v3 ([#398](https://github.com/eigerco/lumina/pull/398))
- *(node-wasm)* clarify edge case when polling worker on startup ([#390](https://github.com/eigerco/lumina/pull/390))

## 0.4.0

This version was skipped because by accident 0.3.0 was released to npmjs also as 0.3.1 and 0.4.0.

## [0.3.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.2.0...lumina-node-wasm-v0.3.0) - 2024-09-24

### Added

- *(node-wasm)* [**breaking**] Align JS api to use camelCase ([#383](https://github.com/eigerco/lumina/pull/383))
- *(node-wasm)* [**breaking**] Webpack compatibility ([#377](https://github.com/eigerco/lumina/pull/377))
- *(node)* [**breaking**] Implement graceful shutdown ([#343](https://github.com/eigerco/lumina/pull/343))

### Fixed

- *(node-wasm)* wait for worker to start when creating client ([#389](https://github.com/eigerco/lumina/pull/389))

### Other

- *(ci)* Automatic release to npmjs ([#378](https://github.com/eigerco/lumina/pull/378))

## [0.2.0](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.1.1...lumina-node-wasm-v0.2.0) - 2024-08-13

### Added
- *(node-wasm)* [**breaking**] Add websocket support ([#341](https://github.com/eigerco/lumina/pull/341))
- feat!(node): make syncer batch sizes configurable ([#327](https://github.com/eigerco/lumina/pull/327))
- add support for dnsaddr resolving in browser ([#319](https://github.com/eigerco/lumina/pull/319))
- Add requesting storage persistence for more quota ([#318](https://github.com/eigerco/lumina/pull/318))
- *(node)* Generate syncing related events ([#312](https://github.com/eigerco/lumina/pull/312))
- *(wasm)* Run Lumina in a Shared Worker ([#265](https://github.com/eigerco/lumina/pull/265))
- *(node)* [**breaking**] Make HeaderSession operate on a single header range again ([#303](https://github.com/eigerco/lumina/pull/303))
- *(node/syncer)* [**breaking**] Implement backwards header sync ([#279](https://github.com/eigerco/lumina/pull/279))
- *(node-wasm)* Add human readable message on node events ([#294](https://github.com/eigerco/lumina/pull/294))
- *(node-wasm)* Implement easier way for handling JS errors. ([#284](https://github.com/eigerco/lumina/pull/284))
- *(node)* Generate events for data sampling that can be used by front-end ([#276](https://github.com/eigerco/lumina/pull/276))
- *(node/daser)* [**breaking**] Implement backward sampling and sampling window ([#269](https://github.com/eigerco/lumina/pull/269))

### Fixed
- *(node-wasm)* Fix high memory consumption ([#356](https://github.com/eigerco/lumina/pull/356))
- *(node)* [**breaking**] Do not skip header-sub reports when store writes are slow ([#333](https://github.com/eigerco/lumina/pull/333))
- *(node)* Patch unreleased libp2p version to include syncing bug fixes ([#290](https://github.com/eigerco/lumina/pull/290))
- *(node-wasm)* require serving and providing worker script ([#313](https://github.com/eigerco/lumina/pull/313))
- new lints coming with 1.78 and 1.80-nightly ([#275](https://github.com/eigerco/lumina/pull/275))

### Other
- *(node)* [**breaking**] Hide internal components ([#342](https://github.com/eigerco/lumina/pull/342))
- remove genesis hash from node configuration ([#316](https://github.com/eigerco/lumina/pull/316))
- [**breaking**] Upgrade dependencies but exclude the ones that are patched by risc0 ([#292](https://github.com/eigerco/lumina/pull/292))

## [0.1.1](https://github.com/eigerco/lumina/compare/lumina-node-wasm-v0.1.0...lumina-node-wasm-v0.1.1) - 2024-04-18

### Added
- Expose get_sampling_metadata in node and node-wasm ([#234](https://github.com/eigerco/lumina/pull/234))
- *(blockstore)* add IndexedDb blockstore ([#221](https://github.com/eigerco/lumina/pull/221))
- feat!(node): use generic blockstore in node ([#218](https://github.com/eigerco/lumina/pull/218))

### Fixed
- *(ci)* Fix release for lumina (cli) ([#190](https://github.com/eigerco/lumina/pull/190))

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/lumina-node-wasm-v0.1.0) - 2024-01-12

### Added
- Return bootstrap peers as iterator and filter relevant ones ([#147](https://github.com/eigerco/lumina/pull/147))
- *(node)* Implement running node in browser ([#112](https://github.com/eigerco/lumina/pull/112))

### Other
- add missing metadata to the toml files ([#170](https://github.com/eigerco/lumina/pull/170))
- document public api ([#161](https://github.com/eigerco/lumina/pull/161))
- rename the node implementation to Lumina ([#156](https://github.com/eigerco/lumina/pull/156))
- switch browser logging to tracing-web ([#150](https://github.com/eigerco/lumina/pull/150))
