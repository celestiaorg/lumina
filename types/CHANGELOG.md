# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.20.0](https://github.com/celestiaorg/lumina/compare/celestia-types-v0.19.0...celestia-types-v0.20.0) - 2026-01-27

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

### Other

- *(types)* [**breaking**] Support celestia-app V7 ([#877](https://github.com/celestiaorg/lumina/pull/877))

## [0.19.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.18.0...celestia-types-v0.19.0) - 2026-01-08

### Other

- moves non re-usable types from types to rpc ([#856](https://github.com/eigerco/lumina/pull/856))

## [0.18.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.17.0...celestia-types-v0.18.0) - 2026-01-05

### Added

- *(proto,types,node)* [**breaking**] Implement shrex data availability protocol ([#857](https://github.com/eigerco/lumina/pull/857))
- *(types,grpc)* [**breaking**] switch from Height to u64, infallible app_version() ([#846](https://github.com/eigerco/lumina/pull/846))
- *(node,node-wasm,node-uniffi)* Add header, blob, and namespace subscriptions ([#772](https://github.com/eigerco/lumina/pull/772))

### Other

- adds AppVersion()::latest() in doc-comments ([#855](https://github.com/eigerco/lumina/pull/855))

## [0.17.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.16.0...celestia-types-v0.17.0) - 2025-11-19

### Added

- *(grpc)* resigning and resubmission of transactions ([#768](https://github.com/eigerco/lumina/pull/768))
- *(node)* Node identity retention for native,ios,android ([#751](https://github.com/eigerco/lumina/pull/751))

### Fixed

- *(types,grpc)* [**breaking**] Address::from_account_verifying_key spelling #764

### Other

- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.16.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.15.0...celestia-types-v0.16.0) - 2025-09-25

### Added

- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(types)* [**breaking**] singular `Blob::new` constructor ([#719](https://github.com/eigerco/lumina/pull/719))

### Other

- Fix clippy::manual-is-multiple-of on new rust version ([#752](https://github.com/eigerco/lumina/pull/752))

## [0.15.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.14.1...celestia-types-v0.15.0) - 2025-09-08

### Added

- *(types)* [**breaking**] Add support for app v6 ([#733](https://github.com/eigerco/lumina/pull/733))

### Other

- *(client,grpc,types)* Make all the celestia-client types types Serialisable ([#734](https://github.com/eigerco/lumina/pull/734))

## [0.14.1](https://github.com/eigerco/lumina/compare/celestia-types-v0.14.0...celestia-types-v0.14.1) - 2025-08-19

### Fixed

- *(proto,types)* fix binary serialization of maybe_quoted ([#724](https://github.com/eigerco/lumina/pull/724))

## [0.14.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.13.0...celestia-types-v0.14.0) - 2025-08-13

### Added

- *(proto,types,rpc)* [**breaking**] upgrade to celestia-node v0.25 ([#720](https://github.com/eigerco/lumina/pull/720))
- *(proto,types)* [**breaking**] Update protos and switch to tendermint v0.38 ([#707](https://github.com/eigerco/lumina/pull/707))
- [**breaking**] Implement `celestia-client` crate ([#682](https://github.com/eigerco/lumina/pull/682))

### Fixed

- *(doc)* Fix incorrectly concatenated changelogs ([#713](https://github.com/eigerco/lumina/pull/713))

## [0.13.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.12.0...celestia-types-v0.13.0) - 2025-07-29

### Added

- *(types)* [**breaking**] Add appconsts for v5 upgrade ([#705](https://github.com/eigerco/lumina/pull/705))
- *(grpc)* [**breaking**] Expose entire GrpcClient API plus required type changes  ([#655](https://github.com/eigerco/lumina/pull/655))
- *(grpc)* [**breaking**] Trustless balance queries ([#677](https://github.com/eigerco/lumina/pull/677))
- *(grpc,types,node)* [**breaking**] Wasm grpc client ([#654](https://github.com/eigerco/lumina/pull/654))
- *(grpc)* Streamline TxClient creation API, add docs with example ([#673](https://github.com/eigerco/lumina/pull/673))

## [0.12.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.11.2...celestia-types-v0.12.0) - 2025-07-02

### Added

- *(types, node, grpc)* Network v4 upgrade, grpcwebproxy is back ([#674](https://github.com/eigerco/lumina/pull/674))
- *(node)* [**breaking**] Implement adaptive backward syncing/sampling ([#606](https://github.com/eigerco/lumina/pull/606))
- make Merkle proof fields pub for Blobstream validation ([#658](https://github.com/eigerco/lumina/pull/658))

### Other

- Fix uninlined-format-args clippy ([#671](https://github.com/eigerco/lumina/pull/671))

## [0.11.2](https://github.com/eigerco/lumina/compare/celestia-types-v0.11.1...celestia-types-v0.11.2) - 2025-06-09

### Added

- *(node-uniffi)* Add grpc types and client for uniffi ([#627](https://github.com/eigerco/lumina/pull/627))

## [0.11.1](https://github.com/eigerco/lumina/compare/celestia-types-v0.11.0...celestia-types-v0.11.1) - 2025-05-26

### Fixed

- *(types)* Fix clippy issues ([#594](https://github.com/eigerco/lumina/pull/594))

### Other

- Upgrade to tendermint 0.40.3 ([#622](https://github.com/eigerco/lumina/pull/622))

## [0.11.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.10.1...celestia-types-v0.11.0) - 2025-04-02

### Added

- *(types)* [**breaking**] add support for share v1 and signer in blob ([#574](https://github.com/eigerco/lumina/pull/574))
- *(types)* remove Deserialize from Row & Sample ([#580](https://github.com/eigerco/lumina/pull/580))
- *(types)* [**breaking**] add state error codes related to blob submission ([#560](https://github.com/eigerco/lumina/pull/560))

### Other

- *(types)* fix flaky blob reconstruct when len == 0 ([#541](https://github.com/eigerco/lumina/pull/541))

## [0.10.1](https://github.com/eigerco/lumina/compare/celestia-types-v0.10.0...celestia-types-v0.10.1) - 2025-02-24

### Added

- *(types)* Make fields of `ShareProof` public (#538)

## [0.10.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.9.0...celestia-types-v0.10.0) - 2025-01-28

### Added

- *(grpc,node-wasm)* add javascript bindings for tx client (#510)
- *(grpc)* [**breaking**] add wasm support and transaction client (#474)
- *(types)* add serde feature for nmt-rs (#480)
- *(node-wasm, types)* [**breaking**] Add method to get blobs for wasm (#468)
- Add remaining node types for wasm (#476)
- *(types,rpc)* [**breaking**] move TxConfig to celestia-rpc (#485)

### Fixed

- *(types)* RowNamespaceData binary deserialization (#493)

### Other

- Update wasm-bindgen, remove cfg_attr wasm-bindgen workaround (#507)
- *(ci)* migrate toolchain action, parallelize (#503)

## [0.9.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.8.0...celestia-types-v0.9.0) - 2024-12-02

### Added

- *(proto,types,rpc)* [**breaking**] celestia node v0.20.4 upgrade ([#469](https://github.com/eigerco/lumina/pull/469))
- *(grpc, types, proto)* [**breaking**] Add tonic gRPC ([#454](https://github.com/eigerco/lumina/pull/454))
- *(proto)* [**breaking**] update celestia-app and node proto definitios ([#459](https://github.com/eigerco/lumina/pull/459))

### Other

- *(proto,types,node,grpc)* [**breaking**] Use `tendermint-rs` instead of `celestia-tendermint-rs` fork ([#463](https://github.com/eigerco/lumina/pull/463))

## [0.8.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.7.0...celestia-types-v0.8.0) - 2024-11-07

### Added

- *(node)* [**breaking**] add a method to get all blobs using shwap ([#452](https://github.com/eigerco/lumina/pull/452))

## [0.7.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.6.1...celestia-types-v0.7.0) - 2024-10-25

### Added

- *(types)* [**breaking**] Add versioned consts ([#412](https://github.com/eigerco/lumina/pull/412))
- *(types)* [**breaking**] add blob reconstruction from shares ([#450](https://github.com/eigerco/lumina/pull/450))
- *(types,rpc,node)* [**breaking**] refactor Share to work for parity and data ([#443](https://github.com/eigerco/lumina/pull/443))

### Fixed

- *(types)* axis type for eds new error reporting ([#447](https://github.com/eigerco/lumina/pull/447))

### Other

- *(types)* [**breaking**] Rename `rsmt2d` module to `eds` ([#449](https://github.com/eigerco/lumina/pull/449))

## [0.6.1](https://github.com/eigerco/lumina/compare/celestia-types-v0.6.0...celestia-types-v0.6.1) - 2024-10-11

### Added

- *(types)* derive PartialOrd and Ord for addresses ([#414](https://github.com/eigerco/lumina/pull/414))

### Fixed

- *(rpc)* use correct blob type in state submit pay for blob ([#418](https://github.com/eigerco/lumina/pull/418))

## [0.6.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.5.0...celestia-types-v0.6.0) - 2024-10-03

### Added

- [**breaking**] shwap protocol updates ([#369](https://github.com/eigerco/lumina/pull/369))
- *(types)* align for building for riscv32 ([#393](https://github.com/eigerco/lumina/pull/393))

## [0.5.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.4.0...celestia-types-v0.5.0) - 2024-09-24

### Added

- feat!(types,rpc): Add share, row, merkle proofs and share.GetRange ([#375](https://github.com/eigerco/lumina/pull/375))

## [0.4.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.3.0...celestia-types-v0.4.0) - 2024-08-22

### Added
- updating API for parity with celestia-node v0.15.0 ([#340](https://github.com/eigerco/lumina/pull/340))
- Header pruning ([#351](https://github.com/eigerco/lumina/pull/351))

## [0.3.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.2.0...celestia-types-v0.3.0) - 2024-08-13

### Added
- *(node)* Add syncing window for header sync ([#309](https://github.com/eigerco/lumina/pull/309))
- *(node/syncer)* [**breaking**] Implement backwards header sync ([#279](https://github.com/eigerco/lumina/pull/279))
- feat!(types): Add Blob::index field introduced in celestia 0.13 ([#274](https://github.com/eigerco/lumina/pull/274))

### Fixed
- Fix clippy issues ([#350](https://github.com/eigerco/lumina/pull/350))
- *(types)* [**breaking**] Align byzantine fraud proofs with Go's implementation ([#338](https://github.com/eigerco/lumina/pull/338))
- Upgrade `time` crate to fix rust-lang/rust[#125319](https://github.com/eigerco/lumina/pull/125319) ([#285](https://github.com/eigerco/lumina/pull/285))
- PAY_FOR_BLOB namespace ([#278](https://github.com/eigerco/lumina/pull/278))
- new lints coming with 1.78 and 1.80-nightly ([#275](https://github.com/eigerco/lumina/pull/275))

### Other
- [**breaking**] Upgrade dependencies but exclude the ones that are patched by risc0 ([#292](https://github.com/eigerco/lumina/pull/292))

## [0.2.0](https://github.com/eigerco/lumina/compare/celestia-types-v0.1.1...celestia-types-v0.2.0) - 2024-04-18

### Added
- *(types)* Add constructor for creating EDS of an empty block ([#241](https://github.com/eigerco/lumina/pull/241))
- [**breaking**] Refactor RowId/SampleId/NamespacedDataId related API ([#236](https://github.com/eigerco/lumina/pull/236))
- *(types)* add encoding check when verifying befp ([#231](https://github.com/eigerco/lumina/pull/231))
- feat!(types): Align with Shwap spec ([#232](https://github.com/eigerco/lumina/pull/232))
- feat!(node): Implement DASer ([#223](https://github.com/eigerco/lumina/pull/223))

### Fixed
- *(types)* Fix typo in codec name ([#240](https://github.com/eigerco/lumina/pull/240))
- row verification logic ([#235](https://github.com/eigerco/lumina/pull/235))
- *(celestia-types)* switch codec and multihash codes in byzantine [#200](https://github.com/eigerco/lumina/pull/200)

### Other
- *(node)* Upgrade blockstore, beetswap, and leopard-codec ([#264](https://github.com/eigerco/lumina/pull/264))
- chore!(types,rpc): compatibility with celestia-node 0.13.1 ([#239](https://github.com/eigerco/lumina/pull/239))
- chore!(types): Shwap API changes for consistency  ([#212](https://github.com/eigerco/lumina/pull/212))

## [0.1.1](https://github.com/eigerco/lumina/compare/celestia-types-v0.1.0...celestia-types-v0.1.1) - 2024-01-15

### Other
- add authors and homepage ([#180](https://github.com/eigerco/lumina/pull/180))

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/celestia-types-v0.1.0) - 2024-01-12

### Added
- *(node)* Add shwap data types ([#169](https://github.com/eigerco/lumina/pull/169))
- Add in-memory blockstore ([#160](https://github.com/eigerco/lumina/pull/160))
- expose Cid related types in public api ([#167](https://github.com/eigerco/lumina/pull/167))
- *(types)* Add `wasm-bindgen` feature flag ([#143](https://github.com/eigerco/lumina/pull/143))
- *(node)* Implement persistent header storage in browser using IndexedDB ([#102](https://github.com/eigerco/lumina/pull/102))
- Improve verification and implement verification in Exchange client ([#85](https://github.com/eigerco/lumina/pull/85))
- Peer discovery with Kademlia ([#79](https://github.com/eigerco/lumina/pull/79))
- *(types)* expose commitment creation for shares ([#78](https://github.com/eigerco/lumina/pull/78))
- *(types)* allow dereferencing namespace to nmt_rs one ([#77](https://github.com/eigerco/lumina/pull/77))
- *(node)* Implement exchange client ([#63](https://github.com/eigerco/lumina/pull/63))
- add RPC calls for p2p module and tests for them ([#52](https://github.com/eigerco/lumina/pull/52))
- update SyncState to the latest serialization ([#36](https://github.com/eigerco/lumina/pull/36))
- *(fraud)* Add an error for unsupported fraud proof types ([#35](https://github.com/eigerco/lumina/pull/35))
- *(fraud)* Add fraud proof trait and byzantine encoding fraud ([#32](https://github.com/eigerco/lumina/pull/32))
- Add State RPC and types ([#31](https://github.com/eigerco/lumina/pull/31))
- *(rpc)* Add all calls for Blob, Share, and Header ([#24](https://github.com/eigerco/lumina/pull/24))
- align namespaced shares deserialization with latest celestia  ([#20](https://github.com/eigerco/lumina/pull/20))
- serialize proof and commitment ([#19](https://github.com/eigerco/lumina/pull/19))
- *(rpc)* Create celestia-rpc crate and add integration tests ([#17](https://github.com/eigerco/lumina/pull/17))
- implement Blob commitment calculation ([#10](https://github.com/eigerco/lumina/pull/10))
- *(types)* implement `ExtendedHeader::verify` ([#9](https://github.com/eigerco/lumina/pull/9))
- *(ext-header)* Implement validate method ([#8](https://github.com/eigerco/lumina/pull/8))
- add NamespacedShares type ([#7](https://github.com/eigerco/lumina/pull/7))
- *(types)* add initial `Blob` implementation

### Fixed
- handling of the namespaces in v255 ([#164](https://github.com/eigerco/lumina/pull/164))
- Yield between multiple `ExtendedHeader::validate` ([#107](https://github.com/eigerco/lumina/pull/107))
- *(types)* fix the json representation of SubmitOptions ([#66](https://github.com/eigerco/lumina/pull/66))
- *(types)* Propagate and validate the share version from blob

### Other
- add missing metadata to the toml files ([#170](https://github.com/eigerco/lumina/pull/170))
- document public api ([#161](https://github.com/eigerco/lumina/pull/161))
- Enable compatibility with sha2 v0.10.6 ([#171](https://github.com/eigerco/lumina/pull/171))
- error message for missing token and cleanups ([#168](https://github.com/eigerco/lumina/pull/168))
- use Share instead of u8 slice in commitment ([#166](https://github.com/eigerco/lumina/pull/166))
- Add Multihash for NMT node data ([#153](https://github.com/eigerco/lumina/pull/153))
- Migrate to nmt-rs of crates.io ([#144](https://github.com/eigerco/lumina/pull/144))
- hide p2p and syncer components from public node api ([#127](https://github.com/eigerco/lumina/pull/127))
- Upgrade libp2p to v0.53.0 ([#126](https://github.com/eigerco/lumina/pull/126))
- trim the features of workspace dependencies ([#99](https://github.com/eigerco/lumina/pull/99))
- Write test cases for invalid and bad headers ([#87](https://github.com/eigerco/lumina/pull/87))
- Add missing Clone and Copy in P2P types ([#84](https://github.com/eigerco/lumina/pull/84))
- Update protobuf definitions ([#40](https://github.com/eigerco/lumina/pull/40))
- switch to upstream nmt-rs ([#74](https://github.com/eigerco/lumina/pull/74))
- derive Hash for accounts and Namespace ([#76](https://github.com/eigerco/lumina/pull/76))
- Implement header Store ([#73](https://github.com/eigerco/lumina/pull/73))
- align to celestia node v0.11-rc ([#65](https://github.com/eigerco/lumina/pull/65))
- *(types)* remove obsolete NotEnoughVotingPower error
- *(types)* simplify validation error
- *(license)* Set Apache 2.0 license ([#6](https://github.com/eigerco/lumina/pull/6))
- Implement Celestia types
- initial commit
