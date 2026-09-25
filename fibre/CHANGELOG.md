# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0-rc.1](https://github.com/celestiaorg/lumina/compare/celestia-fibre-v1.1.0-rc.1...celestia-fibre-v1.2.0-rc.1) - 2026-09-24

### Added

- make download reconstruction budget configurable ([#1068](https://github.com/celestiaorg/lumina/pull/1068))
- *(fibre)* Allow setting request timeout ([#1077](https://github.com/celestiaorg/lumina/pull/1077))
- *(fibre)* allow shared validator connector for several fibre clients in a single process ([#1053](https://github.com/celestiaorg/lumina/pull/1053))
- *(fibre)* expose an upload completion handle with per-validator stats ([#1024](https://github.com/celestiaorg/lumina/pull/1024))
- *(fibre)* Add TLS support ([#996](https://github.com/celestiaorg/lumina/pull/996))

### Fixed

- *(fibre)* offload native encoding and shard verification ([#1072](https://github.com/celestiaorg/lumina/pull/1072))
- *(fibre)* coalesce historical validator set queries ([#1063](https://github.com/celestiaorg/lumina/pull/1063))
- *(fibre)* offload native blob reconstruction on bloking tokio ([#1062](https://github.com/celestiaorg/lumina/pull/1062))
- *(fibre)* fix merge race ([#1036](https://github.com/celestiaorg/lumina/pull/1036))
- *(fibre)* cancel uploads when client closes ([#1022](https://github.com/celestiaorg/lumina/pull/1022))
- *(fibre)* reject invalid validator voting power ([#1020](https://github.com/celestiaorg/lumina/pull/1020))
- *(fibre)* fibre-client does proper download ([#992](https://github.com/celestiaorg/lumina/pull/992))

### Other

- *(fibre)* reuse downloaded RLC vector in verification cache ([#1073](https://github.com/celestiaorg/lumina/pull/1073))
- *(fibre)* return reconstructed blobs as shared bytes ([#1066](https://github.com/celestiaorg/lumina/pull/1066))
- *(fibre)* preserve shared bytes in downloaded row proofs ([#1065](https://github.com/celestiaorg/lumina/pull/1065))
- *(fibre)* enable fibre local devnet ([#1081](https://github.com/celestiaorg/lumina/pull/1081))
- *(fibre)* add wire and TLS tests for fibre ([#1047](https://github.com/celestiaorg/lumina/pull/1047))
- *(fibre)* strengthen download tests ([#1046](https://github.com/celestiaorg/lumina/pull/1046))
- *(fibre)* strengthen upload tests ([#1045](https://github.com/celestiaorg/lumina/pull/1045))
- *(fibre)* add ProtocolParams validation and tests ([#1043](https://github.com/celestiaorg/lumina/pull/1043))
- *(fibre)* reuse fixutres accross existing tests ([#1041](https://github.com/celestiaorg/lumina/pull/1041))
- *(fibre)* consolidate tests and remove unused `ShardMap::verify` ([#1038](https://github.com/celestiaorg/lumina/pull/1038))
- *(fibre)* simplify blob reconstruction ([#1049](https://github.com/celestiaorg/lumina/pull/1049))
- *(fibre)* reduce string interpolation on error origination sites ([#1028](https://github.com/celestiaorg/lumina/pull/1028))
- *(proto)* decode Fibre proofs and RLCs as Bytes ([#1027](https://github.com/celestiaorg/lumina/pull/1027))
- *(fibre)* remove unused message size config ([#1026](https://github.com/celestiaorg/lumina/pull/1026))
- *(fibre)* type safe config and client builder ([#1025](https://github.com/celestiaorg/lumina/pull/1025))
- *(fibre,rsema1d)* hand rows to wire without copying ([#1015](https://github.com/celestiaorg/lumina/pull/1015))
- *(fibre)* safe hugepage ([#1012](https://github.com/celestiaorg/lumina/pull/1012))
- *(rsema1d)* (safe) encode parity in cache-resident column stripes in parallel ([#1008](https://github.com/celestiaorg/lumina/pull/1008))
- *(fibre)* [**breaking**] split blob lifecycle states ([#1011](https://github.com/celestiaorg/lumina/pull/1011))
- *(fibre)* adding more benches ([#1002](https://github.com/celestiaorg/lumina/pull/1002))
- *(fibre)* use more existing types instead of bytes and strings ([#985](https://github.com/celestiaorg/lumina/pull/985))

### Changed

- [**breaking**] add `DownloadOptions::reconstruction_work_budget` to bound Reed-Solomon reconstruction scratch space per download
- keep reconstructed blob data in shared `Bytes`; call `Blob::into_data` to take the decoded payload without copying
- [**breaking**] keep downloaded row data in shared `Bytes` from protobuf decoding through reconstruction instead of copying each row into a `Vec`
- [**breaking**] replace free-form `FibreError` messages with typed error variants and change `FibreIoConnector::connect` to return `std::io::Error`
- [**breaking**] split the Fibre blob lifecycle into upload-ready `EncodedBlob`, private reconstruction state, and decoded `Blob`
- remove the public manual-reconstruction API (`Blob::empty` and `Blob::set_row`); use `FibreClient::download` instead

## [1.1.0-rc.1](https://github.com/celestiaorg/lumina/compare/celestia-fibre-v1.0.0...celestia-fibre-v1.1.0-rc.1) - 2026-06-24

### Added

- add fibre download client and lumina client integration ([#971](https://github.com/celestiaorg/lumina/pull/971))
- add fibre gRPC transport and upload client ([#969](https://github.com/celestiaorg/lumina/pull/969))
- add fibre validator tracking and signature collection ([#965](https://github.com/celestiaorg/lumina/pull/965))
- add fibre crate with domain types ([#959](https://github.com/celestiaorg/lumina/pull/959))
