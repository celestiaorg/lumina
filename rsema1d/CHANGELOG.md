# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0-rc.1](https://github.com/celestiaorg/lumina/compare/rsema1d-v1.1.0-rc.1...rsema1d-v1.2.0-rc.1) - 2026-09-24

### Fixed

- *(fibre)* fibre-client does proper download ([#992](https://github.com/celestiaorg/lumina/pull/992))
- *(fiber)* update rsema1d to match celestia app ([#988](https://github.com/celestiaorg/lumina/pull/988))

### Other

- hash commitment roots without allocation ([#1069](https://github.com/celestiaorg/lumina/pull/1069))
- *(fibre)* return reconstructed blobs as shared bytes ([#1066](https://github.com/celestiaorg/lumina/pull/1066))
- *(fibre)* preserve shared bytes in downloaded row proofs ([#1065](https://github.com/celestiaorg/lumina/pull/1065))
- *(rsema1d)* compute Merkle proof depth with integer arithmetic ([#1061](https://github.com/celestiaorg/lumina/pull/1061))
- *(rsema1d)* scatter parity directly from encoder results ([#1059](https://github.com/celestiaorg/lumina/pull/1059))
- *(rsema1d)* parallelize reconstruction with bounded work buffers ([#1055](https://github.com/celestiaorg/lumina/pull/1055))
- *(rsema1d)* add production reconstruction and verification benchmarks ([#1054](https://github.com/celestiaorg/lumina/pull/1054))
- *(fibre,rsema1d)* hand rows to wire without copying ([#1015](https://github.com/celestiaorg/lumina/pull/1015))
- *(fibre)* safe hugepage ([#1012](https://github.com/celestiaorg/lumina/pull/1012))
- *(rsema1d)* (safe) encode parity in cache-resident column stripes in parallel ([#1008](https://github.com/celestiaorg/lumina/pull/1008))
- *(fibre,rsema1d)* incluse packages in CI ([#1010](https://github.com/celestiaorg/lumina/pull/1010))
- *(rsema1d)* make codec benches statistically less noisy ([#1007](https://github.com/celestiaorg/lumina/pull/1007))
- *(rsema1d)* precompute coefficient logarithms for RLC computation ([#1005](https://github.com/celestiaorg/lumina/pull/1005))
- *(fibre)* adding more benches ([#1002](https://github.com/celestiaorg/lumina/pull/1002))
- *(rsema1d)* remove generic-array deprecation warning ([#999](https://github.com/celestiaorg/lumina/pull/999))
- chore(rsema1d) update bench and fuzz. ([#995](https://github.com/celestiaorg/lumina/pull/995))
- *(fibre)* use more existing types instead of bytes and strings ([#985](https://github.com/celestiaorg/lumina/pull/985))

### Added

- add `RowMatrix::into_bytes` to take the backing storage as shared `Bytes` without copying

### Changed

- [**breaking**] make `RowProof` own shared `Bytes` row data instead of a lifetime-bound `Cow`, avoiding row copies when proofs outlive encoded data borrows
- avoid temporary allocations when hashing commitment root pairs

## [1.1.0-rc.1](https://github.com/celestiaorg/lumina/compare/rsema1d-v1.0.0...rsema1d-v1.1.0-rc.1) - 2026-06-24

### Added

- add fibre crate with domain types ([#959](https://github.com/celestiaorg/lumina/pull/959))
- add CondSend trait and update rsema1d codec API ([#955](https://github.com/celestiaorg/lumina/pull/955))

## [1.0.0-rc.3](https://github.com/celestiaorg/lumina/compare/rsema1d-v1.0.0-rc.2...rsema1d-v1.0.0-rc.3) - 2026-03-19

### Added

- update rsema1d import path to celestia-app/v8/pkg/rsema1d ([#937](https://github.com/celestiaorg/lumina/pull/937))
- implement rsema1d ([#933](https://github.com/celestiaorg/lumina/pull/933))
