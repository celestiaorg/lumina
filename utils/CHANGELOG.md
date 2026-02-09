# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.2](https://github.com/celestiaorg/lumina/compare/lumina-utils-v0.5.1...lumina-utils-v0.5.2) - 2026-01-27

### Fixed

- update npm repo ([#880](https://github.com/celestiaorg/lumina/pull/880))

## [0.5.1](https://github.com/eigerco/lumina/compare/lumina-utils-v0.5.0...lumina-utils-v0.5.1) - 2026-01-05

### Other

- *(utils)* relax interval assertions slightly ([#821](https://github.com/eigerco/lumina/pull/821))

## [0.5.0](https://github.com/eigerco/lumina/compare/lumina-utils-v0.4.0...lumina-utils-v0.5.0) - 2025-11-19

### Added

- *(utils)* [**breaking**] Make `Interval::new` sync constructor ([#799](https://github.com/eigerco/lumina/pull/799))
- *(client,grpc)* [**breaking**] expose more tls config options, error if tls is not supported ([#796](https://github.com/eigerco/lumina/pull/796))

### Other

- [**breaking**] Migrate to Rust 2024 ([#773](https://github.com/eigerco/lumina/pull/773))

## [0.4.0](https://github.com/eigerco/lumina/compare/lumina-utils-v0.3.0...lumina-utils-v0.4.0) - 2025-09-25

### Added

- [**breaking**] unify and upgrade dependencies, add explicit msrv ([#742](https://github.com/eigerco/lumina/pull/742))
- *(grpc)* [**breaking**] Merge TxClient and GrpcClient, add builder ([#712](https://github.com/eigerco/lumina/pull/712))

## [0.3.0](https://github.com/eigerco/lumina/compare/lumina-utils-v0.2.0...lumina-utils-v0.3.0) - 2025-07-29

### Added

- *(grpc,types,node)* [**breaking**] Wasm grpc client ([#654](https://github.com/eigerco/lumina/pull/654))

## [0.2.0](https://github.com/eigerco/lumina/compare/lumina-utils-v0.1.0...lumina-utils-v0.2.0) - 2025-05-26

### Fixed

- *(utils)* [**breaking**] Fix `timeout` when duration is more than 24.9 days ([#617](https://github.com/eigerco/lumina/pull/617))

## [0.1.0](https://github.com/eigerco/lumina/releases/tag/lumina-utils-v0.1.0) - 2025-04-02

### Added

- lumina-utils crate ([#564](https://github.com/eigerco/lumina/pull/564))
