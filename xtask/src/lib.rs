//! In-house release tooling for the lumina workspace.
//!
//! This crate implements the two `cargo xtask` subcommands, `prepare-release`
//! (build a single-commit release PR) and `release` (publish crates + npm and cut
//! tags/releases). The modules are layered: I/O primitives, then pure feature
//! logic, then command orchestration, then the CLI.

// I/O primitives — git, cargo/registry, GitHub, npm, workspace/repo discovery.
pub mod cargoops;
pub mod commit;
pub mod config;
pub mod forge;
pub mod gitops;
pub mod npmops;
pub mod repo;
pub mod version;
pub mod workspace;

// Feature logic — breaking-change detection, changelog + PR-body rendering.
pub mod breaking;
pub mod changelog;
pub mod prbody;

// Command orchestration.
pub mod cmd_prepare;
pub mod cmd_release;

// CLI entry point.
pub mod cli;
