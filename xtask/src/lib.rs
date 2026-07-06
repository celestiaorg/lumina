//! In-house release tooling for the lumina workspace.
//!
//! This crate implements the two `cargo xtask` subcommands specified in
//! `release-spec/orchestrator.md`: `prepare-release` and `release`.
//!
//! Modules are declared here by each wave's Integration agent as they are built
//! (the `mod …;` lines below are a stitch point — see
//! `release-spec/IMPLEMENTATION-GUIDE.md` §8.4). The crate starts empty.

// Module declarations are added by the per-wave Integration agent.

// Wave 1 — foundation modules (M1-M8).
pub mod cargoops;
pub mod commit;
pub mod config;
pub mod forge;
pub mod gitops;
pub mod npmops;
pub mod repo;
pub mod version;
pub mod workspace;

// Wave 2 — feature logic (M9, M10, M11).
pub mod breaking;
pub mod changelog;
pub mod prbody;

// Wave 3 — command orchestration (M12, M13).
pub mod cmd_prepare;
pub mod cmd_release;

// Wave 4 — entry point (M14).
pub mod cli;
