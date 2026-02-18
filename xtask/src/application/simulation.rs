use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use cargo_metadata::{Metadata, semver::Version};
use release_plz_core::set_version::{
    SetVersionRequest, SetVersionSpec, VersionChange, set_version,
};

use crate::adapters::git_refs::snapshot_workspace_to_temp;
use crate::adapters::metadata::metadata_for_manifest;
use crate::domain::types::VersionEntry;
use crate::domain::workspace::Workspace;

#[derive(Debug, Clone, Default)]
pub struct StrictSimulationResult {
    /// Conflicts where the same publishable package name resolves to multiple versions.
    pub duplicate_publishable_versions: Vec<(String, Vec<String>)>,
}

/// Runs strict duplicate-version simulation in an isolated workspace snapshot.
/// It applies effective computed versions and validates resulting publishable package graph.
pub fn run_simulation(
    workspace_root: &Path,
    default_branch: &str,
    current_commit: &str,
    versions: &[VersionEntry],
) -> Result<StrictSimulationResult> {
    // Create isolated snapshot at the exact current commit to avoid touching user checkout.
    let snapshot =
        snapshot_workspace_to_temp(workspace_root, current_commit, default_branch)?;
    let _hold_temp_dir = &snapshot.temp_dir;
    let temp_workspace_root = snapshot.repo_root.as_path();
    let temp_manifest = temp_workspace_root.join("Cargo.toml");

    // Guard against malformed snapshots before invoking cargo metadata.
    ensure!(
        temp_manifest.exists(),
        "temporary workspace manifest not found at `{}`",
        temp_manifest.display()
    );

    // Apply computed effective versions exactly as check would resolve them.
    let temp_metadata = metadata_for_manifest(&temp_manifest)?;
    let changes = version_changes_for_mode(versions);
    if !changes.is_empty() {
        apply_version_changes_with_metadata(changes, temp_metadata)?;
    }

    // Reload metadata after applying changes and check for duplicate publishable versions.
    let simulated_metadata = metadata_for_manifest(&temp_manifest)?;
    let simulated_workspace = Workspace::from_metadata(simulated_metadata);
    let duplicates = simulated_workspace.duplicate_publishable_versions();

    Ok(StrictSimulationResult {
        duplicate_publishable_versions: duplicates,
    })
}

/// Extracts mode-specific effective versions for publishable packages only.
fn version_changes_for_mode(versions: &[VersionEntry]) -> BTreeMap<String, Version> {
    versions
        .iter()
        .filter(|version| version.publishable)
        .map(|version| (version.package.clone(), version.next_effective.clone()))
        .collect()
}

/// Applies version changes in snapshot metadata using release-plz set_version primitives.
fn apply_version_changes_with_metadata(
    changes: BTreeMap<String, Version>,
    metadata: Metadata,
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }

    let version_changes = changes
        .into_iter()
        .map(|(package, version)| (package, VersionChange::new(version)))
        .collect();

    let request = SetVersionRequest::new(SetVersionSpec::Workspace(version_changes), metadata)?;
    set_version(&request).context("failed to apply simulated version changes")
}
