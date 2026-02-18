use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use cargo_metadata::{Metadata, semver::Version};
use release_plz_core::set_version::{
    SetVersionRequest, SetVersionSpec, VersionChange, set_version,
};

use crate::adapters::git_refs::snapshot_workspace_to_temp;
use crate::adapters::metadata::metadata_for_manifest;
use crate::domain::types::Plan;
use crate::domain::workspace::Workspace;

#[derive(Debug, Clone, Default)]
pub struct StrictSimulationResult {
    pub duplicate_publishable_versions: Vec<(String, Vec<String>)>,
}

pub fn run_strict_simulation(
    workspace_root: &Path,
    default_branch: &str,
    comparison_commit: &str,
    plans: &[Plan],
) -> Result<StrictSimulationResult> {
    let snapshot =
        snapshot_workspace_to_temp(workspace_root, comparison_commit, default_branch)?;
    let _hold_temp_dir = &snapshot.temp_dir;
    let temp_workspace_root = snapshot.repo_root.as_path();
    let temp_manifest = temp_workspace_root.join("Cargo.toml");

    ensure!(
        temp_manifest.exists(),
        "temporary workspace manifest not found at `{}`",
        temp_manifest.display()
    );

    let temp_metadata = metadata_for_manifest(&temp_manifest)?;
    let changes = version_changes_for_mode(&plans);
    if !changes.is_empty() {
        apply_version_changes_with_metadata(changes, temp_metadata)?;
    }

    let simulated_metadata = metadata_for_manifest(&temp_manifest)?;
    let simulated_workspace = Workspace::from_metadata(simulated_metadata);
    let duplicates = simulated_workspace.duplicate_publishable_versions();

    Ok(StrictSimulationResult {
        duplicate_publishable_versions: duplicates,
    })
}

fn version_changes_for_mode(plans: &[Plan]) -> BTreeMap<String, Version> {
    plans
        .iter()
        .filter(|plan| plan.publishable)
        .map(|plan| (plan.package.clone(), plan.next_effective.clone()))
        .collect()
}

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
