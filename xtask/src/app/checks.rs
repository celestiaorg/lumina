use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use cargo_metadata::{Metadata, semver::Version};
use release_plz_core::set_version::{
    SetVersionRequest, SetVersionSpec, VersionChange, set_version,
};

use crate::domain::types::ReleaseContext;
use crate::domain::workspace::Workspace;
use crate::engine::release_plz::ReleasePlzEngine;
use crate::infra::git_refs::{checkout_commit, clone_workspace_to_temp, resolve_comparison_commit};
use crate::infra::metadata::metadata_for_manifest;

#[derive(Debug, Clone, Default)]
pub struct StrictSimulationResult {
    pub duplicate_publishable_versions: Vec<(String, Vec<String>)>,
}

pub async fn run_strict_simulation(
    workspace_root: &Path,
    release_engine: &ReleasePlzEngine,
    ctx: &ReleaseContext,
) -> Result<StrictSimulationResult> {
    let snapshot = clone_workspace_to_temp(workspace_root)?;
    let _hold_temp_dir = &snapshot.temp_dir;
    let temp_workspace_root = snapshot.repo_root.as_path();
    let temp_manifest = temp_workspace_root.join("Cargo.toml");

    ensure!(
        temp_manifest.exists(),
        "temporary workspace manifest not found at `{}`",
        temp_manifest.display()
    );

    let comparison_commit = resolve_comparison_commit(
        workspace_root,
        &ctx.default_branch,
        ctx.base_commit.as_deref(),
    )?;
    checkout_commit(temp_workspace_root, &comparison_commit, &ctx.default_branch)
        .with_context(|| "failed to checkout comparison commit in strict simulation")?;

    let temp_metadata = metadata_for_manifest(&temp_manifest)?;
    let plans = release_engine
        .plan_from_snapshot(&temp_metadata, temp_workspace_root, ctx)
        .await?;
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

fn version_changes_for_mode(
    plans: &[crate::domain::types::Plan],
) -> BTreeMap<String, Version> {
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
