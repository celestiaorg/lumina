use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cargo_metadata::{Metadata, MetadataCommand, camino::Utf8Path};
use release_plz_core::set_version::{
    SetVersionRequest, SetVersionSpec, VersionChange, set_version,
};
use release_plz_core::update_request::UpdateRequest;
use tracing::{debug, info};

use crate::adapters::git_refs::{
    changed_files_since_tag, commit_for_tag, latest_non_rc_release_tag_on_branch,
    latest_release_tag_on_branch, remote_origin_url, resolve_current_commit,
    snapshot_workspace_to_temp,
};
use crate::adapters::metadata::metadata_for_manifest;
use crate::domain::types::{
    CheckContext, ComparisonVersionView, PublishContext, ReleaseMode, VersionComputation,
    VersionEntry,
};
use crate::domain::versioning::{convert_release_version_to_rc, to_stable_if_prerelease};
use crate::domain::workspace::Workspace;

#[derive(Debug, Clone)]
struct ContextVersions {
    /// Previous release commit used as baseline anchor.
    previous_commit: String,
    /// Latest release tag (RC or final) visible from comparison snapshot.
    latest_release_tag: Option<String>,
    /// Latest non-RC release tag visible from comparison snapshot.
    latest_non_rc_release_tag: Option<String>,
    /// Commit used as current workspace snapshot root.
    current_commit: String,
    /// Versions observed at current commit before applying computed changes.
    current_versions: Vec<ComparisonVersionView>,
    /// Computed package version updates for selected mode.
    versions: Vec<VersionEntry>,
    /// Duplicate publishable package versions after applying computed versions in snapshot.
    duplicate_publishable_versions: Vec<(String, Vec<String>)>,
}

#[derive(Debug)]
struct SnapshotState {
    snapshot: crate::adapters::git_refs::RepoSnapshot,
    metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct ReleasePlzAdapter {
    /// Root of repository/workspace where release operations are executed.
    workspace_root: PathBuf,
}

impl ReleasePlzAdapter {
    /// Constructs release-plz adapter scoped to one workspace root.
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    /// Computes release versions and reporting metadata for check/prepare stages.
    /// Baseline previous commit must be resolvable for both rc and final modes.
    pub async fn versions(&self, ctx: &CheckContext) -> Result<VersionComputation> {
        info!(
            mode=?ctx.common.mode,
            default_branch=%ctx.common.default_branch,
            requested_current_commit=?ctx.current_commit,
            workspace_root=%self.workspace_root.display(),
            "release_plz: computing versions"
        );
        let from_context = self.versions_from_context(ctx).await?;
        info!(
            current_commit=%from_context.current_commit,
            previous_commit=%from_context.previous_commit,
            latest_release_tag=?from_context.latest_release_tag,
            latest_non_rc_release_tag=?from_context.latest_non_rc_release_tag,
            planned_versions=from_context.versions.len(),
            duplicates=from_context.duplicate_publishable_versions.len(),
            "release_plz: versions computed"
        );

        Ok(VersionComputation {
            previous_commit: from_context.previous_commit,
            latest_release_tag: from_context.latest_release_tag,
            latest_non_rc_release_tag: from_context.latest_non_rc_release_tag,
            current_commit: from_context.current_commit,
            current_versions: from_context.current_versions,
            versions: from_context.versions,
            duplicate_publishable_versions: from_context.duplicate_publishable_versions,
        })
    }

    /// Computes versions from an isolated snapshot at current commit and gathers reporting context.
    async fn versions_from_context(&self, ctx: &CheckContext) -> Result<ContextVersions> {
        // Resolve the current commit used as analysis input.
        let current_commit = self.resolve_current_for_context(ctx)?;
        debug!(current_commit=%current_commit, "release_plz: resolved current commit");
        // Load isolated snapshot + metadata at that commit.
        let snapshot_state =
            self.load_snapshot_state(&current_commit, &ctx.common.default_branch)?;
        let temp_workspace_root = snapshot_state.snapshot.repo_root.as_path();
        debug!(
            temp_workspace_root=%temp_workspace_root.display(),
            "release_plz: created current snapshot worktree"
        );
        // Resolve previous release commit according to mode policy.
        let previous_commit = self.resolve_previous_for_mode(
            ctx.common.mode,
            temp_workspace_root,
            &ctx.common.default_branch,
        )?;
        debug!(previous_commit=%previous_commit, "release_plz: resolved previous baseline commit");
        let latest_release_tag =
            latest_release_tag_on_branch(temp_workspace_root, &ctx.common.default_branch)?;
        let latest_non_rc_release_tag =
            latest_non_rc_release_tag_on_branch(temp_workspace_root, &ctx.common.default_branch)?;
        debug!(
            latest_release_tag=?latest_release_tag,
            latest_non_rc_release_tag=?latest_non_rc_release_tag,
            "release_plz: resolved baseline tags"
        );
        // Load previous release snapshot so release-plz can use it as baseline.
        let previous_snapshot =
            self.load_previous_snapshot(&previous_commit, &ctx.common.default_branch)?;
        let previous_manifest_path = previous_snapshot.snapshot.repo_root.join("Cargo.toml");
        debug!(
            previous_snapshot_root=%previous_snapshot.snapshot.repo_root.display(),
            previous_manifest_path=%previous_manifest_path.display(),
            "release_plz: prepared previous baseline snapshot"
        );

        // Capture package versions before applying computed updates.
        let reported_current_versions = current_versions_from_metadata(&snapshot_state.metadata);
        let versions = self
            .versions_from_snapshot(
                &snapshot_state.metadata,
                temp_workspace_root,
                &previous_manifest_path,
                ctx.common.mode,
                &ctx.common.default_branch,
            )
            .await?;
        let duplicate_publishable_versions = duplicate_versions(
            temp_workspace_root,
            snapshot_state.metadata.clone(),
            &versions,
        )?;
        debug!(
            planned_versions=versions.len(),
            duplicate_groups=duplicate_publishable_versions.len(),
            "release_plz: finished context version computation"
        );

        Ok(ContextVersions {
            previous_commit,
            latest_release_tag,
            latest_non_rc_release_tag,
            current_commit,
            current_versions: reported_current_versions,
            versions,
            duplicate_publishable_versions,
        })
    }

    /// Resolves current commit for check/prepare computation.
    fn resolve_current_for_context(&self, ctx: &CheckContext) -> Result<String> {
        let commit = resolve_current_commit(
            &self.workspace_root,
            &ctx.common.default_branch,
            ctx.current_commit.as_deref(),
        )?;
        debug!(
            selected_commit=%commit,
            default_branch=%ctx.common.default_branch,
            "release_plz: selected comparison commit"
        );
        Ok(commit)
    }

    /// Creates snapshot worktree at current commit and loads metadata from it.
    fn load_snapshot_state(
        &self,
        current_commit: &str,
        default_branch: &str,
    ) -> Result<SnapshotState> {
        let snapshot =
            snapshot_workspace_to_temp(&self.workspace_root, current_commit, default_branch)?;
        let _keep_temp_dir_alive = &snapshot.temp_dir;
        let temp_manifest = snapshot.repo_root.join("Cargo.toml");
        let metadata = metadata_for_manifest(&temp_manifest)?;
        debug!(
            source_workspace_root=%self.workspace_root.display(),
            snapshot_root=%snapshot.repo_root.display(),
            temp_manifest=%temp_manifest.display(),
            "release_plz: loaded snapshot metadata"
        );
        Ok(SnapshotState { snapshot, metadata })
    }

    /// Loads previous release snapshot for release-plz baseline input.
    fn load_previous_snapshot(
        &self,
        previous_commit: &str,
        default_branch: &str,
    ) -> Result<SnapshotState> {
        let snapshot_state = self.load_snapshot_state(previous_commit, default_branch)?;
        debug!(
            previous_commit=%previous_commit,
            previous_snapshot_root=%snapshot_state.snapshot.repo_root.display(),
            "release_plz: loaded previous snapshot"
        );
        Ok(snapshot_state)
    }

    /// Resolves previous release commit according to mode policy.
    fn resolve_previous_for_mode(
        &self,
        mode: ReleaseMode,
        snapshot_repo_root: &Path,
        default_branch: &str,
    ) -> Result<String> {
        let previous_tag = match mode {
            ReleaseMode::Rc => latest_release_tag_on_branch(snapshot_repo_root, default_branch)?
                .context("rc mode requires latest release tag for previous-commit reporting")?,
            ReleaseMode::Final => {
                latest_non_rc_release_tag_on_branch(snapshot_repo_root, default_branch)?.context(
                    "final mode requires latest non-RC release tag for previous-commit reporting",
                )?
            }
        };
        let previous_commit = commit_for_tag(snapshot_repo_root, &previous_tag)?;
        info!(
            mode=?mode,
            previous_tag=%previous_tag,
            previous_commit=%previous_commit,
            snapshot_repo_root=%snapshot_repo_root.display(),
            "release_plz: resolved previous baseline"
        );
        Ok(previous_commit)
    }

    /// Computes package versions from snapshot metadata and applies final-mode baseline filtering.
    pub async fn versions_from_snapshot(
        &self,
        metadata: &Metadata,
        snapshot_repo_root: &std::path::Path,
        previous_manifest_path: &std::path::Path,
        mode: ReleaseMode,
        default_branch: &str,
    ) -> Result<Vec<VersionEntry>> {
        debug!(
            mode=?mode,
            snapshot_repo_root=%snapshot_repo_root.display(),
            previous_manifest_path=%previous_manifest_path.display(),
            "release_plz: computing versions from snapshot"
        );
        // First compute raw per-package next versions from release-plz rules.
        let mut versions = self
            .versions_for_mode(metadata, mode, previous_manifest_path)
            .await?;

        if matches!(mode, ReleaseMode::Final) {
            // In final mode, include only publishable packages changed since latest stable baseline tag.
            let baseline_tag =
                latest_non_rc_release_tag_on_branch(snapshot_repo_root, default_branch)?.context(
                    "final mode requires at least one non-RC release tag in comparison workspace",
                )?;
            let changed_packages = changed_publishable_packages_since_tag(
                snapshot_repo_root,
                metadata,
                &baseline_tag,
            )?;
            debug!(
                baseline_tag=%baseline_tag,
                changed_packages=changed_packages.len(),
                "release_plz: filtered final-mode packages by baseline tag"
            );
            versions.retain(|version| changed_packages.contains(&version.package));
        }

        Ok(versions)
    }

    /// Uses release-plz next-version rules, then maps to mode-specific effective versions.
    pub async fn versions_for_mode(
        &self,
        metadata: &Metadata,
        mode: ReleaseMode,
        previous_manifest_path: &std::path::Path,
    ) -> Result<Vec<VersionEntry>> {
        debug!(
            mode=?mode,
            previous_manifest_path=%previous_manifest_path.display(),
            "release_plz: invoking next_versions"
        );
        // Run release-plz next_versions once and reuse that as source of truth.
        let mut update_request = UpdateRequest::new(metadata.clone())
            .context("failed to build release-plz update request")?
            .with_allow_dirty(true);
        let previous_manifest_path =
            Utf8Path::from_path(previous_manifest_path).with_context(|| {
                format!(
                    "previous manifest path is not valid UTF-8: {}",
                    previous_manifest_path.display()
                )
            })?;
        update_request = update_request
            .with_registry_manifest_path(previous_manifest_path)
            .context("failed to set previous release manifest for mode-specific versions")?;
        let (packages_to_update, _temp_repo) = release_plz_core::next_versions(&update_request)
            .await
            .context("failed to compute next versions with release-plz")?;
        debug!(
            updates=packages_to_update.updates().len(),
            "release_plz: next_versions returned updates"
        );

        let mut versions = packages_to_update
            .updates()
            .iter()
            .filter(|(package, _)| is_publishable(&package.publish))
            .map(|(package, update)| {
                let current = package.version.clone();
                let next_release = update.version.clone();
                // Mode-specific projection:
                // RC => convert release-plz next version into rc.N variant.
                // Final => normalize prerelease to stable if needed.
                let next_effective = match mode {
                    ReleaseMode::Rc => convert_release_version_to_rc(&next_release)?,
                    ReleaseMode::Final => {
                        to_stable_if_prerelease(&next_release).unwrap_or(next_release.clone())
                    }
                };

                Ok(VersionEntry {
                    package: package.name.to_string(),
                    current,
                    next_release,
                    next_effective,
                    publishable: true,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        versions.sort_by(|a, b| a.package.cmp(&b.package));
        debug!(planned_versions=versions.len(), "release_plz: projected mode-specific versions");
        Ok(versions)
    }

    /// Regenerates release artifacts with release-plz update and applies mode-specific version rewrite.
    pub async fn regenerate_artifacts(
        &self,
        mode: ReleaseMode,
        default_branch: &str,
        previous_commit: &str,
        latest_release_tag: Option<&str>,
        latest_non_rc_release_tag: Option<&str>,
    ) -> Result<Vec<String>> {
        info!(
            mode=?mode,
            default_branch=%default_branch,
            previous_commit=%previous_commit,
            workspace_root=%self.workspace_root.display(),
            "release_plz: regenerating artifacts"
        );
        // Re-run release-plz update in the active branch workspace to regenerate changelog + versions.
        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for release artifact regeneration")?;

        let mut update_request = UpdateRequest::new(metadata.clone())
            .context("failed to build update request for release artifact regeneration")?
            .with_allow_dirty(true);
        let previous_snapshot = self.load_previous_snapshot(previous_commit, default_branch)?;
        let previous_manifest_path = previous_snapshot.snapshot.repo_root.join("Cargo.toml");
        debug!(
            previous_snapshot_root=%previous_snapshot.snapshot.repo_root.display(),
            previous_manifest_path=%previous_manifest_path.display(),
            "release_plz: using previous manifest path for update baseline"
        );
        let previous_manifest_path =
            Utf8Path::from_path(&previous_manifest_path).with_context(|| {
                format!(
                    "previous manifest path is not valid UTF-8: {}",
                    previous_manifest_path.display()
                )
            })?;
        update_request = update_request
            .with_registry_manifest_path(previous_manifest_path)
            .context("failed to set previous release manifest for regeneration")?;

        let (packages_to_update, _temp_repo) = release_plz_core::update(&update_request)
            .await
            .context("failed to regenerate release artifacts with release-plz update")?;
        debug!(
            updates=packages_to_update.updates().len(),
            "release_plz: update regenerated release artifacts"
        );

        let mut descriptions =
            vec!["regenerated release versions/changelog with release-plz update".to_string()];

        // Re-apply mode-specific version projection after update() so artifacts match check semantics.
        let mode_specific_changes = packages_to_update
            .updates()
            .iter()
            .filter(|(package, _)| is_publishable(&package.publish))
            .filter_map(|(package, update)| {
                let next = match mode {
                    ReleaseMode::Rc => convert_release_version_to_rc(&update.version).ok(),
                    ReleaseMode::Final => Some(
                        to_stable_if_prerelease(&update.version).unwrap_or(update.version.clone()),
                    ),
                }?;
                Some((package.name.to_string(), next))
            })
            .collect::<BTreeMap<_, _>>();

        if !mode_specific_changes.is_empty() {
            apply_version_changes_with_metadata(mode_specific_changes, metadata)?;
            debug!("release_plz: applied mode-specific version overrides");
            match mode {
                ReleaseMode::Rc => descriptions.push(
                    "converted release-plz versions to rc variants and updated workspace references"
                        .to_string(),
                ),
                ReleaseMode::Final => {
                    descriptions.push("normalized prerelease versions to stable".to_string())
                }
            }
        }

        if let Some(latest_release) = latest_release_tag {
            // Report release-plz-style changelog comparison anchor.
            descriptions.push(format!(
                "changelog baseline: latest release tag `{latest_release}`"
            ));
            descriptions.push(format!("includes changes since `{latest_release}`"));
        }
        if matches!(mode, ReleaseMode::Final)
            && let Some(latest_non_rc_release) = latest_non_rc_release_tag
        {
            // Report stable baseline separately to remove RC/final ambiguity.
            descriptions.push(format!(
                "final baseline: latest non-RC release tag `{latest_non_rc_release}`"
            ));
            descriptions.push(format!(
                "includes changes since latest non-RC release `{latest_non_rc_release}`"
            ));
        }

        info!(description_items=descriptions.len(), "release_plz: artifact regeneration completed");
        Ok(descriptions)
    }

    /// Runs release-plz publish flow and returns machine-readable release payload.
    pub async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        info!(
            mode=?ctx.common.mode,
            default_branch=%ctx.common.default_branch,
            workspace_root=%self.workspace_root.display(),
            has_release_plz_token=ctx.common.auth.release_plz_token.is_some(),
            has_github_token=ctx.common.auth.github_token.is_some(),
            has_cargo_registry_token=ctx.common.auth.cargo_registry_token.is_some(),
            "release_plz: starting publish"
        );
        // Build release request from local metadata + release-plz workspace configuration.
        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for publish")?;

        let mut request = release_plz_core::ReleaseRequest::new(metadata)
            .with_release_always(false)
            .with_branch_prefix(Some(match ctx.common.mode {
                ReleaseMode::Rc => ctx.rc_branch_prefix.clone(),
                ReleaseMode::Final => ctx.final_branch_prefix.clone(),
            }));

        if let Some(repo_url) = parse_remote_repo_url(&self.workspace_root)? {
            request = request.with_repo_url(repo_url.full_host());
        }

        // Enable GitHub release publishing when token + repo URL are available.
        if let Some(token) = ctx
            .common
            .auth
            .release_plz_token
            .clone()
            .or_else(|| ctx.common.auth.github_token.clone())
            && let Some(repo_url) = parse_remote_repo_url(&self.workspace_root)?
        {
            request = request.with_git_release(release_plz_core::GitRelease {
                forge: release_plz_core::GitForge::Github(release_plz_core::GitHub::new(
                    repo_url.owner,
                    repo_url.name,
                    token.into(),
                )),
            });
        }

        if let Some(token) = &ctx.common.auth.cargo_registry_token {
            // Use cargo registry token for crates.io publish step.
            request = request.with_token(token.clone());
        }

        // Validate mandatory publish fields before performing network operations.
        request.check_publish_fields()?;

        // Execute release-plz publish and always normalize to a JSON payload.
        let released = release_plz_core::release(&request)
            .await
            .context("failed to publish release with release-plz")?;

        let payload = match released {
            Some(value) => serde_json::to_value(value)
                .context("failed to serialize release-plz publish output")?,
            None => serde_json::json!([]),
        };
        info!(
            released_items=payload.as_array().map(|items| items.len()).unwrap_or(0),
            "release_plz: publish completed"
        );

        Ok(payload)
    }
}

/// Returns whether a package should be considered publishable for release checks/versions.
fn is_publishable(publish: &Option<Vec<String>>) -> bool {
    match publish {
        None => true,
        Some(registries) => !registries.is_empty(),
    }
}

/// Extracts package versions visible at comparison workspace commit for JSON reporting.
fn current_versions_from_metadata(metadata: &Metadata) -> Vec<ComparisonVersionView> {
    // Keep deterministic package order for stable JSON output.
    let mut versions = metadata
        .workspace_packages()
        .iter()
        .map(|package| ComparisonVersionView {
            package: package.name.to_string(),
            version: package.version.to_string(),
            publishable: is_publishable(&package.publish),
        })
        .collect::<Vec<_>>();
    versions.sort_by(|a, b| a.package.cmp(&b.package));
    versions
}

/// Applies computed versions in snapshot metadata and returns duplicate publishable versions.
fn duplicate_versions(
    snapshot_repo_root: &Path,
    metadata: Metadata,
    versions: &[VersionEntry],
) -> Result<Vec<(String, Vec<String>)>> {
    debug!(
        snapshot_repo_root=%snapshot_repo_root.display(),
        planned_versions=versions.len(),
        "release_plz: running duplicate version simulation"
    );
    let changes = version_changes_for_versions(versions);
    if !changes.is_empty() {
        apply_version_changes_with_metadata(changes, metadata)?;
    }

    let snapshot_manifest = snapshot_repo_root.join("Cargo.toml");
    let simulated_metadata = metadata_for_manifest(&snapshot_manifest)?;
    let simulated_workspace = Workspace::from_metadata(simulated_metadata);
    let duplicates = simulated_workspace.duplicate_publishable_versions();
    debug!(duplicate_groups=duplicates.len(), "release_plz: duplicate simulation completed");
    Ok(duplicates)
}

/// Extracts effective version overrides for publishable version entries.
fn version_changes_for_versions(
    versions: &[VersionEntry],
) -> BTreeMap<String, cargo_metadata::semver::Version> {
    versions
        .iter()
        .filter(|version| version.publishable)
        .map(|version| (version.package.clone(), version.next_effective.clone()))
        .collect()
}

/// Applies deterministic version overrides across workspace metadata.
fn apply_version_changes_with_metadata(
    changes: BTreeMap<String, cargo_metadata::semver::Version>,
    metadata: Metadata,
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }

    // Apply version overrides at workspace scope so dependency references are updated consistently.
    let version_changes = changes
        .into_iter()
        .map(|(package, version)| (package, VersionChange::new(version)))
        .collect();

    let request = SetVersionRequest::new(SetVersionSpec::Workspace(version_changes), metadata)?;
    set_version(&request).context("failed to apply mode-specific version overrides")
}

/// Parses origin remote into release-plz repository URL model.
fn parse_remote_repo_url(
    workspace_root: &std::path::Path,
) -> Result<Option<release_plz_core::RepoUrl>> {
    let Some(remote) = remote_origin_url(workspace_root)? else {
        return Ok(None);
    };
    let repo_url = release_plz_core::RepoUrl::new(&remote)
        .with_context(|| format!("failed to parse repository URL from `{remote}`"))?;
    Ok(Some(repo_url))
}

/// Computes publishable workspace package names changed since the provided baseline tag.
fn changed_publishable_packages_since_tag(
    repo_root: &std::path::Path,
    metadata: &Metadata,
    tag: &str,
) -> Result<std::collections::HashSet<String>> {
    // Use git diff against baseline tag to scope final-mode packages to actual changed crates.
    let changed_files = changed_files_since_tag(repo_root, tag)?;
    debug!(
        repo_root=%repo_root.display(),
        baseline_tag=%tag,
        changed_files=changed_files.len(),
        "release_plz: collected changed files since baseline tag"
    );

    let mut changed_packages = std::collections::HashSet::new();
    // Workspace-level files are treated as impacting all publishable packages.
    let workspace_wide_change = changed_files.iter().any(|path| {
        path == "Cargo.toml"
            || path == "Cargo.lock"
            || path == "release-plz.toml"
            || path.starts_with(".github/workflows/")
    });

    for package in metadata.workspace_packages() {
        if !is_publishable(&package.publish) {
            continue;
        }
        if workspace_wide_change {
            changed_packages.insert(package.name.to_string());
            continue;
        }

        let Some(package_dir) = package.manifest_path.parent() else {
            continue;
        };
        // Map changed file paths to package roots to determine touched publishable packages.
        if changed_files
            .iter()
            .map(|file| repo_root.join(file))
            .any(|file_path| file_path.starts_with(package_dir.as_std_path()))
        {
            changed_packages.insert(package.name.to_string());
        }
    }

    debug!(changed_packages=changed_packages.len(), "release_plz: computed changed publishable packages");
    Ok(changed_packages)
}
