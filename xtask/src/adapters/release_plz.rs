use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use cargo_metadata::{
    DependencyKind, Metadata, MetadataCommand, camino::Utf8Path, semver::Prerelease,
};
use release_plz_core::set_version::{
    SetVersionRequest, SetVersionSpec, VersionChange, set_version,
};
use release_plz_core::update_request::UpdateRequest;
use tracing::info;

use crate::adapters::git_refs::{
    commit_for_tag, latest_non_rc_release_tag_on_branch, latest_release_tag_on_branch,
    remote_origin_url, resolve_current_commit, snapshot_workspace_to_temp,
};
use crate::adapters::metadata::metadata_for_manifest;
use crate::domain::types::{
    ComputeVersionsContext, PlannedVersion, PublishContext, ReleaseMode, VersionsReport,
};
use crate::domain::versioning::{
    convert_release_version_to_rc, to_stable_if_prerelease, validate_rc_conversion,
};

#[derive(Debug)]
struct SnapshotState {
    snapshot: crate::adapters::git_refs::RepoSnapshot,
    metadata: Metadata,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
enum VersionBumpKind {
    None,
    Patch,
    Minor,
    Major,
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
    /// Previous commit must be resolvable for both rc and final modes.
    pub async fn versions(&self, ctx: &ComputeVersionsContext) -> Result<VersionsReport> {
        let report = self.versions_from_context(ctx).await?;
        info!(
            current_commit=%report.current_commit,
            previous_commit=%report.previous_commit,
            latest_release_tag=?report.latest_release_tag,
            latest_non_rc_release_tag=?report.latest_non_rc_release_tag,
            planned_versions=report.planned_versions.len(),
            "release_plz: versions computed"
        );

        Ok(report)
    }

    /// Computes versions from an isolated snapshot at current commit and gathers reporting context.
    async fn versions_from_context(&self, ctx: &ComputeVersionsContext) -> Result<VersionsReport> {
        // Resolve the current commit used as analysis input.
        let current_commit = self.resolve_current_for_context(ctx)?;
        // Load isolated snapshot + metadata at that commit.
        let snapshot_state =
            self.load_snapshot_state(&current_commit, &ctx.common.default_branch)?;
        let temp_workspace_root = snapshot_state.snapshot.repo_root.as_path();
        let latest_release_tag =
            latest_release_tag_on_branch(temp_workspace_root, &ctx.common.default_branch)?;
        let latest_non_rc_release_tag =
            latest_non_rc_release_tag_on_branch(temp_workspace_root, &ctx.common.default_branch)?;
        // Resolve previous release point from already-fetched tags so final mode is always anchored
        // to the latest non-RC release.
        let previous_tag = match ctx.common.mode {
            ReleaseMode::Rc => latest_release_tag
                .as_deref()
                .context("rc mode requires latest release tag for previous-commit reporting")?,
            ReleaseMode::Final => latest_non_rc_release_tag.as_deref().context(
                "final mode requires latest non-RC release tag for previous-commit reporting",
            )?,
        };
        let previous_commit = commit_for_tag(temp_workspace_root, previous_tag)?;
        info!(
            mode=?ctx.common.mode,
            previous_tag=%previous_tag,
            previous_commit=%previous_commit,
            snapshot_repo_root=%temp_workspace_root.display(),
            "release_plz: resolved previous release point"
        );
        // Load previous release snapshot so release-plz can use it as previous-state input.
        let previous_snapshot =
            self.load_snapshot_state(&previous_commit, &ctx.common.default_branch)?;
        let previous_manifest_path = previous_snapshot.snapshot.repo_root.join("Cargo.toml");

        let planned_versions = self
            .compute_snapshot_versions(
                &snapshot_state.metadata,
                &previous_manifest_path,
                ctx.common.mode,
            )
            .await?;

        Ok(VersionsReport {
            mode: ctx.common.mode,
            previous_commit,
            latest_release_tag,
            latest_non_rc_release_tag,
            current_commit,
            planned_versions,
        })
    }

    /// Resolves current commit for check/prepare computation.
    fn resolve_current_for_context(&self, ctx: &ComputeVersionsContext) -> Result<String> {
        let commit = resolve_current_commit(
            &self.workspace_root,
            &ctx.common.default_branch,
            ctx.current_commit.as_deref(),
        )?;
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
        Ok(SnapshotState { snapshot, metadata })
    }

    /// Computes package versions from snapshot metadata and applies transitive bump policy.
    pub async fn compute_snapshot_versions(
        &self,
        metadata: &Metadata,
        previous_manifest_path: &std::path::Path,
        mode: ReleaseMode,
    ) -> Result<Vec<PlannedVersion>> {
        // Compute versions from release-plz output + transitive bump policy.
        let versions = self
            .versions_for_mode(metadata, mode, previous_manifest_path)
            .await?;

        Ok(versions)
    }

    /// Uses release-plz next-version rules, then maps to mode-specific effective versions.
    pub async fn versions_for_mode(
        &self,
        metadata: &Metadata,
        mode: ReleaseMode,
        previous_manifest_path: &std::path::Path,
    ) -> Result<Vec<PlannedVersion>> {
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

        let current_versions = publishable_workspace_current_versions(metadata);
        let mut planned_release_versions = collect_planned_release_versions(
            &current_versions,
            packages_to_update
                .updates()
                .iter()
                .filter(|(package, _)| is_publishable(&package.publish))
                .map(|(package, update)| (package.name.to_string(), update.version.clone())),
        );
        apply_and_log_transitive_bump_policy(
            metadata,
            &current_versions,
            &mut planned_release_versions,
            "release_plz: applied transitive dependency bump policy",
        );

        let mut versions = planned_release_versions
            .into_iter()
            .filter_map(|(package, next_release)| {
                let current = current_versions.get(&package)?.clone();
                Some((package, current, next_release))
            })
            .map(|(package, current, next_release)| {
                // Mode-specific projection:
                // RC => convert release-plz next version into rc.N variant.
                // Final => normalize prerelease to stable if needed.
                let next_effective = match mode {
                    ReleaseMode::Rc => convert_release_version_to_rc(&next_release)?,
                    ReleaseMode::Final => {
                        to_stable_if_prerelease(&next_release).unwrap_or(next_release.clone())
                    }
                };
                if matches!(mode, ReleaseMode::Rc) {
                    validate_rc_conversion(&next_release, &next_effective).with_context(|| {
                        format!("invalid rc transition computed for package `{package}`")
                    })?;
                }

                Ok(PlannedVersion {
                    package,
                    current: current.to_string(),
                    next_effective: next_effective.to_string(),
                    publishable: true,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        versions.sort_by(|a, b| a.package.cmp(&b.package));
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
        planned_versions: &[PlannedVersion],
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
        let previous_snapshot = self.load_snapshot_state(previous_commit, default_branch)?;
        let previous_manifest_path = previous_snapshot.snapshot.repo_root.join("Cargo.toml");
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

        let (_packages_to_update, _temp_repo) = release_plz_core::update(&update_request)
            .await
            .context("failed to regenerate release artifacts with release-plz update")?;

        let mut descriptions =
            vec!["regenerated release versions/changelog with release-plz update".to_string()];

        // Reuse already computed effective versions to keep compute/apply logic single-sourced.
        let mode_specific_changes = planned_versions
            .iter()
            .filter(|version| version.publishable)
            .map(|version| {
                let parsed = cargo_metadata::semver::Version::parse(&version.next_effective)
                    .with_context(|| {
                        format!(
                            "failed to parse planned effective version `{}` for package `{}`",
                            version.next_effective, version.package
                        )
                    })?;
                Ok((version.package.clone(), parsed))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;

        if !mode_specific_changes.is_empty() {
            apply_version_changes_with_metadata(mode_specific_changes, metadata)?;
            descriptions
                .push("applied computed release versions to workspace references".to_string());
        }

        if let Some(latest_release) = latest_release_tag {
            // Report release-plz-style changelog previous-release point.
            descriptions.push(format!(
                "previous release tag for changelog: `{latest_release}`"
            ));
            descriptions.push(format!("includes changes since `{latest_release}`"));
        }
        if matches!(mode, ReleaseMode::Final)
            && let Some(latest_non_rc_release) = latest_non_rc_release_tag
        {
            // Report stable previous tag separately to remove RC/final ambiguity.
            descriptions.push(format!(
                "final previous non-RC release tag: `{latest_non_rc_release}`"
            ));
            descriptions.push(format!(
                "includes changes since latest non-RC release `{latest_non_rc_release}`"
            ));
        }

        info!(
            description_items = descriptions.len(),
            "release_plz: artifact regeneration completed"
        );
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
            released_items = payload.as_array().map(|items| items.len()).unwrap_or(0),
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

/// Captures current versions for publishable workspace members.
fn publishable_workspace_current_versions(
    metadata: &Metadata,
) -> BTreeMap<String, cargo_metadata::semver::Version> {
    metadata
        .workspace_packages()
        .iter()
        .filter(|package| is_publishable(&package.publish))
        .map(|package| (package.name.to_string(), package.version.clone()))
        .collect()
}

/// Collects release-plz proposed next versions for publishable workspace members.
fn collect_planned_release_versions<I>(
    current_versions: &BTreeMap<String, cargo_metadata::semver::Version>,
    updates: I,
) -> BTreeMap<String, cargo_metadata::semver::Version>
where
    I: IntoIterator<Item = (String, cargo_metadata::semver::Version)>,
{
    updates
        .into_iter()
        .filter(|(package, _)| current_versions.contains_key(package.as_str()))
        .collect()
}

/// Applies and logs transitive bump propagation over planned release versions.
fn apply_and_log_transitive_bump_policy(
    metadata: &Metadata,
    current_versions: &BTreeMap<String, cargo_metadata::semver::Version>,
    planned_release_versions: &mut BTreeMap<String, cargo_metadata::semver::Version>,
    phase: &str,
) {
    let dependency_graph = workspace_publishable_dependencies(metadata);
    let transitive_bumps = apply_transitive_bump_policy(
        current_versions,
        &dependency_graph,
        planned_release_versions,
    );
    if transitive_bumps > 0 {
        info!(
            count = transitive_bumps,
            phase, "release_plz: applied transitive bump policy"
        );
    }
}

/// Returns workspace dependency graph restricted to publishable workspace members.
/// Edges are `dependent -> dependency`.
fn workspace_publishable_dependencies(metadata: &Metadata) -> BTreeMap<String, Vec<String>> {
    let publishable_names = metadata
        .workspace_packages()
        .iter()
        .filter(|package| is_publishable(&package.publish))
        .map(|package| package.name.to_string())
        .collect::<HashSet<_>>();

    metadata
        .workspace_packages()
        .iter()
        .filter(|package| publishable_names.contains(package.name.as_str()))
        .map(|package| {
            let dependencies = package
                .dependencies
                .iter()
                // Dev dependencies do not affect published API compatibility.
                .filter(|dependency| dependency.kind != DependencyKind::Development)
                .filter_map(|dependency| {
                    publishable_names
                        .contains(dependency.name.as_str())
                        .then(|| dependency.name.clone())
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            (package.name.to_string(), dependencies)
        })
        .collect()
}

/// Applies transitive bump policy:
/// if dependency gets patch/minor/major bump, dependent is bumped at least at same level.
fn apply_transitive_bump_policy(
    current_versions: &BTreeMap<String, cargo_metadata::semver::Version>,
    dependencies: &BTreeMap<String, Vec<String>>,
    planned_next_versions: &mut BTreeMap<String, cargo_metadata::semver::Version>,
) -> usize {
    let mut forced_bump_count = 0usize;

    loop {
        let mut changed = false;
        for (package, dependency_list) in dependencies {
            let Some(current) = current_versions.get(package) else {
                continue;
            };

            let required = dependency_list
                .iter()
                .filter_map(|dependency| {
                    let dependency_current = current_versions.get(dependency)?;
                    let dependency_next = planned_next_versions
                        .get(dependency)
                        .unwrap_or(dependency_current);
                    Some(classify_version_bump(dependency_current, dependency_next))
                })
                .max()
                .unwrap_or(VersionBumpKind::None);

            let planned = planned_next_versions
                .get(package)
                .cloned()
                .unwrap_or_else(|| current.clone());
            if required == VersionBumpKind::None
                || classify_version_bump(current, &planned) >= required
            {
                continue;
            }

            let next_planned = bump_version(current, required);
            if next_planned == planned {
                continue;
            }

            planned_next_versions.insert(package.clone(), next_planned.clone());
            forced_bump_count = forced_bump_count.saturating_add(1);
            changed = true;
        }

        if !changed {
            break;
        }
    }

    forced_bump_count
}

/// Classifies semantic bump level based on major/minor/patch transition.
fn classify_version_bump(
    current: &cargo_metadata::semver::Version,
    next: &cargo_metadata::semver::Version,
) -> VersionBumpKind {
    let current = stable_version(current);
    let next = stable_version(next);

    if next.major > current.major {
        VersionBumpKind::Major
    } else if next.major == current.major && next.minor > current.minor {
        VersionBumpKind::Minor
    } else if next.major == current.major
        && next.minor == current.minor
        && next.patch > current.patch
    {
        VersionBumpKind::Patch
    } else {
        VersionBumpKind::None
    }
}

/// Returns a version bumped from current by the requested semantic level.
fn bump_version(
    current: &cargo_metadata::semver::Version,
    bump: VersionBumpKind,
) -> cargo_metadata::semver::Version {
    let mut next = stable_version(current);
    match bump {
        VersionBumpKind::None => {}
        VersionBumpKind::Patch => {
            next.patch = next.patch.saturating_add(1);
        }
        VersionBumpKind::Minor => {
            next.minor = next.minor.saturating_add(1);
            next.patch = 0;
        }
        VersionBumpKind::Major => {
            next.major = next.major.saturating_add(1);
            next.minor = 0;
            next.patch = 0;
        }
    }
    next
}

/// Removes prerelease/build metadata for stable bump comparisons.
fn stable_version(version: &cargo_metadata::semver::Version) -> cargo_metadata::semver::Version {
    let mut stable = version.clone();
    stable.pre = Prerelease::EMPTY;
    stable.build = Default::default();
    stable
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cargo_metadata::semver::Version;

    use super::{
        VersionBumpKind, apply_transitive_bump_policy, bump_version, classify_version_bump,
    };

    #[test]
    fn direct_minor_bump_propagates_to_dependent() {
        let current = BTreeMap::from([
            ("a".to_string(), Version::parse("1.0.0").unwrap()),
            ("b".to_string(), Version::parse("2.0.0").unwrap()),
        ]);
        let dependencies = BTreeMap::from([("b".to_string(), vec!["a".to_string()])]);
        let mut planned = BTreeMap::from([("a".to_string(), Version::parse("1.1.0").unwrap())]);

        let forced = apply_transitive_bump_policy(&current, &dependencies, &mut planned);

        assert_eq!(forced, 1);
        assert_eq!(
            planned.get("b").unwrap().to_string(),
            Version::parse("2.1.0").unwrap().to_string()
        );
    }

    #[test]
    fn transitive_major_bump_propagates_across_chain() {
        let current = BTreeMap::from([
            ("a".to_string(), Version::parse("1.0.0").unwrap()),
            ("b".to_string(), Version::parse("1.2.0").unwrap()),
            ("c".to_string(), Version::parse("3.4.5").unwrap()),
        ]);
        let dependencies = BTreeMap::from([
            ("b".to_string(), vec!["a".to_string()]),
            ("c".to_string(), vec!["b".to_string()]),
        ]);
        let mut planned = BTreeMap::from([("a".to_string(), Version::parse("2.0.0").unwrap())]);

        let forced = apply_transitive_bump_policy(&current, &dependencies, &mut planned);

        assert_eq!(forced, 2);
        assert_eq!(planned.get("b").unwrap().to_string(), "2.0.0");
        assert_eq!(planned.get("c").unwrap().to_string(), "4.0.0");
    }

    #[test]
    fn existing_higher_bump_is_not_downgraded() {
        let current = BTreeMap::from([
            ("a".to_string(), Version::parse("1.0.0").unwrap()),
            ("b".to_string(), Version::parse("1.0.0").unwrap()),
        ]);
        let dependencies = BTreeMap::from([("b".to_string(), vec!["a".to_string()])]);
        let mut planned = BTreeMap::from([
            ("a".to_string(), Version::parse("1.0.1").unwrap()),
            ("b".to_string(), Version::parse("2.0.0").unwrap()),
        ]);

        let forced = apply_transitive_bump_policy(&current, &dependencies, &mut planned);

        assert_eq!(forced, 0);
        assert_eq!(planned.get("b").unwrap().to_string(), "2.0.0");
    }

    #[test]
    fn bump_classifier_ignores_prerelease_for_comparison() {
        let current = Version::parse("1.2.3-rc.4").unwrap();
        let next = Version::parse("1.2.4-rc.1").unwrap();
        let kind = classify_version_bump(&current, &next);
        assert_eq!(kind, VersionBumpKind::Patch);
    }

    #[test]
    fn bump_version_resets_components() {
        let current = Version::parse("4.5.6-rc.3").unwrap();
        assert_eq!(
            bump_version(&current, VersionBumpKind::Minor).to_string(),
            "4.6.0"
        );
        assert_eq!(
            bump_version(&current, VersionBumpKind::Major).to_string(),
            "5.0.0"
        );
    }
}
