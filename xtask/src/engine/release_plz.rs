use std::path::PathBuf;

use anyhow::{Context, Result};
use cargo_metadata::{Metadata, MetadataCommand};
use release_plz_core::set_version::{
    SetVersionRequest, SetVersionSpec, VersionChange, set_version,
};
use release_plz_core::update_request::UpdateRequest;
use serde::Deserialize;

use crate::domain::types::{
    BaselinePolicy, ComparisonVersionView, Plan, ReleaseContext, ReleaseMode,
};
use crate::domain::versioning::{convert_release_version_to_rc, to_stable_if_prerelease};
use crate::infra::git_refs::{
    changed_files_since_tag, checkout_commit, clone_workspace_to_temp, commit_for_tag,
    latest_non_rc_release_tag_on_branch, latest_release_tag_on_branch, remote_origin_url,
    resolve_comparison_commit,
};
use crate::infra::metadata::metadata_for_manifest;

#[derive(Debug, Clone)]
pub struct PlanComputation {
    pub baseline_policy: BaselinePolicy,
    pub comparison_commit: String,
    pub comparison_versions: Vec<ComparisonVersionView>,
    pub plans: Vec<Plan>,
}

#[derive(Debug, Clone)]
struct ContextPlan {
    comparison_commit: String,
    comparison_versions: Vec<ComparisonVersionView>,
    plans: Vec<Plan>,
}

#[derive(Debug, Clone)]
pub struct ReleasePlzEngine {
    workspace_root: PathBuf,
}

impl ReleasePlzEngine {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    pub async fn plan(&self, ctx: &ReleaseContext) -> Result<PlanComputation> {
        if matches!(ctx.mode, ReleaseMode::Final)
            && latest_non_rc_release_tag_on_branch(&self.workspace_root, &ctx.default_branch)?
                .is_none()
        {
            anyhow::bail!("final mode requires at least one non-RC release tag");
        }

        let from_context = self.plan_from_context(ctx).await?;

        Ok(PlanComputation {
            baseline_policy: ctx.baseline_policy(),
            comparison_commit: from_context.comparison_commit,
            comparison_versions: from_context.comparison_versions,
            plans: from_context.plans,
        })
    }

    async fn plan_from_context(&self, ctx: &ReleaseContext) -> Result<ContextPlan> {
        let comparison_commit = resolve_comparison_commit(
            &self.workspace_root,
            &ctx.default_branch,
            ctx.base_commit.as_deref(),
        )?;
        let snapshot = clone_workspace_to_temp(&self.workspace_root)?;
        let _hold_temp_dir = &snapshot.temp_dir;
        let temp_workspace_root = snapshot.repo_root.as_path();
        let temp_manifest = temp_workspace_root.join("Cargo.toml");

        checkout_commit(temp_workspace_root, &comparison_commit, &ctx.default_branch)?;

        let temp_metadata = metadata_for_manifest(&temp_manifest)?;
        let mut reported_comparison_commit = comparison_commit.clone();
        let mut reported_comparison_versions = comparison_versions_from_metadata(&temp_metadata);

        if matches!(ctx.mode, ReleaseMode::Final) {
            let baseline_tag =
                latest_non_rc_release_tag_on_branch(temp_workspace_root, &ctx.default_branch)?
                    .context(
                        "final mode requires latest non-RC release tag for baseline reporting",
                    )?;
            let baseline_commit = commit_for_tag(temp_workspace_root, &baseline_tag)?;

            checkout_commit(temp_workspace_root, &baseline_commit, &ctx.default_branch)?;
            let baseline_metadata = metadata_for_manifest(&temp_manifest)?;
            reported_comparison_commit = baseline_commit;
            reported_comparison_versions = comparison_versions_from_metadata(&baseline_metadata);

            // Restore snapshot to the comparison commit used for plan generation.
            checkout_commit(temp_workspace_root, &comparison_commit, &ctx.default_branch)?;
        }

        let plans = self
            .plan_from_snapshot(&temp_metadata, temp_workspace_root, ctx)
            .await?;

        Ok(ContextPlan {
            comparison_commit: reported_comparison_commit,
            comparison_versions: reported_comparison_versions,
            plans,
        })
    }

    pub async fn plan_from_snapshot(
        &self,
        metadata: &Metadata,
        snapshot_repo_root: &std::path::Path,
        ctx: &ReleaseContext,
    ) -> Result<Vec<Plan>> {
        let mut plans = self.plan_for_mode(metadata, ctx.mode).await?;

        if matches!(ctx.mode, ReleaseMode::Final) {
            let baseline_tag = latest_non_rc_release_tag_on_branch(
                snapshot_repo_root,
                &ctx.default_branch,
            )?
            .context(
                "final mode requires at least one non-RC release tag in comparison workspace",
            )?;
            let changed_packages = changed_publishable_packages_since_tag(
                snapshot_repo_root,
                metadata,
                &baseline_tag,
            )?;
            plans.retain(|plan| changed_packages.contains(&plan.package));
        }

        Ok(plans)
    }

    pub async fn plan_for_mode(
        &self,
        metadata: &Metadata,
        mode: ReleaseMode,
    ) -> Result<Vec<Plan>> {
        let update_request = UpdateRequest::new(metadata.clone())
            .context("failed to build release-plz update request")?
            .with_allow_dirty(true);
        let (packages_to_update, _temp_repo) = release_plz_core::next_versions(&update_request)
            .await
            .context("failed to compute next versions with release-plz")?;

        let mut plans = packages_to_update
            .updates()
            .iter()
            .filter(|(package, _)| is_publishable(&package.publish))
            .map(|(package, update)| {
                let current = package.version.clone();
                let next_release = update.version.clone();
                let next_effective = match mode {
                    ReleaseMode::Rc => convert_release_version_to_rc(&next_release)?,
                    ReleaseMode::Final => {
                        to_stable_if_prerelease(&next_release).unwrap_or(next_release.clone())
                    }
                };

                Ok(Plan {
                    package: package.name.to_string(),
                    current,
                    next_release,
                    next_effective,
                    publishable: true,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        plans.sort_by(|a, b| a.package.cmp(&b.package));
        Ok(plans)
    }

    pub fn workspace_root(&self) -> &std::path::Path {
        &self.workspace_root
    }

    pub async fn regenerate_artifacts(&self, ctx: &ReleaseContext) -> Result<Vec<String>> {
        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for release artifact regeneration")?;

        let update_request = UpdateRequest::new(metadata.clone())
            .context("failed to build update request for release artifact regeneration")?
            .with_allow_dirty(true);

        let (packages_to_update, _temp_repo) = release_plz_core::update(&update_request)
            .await
            .context("failed to regenerate release artifacts with release-plz update")?;

        let mut actions =
            vec!["regenerated release versions/changelog with release-plz update".to_string()];

        let mode_specific_changes = packages_to_update
            .updates()
            .iter()
            .filter(|(package, _)| is_publishable(&package.publish))
            .filter_map(|(package, update)| {
                let next = match ctx.mode {
                    ReleaseMode::Rc => convert_release_version_to_rc(&update.version).ok(),
                    ReleaseMode::Final => Some(
                        to_stable_if_prerelease(&update.version).unwrap_or(update.version.clone()),
                    ),
                }?;
                Some((package.name.to_string(), next))
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        if !mode_specific_changes.is_empty() {
            apply_version_changes_with_metadata(mode_specific_changes, metadata)?;
            match ctx.mode {
                ReleaseMode::Rc => actions.push(
                    "converted release-plz versions to rc variants and updated workspace references"
                        .to_string(),
                ),
                ReleaseMode::Final => {
                    actions.push("normalized prerelease versions to stable".to_string())
                }
            }
        }

        if let Some(latest_release) =
            latest_release_tag_on_branch(&self.workspace_root, &ctx.default_branch)?
        {
            actions.push(format!(
                "changelog baseline: latest release tag `{latest_release}`"
            ));
            actions.push(format!("includes changes since `{latest_release}`"));
        }
        if matches!(ctx.mode, ReleaseMode::Final)
            && let Some(latest_non_rc_release) =
                latest_non_rc_release_tag_on_branch(&self.workspace_root, &ctx.default_branch)?
        {
            actions.push(format!(
                "final baseline: latest non-RC release tag `{latest_non_rc_release}`"
            ));
            actions.push(format!(
                "includes changes since latest non-RC release `{latest_non_rc_release}`"
            ));
        }

        Ok(actions)
    }

    pub async fn publish(&self, ctx: &ReleaseContext) -> Result<serde_json::Value> {
        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for publish")?;

        let mut request = release_plz_core::ReleaseRequest::new(metadata)
            .with_release_always(read_release_always(&self.workspace_root)?)
            .with_branch_prefix(Some(match ctx.mode {
                ReleaseMode::Rc => ctx.rc_branch_prefix.clone(),
                ReleaseMode::Final => ctx.final_branch_prefix.clone(),
            }));

        if let Some(repo_url) = parse_remote_repo_url(&self.workspace_root)? {
            request = request.with_repo_url(repo_url.full_host());
        }

        if let Some(token) = ctx
            .auth
            .release_plz_token
            .clone()
            .or_else(|| ctx.auth.github_token.clone())
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

        if let Some(token) = &ctx.auth.cargo_registry_token {
            request = request.with_token(token.clone());
        }

        request.check_publish_fields()?;

        let released = release_plz_core::release(&request)
            .await
            .context("failed to publish release with release-plz")?;

        let payload = match released {
            Some(value) => serde_json::to_value(value)
                .context("failed to serialize release-plz publish output")?,
            None => serde_json::json!([]),
        };

        Ok(payload)
    }
}

fn is_publishable(publish: &Option<Vec<String>>) -> bool {
    match publish {
        None => true,
        Some(registries) => !registries.is_empty(),
    }
}

fn comparison_versions_from_metadata(metadata: &Metadata) -> Vec<ComparisonVersionView> {
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

fn apply_version_changes_with_metadata(
    changes: std::collections::BTreeMap<String, cargo_metadata::semver::Version>,
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
    set_version(&request).context("failed to apply mode-specific version overrides")
}

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

fn read_release_always(workspace_root: &std::path::Path) -> Result<bool> {
    #[derive(Debug, Deserialize, Default)]
    struct ReleasePlzConfig {
        workspace: Option<WorkspaceSection>,
    }

    #[derive(Debug, Deserialize, Default)]
    struct WorkspaceSection {
        release_always: Option<bool>,
    }

    let config_path = workspace_root.join("release-plz.toml");
    if !config_path.exists() {
        return Ok(false);
    }

    let body = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let parsed: ReleasePlzConfig = toml::from_str(&body)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;
    Ok(parsed
        .workspace
        .and_then(|workspace| workspace.release_always)
        .unwrap_or(false))
}

fn changed_publishable_packages_since_tag(
    repo_root: &std::path::Path,
    metadata: &Metadata,
    tag: &str,
) -> Result<std::collections::HashSet<String>> {
    let changed_files = changed_files_since_tag(repo_root, tag)?;

    let mut changed_packages = std::collections::HashSet::new();
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
        if changed_files
            .iter()
            .map(|file| repo_root.join(file))
            .any(|file_path| file_path.starts_with(package_dir.as_std_path()))
        {
            changed_packages.insert(package.name.to_string());
        }
    }

    Ok(changed_packages)
}
