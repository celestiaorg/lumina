use std::path::PathBuf;

use anyhow::{Context, Result};
use cargo_metadata::MetadataCommand;
use release_plz_core::update_request::UpdateRequest;
use tracing::info;

use crate::adapters::git_refs::parse_remote_repo_url;
use crate::domain::types::{PublishContext, ReleaseMode, UpdatedPackage};

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

    /// Runs release-plz update with the appropriate ReleaseMode.
    /// Writes all artifacts (Cargo.toml versions, Cargo.lock, changelogs) to disk.
    /// Returns the list of updated packages.
    pub async fn update(&self, mode: ReleaseMode) -> Result<Vec<UpdatedPackage>> {
        info!(
            mode=?mode,
            workspace_root=%self.workspace_root.display(),
            "release_plz: running update"
        );

        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for update")?;

        let release_mode = match mode {
            ReleaseMode::Rc => release_plz_core::ReleaseMode::Rc,
            ReleaseMode::Final => release_plz_core::ReleaseMode::Stable,
        };

        let update_request = UpdateRequest::new(metadata)
            .context("failed to build release-plz update request")?
            .with_release_mode(release_mode)
            .with_allow_dirty(true);

        let (packages_update, _temp_repo) = release_plz_core::update(&update_request)
            .await
            .context("failed to run release-plz update")?;

        let updated: Vec<UpdatedPackage> = packages_update
            .updates()
            .iter()
            .map(|(pkg, upd)| UpdatedPackage {
                package: pkg.name.to_string(),
                version: upd.version.to_string(),
            })
            .collect();

        info!(
            updated_count = updated.len(),
            "release_plz: update completed"
        );

        Ok(updated)
    }

    /// Runs release-plz publish flow and returns machine-readable release payload.
    pub async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        info!(
            mode=?ctx.common.mode,
            default_branch=%ctx.common.default_branch,
            no_artifacts=ctx.no_artifacts,
            workspace_root=%self.workspace_root.display(),
            has_release_plz_token=ctx.common.auth.release_plz_token.is_some(),
            has_github_token=ctx.common.auth.github_token.is_some(),
            has_cargo_registry_token=ctx.common.auth.cargo_registry_token.is_some(),
            "release_plz: starting publish"
        );

        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for publish")?;

        let mut request = release_plz_core::ReleaseRequest::new(metadata)
            .with_release_always(false)
            .with_dry_run(ctx.no_artifacts)
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
        info!(
            released_items = payload.as_array().map(|items| items.len()).unwrap_or(0),
            "release_plz: publish completed"
        );

        Ok(payload)
    }
}
