use std::path::PathBuf;

use anyhow::{Context, Result};
use cargo_metadata::MetadataCommand;
use tracing::info;

use crate::adapters::github::parse_remote_repo_url;
use crate::domain::types::{
    AuthContext, ExecuteReport, PublishContext, ReleaseMode, UpdatedPackage,
};

#[derive(Debug, Clone)]
pub struct ReleasePlzAdapter {
    workspace_root: PathBuf,
}

impl ReleasePlzAdapter {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    /// Opens or updates a release PR via release-plz.
    /// Returns `None` when all packages are already up-to-date.
    pub async fn release_pr(
        &self,
        mode: ReleaseMode,
        auth: &AuthContext,
    ) -> Result<Option<ExecuteReport>> {
        info!(
            mode=?mode,
            workspace_root=%self.workspace_root.display(),
            "release_plz: running release_pr"
        );

        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for release_pr")?;

        let release_mode = match mode {
            ReleaseMode::Rc => release_plz_core::ReleaseMode::Rc,
            ReleaseMode::Final => release_plz_core::ReleaseMode::Stable,
        };

        let pr_title = match mode {
            ReleaseMode::Rc => "chore: release rc",
            ReleaseMode::Final => "chore: release final",
        };

        let mut update_request = release_plz_core::update_request::UpdateRequest::new(metadata)
            .context("failed to build release-plz update request")?
            .with_release_mode(release_mode)
            .with_allow_dirty(true);

        if let Some(token) = auth
            .release_plz_token
            .clone()
            .or_else(|| auth.github_token.clone())
            && let Some(repo_url) = parse_remote_repo_url(&self.workspace_root)?
        {
            update_request = update_request.with_git_client(release_plz_core::GitForge::Github(
                release_plz_core::GitHub::new(repo_url.owner, repo_url.name, token.into()),
            ));
        }

        let request = release_plz_core::ReleasePrRequest::new(update_request)
            .with_pr_name_template(Some(pr_title.to_string()));

        let result = release_plz_core::release_pr(&request)
            .await
            .context("failed to run release-plz release_pr")?;

        let report = result.map(|pr| {
            let updated_packages = pr
                .releases
                .iter()
                .map(|r| UpdatedPackage {
                    package: r.package_name.clone(),
                    version: r.version.to_string(),
                })
                .collect();

            ExecuteReport {
                head_branch: pr.head_branch,
                pr_url: Some(pr.html_url.to_string()),
                updated_packages,
            }
        });

        info!(
            pr_created = report.is_some(),
            "release_plz: release_pr completed"
        );

        Ok(report)
    }

    pub async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        info!(
            no_artifacts=ctx.no_artifacts,
            workspace_root=%self.workspace_root.display(),
            has_release_plz_token=ctx.auth.release_plz_token.is_some(),
            has_github_token=ctx.auth.github_token.is_some(),
            has_cargo_registry_token=ctx.auth.cargo_registry_token.is_some(),
            "release_plz: starting publish"
        );

        let metadata = MetadataCommand::new()
            .current_dir(&self.workspace_root)
            .exec()
            .context("failed to load cargo metadata for publish")?;

        let mut request = release_plz_core::ReleaseRequest::new(metadata)
            .with_release_always(false)
            .with_dry_run(ctx.no_artifacts);

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
        info!(
            released_items = payload.as_array().map(|items| items.len()).unwrap_or(0),
            "release_plz: publish completed"
        );

        Ok(payload)
    }
}
