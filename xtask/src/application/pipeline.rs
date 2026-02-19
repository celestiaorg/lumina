use std::path::PathBuf;

use anyhow::Result;
use tracing::info;

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::application::prepare::handle_prepare;
use crate::application::publish::handle_publish;
use crate::application::submit::{SubmitArgs, handle_submit};
use crate::domain::types::{
    ComputeVersionsContext, ExecuteContext, ExecuteReport, PublishContext, ReleaseReport,
    VersionsReport,
};

#[derive(Debug, Clone)]
pub struct ExecuteArgs {
    /// Context shared across check/prepare/submit stages.
    pub ctx: ExecuteContext,
    /// When true, submit stage skips creating commits/pushes while still reporting intended steps.
    pub dry_run: bool,
}

/// Top-level orchestrator that wires concrete adapters into command-level flows.
pub struct ReleasePipeline {
    release_engine: ReleasePlzAdapter,
    publisher: ReleasePlzAdapter,
    git: Git2Repo,
    pr_client: GitHubPrClient,
}

impl ReleasePipeline {
    /// Builds pipeline with concrete adapter implementations rooted at the selected workspace.
    pub fn new(workspace_root: PathBuf) -> Self {
        let release_engine = ReleasePlzAdapter::new(workspace_root.clone());
        let publisher = ReleasePlzAdapter::new(workspace_root.clone());
        let git = Git2Repo::new(workspace_root.clone());
        let pr_client = GitHubPrClient::new(workspace_root);

        Self {
            release_engine,
            publisher,
            git,
            pr_client,
        }
    }

    /// Computes release versions and returns a report.
    pub async fn compute_versions(&self, ctx: ComputeVersionsContext) -> Result<VersionsReport> {
        info!(
            mode=?ctx.common.mode,
            default_branch=%ctx.common.default_branch,
            requested_current_commit=?ctx.current_commit,
            "starting version computation"
        );
        let report = self.release_engine.versions(&ctx).await?;
        info!(
            mode=?report.mode,
            current_commit=%report.current_commit,
            previous_commit=%report.previous_commit,
            latest_release_tag=?report.latest_release_tag,
            latest_non_rc_release_tag=?report.latest_non_rc_release_tag,
            planned_versions=report.planned_versions.len(),
            "version computation completed"
        );
        Ok(report)
    }

    /// Full non-publishing flow: compute versions -> prepare -> submit.
    /// Publishing is intentionally kept in a separate command.
    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        info!(
            mode=?args.ctx.common.mode,
            default_branch=%args.ctx.common.default_branch,
            dry_run=args.dry_run,
            "starting execute"
        );
        // Stage 1: compute versions.
        let versions = self
            .compute_versions(args.ctx.to_compute_versions_context())
            .await?;

        // Stage 2: branch/artifact preparation.
        let prepare_ctx = args.ctx.to_prepare_context();
        let prepare = handle_prepare(
            &self.git,
            &self.pr_client,
            &self.release_engine,
            prepare_ctx,
            &versions,
        )
        .await?;

        // Stage 3: commit/push/PR update.
        let submit_ctx = args.ctx.to_submit_context();
        let submit = handle_submit(
            &self.git,
            &self.pr_client,
            SubmitArgs {
                ctx: submit_ctx,
                dry_run: args.dry_run,
                branch_name_override: Some(prepare.branch_name.clone()),
                update_strategy_override: Some(prepare.update_strategy),
            },
        )
        .await?;

        let report = ExecuteReport {
            versions,
            prepare,
            submit,
        };
        info!(branch=%report.submit.branch_name, pushed=report.submit.pushed, "execute completed");
        Ok(report)
    }

    /// Runs registry/GitHub publishing only, using release-plz publish semantics.
    pub async fn publish(&self, ctx: PublishContext) -> Result<ReleaseReport> {
        info!(mode=?ctx.common.mode, default_branch=%ctx.common.default_branch, "starting publish");
        let report = handle_publish(&self.publisher, ctx).await?;
        info!(published = report.published, "publish completed");
        Ok(report)
    }
}
