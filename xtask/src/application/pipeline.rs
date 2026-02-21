use std::path::PathBuf;

use anyhow::Result;
use tracing::info;

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::application::prepare::handle_prepare;
use crate::application::publish::handle_publish;
use crate::application::submit::{SubmitArgs, handle_submit};
use crate::domain::types::{ExecuteContext, ExecuteReport, PublishContext, ReleaseReport};

#[derive(Debug, Clone)]
pub struct ExecuteArgs {
    /// Context shared across prepare/submit stages.
    pub ctx: ExecuteContext,
}

/// Top-level orchestrator that wires concrete adapters into command-level flows.
pub struct ReleasePipeline {
    release_engine: ReleasePlzAdapter,
    git: Git2Repo,
    pr_client: GitHubPrClient,
}

impl ReleasePipeline {
    /// Builds pipeline with concrete adapter implementations rooted at the selected workspace.
    pub fn new(workspace_root: PathBuf) -> Self {
        let release_engine = ReleasePlzAdapter::new(workspace_root.clone());
        let git = Git2Repo::new(workspace_root.clone());
        let pr_client = GitHubPrClient::new(workspace_root);

        Self {
            release_engine,
            git,
            pr_client,
        }
    }

    /// Full non-publishing flow: prepare (branch + update) -> submit (commit/push/PR).
    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        // Stage 1: create branch, run release-plz update.
        let prepare_ctx = args.ctx.to_prepare_context();
        let prepare = handle_prepare(&self.git, &self.release_engine, prepare_ctx).await?;

        // Stage 2: commit/push/PR.
        let submit_ctx = args.ctx.to_submit_context();
        let submit = handle_submit(
            &self.git,
            &self.pr_client,
            SubmitArgs {
                ctx: submit_ctx,
                branch_name_override: Some(prepare.branch_name.clone()),
            },
        )
        .await?;

        let report = ExecuteReport { prepare, submit };
        info!(branch=%report.submit.branch_name, pushed=report.submit.pushed, "execute completed");
        Ok(report)
    }

    /// Runs registry/GitHub publishing only, using release-plz publish semantics.
    pub async fn publish(&self, ctx: PublishContext) -> Result<ReleaseReport> {
        let report = handle_publish(&self.release_engine, ctx).await?;
        info!(published = report.published, "publish completed");
        Ok(report)
    }
}
