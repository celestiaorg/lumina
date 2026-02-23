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
    pub ctx: ExecuteContext,
}

/// Runs prepare -> submit -> publish using real adapters.
pub struct ReleasePipeline {
    release_engine: ReleasePlzAdapter,
    git: Git2Repo,
    pr_client: GitHubPrClient,
}

impl ReleasePipeline {
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

    /// Prepare (branch + update) then submit (commit/push/PR). Does not publish.
    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        let prepare_ctx = args.ctx.to_prepare_context();
        let prepare = handle_prepare(&self.git, &self.release_engine, prepare_ctx).await?;

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

    pub async fn publish(&self, ctx: PublishContext) -> Result<ReleaseReport> {
        let report = handle_publish(&self.release_engine, ctx).await?;
        info!(published = report.published, "publish completed");
        Ok(report)
    }
}
