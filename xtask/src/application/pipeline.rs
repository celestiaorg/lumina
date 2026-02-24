use std::path::PathBuf;

use anyhow::Result;
use tracing::info;

use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::application::publish::handle_publish;
use crate::domain::types::{ExecuteContext, ExecuteReport, PublishContext, ReleaseReport};

/// Runs release_pr -> publish using real adapters.
pub struct ReleasePipeline {
    release_engine: ReleasePlzAdapter,
}

impl ReleasePipeline {
    pub fn new(workspace_root: PathBuf) -> Self {
        let release_engine = ReleasePlzAdapter::new(workspace_root);
        Self { release_engine }
    }

    /// Opens or updates a release PR. Does not publish.
    pub async fn execute(&self, ctx: ExecuteContext) -> Result<Option<ExecuteReport>> {
        let report = self
            .release_engine
            .release_pr(ctx.common.mode, &ctx.common.auth)
            .await?;

        if let Some(ref r) = report {
            info!(
                branch=%r.head_branch,
                pr_url=?r.pr_url,
                packages=r.updated_packages.len(),
                "execute completed"
            );
        } else {
            info!("execute completed: no updates needed");
        }

        Ok(report)
    }

    pub async fn publish(&self, ctx: PublishContext) -> Result<ReleaseReport> {
        let report = handle_publish(&self.release_engine, ctx).await?;
        info!(published = report.published, "publish completed");
        Ok(report)
    }
}
