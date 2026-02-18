use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, bail};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::application::prepare::handle_prepare;
use crate::application::publish::handle_publish;
use crate::application::simulation::run_strict_simulation;
use crate::application::submit::{SubmitArgs, handle_submit};
use crate::domain::ports::{GitRepo, PrClient, Publisher, ReleaseEngine};
use crate::domain::types::{
    CheckReport, ExecuteReport, ExecutionStage, PrepareReport, ReleaseContext, ReleaseReport,
};
use crate::domain::validation::collect_validation_issues;

#[derive(Debug, Clone)]
pub struct ExecuteArgs {
    pub ctx: ReleaseContext,
    pub dry_run: bool,
}

#[derive(Clone)]
pub struct ReleasePipeline {
    release_engine: Arc<dyn ReleaseEngine>,
    publisher: Arc<dyn Publisher>,
    git: Arc<dyn GitRepo>,
    pr_client: Arc<dyn PrClient>,
}

impl ReleasePipeline {
    pub fn new(workspace_root: PathBuf) -> Self {
        let release_plz = Arc::new(ReleasePlzAdapter::new(workspace_root.clone()));
        let release_engine: Arc<dyn ReleaseEngine> = release_plz.clone();
        let publisher: Arc<dyn Publisher> = release_plz;

        let git: Arc<dyn GitRepo> = Arc::new(Git2Repo::new(workspace_root.clone()));
        let pr_client: Arc<dyn PrClient> = Arc::new(GitHubPrClient::new(workspace_root));

        Self {
            release_engine,
            publisher,
            git,
            pr_client,
        }
    }

    pub async fn check(&self, ctx: ReleaseContext) -> Result<CheckReport> {
        let computation = self.release_engine.plan(&ctx).await?;
        let strict_simulation = run_strict_simulation(
            self.release_engine.workspace_root(),
            &ctx.default_branch,
            &computation.comparison_commit,
            &computation.plans,
        )?;
        let validation_issues = collect_validation_issues(
            ctx.mode,
            &computation.plans,
            &strict_simulation.duplicate_publishable_versions,
        );

        Ok(CheckReport {
            mode: ctx.mode,
            baseline_commit: computation.baseline_commit,
            comparison_commit: computation.comparison_commit,
            comparison_versions: computation.comparison_versions,
            versions: computation.plans.iter().map(|plan| plan.as_view()).collect(),
            validation_issues,
        })
    }

    pub async fn prepare(&self, mut ctx: ReleaseContext) -> Result<PrepareReport> {
        // Prepare must always validate against tip of default branch.
        ctx.base_commit = None;
        let check = self.check(ctx.clone()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }

        handle_prepare(
            &*self.git,
            &*self.pr_client,
            &*self.release_engine,
            ctx,
            check,
        )
        .await
    }

    pub async fn submit(&self, args: SubmitArgs) -> Result<crate::domain::types::SubmitReport> {
        handle_submit(&*self.git, &*self.pr_client, args).await
    }

    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        let mut ctx = args.ctx;
        ctx.base_commit = None;

        let check = self.check(ctx.clone()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }

        let prepare = handle_prepare(
            &*self.git,
            &*self.pr_client,
            &*self.release_engine,
            ctx.clone(),
            check.clone(),
        )
        .await?;

        let submit = handle_submit(
            &*self.git,
            &*self.pr_client,
            SubmitArgs {
                ctx,
                dry_run: args.dry_run,
                branch_name_override: Some(prepare.branch_name.clone()),
            },
        )
        .await?;

        Ok(ExecuteReport {
            check,
            prepare,
            submit,
            stage: ExecutionStage::Executed,
        })
    }

    pub async fn publish(&self, ctx: ReleaseContext) -> Result<ReleaseReport> {
        handle_publish(&*self.publisher, ctx).await
    }
}
