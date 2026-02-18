use std::path::PathBuf;

use anyhow::{Result, bail};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::application::prepare::handle_prepare;
use crate::application::publish::handle_publish;
use crate::application::simulation::run_simulation;
use crate::application::submit::{SubmitArgs, handle_submit};
use crate::domain::types::{
    CheckReport, ExecuteReport, ExecutionStage, PrepareReport, ReleaseContext, ReleaseReport,
};
use crate::domain::validation::collect_validation_issues;

#[derive(Debug, Clone)]
pub struct ExecuteArgs {
    /// Release context shared across check/prepare/submit stages.
    pub ctx: ReleaseContext,
    /// When true, submit stage skips creating commits/pushes while still reporting intended actions.
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

    /// Runs release version computation + strict duplicate simulation and returns a validation report.
    pub async fn check(&self, ctx: ReleaseContext) -> Result<CheckReport> {
        // Compute mode-specific versions and reporting metadata from release-plz.
        let computation = self.release_engine.versions(&ctx).await?;
        // Simulate resulting workspace versions in isolation and detect duplicate publishable versions.
        let simulation = run_simulation(
            self.release_engine.workspace_root(),
            &ctx.default_branch,
            &computation.current_commit,
            &computation.versions,
        )?;
        // Convert simulation findings + RC transition checks into user-facing validation issues.
        let validation_issues = collect_validation_issues(
            ctx.mode,
            &computation.versions,
            &simulation.duplicate_publishable_versions,
        );

        Ok(CheckReport {
            mode: ctx.mode,
            previous_commit: computation.previous_commit,
            current_commit: computation.current_commit,
            current_versions: computation.current_versions,
            versions: computation
                .versions
                .iter()
                .map(|version| version.as_view())
                .collect(),
            validation_issues,
        })
    }

    /// Executes `check` against default-branch tip and regenerates branch artifacts when valid.
    pub async fn prepare(&self, mut ctx: ReleaseContext) -> Result<PrepareReport> {
        // Prepare must always validate against tip of default branch.
        ctx.current_commit = None;
        // Stop early if check reports any validation issues.
        let check = self.check(ctx.clone()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }

        handle_prepare(
            &self.git,
            &self.pr_client,
            &self.release_engine,
            ctx,
            check,
        )
        .await
    }

    /// Commits/pushes prepared changes and ensures PR behavior according to contributor safety rules.
    pub async fn submit(&self, args: SubmitArgs) -> Result<crate::domain::types::SubmitReport> {
        handle_submit(&self.git, &self.pr_client, args).await
    }

    /// Full non-publishing flow: check -> prepare -> submit.
    /// Publishing is intentionally kept in a separate command.
    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        // Execute is always based on latest default-branch tip, never a custom base commit.
        let mut ctx = args.ctx;
        ctx.current_commit = None;

        // Stage 1: validation/check.
        let check = self.check(ctx.clone()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }

        // Stage 2: branch/artifact preparation.
        let prepare = handle_prepare(
            &self.git,
            &self.pr_client,
            &self.release_engine,
            ctx.clone(),
            check.clone(),
        )
        .await?;

        // Stage 3: commit/push/PR update.
        let submit = handle_submit(
            &self.git,
            &self.pr_client,
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

    /// Runs registry/GitHub publishing only, using release-plz publish semantics.
    pub async fn publish(&self, ctx: ReleaseContext) -> Result<ReleaseReport> {
        handle_publish(&self.publisher, ctx).await
    }
}
