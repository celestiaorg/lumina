use std::path::PathBuf;

use anyhow::{Result, bail};
use tracing::{debug, info};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::application::prepare::handle_prepare;
use crate::application::publish::handle_publish;
use crate::application::submit::{SubmitArgs, handle_submit};
use crate::domain::types::{
    CheckContext, CheckReport, ExecuteContext, ExecuteReport, PrepareContext, PrepareReport,
    PublishContext, ReleaseReport, VersionStateReport,
};
use crate::domain::validation::collect_validation_issues;

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
        debug!(workspace_root=%workspace_root.display(), "initializing release pipeline");
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
    pub async fn check(&self, ctx: CheckContext) -> Result<CheckReport> {
        let mode = ctx.common.mode;
        info!(
            mode=?mode,
            default_branch=%ctx.common.default_branch,
            requested_current_commit=?ctx.current_commit,
            "starting release check"
        );
        // Compute mode-specific versions and reporting metadata from release-plz.
        let computation = self.release_engine.versions(&ctx).await?;
        // Convert simulation findings + RC transition checks into user-facing validation issues.
        let validation_issues = collect_validation_issues(
            mode,
            &computation.versions,
            &computation.duplicate_publishable_versions,
        );

        let report = CheckReport {
            mode,
            previous_commit: computation.previous_commit,
            latest_release_tag: computation.latest_release_tag,
            latest_non_rc_release_tag: computation.latest_non_rc_release_tag,
            current_commit: computation.current_commit,
            version_state: VersionStateReport {
                current: computation.current_versions,
                planned: computation
                    .versions
                    .iter()
                    .map(|version| version.as_view())
                    .collect(),
            },
            validation_issues,
        };
        info!(
            mode=?report.mode,
            current_commit=%report.current_commit,
            previous_commit=%report.previous_commit,
            latest_release_tag=?report.latest_release_tag,
            latest_non_rc_release_tag=?report.latest_non_rc_release_tag,
            planned_versions=report.version_state.planned.len(),
            validation_issues=report.validation_issues.len(),
            "release check completed"
        );
        Ok(report)
    }

    /// Executes `check` against default-branch tip and regenerates branch artifacts when valid.
    pub async fn prepare(&self, ctx: PrepareContext) -> Result<PrepareReport> {
        info!(mode=?ctx.common.mode, default_branch=%ctx.common.default_branch, "starting prepare");
        // Stop early if check reports any validation issues.
        let check = self.check(ctx.to_check_context()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }

        let report =
            handle_prepare(&self.git, &self.pr_client, &self.release_engine, ctx, check).await?;
        info!(
            mode=?report.mode,
            branch=%report.branch_name,
            strategy=?report.update_strategy,
            description_items=report.description.len(),
            "prepare completed"
        );
        Ok(report)
    }

    /// Commits/pushes prepared changes and ensures PR behavior according to contributor safety rules.
    pub async fn submit(&self, args: SubmitArgs) -> Result<crate::domain::types::SubmitReport> {
        info!(mode=?args.ctx.common.mode, dry_run=args.dry_run, "starting submit");
        let report = handle_submit(&self.git, &self.pr_client, args).await?;
        info!(
            mode=?report.mode,
            branch=%report.branch_name,
            strategy=?report.update_strategy,
            pushed=report.pushed,
            pr_url=?report.pr_url,
            "submit completed"
        );
        Ok(report)
    }

    /// Full non-publishing flow: check -> prepare -> submit.
    /// Publishing is intentionally kept in a separate command.
    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        info!(
            mode=?args.ctx.common.mode,
            default_branch=%args.ctx.common.default_branch,
            dry_run=args.dry_run,
            "starting execute"
        );
        // Stage 1: validation/check.
        let check = self.check(args.ctx.to_check_context()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }

        // Stage 2: branch/artifact preparation.
        let prepare_ctx = args.ctx.to_prepare_context();
        let prepare = handle_prepare(
            &self.git,
            &self.pr_client,
            &self.release_engine,
            prepare_ctx,
            check.clone(),
        )
        .await?;
        debug!(branch=%prepare.branch_name, strategy=?prepare.update_strategy, "execute prepare stage completed");

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
        debug!(branch=%submit.branch_name, pushed=submit.pushed, pr_url=?submit.pr_url, "execute submit stage completed");

        let report = ExecuteReport {
            check,
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
