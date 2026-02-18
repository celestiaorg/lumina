use std::path::PathBuf;

use anyhow::{Result, bail};
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};

use crate::app::checks::run_strict_simulation;
use crate::domain::types::{
    CheckReport, ExecuteReport, ExecutionStage, PrepareReport, ReleaseContext, ReleaseReport,
    SubmitReport, UpdateStrategy,
};
use crate::domain::validation::collect_validation_issues;
use crate::engine::release_plz::ReleasePlzEngine;
use crate::infra::git2_ops::GitRepoOps;
use crate::infra::github::GitHubCliOps;
use crate::output::write_github_output;

#[derive(Debug, Clone)]
pub struct SubmitArgs {
    pub ctx: ReleaseContext,
    pub dry_run: bool,
    pub branch_name_override: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExecuteArgs {
    pub ctx: ReleaseContext,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct Orchestrator {
    release_engine: ReleasePlzEngine,
    git: GitRepoOps,
    github: GitHubCliOps,
}

impl Orchestrator {
    pub fn new(workspace_root: PathBuf) -> Result<Self> {
        Ok(Self {
            release_engine: ReleasePlzEngine::new(workspace_root.clone()),
            git: GitRepoOps::new(workspace_root.clone()),
            github: GitHubCliOps::new(workspace_root),
        })
    }

    pub async fn check(&self, ctx: ReleaseContext) -> Result<CheckReport> {
        let computation = self.release_engine.plan(&ctx).await?;
        let strict_simulation = run_strict_simulation(
            self.release_engine.workspace_root(),
            &self.release_engine,
            &ctx,
        )
        .await?;
        let validation_issues = collect_validation_issues(
            ctx.mode,
            &computation.plans,
            &strict_simulation.duplicate_publishable_versions,
        );

        Ok(CheckReport {
            mode: ctx.mode,
            baseline_policy: computation.baseline_policy,
            default_branch: ctx.default_branch,
            base_commit: ctx.base_commit,
            comparison_commit: computation.comparison_commit,
            comparison_versions: computation.comparison_versions,
            plans: computation
                .plans
                .iter()
                .map(|plan| plan.as_view())
                .collect(),
            strict_simulation_applied: true,
            validation_issues,
            stage: ExecutionStage::Checked,
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
        self.prepare_from_check(ctx, check).await
    }

    async fn prepare_from_check(
        &self,
        ctx: ReleaseContext,
        check: CheckReport,
    ) -> Result<PrepareReport> {
        let mut branch_name = ctx
            .branch_name
            .clone()
            .unwrap_or_else(|| make_release_branch_name(ctx.mode));
        if ctx.branch_name.is_some() {
            self.git
                .validate_branch_name(ctx.mode, &branch_name, &ctx.rc_branch_prefix)?;
        }

        let branch_state = self.git.branch_state(&branch_name)?;
        let update_strategy = match branch_state {
            crate::domain::types::BranchState::Missing => UpdateStrategy::RecreateBranch,
            _ => {
                let has_external_contributors = self
                    .github
                    .has_external_contributors_on_open_release_pr(&ctx, &branch_name)
                    .await?;
                if has_external_contributors {
                    UpdateStrategy::ClosePrAndRecreate
                } else {
                    UpdateStrategy::InPlaceForcePush
                }
            }
        };

        let mut actions = match update_strategy {
            UpdateStrategy::RecreateBranch => self
                .git
                .create_release_branch_from_default(&branch_name, &ctx.default_branch)?,
            UpdateStrategy::InPlaceForcePush => self
                .git
                .refresh_existing_release_branch(&branch_name, &ctx.default_branch)?,
            UpdateStrategy::ClosePrAndRecreate => {
                let _ = self
                    .github
                    .close_open_release_pr(&ctx, &branch_name)
                    .await?;
                let recreated_branch = make_release_branch_name(ctx.mode);
                self.git
                    .create_release_branch_from_default(&recreated_branch, &ctx.default_branch)?;
                branch_name = recreated_branch.clone();
                vec![
                    "closed release PR with external contributors".to_string(),
                    format!("recreated release branch `{recreated_branch}`"),
                ]
            }
        };

        actions.extend(
            self.release_engine
                .regenerate_artifacts(&ctx)
                .await?,
        );

        Ok(PrepareReport {
            mode: ctx.mode,
            branch_name,
            branch_state,
            update_strategy,
            comparison_commit: check.comparison_commit,
            comparison_versions: check.comparison_versions,
            plans: check.plans,
            actions,
            stage: ExecutionStage::Prepared,
        })
    }

    pub async fn submit(&self, args: SubmitArgs) -> Result<SubmitReport> {
        let mut branch_name = args
            .branch_name_override
            .clone()
            .or_else(|| args.ctx.branch_name.clone())
            .unwrap_or_else(|| make_release_branch_name(args.ctx.mode));
        let external_contributors = self
            .github
            .has_external_contributors_on_open_release_pr(&args.ctx, &branch_name)
            .await?;

        let strategy = if external_contributors {
            UpdateStrategy::ClosePrAndRecreate
        } else {
            UpdateStrategy::InPlaceForcePush
        };

        let commit_message = match args.ctx.mode {
            crate::domain::types::ReleaseMode::Rc => "chore(release): prepare rc release",
            crate::domain::types::ReleaseMode::Final => "chore(release): prepare final release",
        }
        .to_string();

        let pr_url = match strategy {
            UpdateStrategy::ClosePrAndRecreate => {
                let _ = self
                    .github
                    .close_open_release_pr(&args.ctx, &branch_name)
                    .await?;
                self.git
                    .stage_all_and_commit(&commit_message, args.dry_run)?;
                let recreated_branch = make_release_branch_name(args.ctx.mode);
                self.git
                    .create_branch_from_current(&recreated_branch, args.dry_run)?;
                self.git
                    .push_branch(&recreated_branch, false, args.dry_run)?;
                branch_name = recreated_branch;
                self.github
                    .ensure_release_pr(&args.ctx, &branch_name)
                    .await?
                    .map(|pr| pr.url)
            }
            _ => {
                self.git
                    .stage_all_and_commit(&commit_message, args.dry_run)?;
                self.git.push_branch(&branch_name, true, args.dry_run)?;
                self.github
                    .ensure_release_pr(&args.ctx, &branch_name)
                    .await?
                    .map(|pr| pr.url)
            }
        };

        write_github_output("release_branch", &branch_name)?;
        if let Some(url) = &pr_url {
            write_github_output("release_pr_url", url)?;
        }

        Ok(SubmitReport {
            mode: args.ctx.mode,
            branch_name,
            update_strategy: strategy,
            commit_message,
            pushed: !args.dry_run,
            pr_url,
            stage: ExecutionStage::Submitted,
        })
    }

    pub async fn execute(&self, args: ExecuteArgs) -> Result<ExecuteReport> {
        let (check, prepare, submit) = self
            .run_pipeline(args.ctx.clone(), args.dry_run)
            .await?;

        Ok(ExecuteReport {
            check,
            prepare,
            submit,
            stage: ExecutionStage::Executed,
        })
    }

    async fn run_pipeline(
        &self,
        ctx: ReleaseContext,
        dry_run: bool,
    ) -> Result<(CheckReport, PrepareReport, SubmitReport)> {
        let check = self.check(ctx.clone()).await?;
        if !check.validation_issues.is_empty() {
            bail!(
                "release-check failed with {} validation issue(s)",
                check.validation_issues.len()
            );
        }
        let prepare = self.prepare_from_check(ctx.clone(), check.clone()).await?;
        let submit = self
            .submit(SubmitArgs {
                ctx,
                dry_run,
                branch_name_override: Some(prepare.branch_name.clone()),
            })
            .await?;
        Ok((check, prepare, submit))
    }

    pub async fn publish(&self, ctx: ReleaseContext) -> Result<ReleaseReport> {
        let publish_payload = self.release_engine.publish(&ctx).await?;
        let published = !publish_payload.is_null()
            && !publish_payload
                .as_array()
                .is_some_and(|releases| releases.is_empty());
        Ok(ReleaseReport {
            mode: ctx.mode,
            published,
            payload: publish_payload,
            stage: ExecutionStage::Released,
        })
    }
}

fn make_release_branch_name(mode: crate::domain::types::ReleaseMode) -> String {
    static FORMAT: &[FormatItem<'static>] =
        format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]Z");
    let timestamp = OffsetDateTime::now_utc()
        .format(FORMAT)
        .unwrap_or_else(|_| "1970-01-01T00-00-00Z".to_string());
    let suffix = match mode {
        crate::domain::types::ReleaseMode::Rc => "rc",
        crate::domain::types::ReleaseMode::Final => "final",
    };
    format!("lumina/release-plz-{timestamp}-{suffix}")
}
