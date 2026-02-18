use anyhow::Result;

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_output::write_github_output;
use crate::adapters::github_pr::GitHubPrClient;
use crate::application::prepare::make_release_branch_name;
use crate::domain::types::{ReleaseMode, SubmitContext, SubmitReport, UpdateStrategy};

#[derive(Debug, Clone)]
pub struct SubmitArgs {
    /// Command-specific submit context.
    pub ctx: SubmitContext,
    /// When true, computes/reports steps but skips mutating git remotes.
    pub dry_run: bool,
    /// Optional branch resolved during prepare; used to keep execute deterministic.
    pub branch_name_override: Option<String>,
    /// Optional strategy chosen by prepare; used by execute to avoid recomputing policy.
    pub update_strategy_override: Option<UpdateStrategy>,
}

/// Commits generated release artifacts, pushes branch, and ensures an open release PR exists.
pub async fn handle_submit(
    git: &Git2Repo,
    pr_client: &GitHubPrClient,
    args: SubmitArgs,
) -> Result<SubmitReport> {
    // Use prepared branch when provided by execute, otherwise generate canonical name.
    let mut branch_name = args
        .branch_name_override
        .clone()
        .unwrap_or_else(|| make_release_branch_name(args.ctx.common.mode));
    // Decide submit policy. Execute can pass through prepare's strategy; standalone submit recomputes it.
    let strategy = if let Some(strategy) = args.update_strategy_override {
        strategy
    } else {
        let external_contributors = pr_client
            .has_external_contributors_on_open_release_pr(&args.ctx.common.auth, &branch_name)
            .await?;
        if external_contributors {
            UpdateStrategy::ClosePrAndRecreate
        } else {
            UpdateStrategy::InPlaceForcePush
        }
    };

    // Use stable commit message pattern so generated release commits are recognizable.
    let commit_message = match args.ctx.common.mode {
        ReleaseMode::Rc => "chore(release): prepare rc release",
        ReleaseMode::Final => "chore(release): prepare final release",
    }
    .to_string();

    // Apply submit strategy: close/recreate PR flow or in-place force-push flow.
    let pr_url = match strategy {
        UpdateStrategy::ClosePrAndRecreate => {
            // Close old PR first so recreated branch can open a clean PR thread.
            let _ = pr_client
                .close_open_release_pr(&args.ctx.common.auth, args.ctx.branch.skip_pr, &branch_name)
                .await?;
            git.stage_all_and_commit(&commit_message, args.dry_run)?;
            // Fork current HEAD into a fresh release branch and push without force.
            let recreated_branch = make_release_branch_name(args.ctx.common.mode);
            git.create_branch_from_current(&recreated_branch, args.dry_run)?;
            git.push_branch(&recreated_branch, false, args.dry_run)?;
            branch_name = recreated_branch;
            pr_client
                .ensure_release_pr(
                    args.ctx.common.mode,
                    &args.ctx.common.default_branch,
                    &args.ctx.common.auth,
                    args.ctx.branch.skip_pr,
                    &branch_name,
                )
                .await?
                .map(|pr| pr.url)
        }
        _ => {
            // Normal flow: commit current branch changes and force-push update.
            git.stage_all_and_commit(&commit_message, args.dry_run)?;
            git.push_branch(&branch_name, true, args.dry_run)?;
            pr_client
                .ensure_release_pr(
                    args.ctx.common.mode,
                    &args.ctx.common.default_branch,
                    &args.ctx.common.auth,
                    args.ctx.branch.skip_pr,
                    &branch_name,
                )
                .await?
                .map(|pr| pr.url)
        }
    };

    // Expose branch/PR data for GitHub Actions follow-up steps.
    write_github_output("release_branch", &branch_name)?;
    if let Some(url) = &pr_url {
        write_github_output("release_pr_url", url)?;
    }

    Ok(SubmitReport {
        mode: args.ctx.common.mode,
        branch_name,
        update_strategy: strategy,
        commit_message,
        pushed: !args.dry_run,
        pr_url,
    })
}
