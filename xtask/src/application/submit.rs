use anyhow::Result;
use tracing::info;

use crate::adapters::github_output::write_github_output;
use crate::application::pipeline_ops::{GitRepo, PrClient};
use crate::application::prepare::make_release_branch_name;
use crate::domain::model::{RELEASE_COMMIT_MESSAGE_FINAL, RELEASE_COMMIT_MESSAGE_RC};
use crate::domain::types::{ReleaseMode, SubmitContext, SubmitReport};

#[derive(Debug, Clone)]
pub struct SubmitArgs {
    /// Command-specific submit context.
    pub ctx: SubmitContext,
    /// Optional branch resolved during prepare; used to keep execute deterministic.
    pub branch_name_override: Option<String>,
}

/// Commits generated release artifacts, pushes branch, and ensures an open release PR exists.
pub async fn handle_submit(
    git: &impl GitRepo,
    pr_client: &impl PrClient,
    args: SubmitArgs,
) -> Result<SubmitReport> {
    // Use prepared branch when provided by execute, otherwise generate canonical name.
    let branch_name = args
        .branch_name_override
        .clone()
        .unwrap_or_else(|| make_release_branch_name(args.ctx.common.mode));
    info!(
        mode=?args.ctx.common.mode,
        branch=%branch_name,
        branch_override=args.branch_name_override.is_some(),
        "submit: resolved target branch and inputs"
    );

    // Use stable commit message pattern so generated release commits are recognizable.
    let commit_message = match args.ctx.common.mode {
        ReleaseMode::Rc => RELEASE_COMMIT_MESSAGE_RC,
        ReleaseMode::Final => RELEASE_COMMIT_MESSAGE_FINAL,
    }
    .to_string();

    // Submit always follows recreate-branch flow and force-pushes release updates.
    git.stage_all_and_commit(&commit_message, false)?;
    git.push_branch(&branch_name, true, false)?;

    // Close stale release PRs (across rc/final) before opening/ensuring current one.
    let closed_stale_prs = pr_client
        .close_stale_open_release_prs(
            &args.ctx.common.auth,
            args.ctx.branch.skip_pr,
            Some(&branch_name),
        )
        .await?;
    if !closed_stale_prs.is_empty() {
        info!(
            branch=%branch_name,
            closed_stale_prs=closed_stale_prs.len(),
            "submit: closed stale release PRs before ensuring current PR"
        );
    }

    let pr_url = pr_client
        .ensure_release_pr(
            args.ctx.common.mode,
            &args.ctx.common.default_branch,
            &args.ctx.common.auth,
            args.ctx.branch.skip_pr,
            &branch_name,
        )
        .await?
        .map(|pr| pr.url);

    // Expose branch/PR data for GitHub Actions follow-up steps.
    write_github_output("release_branch", &branch_name)?;
    if let Some(url) = &pr_url {
        write_github_output("release_pr_url", url)?;
    }
    info!(branch=%branch_name, pushed=true, pr_url=?pr_url, "submit: completed");

    Ok(SubmitReport {
        mode: args.ctx.common.mode,
        branch_name,
        commit_message,
        pushed: true,
        pr_url,
    })
}

#[cfg(test)]
#[path = "submit_tests.rs"]
mod tests;
