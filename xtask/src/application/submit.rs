use anyhow::Result;
use tracing::info;

use crate::application::pipeline_ops::{GitRepo, PrClient};
use crate::application::prepare::make_release_branch_name;
use crate::domain::model::{RELEASE_COMMIT_MESSAGE_FINAL, RELEASE_COMMIT_MESSAGE_RC};
use crate::domain::types::{ReleaseMode, SubmitContext, SubmitReport};

#[derive(Debug, Clone)]
pub struct SubmitArgs {
    pub ctx: SubmitContext,
    /// Overrides generated branch name when set (e.g. from a preceding prepare step).
    pub branch_name_override: Option<String>,
}

/// Commits release artifacts, force-pushes the branch, and opens/reuses a release PR.
pub async fn handle_submit(
    git: &impl GitRepo,
    pr_client: &impl PrClient,
    args: SubmitArgs,
) -> Result<SubmitReport> {
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

    let commit_message = match args.ctx.common.mode {
        ReleaseMode::Rc => RELEASE_COMMIT_MESSAGE_RC,
        ReleaseMode::Final => RELEASE_COMMIT_MESSAGE_FINAL,
    }
    .to_string();

    git.stage_all_and_commit(&commit_message, false)?;
    git.push_branch(&branch_name, true, false)?;

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
