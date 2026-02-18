use anyhow::Result;
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::domain::types::{
    BranchState, CheckReport, ExecutionStage, PrepareReport, ReleaseContext, ReleaseMode,
    UpdateStrategy,
};

/// Prepares release artifacts and branch state for RC/final flow.
/// This does not create commits or push; submit handles that stage.
pub async fn handle_prepare(
    git: &Git2Repo,
    pr_client: &GitHubPrClient,
    release_engine: &ReleasePlzAdapter,
    ctx: ReleaseContext,
    check: CheckReport,
) -> Result<PrepareReport> {
    // Resolve or generate the release branch name for this run.
    let mut branch_name = ctx
        .branch_name
        .clone()
        .unwrap_or_else(|| make_release_branch_name(ctx.mode));
    // If branch name was user-specified, enforce prefix policy immediately.
    if ctx.branch_name.is_some() {
        git.validate_branch_name(ctx.mode, &branch_name, &ctx.rc_branch_prefix)?;
    }

    // Pick branch update strategy based on branch existence and PR contributor safety check.
    let branch_state = git.branch_state(&branch_name)?;
    let update_strategy = match branch_state {
        // No branch yet: create from default branch and continue.
        BranchState::Missing => UpdateStrategy::RecreateBranch,
        _ => {
            // Existing branch: force-push only if PR has no external contributors.
            let has_external_contributors = pr_client
                .has_external_contributors_on_open_release_pr(&ctx, &branch_name)
                .await?;
            if has_external_contributors {
                UpdateStrategy::ClosePrAndRecreate
            } else {
                UpdateStrategy::InPlaceForcePush
            }
        }
    };

    // Apply chosen branch strategy and collect actions performed.
    let mut actions = match update_strategy {
        UpdateStrategy::RecreateBranch => {
            git.create_release_branch_from_default(&branch_name, &ctx.default_branch)?
        }
        UpdateStrategy::InPlaceForcePush => {
            // Keep same branch and refresh it release-plz style.
            git.refresh_existing_release_branch(&branch_name, &ctx.default_branch)?
        }
        UpdateStrategy::ClosePrAndRecreate => {
            // Contributor-safe flow: close old PR, mint a fresh branch, and continue there.
            let _ = pr_client.close_open_release_pr(&ctx, &branch_name).await?;
            let recreated_branch = make_release_branch_name(ctx.mode);
            git.create_release_branch_from_default(&recreated_branch, &ctx.default_branch)?;
            branch_name = recreated_branch.clone();
            vec![
                "closed release PR with external contributors".to_string(),
                format!("recreated release branch `{recreated_branch}`"),
            ]
        }
    };

    // Regenerate versions/changelog using release-plz-backed adapter logic.
    actions.extend(release_engine.regenerate_artifacts(&ctx).await?);

    Ok(PrepareReport {
        mode: ctx.mode,
        branch_name,
        branch_state,
        update_strategy,
        current_commit: check.current_commit,
        current_versions: check.current_versions,
        versions: check.versions,
        actions,
        stage: ExecutionStage::Prepared,
    })
}

/// Builds canonical release branch name with timestamp and mode suffix (`-rc`/`-final`).
pub(crate) fn make_release_branch_name(mode: ReleaseMode) -> String {
    static FORMAT: &[FormatItem<'static>] =
        format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]Z");
    let timestamp = OffsetDateTime::now_utc()
        .format(FORMAT)
        .unwrap_or_else(|_| "1970-01-01T00-00-00Z".to_string());
    let suffix = match mode {
        ReleaseMode::Rc => "rc",
        ReleaseMode::Final => "final",
    };
    format!("lumina/release-plz-{timestamp}-{suffix}")
}
