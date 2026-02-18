use anyhow::Result;
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};
use tracing::{debug, info};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::domain::types::{
    BranchState, CheckReport, PrepareContext, PrepareReport, ReleaseMode, UpdateStrategy,
};

/// Prepares release artifacts and branch state for RC/final flow.
/// This does not create commits or push; submit handles that stage.
pub async fn handle_prepare(
    git: &Git2Repo,
    pr_client: &GitHubPrClient,
    release_engine: &ReleasePlzAdapter,
    ctx: PrepareContext,
    check: CheckReport,
) -> Result<PrepareReport> {
    // Release flows always use canonical generated branch names.
    let mut branch_name = make_release_branch_name(ctx.common.mode);
    info!(
        mode=?ctx.common.mode,
        default_branch=%ctx.common.default_branch,
        branch=%branch_name,
        current_commit=%check.current_commit,
        previous_commit=%check.previous_commit,
        latest_release_tag=?check.latest_release_tag,
        latest_non_rc_release_tag=?check.latest_non_rc_release_tag,
        "prepare: resolved initial release branch and baseline"
    );

    // Pick branch update strategy based on branch existence and PR contributor safety check.
    let branch_state = git.branch_state(&branch_name)?;
    debug!(branch=%branch_name, state=?branch_state, "prepare: branch state detected");
    let update_strategy = match branch_state {
        // No branch yet: create from default branch and continue.
        BranchState::Missing => UpdateStrategy::RecreateBranch,
        _ => {
            // Existing branch: force-push only if PR has no external contributors.
            let has_external_contributors = pr_client
                .has_external_contributors_on_open_release_pr(&ctx.common.auth, &branch_name)
                .await?;
            if has_external_contributors {
                UpdateStrategy::ClosePrAndRecreate
            } else {
                UpdateStrategy::InPlaceForcePush
            }
        }
    };
    info!(branch=%branch_name, strategy=?update_strategy, "prepare: selected update strategy");

    // Apply chosen branch strategy and collect step descriptions.
    let mut descriptions = match update_strategy {
        UpdateStrategy::RecreateBranch => {
            git.create_release_branch_from_default(&branch_name, &ctx.common.default_branch)?
        }
        UpdateStrategy::InPlaceForcePush => {
            // Keep same branch and refresh it release-plz style.
            git.refresh_existing_release_branch(&branch_name, &ctx.common.default_branch)?
        }
        UpdateStrategy::ClosePrAndRecreate => {
            // Contributor-safe flow: mint a fresh branch and continue there.
            // PR close/recreate is deferred to submit stage.
            let recreated_branch = make_release_branch_name(ctx.common.mode);
            git.create_release_branch_from_default(&recreated_branch, &ctx.common.default_branch)?;
            branch_name = recreated_branch.clone();
            info!(branch=%branch_name, "prepare: recreated branch for contributor-safe flow");
            vec![
                format!("recreated release branch `{recreated_branch}`"),
                "deferred PR close/recreate to submit stage".to_string(),
            ]
        }
    };

    // Regenerate versions/changelog using release-plz-backed adapter logic.
    descriptions.extend(
        release_engine
            .regenerate_artifacts(
                ctx.common.mode,
                &ctx.common.default_branch,
                &check.previous_commit,
                check.latest_release_tag.as_deref(),
                check.latest_non_rc_release_tag.as_deref(),
            )
            .await?,
    );
    info!(
        branch=%branch_name,
        description_items=descriptions.len(),
        "prepare: regenerated release artifacts"
    );

    Ok(PrepareReport {
        mode: ctx.common.mode,
        branch_name,
        branch_state,
        update_strategy,
        current_commit: check.current_commit,
        version_state: check.version_state,
        description: descriptions,
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
    let branch = format!("lumina/release-plz-{timestamp}-{suffix}");
    debug!(mode=?mode, branch=%branch, "generated release branch name");
    branch
}
