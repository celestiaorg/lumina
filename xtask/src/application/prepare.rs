use anyhow::{Result, bail};
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};
use tracing::info;

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::domain::model::RELEASE_PR_BRANCH_PREFIX;
use crate::domain::types::{
    BranchState, PrepareContext, PrepareReport, ReleaseMode, VersionsReport,
};

/// Prepares release artifacts and branch state for RC/final flow.
/// This does not create commits or push; submit handles that stage.
pub async fn handle_prepare(
    git: &Git2Repo,
    release_engine: &ReleasePlzAdapter,
    ctx: PrepareContext,
    versions: &VersionsReport,
) -> Result<PrepareReport> {
    // Release flows always use canonical generated branch names.
    let branch_name = make_release_branch_name(ctx.common.mode);
    info!(
        mode=?ctx.common.mode,
        default_branch=%ctx.common.default_branch,
        branch=%branch_name,
        current_commit=%versions.current_commit,
        previous_commit=%versions.previous_commit,
        latest_release_tag=?versions.latest_release_tag,
        latest_non_rc_release_tag=?versions.latest_non_rc_release_tag,
        "prepare: resolved initial release branch, previous commit, and current commit"
    );

    // Only allow creating a fresh release branch. Existing release branches are treated as errors.
    let branch_state = git.branch_state(&branch_name)?;
    if !matches!(branch_state, BranchState::Missing) {
        bail!(
            "prepare: release branch `{branch_name}` already exists with state `{branch_state:?}`; expected missing branch"
        );
    }

    let mut descriptions =
        git.create_release_branch_from_default(&branch_name, &ctx.common.default_branch)?;

    // Regenerate versions/changelog using release-plz-backed adapter logic.
    descriptions.extend(
        release_engine
            .regenerate_artifacts(
                ctx.common.mode,
                &ctx.common.default_branch,
                &versions.previous_commit,
                versions.latest_release_tag.as_deref(),
                versions.latest_non_rc_release_tag.as_deref(),
                &versions.planned_versions,
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
    let branch = format!("{RELEASE_PR_BRANCH_PREFIX}-{timestamp}-{suffix}");
    branch
}
