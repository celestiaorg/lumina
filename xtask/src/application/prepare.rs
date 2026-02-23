use anyhow::{Result, bail};
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};
use tracing::info;

use crate::application::pipeline_ops::{GitRepo, ReleaseEngine};
use crate::domain::model::RELEASE_PR_BRANCH_PREFIX;
use crate::domain::types::{BranchState, PrepareContext, PrepareReport, ReleaseMode};

/// Creates a release branch, runs release-plz update. Does not commit or push.
pub async fn handle_prepare(
    git: &impl GitRepo,
    release_engine: &impl ReleaseEngine,
    ctx: PrepareContext,
) -> Result<PrepareReport> {
    let branch_name = make_release_branch_name(ctx.mode);
    info!(
        mode=?ctx.mode,
        default_branch=%ctx.default_branch,
        branch=%branch_name,
        "prepare: creating release branch and running update"
    );

    let branch_state = git.branch_state(&branch_name)?;
    if !matches!(branch_state, BranchState::Missing) {
        bail!(
            "prepare: release branch `{branch_name}` already exists with state `{branch_state:?}`; expected missing branch"
        );
    }

    git.create_release_branch_from_default(&branch_name, &ctx.default_branch)?;

    let updated_packages = release_engine.update(ctx.mode).await?;

    info!(
        branch=%branch_name,
        updated_count=updated_packages.len(),
        "prepare: release artifacts generated"
    );

    Ok(PrepareReport {
        mode: ctx.mode,
        branch_name,
        branch_state,
        updated_packages,
    })
}

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
    format!("{RELEASE_PR_BRANCH_PREFIX}-{timestamp}-{suffix}")
}

#[cfg(test)]
#[path = "prepare_tests.rs"]
mod tests;
