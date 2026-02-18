use anyhow::Result;
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};

use crate::domain::ports::{GitRepo, PrClient, ReleaseEngine};
use crate::domain::types::{
    BranchState, CheckReport, ExecutionStage, PrepareReport, ReleaseContext, ReleaseMode,
    UpdateStrategy,
};

pub async fn handle_prepare(
    git: &dyn GitRepo,
    pr_client: &dyn PrClient,
    release_engine: &dyn ReleaseEngine,
    ctx: ReleaseContext,
    check: CheckReport,
) -> Result<PrepareReport> {
    let mut branch_name = ctx
        .branch_name
        .clone()
        .unwrap_or_else(|| make_release_branch_name(ctx.mode));
    if ctx.branch_name.is_some() {
        git.validate_branch_name(ctx.mode, &branch_name, &ctx.rc_branch_prefix)?;
    }

    let branch_state = git.branch_state(&branch_name)?;
    let update_strategy = match branch_state {
        BranchState::Missing => UpdateStrategy::RecreateBranch,
        _ => {
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

    let mut actions = match update_strategy {
        UpdateStrategy::RecreateBranch => {
            git.create_release_branch_from_default(&branch_name, &ctx.default_branch)?
        }
        UpdateStrategy::InPlaceForcePush => {
            git.refresh_existing_release_branch(&branch_name, &ctx.default_branch)?
        }
        UpdateStrategy::ClosePrAndRecreate => {
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

    actions.extend(release_engine.regenerate_artifacts(&ctx).await?);

    Ok(PrepareReport {
        mode: ctx.mode,
        branch_name,
        branch_state,
        update_strategy,
        comparison_commit: check.comparison_commit,
        comparison_versions: check.comparison_versions,
        plans: check.versions,
        actions,
        stage: ExecutionStage::Prepared,
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
    format!("lumina/release-plz-{timestamp}-{suffix}")
}
