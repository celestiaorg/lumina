use anyhow::Result;

use crate::adapters::github_output::write_github_output;
use crate::application::prepare::make_release_branch_name;
use crate::domain::ports::{GitRepo, PrClient};
use crate::domain::types::{ExecutionStage, ReleaseContext, ReleaseMode, SubmitReport, UpdateStrategy};

#[derive(Debug, Clone)]
pub struct SubmitArgs {
    pub ctx: ReleaseContext,
    pub dry_run: bool,
    pub branch_name_override: Option<String>,
}

pub async fn handle_submit(
    git: &dyn GitRepo,
    pr_client: &dyn PrClient,
    args: SubmitArgs,
) -> Result<SubmitReport> {
    let mut branch_name = args
        .branch_name_override
        .clone()
        .or_else(|| args.ctx.branch_name.clone())
        .unwrap_or_else(|| make_release_branch_name(args.ctx.mode));
    let external_contributors = pr_client
        .has_external_contributors_on_open_release_pr(&args.ctx, &branch_name)
        .await?;

    let strategy = if external_contributors {
        UpdateStrategy::ClosePrAndRecreate
    } else {
        UpdateStrategy::InPlaceForcePush
    };

    let commit_message = match args.ctx.mode {
        ReleaseMode::Rc => "chore(release): prepare rc release",
        ReleaseMode::Final => "chore(release): prepare final release",
    }
    .to_string();

    let pr_url = match strategy {
        UpdateStrategy::ClosePrAndRecreate => {
            let _ = pr_client
                .close_open_release_pr(&args.ctx, &branch_name)
                .await?;
            git.stage_all_and_commit(&commit_message, args.dry_run)?;
            let recreated_branch = make_release_branch_name(args.ctx.mode);
            git.create_branch_from_current(&recreated_branch, args.dry_run)?;
            git.push_branch(&recreated_branch, false, args.dry_run)?;
            branch_name = recreated_branch;
            pr_client
                .ensure_release_pr(&args.ctx, &branch_name)
                .await?
                .map(|pr| pr.url)
        }
        _ => {
            git.stage_all_and_commit(&commit_message, args.dry_run)?;
            git.push_branch(&branch_name, true, args.dry_run)?;
            pr_client
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
