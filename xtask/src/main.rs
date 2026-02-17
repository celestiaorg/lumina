mod app;
mod cli;
mod domain;
mod engine;
mod infra;
mod orchestrator;
mod output;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use domain::types::ReleaseContext;
use orchestrator::{ExecuteArgs, Orchestrator, SubmitArgs};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let orchestrator = Orchestrator::new(cli.workspace_root.clone())?;

    match cli.command {
        Commands::Check(args) => {
            let report = orchestrator.release_check(to_context(args.common)).await?;
            output::maybe_print_json(args.json, &report)?;
        }
        Commands::Prepare(args) => {
            let report = orchestrator.prepare(to_context(args.common)).await?;
            output::maybe_print_json(args.json, &report)?;
        }
        Commands::Submit(args) => {
            let report = orchestrator
                .submit(SubmitArgs {
                    ctx: to_context(args.common),
                    dry_run: args.dry_run,
                    branch_name_override: None,
                })
                .await?;
            output::maybe_print_json(args.json, &report)?;
        }
        Commands::Publish(args) => {
            let report = orchestrator.publish(to_context(args.common)).await?;
            output::maybe_print_json(args.json, &report)?;
        }
        Commands::Execute(args) => {
            let report = orchestrator
                .execute(ExecuteArgs {
                    ctx: to_context(args.common),
                    dry_run: args.dry_run,
                })
                .await?;
            output::maybe_print_json(args.json, &report)?;
        }
    }

    Ok(())
}

fn to_context(common: cli::CommonArgs) -> ReleaseContext {
    ReleaseContext {
        mode: common.mode.into(),
        base_commit: common.base_commit,
        default_branch: common.default_branch,
        branch_name: common.branch_name,
        rc_branch_prefix: common.rc_branch_prefix,
        final_branch_prefix: common.final_branch_prefix,
        skip_pr: common.skip_pr,
        auth: domain::types::AuthContext::from_env(),
    }
}
