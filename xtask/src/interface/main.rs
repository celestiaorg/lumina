use anyhow::Result;
use clap::Parser;

use crate::application::pipeline::{ExecuteArgs, ReleasePipeline};
use crate::application::submit::SubmitArgs;
use crate::domain::context::{AuthContext, ReleaseContext};
use crate::interface::cli::{Cli, Commands};
use crate::interface::json_output::maybe_print_json;

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let pipeline = ReleasePipeline::new(cli.workspace_root.clone());

    match cli.command {
        Commands::Check(args) => {
            let report = pipeline.check(to_context(args.common)).await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Prepare(args) => {
            let report = pipeline.prepare(to_context(args.common)).await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Submit(args) => {
            let report = pipeline
                .submit(SubmitArgs {
                    ctx: to_context(args.common),
                    dry_run: args.dry_run,
                    branch_name_override: None,
                })
                .await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Publish(args) => {
            let report = pipeline.publish(to_context(args.common)).await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Execute(args) => {
            let report = pipeline
                .execute(ExecuteArgs {
                    ctx: to_context(args.common),
                    dry_run: args.dry_run,
                })
                .await?;
            maybe_print_json(args.json, &report)?;
        }
    }

    Ok(())
}

fn to_context(common: crate::interface::cli::CommonArgs) -> ReleaseContext {
    ReleaseContext {
        mode: common.mode.into(),
        base_commit: common.base_commit,
        default_branch: common.default_branch,
        branch_name: common.branch_name,
        rc_branch_prefix: common.rc_branch_prefix,
        final_branch_prefix: common.final_branch_prefix,
        skip_pr: common.skip_pr,
        auth: AuthContext::from_env(),
    }
}
