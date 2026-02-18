use anyhow::Result;
use clap::Parser;

use crate::application::pipeline::{ExecuteArgs, ReleasePipeline};
use crate::application::submit::SubmitArgs;
use crate::domain::context::{
    AuthContext, BranchContext, CheckContext, CommonContext, ExecuteContext, PrepareContext,
    PublishContext, SubmitContext,
};
use crate::interface::cli::{Cli, Commands, CommonArgs};
use crate::interface::json_output::maybe_print_json;

/// Parses CLI args, dispatches command handlers, and prints optional JSON reports.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let pipeline = ReleasePipeline::new(cli.workspace_root.clone());

    // Dispatch each CLI command to the matching pipeline stage and optional JSON output.
    match cli.command {
        Commands::Check(args) => {
            let report = pipeline.check(to_check_context(args.common)).await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Prepare(args) => {
            let report = pipeline.prepare(to_prepare_context(args.common)).await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Submit(args) => {
            let report = pipeline
                .submit(SubmitArgs {
                    ctx: to_submit_context(args.common),
                    dry_run: args.dry_run,
                    branch_name_override: None,
                    update_strategy_override: None,
                })
                .await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Publish(args) => {
            let report = pipeline.publish(to_publish_context(args.common)).await?;
            maybe_print_json(args.json, &report)?;
        }
        Commands::Execute(args) => {
            let report = pipeline
                .execute(ExecuteArgs {
                    ctx: to_execute_context(args.common),
                    dry_run: args.dry_run,
                })
                .await?;
            maybe_print_json(args.json, &report)?;
        }
    }

    Ok(())
}

fn to_common_context(common: &CommonArgs) -> CommonContext {
    // Auth is loaded from environment to keep CLI flags focused on flow configuration.
    CommonContext {
        mode: common.mode.into(),
        default_branch: common.default_branch.clone(),
        auth: AuthContext::from_env(),
    }
}

fn to_branch_context(common: &CommonArgs) -> BranchContext {
    BranchContext {
        skip_pr: common.skip_pr,
    }
}

fn to_check_context(common: CommonArgs) -> CheckContext {
    CheckContext {
        common: to_common_context(&common),
        current_commit: common.current_commit,
    }
}

fn to_prepare_context(common: CommonArgs) -> PrepareContext {
    PrepareContext {
        common: to_common_context(&common),
    }
}

fn to_submit_context(common: CommonArgs) -> SubmitContext {
    SubmitContext {
        common: to_common_context(&common),
        branch: to_branch_context(&common),
    }
}

fn to_publish_context(common: CommonArgs) -> PublishContext {
    PublishContext {
        common: to_common_context(&common),
        rc_branch_prefix: common.rc_branch_prefix,
        final_branch_prefix: common.final_branch_prefix,
    }
}

fn to_execute_context(common: CommonArgs) -> ExecuteContext {
    ExecuteContext {
        common: to_common_context(&common),
        branch: to_branch_context(&common),
    }
}
