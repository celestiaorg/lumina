use anyhow::Result;
use clap::Parser;
use tracing::{debug, info};

use crate::application::gha::{
    GhaNpmUpdatePrArgs as AppGhaNpmUpdatePrArgs, GhaReleasePlzArgs as AppGhaReleasePlzArgs,
    GhaUniffiReleaseArgs as AppGhaUniffiReleaseArgs, handle_gha_npm_publish,
    handle_gha_npm_update_pr, handle_gha_release_plz, handle_gha_uniffi_release,
};
use crate::application::pipeline::{ExecuteArgs, ReleasePipeline};
use crate::application::submit::SubmitArgs;
use crate::domain::context::{
    AuthContext, BranchContext, CheckContext, CommonContext, ExecuteContext, PrepareContext,
    PublishContext, SubmitContext,
};
use crate::interface::cli::{Cli, Commands, CommonArgs, GhaCommands};
use crate::interface::json_output::maybe_print_json;

/// Parses CLI args, dispatches command handlers, and prints optional JSON reports.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    info!(workspace_root=%cli.workspace_root.display(), "starting xtask");
    let pipeline = ReleasePipeline::new(cli.workspace_root.clone());

    // Dispatch each CLI command to the matching pipeline stage and optional JSON output.
    match cli.command {
        Commands::Check(args) => {
            info!(
                mode=?args.common.mode,
                current_commit=?args.common.current_commit,
                default_branch=%args.common.default_branch,
                json=args.json,
                "running check"
            );
            let report = pipeline.check(to_check_context(args.common)).await?;
            debug!("check completed");
            maybe_print_json(args.json, &report)?;
        }
        Commands::Prepare(args) => {
            info!(
                mode=?args.common.mode,
                default_branch=%args.common.default_branch,
                skip_pr=args.common.skip_pr,
                json=args.json,
                "running prepare"
            );
            let report = pipeline.prepare(to_prepare_context(args.common)).await?;
            info!(branch=%report.branch_name, strategy=?report.update_strategy, "prepare completed");
            maybe_print_json(args.json, &report)?;
        }
        Commands::Submit(args) => {
            info!(
                mode=?args.common.mode,
                default_branch=%args.common.default_branch,
                skip_pr=args.common.skip_pr,
                dry_run=args.dry_run,
                json=args.json,
                "running submit"
            );
            let report = pipeline
                .submit(SubmitArgs {
                    ctx: to_submit_context(args.common),
                    dry_run: args.dry_run,
                    branch_name_override: None,
                    update_strategy_override: None,
                })
                .await?;
            info!(
                branch=%report.branch_name,
                strategy=?report.update_strategy,
                pushed=report.pushed,
                pr_url=?report.pr_url,
                "submit completed"
            );
            maybe_print_json(args.json, &report)?;
        }
        Commands::Publish(args) => {
            info!(
                mode=?args.common.mode,
                default_branch=%args.common.default_branch,
                json=args.json,
                "running publish"
            );
            let report = pipeline.publish(to_publish_context(args.common)).await?;
            info!(published = report.published, "publish completed");
            maybe_print_json(args.json, &report)?;
        }
        Commands::Execute(args) => {
            info!(
                mode=?args.common.mode,
                default_branch=%args.common.default_branch,
                skip_pr=args.common.skip_pr,
                dry_run=args.dry_run,
                json=args.json,
                "running execute"
            );
            let report = pipeline
                .execute(ExecuteArgs {
                    ctx: to_execute_context(args.common),
                    dry_run: args.dry_run,
                })
                .await?;
            info!(
                branch=%report.submit.branch_name,
                pushed=report.submit.pushed,
                pr_url=?report.submit.pr_url,
                "execute completed"
            );
            maybe_print_json(args.json, &report)?;
        }
        Commands::Gha(args) => match args.command {
            GhaCommands::ReleasePlz(cmd) => {
                info!(
                    default_branch=%cmd.default_branch,
                    rc_branch_prefix=%cmd.rc_branch_prefix,
                    final_branch_prefix=%cmd.final_branch_prefix,
                    gha_output=cmd.gha_output,
                    json_out=?cmd.json_out,
                    "running gha release-plz driver"
                );
                let contract = handle_gha_release_plz(
                    &pipeline,
                    AppGhaReleasePlzArgs {
                        default_branch: cmd.default_branch,
                        rc_branch_prefix: cmd.rc_branch_prefix,
                        final_branch_prefix: cmd.final_branch_prefix,
                        gha_output: cmd.gha_output,
                        json_out: cmd.json_out,
                    },
                )
                .await?;
                maybe_print_json(true, &contract)?;
            }
            GhaCommands::NpmUpdatePr(cmd) => {
                info!("running gha npm-update-pr");
                handle_gha_npm_update_pr(AppGhaNpmUpdatePrArgs {
                    pr_json: cmd.pr_json,
                    node_rc_prefix: cmd.node_rc_prefix,
                })?;
            }
            GhaCommands::NpmPublish => {
                info!("running gha npm-publish");
                handle_gha_npm_publish()?;
            }
            GhaCommands::UniffiRelease(cmd) => {
                info!("running gha uniffi-release");
                handle_gha_uniffi_release(AppGhaUniffiReleaseArgs {
                    releases_json: cmd.releases_json,
                })?;
            }
        },
    }

    Ok(())
}

fn to_common_context(common: &CommonArgs) -> CommonContext {
    // Auth is loaded from environment to keep CLI flags focused on flow configuration.
    let auth = AuthContext::from_env();
    debug!(
        mode=?common.mode,
        default_branch=%common.default_branch,
        has_release_plz_token=auth.release_plz_token.is_some(),
        has_github_token=auth.github_token.is_some(),
        has_cargo_registry_token=auth.cargo_registry_token.is_some(),
        "constructed common context"
    );
    CommonContext {
        mode: common.mode.into(),
        default_branch: common.default_branch.clone(),
        auth,
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
