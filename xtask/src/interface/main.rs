use anyhow::Result;
use clap::Parser;
use tracing::info;

use crate::application::gha::{
    GhaNpmPublishArgs as AppGhaNpmPublishArgs, GhaNpmUpdatePrArgs as AppGhaNpmUpdatePrArgs,
    GhaPrArgs as AppGhaPrArgs, GhaPublishArgs as AppGhaPublishArgs,
    GhaUniffiReleaseArgs as AppGhaUniffiReleaseArgs, handle_gha_npm_publish,
    handle_gha_npm_update_pr, handle_gha_pr, handle_gha_publish, handle_gha_uniffi_release,
};
use crate::application::pipeline::ReleasePipeline;
use crate::domain::types::ReleaseMode;
use crate::interface::cli::{Cli, CliReleaseMode, Commands, GhaCommands};

/// Parses CLI args, dispatches command handlers, and prints optional JSON reports.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let pipeline = ReleasePipeline::new(cli.workspace_root.clone());

    match cli.command {
        Commands::Gha(args) => match args.command {
            GhaCommands::Pr(cmd) => {
                info!(
                    compare_branch=?cmd.compare_branch,
                    default_branch=%cmd.default_branch,
                    gha_output=cmd.gha_output,
                    "running gha pr"
                );
                let contract = handle_gha_pr(
                    &pipeline,
                    AppGhaPrArgs {
                        mode: match cmd.mode {
                            CliReleaseMode::Rc => ReleaseMode::Rc,
                            CliReleaseMode::Final => ReleaseMode::Final,
                        },
                        compare_branch: cmd.compare_branch,
                        default_branch: cmd.default_branch,
                        gha_output: cmd.gha_output,
                    },
                )
                .await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&contract)
                        .unwrap_or_else(|_| format!("{contract:?}"))
                );
            }
            GhaCommands::Publish(cmd) => {
                info!(
                    compare_branch=?cmd.compare_branch,
                    default_branch=%cmd.default_branch,
                    rc_branch_prefix=%cmd.rc_branch_prefix,
                    final_branch_prefix=%cmd.final_branch_prefix,
                    gha_output=cmd.gha_output,
                    no_artifacts=cmd.no_artifacts,
                    "running gha publish"
                );
                let contract = handle_gha_publish(
                    &pipeline,
                    AppGhaPublishArgs {
                        commit_msg: cmd.commit_msg,
                        compare_branch: cmd.compare_branch,
                        default_branch: cmd.default_branch,
                        rc_branch_prefix: cmd.rc_branch_prefix,
                        final_branch_prefix: cmd.final_branch_prefix,
                        gha_output: cmd.gha_output,
                        no_artifacts: cmd.no_artifacts,
                    },
                )
                .await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&contract)
                        .unwrap_or_else(|_| format!("{contract:?}"))
                );
            }
            GhaCommands::NpmUpdatePr(cmd) => {
                info!("running gha npm-update-pr");
                handle_gha_npm_update_pr(AppGhaNpmUpdatePrArgs {
                    pr_json: cmd.pr_json,
                    node_rc_prefix: cmd.node_rc_prefix,
                })?;
            }
            GhaCommands::NpmPublish(cmd) => {
                info!("running gha npm-publish");
                handle_gha_npm_publish(AppGhaNpmPublishArgs {
                    no_artifacts: cmd.no_artifacts,
                })?;
            }
            GhaCommands::UniffiRelease(cmd) => {
                info!("running gha uniffi-release");
                handle_gha_uniffi_release(AppGhaUniffiReleaseArgs {
                    releases_json: cmd.releases_json,
                    gha_output: cmd.gha_output,
                    no_artifacts: cmd.no_artifacts,
                })?;
            }
        },
    }

    Ok(())
}
