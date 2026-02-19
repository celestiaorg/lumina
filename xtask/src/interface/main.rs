use anyhow::Result;
use clap::Parser;
use tracing::info;

use crate::application::gha::{
    GhaNpmPublishArgs as AppGhaNpmPublishArgs, GhaNpmUpdatePrArgs as AppGhaNpmUpdatePrArgs,
    GhaReleasePlzArgs as AppGhaReleasePlzArgs, GhaUniffiReleaseArgs as AppGhaUniffiReleaseArgs,
    handle_gha_npm_publish, handle_gha_npm_update_pr, handle_gha_release_plz,
    handle_gha_uniffi_release,
};
use crate::application::pipeline::ReleasePipeline;
use crate::interface::cli::{Cli, Commands, GhaCommands};
use crate::interface::json_output::maybe_print_json;

/// Parses CLI args, dispatches command handlers, and prints optional JSON reports.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    info!(workspace_root=%cli.workspace_root.display(), "starting xtask");
    let pipeline = ReleasePipeline::new(cli.workspace_root.clone());

    // Only GHA commands are exposed through interface CLI.
    match cli.command {
        Commands::Gha(args) => match args.command {
            GhaCommands::ReleasePlz(cmd) => {
                info!(
                    compare_branch=?cmd.compare_branch,
                    default_branch=%cmd.default_branch,
                    rc_branch_prefix=%cmd.rc_branch_prefix,
                    final_branch_prefix=%cmd.final_branch_prefix,
                    gha_output=cmd.gha_output,
                    json_out=?cmd.json_out,
                    dry_run=cmd.dry_run,
                    "running gha release-plz driver"
                );
                let contract = handle_gha_release_plz(
                    &pipeline,
                    AppGhaReleasePlzArgs {
                        compare_branch: cmd.compare_branch,
                        default_branch: cmd.default_branch,
                        rc_branch_prefix: cmd.rc_branch_prefix,
                        final_branch_prefix: cmd.final_branch_prefix,
                        gha_output: cmd.gha_output,
                        json_out: cmd.json_out,
                        dry_run: cmd.dry_run,
                    },
                )
                .await?;
                maybe_print_json(true, &contract)?;
            }
            GhaCommands::NpmUpdatePr(cmd) => {
                info!(
                    pr_json_len=cmd.pr_json.len(),
                    node_rc_prefix=%cmd.node_rc_prefix,
                    "running gha npm-update-pr"
                );
                handle_gha_npm_update_pr(AppGhaNpmUpdatePrArgs {
                    pr_json: cmd.pr_json,
                    node_rc_prefix: cmd.node_rc_prefix,
                })?;
            }
            GhaCommands::NpmPublish(cmd) => {
                info!(dry_run=cmd.dry_run, "running gha npm-publish");
                handle_gha_npm_publish(AppGhaNpmPublishArgs {
                    dry_run: cmd.dry_run,
                })?;
            }
            GhaCommands::UniffiRelease(cmd) => {
                info!(
                    releases_json_len=cmd.releases_json.len(),
                    gha_output=cmd.gha_output,
                    dry_run=cmd.dry_run,
                    "running gha uniffi-release"
                );
                handle_gha_uniffi_release(AppGhaUniffiReleaseArgs {
                    releases_json: cmd.releases_json,
                    gha_output: cmd.gha_output,
                    dry_run: cmd.dry_run,
                })?;
            }
        },
    }

    Ok(())
}
