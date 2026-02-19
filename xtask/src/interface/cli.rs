use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Release process orchestrator")]
pub struct Cli {
    #[arg(long, default_value = ".")]
    pub workspace_root: std::path::PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Gha(GhaArgs),
}

#[derive(Debug, Clone, Args)]
pub struct GhaArgs {
    #[command(subcommand)]
    pub command: GhaCommands,
}

#[derive(Debug, Clone, Subcommand)]
pub enum GhaCommands {
    ReleasePlz(GhaReleasePlzArgs),
    NpmUpdatePr(GhaNpmUpdatePrArgs),
    NpmPublish(GhaNpmPublishArgs),
    UniffiRelease(GhaUniffiReleaseArgs),
}

#[derive(Debug, Clone, Args)]
pub struct GhaReleasePlzArgs {
    #[arg(
        long,
        help = "Branch to compare against for release planning and as target branch for release PRs"
    )]
    pub compare_branch: Option<String>,

    #[arg(long, default_value = "main")]
    pub default_branch: String,

    #[arg(long, default_value = "release/rc")]
    pub rc_branch_prefix: String,

    #[arg(long, default_value = "release")]
    pub final_branch_prefix: String,

    #[arg(long, help = "Write normalized contract fields to GITHUB_OUTPUT")]
    pub gha_output: bool,

    #[arg(long, help = "Write full normalized contract as JSON file")]
    pub json_out: Option<std::path::PathBuf>,

    #[arg(
        long,
        help = "Dry-run: never execute crates publish path; force prepare/PR flow only"
    )]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct GhaNpmUpdatePrArgs {
    #[arg(long, help = "Release PR payload JSON from previous job output")]
    pub pr_json: String,

    #[arg(
        long,
        default_value = "",
        allow_hyphen_values = true,
        help = "Optional `-rc.N` suffix override"
    )]
    pub node_rc_prefix: String,
}

#[derive(Debug, Clone, Args)]
pub struct GhaNpmPublishArgs {
    #[arg(long, help = "Dry-run: do not publish npm artifacts")]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct GhaUniffiReleaseArgs {
    #[arg(long, help = "Release payload JSON from previous job output")]
    pub releases_json: String,

    #[arg(long, help = "Write uniffi tag/files outputs to GITHUB_OUTPUT")]
    pub gha_output: bool,

    #[arg(
        long,
        help = "Dry-run: skip uniffi build and release upload preparation"
    )]
    pub dry_run: bool,
}
