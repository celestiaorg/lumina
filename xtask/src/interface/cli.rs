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
    Release(GhaReleasePlzArgs),
    NpmUpdatePr(GhaNpmUpdatePrArgs),
    NpmPublish(GhaNpmPublishArgs),
    UniffiRelease(GhaUniffiReleaseArgs),
}

#[derive(Debug, Clone, clap::ValueEnum, Copy)]
pub enum CliReleaseMode {
    Rc,
    Final,
}

#[derive(Debug, Clone, Args)]
pub struct GhaReleasePlzArgs {
    #[arg(long, default_value = "rc", help = "Release mode (rc or final)")]
    pub mode: CliReleaseMode,

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

    #[arg(long, help = "Skip publishing artifacts (crates, GitHub releases)")]
    pub no_artifacts: bool,
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
    #[arg(long, help = "Skip publishing npm artifacts")]
    pub no_artifacts: bool,
}

#[derive(Debug, Clone, Args)]
pub struct GhaUniffiReleaseArgs {
    #[arg(long, help = "Release payload JSON from previous job output")]
    pub releases_json: String,

    #[arg(long, help = "Write uniffi tag/files outputs to GITHUB_OUTPUT")]
    pub gha_output: bool,

    #[arg(long, help = "Skip uniffi build and release upload preparation")]
    pub no_artifacts: bool,
}
