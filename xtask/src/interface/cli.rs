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
    /// Create release branch, run release-plz update, commit/push/PR.
    Pr(GhaPrArgs),
    /// Publish crates and GitHub releases via release-plz.
    Publish(GhaPublishArgs),
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
pub struct GhaPrArgs {
    #[arg(long, default_value = "rc", help = "Release mode (rc or final)")]
    pub mode: CliReleaseMode,

    #[arg(long, help = "Write normalized contract fields to GITHUB_OUTPUT")]
    pub gha_output: bool,
}

#[derive(Debug, Clone, Args)]
pub struct GhaPublishArgs {
    #[arg(long, help = "Head commit message used to determine release mode")]
    pub commit_msg: String,

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
