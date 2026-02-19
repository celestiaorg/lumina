use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::domain::model::ReleaseMode;

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
    Check(CheckArgs),
    Prepare(PrepareArgs),
    Submit(SubmitArgs),
    Publish(PublishArgs),
    Execute(ExecuteArgs),
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
    NpmPublish,
    UniffiRelease(GhaUniffiReleaseArgs),
}

#[derive(Debug, Clone, Args)]
pub struct GhaReleasePlzArgs {
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
}

#[derive(Debug, Clone, Args)]
pub struct GhaNpmUpdatePrArgs {
    #[arg(long, help = "Release PR payload JSON from previous job output")]
    pub pr_json: String,

    #[arg(long, default_value = "", help = "Optional `-rc.N` suffix override")]
    pub node_rc_prefix: String,
}

#[derive(Debug, Clone, Args)]
pub struct GhaUniffiReleaseArgs {
    #[arg(long, help = "Release payload JSON from previous job output")]
    pub releases_json: String,
}

#[derive(Debug, Clone, Args)]
pub struct CommonArgs {
    #[arg(value_enum)]
    pub mode: ReleaseModeArg,

    #[arg(long = "current-commit")]
    pub current_commit: Option<String>,

    #[arg(long, default_value = "main")]
    pub default_branch: String,

    #[arg(long, default_value = "release/rc")]
    pub rc_branch_prefix: String,

    #[arg(long, default_value = "release")]
    pub final_branch_prefix: String,

    #[arg(
        long,
        help = "Run full release flow but skip opening/closing GitHub PRs"
    )]
    pub skip_pr: bool,
}

#[derive(Debug, Clone, Args)]
pub struct CheckArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct PrepareArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct SubmitArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub dry_run: bool,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct PublishArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct ExecuteArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub dry_run: bool,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ReleaseModeArg {
    Rc,
    Final,
}

impl From<ReleaseModeArg> for ReleaseMode {
    /// Maps clap enum values to internal domain mode used by the release pipeline.
    fn from(value: ReleaseModeArg) -> Self {
        match value {
            ReleaseModeArg::Rc => ReleaseMode::Rc,
            ReleaseModeArg::Final => ReleaseMode::Final,
        }
    }
}
