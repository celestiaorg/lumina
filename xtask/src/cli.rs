use clap::{Args, Parser, Subcommand, ValueEnum};

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
    ReleaseCheck(ReleaseCheckArgs),
    Prepare(PrepareArgs),
    Submit(SubmitArgs),
    Release(ReleaseArgs),
    Execute(ExecuteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct CommonArgs {
    #[arg(value_enum)]
    pub mode: ReleaseModeArg,

    #[arg(long)]
    pub base_commit: Option<String>,

    #[arg(long, default_value = "main")]
    pub default_branch: String,

    #[arg(long)]
    pub branch_name: Option<String>,

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
pub struct ReleaseCheckArgs {
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
pub struct ReleaseArgs {
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
    pub do_release: bool,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ReleaseModeArg {
    Rc,
    Final,
}

impl From<ReleaseModeArg> for crate::domain::types::ReleaseMode {
    fn from(value: ReleaseModeArg) -> Self {
        match value {
            ReleaseModeArg::Rc => crate::domain::types::ReleaseMode::Rc,
            ReleaseModeArg::Final => crate::domain::types::ReleaseMode::Final,
        }
    }
}
