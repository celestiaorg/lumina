use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

use crate::adapters::git_refs::parse_remote_repo_url;
use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_output::{write_github_output, write_github_output_multiline};

pub(crate) trait Ops {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String>;
    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool>;
    fn read_file(&self, path: &Path) -> Result<String>;
    fn write_file(&self, path: &Path, content: &str) -> Result<()>;
    fn git_checkout(&self, branch: &str) -> Result<()>;
    fn git_has_changes(&self, paths: &[&str]) -> Result<bool>;
    fn commit_and_push(&self, branch: &str, message: &str, paths: &[&str]) -> Result<()>;
    fn gha_output(&self, key: &str, value: &str) -> Result<()>;
    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()>;
}

pub(crate) struct RealOps {
    git: Git2Repo,
    root: std::path::PathBuf,
}

impl RealOps {
    pub(crate) fn new() -> Result<Self> {
        let root = std::env::current_dir().context("failed to resolve current workspace root")?;
        Ok(Self {
            git: Git2Repo::new(root.clone()),
            root,
        })
    }
}

impl Ops for RealOps {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String> {
        super::gha::run_checked(program, args, cwd)
    }

    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool> {
        let status = Command::new(program)
            .args(args)
            .status()
            .with_context(|| format!("failed to execute `{program}`"))?;
        Ok(status.success())
    }

    fn read_file(&self, path: &Path) -> Result<String> {
        fs::read_to_string(path)
            .with_context(|| format!("failed to read file at {}", path.display()))
    }

    fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        fs::write(path, content)
            .with_context(|| format!("failed to write file at {}", path.display()))
    }

    fn git_checkout(&self, branch: &str) -> Result<()> {
        self.git.checkout_branch_from_origin(branch)
    }

    fn git_has_changes(&self, paths: &[&str]) -> Result<bool> {
        self.git.paths_have_changes(paths)
    }

    fn commit_and_push(&self, branch: &str, message: &str, paths: &[&str]) -> Result<()> {
        let repo_url = parse_remote_repo_url(&self.root)?
            .context("no origin remote URL found; cannot create GraphQL commit")?;

        let token =
            std::env::var("GITHUB_TOKEN").context("GITHUB_TOKEN is required for GraphQL commit")?;

        let git_client = release_plz_core::GitClient::new(release_plz_core::GitForge::Github(
            release_plz_core::GitHub::new(repo_url.owner, repo_url.name, token.into()),
        ))
        .context("failed to create GitClient for GraphQL commit")?;

        let root_str = self
            .root
            .to_str()
            .context("workspace root is not valid UTF-8")?;
        let git_repo = release_plz_core::git_cmd::Repo::new(root_str)
            .context("failed to open git_cmd::Repo for GraphQL commit")?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                release_plz_core::commit_specific_files(
                    &git_client,
                    &git_repo,
                    message,
                    branch,
                    paths,
                )
                .await
                .context("failed to create commit via GitHub GraphQL API")?;
                Ok(())
            })
        })
    }

    fn gha_output(&self, key: &str, value: &str) -> Result<()> {
        write_github_output(key, value)
    }

    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()> {
        write_github_output_multiline(key, value)
    }
}
