use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_output::{write_github_output, write_github_output_multiline};

pub(crate) trait Ops {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String>;
    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool>;
    fn read_file(&self, path: &Path) -> Result<String>;
    fn write_file(&self, path: &Path, content: &str) -> Result<()>;
    fn git_checkout(&self, branch: &str) -> Result<()>;
    fn git_has_changes(&self, paths: &[&str]) -> Result<bool>;
    fn git_commit(&self, paths: &[&str], message: &str) -> Result<bool>;
    fn git_push(&self, branch: &str, force: bool, dry_run: bool) -> Result<()>;
    fn gha_output(&self, key: &str, value: &str) -> Result<()>;
    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()>;
}

pub(crate) struct RealOps {
    git: Git2Repo,
}

impl RealOps {
    pub(crate) fn new() -> Result<Self> {
        let root = std::env::current_dir().context("failed to resolve current workspace root")?;
        Ok(Self {
            git: Git2Repo::new(root),
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

    fn git_commit(&self, paths: &[&str], message: &str) -> Result<bool> {
        self.git.stage_paths_and_commit(paths, message)
    }

    fn git_push(&self, branch: &str, force: bool, dry_run: bool) -> Result<()> {
        self.git.push_branch(branch, force, dry_run)
    }

    fn gha_output(&self, key: &str, value: &str) -> Result<()> {
        write_github_output(key, value)
    }

    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()> {
        write_github_output_multiline(key, value)
    }
}
