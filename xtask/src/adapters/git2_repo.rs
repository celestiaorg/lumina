use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use git2::build::CheckoutBuilder;
use git2::{
    AutotagOption, BranchType, Config, Cred, CredentialType, FetchOptions, RemoteCallbacks,
    Repository, StatusOptions,
};
use tracing::info;

#[derive(Debug, Clone)]
pub struct Git2Repo {
    workspace_root: PathBuf,
}

impl Git2Repo {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    pub fn checkout_branch_from_origin(&self, branch_name: &str) -> Result<()> {
        info!(branch=%branch_name, "git2_repo: checking out branch from local/origin");
        let repo = self.repo()?;

        if local_branch_exists(&repo, branch_name) {
            checkout_local_branch(&repo, branch_name)?;
            return Ok(());
        }

        if has_origin_remote(&repo) {
            fetch_origin_branch(&repo, branch_name)?;
        }

        if checkout_remote_branch_if_exists(&repo, branch_name)? {
            return Ok(());
        }

        bail!("failed to resolve branch `{branch_name}` from local refs or origin remote");
    }

    pub fn paths_have_changes(&self, paths: &[&str]) -> Result<bool> {
        let repo = self.repo()?;
        let mut options = StatusOptions::new();
        options.include_untracked(true).recurse_untracked_dirs(true);
        for path in paths {
            options.pathspec(*path);
        }
        let statuses = repo
            .statuses(Some(&mut options))
            .context("failed to inspect path-scoped local changes")?;
        Ok(!statuses.is_empty())
    }

    fn repo(&self) -> Result<Repository> {
        Repository::open(&self.workspace_root).with_context(|| {
            format!(
                "failed to open git repository at {}",
                self.workspace_root.display()
            )
        })
    }
}

fn local_branch_exists(repo: &Repository, branch_name: &str) -> bool {
    repo.find_branch(branch_name, BranchType::Local).is_ok()
}

fn has_origin_remote(repo: &Repository) -> bool {
    repo.find_remote("origin").is_ok()
}

fn remote_branch_exists(repo: &Repository, branch_name: &str) -> bool {
    repo.find_branch(&format!("origin/{branch_name}"), BranchType::Remote)
        .is_ok()
}

fn checkout_remote_branch_if_exists(repo: &Repository, branch_name: &str) -> Result<bool> {
    if !remote_branch_exists(repo, branch_name) {
        return Ok(false);
    }
    create_local_from_remote_tracking(repo, branch_name)?;
    checkout_local_branch(repo, branch_name)?;
    Ok(true)
}

fn checkout_local_branch(repo: &Repository, branch_name: &str) -> Result<()> {
    let local_ref = format!("refs/heads/{branch_name}");
    let object = repo
        .revparse_single(&local_ref)
        .with_context(|| format!("failed to resolve branch `{branch_name}`"))?;
    let mut checkout = CheckoutBuilder::new();
    checkout.force();
    repo.checkout_tree(&object, Some(&mut checkout))
        .with_context(|| format!("failed to checkout branch `{branch_name}`"))?;
    repo.set_head(&local_ref)
        .with_context(|| format!("failed to set HEAD to branch `{branch_name}`"))?;
    Ok(())
}

fn create_local_from_remote_tracking(repo: &Repository, branch_name: &str) -> Result<()> {
    let remote_name = format!("origin/{branch_name}");
    let remote_branch = repo
        .find_branch(&remote_name, BranchType::Remote)
        .with_context(|| format!("failed to find remote branch `{remote_name}`"))?;
    let commit = remote_branch
        .get()
        .peel_to_commit()
        .with_context(|| format!("failed to peel remote branch `{remote_name}` to commit"))?;

    if repo.find_branch(branch_name, BranchType::Local).is_err() {
        repo.branch(branch_name, &commit, false)
            .with_context(|| format!("failed to create local branch `{branch_name}`"))?;
    }

    let mut local = repo
        .find_branch(branch_name, BranchType::Local)
        .with_context(|| format!("failed to find local branch `{branch_name}`"))?;
    let _ = local.set_upstream(Some(&remote_name));
    Ok(())
}

fn fetch_origin_branch(repo: &Repository, branch: &str) -> Result<()> {
    let mut remote = repo
        .find_remote("origin")
        .context("failed to resolve `origin` remote")?;

    let mut callbacks = auth_callbacks(repo.config().ok());
    callbacks.transfer_progress(|progress| {
        let _ = progress;
        true
    });

    let mut fetch_options = FetchOptions::new();
    fetch_options.download_tags(AutotagOption::All);
    fetch_options.remote_callbacks(callbacks);

    remote
        .fetch(&[branch], Some(&mut fetch_options), None)
        .with_context(|| format!("failed to fetch `origin/{branch}`"))
}

/// Priority: token env vars -> git credential helper -> SSH agent -> libgit2 default.
fn auth_callbacks(config: Option<Config>) -> RemoteCallbacks<'static> {
    let mut callbacks = RemoteCallbacks::new();
    callbacks.credentials(move |url, username_from_url, allowed| {
        if allowed.contains(CredentialType::USER_PASS_PLAINTEXT)
            && let Ok(token) =
                std::env::var("RELEASE_PLZ_TOKEN").or_else(|_| std::env::var("GITHUB_TOKEN"))
        {
            let username = username_from_url.unwrap_or("x-access-token");
            return Cred::userpass_plaintext(username, &token);
        }

        if let Some(cfg) = config.as_ref()
            && let Ok(cred) = Cred::credential_helper(cfg, url, username_from_url)
        {
            return Ok(cred);
        }

        if allowed.contains(CredentialType::SSH_KEY)
            && let Some(username) = username_from_url
            && let Ok(cred) = Cred::ssh_key_from_agent(username)
        {
            return Ok(cred);
        }

        Cred::default()
    });
    callbacks
}
