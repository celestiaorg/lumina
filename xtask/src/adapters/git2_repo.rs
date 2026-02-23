use std::path::{Path, PathBuf};

use crate::domain::types::BranchState;
use anyhow::{Context, Result, bail};
use git2::build::CheckoutBuilder;
use git2::{
    AutotagOption, BranchType, Config, Cred, CredentialType, ErrorCode, FetchOptions,
    IndexAddOption, PushOptions, RemoteCallbacks, Repository, Signature, StatusOptions,
};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct Git2Repo {
    workspace_root: PathBuf,
}

impl Git2Repo {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    pub fn branch_state(&self, branch_name: &str) -> Result<BranchState> {
        let repo = self.repo()?;
        if !branch_exists(&repo, branch_name) {
            info!(branch=%branch_name, "git2_repo: branch missing");
            return Ok(BranchState::Missing);
        }

        if has_local_changes(&repo)? {
            warn!(branch=%branch_name, "git2_repo: branch exists with dirty workspace");
            Ok(BranchState::ExistsDirtyLocal)
        } else {
            warn!(branch=%branch_name, "git2_repo: branch exists and workspace clean");
            Ok(BranchState::ExistsClean)
        }
    }

    /// Tries local branch, then remote-tracking branch, then creates from default branch tip.
    pub fn ensure_branch_exists(&self, branch_name: &str, default_branch: &str) -> Result<()> {
        info!(
            branch=%branch_name,
            default_branch=%default_branch,
            "git2_repo: ensuring branch exists and is checked out"
        );
        let repo = self.repo()?;

        if local_branch_exists(&repo, branch_name) {
            checkout_local_branch(&repo, branch_name)?;
            return Ok(());
        }

        if checkout_remote_branch_if_exists(&repo, branch_name)? {
            return Ok(());
        }

        if has_origin_remote(&repo) {
            fetch_origin_branch(&repo, default_branch)?;
        }

        if checkout_remote_branch_if_exists(&repo, branch_name)? {
            return Ok(());
        }

        create_local_from_default_branch(&repo, branch_name, default_branch)?;
        checkout_local_branch(&repo, branch_name)?;
        info!(branch=%branch_name, "git2_repo: created and checked out branch from default branch");
        Ok(())
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

    pub fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        info!(
            branch=%branch_name,
            default_branch=%default_branch,
            "git2_repo: creating/reusing release branch from default"
        );
        let repo = self.repo()?;
        let fetched_origin = if has_origin_remote(&repo) {
            fetch_origin_branch(&repo, default_branch)?;
            true
        } else {
            false
        };
        drop(repo);

        self.ensure_branch_exists(branch_name, default_branch)?;
        let mut descriptions = Vec::new();
        if fetched_origin {
            descriptions.push("fetched origin".to_string());
            descriptions.push("created release branch from latest default branch".to_string());
        } else {
            descriptions.push("origin remote missing; used local default branch refs".to_string());
            descriptions.push("created release branch from local default branch".to_string());
        }
        Ok(descriptions)
    }

    /// Skipped when `dry_run` is true. Skips empty commits.
    pub fn stage_all_and_commit(&self, message: &str, dry_run: bool) -> Result<()> {
        if dry_run {
            return Ok(());
        }

        let repo = self.repo()?;
        let mut index = repo.index().context("failed to open git index")?;
        index
            .add_all(["*"].iter(), IndexAddOption::DEFAULT, None)
            .context("failed to stage workspace changes")?;
        index.write().context("failed to write git index")?;

        let tree_id = index
            .write_tree()
            .context("failed to write git tree from index")?;
        let tree = repo
            .find_tree(tree_id)
            .context("failed to find staged tree")?;

        let signature = signature_for_repo(&repo)?;
        match repo.head() {
            Ok(head_ref) => {
                let parent = head_ref
                    .peel_to_commit()
                    .context("failed to resolve HEAD commit")?;
                if parent.tree_id() == tree_id {
                    return Ok(());
                }
                repo.commit(
                    Some("HEAD"),
                    &signature,
                    &signature,
                    message,
                    &tree,
                    &[&parent],
                )
                .context("failed to create commit")?;
            }
            Err(_) => {
                if tree.is_empty() {
                    return Ok(());
                }
                repo.commit(Some("HEAD"), &signature, &signature, message, &tree, &[])
                    .context("failed to create initial commit")?;
            }
        }

        Ok(())
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

    /// Returns `true` when a commit was created.
    pub fn stage_paths_and_commit(&self, paths: &[&str], message: &str) -> Result<bool> {
        let repo = self.repo()?;
        let mut index = repo.index().context("failed to open git index")?;

        for path in paths {
            let relative = Path::new(path);
            let absolute = self.workspace_root.join(relative);
            if absolute.exists() {
                index
                    .add_path(relative)
                    .with_context(|| format!("failed to stage path `{path}`"))?;
            } else {
                match index.remove_path(relative) {
                    Ok(_) => {}
                    Err(err) if err.code() == ErrorCode::NotFound => {}
                    Err(err) => {
                        return Err(anyhow::Error::new(err)
                            .context(format!("failed to remove path `{path}` from index")));
                    }
                }
            }
        }
        index.write().context("failed to write git index")?;

        let tree_id = index
            .write_tree()
            .context("failed to write git tree from index")?;
        let tree = repo
            .find_tree(tree_id)
            .context("failed to find staged tree")?;

        let signature = signature_for_repo(&repo)?;
        match repo.head() {
            Ok(head_ref) => {
                let parent = head_ref
                    .peel_to_commit()
                    .context("failed to resolve HEAD commit")?;
                if parent.tree_id() == tree_id {
                    return Ok(false);
                }
                repo.commit(
                    Some("HEAD"),
                    &signature,
                    &signature,
                    message,
                    &tree,
                    &[&parent],
                )
                .context("failed to create commit")?;
                Ok(true)
            }
            Err(_) => {
                if tree.is_empty() {
                    return Ok(false);
                }
                repo.commit(Some("HEAD"), &signature, &signature, message, &tree, &[])
                    .context("failed to create initial commit")?;
                Ok(true)
            }
        }
    }

    /// Skipped when `dry_run` is true. Sets upstream tracking after push.
    pub fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()> {
        info!(branch=%branch_name, force, dry_run, "git2_repo: pushing branch");
        if dry_run {
            return Ok(());
        }

        let repo = self.repo()?;
        let mut remote = repo
            .find_remote("origin")
            .context("failed to resolve `origin` remote")?;

        let mut callbacks = auth_callbacks(repo.config().ok());
        callbacks.push_update_reference(|reference, status| {
            if let Some(err) = status {
                return Err(git2::Error::from_str(&format!(
                    "remote rejected push for reference {reference}: {err}"
                )));
            }
            Ok(())
        });

        let mut push_options = PushOptions::new();
        push_options.remote_callbacks(callbacks);

        let refspec = if force {
            format!("+refs/heads/{branch_name}:refs/heads/{branch_name}")
        } else {
            format!("refs/heads/{branch_name}:refs/heads/{branch_name}")
        };

        remote
            .push(&[refspec.as_str()], Some(&mut push_options))
            .with_context(|| format!("failed to push branch `{branch_name}` to origin"))?;

        if let Ok(mut local) = repo.find_branch(branch_name, BranchType::Local) {
            let _ = local.set_upstream(Some(&format!("origin/{branch_name}")));
        }

        Ok(())
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

fn branch_exists(repo: &Repository, branch_name: &str) -> bool {
    local_branch_exists(repo, branch_name) || remote_branch_exists(repo, branch_name)
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

fn create_local_from_default_branch(
    repo: &Repository,
    branch_name: &str,
    default_branch: &str,
) -> Result<()> {
    let start = repo
        .find_reference(&format!("refs/remotes/origin/{default_branch}"))
        .or_else(|_| repo.find_reference(&format!("refs/heads/{default_branch}")))
        .with_context(|| {
            format!(
                "failed to resolve default branch `{default_branch}` from local or origin references"
            )
        })?;

    let commit = start
        .peel_to_commit()
        .context("failed to peel default branch reference to commit")?;

    repo.branch(branch_name, &commit, false)
        .with_context(|| format!("failed to create branch `{branch_name}` from default branch"))?;

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

fn has_local_changes(repo: &Repository) -> Result<bool> {
    let mut options = StatusOptions::new();
    options.include_untracked(true).recurse_untracked_dirs(true);
    let statuses = repo
        .statuses(Some(&mut options))
        .context("failed to inspect local changes")?;
    Ok(!statuses.is_empty())
}

fn signature_for_repo(repo: &Repository) -> Result<Signature<'static>> {
    match repo.signature() {
        Ok(signature) => Ok(signature),
        Err(_) => Signature::now("xtask", "xtask@localhost")
            .context("failed to construct fallback git signature"),
    }
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
