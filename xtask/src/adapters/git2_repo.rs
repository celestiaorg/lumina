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
    /// Creates a git adapter bound to the target workspace repository.
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    /// Reports whether the release branch exists and whether local working tree is dirty.
    pub fn branch_state(&self, branch_name: &str) -> Result<BranchState> {
        let repo = self.repo()?;
        // Missing branch is an explicit state used by prepare strategy selection.
        if !branch_exists(&repo, branch_name) {
            info!(branch=%branch_name, "git2_repo: branch missing");
            return Ok(BranchState::Missing);
        }

        // Dirty workspace influences how branch refresh behaves.
        if has_local_changes(&repo)? {
            warn!(branch=%branch_name, "git2_repo: branch exists with dirty workspace");
            Ok(BranchState::ExistsDirtyLocal)
        } else {
            warn!(branch=%branch_name, "git2_repo: branch exists and workspace clean");
            Ok(BranchState::ExistsClean)
        }
    }

    /// Ensures a local branch is checked out.
    /// Resolution order: local branch -> tracked remote branch -> create from default branch tip.
    pub fn ensure_branch_exists(&self, branch_name: &str, default_branch: &str) -> Result<()> {
        info!(
            branch=%branch_name,
            default_branch=%default_branch,
            "git2_repo: ensuring branch exists and is checked out"
        );
        let repo = self.repo()?;

        // Fast path: local branch already exists.
        if local_branch_exists(&repo, branch_name) {
            checkout_local_branch(&repo, branch_name)?;
            return Ok(());
        }

        // Next preference: materialize and checkout existing remote-tracking branch.
        if checkout_remote_branch_if_exists(&repo, branch_name)? {
            return Ok(());
        }

        // Refresh refs before deciding branch truly does not exist remotely.
        if has_origin_remote(&repo) {
            fetch_origin_branch(&repo, default_branch)?;
        }

        // Retry remote checkout after fetch.
        if checkout_remote_branch_if_exists(&repo, branch_name)? {
            return Ok(());
        }

        // Last resort: create release branch from default branch tip.
        create_local_from_default_branch(&repo, branch_name, default_branch)?;
        checkout_local_branch(&repo, branch_name)?;
        info!(branch=%branch_name, "git2_repo: created and checked out branch from default branch");
        Ok(())
    }

    /// Checks out an existing branch, preferring local first, then `origin/<branch>`.
    /// Returns an error when neither local nor remote branch can be resolved.
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

    /// Creates (or resolves) release branch from latest default branch state and returns step descriptions.
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
        // Prefer remote default-branch tip when origin is available.
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

    /// Stages all workspace changes and creates a commit unless there are no content changes.
    /// No-op when `dry_run` is enabled.
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
                // Regular commit path with one parent.
                let parent = head_ref
                    .peel_to_commit()
                    .context("failed to resolve HEAD commit")?;
                // Skip empty commit when staged tree is unchanged.
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
                // Initial-commit path when repository has no HEAD yet.
                if tree.is_empty() {
                    return Ok(());
                }
                repo.commit(Some("HEAD"), &signature, &signature, message, &tree, &[])
                    .context("failed to create initial commit")?;
            }
        }

        Ok(())
    }

    /// Returns true when any of the provided paths has local changes in working tree or index.
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

    /// Stages only provided paths and creates a commit when staged tree differs from HEAD.
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

    /// Pushes branch to `origin`, optionally as force push, and configures upstream tracking.
    /// No-op when `dry_run` is enabled.
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

        // Push branch update and fail fast on any remote-side rejection.
        remote
            .push(&[refspec.as_str()], Some(&mut push_options))
            .with_context(|| format!("failed to push branch `{branch_name}` to origin"))?;

        // Best-effort upstream tracking setup for future operations.
        if let Ok(mut local) = repo.find_branch(branch_name, BranchType::Local) {
            let _ = local.set_upstream(Some(&format!("origin/{branch_name}")));
        }

        Ok(())
    }

    /// Opens the git repository at configured workspace root.
    fn repo(&self) -> Result<Repository> {
        Repository::open(&self.workspace_root).with_context(|| {
            format!(
                "failed to open git repository at {}",
                self.workspace_root.display()
            )
        })
    }
}

/// True when local branch with given name exists.
fn local_branch_exists(repo: &Repository, branch_name: &str) -> bool {
    repo.find_branch(branch_name, BranchType::Local).is_ok()
}

/// True when repository has `origin` remote configured.
fn has_origin_remote(repo: &Repository) -> bool {
    repo.find_remote("origin").is_ok()
}

/// True when remote-tracking branch `origin/<name>` exists locally.
fn remote_branch_exists(repo: &Repository, branch_name: &str) -> bool {
    repo.find_branch(&format!("origin/{branch_name}"), BranchType::Remote)
        .is_ok()
}

/// True when either local branch or remote-tracking branch exists.
fn branch_exists(repo: &Repository, branch_name: &str) -> bool {
    local_branch_exists(repo, branch_name) || remote_branch_exists(repo, branch_name)
}

/// Checks out branch by first materializing local tracking branch from remote if needed.
fn checkout_remote_branch_if_exists(repo: &Repository, branch_name: &str) -> Result<bool> {
    // Only materialize local tracking branch when remote reference actually exists.
    if !remote_branch_exists(repo, branch_name) {
        return Ok(false);
    }
    create_local_from_remote_tracking(repo, branch_name)?;
    checkout_local_branch(repo, branch_name)?;
    Ok(true)
}

/// Performs hard checkout of an existing local branch and updates `HEAD`.
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

/// Creates local branch from `origin/<branch>` (if missing) and sets upstream tracking.
fn create_local_from_remote_tracking(repo: &Repository, branch_name: &str) -> Result<()> {
    let remote_name = format!("origin/{branch_name}");
    let remote_branch = repo
        .find_branch(&remote_name, BranchType::Remote)
        .with_context(|| format!("failed to find remote branch `{remote_name}`"))?;
    let commit = remote_branch
        .get()
        .peel_to_commit()
        .with_context(|| format!("failed to peel remote branch `{remote_name}` to commit"))?;

    // Create local branch once and keep it aligned to the remote-tracking reference.
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

/// Creates a new local branch from default branch, preferring remote tip when available.
fn create_local_from_default_branch(
    repo: &Repository,
    branch_name: &str,
    default_branch: &str,
) -> Result<()> {
    // Prefer remote default branch; fallback to local default branch reference.
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

/// Fetches a single branch from `origin` with authentication callbacks.
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

/// Returns true when working tree has tracked or untracked changes.
fn has_local_changes(repo: &Repository) -> Result<bool> {
    let mut options = StatusOptions::new();
    options.include_untracked(true).recurse_untracked_dirs(true);
    let statuses = repo
        .statuses(Some(&mut options))
        .context("failed to inspect local changes")?;
    Ok(!statuses.is_empty())
}

/// Returns repository signature, falling back to deterministic local signature.
fn signature_for_repo(repo: &Repository) -> Result<Signature<'static>> {
    match repo.signature() {
        Ok(signature) => Ok(signature),
        Err(_) => Signature::now("xtask", "xtask@localhost")
            .context("failed to construct fallback git signature"),
    }
}

/// Builds credential callback chain for fetch/push operations.
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
