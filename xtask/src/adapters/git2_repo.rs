use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use git2::build::CheckoutBuilder;
use git2::{
    AutotagOption, BranchType, Config, Cred, CredentialType, ErrorCode, FetchOptions,
    IndexAddOption, PushOptions, RemoteCallbacks, Repository, ResetType, Signature,
    StashApplyOptions, StashFlags, StatusOptions,
};

use crate::domain::ports::GitRepo;
use crate::domain::types::{BranchKind, BranchState, ReleaseMode};

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
            return Ok(BranchState::Missing);
        }

        if has_local_changes(&repo)? {
            Ok(BranchState::ExistsDirtyLocal)
        } else {
            Ok(BranchState::ExistsClean)
        }
    }

    pub fn validate_branch_name(
        &self,
        mode: ReleaseMode,
        branch_name: &str,
        rc_prefix: &str,
    ) -> Result<BranchKind> {
        match mode {
            ReleaseMode::Rc => {
                if !branch_name.starts_with(rc_prefix) {
                    bail!("rc mode requires branch prefix '{rc_prefix}', got '{branch_name}'");
                }
                Ok(BranchKind::RcRelease)
            }
            ReleaseMode::Final => {
                if branch_name.starts_with(rc_prefix) {
                    bail!(
                        "final mode cannot use rc branch prefix '{rc_prefix}', got '{branch_name}'"
                    );
                }
                Ok(BranchKind::FinalRelease)
            }
        }
    }

    pub fn ensure_branch_exists(&self, branch_name: &str, default_branch: &str) -> Result<()> {
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
        Ok(())
    }

    pub fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        let repo = self.repo()?;
        let fetched_origin = if has_origin_remote(&repo) {
            fetch_origin_branch(&repo, default_branch)?;
            true
        } else {
            false
        };
        drop(repo);

        self.ensure_branch_exists(branch_name, default_branch)?;
        let mut actions = Vec::new();
        if fetched_origin {
            actions.push("fetched origin".to_string());
            actions.push("created release branch from latest default branch".to_string());
        } else {
            actions.push("origin remote missing; used local default branch refs".to_string());
            actions.push("created release branch from local default branch".to_string());
        }
        Ok(actions)
    }

    pub fn refresh_existing_release_branch(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        let mut repo = self.repo()?;
        let had_changes = has_local_changes(&repo)?;
        let mut actions = Vec::new();

        if had_changes {
            let signature = signature_for_repo(&repo)?;
            repo.stash_save(
                &signature,
                "xtask release prepare stash",
                Some(StashFlags::INCLUDE_UNTRACKED),
            )
            .context("failed to stash local changes")?;
            actions.push("stashed local changes".to_string());
        }

        if has_origin_remote(&repo) {
            fetch_origin_branch(&repo, default_branch)?;
            actions.push("fetched origin".to_string());
        } else {
            actions.push("origin remote missing; skipped fetch and used local refs".to_string());
        }
        drop(repo);

        self.ensure_branch_exists(branch_name, default_branch)?;
        actions.push("checked out release branch".to_string());

        let mut repo = self.repo()?;
        let generated_count = generated_release_commits_count(&repo, 25)?;
        if generated_count > 0 {
            let target = repo
                .revparse_single(&format!("HEAD~{generated_count}"))
                .context("failed to resolve reset target for generated release commits")?;
            repo.reset(&target, ResetType::Hard, None)
                .context("failed to reset generated release commits")?;
            actions.push(format!(
                "reset {} generated release commit(s)",
                generated_count
            ));
        }

        rebase_current_branch_onto(&repo, default_branch)?;
        actions.push("rebased release branch onto latest default branch".to_string());

        if had_changes {
            let mut apply_opts = StashApplyOptions::new();
            if repo.stash_pop(0, Some(&mut apply_opts)).is_ok() {
                actions.push("restored stashed local changes".to_string());
            }
        }

        Ok(actions)
    }

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

    pub fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()> {
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
                eprintln!("push reference {reference} failed: {err}");
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

    pub fn create_branch_from_current(&self, branch_name: &str, dry_run: bool) -> Result<()> {
        if dry_run {
            return Ok(());
        }

        let repo = self.repo()?;
        let head_commit = repo
            .head()
            .context("failed to resolve HEAD")?
            .peel_to_commit()
            .context("failed to resolve HEAD commit")?;

        repo.branch(branch_name, &head_commit, false)
            .with_context(|| format!("failed to create branch `{branch_name}`"))?;
        checkout_local_branch(&repo, branch_name)?;

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

fn rebase_current_branch_onto(repo: &Repository, default_branch: &str) -> Result<()> {
    let upstream_ref = format!("refs/remotes/origin/{default_branch}");
    let upstream_oid = repo
        .refname_to_id(&upstream_ref)
        .or_else(|_| repo.refname_to_id(&format!("refs/heads/{default_branch}")))
        .with_context(|| {
            format!("failed to resolve upstream branch `{default_branch}` for rebase")
        })?;
    let upstream = repo
        .find_annotated_commit(upstream_oid)
        .context("failed to resolve upstream annotated commit for rebase")?;

    let mut rebase = repo
        .rebase(None, Some(&upstream), None, None)
        .context("failed to start rebase")?;

    let signature = signature_for_repo(repo)?;
    loop {
        match rebase.next() {
            Some(Ok(_)) => {
                let index = repo.index().context("failed to load index during rebase")?;
                if index.has_conflicts() {
                    bail!("rebase stopped due to merge conflicts; resolve manually");
                }
                match rebase.commit(None, &signature, None) {
                    Ok(_) => {}
                    Err(err) if err.code() == ErrorCode::Applied => {}
                    Err(err) => {
                        return Err(
                            anyhow::Error::new(err).context("failed to apply rebase operation")
                        );
                    }
                }
            }
            Some(Err(err)) => {
                return Err(
                    anyhow::Error::new(err).context("failed while iterating rebase operations")
                );
            }
            None => break,
        }
    }

    rebase
        .finish(Some(&signature))
        .context("failed to finish rebase")?;

    Ok(())
}

fn has_local_changes(repo: &Repository) -> Result<bool> {
    let mut options = StatusOptions::new();
    options.include_untracked(true).recurse_untracked_dirs(true);
    let statuses = repo
        .statuses(Some(&mut options))
        .context("failed to inspect local changes")?;
    Ok(!statuses.is_empty())
}

fn generated_release_commits_count(repo: &Repository, max_depth: usize) -> Result<usize> {
    let mut count = 0usize;
    let mut current = match repo.head() {
        Ok(head) => head
            .peel_to_commit()
            .context("failed to resolve HEAD commit while scanning generated commits")?,
        Err(_) => return Ok(0),
    };

    for _ in 0..max_depth {
        let subject = current.summary().unwrap_or_default();
        let is_generated = subject.starts_with("chore(release):")
            || subject.starts_with("chore: prepare rc release")
            || subject.starts_with("chore: prepare final release")
            || subject.starts_with("chore: finalize release");
        if !is_generated {
            break;
        }

        count += 1;
        if current.parent_count() == 0 {
            break;
        }
        current = current
            .parent(0)
            .context("failed to walk parent commit while scanning generated commits")?;
    }

    Ok(count)
}

fn signature_for_repo(repo: &Repository) -> Result<Signature<'static>> {
    match repo.signature() {
        Ok(signature) => Ok(signature),
        Err(_) => Signature::now("xtask", "xtask@localhost")
            .context("failed to construct fallback git signature"),
    }
}

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

impl GitRepo for Git2Repo {
    fn branch_state(&self, branch_name: &str) -> Result<BranchState> {
        Git2Repo::branch_state(self, branch_name)
    }

    fn validate_branch_name(
        &self,
        mode: ReleaseMode,
        branch_name: &str,
        rc_prefix: &str,
    ) -> Result<BranchKind> {
        Git2Repo::validate_branch_name(self, mode, branch_name, rc_prefix)
    }

    fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        Git2Repo::create_release_branch_from_default(self, branch_name, default_branch)
    }

    fn refresh_existing_release_branch(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        Git2Repo::refresh_existing_release_branch(self, branch_name, default_branch)
    }

    fn stage_all_and_commit(&self, message: &str, dry_run: bool) -> Result<()> {
        Git2Repo::stage_all_and_commit(self, message, dry_run)
    }

    fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()> {
        Git2Repo::push_branch(self, branch_name, force, dry_run)
    }

    fn create_branch_from_current(&self, branch_name: &str, dry_run: bool) -> Result<()> {
        Git2Repo::create_branch_from_current(self, branch_name, dry_run)
    }
}
