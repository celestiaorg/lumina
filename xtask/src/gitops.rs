//! Thin, well-typed git primitives for the release tool.
//!
//! This module shells out to the `git` CLI (via [`std::process::Command`]) to
//! provide exactly the set of git operations the two orchestrator flows need:
//! branch existence checks, create-or-reset of the release branch, a stage-all +
//! single-commit helper, push, tag existence checks, and tag creation.
//!
//! The module owns *no policy*: it does not decide *when* to call these (branch
//! guard, `--push` gating, idempotency scan) — that lives in the command modules.
//! It only exposes correct, well-documented primitives.
//!
//! ## Side effects & network
//!
//! Every method runs `git` in [`Git::repo_root`] via
//! [`Command::current_dir`](std::process::Command::current_dir), so behavior never
//! depends on the process's current directory.
//!
//! | Method | Mutates repo | Network |
//! | --- | --- | --- |
//! | [`Git::new`] | no | no |
//! | [`Git::branch_exists`] | no | only with [`Where::Remote`]/[`Where::Both`] |
//! | [`Git::create_or_reset_branch`] | yes | no |
//! | [`Git::stage_all_and_commit`] | yes | no |
//! | [`Git::push`] | remote ref | **yes** |
//! | [`Git::tag_exists`] | no | only with [`Where::Remote`]/[`Where::Both`] |
//! | [`Git::list_tags`] | no | only with [`Where::Remote`]/[`Where::Both`] |
//! | [`Git::create_tag`] | yes (local tag) | no |
//!
//! Tokens/secrets are **never** handled here: [`Git::push`] and the remote checks
//! rely on git's ambient credential configuration (credential helper / `gh auth`).
//!
//! Spec traceability: `release-spec/orchestrator.md` Flow 1 (branch guard, single
//! commit, push) and `release-spec/publish-crates-logic.md` (tag existence scan +
//! idempotent tag creation, the orphan-tag fix).

use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// Selects which side(s) of an existence check to consult.
///
/// Encodes the network intent explicitly: Flow 1 checks the local repo always and
/// the remote only under `--push`, while the publish idempotency scan checks
/// "locally and on the remote".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Where {
    /// Consult only the local repository (no network).
    Local,
    /// Consult only the named remote (touches the network).
    Remote(String),
    /// True if it exists locally **or** on the named remote. Local is checked
    /// first and short-circuits, so a local hit avoids any network call.
    Both(String),
}

/// The set of staged changes relative to `HEAD`, split into upserts (added or
/// modified paths, whose content is uploaded) and deletions (removed paths).
///
/// Feeds the GitHub-signed commit path: [`crate::forge::create_commit_on_branch`]
/// takes file *additions* (path + content) and *deletions* (path only), so the
/// working-tree change set is enumerated as these two lists. Renames are decomposed
/// into a deletion + an addition (the diff is taken `--no-renames`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagedPaths {
    /// Added/modified/type-changed paths (git status `A`/`M`/`T`/`C`), repo-relative.
    pub upserts: Vec<String>,
    /// Deleted paths (git status `D`), repo-relative.
    pub deletions: Vec<String>,
}

/// Options controlling [`Git::push`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushOptions {
    /// Pass `--set-upstream` so the local branch tracks the pushed remote branch.
    pub set_upstream: bool,
    /// Force the push using `--force-with-lease` (the safe force that refuses to
    /// clobber a remote that moved unexpectedly). Never a bare `--force`.
    pub force: bool,
}

/// A git author/committer identity to inject into the release commit and
/// annotated tags via `git -c user.name=… -c user.email=…`, so they are
/// attributed to a specific account regardless of the runner's ambient git
/// config. Resolved from the GitHub token owner by
/// [`crate::forge::token_committer`], mirroring how release-plz attributes the
/// release commit/tags to the account that owns the token.
#[derive(Debug, Clone)]
pub struct GitIdentity {
    pub name: String,
    pub email: String,
}

impl GitIdentity {
    /// The `-c user.name=… -c user.email=…` prefix args for a `git` invocation.
    /// These override any repo/global `user.*` config for that single command.
    fn config_args(&self) -> Vec<String> {
        vec![
            "-c".to_string(),
            format!("user.name={}", self.name),
            "-c".to_string(),
            format!("user.email={}", self.email),
        ]
    }
}

/// Git primitives bound to a single working-tree root.
#[derive(Debug, Clone)]
pub struct Git {
    repo_root: PathBuf,
    /// Identity injected into `commit` / annotated `tag` invocations. `None` ⇒
    /// rely on the ambient repo/global `user.name` / `user.email`.
    identity: Option<GitIdentity>,
}

impl Git {
    /// Create a `Git` bound to `repo_root`. Performs **no** I/O or validation; the
    /// path is only stored. Validation surfaces from the first real command.
    pub fn new(repo_root: impl Into<PathBuf>) -> Self {
        Self {
            repo_root: repo_root.into(),
            identity: None,
        }
    }

    /// Set the committer/author identity used for `commit` and annotated `tag`
    /// invocations (see [`GitIdentity`]). `None` leaves the ambient git config in
    /// charge.
    pub fn with_identity(mut self, identity: Option<GitIdentity>) -> Self {
        self.identity = identity;
        self
    }

    /// The `-c user.name/user.email` prefix args for the configured identity, or an
    /// empty vec when none is set (fall back to ambient git config).
    fn identity_args(&self) -> Vec<String> {
        self.identity
            .as_ref()
            .map(GitIdentity::config_args)
            .unwrap_or_default()
    }

    /// The working-tree root every `git` invocation runs in.
    pub fn repo_root(&self) -> &PathBuf {
        &self.repo_root
    }

    /// Run `git` with the given args in [`Self::repo_root`], capturing output.
    ///
    /// Returns the raw [`std::process::Output`]; callers decide how to interpret a
    /// non-zero exit. Errors only when the process could not be spawned.
    fn run<I, S>(&self, args: I) -> Result<std::process::Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args: Vec<S> = args.into_iter().collect();
        let pretty: Vec<String> = args
            .iter()
            .map(|a| a.as_ref().to_string_lossy().into_owned())
            .collect();
        Command::new("git")
            .current_dir(&self.repo_root)
            .args(&args)
            .output()
            .with_context(|| format!("failed to spawn `git {}`", pretty.join(" ")))
    }

    /// Run `git`, requiring a zero exit; on failure return an error carrying the
    /// subcommand and captured stderr.
    fn run_checked<I, S>(&self, args: I) -> Result<std::process::Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args: Vec<S> = args.into_iter().collect();
        let pretty: Vec<String> = args
            .iter()
            .map(|a| a.as_ref().to_string_lossy().into_owned())
            .collect();
        let out = self.run(&args)?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            bail!(
                "`git {}` failed ({}): {}",
                pretty.join(" "),
                out.status,
                stderr.trim()
            );
        }
        Ok(out)
    }

    /// Check whether a branch exists.
    ///
    /// - Local: true iff `refs/heads/<name>` resolves. Absent ⇒ `Ok(false)`.
    /// - Remote: true iff `git ls-remote --heads <remote> <name>` matches
    ///   (**network**).
    /// - [`Where::Both`]: local first, short-circuiting before any network call.
    ///
    /// # Errors
    /// The `git` process could not be spawned, or the remote lookup failed for a
    /// reason other than "ref absent" (unknown remote, auth/network failure). An
    /// absent ref is never an error.
    pub fn branch_exists(&self, name: &str, where_: Where) -> Result<bool> {
        self.ref_exists("refs/heads/", "--heads", name, where_)
    }

    /// Shared existence check for a ref in the `refs_prefix` namespace
    /// (`refs/heads/` or `refs/tags/`), consulting local and/or the remote per
    /// `where_`. `ls_flag` is the matching `git ls-remote` selector (`--heads` /
    /// `--tags`). [`Where::Both`] checks local first, short-circuiting the network.
    fn ref_exists(
        &self,
        refs_prefix: &str,
        ls_flag: &str,
        name: &str,
        where_: Where,
    ) -> Result<bool> {
        match where_ {
            Where::Local => self.local_ref_exists(&format!("{refs_prefix}{name}")),
            Where::Remote(remote) => self.remote_ref_exists(&remote, ls_flag, name),
            Where::Both(remote) => {
                if self.local_ref_exists(&format!("{refs_prefix}{name}"))? {
                    Ok(true)
                } else {
                    self.remote_ref_exists(&remote, ls_flag, name)
                }
            }
        }
    }

    /// Check whether a tag exists. Mirrors [`Self::branch_exists`] for the tag
    /// namespace; used by the publish-crates idempotency scan.
    ///
    /// - Local: true iff `refs/tags/<name>` resolves. Absent ⇒ `Ok(false)`.
    /// - Remote: true iff `git ls-remote --tags <remote> <name>` matches
    ///   (**network**).
    /// - [`Where::Both`]: local first, short-circuiting before any network call.
    ///
    /// # Errors
    /// As for [`Self::branch_exists`].
    pub fn tag_exists(&self, name: &str, where_: Where) -> Result<bool> {
        self.ref_exists("refs/tags/", "--tags", name, where_)
    }

    /// List tag names.
    ///
    /// - [`Where::Local`]: `git tag --list` — every local tag.
    /// - [`Where::Remote`]: `git ls-remote --tags <remote>` (**network**). The raw
    ///   ref lines are parsed: the `refs/tags/` prefix is stripped, the dereferenced
    ///   `^{}` peel suffix that annotated tags add is dropped, and the result is
    ///   deduplicated.
    /// - [`Where::Both`]: the union of local and remote names, deduplicated.
    ///
    /// Returns the **raw** tag names with no version parsing or ordering — callers
    /// (e.g. M12 `cmd_prepare` current-version discovery) parse them with `semver`
    /// and pick the highest. Order is unspecified.
    ///
    /// # Errors
    /// The `git` process could not be spawned, or `git ls-remote` failed (unknown
    /// remote, auth/network failure). A repo with no tags yields an empty `Vec`, not
    /// an error.
    pub fn list_tags(&self, where_: Where) -> Result<Vec<String>> {
        match where_ {
            Where::Local => self.list_local_tags(),
            Where::Remote(remote) => self.list_remote_tags(&remote),
            Where::Both(remote) => {
                let mut tags = self.list_local_tags()?;
                tags.extend(self.list_remote_tags(&remote)?);
                Ok(dedup_preserving_order(tags))
            }
        }
    }

    /// Local tag names via `git tag --list` (one per line).
    fn list_local_tags(&self) -> Result<Vec<String>> {
        let out = self.run_checked(["tag", "--list"])?;
        let tags = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect();
        Ok(tags)
    }

    /// Remote tag names via `git ls-remote --tags <remote>` (**network**). Each line
    /// is `<sha>\t<ref>`; we keep the ref, strip `refs/tags/`, drop the `^{}`
    /// annotated-tag peel suffix, and deduplicate.
    fn list_remote_tags(&self, remote: &str) -> Result<Vec<String>> {
        let out = self.run_checked(["ls-remote", "--tags", remote])?;
        let tags = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|line| line.split_whitespace().nth(1))
            .filter_map(|fqref| fqref.strip_prefix("refs/tags/"))
            .map(|name| name.strip_suffix("^{}").unwrap_or(name))
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        Ok(dedup_preserving_order(tags))
    }

    /// Local existence of a fully-qualified ref via `git show-ref --verify --quiet`.
    fn local_ref_exists(&self, fqref: &str) -> Result<bool> {
        let out = self.run(["show-ref", "--verify", "--quiet", fqref])?;
        // `show-ref --verify --quiet` exits 0 if the ref exists, 1 if it does not.
        // A code other than 0/1 (or a signal) indicates a real failure.
        match out.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            other => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                bail!(
                    "`git show-ref --verify {fqref}` failed (code {:?}): {}",
                    other,
                    stderr.trim()
                )
            }
        }
    }

    /// Remote existence of a ref via `git ls-remote <flag> <remote> <name>`.
    /// `flag` is `--heads` or `--tags`. Non-empty stdout ⇒ the ref exists.
    fn remote_ref_exists(&self, remote: &str, flag: &str, name: &str) -> Result<bool> {
        let out = self.run_checked(["ls-remote", flag, remote, name])?;
        Ok(!out.stdout.is_empty())
    }

    /// Create branch `name` at `base`, **resetting** it there if it already exists,
    /// and check it out. Implemented as `git checkout -B <name> <base>`, which is
    /// create-or-reset in one atomic call.
    ///
    /// Recreating from scratch preserves the orchestrator's "one commit on the
    /// release branch" guarantee: prior commits unique to a stale `name` become
    /// unreachable from it.
    ///
    /// Postcondition: HEAD is on `name`, pointing at `base`. Local only (no
    /// network).
    ///
    /// # Errors
    /// `base` does not resolve, or git refuses the switch; spawn/exec failure.
    pub fn create_or_reset_branch(&self, name: &str, base: &str) -> Result<()> {
        self.run_checked(["checkout", "-B", name, base])?;
        Ok(())
    }

    /// `git add -A` followed by `git commit -m <message>`, producing exactly one
    /// commit, and return the new commit's full SHA.
    ///
    /// The committer/author identity comes from [`Self::with_identity`] when set
    /// (injected as `git -c user.name/user.email`); otherwise it relies on the
    /// ambient repo/global config.
    ///
    /// # Errors
    /// Nothing staged ("nothing to commit") is reported as an error — Flow 1 is
    /// expected to have real changes, so an empty release commit signals an upstream
    /// bug. Also: missing committer identity, non-zero commit exit, spawn/exec
    /// failure. Local only (no network).
    pub fn stage_all_and_commit(&self, message: &str) -> Result<String> {
        self.run_checked(["add", "-A"])?;
        let mut args: Vec<String> = self.identity_args();
        args.extend(["commit".to_string(), "-m".to_string(), message.to_string()]);
        self.run_checked(args)
            .context("git commit failed (nothing to commit, or missing committer identity)")?;
        let out = self.run_checked(["rev-parse", "HEAD"])?;
        let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok(sha)
    }

    /// Push `branch` to `remote`, honoring [`PushOptions`].
    ///
    /// Runs `git push [--set-upstream] [--force-with-lease] <remote> <branch>`.
    /// **Network operation** — called only when the orchestrator is in `--push`
    /// mode. Authentication is whatever `git` is already configured to use; no token
    /// is passed by this module.
    ///
    /// # Errors
    /// Unknown remote, rejected push (non-fast-forward / failed lease), auth/network
    /// failure, spawn/exec failure.
    pub fn push(&self, remote: &str, branch: &str, opts: PushOptions) -> Result<()> {
        let mut args: Vec<&str> = vec!["push"];
        if opts.set_upstream {
            args.push("--set-upstream");
        }
        if opts.force {
            args.push("--force-with-lease");
        }
        args.push(remote);
        args.push(branch);
        self.run_checked(args)?;
        Ok(())
    }

    /// `git add -A` — stage every working-tree change (adds, modifications,
    /// deletions), so the staged set can be enumerated by [`Self::staged_paths`]
    /// for the GitHub-signed commit path. Local only (no network).
    pub fn stage_all(&self) -> Result<()> {
        self.run_checked(["add", "-A"])?;
        Ok(())
    }

    /// Resolve `rev` to a full commit SHA via `git rev-parse <rev>`.
    ///
    /// Used to capture the base commit the release branch was reset to, which
    /// becomes the `expectedHeadOid` of the GitHub-signed commit. Local only.
    pub fn rev_parse(&self, rev: &str) -> Result<String> {
        let out = self.run_checked(["rev-parse", rev])?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Enumerate the currently **staged** changes relative to `HEAD` as a
    /// [`StagedPaths`] (upserts vs deletions). Call [`Self::stage_all`] first.
    ///
    /// Runs `git diff --cached --name-status --no-renames -z HEAD`. `--no-renames`
    /// decomposes renames into a delete + an add (the GitHub file-changes API has no
    /// rename primitive), and `-z` gives NUL-separated `status\0path\0…` records so
    /// paths with spaces/newlines are handled verbatim. Status `D` → deletion;
    /// anything else (`A`/`M`/`T`/`C`) → upsert. Local only (no network).
    pub fn staged_paths(&self) -> Result<StagedPaths> {
        let out = self.run_checked([
            "diff",
            "--cached",
            "--name-status",
            "--no-renames",
            "-z",
            "HEAD",
        ])?;
        let raw = String::from_utf8_lossy(&out.stdout);
        let mut fields = raw.split('\0').filter(|s| !s.is_empty());
        let mut staged = StagedPaths::default();
        while let Some(status) = fields.next() {
            let Some(path) = fields.next() else { break };
            match status.chars().next() {
                Some('D') => staged.deletions.push(path.to_string()),
                Some(_) => staged.upserts.push(path.to_string()),
                None => {}
            }
        }
        Ok(staged)
    }

    /// Force-push commit `sha` to `remote`'s `branch`, creating or resetting the
    /// remote branch to point at that commit (`git push --force <remote>
    /// <sha>:refs/heads/<branch>`).
    ///
    /// Used by the signed-commit path to publish the release branch at its **base**
    /// commit before a single GitHub-signed commit is created on top of it via the
    /// API. **Network operation.** Authentication is git's ambient credential config;
    /// no token is passed here.
    pub fn push_commit_to_branch(&self, remote: &str, sha: &str, branch: &str) -> Result<()> {
        let refspec = format!("{sha}:refs/heads/{branch}");
        self.run_checked(["push", "--force", remote, &refspec])?;
        Ok(())
    }

    /// Create a git tag `name` at `target_sha`.
    ///
    /// `message`:
    /// - `Some(m)` → annotated tag (`git tag -a <name> <target_sha> -m <m>`),
    /// - `None` → lightweight tag (`git tag <name> <target_sha>`).
    ///
    /// Local only — does **not** push the tag (pushing tags / cutting GitHub
    /// releases is the forge module's job).
    ///
    /// ## Idempotency (the orphan-tag fix)
    /// Tag creation is idempotent-friendly: creating an already-existing tag is a
    /// graceful no-op, not a panic and not an error
    /// (`publish-crates-logic.md` §"The orphan-tag fix").
    ///
    /// - If the tag already exists locally, return `Ok(())` without invoking
    ///   `git tag`.
    /// - Otherwise run `git tag`; as a race guard, an "already exists" failure from
    ///   `git tag` is also mapped to `Ok(())`.
    /// - The existing tag is **not** re-pointed even if it targets a different SHA
    ///   (a no-op, per spec; re-pointing would be a destructive surprise).
    ///
    /// # Errors
    /// `target_sha` does not resolve, or git refuses for a reason other than "tag
    /// already exists"; spawn/exec failure.
    pub fn create_tag(&self, name: &str, target_sha: &str, message: Option<&str>) -> Result<()> {
        // Graceful no-op if the tag is already present locally.
        if self.tag_exists(name, Where::Local)? {
            return Ok(());
        }

        // Identity matters for the tagger of an annotated (`-a`) tag; harmless for
        // a lightweight tag.
        let mut args: Vec<String> = self.identity_args();
        match message {
            Some(msg) => {
                args.extend(["tag", "-a", name, target_sha, "-m", msg].map(String::from))
            }
            None => args.extend(["tag", name, target_sha].map(String::from)),
        };
        let out = self.run(&args)?;
        if out.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&out.stderr);
        // Race guard: another writer created the tag between our check and our
        // attempt — treat "already exists" as success.
        if stderr.contains("already exists") {
            return Ok(());
        }
        bail!(
            "`git tag {name}` failed ({}): {}",
            out.status,
            stderr.trim()
        );
    }
}

/// Deduplicate tag names while preserving first-seen order. Used to fold the `^{}`
/// peel lines of annotated tags and the local+remote union into a clean list.
fn dedup_preserving_order(tags: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    tags.into_iter()
        .filter(|t| seen.insert(t.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::TempDir;

    /// Spin up a fresh, hermetic git repo in a temp dir with a dummy identity and a
    /// single initial commit on branch `main`. Returns the tempdir (kept alive) and
    /// a `Git` bound to it. All operations are local — no network, no shared state.
    fn fresh_repo() -> (TempDir, Git) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path();

        let run = |args: &[&str]| {
            let out = Command::new("git")
                .current_dir(path)
                .args(args)
                .output()
                .expect("spawn git");
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };

        // Force a deterministic default branch name regardless of host git config.
        run(&["init", "-q", "-b", "main"]);
        run(&["config", "user.name", "Test User"]);
        run(&["config", "user.email", "test@example.com"]);
        // Keep the suite hermetic: never sign commits/tags (the host may have
        // `commit.gpgsign`/`tag.gpgsign` enabled, which fails without a usable key).
        run(&["config", "commit.gpgsign", "false"]);
        run(&["config", "tag.gpgsign", "false"]);
        // Make an initial commit so HEAD/main exist and we can branch/tag from it.
        std::fs::write(path.join("README.md"), "init\n").expect("write file");
        run(&["add", "-A"]);
        run(&["commit", "-q", "-m", "chore: init"]);

        let git = Git::new(path.to_path_buf());
        (dir, git)
    }

    fn head_sha(git: &Git) -> String {
        let out = git.run_checked(["rev-parse", "HEAD"]).unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    // --- branch_exists (local) ----------------------------------------------
    // Maps to: orchestrator Flow 1 step 1 (branch guard, local).
    #[test]
    fn branch_exists_local_true_and_false() {
        let (_d, git) = fresh_repo();
        assert!(git.branch_exists("main", Where::Local).unwrap());
        assert!(!git.branch_exists("does-not-exist", Where::Local).unwrap());
    }

    // --- create_or_reset_branch ---------------------------------------------
    // Maps to: orchestrator Flow 1 step 1 ("recreate from scratch → one commit").
    #[test]
    fn create_branch_then_branch_exists() {
        let (_d, git) = fresh_repo();
        assert!(!git.branch_exists("release-0.2.0", Where::Local).unwrap());
        git.create_or_reset_branch("release-0.2.0", "main").unwrap();
        assert!(git.branch_exists("release-0.2.0", Where::Local).unwrap());
    }

    #[test]
    fn reset_branch_recreates_from_base_dropping_prior_commits() {
        let (_d, git) = fresh_repo();
        let base = head_sha(&git);

        // Create the release branch and stack a commit on it.
        git.create_or_reset_branch("release-1", "main").unwrap();
        std::fs::write(git.repo_root().join("extra.txt"), "x\n").unwrap();
        let stacked = git
            .stage_all_and_commit("feat: stale work on release branch")
            .unwrap();
        assert_ne!(stacked, base, "a new commit should have been created");

        // Recreate from main: the branch must point back at base, dropping `stacked`.
        git.create_or_reset_branch("release-1", "main").unwrap();
        assert_eq!(
            head_sha(&git),
            base,
            "create_or_reset must reset the branch back to base (one-commit guarantee)"
        );
    }

    // --- stage_all_and_commit -----------------------------------------------
    // Maps to: orchestrator Flow 1 step 8 (single commit) + Goal 6.
    #[test]
    fn stage_all_and_commit_creates_one_commit_and_returns_sha() {
        let (_d, git) = fresh_repo();
        let before = head_sha(&git);
        std::fs::write(git.repo_root().join("a.txt"), "hello\n").unwrap();
        let sha = git.stage_all_and_commit("feat: add a.txt").unwrap();

        assert_eq!(sha.len(), 40, "should return a full 40-char SHA");
        assert_eq!(sha, head_sha(&git), "returned SHA must be the new HEAD");
        assert_ne!(sha, before, "a new commit must have been created");

        // Exactly one new commit was added (count went from 1 to 2).
        let count = git.run_checked(["rev-list", "--count", "HEAD"]).unwrap();
        assert_eq!(String::from_utf8_lossy(&count.stdout).trim(), "2");
    }

    #[test]
    fn stage_all_and_commit_errors_with_nothing_to_commit() {
        let (_d, git) = fresh_repo();
        // Clean tree: no changes staged → git commit fails → we surface an error.
        let err = git.stage_all_and_commit("chore: empty").unwrap_err();
        assert!(
            format!("{err:#}").contains("nothing to commit")
                || format!("{err:#}").to_lowercase().contains("commit"),
            "expected a commit failure, got: {err:#}"
        );
    }

    // --- tag_exists (local) -------------------------------------------------
    // Maps to: publish-crates-logic.md §Idempotency scan (a).
    #[test]
    fn tag_exists_local_true_and_false() {
        let (_d, git) = fresh_repo();
        assert!(!git.tag_exists("v0.2.0", Where::Local).unwrap());
        git.create_tag("v0.2.0", &head_sha(&git), None).unwrap();
        assert!(git.tag_exists("v0.2.0", Where::Local).unwrap());
    }

    // --- list_tags (local) --------------------------------------------------
    // Maps to: M12 cmd_prepare current-version discovery (enumerate tags, then the
    // caller parses with semver and picks the highest).
    #[test]
    fn list_tags_local_empty_repo_is_empty() {
        let (_d, git) = fresh_repo();
        let tags = git.list_tags(Where::Local).unwrap();
        assert!(tags.is_empty(), "fresh repo has no tags, got: {tags:?}");
    }

    #[test]
    fn list_tags_local_returns_all_created_tags() {
        let (_d, git) = fresh_repo();
        let sha = head_sha(&git);
        // A mix of bare-version and crate-prefixed tags, lightweight and annotated.
        git.create_tag("v0.1.0", &sha, None).unwrap();
        git.create_tag("v0.2.0", &sha, Some("release 0.2.0"))
            .unwrap();
        git.create_tag("toy-kv-v0.1.0", &sha, None).unwrap();

        let mut tags = git.list_tags(Where::Local).unwrap();
        tags.sort();
        assert_eq!(
            tags,
            vec![
                "toy-kv-v0.1.0".to_string(),
                "v0.1.0".to_string(),
                "v0.2.0".to_string(),
            ],
            "list_tags must return all tag names verbatim (no version parsing)"
        );
    }

    // --- create_tag ---------------------------------------------------------
    // Maps to: publish-crates-logic.md §Per-crate flow step 4 + §orphan-tag fix.
    #[test]
    fn create_lightweight_tag_on_sha() {
        let (_d, git) = fresh_repo();
        let sha = head_sha(&git);
        git.create_tag("v1.0.0", &sha, None).unwrap();
        // The tag resolves to the target commit.
        let out = git.run_checked(["rev-list", "-n", "1", "v1.0.0"]).unwrap();
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), sha);
    }

    #[test]
    fn create_annotated_tag_on_sha() {
        let (_d, git) = fresh_repo();
        let sha = head_sha(&git);
        git.create_tag("v1.1.0", &sha, Some("release 1.1.0"))
            .unwrap();
        assert!(git.tag_exists("v1.1.0", Where::Local).unwrap());
        // Annotated tags carry a tag object whose type is "tag".
        let out = git.run_checked(["cat-file", "-t", "v1.1.0"]).unwrap();
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "tag");
    }

    #[test]
    fn create_tag_is_idempotent_for_existing_tag() {
        let (_d, git) = fresh_repo();
        let sha = head_sha(&git);
        git.create_tag("v2.0.0", &sha, None).unwrap();
        // Second creation of the same tag must be a graceful no-op, not an error.
        git.create_tag("v2.0.0", &sha, None)
            .expect("re-creating an existing tag must be Ok (orphan-tag fix)");
        // And annotated re-creation over an existing lightweight tag is also a no-op.
        git.create_tag("v2.0.0", &sha, Some("msg"))
            .expect("re-creating an existing tag (annotated) must be Ok");
    }

    #[test]
    fn create_tag_errors_on_unresolvable_sha() {
        let (_d, git) = fresh_repo();
        // A commit-ish that does not resolve to any object in this repo.
        let err = git
            .create_tag("v3.0.0", "no-such-ref-or-object", None)
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("git tag"),
            "expected a git tag failure, got: {err:#}"
        );
        assert!(!git.tag_exists("v3.0.0", Where::Local).unwrap());
    }

    // --- injected identity (GitIdentity via with_identity) ------------------
    // release-plz-style attribution: the release commit and annotated tag are
    // authored by the configured identity (resolved from the GitHub token owner),
    // overriding the runner's ambient git config.

    fn bot_identity() -> GitIdentity {
        GitIdentity {
            name: "bot-account".to_string(),
            email: "999+bot-account@users.noreply.github.com".to_string(),
        }
    }

    #[test]
    fn config_args_builds_c_user_pairs() {
        assert_eq!(
            bot_identity().config_args(),
            vec![
                "-c".to_string(),
                "user.name=bot-account".to_string(),
                "-c".to_string(),
                "user.email=999+bot-account@users.noreply.github.com".to_string(),
            ]
        );
    }

    #[test]
    fn commit_uses_injected_identity_over_ambient() {
        // fresh_repo's ambient identity is Test User / test@example.com.
        let (_d, git) = fresh_repo();
        let git = git.with_identity(Some(bot_identity()));
        std::fs::write(git.repo_root().join("CHANGES.md"), "x\n").unwrap();
        let sha = git.stage_all_and_commit("chore: release v9.9.9").unwrap();
        // Both author and committer must be the injected identity, NOT the ambient.
        let out = git
            .run_checked(["log", "-1", "--format=%an|%ae|%cn|%ce", &sha])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "bot-account|999+bot-account@users.noreply.github.com|\
             bot-account|999+bot-account@users.noreply.github.com"
        );
    }

    #[test]
    fn annotated_tag_uses_injected_identity() {
        let (_d, git) = fresh_repo();
        let git = git.with_identity(Some(bot_identity()));
        let sha = head_sha(&git);
        git.create_tag("toy-kv-v9.9.9", &sha, Some("release 9.9.9"))
            .unwrap();
        let out = git
            .run_checked([
                "for-each-ref",
                "--format=%(taggername)|%(taggeremail)",
                "refs/tags/toy-kv-v9.9.9",
            ])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "bot-account|<999+bot-account@users.noreply.github.com>"
        );
    }

    // --- stage_all + staged_paths (signed-commit change enumeration) --------
    // Maps to: cmd_prepare signed-commit path (enumerate the release change set as
    // additions/deletions for forge::create_commit_on_branch).
    #[test]
    fn staged_paths_splits_upserts_and_deletions() {
        let (_d, git) = fresh_repo();
        // fresh_repo committed README.md. Modify it, add a new file, delete... nothing
        // yet, so add a second committed file we can then remove.
        std::fs::write(git.repo_root().join("keep.txt"), "keep\n").unwrap();
        git.stage_all_and_commit("chore: add keep.txt").unwrap();

        // Now stage: modify README.md, add new.txt, delete keep.txt.
        std::fs::write(git.repo_root().join("README.md"), "changed\n").unwrap();
        std::fs::write(git.repo_root().join("new.txt"), "new\n").unwrap();
        std::fs::remove_file(git.repo_root().join("keep.txt")).unwrap();
        git.stage_all().unwrap();

        let mut staged = git.staged_paths().unwrap();
        staged.upserts.sort();
        assert_eq!(
            staged.upserts,
            vec!["README.md".to_string(), "new.txt".to_string()]
        );
        assert_eq!(staged.deletions, vec!["keep.txt".to_string()]);
    }

    #[test]
    fn staged_paths_empty_when_clean() {
        let (_d, git) = fresh_repo();
        git.stage_all().unwrap();
        assert_eq!(git.staged_paths().unwrap(), StagedPaths::default());
    }

    #[test]
    fn rev_parse_head_matches_head_sha() {
        let (_d, git) = fresh_repo();
        assert_eq!(git.rev_parse("HEAD").unwrap(), head_sha(&git));
    }

    // --- remote/push: intentionally NOT exercised (would require network). ---
    // branch_exists/tag_exists with Where::Remote(..)/Both(..) and push() touch the
    // network and are excluded from the hermetic suite by design (see test-report).
}
