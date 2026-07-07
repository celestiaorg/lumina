//! Every `git` CLI call the release commands make. GitHub REST/GraphQL lives in
//! [`crate::forge`]; everything shelling out to `git` lives here.
//!
//! Every method runs `git` in [`Git::repo_root`], so behavior never depends on the
//! process's current directory. Tokens are never handled here; the remote calls rely
//! on git's ambient credential configuration.

use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// Selects which side(s) of an existence check to consult.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Where {
    /// Only the local repository (no network).
    Local,
    /// Only the named remote (network).
    Remote(String),
    /// Local **or** the named remote. Local is checked first and short-circuits, so
    /// a local hit avoids any network call.
    Both(String),
}

/// Staged changes relative to `HEAD`, split into upserts and deletions. Feeds the
/// GitHub-signed commit path. Renames are decomposed into a deletion + an addition
/// (the diff is taken `--no-renames`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagedPaths {
    /// Added/modified/type-changed paths, repo-relative.
    pub upserts: Vec<String>,
    /// Deleted paths, repo-relative.
    pub deletions: Vec<String>,
}

/// Options controlling [`Git::push`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushOptions {
    /// Pass `--set-upstream` so the local branch tracks the pushed remote branch.
    pub set_upstream: bool,
    /// Force with `--force-with-lease` (never a bare `--force`), which refuses to
    /// clobber a remote that moved unexpectedly.
    pub force: bool,
}

/// A git author/committer identity injected into the release commit and annotated
/// tags via `git -c user.name=… -c user.email=…`, so they are attributed to a
/// specific account regardless of the runner's ambient git config.
#[derive(Debug, Clone)]
pub struct GitIdentity {
    pub name: String,
    pub email: String,
}

impl GitIdentity {
    /// The `-c user.name=… -c user.email=…` prefix args, overriding repo/global
    /// `user.*` config for a single command.
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
    /// Identity for `commit` / annotated `tag`. `None` ⇒ ambient git config.
    identity: Option<GitIdentity>,
}

impl Git {
    /// Create a `Git` bound to `repo_root`. Performs no I/O or validation.
    pub fn new(repo_root: impl Into<PathBuf>) -> Self {
        Self {
            repo_root: repo_root.into(),
            identity: None,
        }
    }

    /// Set the identity used for `commit` and annotated `tag`. `None` leaves the
    /// ambient git config in charge.
    pub fn with_identity(mut self, identity: Option<GitIdentity>) -> Self {
        self.identity = identity;
        self
    }

    /// The `-c user.name/user.email` args for the configured identity, or empty.
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

    /// Run `git` in [`Self::repo_root`], capturing output. Callers interpret the
    /// exit code; errors only when the process could not be spawned.
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

    /// Run `git`, requiring a zero exit; on failure return an error with the
    /// subcommand and stderr.
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

    /// Check whether a branch exists (see [`Where`]). An absent ref is never an
    /// error; the remote side touches the network.
    pub fn branch_exists(&self, name: &str, where_: Where) -> Result<bool> {
        self.ref_exists("refs/heads/", "--heads", name, where_)
    }

    /// Shared existence check for a ref in `refs_prefix` (`refs/heads/` or
    /// `refs/tags/`). `ls_flag` is the `git ls-remote` selector. [`Where::Both`]
    /// checks local first, short-circuiting the network.
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

    /// Check whether a tag exists (see [`Self::branch_exists`], tag namespace).
    pub fn tag_exists(&self, name: &str, where_: Where) -> Result<bool> {
        self.ref_exists("refs/tags/", "--tags", name, where_)
    }

    /// List tag names (local, remote via network, or the deduplicated union). Returns
    /// raw names with no version parsing or ordering; callers parse them. A repo with
    /// no tags yields an empty `Vec`.
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

    /// Local tag names via `git tag --list`.
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

    /// Remote tag names via `git ls-remote --tags <remote>` (network). Strips
    /// `refs/tags/`, drops the `^{}` annotated-tag peel suffix, and deduplicates.
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
        // Exit 0 = ref exists, 1 = absent; anything else is a real failure.
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

    /// Remote existence via `git ls-remote <flag> <remote> <name>`. Non-empty stdout
    /// ⇒ the ref exists.
    fn remote_ref_exists(&self, remote: &str, flag: &str, name: &str) -> Result<bool> {
        let out = self.run_checked(["ls-remote", flag, remote, name])?;
        Ok(!out.stdout.is_empty())
    }

    /// Create branch `name` at `base`, resetting it there if it already exists, and
    /// check it out (`git checkout -B`). Recreating from scratch preserves the
    /// one-commit-on-the-release-branch guarantee. Local only.
    pub fn create_or_reset_branch(&self, name: &str, base: &str) -> Result<()> {
        self.run_checked(["checkout", "-B", name, base])?;
        Ok(())
    }

    /// `git add -A` then `git commit -m <message>`, producing exactly one commit, and
    /// return its full SHA. Identity comes from [`Self::with_identity`] when set.
    /// Nothing staged is reported as an error. Local only.
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

    /// Push `branch` to `remote`, honoring [`PushOptions`]
    /// (`git push [--set-upstream] [--force-with-lease] <remote> <branch>`). Network
    /// operation; uses git's ambient credentials.
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

    /// `git add -A` — stage every working-tree change, so [`Self::staged_paths`] can
    /// enumerate it. Local only.
    pub fn stage_all(&self) -> Result<()> {
        self.run_checked(["add", "-A"])?;
        Ok(())
    }

    /// Resolve `rev` to a full commit SHA via `git rev-parse <rev>`. Used to capture
    /// the base commit that becomes the `expectedHeadOid` of the signed commit.
    /// Local only.
    pub fn rev_parse(&self, rev: &str) -> Result<String> {
        let out = self.run_checked(["rev-parse", rev])?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Enumerate the staged changes relative to `HEAD` as a [`StagedPaths`]. Call
    /// [`Self::stage_all`] first. `--no-renames` decomposes renames into a delete + an
    /// add (the GitHub file-changes API has no rename primitive), and `-z` gives
    /// NUL-separated records so unusual paths survive. Status `D` → deletion, else →
    /// upsert. Local only.
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

    /// Force-push commit `sha` to `remote`'s `branch` (`git push --force <remote>
    /// <sha>:refs/heads/<branch>`), publishing the release branch at its base commit
    /// before the signed commit is created on top via the API. Network operation.
    pub fn push_commit_to_branch(&self, remote: &str, sha: &str, branch: &str) -> Result<()> {
        let refspec = format!("{sha}:refs/heads/{branch}");
        self.run_checked(["push", "--force", remote, &refspec])?;
        Ok(())
    }

    /// Create a git tag `name` at `target_sha`: `Some(m)` → annotated
    /// (`git tag -a … -m`), `None` → lightweight. Local only — does not push.
    ///
    /// Idempotent: an already-existing tag is a graceful no-op (checked up front, and
    /// an "already exists" failure from `git tag` is mapped to `Ok(())` as a race
    /// guard). The existing tag is never re-pointed. Errors if `target_sha` does not
    /// resolve.
    pub fn create_tag(&self, name: &str, target_sha: &str, message: Option<&str>) -> Result<()> {
        if self.tag_exists(name, Where::Local)? {
            return Ok(());
        }

        // Identity matters for the tagger of an annotated tag; harmless otherwise.
        let mut args: Vec<String> = self.identity_args();
        match message {
            Some(msg) => args.extend(["tag", "-a", name, target_sha, "-m", msg].map(String::from)),
            None => args.extend(["tag", name, target_sha].map(String::from)),
        };
        let out = self.run(&args)?;
        if out.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&out.stderr);
        // Race guard: treat "already exists" as success.
        if stderr.contains("already exists") {
            return Ok(());
        }
        bail!(
            "`git tag {name}` failed ({}): {}",
            out.status,
            stderr.trim()
        );
    }

    /// The repository's default branch — the release-branch base and PR base. Never
    /// returns `"HEAD"` (an invalid PR base). Tried in order:
    /// `refs/remotes/origin/HEAD` → set it from the remote and re-read → the current
    /// branch → `"main"`.
    pub fn default_branch(&self) -> String {
        if let Some(b) = self.origin_head_branch() {
            return b;
        }
        let _ = self.run(["remote", "set-head", "origin", "--auto"]);
        if let Some(b) = self.origin_head_branch() {
            return b;
        }
        match self.current_branch() {
            Some(cur) if cur != "HEAD" => cur,
            _ => "main".to_string(),
        }
    }

    /// `refs/remotes/origin/HEAD` as a plain branch name, or `None` if unset.
    fn origin_head_branch(&self) -> Option<String> {
        let out = self.capture_ok(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"])?;
        let s = out.trim();
        let base = s.strip_prefix("origin/").unwrap_or(s);
        (!base.is_empty()).then(|| base.to_string())
    }

    /// The currently checked-out branch, or `None` when detached.
    fn current_branch(&self) -> Option<String> {
        let s = self.capture_ok(["rev-parse", "--abbrev-ref", "HEAD"])?;
        let s = s.trim().to_string();
        (!s.is_empty()).then_some(s)
    }

    /// The `origin` remote URL, or `None` if there is no origin.
    pub fn origin_url(&self) -> Option<String> {
        let url = self.capture_ok(["config", "--get", "remote.origin.url"])?;
        let url = url.trim();
        (!url.is_empty()).then(|| url.to_string())
    }

    /// Repo-relative paths of every tracked file under `dir` (`git ls-files -z`).
    pub fn tracked_files(&self, dir: &str) -> Result<Vec<String>> {
        let out = self.run_checked(["ls-files", "-z", "--", dir])?;
        Ok(String::from_utf8_lossy(&out.stdout)
            .split('\0')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// Commit hashes (newest-first) that touched `file`, following it across renames
    /// (`git log [range] --no-merges --follow --format=%H -- <file>`).
    pub fn commits_touching(&self, range: Option<&str>, file: &str) -> Result<Vec<String>> {
        let out = self.log(range, &["--follow", "--format=%H", "--", file])?;
        Ok(out
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// Raw `git log [range] --no-merges <tail…>` stdout. `range` is an optional
    /// `<ref>..HEAD`; `None` walks all history.
    pub fn log(&self, range: Option<&str>, tail: &[&str]) -> Result<String> {
        let mut args: Vec<&str> = vec!["log"];
        if let Some(range) = range {
            args.push(range);
        }
        args.push("--no-merges");
        args.extend_from_slice(tail);
        let out = self.run_checked(args)?;
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Run `git <args>`, returning stdout on a zero exit, else `None`. For
    /// best-effort local reads.
    fn capture_ok<I, S>(&self, args: I) -> Option<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let out = self.run(args).ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }
}

/// Deduplicate tag names while preserving first-seen order.
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

    /// A fresh, hermetic git repo in a temp dir with a dummy identity and one initial
    /// commit on branch `main`. Returns the (kept-alive) tempdir and a bound `Git`.
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

        // Deterministic default branch regardless of host git config.
        run(&["init", "-q", "-b", "main"]);
        run(&["config", "user.name", "Test User"]);
        run(&["config", "user.email", "test@example.com"]);
        // Never sign: the host may have gpgsign enabled without a usable key.
        run(&["config", "commit.gpgsign", "false"]);
        run(&["config", "tag.gpgsign", "false"]);
        // Initial commit so HEAD/main exist.
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

    #[test]
    fn branch_exists_local_true_and_false() {
        let (_d, git) = fresh_repo();
        assert!(git.branch_exists("main", Where::Local).unwrap());
        assert!(!git.branch_exists("does-not-exist", Where::Local).unwrap());
    }

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

    #[test]
    fn tag_exists_local_true_and_false() {
        let (_d, git) = fresh_repo();
        assert!(!git.tag_exists("v0.2.0", Where::Local).unwrap());
        git.create_tag("v0.2.0", &head_sha(&git), None).unwrap();
        assert!(git.tag_exists("v0.2.0", Where::Local).unwrap());
    }

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
        git.create_tag("lumina-v0.1.0", &sha, None).unwrap();

        let mut tags = git.list_tags(Where::Local).unwrap();
        tags.sort();
        assert_eq!(
            tags,
            vec![
                "lumina-v0.1.0".to_string(),
                "v0.1.0".to_string(),
                "v0.2.0".to_string(),
            ],
            "list_tags must return all tag names verbatim (no version parsing)"
        );
    }

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

    // The release commit and annotated tag are authored by the configured identity,
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
        git.create_tag("lumina-v9.9.9", &sha, Some("release 9.9.9"))
            .unwrap();
        let out = git
            .run_checked([
                "for-each-ref",
                "--format=%(taggername)|%(taggeremail)",
                "refs/tags/lumina-v9.9.9",
            ])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "bot-account|<999+bot-account@users.noreply.github.com>"
        );
    }

    #[test]
    fn staged_paths_splits_upserts_and_deletions() {
        let (_d, git) = fresh_repo();
        // Add a second committed file we can later remove.
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

    // remote/push paths (Where::Remote/Both, push()) touch the network and are
    // excluded from the hermetic suite by design.
}
