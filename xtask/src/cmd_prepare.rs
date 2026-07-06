//! M12 `cmd_prepare` — orchestration of **Flow 1: Prepare release PR**.
//!
//! Implements [`orchestrator.md` §Flow 1] steps 1–10: branch guard, current-version
//! discovery (git tags + registry), exact legal-successor validation, the
//! format-preserving workspace version bump, per-crate breaking analysis (diagnostic)
//! and changelogs, the optional npm wrapper update, the single release commit, the
//! PR-body assembly, and — only under `--push` — pushing the branch and opening or
//! updating the PR.
//!
//! All real work is delegated to the dependency modules (`config`, `workspace`,
//! `version`, `commit`, `gitops`, `cargoops`, `forge`, `npmops`, `changelog`,
//! `breaking`, `prbody`); this module owns only the wiring and the pure helpers below.
//!
//! The two orchestrator-fixed conventions (shared with M13 `cmd_release`):
//! - per-crate tag name `format!("{crate_name}-v{version}")`;
//! - a tag counts as a version tag iff it matches
//!   `^.+-v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$`, parsing the substring after the
//!   final `-v` as semver.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use semver::Version;

use crate::gitops::{Git, Where};
use crate::prbody::PackageReport;

/// The default release-branch prefix when neither the option nor config supplies one.
const DEFAULT_BRANCH_PREFIX: &str = "release-";

/// Fully-resolved inputs to a prepare run (the CLI / M14 turns prompts + flags into
/// this).
#[derive(Debug, Clone)]
pub struct PrepareOptions {
    /// Release-branch prefix. Resolution: this → `config.defaults.branch_prefix` →
    /// `"release-"`.
    pub branch_prefix: Option<String>,
    /// The requested next version (already resolved by the CLI). Validated in step 3.
    pub version: String,
    /// Auto-confirm destructive prompts (the branch-delete prompt in step 1).
    pub yes: bool,
    /// Opt into remote actions (the remote branch check in step 1 and step 10).
    pub push: bool,
    /// The **name** of the env var holding the GitHub token. Only used under `push`.
    pub github_token_env: String,
    /// Override the release date (`YYYY-MM-DD`); `None` ⇒ today, UTC.
    pub date: Option<String>,
    /// If set, also write the rendered PR description to this Markdown file
    /// (independent of `push`) — useful for previewing a local test run.
    pub pr_body_out: Option<PathBuf>,
    /// Skip the pre-commit publishability preflight (`cargo publish --workspace
    /// --dry-run`).
    pub no_verify: bool,
}

/// The result of a successful prepare run.
#[derive(Debug, Clone)]
pub struct PrepareOutcome {
    /// The release branch that was created/reset (`"<prefix><version>"`).
    pub branch: String,
    /// SHA of the single release commit (step 8).
    pub commit_sha: String,
    /// `true` iff the branch was pushed and the PR opened/updated (only under `push`).
    pub pushed: bool,
    /// The PR html_url when `pushed`; `None` otherwise.
    pub pr_url: Option<String>,
    /// The discovered current version (step 2); `None` for a first release.
    pub current_version: Option<String>,
    /// The per-crate reports fed to M11 (one per publishable crate).
    pub packages: Vec<PackageReport>,
}

/// Execute Flow 1 steps 1–10. See the module docs / `artifacts/M12/contract.md` for
/// the per-step behavioral contract.
pub fn run(repo_root: &Path, opts: &PrepareOptions) -> Result<PrepareOutcome> {
    let config = crate::config::load(repo_root).context("loading release.toml")?;
    let ws = crate::workspace::Workspace::discover(repo_root).context("discovering workspace")?;
    // When pushing (the CI path) attribute the release commit to the GitHub token
    // owner, the way release-plz does (orchestrator.md §Actions wiring). A local,
    // no-push run keeps the developer's ambient git identity.
    let identity = if opts.push {
        crate::forge::token_committer(&opts.github_token_env)
            .context("resolving the release-commit author from the GitHub token")?
    } else {
        None
    };
    let git = Git::new(repo_root).with_identity(identity);
    let date = opts
        .date
        .clone()
        .unwrap_or_else(crate::changelog::today_utc);

    let prefix = resolve_branch_prefix(
        opts.branch_prefix.as_deref(),
        config.defaults.branch_prefix.as_deref(),
    );
    let branch = target_branch(&prefix, &opts.version);

    // Under --push, ref existence checks consult the remote as well as local;
    // otherwise they stay local-only. Used for both the branch and the tag scan.
    let remote_scope = if opts.push {
        Where::Both("origin".to_string())
    } else {
        Where::Local
    };
    if git
        .branch_exists(&branch, remote_scope.clone())
        .with_context(|| format!("checking whether release branch `{branch}` exists"))?
    {
        let prompt = format!(
            "Release branch `{branch}` already exists. Delete and recreate it from scratch?"
        );
        if !confirm(&prompt, opts.yes)? {
            return Err(anyhow!(
                "refusing to overwrite existing release branch `{branch}` (declined). \
                 Re-run with --yes to recreate it, or delete it manually."
            ));
        }
    }
    let base = git_default_branch(repo_root);
    git.create_or_reset_branch(&branch, &base)
        .with_context(|| format!("creating release branch `{branch}` at `{base}`"))?;

    let tags = git
        .list_tags(remote_scope)
        .context("listing git tags for current-version discovery")?;
    let tag_versions: Vec<Version> = tags.iter().filter_map(|t| parse_version_tag(t)).collect();

    let mut registry_versions: Vec<Version> = Vec::new();
    for crate_info in &ws.crates {
        if let Some(v) = crate::cargoops::max_published_version(&crate_info.name)
            .with_context(|| format!("querying published versions of `{}`", crate_info.name))?
        {
            registry_versions.push(v);
        }
    }
    let current = select_current_version(&tag_versions, &registry_versions);

    match &current {
        Some(cur) => {
            crate::version::is_legal_successor(&cur.to_string(), &opts.version).map_err(|e| {
                anyhow!(
                    "requested version `{}` is not a legal successor of current `{}`: {e}",
                    opts.version,
                    cur
                )
            })?;
        }
        None => {
            // First release: accept any parseable semver (design decision — see
            // contract Spec question Q1).
            first_release_accept(&opts.version).map_err(|e| {
                anyhow!(
                    "requested first-release version `{}` is not valid semver: {e}",
                    opts.version
                )
            })?;
        }
    }

    let manifest_path = repo_root.join("Cargo.toml");
    let manifest_src = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let bumped = apply_version_bump(&manifest_src, &opts.version)
        .with_context(|| format!("bumping workspace version in {}", manifest_path.display()))?;
    std::fs::write(&manifest_path, bumped)
        .with_context(|| format!("writing {}", manifest_path.display()))?;

    // --- Step 4.5: publishability preflight (before we commit) -------------------
    // With the versions bumped (but nothing committed yet), verify the whole
    // workspace can actually be packaged + published. This catches problems like a
    // yanked dependency now, rather than half-way through the real release. Runs on
    // the dirty tree via `--allow-dirty`; skip with `--no-verify`.
    if !opts.no_verify {
        println!("Running publishability preflight (cargo publish --workspace --dry-run)…");
        crate::cargoops::verify_publishable(repo_root)
            .context("publishability preflight failed — aborting before the release commit")?;
    }

    let publishable: Vec<crate::workspace::CrateInfo> =
        ws.publish_order()?.into_iter().cloned().collect();

    let mut reports: Vec<PackageReport> = Vec::with_capacity(publishable.len());
    for crate_info in &publishable {
        let last_ref = current
            .as_ref()
            .map(|cur| crate::cmd_release::tag_name(&crate_info.name, &cur.to_string()));
        let dir = crate::commit::pathspec(repo_root, &crate_info.manifest_dir);

        // Collect the package's commits once and share them with both breaking
        // analysis and changelog rendering (M9 no longer re-collects).
        let range = last_ref.as_ref().map(|r| format!("{r}..HEAD"));
        let commits = crate::commit::collect(repo_root, range.as_deref(), &dir)
            .with_context(|| format!("collecting commits for `{}`", crate_info.name))?;
        let parsed: Vec<crate::commit::ParsedCommit> =
            commits.iter().map(crate::commit::parse).collect();
        let breaking = crate::breaking::analyze(repo_root, crate_info, current.as_ref(), &parsed);

        let changelog = crate::changelog::generate(crate_info, &opts.version, &date, &commits)
            .with_context(|| format!("generating changelog for `{}`", crate_info.name))?;

        // Step 9 (assembled here while the per-crate data is in hand).
        // The PR body wants the entry heading text without the `## ` prefix.
        let entry_title = changelog
            .file_entry
            .lines()
            .next()
            .unwrap_or("")
            .trim_start_matches("## ")
            .to_string();
        let report = build_package_report(
            &crate_info.name,
            current.as_ref(),
            &opts.version,
            &entry_title,
            breaking.has_breaking,
            breaking.reason.as_deref().unwrap_or(""),
            &changelog.body_only,
        );
        reports.push(report);
    }

    // --- Step 7: npm update (only if a component is configured) -------------------
    for component in &config.npm {
        let npm = crate::npmops::NpmComponent {
            wasm_crate: component.wasm_crate.clone(),
            package_dir: component.package_dir.clone(),
        };
        crate::npmops::update_for_pr(&npm, &opts.version)
            .with_context(|| format!("updating npm wrapper `{}`", component.package_dir))?;
    }

    // --- Step 9: PR body (assembled before the commit) ---------------------------
    // The signed-commit path needs the title/body, and neither depends on the commit
    // SHA, so render them first.
    let commit_message = format!("chore: release v{}", opts.version);
    let title = crate::prbody::title(&reports);
    let body = crate::prbody::body(&reports).context("rendering the PR body")?;
    println!("{body}");

    // Optionally persist the PR description to a Markdown file (e.g. for a local,
    // no-`--push` preview).
    if let Some(path) = &opts.pr_body_out {
        std::fs::write(path, &body)
            .with_context(|| format!("writing PR description to {}", path.display()))?;
        eprintln!("PR description written to {}", path.display());
    }

    // --- Steps 8 + 10: the release commit, then (under --push) the PR -------------
    // Under `--push` the commit is created through GitHub's `createCommitOnBranch`
    // API so it is GitHub-signed ("Verified"): the release branch is first pushed at
    // its base commit, then a single signed commit carrying the whole release change
    // set is created on top, then the PR is opened/updated. A local (no-`--push`) run
    // keeps a plain local commit — a preview that cannot be signed without a key.
    let mut pushed = false;
    let mut pr_url = None;
    let commit_sha = if opts.push {
        // The base commit the branch was reset to — still HEAD, since nothing has
        // been committed locally. It becomes the signed commit's expectedHeadOid.
        let base_sha = git
            .rev_parse("HEAD")
            .context("resolving the base commit SHA")?;

        // Enumerate the release change set (version bump, changelogs, npm mirror) as
        // additions + deletions for the API commit.
        git.stage_all().context("staging the release changes")?;
        let staged = git
            .staged_paths()
            .context("enumerating the staged release changes")?;
        if staged.upserts.is_empty() && staged.deletions.is_empty() {
            return Err(anyhow!(
                "no changes to release — the release commit would be empty"
            ));
        }
        let mut additions = Vec::with_capacity(staged.upserts.len());
        for path in &staged.upserts {
            let contents = std::fs::read(repo_root.join(path))
                .with_context(|| format!("reading staged file `{path}`"))?;
            additions.push(crate::forge::FileAddition {
                path: path.clone(),
                contents,
            });
        }

        let repo =
            crate::repo::derive(repo_root).context("deriving the GitHub owner/name for the PR")?;

        // Publish the branch at its base commit, then create one signed commit on top.
        git.push_commit_to_branch("origin", &base_sha, &branch)
            .with_context(|| format!("pushing release branch `{branch}` base to origin"))?;
        let sha = crate::forge::create_commit_on_branch(
            &repo,
            &branch,
            &base_sha,
            &commit_message,
            &additions,
            &staged.deletions,
            &opts.github_token_env,
        )
        .context("creating the GitHub-signed release commit")?;

        let pr = crate::forge::open_or_update_pr(
            &repo,
            &branch,
            &base,
            &title,
            &body,
            &opts.github_token_env,
        )
        .context("opening or updating the release PR")?;
        pushed = true;
        pr_url = Some(pr.url);
        sha
    } else {
        git.stage_all_and_commit(&commit_message)
            .context("creating the single release commit")?
    };

    Ok(PrepareOutcome {
        branch,
        commit_sha,
        pushed,
        pr_url,
        current_version: current.map(|v| v.to_string()),
        packages: reports,
    })
}

// ============================================================================
// Pure / decomposable helpers (unit-tested without network/stdin/registry).
// ============================================================================

/// Resolve the branch prefix: explicit option → config default → `"release-"`.
fn resolve_branch_prefix(opt: Option<&str>, cfg: Option<&str>) -> String {
    opt.or(cfg).unwrap_or(DEFAULT_BRANCH_PREFIX).to_string()
}

/// The release-branch name `"<prefix><version>"`.
fn target_branch(prefix: &str, version: &str) -> String {
    format!("{prefix}{version}")
}

/// Parse a tag under the version-tag convention: a tag counts iff it matches
/// `^.+-v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$`. Returns the parsed semver of the
/// substring after the **final** `-v`, or `None` if the tag does not match / does not
/// parse. No regex crate — a hand-rolled matcher.
fn parse_version_tag(tag: &str) -> Option<Version> {
    // Find the final `-v` separator; there must be at least one char of prefix
    // before it (the `^.+` requirement).
    let sep = tag.rfind("-v")?;
    if sep == 0 {
        return None; // nothing before `-v`
    }
    let ver = &tag[sep + 2..];
    if ver.is_empty() {
        return None;
    }
    // The convention's `<ver>` group is a strict semver core + optional pre-release;
    // semver::Version::parse enforces the X.Y.Z(-pre)(+build) grammar. We additionally
    // reject build metadata (`+...`) since the convention regex does not allow it.
    if ver.contains('+') {
        return None;
    }
    Version::parse(ver).ok()
}

/// The current version = the max semver across version tags and published registry
/// versions, or `None` when both are empty (first release).
fn select_current_version(
    tag_versions: &[Version],
    registry_versions: &[Version],
) -> Option<Version> {
    tag_versions
        .iter()
        .chain(registry_versions.iter())
        .max()
        .cloned()
}

/// First-release acceptance rule: accept any parseable semver (reject only
/// un-parseable). See contract Spec question Q1.
fn first_release_accept(requested: &str) -> Result<Version, crate::version::VersionError> {
    crate::version::parse_next(requested)
}

/// Format-preserving workspace-version bump applied to a manifest's text.
///
/// Sets `[workspace.package].version` to `new_version`, and rewrites every
/// intra-workspace `version = "=<old>"` pin in `[workspace.dependencies]` to
/// `"=<new>"` (both the inline-table `{ version = "=…", path = "…" }` form and a bare
/// `version = "=…"` string). All other formatting/comments/order are preserved by
/// `toml_edit`.
fn apply_version_bump(manifest_src: &str, new_version: &str) -> Result<String> {
    let mut doc = manifest_src
        .parse::<toml_edit::DocumentMut>()
        .context("parsing root Cargo.toml as TOML")?;

    // [workspace.package].version
    let wp_version = doc
        .get_mut("workspace")
        .and_then(|w| w.as_table_like_mut())
        .and_then(|w| w.get_mut("package"))
        .and_then(|p| p.as_table_like_mut())
        .and_then(|p| p.get_mut("version"))
        .ok_or_else(|| anyhow!("root Cargo.toml has no [workspace.package].version to bump"))?;
    set_string_value(wp_version, new_version);

    // [workspace.dependencies] — repin every `=<old>` exact pin to `=<new>`.
    if let Some(deps) = doc
        .get_mut("workspace")
        .and_then(|w| w.as_table_like_mut())
        .and_then(|w| w.get_mut("dependencies"))
        .and_then(|d| d.as_table_like_mut())
    {
        // Collect keys first to avoid borrowing `deps` mutably while iterating.
        let keys: Vec<String> = deps.iter().map(|(k, _)| k.to_string()).collect();
        for key in keys {
            if let Some(item) = deps.get_mut(&key) {
                repin_exact_dep(item, new_version);
            }
        }
    }

    Ok(doc.to_string())
}

/// If `item` carries a `version` field that is an exact `=`-prefixed pin (or is itself
/// such a bare string), rewrite it to `=<new_version>`. Non-`=` pins and non-version
/// items are left untouched.
fn repin_exact_dep(item: &mut toml_edit::Item, new_version: &str) {
    // Inline-table / table form: `{ version = "=x", path = "…" }`.
    if let Some(tbl) = item.as_table_like_mut() {
        if let Some(v) = tbl.get_mut("version") {
            if v.as_str().is_some_and(|s| s.starts_with('=')) {
                set_string_value(v, &format!("={new_version}"));
            }
        }
        return;
    }
    // Bare string form: `dep = "=x"`.
    if item.as_str().is_some_and(|s| s.starts_with('=')) {
        set_string_value(item, &format!("={new_version}"));
    }
}

/// Replace the string content of a TOML value-item in place, **preserving** its
/// surrounding decor (whitespace + trailing comment). Used instead of
/// `*item = toml_edit::value(..)`, which would discard the decor.
fn set_string_value(item: &mut toml_edit::Item, new: &str) {
    if let Some(val) = item.as_value_mut() {
        // Reuse the existing value's decor so a trailing `# comment` survives.
        let decor = val.decor().clone();
        let mut replacement = toml_edit::Value::from(new);
        *replacement.decor_mut() = decor;
        *val = replacement;
    } else {
        *item = toml_edit::value(new);
    }
}

/// Assemble one M11 `PackageReport` from the per-crate data.
fn build_package_report(
    name: &str,
    prev: Option<&Version>,
    new_version: &str,
    title: &str,
    breaking: bool,
    reason: &str,
    changelog_body: &str,
) -> PackageReport {
    PackageReport {
        name: name.to_string(),
        // Empty for a first release ⇒ the PR body omits the `prev ->` arrow.
        previous_version: prev.map(|v| v.to_string()).unwrap_or_default(),
        new_version: new_version.to_string(),
        title: title.to_string(),
        breaking,
        breaking_reason: reason.to_string(),
        changelog_body: changelog_body.to_string(),
    }
}

/// Decision logic for an interactive y/n confirmation, split out from stdin so it is
/// unit-testable. `yes` short-circuits to `true`; otherwise an input starting with
/// `y`/`Y` (case-insensitive) is `true`, anything else `false`.
fn confirm_decision(yes: bool, input: &str) -> bool {
    if yes {
        return true;
    }
    matches!(input.trim().chars().next(), Some('y') | Some('Y'))
}

/// Interactive confirmation: `yes ⇒ Ok(true)`; otherwise prints `prompt` and reads a
/// y/n answer from stdin. The only stdin seam in the module.
fn confirm(prompt: &str, yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    print!("{prompt} [y/N] ");
    std::io::stdout().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading confirmation from stdin")?;
    Ok(confirm_decision(false, &line))
}

// ============================================================================
// I/O seams (git default-branch lookup). Repo derivation lives in `crate::repo`.
// ============================================================================

/// Best-effort resolution of the default branch the release is cut from. Tries
/// `origin/HEAD` (`git symbolic-ref --short refs/remotes/origin/HEAD`); falls back to
/// `HEAD` when no remote is configured (e.g. a local fixture with no `origin`). See
/// contract Spec question Q2. Shells `git` directly (orchestration glue, not an M5
/// primitive — M5's contract does not expose a default-branch lookup).
/// The repository's default branch name, used as the release-branch base *and* the
/// PR base. Must resolve to a real branch name: `"HEAD"` is a valid commit-ish for
/// branching but an **invalid PR base** (GitHub rejects it `422 field: base`).
///
/// Layered so it works both locally and on a fresh CI checkout:
/// 1. `refs/remotes/origin/HEAD` — set by a normal `git clone` (local dev).
/// 2. `git remote set-head origin --auto` then re-read — `actions/checkout` does
///    **not** set that ref, so ask the remote for its default and set it (network).
/// 3. the currently checked-out branch — resolved before we switch to the release
///    branch, so on a CI runner this is the base branch (e.g. `main`); no network.
/// 4. `"main"` — conventional last resort, still a real branch name.
fn git_default_branch(repo_root: &Path) -> String {
    if let Some(b) = origin_head_branch(repo_root) {
        return b;
    }
    let _ = git_capture(repo_root, &["remote", "set-head", "origin", "--auto"]);
    if let Some(b) = origin_head_branch(repo_root) {
        return b;
    }
    if let Some(cur) = current_branch(repo_root) {
        if cur != "HEAD" && !cur.is_empty() {
            return cur;
        }
    }
    "main".to_string()
}

/// Read `refs/remotes/origin/HEAD` as a plain branch name (`origin/` stripped), or
/// `None` if the symbolic ref is unset.
fn origin_head_branch(repo_root: &Path) -> Option<String> {
    let out = git_capture(repo_root, &["symbolic-ref", "--short", "refs/remotes/origin/HEAD"])?;
    let s = out.trim();
    let base = s.strip_prefix("origin/").unwrap_or(s);
    (!base.is_empty()).then(|| base.to_string())
}

/// The currently checked-out branch, or `None`/`"HEAD"` when detached.
fn current_branch(repo_root: &Path) -> Option<String> {
    let out = git_capture(repo_root, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let s = out.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// Run `git <args>` in `repo_root`, returning stdout as a `String` on a zero exit.
fn git_capture(repo_root: &Path, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git")
        .current_dir(repo_root)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Version {
        Version::parse(s).unwrap()
    }

    // --- Default-branch detection (base for the release branch + PR) -------------
    // Regression: `actions/checkout` leaves `refs/remotes/origin/HEAD` unset, which
    // used to fall back to the literal "HEAD" — a valid branch-from ref but an
    // INVALID PR base (`422 field: base`). Detection must yield a real branch name.

    #[test]
    fn git_default_branch_falls_back_to_current_branch_without_origin_head() {
        // A repo with a commit on `main` and NO origin/HEAD (mirrors a CI checkout).
        let dir = tempfile::TempDir::new().unwrap();
        let p = dir.path();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .current_dir(p)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        };
        git(&["init", "-q", "-b", "main"]);
        git(&["config", "user.name", "t"]);
        git(&["config", "user.email", "t@e.com"]);
        std::fs::write(p.join("f"), "x").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "init"]);

        // No origin/HEAD and no reachable remote → falls through to the current
        // branch, never the invalid literal "HEAD".
        let base = git_default_branch(p);
        assert_eq!(base, "main");
        assert_ne!(base, "HEAD");
    }

    // --- Branch-name computation (Flow 1 step 1) ---------------------------------

    #[test]
    fn target_branch_concatenates_prefix_and_version() {
        assert_eq!(target_branch("release-", "0.2.0"), "release-0.2.0");
        assert_eq!(target_branch("rel/", "1.0.0-rc.1"), "rel/1.0.0-rc.1");
    }

    #[test]
    fn resolve_branch_prefix_precedence() {
        // explicit option wins
        assert_eq!(resolve_branch_prefix(Some("opt-"), Some("cfg-")), "opt-");
        // falls back to config default
        assert_eq!(resolve_branch_prefix(None, Some("cfg-")), "cfg-");
        // falls back to the hard default
        assert_eq!(resolve_branch_prefix(None, None), "release-");
        assert_eq!(resolve_branch_prefix(None, None), DEFAULT_BRANCH_PREFIX);
    }

    // --- Version-tag convention (orchestrator-fixed) -----------------------------
    // The tag *name* convention (`{crate}-v{version}`) is `cmd_release::tag_name`,
    // tested there; here we test that the *parser* round-trips it.

    #[test]
    fn parse_version_tag_matches_convention() {
        // crate-name with internal hyphens: split on the FINAL `-v`.
        assert_eq!(parse_version_tag("toy-kv-utils-v0.2.0"), Some(v("0.2.0")));
        assert_eq!(parse_version_tag("toy-kv-v0.2.0"), Some(v("0.2.0")));
        // prerelease is captured.
        assert_eq!(
            parse_version_tag("toy-kv-v1.0.0-rc.1"),
            Some(v("1.0.0-rc.1"))
        );
        // round-trips the convention tag.
        let tag = crate::cmd_release::tag_name("a-b-c", "3.4.5-rc.2");
        assert_eq!(parse_version_tag(&tag), Some(v("3.4.5-rc.2")));
    }

    #[test]
    fn parse_version_tag_rejects_non_matching() {
        assert_eq!(parse_version_tag("v0.2.0"), None); // nothing before `-v`
        assert_eq!(parse_version_tag("-v0.2.0"), None); // empty prefix
        assert_eq!(parse_version_tag("toy-kv-0.2.0"), None); // no `-v`
        assert_eq!(parse_version_tag("toy-kv-v"), None); // empty version
        assert_eq!(parse_version_tag("toy-kv-vlatest"), None); // not semver
        assert_eq!(parse_version_tag("toy-kv-v1.2"), None); // not X.Y.Z
        assert_eq!(parse_version_tag("toy-kv-v1.2.3+build"), None); // build metadata rejected
        assert_eq!(parse_version_tag("random-tag"), None);
    }

    // --- Current-version selection (Flow 1 step 2 + first-release rule) ----------

    #[test]
    fn select_current_version_takes_max_across_both_sets() {
        let tags = vec![v("0.1.0"), v("0.3.0")];
        let reg = vec![v("0.2.0"), v("0.2.5")];
        assert_eq!(select_current_version(&tags, &reg), Some(v("0.3.0")));
    }

    #[test]
    fn select_current_version_registry_can_win() {
        let tags = vec![v("0.1.0")];
        let reg = vec![v("0.9.0")];
        assert_eq!(select_current_version(&tags, &reg), Some(v("0.9.0")));
    }

    #[test]
    fn select_current_version_prerelease_ordering() {
        // a final release outranks its own rc per semver.
        let tags = vec![v("1.0.0-rc.1"), v("1.0.0-rc.2")];
        let reg = vec![v("1.0.0")];
        assert_eq!(select_current_version(&tags, &reg), Some(v("1.0.0")));
        // rc only, no final yet.
        let reg2: Vec<Version> = vec![];
        assert_eq!(select_current_version(&tags, &reg2), Some(v("1.0.0-rc.2")));
    }

    #[test]
    fn select_current_version_first_release_is_none() {
        let empty: Vec<Version> = vec![];
        assert_eq!(select_current_version(&empty, &empty), None);
    }

    #[test]
    fn first_release_accepts_any_parseable_semver() {
        // The lenient first-release rule (contract Spec question Q1).
        assert!(first_release_accept("0.1.0").is_ok());
        assert!(first_release_accept("1.0.0").is_ok());
        assert!(first_release_accept("2.3.4-rc.1").is_ok());
        // rejects only un-parseable input.
        assert!(first_release_accept("not-a-version").is_err());
        assert!(first_release_accept("1.2").is_err());
    }

    // --- Version bump on a Cargo.toml (Flow 1 step 4) ----------------------------

    const SAMPLE_MANIFEST: &str = r#"# top comment
[workspace]
members = ["core", "utils"]

[workspace.package]
version = "0.1.0"   # inline comment
edition = "2021"

[workspace.dependencies]
toy-kv-utils = { version = "=0.1.0", path = "utils" }
toy-kv = { path = "core", version = "=0.1.0" }
serde = "1"
external = { version = "1.2", features = ["derive"] }
bare-pin = "=0.1.0"
"#;

    #[test]
    fn apply_version_bump_sets_workspace_package_version() {
        let out = apply_version_bump(SAMPLE_MANIFEST, "0.2.0").unwrap();
        let doc = out.parse::<toml_edit::DocumentMut>().unwrap();
        assert_eq!(
            doc["workspace"]["package"]["version"].as_str(),
            Some("0.2.0")
        );
    }

    #[test]
    fn apply_version_bump_repins_intra_workspace_exact_pins() {
        let out = apply_version_bump(SAMPLE_MANIFEST, "0.2.0").unwrap();
        let doc = out.parse::<toml_edit::DocumentMut>().unwrap();
        let deps = &doc["workspace"]["dependencies"];
        // both intra-workspace `=` pins (inline-table form, either key order) repinned
        assert_eq!(deps["toy-kv-utils"]["version"].as_str(), Some("=0.2.0"));
        assert_eq!(deps["toy-kv"]["version"].as_str(), Some("=0.2.0"));
        // bare `=`-string pin repinned
        assert_eq!(deps["bare-pin"].as_str(), Some("=0.2.0"));
    }

    #[test]
    fn apply_version_bump_leaves_non_exact_and_external_deps_untouched() {
        let out = apply_version_bump(SAMPLE_MANIFEST, "0.2.0").unwrap();
        let doc = out.parse::<toml_edit::DocumentMut>().unwrap();
        let deps = &doc["workspace"]["dependencies"];
        // plain (non-`=`) external version pins are NOT rewritten
        assert_eq!(deps["serde"].as_str(), Some("1"));
        assert_eq!(deps["external"]["version"].as_str(), Some("1.2"));
    }

    #[test]
    fn apply_version_bump_preserves_formatting_and_comments() {
        let out = apply_version_bump(SAMPLE_MANIFEST, "0.2.0").unwrap();
        assert!(out.starts_with("# top comment\n"), "leading comment kept");
        assert!(out.contains("# inline comment"), "inline comment kept");
        assert!(out.contains(r#"edition = "2021""#), "other keys kept");
        // path + features preserved alongside the repin
        assert!(out.contains(r#"path = "utils""#));
        assert!(out.contains(r#"features = ["derive"]"#));
    }

    #[test]
    fn apply_version_bump_errors_without_workspace_package_version() {
        let bad = "[workspace]\nmembers = []\n";
        assert!(apply_version_bump(bad, "0.2.0").is_err());
    }

    // --- PackageReport assembly (Flow 1 step 9) ----------------------------------

    #[test]
    fn build_package_report_normal_release() {
        let r = build_package_report(
            "toy-kv",
            Some(&v("0.1.0")),
            "0.2.0",
            "[0.2.0] - 2026-06-14",
            true,
            "boom",
            "### Added\n\n- thing\n",
        );
        assert_eq!(r.name, "toy-kv");
        assert_eq!(r.previous_version, "0.1.0");
        assert_eq!(r.new_version, "0.2.0");
        assert_eq!(r.title, "[0.2.0] - 2026-06-14");
        assert!(r.breaking);
        assert_eq!(r.breaking_reason, "boom");
        assert_eq!(r.changelog_body, "### Added\n\n- thing\n");
    }

    #[test]
    fn build_package_report_first_release_has_empty_previous() {
        let r = build_package_report("toy-kv", None, "0.1.0", "", false, "", "");
        // First release ⇒ empty previous (PR body omits the arrow).
        assert_eq!(r.previous_version, "");
        assert!(!r.breaking);
    }

    // Repo-URL parsing now lives in `crate::repo` (shared with M13); see its
    // unit tests there.

    // --- Confirm decision logic (Flow 1 step 1, stdin split off) -----------------

    #[test]
    fn confirm_decision_yes_flag_short_circuits() {
        assert!(confirm_decision(true, ""));
        assert!(confirm_decision(true, "n"));
    }

    #[test]
    fn confirm_decision_parses_yes_no() {
        assert!(confirm_decision(false, "y"));
        assert!(confirm_decision(false, "Y\n"));
        assert!(confirm_decision(false, "yes"));
        assert!(!confirm_decision(false, "n"));
        assert!(!confirm_decision(false, "no"));
        assert!(!confirm_decision(false, "")); // default is No
        assert!(!confirm_decision(false, "\n"));
        assert!(!confirm_decision(false, "maybe"));
    }
}
