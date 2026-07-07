//! Prepare the release PR: branch guard, current-version discovery (git tags +
//! registry), legal-successor validation, the format-preserving workspace version
//! bump, per-crate breaking analysis (diagnostic) and changelogs, the optional npm
//! wrapper update, the single release commit, the PR-body assembly, and — only under
//! `--push` — pushing the branch and opening or updating the PR.
//!
//! The real work is delegated to the sibling modules (`config`, `workspace`,
//! `version`, `commit`, `gitops`, `cargoops`, `forge`, `npmops`, `changelog`,
//! `breaking`, `prbody`); this module owns the wiring and the pure helpers below.
//! Tag naming/parsing (`{crate}-v{version}`) lives in [`crate::version`].

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use semver::Version;

use crate::gitops::{Git, Where};
use crate::prbody::PackageReport;

/// The default release-branch prefix when neither the option nor config supplies one.
const DEFAULT_BRANCH_PREFIX: &str = "release-";

/// Fully-resolved inputs to a prepare run (the CLI turns prompts + flags into this).
#[derive(Debug, Clone)]
pub struct PrepareOptions {
    /// Release-branch prefix. Resolution: this → `config.defaults.branch_prefix` →
    /// `"release-"`.
    pub branch_prefix: Option<String>,
    /// The requested next version, already resolved by the CLI.
    pub version: String,
    /// Auto-confirm destructive prompts (the branch-delete prompt).
    pub yes: bool,
    /// Opt into remote actions (the remote branch check and the push/PR step).
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
    /// SHA of the single release commit.
    pub commit_sha: String,
    /// `true` iff the branch was pushed and the PR opened/updated (only under `push`).
    pub pushed: bool,
    /// The PR html_url when `pushed`; `None` otherwise.
    pub pr_url: Option<String>,
    /// The discovered current version; `None` for a first release.
    pub current_version: Option<String>,
    /// The per-crate reports, one per publishable crate.
    pub packages: Vec<PackageReport>,
}

/// Build the release PR end to end. See the module docs for the overall flow.
pub fn run(repo_root: &Path, opts: &PrepareOptions) -> Result<PrepareOutcome> {
    let config = crate::config::load(repo_root).context("loading release.toml")?;
    let ws = crate::workspace::Workspace::discover(repo_root).context("discovering workspace")?;
    // When pushing (the CI path) attribute the release commit to the GitHub token
    // owner. A local, no-push run keeps the developer's ambient git identity.
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
    guard_release_branch(&git, &branch, remote_scope.clone(), opts)?;
    // Resolve the base branch *before* switching to the release branch (its
    // current-branch fallback depends on that), then create/reset the branch.
    let base = git.default_branch();
    git.create_or_reset_branch(&branch, &base)
        .with_context(|| format!("creating release branch `{branch}` at `{base}`"))?;

    let current = discover_current_version(&git, &ws, remote_scope)?;
    validate_requested_version(current.as_ref(), &opts.version)?;

    bump_workspace_version(repo_root, &opts.version)?;
    preflight_publishable(repo_root, opts.no_verify)?;

    let reports =
        build_package_reports(&git, repo_root, &ws, current.as_ref(), &opts.version, &date)?;

    update_npm_wrappers(&config, &opts.version)?;

    // The signed-commit path needs the title/body, and neither depends on the commit
    // SHA, so render them before creating the commit.
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

    let commit = create_release_commit(&git, repo_root, opts, &branch, &base, &title, &body)?;

    Ok(PrepareOutcome {
        branch,
        commit_sha: commit.sha,
        pushed: commit.pushed,
        pr_url: commit.pr_url,
        current_version: current.map(|v| v.to_string()),
        packages: reports,
    })
}

/// Guard against clobbering an existing release branch: if it already exists and
/// `--yes` was not given, error. Under `--push` the existence check consults the
/// remote too. Does **not** create the branch — the caller resolves the base branch
/// and creates it, so `Git::default_branch`'s current-branch fallback runs at the
/// right point (before the branch switch, and only past this guard).
fn guard_release_branch(
    git: &Git,
    branch: &str,
    remote_scope: Where,
    opts: &PrepareOptions,
) -> Result<()> {
    if !opts.yes
        && git
            .branch_exists(branch, remote_scope)
            .with_context(|| format!("checking whether release branch `{branch}` exists"))?
    {
        return Err(anyhow!(
            "release branch `{branch}` already exists; pass --yes to delete and recreate it \
             from scratch, or delete it manually."
        ));
    }
    Ok(())
}

/// Discover the current version: the max of the version git tags and the highest
/// version published to the registry across all members. `None` for a first release.
fn discover_current_version(
    git: &Git,
    ws: &crate::workspace::Workspace,
    remote_scope: Where,
) -> Result<Option<Version>> {
    let tags = git
        .list_tags(remote_scope)
        .context("listing git tags for current-version discovery")?;
    let mut versions: Vec<Version> = tags
        .iter()
        .filter_map(|t| crate::version::parse_version_tag(t))
        .collect();

    for crate_info in &ws.crates {
        if let Some(v) = crate::cargoops::max_published_version(&crate_info.name)
            .with_context(|| format!("querying published versions of `{}`", crate_info.name))?
        {
            versions.push(v);
        }
    }
    Ok(crate::version::max_version(&versions))
}

/// Validate the requested version: a legal successor of `current`, or — for a first
/// release (`current` is `None`) — any parseable semver.
fn validate_requested_version(current: Option<&Version>, requested: &str) -> Result<()> {
    match current {
        Some(cur) => {
            crate::version::is_legal_successor(&cur.to_string(), requested).map_err(|e| {
                anyhow!(
                    "requested version `{requested}` is not a legal successor of current `{cur}`: {e}"
                )
            })?;
        }
        None => {
            crate::version::parse_next(requested).map_err(|e| {
                anyhow!("requested first-release version `{requested}` is not valid semver: {e}")
            })?;
        }
    }
    Ok(())
}

/// Format-preserving rewrite of the workspace version in the root `Cargo.toml`.
fn bump_workspace_version(repo_root: &Path, version: &str) -> Result<()> {
    let manifest_path = repo_root.join("Cargo.toml");
    let manifest_src = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let bumped = apply_version_bump(&manifest_src, version)
        .with_context(|| format!("bumping workspace version in {}", manifest_path.display()))?;
    std::fs::write(&manifest_path, bumped)
        .with_context(|| format!("writing {}", manifest_path.display()))
}

/// Verify the whole workspace can be packaged + published before anything is
/// committed, catching problems (e.g. a yanked dependency) up front rather than
/// half-way through the real release. Runs on the dirty tree via `--allow-dirty`;
/// a no-op when `no_verify` is set.
fn preflight_publishable(repo_root: &Path, no_verify: bool) -> Result<()> {
    if no_verify {
        return Ok(());
    }
    println!("Running publishability preflight (cargo publish --workspace --dry-run)…");
    crate::cargoops::verify_publishable(repo_root)
        .context("publishability preflight failed — aborting before the release commit")
}

/// Per publishable crate (topological order): collect its commits once, run the
/// diagnostic breaking analysis, render + write its changelog entry, and assemble
/// the PR-body report.
fn build_package_reports(
    git: &Git,
    repo_root: &Path,
    ws: &crate::workspace::Workspace,
    current: Option<&Version>,
    version: &str,
    date: &str,
) -> Result<Vec<PackageReport>> {
    let publishable: Vec<crate::workspace::CrateInfo> =
        ws.publish_order()?.into_iter().cloned().collect();

    let mut reports: Vec<PackageReport> = Vec::with_capacity(publishable.len());
    for crate_info in &publishable {
        let last_ref =
            current.map(|cur| crate::version::tag_name(&crate_info.name, &cur.to_string()));
        let dir = crate::commit::pathspec(repo_root, &crate_info.manifest_dir);

        // Collect the package's commits once and share them with both breaking
        // analysis and changelog rendering.
        let range = last_ref.as_ref().map(|r| format!("{r}..HEAD"));
        let commits = crate::commit::collect(git, range.as_deref(), &dir)
            .with_context(|| format!("collecting commits for `{}`", crate_info.name))?;
        let parsed: Vec<crate::commit::ParsedCommit> =
            commits.iter().map(crate::commit::parse).collect();
        let breaking = crate::breaking::analyze(repo_root, crate_info, current, &parsed);

        let changelog = crate::changelog::generate(crate_info, version, date, &commits)
            .with_context(|| format!("generating changelog for `{}`", crate_info.name))?;

        // The PR body wants the entry heading text without the `## ` prefix.
        let entry_title = changelog
            .file_entry
            .lines()
            .next()
            .unwrap_or("")
            .trim_start_matches("## ")
            .to_string();
        reports.push(build_package_report(
            &crate_info.name,
            current,
            version,
            &entry_title,
            breaking.has_breaking,
            breaking.reason.as_deref().unwrap_or(""),
            &changelog.body_only,
        ));
    }
    Ok(reports)
}

/// Mirror the release version into each configured npm wrapper (a no-op when no
/// component is configured). Publishes nothing.
fn update_npm_wrappers(config: &crate::config::Config, version: &str) -> Result<()> {
    for component in &config.npm {
        let npm = crate::npmops::NpmComponent {
            wasm_crate: component.wasm_crate.clone(),
            package_dir: component.package_dir.clone(),
        };
        crate::npmops::update_for_pr(&npm, version)
            .with_context(|| format!("updating npm wrapper `{}`", component.package_dir))?;
    }
    Ok(())
}

/// The outcome of creating the single release commit.
struct ReleaseCommit {
    /// SHA of the release commit.
    sha: String,
    /// `true` iff the branch was pushed and the PR opened/updated (only under `--push`).
    pushed: bool,
    /// The PR html_url when `pushed`; `None` otherwise.
    pr_url: Option<String>,
}

/// Create the single release commit. Under `--push` the commit is made through
/// GitHub's `createCommitOnBranch` API so it is GitHub-signed ("Verified"): the
/// branch is pushed at its base commit, one signed commit carrying the whole release
/// change set is created on top, then the PR is opened/updated. A local (no-`--push`)
/// run keeps a plain local commit — a preview that cannot be signed without a key.
fn create_release_commit(
    git: &Git,
    repo_root: &Path,
    opts: &PrepareOptions,
    branch: &str,
    base: &str,
    title: &str,
    body: &str,
) -> Result<ReleaseCommit> {
    let commit_message = format!("chore: release v{}", opts.version);
    if !opts.push {
        let sha = git
            .stage_all_and_commit(&commit_message)
            .context("creating the single release commit")?;
        return Ok(ReleaseCommit {
            sha,
            pushed: false,
            pr_url: None,
        });
    }

    // The base commit the branch was reset to — still HEAD, since nothing has been
    // committed locally. It becomes the signed commit's expectedHeadOid.
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

    let repo = crate::forge::derive(git).context("deriving the GitHub owner/name for the PR")?;

    // Publish the branch at its base commit, then create one signed commit on top.
    git.push_commit_to_branch("origin", &base_sha, branch)
        .with_context(|| format!("pushing release branch `{branch}` base to origin"))?;
    let sha = crate::forge::create_commit_on_branch(
        &repo,
        branch,
        &base_sha,
        &commit_message,
        &additions,
        &staged.deletions,
        &opts.github_token_env,
    )
    .context("creating the GitHub-signed release commit")?;

    let pr =
        crate::forge::open_or_update_pr(&repo, branch, base, title, body, &opts.github_token_env)
            .context("opening or updating the release PR")?;

    Ok(ReleaseCommit {
        sha,
        pushed: true,
        pr_url: Some(pr.url),
    })
}

/// Resolve the branch prefix: explicit option → config default → `"release-"`.
fn resolve_branch_prefix(opt: Option<&str>, cfg: Option<&str>) -> String {
    opt.or(cfg).unwrap_or(DEFAULT_BRANCH_PREFIX).to_string()
}

/// The release-branch name `"<prefix><version>"`.
fn target_branch(prefix: &str, version: &str) -> String {
    format!("{prefix}{version}")
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

/// Assemble one `PackageReport` from the per-crate data.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Version {
        Version::parse(s).unwrap()
    }

    // Regression: `actions/checkout` leaves `refs/remotes/origin/HEAD` unset, which
    // used to fall back to the literal "HEAD" — a valid branch-from ref but an
    // INVALID PR base (`422 field: base`). Detection must yield a real branch name.

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

    const SAMPLE_MANIFEST: &str = r#"# top comment
[workspace]
members = ["core", "utils"]

[workspace.package]
version = "0.1.0"   # inline comment
edition = "2021"

[workspace.dependencies]
lumina-utils = { version = "=0.1.0", path = "utils" }
lumina = { path = "core", version = "=0.1.0" }
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
        assert_eq!(deps["lumina-utils"]["version"].as_str(), Some("=0.2.0"));
        assert_eq!(deps["lumina"]["version"].as_str(), Some("=0.2.0"));
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

    #[test]
    fn build_package_report_normal_release() {
        let r = build_package_report(
            "lumina",
            Some(&v("0.1.0")),
            "0.2.0",
            "[0.2.0] - 2026-06-14",
            true,
            "boom",
            "### Added\n\n- thing\n",
        );
        assert_eq!(r.name, "lumina");
        assert_eq!(r.previous_version, "0.1.0");
        assert_eq!(r.new_version, "0.2.0");
        assert_eq!(r.title, "[0.2.0] - 2026-06-14");
        assert!(r.breaking);
        assert_eq!(r.breaking_reason, "boom");
        assert_eq!(r.changelog_body, "### Added\n\n- thing\n");
    }

    #[test]
    fn build_package_report_first_release_has_empty_previous() {
        let r = build_package_report("lumina", None, "0.1.0", "", false, "", "");
        // First release ⇒ empty previous (PR body omits the arrow).
        assert_eq!(r.previous_version, "");
        assert!(!r.breaking);
    }
}
