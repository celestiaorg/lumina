//! Release orchestration (`cargo xtask release`).
//!
//! Publishes every publishable workspace crate to the registry in dependency
//! (topological) order, creates each crate's git tag + GitHub release, then
//! publishes the configured npm component(s). Fully idempotent / resumable: there
//! is no state file — re-running converges on "every crate published and tagged".

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::cargoops;
use crate::config;
use crate::forge::{self, Repo};
use crate::gitops::{Git, Where};
use crate::npmops::{self, NpmComponent};
use crate::workspace::{CrateInfo, Workspace};

/// Default per-crate registry-indexing wait budget (passed to
/// [`cargoops::wait_until_published`]). Single source of truth — the CLI builds
/// [`ReleaseOptions`] through [`ReleaseOptions::new`], which applies this default.
pub(crate) const DEFAULT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(600);

/// The remote consulted by the idempotency scan's remote tag check.
const REMOTE: &str = "origin";

/// Inputs for one release run.
///
/// Every credential is the **name** of an environment variable holding the
/// token, never a literal token (the wired deps read the value from the env).
#[derive(Debug, Clone)]
pub struct ReleaseOptions {
    /// Commit to release *from* and place every git tag *on* (the release-PR
    /// merge commit, normally). All tags + GitHub releases point at this SHA.
    pub sha: String,
    /// Name of the env var holding the GitHub token (tag pushes + releases).
    pub github_token_env: String,
    /// Name of the env var holding the cargo registry token (`cargo publish`).
    pub registry_token_env: String,
    /// Name of the env var holding the npm token; `Some` only when an npm
    /// component is configured. `None` skips the npm step regardless of config.
    pub npm_token_env: Option<String>,
    /// Per-crate registry-indexing wait budget.
    pub publish_timeout: Duration,
}

impl ReleaseOptions {
    /// Construct options with the default publish timeout.
    pub fn new(
        sha: impl Into<String>,
        github_token_env: impl Into<String>,
        registry_token_env: impl Into<String>,
        npm_token_env: Option<String>,
    ) -> Self {
        Self {
            sha: sha.into(),
            github_token_env: github_token_env.into(),
            registry_token_env: registry_token_env.into(),
            npm_token_env,
            publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
        }
    }
}

// Pure decision core (unit-tested without I/O).

/// The per-crate publish decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrateAction {
    /// Tag already exists → fully done, do nothing.
    Skip,
    /// Orphan-tag fix: on the registry but no tag → create tag + release only.
    TagOnly,
    /// Never published, no tag → publish, wait, then tag + release.
    Publish,
}

/// Decide what to do for one crate. `tag_exists == true` short-circuits to
/// [`CrateAction::Skip`] regardless of `is_published` (a tag implies the version
/// was published before the tag was created).
///
/// | `tag_exists` | `is_published` | Result |
/// |:---:|:---:|---|
/// | `true` | (any) | `Skip` |
/// | `false` | `true` | `TagOnly` |
/// | `false` | `false` | `Publish` |
pub fn decide_action(tag_exists: bool, is_published: bool) -> CrateAction {
    if tag_exists {
        CrateAction::Skip
    } else if is_published {
        CrateAction::TagOnly
    } else {
        CrateAction::Publish
    }
}

/// The per-crate tag convention: `{crate_name}-v{version}`, e.g. `toy-kv-v0.2.0`.
/// Whatever reads these tags back must use the same convention.
pub fn tag_name(crate_name: &str, version: &str) -> String {
    format!("{crate_name}-v{version}")
}

/// What happened for one crate during the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrateOutcome {
    /// Tag already existed; nothing done.
    Skipped,
    /// Uploaded (or "already uploaded" race), indexed, then tagged + released.
    Published,
    /// Orphan-tag fix: already on the registry, created the missing tag +
    /// release (no upload).
    Tagged,
}

/// Per-crate release record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrateRelease {
    /// Crate name.
    pub name: String,
    /// Workspace version released.
    pub version: String,
    /// Tag name (`{name}-v{version}`).
    pub tag: String,
    /// What happened for this crate.
    pub outcome: CrateOutcome,
}

/// Outcome of the optional npm step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NpmStatus {
    /// No `[[npm]]` configured (crate-only workspace).
    SkippedNoComponent,
    /// npm component(s) configured but no npm token was supplied.
    SkippedNoToken,
    /// npm published; payload = the `wasm_crate` names published (one per
    /// component).
    Published(Vec<String>),
}

/// Full result of a release run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseOutcome {
    /// Workspace version released.
    pub version: String,
    /// One entry per publishable crate, in publish (topological) order.
    pub crates: Vec<CrateRelease>,
    /// The npm step result.
    pub npm: NpmStatus,
}

/// Best-effort release-note body: the top entry of the crate's `CHANGELOG.md`
/// (the text under the first `## [..]` heading), or a short fallback note. Never
/// errors — release notes are non-critical.
fn release_body(crate_info: &CrateInfo, version: &str) -> String {
    let path = crate_info.manifest_dir.join("CHANGELOG.md");
    if let Ok(contents) = std::fs::read_to_string(&path) {
        if let Some(body) = top_changelog_entry(&contents) {
            if !body.trim().is_empty() {
                return body;
            }
        }
    }
    format!("Release {} v{}.", crate_info.name, version)
}

/// Extract the body of the first `## [..]` entry from a `CHANGELOG.md` string:
/// everything after the first `## ` heading up to (but excluding) the next
/// `## ` heading. Returns `None` if there is no `## ` entry heading.
fn top_changelog_entry(contents: &str) -> Option<String> {
    // Collect the lines after the first `## ` heading, stopping at the next
    // `## ` heading. (A leading `## [Unreleased]` typically has an empty body,
    // in which case the caller's blank-check falls through to the fallback.)
    let mut started = false;
    let mut body: Vec<&str> = Vec::new();
    for line in contents.lines() {
        if line.starts_with("## ") {
            if started {
                break;
            }
            started = true;
            continue;
        }
        if started {
            body.push(line);
        }
    }
    if !started {
        return None;
    }
    Some(body.join("\n").trim().to_string())
}

/// Execute the release run. See the module docs for the behavior.
pub fn run(repo_root: &Path, opts: &ReleaseOptions) -> Result<ReleaseOutcome> {
    let ws: Workspace =
        Workspace::discover(repo_root).context("discovering the workspace (cargo metadata)")?;
    let version = ws.version.to_string();
    let order = ws
        .publish_order()
        .context("computing the topological publish order")?;
    let repo = crate::repo::derive(repo_root)?;
    // Attribute the annotated tags to the GitHub token owner, the way release-plz
    // does. The default Actions `GITHUB_TOKEN` is an installation token with no
    // associated user (`token_committer` -> None), so the tagger then falls back
    // to the runner's ambient git identity.
    let identity = crate::forge::token_committer(&opts.github_token_env)
        .context("resolving the tagger from the GitHub token")?;
    let git = Git::new(repo_root).with_identity(identity);

    // For each crate in publish order: scan its release state (tag + registry),
    // then apply the decision — skip, publish + tag, or tag-only (orphan-tag fix).
    let mut crates = Vec::with_capacity(order.len());
    for crate_info in &order {
        let tag = tag_name(&crate_info.name, &version);

        let tag_exists = git
            .tag_exists(&tag, Where::Both(REMOTE.to_string()))
            .with_context(|| format!("checking whether tag `{tag}` exists"))?;
        let is_published =
            cargoops::is_published(&crate_info.name, &version).with_context(|| {
                format!(
                    "checking whether `{}@{}` is already published",
                    crate_info.name, version
                )
            })?;

        let action = decide_action(tag_exists, is_published);
        let outcome = match action {
            CrateAction::Skip => CrateOutcome::Skipped,
            CrateAction::Publish => {
                cargoops::publish(&crate_info.manifest_dir, &opts.registry_token_env)
                    .with_context(|| format!("publishing `{}@{}`", crate_info.name, version))?;
                cargoops::wait_until_published(&crate_info.name, &version, opts.publish_timeout)
                    .with_context(|| {
                        format!(
                            "waiting for `{}@{}` to index on the registry",
                            crate_info.name, version
                        )
                    })?;
                create_tag_and_release(&git, &repo, crate_info, &version, &tag, opts)?;
                CrateOutcome::Published
            }
            CrateAction::TagOnly => {
                // Orphan-tag fix: on the registry but no tag → create the tag +
                // release, do NOT skip and do NOT re-upload.
                create_tag_and_release(&git, &repo, crate_info, &version, &tag, opts)?;
                CrateOutcome::Tagged
            }
        };

        crates.push(CrateRelease {
            name: crate_info.name.clone(),
            version: version.clone(),
            tag,
            outcome,
        });
    }

    let npm = run_npm_step(repo_root, &version, opts)?;

    Ok(ReleaseOutcome {
        version,
        crates,
        npm,
    })
}

/// Create the crate's git tag on the release SHA and cut its GitHub release.
/// Shared by the `Publish` and `TagOnly` branches. Both `gitops::create_tag`
/// and `forge::create_release` are idempotent (existing tag/release = success).
fn create_tag_and_release(
    git: &Git,
    repo: &Repo,
    crate_info: &CrateInfo,
    version: &str,
    tag: &str,
    opts: &ReleaseOptions,
) -> Result<()> {
    let tag_msg = format!("{} {}", crate_info.name, version);
    git.create_tag(tag, &opts.sha, Some(&tag_msg))
        .with_context(|| format!("creating tag `{tag}` at {}", opts.sha))?;

    let body = release_body(crate_info, version);
    let prerelease = forge::is_prerelease(&crate_info.version);
    forge::create_release(
        repo,
        tag,
        tag, // release name = tag name
        &body,
        prerelease,
        &opts.github_token_env,
    )
    .with_context(|| format!("creating GitHub release for tag `{tag}`"))?;
    Ok(())
}

/// The pure npm-step gate decision: the npm step runs only if a component is
/// configured AND a token was supplied; otherwise it is skipped wholesale.
/// Factored out so the gate is unit-testable without I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NpmGate {
    /// No `[[npm]]` configured → `NpmStatus::SkippedNoComponent`.
    SkipNoComponent,
    /// Component(s) configured but no token → `NpmStatus::SkippedNoToken`.
    SkipNoToken,
    /// Run the publish loop.
    Publish,
}

/// Pure gate: `has_component && has_token` decides whether to publish.
pub(crate) fn npm_gate(has_component: bool, has_token: bool) -> NpmGate {
    if !has_component {
        NpmGate::SkipNoComponent
    } else if !has_token {
        NpmGate::SkipNoToken
    } else {
        NpmGate::Publish
    }
}

/// Run the optional npm publish step. Skipped wholesale unless `config::load`
/// yields ≥1 `[[npm]]` AND `opts.npm_token_env` is `Some`.
fn run_npm_step(repo_root: &Path, version: &str, opts: &ReleaseOptions) -> Result<NpmStatus> {
    let cfg = config::load(repo_root).context("loading release.toml for the npm step")?;

    match npm_gate(!cfg.npm.is_empty(), opts.npm_token_env.is_some()) {
        NpmGate::SkipNoComponent => return Ok(NpmStatus::SkippedNoComponent),
        NpmGate::SkipNoToken => return Ok(NpmStatus::SkippedNoToken),
        NpmGate::Publish => {}
    }
    // Safe: `Publish` implies the token is Some.
    let npm_token_env = opts
        .npm_token_env
        .as_deref()
        .expect("npm_gate Publish implies a token");

    let dist_tag = npmops::dist_tag(version);
    let mut published = Vec::with_capacity(cfg.npm.len());
    for c in &cfg.npm {
        let component = NpmComponent {
            wasm_crate: c.wasm_crate.clone(),
            package_dir: c.package_dir.clone(),
        };
        npmops::publish(&component, version, &dist_tag, npm_token_env)
            .with_context(|| format!("publishing npm component `{}`", c.wasm_crate))?;
        published.push(c.wasm_crate.clone());
    }
    Ok(NpmStatus::Published(published))
}

// Unit tests — pure decision logic only (no publish, no tag, no network).

#[cfg(test)]
mod tests {
    use super::*;

    // State table

    #[test]
    fn state_table_tag_exists_skips_regardless_of_published() {
        // Tag exists → Skip, whether or not the registry has the version. A tag
        // implies the version was published before the tag was created.
        assert_eq!(decide_action(true, true), CrateAction::Skip);
        assert_eq!(decide_action(true, false), CrateAction::Skip);
    }

    #[test]
    fn state_table_published_no_tag_is_orphan_tag_fix() {
        // The critical fix: published but un-tagged → TagOnly, NOT Skip.
        assert_eq!(decide_action(false, true), CrateAction::TagOnly);
    }

    #[test]
    fn state_table_neither_publishes() {
        assert_eq!(decide_action(false, false), CrateAction::Publish);
    }

    #[test]
    fn state_table_is_total() {
        // All four boolean combinations are covered with no panics / no default.
        for &tag in &[true, false] {
            for &pubd in &[true, false] {
                let a = decide_action(tag, pubd);
                if tag {
                    assert_eq!(a, CrateAction::Skip);
                } else if pubd {
                    assert_eq!(a, CrateAction::TagOnly);
                } else {
                    assert_eq!(a, CrateAction::Publish);
                }
            }
        }
    }

    // Tag-name convention

    #[test]
    fn tag_name_uses_crate_v_version_convention() {
        assert_eq!(tag_name("toy-kv-utils", "0.2.0"), "toy-kv-utils-v0.2.0");
        assert_eq!(tag_name("toy-kv", "0.2.0"), "toy-kv-v0.2.0");
        assert_eq!(tag_name("toy-kv-wasm", "0.2.0"), "toy-kv-wasm-v0.2.0");
    }

    #[test]
    fn tag_name_preserves_prerelease_versions() {
        assert_eq!(tag_name("toy-kv", "1.0.0-rc.1"), "toy-kv-v1.0.0-rc.1");
    }

    // Repo-URL parsing now lives in `crate::repo`; see its unit tests there.

    // npm gate (skip when no component / no token)

    #[test]
    fn npm_gate_skips_without_component() {
        assert_eq!(npm_gate(false, true), NpmGate::SkipNoComponent);
        assert_eq!(npm_gate(false, false), NpmGate::SkipNoComponent);
    }

    #[test]
    fn npm_gate_skips_without_token() {
        assert_eq!(npm_gate(true, false), NpmGate::SkipNoToken);
    }

    #[test]
    fn npm_gate_publishes_with_component_and_token() {
        assert_eq!(npm_gate(true, true), NpmGate::Publish);
    }

    // dist-tag selection wiring (delegates to npmops::dist_tag)

    #[test]
    fn dist_tag_wiring_matches_npmops() {
        // The release flow wires `npmops::dist_tag(version)`; confirm the wiring
        // picks the expected tags for stable vs prerelease versions.
        assert_eq!(npmops::dist_tag("0.2.0"), "latest");
        assert_eq!(npmops::dist_tag("0.2.0-rc.1"), "rc");
    }

    // prerelease flag wiring (forge::is_prerelease)

    #[test]
    fn prerelease_flag_wiring() {
        let stable = semver::Version::parse("0.2.0").unwrap();
        let rc = semver::Version::parse("0.2.0-rc.1").unwrap();
        assert!(!forge::is_prerelease(&stable));
        assert!(forge::is_prerelease(&rc));
    }

    // release-note body extraction

    #[test]
    fn top_changelog_entry_extracts_first_entry_body() {
        let cl = "\
# Changelog

## [Unreleased]

## [0.2.0] - 2026-06-14

### Added

- a new thing

## [0.1.0] - 2026-01-01

### Added

- the first thing
";
        // First `## ` is `[Unreleased]` (empty body) — collection stops at the
        // next `## ` heading, so the body is empty here and the caller falls
        // back. We assert the extractor stops at the next heading.
        let body = top_changelog_entry(cl).unwrap();
        assert_eq!(body, "");
    }

    #[test]
    fn top_changelog_entry_returns_body_when_first_entry_has_one() {
        let cl = "\
# Changelog

## [0.2.0] - 2026-06-14

### Added

- a new thing

## [0.1.0] - 2026-01-01

- old
";
        let body = top_changelog_entry(cl).unwrap();
        assert!(body.contains("### Added"));
        assert!(body.contains("- a new thing"));
        assert!(!body.contains("0.1.0"));
        assert!(!body.contains("old"));
    }

    #[test]
    fn top_changelog_entry_none_without_heading() {
        assert_eq!(top_changelog_entry("just text, no heading"), None);
    }

    // ReleaseOptions constructor default

    #[test]
    fn release_options_new_sets_default_timeout() {
        let o = ReleaseOptions::new("deadbeef", "GH", "CARGO", None);
        assert_eq!(o.sha, "deadbeef");
        assert_eq!(o.github_token_env, "GH");
        assert_eq!(o.registry_token_env, "CARGO");
        assert!(o.npm_token_env.is_none());
        assert_eq!(o.publish_timeout, DEFAULT_PUBLISH_TIMEOUT);
    }

}
