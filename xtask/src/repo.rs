//! Shared GitHub-repo derivation for the prepare and release flows.
//!
//! Both flows must agree on the same `owner/name`, so the URL parsing and the
//! `origin`-remote → workspace-metadata fallback live here once rather than being
//! reimplemented (and silently drifting) in each command module.

use std::path::Path;
use std::process::Command;

use anyhow::{Result, anyhow};

use crate::forge::Repo;

/// Parse a GitHub remote URL into a [`Repo`]. Pure; accepts both forms with an
/// optional `.git` suffix and trailing slash:
///
/// - https/http: `https://github.com/<owner>/<repo>(.git)`
/// - ssh:        `git@github.com:<owner>/<repo>(.git)` and
///   `ssh://git@github.com/<owner>/<repo>(.git)`
/// - git:        `git://github.com/<owner>/<repo>(.git)`
///
/// Returns `None` for an unrecognized scheme or an owner-less / over-deep URL —
/// we never guess.
pub fn from_url(url: &str) -> Option<Repo> {
    let url = url.trim();
    // Strip the scheme/host prefix down to the `<owner>/<repo>` tail.
    let tail = if let Some(rest) = url.strip_prefix("git@") {
        // git@github.com:owner/repo(.git)
        rest.split_once(':').map(|(_host, path)| path)?
    } else if let Some(rest) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("ssh://git@"))
        .or_else(|| url.strip_prefix("git://"))
    {
        // host/owner/repo(.git) — drop the host segment.
        rest.split_once('/').map(|(_host, path)| path)?
    } else {
        return None;
    };

    let tail = tail.trim_end_matches('/');
    let tail = tail.strip_suffix(".git").unwrap_or(tail);

    // A clean `<owner>/<repo>` is required; reject anything else (e.g. extra path
    // depth or an empty segment).
    let (owner, name) = tail.split_once('/')?;
    if owner.is_empty() || name.is_empty() || name.contains('/') {
        return None;
    }
    Some(Repo {
        owner: owner.to_string(),
        name: name.to_string(),
    })
}

/// Resolve the [`Repo`] from the `origin` remote URL, falling back to the
/// workspace `repository` metadata (root `Cargo.toml`) when `origin` is absent or
/// unparseable.
pub fn derive(repo_root: &Path) -> Result<Repo> {
    if let Some(url) = origin_url(repo_root) {
        if let Some(repo) = from_url(&url) {
            return Ok(repo);
        }
    }
    if let Some(url) = workspace_repository_url(repo_root) {
        if let Some(repo) = from_url(&url) {
            return Ok(repo);
        }
    }
    Err(anyhow!(
        "could not determine the GitHub owner/name: no parseable `origin` remote \
         URL and no usable workspace `repository` metadata in {}",
        repo_root.display()
    ))
}

/// `git config --get remote.origin.url`, or `None` if there is no origin.
fn origin_url(repo_root: &Path) -> Option<String> {
    let out = Command::new("git")
        .current_dir(repo_root)
        .args(["config", "--get", "remote.origin.url"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if url.is_empty() { None } else { Some(url) }
}

/// Read `[workspace.package].repository` from the root `Cargo.toml`. `None` if the
/// manifest cannot be read/parsed or the field is absent. Shells nothing — it
/// re-reads the manifest the version bump already touched.
fn workspace_repository_url(repo_root: &Path) -> Option<String> {
    let src = std::fs::read_to_string(repo_root.join("Cargo.toml")).ok()?;
    let doc = src.parse::<toml_edit::DocumentMut>().ok()?;
    doc.get("workspace")?
        .get("package")?
        .get("repository")?
        .as_str()
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    // `forge::Repo` does not derive `PartialEq`, so compare fields.
    #[track_caller]
    fn assert_repo(url: &str, owner: &str, name: &str) {
        let r = from_url(url).unwrap_or_else(|| panic!("expected Some for {url:?}"));
        assert_eq!(r.owner, owner, "owner mismatch for {url:?}");
        assert_eq!(r.name, name, "name mismatch for {url:?}");
    }

    #[test]
    fn parses_https_plain() {
        assert_repo("https://github.com/mcrakhman/toy-kv", "mcrakhman", "toy-kv");
    }

    #[test]
    fn parses_https_dot_git_and_trailing_slash() {
        assert_repo(
            "https://github.com/mcrakhman/toy-kv.git",
            "mcrakhman",
            "toy-kv",
        );
        assert_repo("https://github.com/mcrakhman/toy-kv/", "mcrakhman", "toy-kv");
    }

    #[test]
    fn parses_http_form() {
        assert_repo("http://github.com/mcrakhman/toy-kv", "mcrakhman", "toy-kv");
    }

    #[test]
    fn parses_ssh_scp_form() {
        assert_repo("git@github.com:mcrakhman/toy-kv.git", "mcrakhman", "toy-kv");
        assert_repo("git@github.com:mcrakhman/toy-kv", "mcrakhman", "toy-kv");
    }

    #[test]
    fn parses_ssh_url_form() {
        assert_repo(
            "ssh://git@github.com/mcrakhman/toy-kv.git",
            "mcrakhman",
            "toy-kv",
        );
    }

    #[test]
    fn parses_git_url_form() {
        assert_repo("git://github.com/mcrakhman/toy-kv.git", "mcrakhman", "toy-kv");
    }

    #[test]
    fn trims_whitespace() {
        assert_repo(
            "  https://github.com/mcrakhman/toy-kv\n",
            "mcrakhman",
            "toy-kv",
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(from_url("not-a-url").is_none());
        assert!(from_url("https://github.com/onlyowner").is_none());
        assert!(from_url("").is_none());
        // Extra path depth is rejected (we never guess).
        assert!(from_url("https://github.com/owner/repo/extra").is_none());
    }
}
