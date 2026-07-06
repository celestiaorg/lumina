//! GitHub API primitives over blocking HTTP: open/update the release PR and
//! create a GitHub release for a tag. The token is read from the env var whose
//! name is passed in; a literal token is never accepted and never logged.

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use serde_json::{Value, json};

use crate::gitops::GitIdentity;

/// Base URL of the GitHub REST API.
const API_BASE: &str = "https://api.github.com";
/// GitHub GraphQL API endpoint (used for `createCommitOnBranch`, which produces a
/// GitHub-**signed** ("Verified") commit — the reason we create the release commit
/// through the API rather than `git commit`).
const GRAPHQL_URL: &str = "https://api.github.com/graphql";
/// `User-Agent` sent on every request (GitHub rejects requests without one).
const USER_AGENT: &str = "lumina-xtask";
/// GitHub REST media type.
const ACCEPT: &str = "application/vnd.github+json";

/// Identifies a GitHub repository: `owner/name`.
#[derive(Debug, Clone)]
pub struct Repo {
    /// Repository owner (user or org), e.g. `mcrakhman`.
    pub owner: String,
    /// Repository name, e.g. `toy-kv`.
    pub name: String,
}

impl Repo {
    /// `{API_BASE}/repos/{owner}/{name}` — the REST base every endpoint hangs off.
    fn rest_base(&self) -> String {
        format!("{API_BASE}/repos/{}/{}", self.owner, self.name)
    }

    /// `{owner}/{name}` — GraphQL's `repositoryNameWithOwner`.
    fn name_with_owner(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

/// Result of opening or updating a pull request.
#[derive(Debug, Clone)]
pub struct PrRef {
    /// `html_url` of the PR.
    pub url: String,
}

/// Returns `true` iff `version` carries a prerelease component (e.g.
/// `1.2.0-rc.1`), `false` for a clean release (e.g. `1.2.0`).
///
/// The caller passes the result as the `prerelease` flag of [`create_release`].
pub fn is_prerelease(version: &semver::Version) -> bool {
    !version.pre.is_empty()
}

/// Resolve a GitHub token by reading the env var **named** by `name`.
///
/// Never accepts a literal token and never logs the value: the error names the
/// env var only.
fn token_from_env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| {
        anyhow!("GitHub token env var `{name}` is not set (or not valid UTF-8); export it before running")
    })
}

/// Whether a PR refresh needs to rewrite the title/body: `true` iff either the
/// title or the body differs from the current values.
fn update_needed(current_title: &str, current_body: &str, new_title: &str, new_body: &str) -> bool {
    current_title != new_title || current_body != new_body
}

/// `GET` URL for the open PR with the given `head`/`base`. GitHub qualifies the
/// `head` branch with the owner: `{owner}:{head}`.
fn pulls_list_url(repo: &Repo, head: &str, base: &str) -> String {
    format!(
        "{}/pulls?state=open&head={}:{}&base={}",
        repo.rest_base(),
        repo.owner,
        head,
        base
    )
}

/// `POST` URL to create a PR.
fn pulls_create_url(repo: &Repo) -> String {
    format!("{}/pulls", repo.rest_base())
}

/// `PATCH` URL to update an existing PR by number.
fn pull_update_url(repo: &Repo, number: u64) -> String {
    format!("{}/pulls/{number}", repo.rest_base())
}

/// `POST` URL to create a release.
fn releases_create_url(repo: &Repo) -> String {
    format!("{}/releases", repo.rest_base())
}

/// JSON body for `POST /pulls`.
fn create_pr_body(head: &str, base: &str, title: &str, body: &str) -> Value {
    json!({ "title": title, "head": head, "base": base, "body": body })
}

/// JSON body for `PATCH /pulls/{number}` (title/body only).
fn update_pr_body(title: &str, body: &str) -> Value {
    json!({ "title": title, "body": body })
}

/// JSON body for `POST /releases`.
fn create_release_body(tag: &str, name: &str, body: &str, prerelease: bool) -> Value {
    json!({ "tag_name": tag, "name": name, "body": body, "prerelease": prerelease })
}

/// Apply the common GitHub headers (auth, accept, user-agent) to a request.
fn auth(req: ureq::Request, token: &str) -> ureq::Request {
    req.set("Authorization", &format!("Bearer {token}"))
        .set("Accept", ACCEPT)
        .set("User-Agent", USER_AGENT)
        .set("X-GitHub-Api-Version", "2022-11-28")
}

/// Find an existing open PR for `head` → `base`; create it if none exists; if one
/// exists, rewrite its title/body **only if they differ**.
///
/// Side effects: network. Reads the token value from the env var named
/// `github_token_env`. Returns the PR's `html_url`.
pub fn open_or_update_pr(
    repo: &Repo,
    head: &str,
    base: &str,
    title: &str,
    body: &str,
    github_token_env: &str,
) -> Result<PrRef> {
    let token = token_from_env(github_token_env)?;

    // Look for an existing open PR for this head/base.
    let list: Value = auth(ureq::get(&pulls_list_url(repo, head, base)), &token)
        .call()
        .map_err(|e| status_error("GitHub: list open pull requests", e))?
        .into_json()
        .context("GitHub: decode pull-request list")?;

    let existing = list.as_array().and_then(|a| a.first());

    match existing {
        None => {
            // No open PR: create one.
            let created = create_pr_with_retry(repo, head, base, title, body, &token)?;
            Ok(PrRef {
                url: json_str(&created, "html_url")?,
            })
        }
        Some(pr) => {
            let number = json_u64(pr, "number")?;
            let url = json_str(pr, "html_url")?;
            let cur_title = pr.get("title").and_then(Value::as_str).unwrap_or("");
            let cur_body = pr.get("body").and_then(Value::as_str).unwrap_or("");

            // Rewrite the title/body only when they differ (no PATCH otherwise).
            if update_needed(cur_title, cur_body, title, body) {
                auth(ureq::patch(&pull_update_url(repo, number)), &token)
                    .send_json(update_pr_body(title, body))
                    .map_err(|e| status_error("GitHub: update pull request", e))?;
            }
            Ok(PrRef { url })
        }
    }
}

/// Number of extra attempts to create the PR while riding out the post-push race.
const PR_CREATE_RETRIES: u32 = 5;

/// `POST /pulls`, retrying only the transient post-push race.
///
/// Immediately after `git push`, GitHub's pulls API can still answer
/// `422 "No commits between <base> and <head>"` because the pushed branch tip has
/// not yet propagated to that endpoint's view. That specific 422 is retried with a
/// linear backoff; every other error (including a *non*-race 422 such as "A pull
/// request already exists") is surfaced immediately, with GitHub's response body.
fn create_pr_with_retry(
    repo: &Repo,
    head: &str,
    base: &str,
    title: &str,
    body: &str,
    token: &str,
) -> Result<Value> {
    let mut attempt: u32 = 0;
    loop {
        match auth(ureq::post(&pulls_create_url(repo)), token)
            .send_json(create_pr_body(head, base, title, body))
        {
            Ok(resp) => return resp.into_json().context("GitHub: decode created pull request"),
            Err(ureq::Error::Status(422, resp)) if attempt < PR_CREATE_RETRIES => {
                let detail = resp.into_string().unwrap_or_default();
                if !is_no_commits_race(&detail) {
                    // Not the propagation race — a real validation failure.
                    return Err(anyhow!(
                        "GitHub: create pull request: HTTP 422: {}",
                        detail.trim()
                    ));
                }
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_secs(2 * attempt as u64));
            }
            Err(e) => return Err(status_error("GitHub: create pull request", e)),
        }
    }
}

/// True iff a `422` body is the post-push "No commits between …" propagation race
/// (case-insensitive), as opposed to a permanent validation failure.
fn is_no_commits_race(body: &str) -> bool {
    body.to_lowercase().contains("no commits between")
}

/// Convert a ureq error into an [`anyhow::Error`] that includes the response
/// **body** — GitHub's 422 validation details live in the body, not the status
/// line, so `.context()` alone (which only sees "status code 422") is useless for
/// debugging.
fn status_error(ctx: &str, err: ureq::Error) -> anyhow::Error {
    match err {
        ureq::Error::Status(code, resp) => {
            let body = resp
                .into_string()
                .unwrap_or_else(|_| "<unreadable body>".to_string());
            anyhow!("{ctx}: HTTP {code}: {}", body.trim())
        }
        other => anyhow!("{ctx}: {other}"),
    }
}

/// Resolve the git author/committer identity from the GitHub token owner — the
/// way release-plz attributes the release commit and tags: the account owning the
/// token becomes the author. `GET`s `/user` and returns the login plus the GitHub
/// "noreply" email (`<id>+<login>@users.noreply.github.com`), which attributes the
/// commit to that account without exposing a private address.
///
/// Returns `Ok(None)` when the token is **not** associated with a user account —
/// i.e. the default GitHub Actions `GITHUB_TOKEN` (an installation token), for
/// which `/user` responds `403`/`404`. Callers then fall back to the ambient git
/// identity. Only a genuine network/decode failure, or an unset token env var, is
/// `Err`.
///
/// Side effects: network. Reads the token from the env var named
/// `github_token_env`.
pub fn token_committer(github_token_env: &str) -> Result<Option<GitIdentity>> {
    let token = token_from_env(github_token_env)?;
    match auth(ureq::get(&format!("{API_BASE}/user")), &token).call() {
        Ok(resp) => {
            let user: Value = resp.into_json().context("GitHub: decode /user")?;
            let login = json_str(&user, "login")?;
            let id = json_u64(&user, "id")?;
            Ok(Some(GitIdentity {
                name: login.clone(),
                email: format!("{id}+{login}@users.noreply.github.com"),
            }))
        }
        // Installation token (default GITHUB_TOKEN): no user is associated, so
        // GitHub answers 403 (or 404). Not an error — fall back to ambient config.
        Err(ureq::Error::Status(403, _)) | Err(ureq::Error::Status(404, _)) => Ok(None),
        Err(e) => Err(e).context("GitHub: GET /user"),
    }
}

/// Create a GitHub release for `tag`. The `prerelease` flag is decided by the
/// caller from the semver (see [`is_prerelease`]); `body` is the crate's
/// changelog entry.
///
/// Graceful existing-release handling: if a release for the tag already exists
/// (GitHub returns HTTP 422 `already_exists`), the call is treated as an
/// idempotent no-op success instead of erroring, so the publish step is safe to
/// re-run (the orphan-tag fix's idempotency requirement).
///
/// Side effects: network. Reads the token from the env var named
/// `github_token_env`.
pub fn create_release(
    repo: &Repo,
    tag: &str,
    name: &str,
    body: &str,
    prerelease: bool,
    github_token_env: &str,
) -> Result<()> {
    let token = token_from_env(github_token_env)?;

    let resp = auth(ureq::post(&releases_create_url(repo)), &token)
        .send_json(create_release_body(tag, name, body, prerelease));

    match resp {
        Ok(_) => Ok(()),
        // GitHub answers 422 when a release for the tag already exists. That is an
        // idempotent success — re-running the release must not fail here.
        Err(ureq::Error::Status(422, _)) => Ok(()),
        Err(e) => Err(status_error("GitHub: create release", e)),
    }
}

/// One file added or modified by a [`create_commit_on_branch`] call: a repo-relative
/// path plus its full new byte content (base64-encoded for the API by the function).
#[derive(Debug, Clone)]
pub struct FileAddition {
    /// Repo-relative path, e.g. `wasm/js/package.json`.
    pub path: String,
    /// The file's full new content (encoded to base64 before sending).
    pub contents: Vec<u8>,
}

/// GraphQL mutation creating one commit on `branch` via GitHub's
/// `createCommitOnBranch`. Commits made this way are **signed by GitHub** and show
/// as "Verified", which is why the release commit is created here instead of with a
/// local `git commit` (CI runners have no signing key).
const CREATE_COMMIT_MUTATION: &str = "mutation($input: CreateCommitOnBranchInput!) { \
     createCommitOnBranch(input: $input) { commit { oid } } }";

/// Create a single GitHub-signed commit on `branch` with the given file `additions`
/// and `deletions`, and return its commit SHA (`oid`).
///
/// `expected_head_oid` must equal the branch's current tip (an optimistic-concurrency
/// guard GitHub enforces): the caller first points the remote `branch` at the base
/// commit, then passes that base SHA here. `headline` is the commit message subject.
/// The commit's author/committer is the account that owns the token (read from the
/// env var **named** by `github_token_env`), matching the release-plz attribution
/// model — and, because it is an API commit, GitHub signs it.
///
/// Side effects: network. The token value is never logged.
pub fn create_commit_on_branch(
    repo: &Repo,
    branch: &str,
    expected_head_oid: &str,
    headline: &str,
    additions: &[FileAddition],
    deletions: &[String],
    github_token_env: &str,
) -> Result<String> {
    let token = token_from_env(github_token_env)?;

    let additions_json: Vec<Value> = additions
        .iter()
        .map(|a| {
            json!({
                "path": a.path,
                "contents": base64::engine::general_purpose::STANDARD.encode(&a.contents),
            })
        })
        .collect();
    let deletions_json: Vec<Value> = deletions.iter().map(|p| json!({ "path": p })).collect();

    let input = json!({
        "branch": {
            "repositoryNameWithOwner": repo.name_with_owner(),
            "branchName": branch,
        },
        "message": { "headline": headline },
        "expectedHeadOid": expected_head_oid,
        "fileChanges": { "additions": additions_json, "deletions": deletions_json },
    });
    let body = json!({ "query": CREATE_COMMIT_MUTATION, "variables": { "input": input } });

    let resp: Value = auth(ureq::post(GRAPHQL_URL), &token)
        .send_json(body)
        .map_err(|e| status_error("GitHub: createCommitOnBranch", e))?
        .into_json()
        .context("GitHub: decode createCommitOnBranch response")?;

    // GraphQL reports failures in a top-level `errors` array with HTTP 200, so a
    // successful HTTP status does not imply the mutation succeeded.
    if let Some(errors) = resp.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            bail!("GitHub: createCommitOnBranch returned errors: {errors:?}");
        }
    }

    resp.pointer("/data/createCommitOnBranch/commit/oid")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("GitHub: createCommitOnBranch response missing commit oid"))
}

/// Read a required string field from a JSON object.
fn json_str(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("GitHub response missing string field `{key}`"))
}

/// Read a required `u64` field from a JSON object.
fn json_u64(v: &Value, key: &str) -> Result<u64> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("GitHub response missing numeric field `{key}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> Repo {
        Repo {
            owner: "mcrakhman".to_string(),
            name: "toy-kv".to_string(),
        }
    }

    #[test]
    fn is_prerelease_false_for_stable() {
        let v = semver::Version::parse("1.2.0").unwrap();
        assert!(!is_prerelease(&v));
    }

    #[test]
    fn is_prerelease_true_for_rc() {
        let v = semver::Version::parse("1.2.0-rc.1").unwrap();
        assert!(is_prerelease(&v));
    }

    #[test]
    fn update_needed_false_when_unchanged() {
        assert!(!update_needed("t", "b", "t", "b"));
    }

    #[test]
    fn update_needed_true_when_title_changed() {
        assert!(update_needed("old", "b", "new", "b"));
    }

    #[test]
    fn update_needed_true_when_body_changed() {
        assert!(update_needed("t", "old", "t", "new"));
    }

    #[test]
    fn pulls_list_url_encodes_owner_qualified_head() {
        assert_eq!(
            pulls_list_url(&repo(), "release-0.2.0", "main"),
            "https://api.github.com/repos/mcrakhman/toy-kv/pulls?state=open&head=mcrakhman:release-0.2.0&base=main"
        );
    }

    #[test]
    fn pulls_create_url_is_repo_pulls() {
        assert_eq!(
            pulls_create_url(&repo()),
            "https://api.github.com/repos/mcrakhman/toy-kv/pulls"
        );
    }

    #[test]
    fn pull_update_url_includes_number() {
        assert_eq!(
            pull_update_url(&repo(), 42),
            "https://api.github.com/repos/mcrakhman/toy-kv/pulls/42"
        );
    }

    #[test]
    fn releases_create_url_is_repo_releases() {
        assert_eq!(
            releases_create_url(&repo()),
            "https://api.github.com/repos/mcrakhman/toy-kv/releases"
        );
    }

    #[test]
    fn create_pr_body_shape() {
        let v = create_pr_body("release-0.2.0", "main", "chore: release v0.2.0", "BODY");
        assert_eq!(v["title"], "chore: release v0.2.0");
        assert_eq!(v["head"], "release-0.2.0");
        assert_eq!(v["base"], "main");
        assert_eq!(v["body"], "BODY");
        // Exactly the four expected keys.
        assert_eq!(v.as_object().unwrap().len(), 4);
    }

    #[test]
    fn update_pr_body_shape_is_title_and_body_only() {
        let v = update_pr_body("T", "B");
        assert_eq!(v["title"], "T");
        assert_eq!(v["body"], "B");
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert!(!obj.contains_key("head"));
        assert!(!obj.contains_key("base"));
    }

    #[test]
    fn create_release_body_carries_prerelease_flag() {
        let pre = create_release_body("v1.2.0-rc.1", "v1.2.0-rc.1", "notes", true);
        assert_eq!(pre["tag_name"], "v1.2.0-rc.1");
        assert_eq!(pre["name"], "v1.2.0-rc.1");
        assert_eq!(pre["body"], "notes");
        assert_eq!(pre["prerelease"], true);

        let stable = create_release_body("v1.2.0", "v1.2.0", "notes", false);
        assert_eq!(stable["prerelease"], false);
    }

    #[test]
    fn is_no_commits_race_matches_github_wording() {
        // The transient post-push 422 body GitHub returns (case-insensitive).
        assert!(is_no_commits_race(
            "{\"message\":\"Validation Failed\",\"errors\":[{\"resource\":\"PullRequest\",\"message\":\"No commits between main and release-0.1.0-rc.1\"}]}"
        ));
        assert!(is_no_commits_race("No Commits Between main and x"));
        // A different 422 (already-exists) is NOT the race → surfaced, not retried.
        assert!(!is_no_commits_race(
            "A pull request already exists for mcrakhman:release-0.1.0-rc.1"
        ));
        assert!(!is_no_commits_race(""));
    }

    #[test]
    fn token_from_env_resolves_named_var() {
        // Unique name to avoid clashing with other tests running in parallel.
        let name = "TOYKV_FORGE_TEST_TOKEN_RESOLVE";
        // SAFETY: single-threaded test-local mutation of a uniquely named var.
        unsafe { std::env::set_var(name, "s3cr3t") };
        let got = token_from_env(name).unwrap();
        assert_eq!(got, "s3cr3t");
        unsafe { std::env::remove_var(name) };
    }

    #[test]
    fn token_from_env_errors_when_missing() {
        let name = "TOYKV_FORGE_TEST_TOKEN_MISSING";
        // SAFETY: ensure it is unset; uniquely named so no other test touches it.
        unsafe { std::env::remove_var(name) };
        let err = token_from_env(name).unwrap_err();
        let msg = err.to_string();
        // Error names the env var but never a token value.
        assert!(msg.contains(name));
        assert!(!msg.contains("s3cr3t"));
    }
}
