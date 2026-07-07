//! GitHub API primitives over blocking HTTP: derive the repo `owner/name`,
//! open/update the release PR, and create a GitHub release for a tag. The token is
//! read from the env var whose name is passed in; a literal token is never accepted
//! and never logged.

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use serde_json::{Value, json};

use crate::gitops::{Git, GitIdentity};

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
    /// Repository owner (user or org), e.g. `celestiaorg`.
    pub owner: String,
    /// Repository name, e.g. `lumina`.
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

/// Resolve the [`Repo`] from the `origin` remote URL, falling back to the workspace
/// `repository` metadata (root `Cargo.toml`) when `origin` is absent or unparseable.
pub fn derive(git: &Git) -> Result<Repo> {
    if let Some(repo) = git.origin_url().as_deref().and_then(repo_from_url) {
        return Ok(repo);
    }
    if let Some(repo) = workspace_repository_url(git.repo_root()).and_then(|u| repo_from_url(&u)) {
        return Ok(repo);
    }
    Err(anyhow!(
        "could not determine the GitHub owner/name: no parseable `origin` remote URL \
         and no usable workspace `repository` metadata in {}",
        git.repo_root().display()
    ))
}

/// Parse a GitHub remote URL into a [`Repo`]. Accepts https/http, `git@host:owner/repo`,
/// `ssh://git@host/owner/repo`, and `git://` forms, with an optional `.git` suffix and
/// trailing slash. `None` for an unrecognized scheme or an owner-less / over-deep URL.
fn repo_from_url(url: &str) -> Option<Repo> {
    let url = url.trim();
    let tail = if let Some(rest) = url.strip_prefix("git@") {
        rest.split_once(':').map(|(_host, path)| path)?
    } else if let Some(rest) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("ssh://git@"))
        .or_else(|| url.strip_prefix("git://"))
    {
        rest.split_once('/').map(|(_host, path)| path)?
    } else {
        return None;
    };

    let tail = tail.trim_end_matches('/');
    let tail = tail.strip_suffix(".git").unwrap_or(tail);

    let (owner, name) = tail.split_once('/')?;
    if owner.is_empty() || name.is_empty() || name.contains('/') {
        return None;
    }
    Some(Repo {
        owner: owner.to_string(),
        name: name.to_string(),
    })
}

/// Read `[workspace.package].repository` from the root `Cargo.toml`. `None` if the
/// manifest cannot be read/parsed or the field is absent.
fn workspace_repository_url(repo_root: &std::path::Path) -> Option<String> {
    let src = std::fs::read_to_string(repo_root.join("Cargo.toml")).ok()?;
    let doc = src.parse::<toml_edit::DocumentMut>().ok()?;
    doc.get("workspace")?
        .get("package")?
        .get("repository")?
        .as_str()
        .map(str::to_string)
}

/// Result of opening or updating a pull request.
#[derive(Debug, Clone)]
pub struct PrRef {
    /// `html_url` of the PR.
    pub url: String,
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

/// JSON body for `POST /releases`. `target_commitish` pins the commit GitHub
/// creates the tag from when the tag doesn't already exist (it's ignored when the
/// tag is already present).
fn create_release_body(
    tag: &str,
    target_commitish: &str,
    name: &str,
    body: &str,
    prerelease: bool,
) -> Value {
    json!({
        "tag_name": tag,
        "target_commitish": target_commitish,
        "name": name,
        "body": body,
        "prerelease": prerelease,
    })
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
            Ok(resp) => {
                return resp
                    .into_json()
                    .context("GitHub: decode created pull request");
            }
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
/// caller from the semver (see [`crate::version::is_prerelease`]); `body` is the
/// crate's changelog entry.
///
/// Graceful existing-release handling: if a release for the tag already exists
/// (GitHub returns HTTP 422 `already_exists`), the call is treated as an
/// idempotent no-op success instead of erroring, so the publish step is safe to
/// re-run (the orphan-tag fix's idempotency requirement).
///
/// `target_commitish` is the commit the tag is created from (when GitHub has to
/// create it); pass the release SHA so the tag lands on the released commit rather
/// than the default branch's current HEAD. It's ignored when the tag already exists.
///
/// Side effects: network. Reads the token from the env var named
/// `github_token_env`.
pub fn create_release(
    repo: &Repo,
    tag: &str,
    target_commitish: &str,
    name: &str,
    body: &str,
    prerelease: bool,
    github_token_env: &str,
) -> Result<()> {
    let token = token_from_env(github_token_env)?;

    let resp = auth(ureq::post(&releases_create_url(repo)), &token).send_json(create_release_body(
        tag,
        target_commitish,
        name,
        body,
        prerelease,
    ));

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
            owner: "celestiaorg".to_string(),
            name: "lumina".to_string(),
        }
    }

    #[track_caller]
    fn assert_repo(url: &str, owner: &str, name: &str) {
        let r = repo_from_url(url).unwrap_or_else(|| panic!("expected Some for {url:?}"));
        assert_eq!(r.owner, owner, "owner mismatch for {url:?}");
        assert_eq!(r.name, name, "name mismatch for {url:?}");
    }

    #[test]
    fn repo_from_url_parses_all_forms() {
        assert_repo(
            "https://github.com/celestiaorg/lumina",
            "celestiaorg",
            "lumina",
        );
        assert_repo(
            "https://github.com/celestiaorg/lumina.git",
            "celestiaorg",
            "lumina",
        );
        assert_repo(
            "https://github.com/celestiaorg/lumina/",
            "celestiaorg",
            "lumina",
        );
        assert_repo(
            "http://github.com/celestiaorg/lumina",
            "celestiaorg",
            "lumina",
        );
        assert_repo(
            "git@github.com:celestiaorg/lumina.git",
            "celestiaorg",
            "lumina",
        );
        assert_repo("git@github.com:celestiaorg/lumina", "celestiaorg", "lumina");
        assert_repo(
            "ssh://git@github.com/celestiaorg/lumina.git",
            "celestiaorg",
            "lumina",
        );
        assert_repo(
            "git://github.com/celestiaorg/lumina.git",
            "celestiaorg",
            "lumina",
        );
        assert_repo(
            "  https://github.com/celestiaorg/lumina\n",
            "celestiaorg",
            "lumina",
        );
    }

    #[test]
    fn repo_from_url_rejects_garbage() {
        assert!(repo_from_url("not-a-url").is_none());
        assert!(repo_from_url("https://github.com/onlyowner").is_none());
        assert!(repo_from_url("").is_none());
        // Extra path depth is rejected (we never guess).
        assert!(repo_from_url("https://github.com/owner/repo/extra").is_none());
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
            "https://api.github.com/repos/celestiaorg/lumina/pulls?state=open&head=celestiaorg:release-0.2.0&base=main"
        );
    }

    #[test]
    fn pulls_create_url_is_repo_pulls() {
        assert_eq!(
            pulls_create_url(&repo()),
            "https://api.github.com/repos/celestiaorg/lumina/pulls"
        );
    }

    #[test]
    fn pull_update_url_includes_number() {
        assert_eq!(
            pull_update_url(&repo(), 42),
            "https://api.github.com/repos/celestiaorg/lumina/pulls/42"
        );
    }

    #[test]
    fn releases_create_url_is_repo_releases() {
        assert_eq!(
            releases_create_url(&repo()),
            "https://api.github.com/repos/celestiaorg/lumina/releases"
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
        let pre = create_release_body("v1.2.0-rc.1", "deadbeef", "v1.2.0-rc.1", "notes", true);
        assert_eq!(pre["tag_name"], "v1.2.0-rc.1");
        assert_eq!(pre["target_commitish"], "deadbeef");
        assert_eq!(pre["name"], "v1.2.0-rc.1");
        assert_eq!(pre["body"], "notes");
        assert_eq!(pre["prerelease"], true);

        let stable = create_release_body("v1.2.0", "cafef00d", "v1.2.0", "notes", false);
        assert_eq!(stable["target_commitish"], "cafef00d");
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
            "A pull request already exists for celestiaorg:release-0.1.0-rc.1"
        ));
        assert!(!is_no_commits_race(""));
    }

    #[test]
    fn token_from_env_resolves_named_var() {
        // Unique name to avoid clashing with other tests running in parallel.
        let name = "XTASK_FORGE_TEST_TOKEN_RESOLVE";
        // SAFETY: single-threaded test-local mutation of a uniquely named var.
        unsafe { std::env::set_var(name, "s3cr3t") };
        let got = token_from_env(name).unwrap();
        assert_eq!(got, "s3cr3t");
        unsafe { std::env::remove_var(name) };
    }

    #[test]
    fn token_from_env_errors_when_missing() {
        let name = "XTASK_FORGE_TEST_TOKEN_MISSING";
        // SAFETY: ensure it is unset; uniquely named so no other test touches it.
        unsafe { std::env::remove_var(name) };
        let err = token_from_env(name).unwrap_err();
        let msg = err.to_string();
        // Error names the env var but never a token value.
        assert!(msg.contains(name));
        assert!(!msg.contains("s3cr3t"));
    }
}
