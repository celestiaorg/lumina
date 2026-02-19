use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{debug, info};

use crate::adapters::git_refs::remote_origin_url;
use crate::domain::model::{RELEASE_PR_TITLE_FINAL, RELEASE_PR_TITLE_PREFIX, RELEASE_PR_TITLE_RC};
use crate::domain::types::{AuthContext, PullRequestInfo, ReleaseMode};

#[derive(Debug, Clone)]
pub struct GitHubPrClient {
    /// Workspace root used to resolve repository remote URL.
    workspace_root: PathBuf,
}

impl GitHubPrClient {
    /// Builds PR client bound to repository at workspace root.
    pub fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    /// Finds currently open release PR for a branch, if present.
    async fn find_open_release_pr(
        &self,
        client: &release_plz_core::GitClient,
        branch_name: &str,
    ) -> Result<Option<release_plz_core::GitPr>> {
        let prs = client
            .opened_prs("")
            .await
            .context("failed to list opened PRs")?;
        Ok(prs.into_iter().find(|pr| pr.branch() == branch_name))
    }

    /// Lists open release PRs by title heuristic.
    async fn open_release_prs(
        &self,
        client: &release_plz_core::GitClient,
    ) -> Result<Vec<release_plz_core::GitPr>> {
        let prs = client
            .opened_prs("")
            .await
            .context("failed to list opened PRs")?;
        Ok(prs
            .into_iter()
            .filter(|pr| is_release_pr_candidate(pr))
            .collect())
    }

    /// Creates authenticated GitHub API client from available token + origin URL.
    fn git_client(&self, auth: &AuthContext) -> Result<Option<release_plz_core::GitClient>> {
        // Prefer dedicated release token, fallback to GitHub token.
        let token = auth
            .release_plz_token
            .clone()
            .or_else(|| auth.github_token.clone());
        let Some(token) = token else {
            debug!("github_pr: no token available, skipping GitHub client init");
            return Ok(None);
        };

        let Some(repo_url) = self.remote_repo_url()? else {
            debug!("github_pr: no origin repository URL available, skipping GitHub client init");
            return Ok(None);
        };

        // Build release-plz GitHub client bound to origin owner/repo.
        let client = release_plz_core::GitClient::new(release_plz_core::GitForge::Github(
            release_plz_core::GitHub::new(repo_url.owner, repo_url.name, token.into()),
        ))
        .context("failed to build GitHub client")?;
        debug!("github_pr: initialized GitHub client");
        Ok(Some(client))
    }

    /// Resolves repository owner/name from `origin` URL for API operations.
    fn remote_repo_url(&self) -> Result<Option<release_plz_core::RepoUrl>> {
        remote_repo_url(&self.workspace_root)
    }

    /// Returns true when open release PR has non-bot contributors beyond initial author commit.
    pub async fn has_external_contributors_on_open_release_pr(
        &self,
        auth: &AuthContext,
        branch_name: &str,
    ) -> Result<bool> {
        debug!(branch=%branch_name, "github_pr: checking external contributors");
        // If auth or remote metadata is unavailable, treat as "no external contributors".
        let Some(client) = self.git_client(auth)? else {
            return Ok(false);
        };
        let Some(pr) = self.find_open_release_pr(&client, branch_name).await? else {
            return Ok(false);
        };

        let commits = client
            .pr_commits(pr.number)
            .await
            .with_context(|| format!("failed to load commits for PR #{}", pr.number))?;

        // Skip PR author commit and bots, then deduplicate.
        let mut contributors = commits
            .into_iter()
            .skip(1)
            .filter_map(|commit| commit.author.map(|author| author.login))
            .filter(|login| !login.ends_with("[bot]"))
            .collect::<Vec<_>>();
        contributors.sort();
        contributors.dedup();

        let has_external = !contributors.is_empty();
        info!(
            branch=%branch_name,
            contributors=contributors.len(),
            has_external_contributors=has_external,
            "github_pr: external contributor check completed"
        );
        Ok(has_external)
    }

    /// Closes all open release PRs except optional branch to keep.
    pub async fn close_stale_open_release_prs(
        &self,
        auth: &AuthContext,
        skip_pr: bool,
        keep_branch: Option<&str>,
    ) -> Result<Vec<PullRequestInfo>> {
        if skip_pr {
            debug!("github_pr: skip_pr=true, skipping stale release PR closing");
            return Ok(vec![]);
        }
        let Some(client) = self.git_client(auth)? else {
            return Ok(vec![]);
        };

        let keep_branch = keep_branch.map(str::to_string);
        let release_prs = self.open_release_prs(&client).await?;
        let stale_prs = release_prs
            .into_iter()
            .filter(|pr| keep_branch.as_deref() != Some(pr.branch()))
            .collect::<Vec<_>>();

        let mut closed = Vec::new();
        for pr in stale_prs {
            client
                .close_pr(pr.number)
                .await
                .with_context(|| format!("failed to close stale release PR #{}", pr.number))?;
            closed.push(PullRequestInfo {
                number: pr.number,
                url: pr.html_url.to_string(),
            });
        }
        info!(
            keep_branch=?keep_branch,
            closed_prs=closed.len(),
            "github_pr: stale release PR cleanup completed"
        );

        Ok(closed)
    }

    /// Ensures release PR exists for branch: reuses existing open PR or opens a new one.
    pub async fn ensure_release_pr(
        &self,
        mode: ReleaseMode,
        default_branch: &str,
        auth: &AuthContext,
        skip_pr: bool,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>> {
        // Explicitly allow running full flow without PR operations.
        if skip_pr {
            debug!(branch=%branch_name, "github_pr: skip_pr=true, skipping ensure PR");
            return Ok(None);
        }
        let Some(client) = self.git_client(auth)? else {
            return Ok(None);
        };

        // Reuse already open PR for branch when it exists.
        if let Some(existing) = self.find_open_release_pr(&client, branch_name).await? {
            info!(branch=%branch_name, pr_number=existing.number, "github_pr: reusing existing open PR");
            return Ok(Some(PullRequestInfo {
                number: existing.number,
                url: existing.html_url.to_string(),
            }));
        }

        // Otherwise create mode-specific release PR.
        let (title, body) = match mode {
            ReleaseMode::Rc => (
                RELEASE_PR_TITLE_RC,
                "Prepare rc release updates generated by xtask.",
            ),
            ReleaseMode::Final => (
                RELEASE_PR_TITLE_FINAL,
                "Prepare final release updates generated by xtask.",
            ),
        };

        let pr = release_plz_core::Pr {
            base_branch: default_branch.to_string(),
            branch: branch_name.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            draft: false,
            labels: vec![],
        };
        let opened = client
            .open_pr(&pr)
            .await
            .context("failed to open release PR")?;
        info!(
            branch=%branch_name,
            pr_number=opened.number,
            pr_url=%opened.html_url,
            "github_pr: opened release PR"
        );

        Ok(Some(PullRequestInfo {
            number: opened.number,
            url: opened.html_url.to_string(),
        }))
    }
}

fn is_release_pr_candidate(pr: &release_plz_core::GitPr) -> bool {
    let title = pr.title.to_ascii_lowercase();
    title.contains(RELEASE_PR_TITLE_PREFIX)
}

/// Parses origin URL into release-plz repository URL model.
fn remote_repo_url(workspace_root: &Path) -> Result<Option<release_plz_core::RepoUrl>> {
    let Some(remote) = remote_origin_url(workspace_root)? else {
        return Ok(None);
    };

    let repo_url = release_plz_core::RepoUrl::new(&remote)
        .with_context(|| format!("failed to parse repository URL from `{remote}`"))?;
    Ok(Some(repo_url))
}
