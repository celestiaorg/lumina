use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::adapters::git_refs::remote_origin_url;
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

    /// Creates authenticated GitHub API client from available token + origin URL.
    fn git_client(&self, auth: &AuthContext) -> Result<Option<release_plz_core::GitClient>> {
        // Prefer dedicated release token, fallback to GitHub token.
        let token = auth
            .release_plz_token
            .clone()
            .or_else(|| auth.github_token.clone());
        let Some(token) = token else {
            return Ok(None);
        };

        let Some(repo_url) = self.remote_repo_url()? else {
            return Ok(None);
        };

        // Build release-plz GitHub client bound to origin owner/repo.
        let client = release_plz_core::GitClient::new(release_plz_core::GitForge::Github(
            release_plz_core::GitHub::new(repo_url.owner, repo_url.name, token.into()),
        ))
        .context("failed to build GitHub client")?;
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

        Ok(!contributors.is_empty())
    }

    /// Closes open release PR for branch unless `skip_pr` is enabled.
    pub async fn close_open_release_pr(
        &self,
        auth: &AuthContext,
        skip_pr: bool,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>> {
        // Explicitly allow running full flow without PR operations.
        if skip_pr {
            return Ok(None);
        }
        let Some(client) = self.git_client(auth)? else {
            return Ok(None);
        };
        let Some(pr) = self.find_open_release_pr(&client, branch_name).await? else {
            return Ok(None);
        };

        client
            .close_pr(pr.number)
            .await
            .with_context(|| format!("failed to close PR #{}", pr.number))?;

        Ok(Some(PullRequestInfo {
            number: pr.number,
            url: pr.html_url.to_string(),
        }))
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
            return Ok(None);
        }
        let Some(client) = self.git_client(auth)? else {
            return Ok(None);
        };

        // Reuse already open PR for branch when it exists.
        if let Some(existing) = self.find_open_release_pr(&client, branch_name).await? {
            return Ok(Some(PullRequestInfo {
                number: existing.number,
                url: existing.html_url.to_string(),
            }));
        }

        // Otherwise create mode-specific release PR.
        let (title, body) = match mode {
            ReleaseMode::Rc => (
                "chore: prepare rc release",
                "Prepare rc release updates generated by xtask.",
            ),
            ReleaseMode::Final => (
                "chore: prepare final release",
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

        Ok(Some(PullRequestInfo {
            number: opened.number,
            url: opened.html_url.to_string(),
        }))
    }
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
