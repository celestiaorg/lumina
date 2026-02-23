use std::path::Path;

use anyhow::{Context, Result};
use git2::Repository;

pub fn parse_remote_repo_url(workspace_root: &Path) -> Result<Option<release_plz_core::RepoUrl>> {
    let Some(remote) = remote_origin_url(workspace_root)? else {
        return Ok(None);
    };
    let repo_url = release_plz_core::RepoUrl::new(&remote)
        .with_context(|| format!("failed to parse repository URL from `{remote}`"))?;
    Ok(Some(repo_url))
}

pub fn remote_origin_url(workspace_root: &Path) -> Result<Option<String>> {
    let repo = Repository::open(workspace_root)
        .with_context(|| format!("failed to open repository at {}", workspace_root.display()))?;

    let remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => return Ok(None),
    };

    Ok(remote.url().map(|url| url.to_string()))
}
