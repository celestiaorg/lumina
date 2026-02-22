use anyhow::Result;

use crate::domain::types::{
    AuthContext, BranchState, PublishContext, PullRequestInfo, ReleaseMode, UpdatedPackage,
};

/// Trait abstracting git repository operations used by the prepare/submit pipeline stages.
pub(crate) trait GitRepo {
    fn branch_state(&self, branch_name: &str) -> Result<BranchState>;
    fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>>;
    fn stage_all_and_commit(&self, message: &str, dry_run: bool) -> Result<()>;
    fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()>;
}

/// Trait abstracting GitHub PR operations used by the submit pipeline stage.
pub(crate) trait PrClient {
    async fn close_stale_open_release_prs(
        &self,
        auth: &AuthContext,
        skip_pr: bool,
        keep_branch: Option<&str>,
    ) -> Result<Vec<PullRequestInfo>>;

    async fn ensure_release_pr(
        &self,
        mode: ReleaseMode,
        default_branch: &str,
        auth: &AuthContext,
        skip_pr: bool,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>>;
}

/// Trait abstracting release engine operations (update artifacts, publishing).
pub(crate) trait ReleaseEngine {
    async fn update(&self, mode: ReleaseMode) -> Result<Vec<UpdatedPackage>>;
    async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value>;
}

// ── Implementations for concrete adapters ────────────────────────────────

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_pr::GitHubPrClient;
use crate::adapters::release_plz::ReleasePlzAdapter;

impl GitRepo for Git2Repo {
    fn branch_state(&self, branch_name: &str) -> Result<BranchState> {
        self.branch_state(branch_name)
    }

    fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        self.create_release_branch_from_default(branch_name, default_branch)
    }

    fn stage_all_and_commit(&self, message: &str, dry_run: bool) -> Result<()> {
        self.stage_all_and_commit(message, dry_run)
    }

    fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()> {
        self.push_branch(branch_name, force, dry_run)
    }
}

impl PrClient for GitHubPrClient {
    async fn close_stale_open_release_prs(
        &self,
        auth: &AuthContext,
        skip_pr: bool,
        keep_branch: Option<&str>,
    ) -> Result<Vec<PullRequestInfo>> {
        self.close_stale_open_release_prs(auth, skip_pr, keep_branch)
            .await
    }

    async fn ensure_release_pr(
        &self,
        mode: ReleaseMode,
        default_branch: &str,
        auth: &AuthContext,
        skip_pr: bool,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>> {
        self.ensure_release_pr(mode, default_branch, auth, skip_pr, branch_name)
            .await
    }
}

impl ReleaseEngine for ReleasePlzAdapter {
    async fn update(&self, mode: ReleaseMode) -> Result<Vec<UpdatedPackage>> {
        self.update(mode).await
    }

    async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        self.publish(ctx).await
    }
}
