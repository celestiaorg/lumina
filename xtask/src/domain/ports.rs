use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;

use crate::domain::types::{
    BranchKind, BranchState, PlanComputation, PullRequestInfo, ReleaseContext, ReleaseMode,
};

#[async_trait(?Send)]
pub trait ReleaseEngine: Send + Sync {
    async fn plan(&self, ctx: &ReleaseContext) -> Result<PlanComputation>;
    fn workspace_root(&self) -> &Path;
    async fn regenerate_artifacts(&self, ctx: &ReleaseContext) -> Result<Vec<String>>;
}

#[async_trait(?Send)]
pub trait Publisher: Send + Sync {
    async fn publish(&self, ctx: &ReleaseContext) -> Result<serde_json::Value>;
}

pub trait GitRepo: Send + Sync {
    fn branch_state(&self, branch_name: &str) -> Result<BranchState>;
    fn validate_branch_name(
        &self,
        mode: ReleaseMode,
        branch_name: &str,
        rc_prefix: &str,
    ) -> Result<BranchKind>;
    fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>>;
    fn refresh_existing_release_branch(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>>;
    fn stage_all_and_commit(&self, message: &str, dry_run: bool) -> Result<()>;
    fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()>;
    fn create_branch_from_current(&self, branch_name: &str, dry_run: bool) -> Result<()>;
}

#[async_trait(?Send)]
pub trait PrClient: Send + Sync {
    async fn has_external_contributors_on_open_release_pr(
        &self,
        ctx: &ReleaseContext,
        branch_name: &str,
    ) -> Result<bool>;
    async fn close_open_release_pr(
        &self,
        ctx: &ReleaseContext,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>>;
    async fn ensure_release_pr(
        &self,
        ctx: &ReleaseContext,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>>;
}
