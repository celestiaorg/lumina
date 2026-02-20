use std::cell::RefCell;
use std::collections::VecDeque;

use anyhow::Result;

use crate::domain::types::{
    AuthContext, BranchState, ComputeVersionsContext, PlannedVersion, PublishContext,
    PullRequestInfo, ReleaseMode, VersionsReport,
};

use super::pipeline_ops::{GitRepo, PrClient, ReleaseEngine};

// ── Call recording ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GitCall {
    BranchState {
        branch_name: String,
    },
    CreateReleaseBranch {
        branch_name: String,
        default_branch: String,
    },
    StageAllAndCommit {
        message: String,
        dry_run: bool,
    },
    PushBranch {
        branch_name: String,
        force: bool,
        dry_run: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PrCall {
    CloseStaleReleasePrs {
        skip_pr: bool,
        keep_branch: Option<String>,
    },
    EnsureReleasePr {
        mode: ReleaseMode,
        default_branch: String,
        skip_pr: bool,
        branch_name: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EngineCall {
    ComputeVersions {
        mode: ReleaseMode,
    },
    RegenerateArtifacts {
        mode: ReleaseMode,
        previous_commit: String,
        latest_release_tag: Option<String>,
        latest_non_rc_release_tag: Option<String>,
    },
    Publish {
        mode: ReleaseMode,
        no_artifacts: bool,
    },
}

// ── MockGitRepo ──────────────────────────────────────────────────────────

pub(crate) struct MockGitRepo {
    calls: RefCell<Vec<GitCall>>,
    branch_states: RefCell<VecDeque<BranchState>>,
    create_branch_results: RefCell<VecDeque<Result<Vec<String>>>>,
    stage_commit_results: RefCell<VecDeque<Result<()>>>,
    push_results: RefCell<VecDeque<Result<()>>>,
}

impl MockGitRepo {
    pub(crate) fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            branch_states: RefCell::new(VecDeque::new()),
            create_branch_results: RefCell::new(VecDeque::new()),
            stage_commit_results: RefCell::new(VecDeque::new()),
            push_results: RefCell::new(VecDeque::new()),
        }
    }

    pub(crate) fn with_branch_state(self, state: BranchState) -> Self {
        self.branch_states.borrow_mut().push_back(state);
        self
    }

    pub(crate) fn with_create_branch_descriptions(self, descriptions: Vec<&str>) -> Self {
        self.create_branch_results
            .borrow_mut()
            .push_back(Ok(descriptions.into_iter().map(String::from).collect()));
        self
    }

    pub(crate) fn with_create_branch_error(self, msg: &str) -> Self {
        self.create_branch_results
            .borrow_mut()
            .push_back(Err(anyhow::anyhow!("{}", msg)));
        self
    }

    pub(crate) fn with_stage_commit_ok(self) -> Self {
        self.stage_commit_results.borrow_mut().push_back(Ok(()));
        self
    }

    pub(crate) fn with_stage_commit_error(self, msg: &str) -> Self {
        self.stage_commit_results
            .borrow_mut()
            .push_back(Err(anyhow::anyhow!("{}", msg)));
        self
    }

    pub(crate) fn with_push_ok(self) -> Self {
        self.push_results.borrow_mut().push_back(Ok(()));
        self
    }

    pub(crate) fn with_push_error(self, msg: &str) -> Self {
        self.push_results
            .borrow_mut()
            .push_back(Err(anyhow::anyhow!("{}", msg)));
        self
    }

    pub(crate) fn calls(&self) -> Vec<GitCall> {
        self.calls.borrow().clone()
    }
}

impl GitRepo for MockGitRepo {
    fn branch_state(&self, branch_name: &str) -> Result<BranchState> {
        self.calls.borrow_mut().push(GitCall::BranchState {
            branch_name: branch_name.to_string(),
        });
        Ok(self
            .branch_states
            .borrow_mut()
            .pop_front()
            .unwrap_or(BranchState::Missing))
    }

    fn create_release_branch_from_default(
        &self,
        branch_name: &str,
        default_branch: &str,
    ) -> Result<Vec<String>> {
        self.calls.borrow_mut().push(GitCall::CreateReleaseBranch {
            branch_name: branch_name.to_string(),
            default_branch: default_branch.to_string(),
        });
        self.create_branch_results
            .borrow_mut()
            .pop_front()
            .unwrap_or(Ok(vec!["created branch".to_string()]))
    }

    fn stage_all_and_commit(&self, message: &str, dry_run: bool) -> Result<()> {
        self.calls.borrow_mut().push(GitCall::StageAllAndCommit {
            message: message.to_string(),
            dry_run,
        });
        self.stage_commit_results
            .borrow_mut()
            .pop_front()
            .unwrap_or(Ok(()))
    }

    fn push_branch(&self, branch_name: &str, force: bool, dry_run: bool) -> Result<()> {
        self.calls.borrow_mut().push(GitCall::PushBranch {
            branch_name: branch_name.to_string(),
            force,
            dry_run,
        });
        self.push_results.borrow_mut().pop_front().unwrap_or(Ok(()))
    }
}

// ── MockPrClient ─────────────────────────────────────────────────────────

pub(crate) struct MockPrClient {
    calls: RefCell<Vec<PrCall>>,
    close_stale_results: RefCell<VecDeque<Vec<PullRequestInfo>>>,
    ensure_pr_results: RefCell<VecDeque<Option<PullRequestInfo>>>,
}

impl MockPrClient {
    pub(crate) fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            close_stale_results: RefCell::new(VecDeque::new()),
            ensure_pr_results: RefCell::new(VecDeque::new()),
        }
    }

    pub(crate) fn with_close_stale_result(self, prs: Vec<PullRequestInfo>) -> Self {
        self.close_stale_results.borrow_mut().push_back(prs);
        self
    }

    pub(crate) fn with_ensure_pr_result(self, pr: Option<PullRequestInfo>) -> Self {
        self.ensure_pr_results.borrow_mut().push_back(pr);
        self
    }

    pub(crate) fn calls(&self) -> Vec<PrCall> {
        self.calls.borrow().clone()
    }
}

impl PrClient for MockPrClient {
    async fn close_stale_open_release_prs(
        &self,
        _auth: &AuthContext,
        skip_pr: bool,
        keep_branch: Option<&str>,
    ) -> Result<Vec<PullRequestInfo>> {
        self.calls.borrow_mut().push(PrCall::CloseStaleReleasePrs {
            skip_pr,
            keep_branch: keep_branch.map(String::from),
        });
        Ok(self
            .close_stale_results
            .borrow_mut()
            .pop_front()
            .unwrap_or_default())
    }

    async fn ensure_release_pr(
        &self,
        mode: ReleaseMode,
        default_branch: &str,
        _auth: &AuthContext,
        skip_pr: bool,
        branch_name: &str,
    ) -> Result<Option<PullRequestInfo>> {
        self.calls.borrow_mut().push(PrCall::EnsureReleasePr {
            mode,
            default_branch: default_branch.to_string(),
            skip_pr,
            branch_name: branch_name.to_string(),
        });
        Ok(self.ensure_pr_results.borrow_mut().pop_front().flatten())
    }
}

// ── MockReleaseEngine ────────────────────────────────────────────────────

pub(crate) struct MockReleaseEngine {
    calls: RefCell<Vec<EngineCall>>,
    versions_results: RefCell<VecDeque<Result<VersionsReport>>>,
    regenerate_results: RefCell<VecDeque<Vec<String>>>,
    publish_results: RefCell<VecDeque<serde_json::Value>>,
}

impl MockReleaseEngine {
    pub(crate) fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            versions_results: RefCell::new(VecDeque::new()),
            regenerate_results: RefCell::new(VecDeque::new()),
            publish_results: RefCell::new(VecDeque::new()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_versions_result(self, report: VersionsReport) -> Self {
        self.versions_results.borrow_mut().push_back(Ok(report));
        self
    }

    pub(crate) fn with_regenerate_descriptions(self, descriptions: Vec<&str>) -> Self {
        self.regenerate_results
            .borrow_mut()
            .push_back(descriptions.into_iter().map(String::from).collect());
        self
    }

    pub(crate) fn with_publish_payload(self, payload: serde_json::Value) -> Self {
        self.publish_results.borrow_mut().push_back(payload);
        self
    }

    pub(crate) fn calls(&self) -> Vec<EngineCall> {
        self.calls.borrow().clone()
    }
}

impl ReleaseEngine for MockReleaseEngine {
    async fn compute_versions(&self, ctx: &ComputeVersionsContext) -> Result<VersionsReport> {
        self.calls.borrow_mut().push(EngineCall::ComputeVersions {
            mode: ctx.common.mode,
        });
        self.versions_results
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| Err(anyhow::anyhow!("mock: no versions result queued")))
    }

    async fn regenerate_artifacts(
        &self,
        mode: ReleaseMode,
        _default_branch: &str,
        previous_commit: &str,
        latest_release_tag: Option<&str>,
        latest_non_rc_release_tag: Option<&str>,
        _planned_versions: &[PlannedVersion],
    ) -> Result<Vec<String>> {
        self.calls
            .borrow_mut()
            .push(EngineCall::RegenerateArtifacts {
                mode,
                previous_commit: previous_commit.to_string(),
                latest_release_tag: latest_release_tag.map(String::from),
                latest_non_rc_release_tag: latest_non_rc_release_tag.map(String::from),
            });
        Ok(self
            .regenerate_results
            .borrow_mut()
            .pop_front()
            .unwrap_or_default())
    }

    async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        self.calls.borrow_mut().push(EngineCall::Publish {
            mode: ctx.common.mode,
            no_artifacts: ctx.no_artifacts,
        });
        Ok(self
            .publish_results
            .borrow_mut()
            .pop_front()
            .unwrap_or(serde_json::json!([])))
    }
}
