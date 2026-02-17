use cargo_metadata::semver::Version;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseMode {
    Rc,
    Final,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum CommandKind {
    Check,
    Prepare,
    Submit,
    Publish,
    Execute,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaselinePolicy {
    TipOfDefaultBranch,
    SpecificCommit(String),
    LatestNonRcReleaseTag,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchKind {
    RcRelease,
    FinalRelease,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchState {
    Missing,
    ExistsClean,
    ExistsDirtyLocal,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RcTransformState {
    NotRcInput,
    ExistingRc { n: u64 },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStrategy {
    RecreateBranch,
    InPlaceForcePush,
    ClosePrAndRecreate,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationIssue {
    DuplicatePublishableResolvedVersions {
        package: String,
        versions: Vec<String>,
    },
    InvalidRcTransition {
        package: String,
        from: String,
        to: String,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStage {
    Checked,
    Prepared,
    Submitted,
    Released,
    Executed,
}

#[derive(Debug, Clone)]
pub struct PackagePlan {
    pub package: String,
    pub current: Version,
    pub next_release: Version,
    pub next_effective: Version,
    pub publishable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackagePlanView {
    pub package: String,
    pub current: String,
    pub next_effective: String,
    pub publishable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonPackageVersionView {
    pub package: String,
    pub version: String,
    pub publishable: bool,
}

impl PackagePlan {
    pub fn as_view(&self) -> PackagePlanView {
        PackagePlanView {
            package: self.package.clone(),
            current: self.current.to_string(),
            next_effective: self.next_effective.to_string(),
            publishable: self.publishable,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ReleaseContext {
    pub mode: ReleaseMode,
    pub base_commit: Option<String>,
    pub default_branch: String,
    pub branch_name: Option<String>,
    pub rc_branch_prefix: String,
    pub final_branch_prefix: String,
    pub skip_pr: bool,
    pub auth: AuthContext,
}

impl ReleaseContext {
    pub fn baseline_policy(&self) -> BaselinePolicy {
        match self.mode {
            ReleaseMode::Rc => match &self.base_commit {
                Some(commit) => BaselinePolicy::SpecificCommit(commit.clone()),
                None => BaselinePolicy::TipOfDefaultBranch,
            },
            ReleaseMode::Final => BaselinePolicy::LatestNonRcReleaseTag,
        }
    }

    #[allow(dead_code)]
    pub fn branch_kind(&self) -> BranchKind {
        match self.mode {
            ReleaseMode::Rc => BranchKind::RcRelease,
            ReleaseMode::Final => BranchKind::FinalRelease,
        }
    }

    #[allow(dead_code)]
    pub fn resolved_branch_name(&self) -> String {
        if let Some(name) = &self.branch_name {
            return name.clone();
        }

        match self.mode {
            ReleaseMode::Rc => format!("{}/{}", self.rc_branch_prefix, self.default_branch),
            ReleaseMode::Final => format!("{}/{}", self.final_branch_prefix, self.default_branch),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AuthContext {
    pub release_plz_token: Option<String>,
    pub github_token: Option<String>,
    pub cargo_registry_token: Option<String>,
}

impl AuthContext {
    pub fn from_env() -> Self {
        Self {
            release_plz_token: std::env::var("RELEASE_PLZ_TOKEN").ok(),
            github_token: std::env::var("GITHUB_TOKEN").ok(),
            cargo_registry_token: std::env::var("CARGO_REGISTRY_TOKEN").ok(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseCheckReport {
    pub mode: ReleaseMode,
    pub baseline_policy: BaselinePolicy,
    pub default_branch: String,
    pub base_commit: Option<String>,
    pub comparison_commit: String,
    pub comparison_versions: Vec<ComparisonPackageVersionView>,
    pub plans: Vec<PackagePlanView>,
    pub strict_simulation_applied: bool,
    pub validation_issues: Vec<ValidationIssue>,
    pub stage: ExecutionStage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub branch_state: BranchState,
    pub update_strategy: UpdateStrategy,
    pub comparison_commit: String,
    pub comparison_versions: Vec<ComparisonPackageVersionView>,
    pub plans: Vec<PackagePlanView>,
    pub actions: Vec<String>,
    pub stage: ExecutionStage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub update_strategy: UpdateStrategy,
    pub commit_message: String,
    pub pushed: bool,
    pub pr_url: Option<String>,
    pub stage: ExecutionStage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseReport {
    pub mode: ReleaseMode,
    pub published: bool,
    pub payload: serde_json::Value,
    pub stage: ExecutionStage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteReport {
    pub check: ReleaseCheckReport,
    pub prepare: PrepareReport,
    pub submit: SubmitReport,
    pub stage: ExecutionStage,
}
