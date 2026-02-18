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
pub struct VersionEntry {
    pub package: String,
    pub current: Version,
    pub next_release: Version,
    pub next_effective: Version,
    pub publishable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEntryView {
    pub package: String,
    pub current: String,
    pub next_effective: String,
    pub publishable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonVersionView {
    pub package: String,
    pub version: String,
    pub publishable: bool,
}

#[derive(Debug, Clone)]
pub struct VersionComputation {
    pub previous_commit: Option<String>,
    pub current_commit: String,
    pub current_versions: Vec<ComparisonVersionView>,
    pub versions: Vec<VersionEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestInfo {
    pub number: u64,
    pub url: String,
}

impl VersionEntry {
    /// Converts internal version entry into a stable JSON-facing view.
    pub fn as_view(&self) -> VersionEntryView {
        VersionEntryView {
            package: self.package.clone(),
            current: self.current.to_string(),
            next_effective: self.next_effective.to_string(),
            publishable: self.publishable,
        }
    }
}
