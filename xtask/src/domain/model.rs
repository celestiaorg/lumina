use serde::{Deserialize, Serialize};

pub const RELEASE_PR_TITLE_PREFIX: &str = "chore: test release";
pub const RELEASE_PR_TITLE_RC: &str = "chore: test release rc";
pub const RELEASE_PR_TITLE_FINAL: &str = "chore: test release final";
pub const RELEASE_PR_BRANCH_PREFIX: &str = "lumina/release-plz";
pub const RELEASE_COMMIT_MESSAGE_RC: &str = "chore(release): prepare rc release";
pub const RELEASE_COMMIT_MESSAGE_FINAL: &str = "chore(release): prepare final release";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedVersion {
    pub package: String,
    pub current: String,
    pub next_effective: String,
    pub publishable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestInfo {
    pub number: u64,
    pub url: String,
}
