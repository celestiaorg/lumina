use serde::{Deserialize, Serialize};

use crate::domain::model::{BranchState, PlannedVersion, ReleaseMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionsReport {
    pub mode: ReleaseMode,
    pub previous_commit: String,
    pub latest_release_tag: Option<String>,
    pub latest_non_rc_release_tag: Option<String>,
    pub current_commit: String,
    pub planned_versions: Vec<PlannedVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub branch_state: BranchState,
    pub description: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub commit_message: String,
    pub pushed: bool,
    pub pr_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseReport {
    pub mode: ReleaseMode,
    pub published: bool,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteReport {
    pub versions: VersionsReport,
    pub prepare: PrepareReport,
    pub submit: SubmitReport,
}
