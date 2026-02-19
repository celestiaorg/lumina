use serde::{Deserialize, Serialize};

use crate::domain::model::{
    BranchState, ComparisonVersionView, ReleaseMode, UpdateStrategy, ValidationIssue,
    VersionEntryView,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionStateReport {
    pub current: Vec<ComparisonVersionView>,
    pub planned: Vec<VersionEntryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckReport {
    pub mode: ReleaseMode,
    pub previous_commit: String,
    pub latest_release_tag: Option<String>,
    pub latest_non_rc_release_tag: Option<String>,
    pub current_commit: String,
    pub version_state: VersionStateReport,
    pub validation_issues: Vec<ValidationIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub branch_state: BranchState,
    pub update_strategy: UpdateStrategy,
    pub current_commit: String,
    pub version_state: VersionStateReport,
    pub description: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub update_strategy: UpdateStrategy,
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
    pub check: CheckReport,
    pub prepare: PrepareReport,
    pub submit: SubmitReport,
}
