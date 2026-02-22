use serde::{Deserialize, Serialize};

use crate::domain::model::{BranchState, ReleaseMode, UpdatedPackage};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub branch_state: BranchState,
    pub updated_packages: Vec<UpdatedPackage>,
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
    pub prepare: PrepareReport,
    pub submit: SubmitReport,
}
