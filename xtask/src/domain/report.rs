use serde::{Deserialize, Serialize};

use crate::domain::model::{
    BranchState, ComparisonVersionView, ExecutionStage, PlanView, ReleaseMode,
    UpdateStrategy, ValidationIssue,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckReport {
    pub mode: ReleaseMode,
    pub baseline_commit: Option<String>,
    pub comparison_commit: String,
    pub comparison_versions: Vec<ComparisonVersionView>,
    pub versions: Vec<PlanView>,
    pub validation_issues: Vec<ValidationIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReport {
    pub mode: ReleaseMode,
    pub branch_name: String,
    pub branch_state: BranchState,
    pub update_strategy: UpdateStrategy,
    pub comparison_commit: String,
    pub comparison_versions: Vec<ComparisonVersionView>,
    pub plans: Vec<PlanView>,
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
    pub check: CheckReport,
    pub prepare: PrepareReport,
    pub submit: SubmitReport,
    pub stage: ExecutionStage,
}
