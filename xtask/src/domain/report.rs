use serde::{Deserialize, Serialize};

use crate::domain::model::{ReleaseMode, UpdatedPackage};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteReport {
    pub head_branch: String,
    pub pr_url: Option<String>,
    pub updated_packages: Vec<UpdatedPackage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseReport {
    pub mode: ReleaseMode,
    pub published: bool,
    pub payload: serde_json::Value,
}
