use serde::{Deserialize, Serialize};

pub const RELEASE_PR_TITLE_RC: &str = "chore: release rc";
pub const RELEASE_PR_TITLE_FINAL: &str = "chore: release final";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseMode {
    Rc,
    Final,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatedPackage {
    pub package: String,
    pub version: String,
}
