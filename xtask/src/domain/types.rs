use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
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

#[derive(Debug, Clone)]
pub struct PublishContext {
    pub auth: AuthContext,
    pub no_artifacts: bool,
}

#[derive(Debug, Clone)]
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
pub struct ExecuteReport {
    pub head_branch: String,
    pub pr_url: Option<String>,
    pub updated_packages: Vec<UpdatedPackage>,
}
