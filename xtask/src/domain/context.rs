use crate::domain::model::ReleaseMode;

#[derive(Debug, Clone)]
pub struct CommonContext {
    pub mode: ReleaseMode,
    pub default_branch: String,
    pub auth: AuthContext,
}

#[derive(Debug, Clone)]
pub struct BranchContext {
    pub skip_pr: bool,
}

pub type PrepareContext = CommonContext;

#[derive(Debug, Clone)]
pub struct SubmitContext {
    pub common: CommonContext,
    pub branch: BranchContext,
}

#[derive(Debug, Clone)]
pub struct PublishContext {
    pub common: CommonContext,
    pub rc_branch_prefix: String,
    pub final_branch_prefix: String,
    pub no_artifacts: bool,
}

#[derive(Debug, Clone)]
pub struct ExecuteContext {
    pub common: CommonContext,
    pub branch: BranchContext,
}

impl ExecuteContext {
    pub fn to_prepare_context(&self) -> PrepareContext {
        self.common.clone()
    }

    pub fn to_submit_context(&self) -> SubmitContext {
        SubmitContext {
            common: self.common.clone(),
            branch: self.branch.clone(),
        }
    }
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
