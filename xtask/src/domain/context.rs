use crate::domain::model::{BranchKind, ReleaseMode};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ReleaseContext {
    pub mode: ReleaseMode,
    pub current_commit: Option<String>,
    pub default_branch: String,
    pub branch_name: Option<String>,
    pub rc_branch_prefix: String,
    pub final_branch_prefix: String,
    pub skip_pr: bool,
    pub auth: AuthContext,
}

impl ReleaseContext {
    /// Returns the expected release-branch kind derived from the chosen release mode.
    #[allow(dead_code)]
    pub fn branch_kind(&self) -> BranchKind {
        match self.mode {
            ReleaseMode::Rc => BranchKind::RcRelease,
            ReleaseMode::Final => BranchKind::FinalRelease,
        }
    }

    /// Resolves the branch name used by prepare/submit flows.
    /// Priority: explicit `--branch-name`, otherwise mode-specific prefix + default branch.
    #[allow(dead_code)]
    pub fn resolved_branch_name(&self) -> String {
        // Respect explicit branch override when provided.
        if let Some(name) = &self.branch_name {
            return name.clone();
        }

        // Otherwise derive branch name from mode-specific prefix and default branch name.
        match self.mode {
            ReleaseMode::Rc => format!("{}/{}", self.rc_branch_prefix, self.default_branch),
            ReleaseMode::Final => format!("{}/{}", self.final_branch_prefix, self.default_branch),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AuthContext {
    pub release_plz_token: Option<String>,
    pub github_token: Option<String>,
    pub cargo_registry_token: Option<String>,
}

impl AuthContext {
    /// Loads release/publish credentials from process environment for downstream adapters.
    pub fn from_env() -> Self {
        Self {
            release_plz_token: std::env::var("RELEASE_PLZ_TOKEN").ok(),
            github_token: std::env::var("GITHUB_TOKEN").ok(),
            cargo_registry_token: std::env::var("CARGO_REGISTRY_TOKEN").ok(),
        }
    }
}
