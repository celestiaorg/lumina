use crate::application::pipeline_mock::{EngineCall, GitCall, MockGitRepo, MockReleaseEngine};
use crate::domain::types::{
    AuthContext, BranchState, CommonContext, PrepareContext, ReleaseMode, UpdatedPackage,
};

use super::*;

fn test_auth() -> AuthContext {
    AuthContext {
        release_plz_token: None,
        github_token: None,
        cargo_registry_token: None,
    }
}

fn test_prepare_ctx(mode: ReleaseMode) -> PrepareContext {
    CommonContext {
        mode,
        default_branch: "main".to_string(),
        auth: test_auth(),
    }
}

fn test_updated_packages() -> Vec<UpdatedPackage> {
    vec![UpdatedPackage {
        package: "pkg".to_string(),
        version: "1.0.1-rc.1".to_string(),
    }]
}

#[test]
fn branch_name_has_rc_suffix_for_rc_mode() {
    let name = make_release_branch_name(ReleaseMode::Rc);
    assert!(name.starts_with("lumina/release-plz-"));
    assert!(name.ends_with("-rc"));
}

#[test]
fn branch_name_has_final_suffix_for_final_mode() {
    let name = make_release_branch_name(ReleaseMode::Final);
    assert!(name.starts_with("lumina/release-plz-"));
    assert!(name.ends_with("-final"));
}

#[tokio::test]
async fn prepare_rc_checks_branch_then_creates_then_updates() {
    let git = MockGitRepo::new()
        .with_branch_state(BranchState::Missing)
        .with_create_branch_descriptions(vec!["fetched origin", "created branch"]);
    let engine = MockReleaseEngine::new().with_update_result(test_updated_packages());

    let report = handle_prepare(&git, &engine, test_prepare_ctx(ReleaseMode::Rc))
        .await
        .unwrap();

    let git_calls = git.calls();
    assert_eq!(git_calls.len(), 2);
    assert!(
        matches!(&git_calls[0], GitCall::BranchState { branch_name } if branch_name.ends_with("-rc"))
    );
    assert!(
        matches!(&git_calls[1], GitCall::CreateReleaseBranch { branch_name, default_branch }
            if branch_name.ends_with("-rc") && default_branch == "main")
    );

    let engine_calls = engine.calls();
    assert_eq!(engine_calls.len(), 1);
    assert!(matches!(
        &engine_calls[0],
        EngineCall::Update {
            mode: ReleaseMode::Rc,
        }
    ));

    assert_eq!(report.mode, ReleaseMode::Rc);
    assert!(report.branch_name.ends_with("-rc"));
    assert_eq!(report.branch_state, BranchState::Missing);
    assert_eq!(report.updated_packages.len(), 1);
    assert_eq!(report.updated_packages[0].package, "pkg");
    assert_eq!(report.updated_packages[0].version, "1.0.1-rc.1");
}

#[tokio::test]
async fn prepare_final_passes_correct_mode() {
    let git = MockGitRepo::new()
        .with_branch_state(BranchState::Missing)
        .with_create_branch_descriptions(vec!["created branch"]);
    let engine = MockReleaseEngine::new().with_update_result(vec![UpdatedPackage {
        package: "pkg".to_string(),
        version: "1.1.0".to_string(),
    }]);

    let report = handle_prepare(&git, &engine, test_prepare_ctx(ReleaseMode::Final))
        .await
        .unwrap();

    assert_eq!(report.mode, ReleaseMode::Final);
    assert!(report.branch_name.ends_with("-final"));

    let engine_calls = engine.calls();
    assert!(matches!(
        &engine_calls[0],
        EngineCall::Update {
            mode: ReleaseMode::Final,
        }
    ));
}

#[tokio::test]
async fn prepare_fails_if_branch_already_exists_clean() {
    let git = MockGitRepo::new().with_branch_state(BranchState::ExistsClean);
    let engine = MockReleaseEngine::new();

    let result = handle_prepare(&git, &engine, test_prepare_ctx(ReleaseMode::Rc)).await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("already exists"));
    assert!(err.contains("ExistsClean"));
    assert_eq!(git.calls().len(), 1);
    assert!(engine.calls().is_empty());
}

#[tokio::test]
async fn prepare_fails_if_branch_exists_dirty() {
    let git = MockGitRepo::new().with_branch_state(BranchState::ExistsDirtyLocal);
    let engine = MockReleaseEngine::new();

    let result = handle_prepare(&git, &engine, test_prepare_ctx(ReleaseMode::Rc)).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("ExistsDirtyLocal"));
}

#[tokio::test]
async fn prepare_fails_if_create_branch_fails() {
    let git = MockGitRepo::new()
        .with_branch_state(BranchState::Missing)
        .with_create_branch_error("could not fetch origin");
    let engine = MockReleaseEngine::new();

    let result = handle_prepare(&git, &engine, test_prepare_ctx(ReleaseMode::Rc)).await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("could not fetch origin")
    );
    assert!(engine.calls().is_empty());
}

#[tokio::test]
async fn prepare_returns_empty_packages_when_nothing_updated() {
    let git = MockGitRepo::new()
        .with_branch_state(BranchState::Missing)
        .with_create_branch_descriptions(vec!["created branch"]);
    let engine = MockReleaseEngine::new().with_update_result(vec![]);

    let report = handle_prepare(&git, &engine, test_prepare_ctx(ReleaseMode::Rc))
        .await
        .unwrap();

    assert!(report.updated_packages.is_empty());
}
