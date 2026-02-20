use crate::application::pipeline_mock::{GitCall, MockGitRepo, MockPrClient, PrCall};
use crate::domain::model::{RELEASE_COMMIT_MESSAGE_FINAL, RELEASE_COMMIT_MESSAGE_RC};
use crate::domain::types::{
    AuthContext, BranchContext, CommonContext, PullRequestInfo, ReleaseMode, SubmitContext,
};

use super::*;

fn test_auth() -> AuthContext {
    AuthContext {
        release_plz_token: None,
        github_token: Some("gh-token-123".to_string()),
        cargo_registry_token: None,
    }
}

fn test_submit_args(mode: ReleaseMode, branch_override: Option<&str>) -> SubmitArgs {
    SubmitArgs {
        ctx: SubmitContext {
            common: CommonContext {
                mode,
                default_branch: "main".to_string(),
                auth: test_auth(),
            },
            branch: BranchContext { skip_pr: false },
        },
        branch_name_override: branch_override.map(String::from),
    }
}

fn test_submit_args_skip_pr(mode: ReleaseMode, branch: &str) -> SubmitArgs {
    SubmitArgs {
        ctx: SubmitContext {
            common: CommonContext {
                mode,
                default_branch: "main".to_string(),
                auth: test_auth(),
            },
            branch: BranchContext { skip_pr: true },
        },
        branch_name_override: Some(branch.to_string()),
    }
}

#[tokio::test]
async fn submit_rc_commits_pushes_then_manages_prs() {
    let git = MockGitRepo::new().with_stage_commit_ok().with_push_ok();
    let pr_client = MockPrClient::new()
        .with_close_stale_result(vec![])
        .with_ensure_pr_result(Some(PullRequestInfo {
            number: 42,
            url: "https://github.com/org/repo/pull/42".to_string(),
        }));

    let report = handle_submit(
        &git,
        &pr_client,
        test_submit_args(ReleaseMode::Rc, Some("lumina/release-plz-2025-rc")),
    )
    .await
    .unwrap();

    let git_calls = git.calls();
    assert_eq!(git_calls.len(), 2);
    assert_eq!(
        git_calls[0],
        GitCall::StageAllAndCommit {
            message: RELEASE_COMMIT_MESSAGE_RC.to_string(),
            dry_run: false,
        }
    );
    assert_eq!(
        git_calls[1],
        GitCall::PushBranch {
            branch_name: "lumina/release-plz-2025-rc".to_string(),
            force: true,
            dry_run: false,
        }
    );

    let pr_calls = pr_client.calls();
    assert_eq!(pr_calls.len(), 2);
    assert_eq!(
        pr_calls[0],
        PrCall::CloseStaleReleasePrs {
            skip_pr: false,
            keep_branch: Some("lumina/release-plz-2025-rc".to_string()),
        }
    );
    assert_eq!(
        pr_calls[1],
        PrCall::EnsureReleasePr {
            mode: ReleaseMode::Rc,
            default_branch: "main".to_string(),
            skip_pr: false,
            branch_name: "lumina/release-plz-2025-rc".to_string(),
        }
    );

    assert_eq!(report.mode, ReleaseMode::Rc);
    assert_eq!(report.branch_name, "lumina/release-plz-2025-rc");
    assert_eq!(report.commit_message, RELEASE_COMMIT_MESSAGE_RC);
    assert!(report.pushed);
    assert_eq!(
        report.pr_url,
        Some("https://github.com/org/repo/pull/42".to_string())
    );
}

#[tokio::test]
async fn submit_final_uses_final_commit_message() {
    let git = MockGitRepo::new().with_stage_commit_ok().with_push_ok();
    let pr_client = MockPrClient::new()
        .with_close_stale_result(vec![])
        .with_ensure_pr_result(None);

    let report = handle_submit(
        &git,
        &pr_client,
        test_submit_args(ReleaseMode::Final, Some("lumina/release-plz-final")),
    )
    .await
    .unwrap();

    assert_eq!(report.commit_message, RELEASE_COMMIT_MESSAGE_FINAL);
    assert_eq!(report.mode, ReleaseMode::Final);
    assert_eq!(
        git.calls()[0],
        GitCall::StageAllAndCommit {
            message: RELEASE_COMMIT_MESSAGE_FINAL.to_string(),
            dry_run: false,
        }
    );
    assert!(matches!(
        &pr_client.calls()[1],
        PrCall::EnsureReleasePr {
            mode: ReleaseMode::Final,
            ..
        }
    ));
}

#[tokio::test]
async fn submit_without_branch_override_generates_name() {
    let git = MockGitRepo::new().with_stage_commit_ok().with_push_ok();
    let pr_client = MockPrClient::new()
        .with_close_stale_result(vec![])
        .with_ensure_pr_result(None);

    let report = handle_submit(&git, &pr_client, test_submit_args(ReleaseMode::Rc, None))
        .await
        .unwrap();

    assert!(report.branch_name.starts_with("lumina/release-plz-"));
    assert!(report.branch_name.ends_with("-rc"));
}

#[tokio::test]
async fn submit_skip_pr_still_commits_and_pushes() {
    let git = MockGitRepo::new().with_stage_commit_ok().with_push_ok();
    let pr_client = MockPrClient::new()
        .with_close_stale_result(vec![])
        .with_ensure_pr_result(None);

    let report = handle_submit(
        &git,
        &pr_client,
        test_submit_args_skip_pr(ReleaseMode::Rc, "lumina/release-plz-skip"),
    )
    .await
    .unwrap();

    assert_eq!(git.calls().len(), 2);
    assert!(report.pushed);

    let pr_calls = pr_client.calls();
    assert_eq!(
        pr_calls[0],
        PrCall::CloseStaleReleasePrs {
            skip_pr: true,
            keep_branch: Some("lumina/release-plz-skip".to_string()),
        }
    );
    assert!(report.pr_url.is_none());
}

#[tokio::test]
async fn submit_fails_if_commit_fails() {
    let git = MockGitRepo::new().with_stage_commit_error("index lock held");
    let pr_client = MockPrClient::new();

    let result = handle_submit(
        &git,
        &pr_client,
        test_submit_args(ReleaseMode::Rc, Some("branch")),
    )
    .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("index lock held"));
    assert_eq!(git.calls().len(), 1);
    assert!(pr_client.calls().is_empty());
}

#[tokio::test]
async fn submit_fails_if_push_fails() {
    let git = MockGitRepo::new()
        .with_stage_commit_ok()
        .with_push_error("remote rejected");
    let pr_client = MockPrClient::new();

    let result = handle_submit(
        &git,
        &pr_client,
        test_submit_args(ReleaseMode::Rc, Some("branch")),
    )
    .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("remote rejected"));
    assert_eq!(git.calls().len(), 2);
    assert!(pr_client.calls().is_empty());
}

#[tokio::test]
async fn submit_passes_keep_branch_to_stale_pr_closer() {
    let git = MockGitRepo::new().with_stage_commit_ok().with_push_ok();
    let pr_client = MockPrClient::new()
        .with_close_stale_result(vec![
            PullRequestInfo {
                number: 10,
                url: "https://github.com/org/repo/pull/10".to_string(),
            },
            PullRequestInfo {
                number: 11,
                url: "https://github.com/org/repo/pull/11".to_string(),
            },
        ])
        .with_ensure_pr_result(Some(PullRequestInfo {
            number: 42,
            url: "https://github.com/org/repo/pull/42".to_string(),
        }));

    let report = handle_submit(
        &git,
        &pr_client,
        test_submit_args(ReleaseMode::Rc, Some("release-branch")),
    )
    .await
    .unwrap();

    assert_eq!(
        report.pr_url,
        Some("https://github.com/org/repo/pull/42".to_string())
    );
    assert!(matches!(
        &pr_client.calls()[0],
        PrCall::CloseStaleReleasePrs {
            keep_branch: Some(branch),
            ..
        } if branch == "release-branch"
    ));
}

#[tokio::test]
async fn submit_report_has_none_pr_url_when_no_pr() {
    let git = MockGitRepo::new().with_stage_commit_ok().with_push_ok();
    let pr_client = MockPrClient::new()
        .with_close_stale_result(vec![])
        .with_ensure_pr_result(None);

    let report = handle_submit(
        &git,
        &pr_client,
        test_submit_args(ReleaseMode::Rc, Some("branch")),
    )
    .await
    .unwrap();

    assert!(report.pr_url.is_none());
    assert!(report.pushed);
}
