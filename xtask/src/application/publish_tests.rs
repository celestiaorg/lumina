use crate::application::pipeline_mock::{EngineCall, MockReleaseEngine};
use crate::domain::types::{AuthContext, CommonContext, PublishContext, ReleaseMode};

use super::*;

fn test_publish_ctx(mode: ReleaseMode, no_artifacts: bool) -> PublishContext {
    PublishContext {
        common: CommonContext {
            mode,
            default_branch: "main".to_string(),
            auth: AuthContext {
                release_plz_token: Some("release-token".to_string()),
                github_token: Some("gh-token".to_string()),
                cargo_registry_token: Some("cargo-token".to_string()),
            },
        },
        rc_branch_prefix: "lumina/release-plz-rc".to_string(),
        final_branch_prefix: "lumina/release-plz-final".to_string(),
        no_artifacts,
    }
}

#[tokio::test]
async fn publish_rc_with_releases_reports_published() {
    let engine = MockReleaseEngine::new().with_publish_payload(serde_json::json!([
        {"name": "pkg", "version": "1.0.1-rc.1", "tag": "pkg-v1.0.1-rc.1"}
    ]));

    let report = handle_publish(&engine, test_publish_ctx(ReleaseMode::Rc, false))
        .await
        .unwrap();

    assert_eq!(report.mode, ReleaseMode::Rc);
    assert!(report.published);
    assert!(report.payload.as_array().unwrap().len() == 1);

    let engine_calls = engine.calls();
    assert_eq!(engine_calls.len(), 1);
    assert_eq!(
        engine_calls[0],
        EngineCall::Publish {
            mode: ReleaseMode::Rc,
            no_artifacts: false,
        }
    );
}

#[tokio::test]
async fn publish_final_with_empty_payload_reports_not_published() {
    let engine = MockReleaseEngine::new().with_publish_payload(serde_json::json!([]));

    let report = handle_publish(&engine, test_publish_ctx(ReleaseMode::Final, false))
        .await
        .unwrap();

    assert_eq!(report.mode, ReleaseMode::Final);
    assert!(!report.published);
}

#[tokio::test]
async fn publish_null_payload_reports_not_published() {
    let engine = MockReleaseEngine::new().with_publish_payload(serde_json::Value::Null);

    let report = handle_publish(&engine, test_publish_ctx(ReleaseMode::Rc, false))
        .await
        .unwrap();

    assert!(!report.published);
}

#[tokio::test]
async fn publish_no_artifacts_passes_flag() {
    let engine = MockReleaseEngine::new().with_publish_payload(serde_json::json!([]));

    handle_publish(&engine, test_publish_ctx(ReleaseMode::Rc, true))
        .await
        .unwrap();

    let engine_calls = engine.calls();
    assert_eq!(
        engine_calls[0],
        EngineCall::Publish {
            mode: ReleaseMode::Rc,
            no_artifacts: true,
        }
    );
}

#[tokio::test]
async fn publish_multiple_releases_reports_published() {
    let engine = MockReleaseEngine::new().with_publish_payload(serde_json::json!([
        {"name": "core", "version": "1.0.0"},
        {"name": "sdk", "version": "2.0.0"},
        {"name": "cli", "version": "3.0.0"}
    ]));

    let report = handle_publish(&engine, test_publish_ctx(ReleaseMode::Final, false))
        .await
        .unwrap();

    assert!(report.published);
    assert_eq!(report.payload.as_array().unwrap().len(), 3);
}
