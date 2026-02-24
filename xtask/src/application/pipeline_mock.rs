use std::cell::RefCell;
use std::collections::VecDeque;

use anyhow::Result;

use crate::domain::types::{PublishContext, ReleaseMode};

use super::pipeline_ops::ReleaseEngine;

// ── Call recording ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EngineCall {
    Publish {
        mode: ReleaseMode,
        no_artifacts: bool,
    },
}

// ── MockReleaseEngine ────────────────────────────────────────────────────

pub(crate) struct MockReleaseEngine {
    calls: RefCell<Vec<EngineCall>>,
    publish_results: RefCell<VecDeque<serde_json::Value>>,
}

impl MockReleaseEngine {
    pub(crate) fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            publish_results: RefCell::new(VecDeque::new()),
        }
    }

    pub(crate) fn with_publish_payload(self, payload: serde_json::Value) -> Self {
        self.publish_results.borrow_mut().push_back(payload);
        self
    }

    pub(crate) fn calls(&self) -> Vec<EngineCall> {
        self.calls.borrow().clone()
    }
}

impl ReleaseEngine for MockReleaseEngine {
    async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        self.calls.borrow_mut().push(EngineCall::Publish {
            mode: ctx.common.mode,
            no_artifacts: ctx.no_artifacts,
        });
        Ok(self
            .publish_results
            .borrow_mut()
            .pop_front()
            .unwrap_or(serde_json::json!([])))
    }
}
