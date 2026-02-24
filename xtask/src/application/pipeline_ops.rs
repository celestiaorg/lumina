use anyhow::Result;

use crate::domain::types::PublishContext;

pub(crate) trait ReleaseEngine {
    async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value>;
}

// ── Adapter implementation ───────────────────────────────────────────────

use crate::adapters::release_plz::ReleasePlzAdapter;

impl ReleaseEngine for ReleasePlzAdapter {
    async fn publish(&self, ctx: &PublishContext) -> Result<serde_json::Value> {
        self.publish(ctx).await
    }
}
