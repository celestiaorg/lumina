use anyhow::Result;

use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::domain::types::{ExecutionStage, ReleaseContext, ReleaseReport};

/// Executes publish-only stage through release-plz and converts result into report shape.
pub async fn handle_publish(
    publisher: &ReleasePlzAdapter,
    ctx: ReleaseContext,
) -> Result<ReleaseReport> {
    let publish_payload = publisher.publish(&ctx).await?;
    // release-plz may return `null` or `[]` when nothing was published.
    let published = !publish_payload.is_null()
        && !publish_payload
            .as_array()
            .is_some_and(|releases| releases.is_empty());

    Ok(ReleaseReport {
        mode: ctx.mode,
        published,
        payload: publish_payload,
        stage: ExecutionStage::Released,
    })
}
