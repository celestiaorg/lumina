use anyhow::Result;
use tracing::info;

use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::domain::types::{PublishContext, ReleaseReport};

/// Executes publish-only stage through release-plz and converts result into report shape.
pub async fn handle_publish(
    publisher: &ReleasePlzAdapter,
    ctx: PublishContext,
) -> Result<ReleaseReport> {
    info!(mode=?ctx.common.mode, "publish: invoking release adapter");
    let publish_payload = publisher.publish(&ctx).await?;
    // release-plz may return `null` or `[]` when nothing was published.
    let published = !publish_payload.is_null()
        && !publish_payload
            .as_array()
            .is_some_and(|releases| releases.is_empty());

    Ok(ReleaseReport {
        mode: ctx.common.mode,
        published,
        payload: publish_payload,
    })
}
