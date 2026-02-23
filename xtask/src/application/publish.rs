use anyhow::Result;
use tracing::info;

use crate::application::pipeline_ops::ReleaseEngine;
use crate::domain::types::{PublishContext, ReleaseReport};

pub async fn handle_publish(
    publisher: &impl ReleaseEngine,
    ctx: PublishContext,
) -> Result<ReleaseReport> {
    info!(mode=?ctx.common.mode, "publish: invoking release adapter");
    let publish_payload = publisher.publish(&ctx).await?;
    // release-plz returns null or [] when nothing was published.
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

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
