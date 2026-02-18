use anyhow::Result;

use crate::domain::ports::Publisher;
use crate::domain::types::{ExecutionStage, ReleaseContext, ReleaseReport};

pub async fn handle_publish(
    publisher: &dyn Publisher,
    ctx: ReleaseContext,
) -> Result<ReleaseReport> {
    let publish_payload = publisher.publish(&ctx).await?;
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
