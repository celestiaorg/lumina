use std::path::Path;

use anyhow::{Context, Result};
use cargo_metadata::Metadata;

pub fn metadata_for_manifest(manifest_path: &Path) -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .manifest_path(manifest_path)
        .exec()
        .with_context(|| {
            format!(
                "failed to read workspace metadata for `{}`",
                manifest_path.display()
            )
        })
}
