use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cargo_metadata::Metadata;

pub fn workspace_metadata(workspace_root: &Path) -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .current_dir(workspace_root)
        .exec()
        .context("failed to read workspace metadata")
}

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

pub fn copied_workspace_manifest_path(
    metadata: &Metadata,
    temp_root: &cargo_metadata::camino::Utf8Path,
) -> Result<PathBuf> {
    let workspace_name = metadata
        .workspace_root
        .file_name()
        .context("failed to resolve workspace directory name")?;
    Ok(temp_root
        .join(workspace_name)
        .join("Cargo.toml")
        .as_std_path()
        .to_path_buf())
}
