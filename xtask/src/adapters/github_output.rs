use std::fs::OpenOptions;
use std::io::Write;

use anyhow::{Context, Result};

/// Appends key-value pair to GitHub Actions output file when running inside Actions.
pub fn write_github_output(key: &str, value: &str) -> Result<()> {
    let Some(path) = std::env::var_os("GITHUB_OUTPUT") else {
        return Ok(());
    };

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .context("failed to open GITHUB_OUTPUT file")?;

    writeln!(file, "{key}={value}").context("failed to write GITHUB_OUTPUT entry")?;
    Ok(())
}
