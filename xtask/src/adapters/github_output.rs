use std::fs::OpenOptions;
use std::io::Write;

use anyhow::{Context, Result};

/// Appends key-value pair to GitHub Actions output file when running inside Actions.
pub fn write_github_output(key: &str, value: &str) -> Result<()> {
    let Some(mut file) = open_github_output_file()? else {
        return Ok(());
    };
    writeln!(file, "{key}={value}").context("failed to write GITHUB_OUTPUT entry")?;
    Ok(())
}

/// Appends key-value pair using heredoc format to preserve JSON/special characters.
pub fn write_github_output_multiline(key: &str, value: &str) -> Result<()> {
    let Some(mut file) = open_github_output_file()? else {
        return Ok(());
    };
    let delimiter = unique_delimiter(value);
    writeln!(file, "{key}<<{delimiter}").context("failed to write GITHUB_OUTPUT header")?;
    writeln!(file, "{value}").context("failed to write GITHUB_OUTPUT value body")?;
    writeln!(file, "{delimiter}").context("failed to write GITHUB_OUTPUT footer")?;
    Ok(())
}

fn open_github_output_file() -> Result<Option<std::fs::File>> {
    let Some(path) = std::env::var_os("GITHUB_OUTPUT") else {
        return Ok(None);
    };

    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .context("failed to open GITHUB_OUTPUT file")?;
    Ok(Some(file))
}

fn unique_delimiter(value: &str) -> String {
    let base = "__XTASK_GHA_OUTPUT__";
    if !value.contains(base) {
        return base.to_string();
    }
    for idx in 1..=u32::MAX {
        let candidate = format!("{base}_{idx}");
        if !value.contains(&candidate) {
            return candidate;
        }
    }
    // The loop is practically guaranteed to return earlier.
    "__XTASK_GHA_OUTPUT_FALLBACK__".to_string()
}
