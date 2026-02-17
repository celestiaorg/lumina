use std::fs::OpenOptions;
use std::io::Write;

use anyhow::{Context, Result};
use serde::Serialize;

pub fn maybe_print_json<T: Serialize>(json: bool, payload: &T) -> Result<()> {
    if !json {
        return Ok(());
    }

    let body = serde_json::to_string_pretty(payload).context("failed to serialize JSON output")?;
    println!("{body}");
    Ok(())
}

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
