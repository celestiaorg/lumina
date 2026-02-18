use anyhow::{Context, Result};
use serde::Serialize;

/// Serializes and prints payload in pretty JSON when `--json` flag is enabled.
pub fn maybe_print_json<T: Serialize>(json: bool, payload: &T) -> Result<()> {
    // Keep human-readable default output untouched unless JSON mode is explicitly requested.
    if !json {
        return Ok(());
    }

    // Use pretty JSON so CI/debug logs remain readable.
    let body = serde_json::to_string_pretty(payload).context("failed to serialize JSON output")?;
    println!("{body}");
    Ok(())
}
