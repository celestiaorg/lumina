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
