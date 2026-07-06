//! Diagnostic-only breaking-change detection for the `prepare-release`
//! subcommand: for each crate it decides whether a release contains breaking
//! changes and produces a short human-readable reason. This never influences the
//! version — its output is a per-crate diagnostic consumed only when rendering the
//! PR body.
//!
//! Two independent signals are combined; either alone marks a crate breaking:
//!
//! 1. API breakage (primary) — `cargo-semver-checks` diffs the crate's public
//!    API against its last published baseline (library crates only).
//! 2. Intent breakage (secondary) — author-declared breaking changes in the
//!    Conventional Commits touching the crate (`!` marker / `BREAKING CHANGE:`
//!    footer).
//!
//! If `cargo-semver-checks` is missing or errors, the step fails open to the
//! intent signal (`ApiStatus::Unavailable`) rather than aborting the prepare step.

use regex::Regex;
use std::path::Path;
use std::process::Command;

use crate::commit;
use crate::workspace::{self, semver};

/// Outcome of the API-level (`cargo-semver-checks`) signal for one crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiStatus {
    /// `cargo-semver-checks` ran and reported no API breakage (exit 0).
    Compatible,
    /// `cargo-semver-checks` ran and reported incompatibilities (non-zero).
    Incompatible,
    /// The signal did not apply: no library target (binary-only / `cdylib`),
    /// no published baseline, or no changes since the baseline.
    Skipped,
    /// The tool was missing or errored — failed open to the intent signal.
    Unavailable,
}

/// Per-crate breaking-change diagnostic (the module's output).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakingReport {
    /// `api_breaking || intent_breaking`.
    pub has_breaking: bool,
    /// Human-readable explanation; `Some` iff `has_breaking`.
    pub reason: Option<String>,
    /// The API-signal outcome for this crate.
    pub api_status: ApiStatus,
}

/// Analyze one crate for breaking changes by combining the API and intent signals.
///
/// **Fail-open and total:** this never returns an error and never panics on a tool
/// problem — a missing or broken `cargo-semver-checks` degrades to
/// [`ApiStatus::Unavailable`] and the intent signal alone decides `has_breaking`,
/// so the prepare step is never aborted by this diagnostic.
///
/// * `repo_root` — workspace root (used as the working directory for the
///   `cargo-semver-checks` subprocess).
/// * `crate_info` — the crate under analysis (its `manifest_path` is diffed).
/// * `baseline` — the crate's last published version, supplied by the caller
///   (the caller owns the tags+registry lookup); `None` ⇒ never published.
/// * `parsed_commits` — the already-parsed commits touching this crate since the
///   baseline, shared with the changelog step.
pub fn analyze(
    repo_root: &Path,
    crate_info: &workspace::CrateInfo,
    baseline: Option<&semver::Version>,
    parsed_commits: &[commit::ParsedCommit],
) -> BreakingReport {
    // Never published: neither signal runs.
    let Some(baseline) = baseline else {
        return BreakingReport {
            has_breaking: false,
            reason: Some("first release — no prior version to compare".to_string()),
            api_status: ApiStatus::Skipped,
        };
    };

    // Signal 2 (intent) — always evaluated when a baseline exists.
    let intent_reason = intent_signal(parsed_commits);

    // Signal 1 (API) — library crates with changes since the baseline only.
    let (api_status, api_reason) = if !has_library_target(&crate_info.manifest_path) {
        // Binary-only / cdylib (e.g. toy-kv-wasm) — no Rust library API to diff.
        (ApiStatus::Skipped, None)
    } else if parsed_commits.is_empty() {
        // No changes since the baseline — no point diffing.
        (ApiStatus::Skipped, None)
    } else {
        run_semver_checks(repo_root, &crate_info.manifest_path, baseline)
    };

    combine(api_status, api_reason, intent_reason)
}

/// The intent signal.
///
/// Returns `Some(reason)` if any commit is breaking (`!` marker or
/// `BREAKING CHANGE:`/`BREAKING-CHANGE:` footer, via [`commit::is_breaking`]),
/// else `None`. The `Some`/`None` doubles as the `intent_breaking` boolean.
///
/// Reason precedence: the first commit (in slice order) with a breaking footer
/// wins, using its footer text; otherwise the first `!`-marked commit, citing its
/// (reconstructed) subject.
fn intent_signal(parsed: &[commit::ParsedCommit]) -> Option<String> {
    if !parsed.iter().any(commit::is_breaking) {
        return None;
    }

    // Prefer the first breaking-footer text (more specific).
    if let Some(footer) = parsed.iter().find_map(|p| p.breaking_footer.as_deref()) {
        return Some(footer.trim().to_string());
    }

    // Else cite the first `!`-marked commit's subject.
    let bang = parsed.iter().find(|p| p.breaking_bang)?;
    Some(format!(
        "breaking commit: \"{}\"",
        reconstruct_subject(bang)
    ))
}

/// Reconstruct a Conventional-Commit subject from the parsed fields.
///
/// `ParsedCommit` does not carry the raw subject, so the `!`-only intent reason
/// rebuilds `<kind>[(<scope>)]!: <description>`. Faithful for conventional commits,
/// which are the only ones that can carry a `!` marker.
fn reconstruct_subject(p: &commit::ParsedCommit) -> String {
    let kind = p.kind.as_deref().unwrap_or("");
    let mut head = String::from(kind);
    if let Some(scope) = &p.scope {
        head.push('(');
        head.push_str(scope);
        head.push(')');
    }
    if p.breaking_bang {
        head.push('!');
    }
    if head.is_empty() {
        // Non-conventional message: just the description.
        p.description.clone()
    } else {
        format!("{head}: {}", p.description)
    }
}

/// Pure signal combination + reason building.
fn combine(
    api_status: ApiStatus,
    api_reason: Option<String>,
    intent_reason: Option<String>,
) -> BreakingReport {
    let api_breaking = matches!(api_status, ApiStatus::Incompatible);
    let intent_breaking = intent_reason.is_some();
    let has_breaking = api_breaking || intent_breaking;

    let reason = match (api_reason, intent_reason) {
        // Both fired: API items first, then the author's note.
        (Some(api), Some(intent)) => Some(format!("{api}\n\nauthor note: {intent}")),
        // API only.
        (Some(api), None) => Some(api),
        // Intent only.
        (None, Some(intent)) => Some(intent),
        // Neither.
        (None, None) => None,
    };

    BreakingReport {
        has_breaking,
        reason,
        api_status,
    }
}

/// Run `cargo-semver-checks` for one crate and interpret the result.
///
/// Thin shell-out wrapper around the pure [`interpret_semver_checks`] seam. A spawn
/// failure (the tool is not installed) fails open to [`ApiStatus::Unavailable`] with
/// a warning.
fn run_semver_checks(
    repo_root: &Path,
    manifest_path: &Path,
    baseline: &semver::Version,
) -> (ApiStatus, Option<String>) {
    let output = Command::new("cargo")
        .current_dir(repo_root)
        .args(["semver-checks", "check-release", "--manifest-path"])
        .arg(manifest_path)
        .arg("--baseline-version")
        .arg(baseline.to_string())
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            interpret_semver_checks(out.status.success(), out.status.code(), &stdout, &stderr)
        }
        Err(err) => {
            // Tool missing / could not spawn — fail open to the intent signal.
            eprintln!(
                "warning: cargo-semver-checks unavailable for {}: {err}; \
                 falling back to the intent signal",
                manifest_path.display()
            );
            (ApiStatus::Unavailable, None)
        }
    }
}

/// Interpret a finished `cargo-semver-checks` invocation (the pure shell-out seam).
///
/// Maps exit status + captured output to `(ApiStatus, reason)` with no I/O, so it is
/// unit-testable with synthetic strings:
///
/// * exit 0 → `(Compatible, None)`.
/// * non-zero **with** a human-readable incompatibility report → `(Incompatible,
///   Some(ANSI-stripped text))`.
/// * non-zero with **no** usable report → `(Unavailable, None)` (fail open).
fn interpret_semver_checks(
    exit_ok: bool,
    _exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> (ApiStatus, Option<String>) {
    if exit_ok {
        return (ApiStatus::Compatible, None);
    }

    // `cargo-semver-checks` prints its human-readable report to stderr; fall back
    // to stdout. A non-empty report means real incompatibilities were found.
    let report = if !stderr.trim().is_empty() {
        stderr
    } else {
        stdout
    };

    let stripped = strip_ansi(report);
    let trimmed = stripped.trim();
    if trimmed.is_empty() {
        // Non-zero with nothing to show — treat as a tool error and fail open.
        (ApiStatus::Unavailable, None)
    } else {
        (ApiStatus::Incompatible, Some(trimmed.to_string()))
    }
}

fn strip_ansi(s: &str) -> String {
    static ANSI_SGR: std::sync::LazyLock<Regex> =
        std::sync::LazyLock::new(|| Regex::new(r"\x1b\[[0-9;]*m").unwrap());

    ANSI_SGR.replace_all(s, "").into_owned()
}

/// Whether a crate exposes a diffable Rust library target.
///
/// Returns `false` for binary-only crates and for crates whose library target is
/// exclusively a `cdylib`/`staticlib` (e.g. `toy-kv-wasm`) — those are `Skipped`.
/// Reads the crate `Cargo.toml` (`[lib].crate-type`) directly since target kinds
/// aren't surfaced by the workspace metadata. A read failure is treated
/// conservatively as "no library target" (the crate is `Skipped`, not crashed).
fn has_library_target(manifest_path: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(manifest_path) else {
        return false;
    };
    let Ok(value) = text.parse::<toml::Value>() else {
        return false;
    };

    let manifest_dir = manifest_path.parent();

    match value.get("lib") {
        Some(lib) => {
            // An explicit `[lib]`. Inspect its `crate-type`.
            match lib.get("crate-type").and_then(|v| v.as_array()) {
                Some(types) => types.iter().any(|t| {
                    matches!(
                        t.as_str(),
                        Some("lib") | Some("rlib") | Some("dylib") | Some("proc-macro")
                    )
                }),
                // `[lib]` with no `crate-type` defaults to a normal `lib`.
                None => true,
            }
        }
        None => {
            // No `[lib]` table: a library exists iff `src/lib.rs` is present.
            manifest_dir
                .map(|d| d.join("src").join("lib.rs").is_file())
                .unwrap_or(false)
        }
    }
}

#[cfg(test)]
mod tests;
