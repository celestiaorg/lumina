//! The `xtask` command surface.
//!
//! A [`clap`] (derive) parser for the two subcommands plus the `--*-token-env`
//! resolution that turns an optional flag into the name of an environment variable
//! holding a token — never a literal token. The CLI only parses, maps arguments onto
//! [`crate::cmd_prepare::PrepareOptions`] / [`crate::cmd_release::ReleaseOptions`],
//! dispatches to the flows, and prints an outcome summary. It is non-interactive:
//! every input is a flag. The git / network / registry / npm side effects live in
//! the command modules.

use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

use crate::cmd_prepare::{self, PrepareOptions};
use crate::cmd_release::{self, ReleaseOptions};

/// Default env-var name for the GitHub token in the prepare flow.
///
/// A PAT (or GitHub App token), not the default Actions `GITHUB_TOKEN`, so the
/// release-branch commit re-triggers PR CI.
const PREPARE_GITHUB_TOKEN_ENV: &str = "GH_TOKEN";
/// Default env-var name for the GitHub token in the release flow.
const RELEASE_GITHUB_TOKEN_ENV: &str = "GITHUB_TOKEN";
/// Default env-var name for the cargo registry token in the release flow.
const REGISTRY_TOKEN_ENV: &str = "CARGO_REGISTRY_TOKEN";

/// The `xtask` command-line interface.
#[derive(Debug, Parser)]
#[command(
    name = "xtask",
    about = "In-house release tooling for the lumina workspace.",
    long_about = "Drives the two release flows: `prepare-release` (build a \
                  single-commit release PR) and `release` (publish crates + npm). \
                  Fully non-interactive; credentials are always passed as the NAME \
                  of an env var, never as a literal token."
)]
pub struct Cli {
    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Commands,
}

/// The two `xtask` subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Flow 1 — build (and optionally push) a single-commit release PR.
    PrepareRelease(PrepareArgs),
    /// Flow 2 — publish crates (+ npm) and cut tags / GitHub releases.
    Release(ReleaseArgs),
}

/// Arguments for `cargo xtask prepare-release`.
///
/// The token flag carries the **name** of an env var (default `GH_TOKEN`).
#[derive(Debug, Args, Default, Clone)]
pub struct PrepareArgs {
    /// Release-branch prefix (branch = `<prefix><version>`). Defaults to the config
    /// value, else `release-`.
    #[arg(long)]
    pub branch_prefix: Option<String>,

    /// The next version this release will carry.
    #[arg(long)]
    pub version: String,

    /// Delete and recreate an existing release branch instead of erroring.
    #[arg(long)]
    pub yes: bool,

    /// Opt into the remote actions (push the branch + open/update the PR).
    #[arg(long)]
    pub push: bool,

    /// NAME of the env var holding the GitHub token (default: `GH_TOKEN`, a PAT).
    #[arg(long)]
    pub github_token_env: Option<String>,

    /// Write the rendered PR description to this Markdown file (in addition to
    /// stdout). Handy for a local, no-`--push` test run to preview the PR body.
    #[arg(long, value_name = "FILE")]
    pub pr_body_out: Option<PathBuf>,

    /// Skip the pre-commit publishability preflight
    /// (`cargo publish --workspace --dry-run`). Use when offline, or to bypass a
    /// known false failure.
    #[arg(long)]
    pub no_verify: bool,
}

/// Arguments for `cargo xtask release`.
///
/// Each token flag carries the **name** of an env var. `--npm-token-env` is optional
/// (only needed when an npm component is configured).
#[derive(Debug, Args, Default, Clone)]
pub struct ReleaseArgs {
    /// Commit to release from and to place every tag on (defaults to `HEAD`, which
    /// the flow requires to be the checked-out commit anyway).
    #[arg(long, default_value = "HEAD")]
    pub sha: String,

    /// NAME of the env var holding the GitHub token (default: `GITHUB_TOKEN`).
    #[arg(long)]
    pub github_token_env: Option<String>,

    /// NAME of the env var holding the cargo registry token
    /// (default: `CARGO_REGISTRY_TOKEN`).
    #[arg(long)]
    pub registry_token_env: Option<String>,

    /// NAME of the env var holding the npm token. Optional — omitted means the npm
    /// step is skipped regardless of configuration.
    #[arg(long)]
    pub npm_token_env: Option<String>,
}

/// Parse argv, resolve token-env defaults, dispatch, and summarize.
///
/// Returns `Ok(())` on a successful flow; any error from the flows propagates as an
/// [`anyhow::Error`] for `main` to map onto a non-zero exit code.
pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let repo_root = std::env::current_dir().context("failed to read current directory")?;
    let mut out = io::stdout();

    match cli.command {
        Commands::PrepareRelease(args) => {
            let opts = to_prepare_options(args);
            let outcome =
                cmd_prepare::run(&repo_root, &opts).context("prepare-release flow failed")?;
            print_prepare_summary(&opts, &outcome, &mut out)?;
        }
        Commands::Release(args) => {
            let opts = to_release_options(args);
            let outcome = cmd_release::run(&repo_root, &opts).context("release flow failed")?;
            print_release_summary(&outcome, &mut out)?;
        }
    }

    Ok(())
}

/// Resolve a `--*-token-env` flag to the env-var name to forward, falling back to
/// the default name when the flag was omitted.
pub(crate) fn resolve_token_env(flag: Option<String>, default_name: &str) -> String {
    flag.unwrap_or_else(|| default_name.to_string())
}

/// Map parsed `prepare-release` args onto [`PrepareOptions`]. The GitHub token env
/// defaults to `GH_TOKEN`; `date` is always `None` (the prepare flow defaults to
/// today UTC).
pub(crate) fn to_prepare_options(args: PrepareArgs) -> PrepareOptions {
    PrepareOptions {
        branch_prefix: args.branch_prefix,
        version: args.version,
        yes: args.yes,
        push: args.push,
        github_token_env: resolve_token_env(args.github_token_env, PREPARE_GITHUB_TOKEN_ENV),
        date: None,
        pr_body_out: args.pr_body_out,
        no_verify: args.no_verify,
    }
}

/// Map parsed `release` args onto [`ReleaseOptions`]. The GitHub / registry token
/// envs default to `GITHUB_TOKEN` / `CARGO_REGISTRY_TOKEN`; the npm token env is
/// forwarded as-is (`None` ⇒ npm step skipped).
pub(crate) fn to_release_options(args: ReleaseArgs) -> ReleaseOptions {
    ReleaseOptions::new(
        args.sha,
        resolve_token_env(args.github_token_env, RELEASE_GITHUB_TOKEN_ENV),
        resolve_token_env(args.registry_token_env, REGISTRY_TOKEN_ENV),
        args.npm_token_env,
    )
}

/// Print a short status summary of a successful prepare run. The prepare flow already
/// printed the PR body to stdout; this is just a status line, not a duplicate body.
fn print_prepare_summary(
    opts: &PrepareOptions,
    outcome: &cmd_prepare::PrepareOutcome,
    writer: &mut impl Write,
) -> io::Result<()> {
    writeln!(writer)?;
    writeln!(writer, "Prepared release branch: {}", outcome.branch)?;
    match &outcome.current_version {
        Some(cur) => writeln!(writer, "Version: {cur} -> {}", opts.version)?,
        None => writeln!(writer, "Version: (first release) -> {}", opts.version)?,
    }
    writeln!(writer, "Release commit: {}", outcome.commit_sha)?;
    if outcome.pushed {
        match &outcome.pr_url {
            Some(url) => writeln!(writer, "Pushed; PR: {url}")?,
            None => writeln!(writer, "Pushed.")?,
        }
    } else {
        writeln!(
            writer,
            "Local only (no --push): nothing pushed, no PR opened."
        )?;
    }
    Ok(())
}

/// Print a summary of a successful release run: the version, one line per crate
/// outcome, and the npm step status.
fn print_release_summary(
    outcome: &cmd_release::ReleaseOutcome,
    writer: &mut impl Write,
) -> io::Result<()> {
    writeln!(writer)?;
    writeln!(writer, "Released version: {}", outcome.version)?;
    for c in &outcome.crates {
        writeln!(writer, "  {} ({}): {:?}", c.name, c.tag, c.outcome)?;
    }
    match &outcome.npm {
        cmd_release::NpmStatus::SkippedNoComponent => {
            writeln!(writer, "npm: skipped (no component configured)")?;
        }
        cmd_release::NpmStatus::SkippedNoToken => {
            writeln!(writer, "npm: skipped (no --npm-token-env supplied)")?;
        }
        cmd_release::NpmStatus::Published(names) => {
            writeln!(writer, "npm: published {}", names.join(", "))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// Parse a `prepare-release` invocation, asserting it succeeds, and return the
    /// inner `PrepareArgs`.
    fn parse_prepare(argv: &[&str]) -> PrepareArgs {
        match Cli::try_parse_from(argv)
            .expect("argv should parse")
            .command
        {
            Commands::PrepareRelease(a) => a,
            other => panic!("expected prepare-release, got {other:?}"),
        }
    }

    /// Parse a `release` invocation, asserting it succeeds, and return the inner
    /// `ReleaseArgs`.
    fn parse_release(argv: &[&str]) -> ReleaseArgs {
        match Cli::try_parse_from(argv)
            .expect("argv should parse")
            .command
        {
            Commands::Release(a) => a,
            other => panic!("expected release, got {other:?}"),
        }
    }

    #[test]
    fn clap_command_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn unknown_subcommand_is_a_usage_error() {
        let err = Cli::try_parse_from(["xtask", "bogus"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn token_flags_reject_being_passed_without_a_value() {
        // A `--github-token-env` with no value is a usage error; it must always
        // carry the NAME of an env var.
        let err = Cli::try_parse_from(["xtask", "release", "--github-token-env"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn prepare_parses_all_flags() {
        let args = parse_prepare(&[
            "xtask",
            "prepare-release",
            "--branch-prefix",
            "rel-",
            "--version",
            "0.2.0",
            "--yes",
            "--push",
            "--github-token-env",
            "MY_PAT",
        ]);
        assert_eq!(args.branch_prefix.as_deref(), Some("rel-"));
        assert_eq!(args.version, "0.2.0");
        assert!(args.yes);
        assert!(args.push);
        assert_eq!(args.github_token_env.as_deref(), Some("MY_PAT"));
    }

    #[test]
    fn prepare_version_is_required() {
        // Omitting --version is a usage error (there is no prompt fallback).
        let err = Cli::try_parse_from(["xtask", "prepare-release"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn prepare_bools_default_false_and_optionals_none() {
        let args = parse_prepare(&["xtask", "prepare-release", "--version", "0.2.0"]);
        assert!(!args.yes, "--yes defaults false");
        assert!(!args.push, "--push defaults false");
        assert_eq!(args.branch_prefix, None);
        assert_eq!(args.github_token_env, None);
    }

    #[test]
    fn release_parses_all_flags() {
        let args = parse_release(&[
            "xtask",
            "release",
            "--sha",
            "deadbeef",
            "--github-token-env",
            "GHT",
            "--registry-token-env",
            "RGT",
            "--npm-token-env",
            "NPMT",
        ]);
        assert_eq!(args.sha, "deadbeef");
        assert_eq!(args.github_token_env.as_deref(), Some("GHT"));
        assert_eq!(args.registry_token_env.as_deref(), Some("RGT"));
        assert_eq!(args.npm_token_env.as_deref(), Some("NPMT"));
    }

    #[test]
    fn release_optionals_default() {
        let args = parse_release(&["xtask", "release"]);
        assert_eq!(args.sha, "HEAD", "--sha defaults to HEAD");
        assert_eq!(args.github_token_env, None);
        assert_eq!(args.registry_token_env, None);
        assert_eq!(args.npm_token_env, None, "npm token has no default");
    }

    // token-env defaulting

    #[test]
    fn resolve_token_env_uses_default_when_flag_absent() {
        assert_eq!(resolve_token_env(None, "GH_TOKEN"), "GH_TOKEN");
    }

    #[test]
    fn resolve_token_env_keeps_supplied_name() {
        assert_eq!(
            resolve_token_env(Some("CUSTOM".into()), "GH_TOKEN"),
            "CUSTOM"
        );
    }

    #[test]
    fn prepare_github_default_is_gh_token() {
        // No --github-token-env ⇒ PAT default `GH_TOKEN` (re-triggers PR CI).
        let args = parse_prepare(&["xtask", "prepare-release", "--version", "0.2.0"]);
        let opts = to_prepare_options(args);
        assert_eq!(opts.github_token_env, "GH_TOKEN");
    }

    #[test]
    fn prepare_github_explicit_overrides_default() {
        let args = parse_prepare(&[
            "xtask",
            "prepare-release",
            "--version",
            "0.2.0",
            "--github-token-env",
            "MY_PAT",
        ]);
        let opts = to_prepare_options(args);
        assert_eq!(opts.github_token_env, "MY_PAT");
    }

    #[test]
    fn prepare_options_forward_flags_and_null_date() {
        let args = parse_prepare(&[
            "xtask",
            "prepare-release",
            "--version",
            "0.2.0",
            "--branch-prefix",
            "release-",
            "--yes",
            "--push",
        ]);
        let opts = to_prepare_options(args);
        assert!(opts.yes);
        assert!(opts.push);
        assert_eq!(opts.version, "0.2.0");
        assert_eq!(opts.branch_prefix.as_deref(), Some("release-"));
        assert!(
            opts.date.is_none(),
            "CLI never sets a release date override"
        );
    }

    #[test]
    fn release_token_defaults_match_spec() {
        // release GitHub default differs from prepare: GITHUB_TOKEN (not GH_TOKEN).
        let args = parse_release(&["xtask", "release", "--sha", "abc123"]);
        let opts = to_release_options(args);
        assert_eq!(opts.github_token_env, "GITHUB_TOKEN");
        assert_eq!(opts.registry_token_env, "CARGO_REGISTRY_TOKEN");
        assert_eq!(opts.sha, "abc123");
    }

    #[test]
    fn release_npm_token_absent_stays_none() {
        // No --npm-token-env ⇒ Option<String> None ⇒ the release flow skips the npm step.
        let args = parse_release(&["xtask", "release"]);
        let opts = to_release_options(args);
        assert_eq!(opts.npm_token_env, None);
    }

    #[test]
    fn release_npm_token_present_is_some_name() {
        let args = parse_release(&["xtask", "release", "--npm-token-env", "NPM_REGISTRY_TOKEN"]);
        let opts = to_release_options(args);
        assert_eq!(opts.npm_token_env.as_deref(), Some("NPM_REGISTRY_TOKEN"));
    }
}
