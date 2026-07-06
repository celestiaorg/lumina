//! `xtask` binary entry point.
//!
//! Thin wrapper over [`xtask::cli::run`]: it parses argv, dispatches to the
//! `prepare-release` / `release` flow, and maps any error onto a non-zero process
//! exit with the full error chain printed to stderr. clap's own parse / `--help` /
//! `--version` handling happens inside `cli::run` (via `Cli::parse`), exiting with
//! clap's conventional codes.

use std::process::ExitCode;

fn main() -> ExitCode {
    match xtask::cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}
