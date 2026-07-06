//! Cargo/registry primitives for the crate-publishing step of `cargo xtask
//! release`.
//!
//! Implements `release-spec/publish-crates-logic.md` §"Idempotency scan" (b) and
//! §"Not published, no tag" steps 1–3. Everything here shells out to `cargo` via
//! [`std::process::Command`]; the per-crate orchestration (idempotency scan, tag
//! creation, dependency order) lives in higher modules.
//!
//! Key rules from the spec, enforced here:
//! - the registry token is read from an env var **named** by the caller and
//!   passed to `cargo publish` via the process environment as
//!   `CARGO_REGISTRY_TOKEN` — never on the command line, never logged;
//! - a `cargo publish` failure whose output says the version was *already
//!   uploaded* / *already exists* is treated as **success** (a race won by
//!   another runner or an earlier attempt);
//! - `wait_until_published` polls the registry on a short interval until the
//!   version indexes or a **timeout** elapses (timeout ⇒ `Err`).

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

/// Name of the environment variable `cargo publish` reads the registry token
/// from. We always pass the token to the child process under this name,
/// regardless of which env var the caller stored it in.
const CARGO_REGISTRY_TOKEN_ENV: &str = "CARGO_REGISTRY_TOKEN";

/// Interval between registry polls in [`wait_until_published`]. The spec mandates
/// "a short interval" without fixing a value; this is our concrete choice.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Render a process `ExitStatus` as its numeric code, or `"signal"` when the
/// process was killed by a signal (no code). Pure.
fn exit_code_str(status: &std::process::ExitStatus) -> String {
    status
        .code()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "signal".into())
}

/// Outcome of a `cargo info` query: the crate/version was found (its stdout), or
/// it was **cleanly absent** from the registry (cargo's not-found wording).
enum CargoInfo {
    Found(String),
    NotFound,
}

/// Run `cargo info <args>` with color forced off, optionally from `cwd`.
///
/// Shared by [`is_published`] and [`max_published_version`], which differ only in
/// the args, the working directory, and how they interpret the stdout. Returns
/// [`CargoInfo::Found`] on a zero exit, [`CargoInfo::NotFound`] when stderr is
/// cargo's not-found wording (see [`is_not_found_error`]) — a clean absence, not
/// an error — and `Err` on a spawn failure or any other non-zero exit.
///
/// Color is forced OFF because in some environments (e.g. GitHub Actions) `cargo
/// info` wraps the `version:` line in ANSI SGR codes, which breaks the downstream
/// line parsing (`\x1b[..mversion:` no longer has the `version:` prefix).
fn cargo_info(args: &[&str], cwd: Option<&Path>) -> Result<CargoInfo> {
    let pretty = args.join(" ");
    let mut cmd = Command::new("cargo");
    cmd.args(args).env("CARGO_TERM_COLOR", "never");
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let output = cmd
        .output()
        .with_context(|| format!("failed to spawn `cargo {pretty}`"))?;

    if output.status.success() {
        return Ok(CargoInfo::Found(
            String::from_utf8_lossy(&output.stdout).into_owned(),
        ));
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if is_not_found_error(&stderr) {
        Ok(CargoInfo::NotFound)
    } else {
        Err(anyhow!(
            "`cargo {pretty}` failed (exit {}): {}",
            exit_code_str(&output.status),
            stderr.trim()
        ))
    }
}

/// Query the registry for an exact `name@version`.
///
/// Runs `cargo info <name>@<version>` and reports whether that exact package
/// version exists **on the registry**. A clean "not found" is `Ok(false)` — not
/// an error — because callers branch on existence (see `publish-crates-logic.md`
/// §"Idempotency scan" (b)).
///
/// ## In-workspace hazard (local-source) — why we run from a temp dir
/// When `cargo info <name>@<version>` is run from *inside* the workspace that
/// defines `<name>`, cargo resolves to the LOCAL workspace member and exits 0
/// **regardless of the registry** — printing a `version: <v> (from ./path)` line.
/// After the release commit sets the workspace to the release version, that means
/// a workspace crate would look "resolved" whether or not it is actually
/// published, so the post-publish index poll ([`wait_until_published`]) could
/// never observe the registry and would always time out. To get the true registry
/// answer we run `cargo info` from a directory **outside** the workspace (the OS
/// temp dir), where cargo has no local member to resolve to. The local-source
/// guard is kept as a belt-and-braces fallback.
///
/// # Errors
/// Returns `Err` if `cargo` cannot be spawned, or if `cargo info` fails for a
/// reason other than "not found" (e.g. network/auth failure).
pub fn is_published(name: &str, version: &str) -> Result<bool> {
    let spec = format!("{name}@{version}");
    // Run OUTSIDE the workspace so `cargo info` queries the registry rather than
    // resolving to the local workspace member (see the doc comment above).
    let outside = std::env::temp_dir();
    match cargo_info(&["info", &spec], Some(&outside))? {
        // Outside the workspace this is a genuine registry hit; the local-source
        // check stays only as a defensive fallback.
        CargoInfo::Found(stdout) => Ok(!is_local_source_resolution(&stdout)),
        CargoInfo::NotFound => Ok(false),
    }
}

/// The highest version of `name` currently published on the registry, or `None`
/// if the crate has never been published.
///
/// Runs `cargo info <name>` and parses the `version:` line it prints for the
/// latest released version (see [`parse_max_version`]). Used by higher modules
/// to discover the repo's **current version** from the registry (the highest
/// existing version — `orchestrator.md` §"Version validation": current version =
/// highest of published registry versions + git tags).
///
/// A clean "not found" — the crate has never been published — is `Ok(None)`,
/// **not** an error, mirroring [`is_published`]'s `Ok(false)`. Likewise, a
/// success whose `version:` line is a **local-source resolution**
/// (`version: <v> (from ./path)`) means `cargo info`, run from *inside* the
/// workspace, resolved to the LOCAL workspace member rather than a registry
/// release — that is not a publication, so it too yields `Ok(None)` (see
/// [`is_local_source_resolution`]). Only a genuine query failure
/// (network/auth/spawn), or a *non*-local-source success whose version line is
/// truly unparseable, is `Err`.
///
/// # Errors
/// Returns `Err` if `cargo` cannot be spawned, if `cargo info` fails for a reason
/// other than "not found", or if it succeeds (and did *not* resolve to a local
/// workspace source) but no version line can be parsed.
pub fn max_published_version(name: &str) -> Result<Option<semver::Version>> {
    match cargo_info(&["info", name], None)? {
        CargoInfo::Found(stdout) => {
            // A success can still be the LOCAL workspace crate (path source) rather
            // than a registry release — `version: <v> (from ./path)`. That is not a
            // publication, so report "no published version" rather than erroring.
            if is_local_source_resolution(&stdout) {
                return Ok(None);
            }
            parse_max_version(&stdout).map(Some).ok_or_else(|| {
                anyhow!("`cargo info {name}` succeeded but no version line could be parsed")
            })
        }
        // Never published — a clean absence, not an error.
        CargoInfo::NotFound => Ok(None),
    }
}

/// Publish one crate at `manifest_dir` to the registry.
///
/// Reads the token **value** from the environment variable **named** by
/// `registry_token_env` and passes it to `cargo publish` via the child
/// environment as `CARGO_REGISTRY_TOKEN`. The token is never placed on the
/// command line and never logged.
///
/// If `cargo publish` fails but its output indicates the version was already
/// uploaded / already exists, that is treated as success (the "already exists"
/// race — `publish-crates-logic.md` §"Not published, no tag" step 2).
///
/// # Errors
/// Returns `Err` if the named env var is unset/empty, if `cargo` cannot be
/// spawned, or if `cargo publish` fails for any reason other than the
/// already-uploaded race.
pub fn publish(manifest_dir: &Path, registry_token_env: &str) -> Result<()> {
    let token = std::env::var(registry_token_env).map_err(|_| {
        anyhow!("registry token env var `{registry_token_env}` is not set; cannot publish")
    })?;
    if token.is_empty() {
        bail!("registry token env var `{registry_token_env}` is empty; cannot publish");
    }

    let manifest_path = manifest_dir.join("Cargo.toml");
    let output = Command::new("cargo")
        .args(publish_args(&manifest_path))
        // Token passed via the environment, never as a CLI argument.
        .env(CARGO_REGISTRY_TOKEN_ENV, &token)
        .output()
        .with_context(|| {
            format!(
                "failed to spawn `cargo publish` for {}",
                manifest_dir.display()
            )
        })?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if is_already_uploaded_error(&stderr) {
        // Race won by another runner / earlier attempt: the version is on the
        // registry, so this is a successful publish from our point of view.
        return Ok(());
    }

    Err(anyhow!(
        "`cargo publish` failed for {} (exit {}): {}",
        manifest_dir.display(),
        exit_code_str(&output.status),
        stderr.trim()
    ))
}

/// Publishability preflight: verify every publishable workspace crate can be
/// packaged and verify-built, **without uploading anything**.
///
/// Runs `cargo publish --workspace --dry-run --allow-dirty` from `repo_root`:
/// - `--workspace` packages every publishable member (crates with
///   `publish = false`, like `xtask`, are skipped) and resolves *unpublished*
///   sibling crates against a temporary local registry — so nothing needs to be on
///   crates.io yet;
/// - `--dry-run` performs the full package + verification build but stops before
///   the upload;
/// - `--allow-dirty` lets it run on the prepared-but-uncommitted working tree.
///
/// The verification build re-resolves each crate's dependencies, so it surfaces
/// problems that would otherwise only blow up mid-release — most importantly a
/// **yanked dependency** whose requirement has no non-yanked match.
///
/// Streams cargo's own output. Returns `Err` if cargo cannot be spawned or the
/// dry-run exits non-zero (the streamed output carries the specifics).
pub fn verify_publishable(repo_root: &Path) -> Result<()> {
    let status = Command::new("cargo")
        .current_dir(repo_root)
        .args(["publish", "--workspace", "--dry-run", "--allow-dirty"])
        .status()
        .context("failed to spawn `cargo publish --workspace --dry-run`")?;

    if status.success() {
        return Ok(());
    }

    bail!(
        "publishability preflight failed (`cargo publish --workspace --dry-run`, exit {}). \
         A crate could not be packaged or verify-built — e.g. a yanked dependency, a file \
         referenced but excluded from the package, or a build error. Fix the issue above, or \
         re-run with `--no-verify` to skip this check.",
        exit_code_str(&status),
    )
}

/// Poll the registry until `name@version` indexes, or `timeout` elapses.
///
/// Calls [`is_published`] on a short fixed interval ([`POLL_INTERVAL`]) until it
/// observes `true`. Implements `publish-crates-logic.md` §"Not published, no
/// tag" step 3.
///
/// # Errors
/// Returns `Err` if `timeout` elapses before the version appears (a re-run will
/// resume — the upload itself already succeeded), or if [`is_published`]
/// surfaces an error.
pub fn wait_until_published(name: &str, version: &str, timeout: Duration) -> Result<()> {
    poll_until(
        timeout,
        POLL_INTERVAL,
        || is_published(name, version),
        || std::thread::sleep(POLL_INTERVAL),
    )
    .with_context(|| format!("timed out waiting for {name}@{version} to index on the registry; a re-run will resume (the upload already succeeded)"))
}

/// Generic poll loop, factored out so the success/timeout behavior can be unit-
/// tested without cargo or the network.
///
/// Repeatedly invokes `check`:
/// - returns `Ok(())` the first time `check` yields `Ok(true)`;
/// - propagates any `Err` `check` returns;
/// - returns `Err` (timeout) once `timeout` elapses while `check` keeps yielding
///   `Ok(false)`.
///
/// `sleep` is injected (the real loop sleeps; tests pass a no-op) and `interval`
/// bounds how much budget remains before the final attempt. `now` is read from
/// [`Instant`] so tests with a no-op sleep terminate immediately on timeout.
fn poll_until<C, S>(timeout: Duration, interval: Duration, mut check: C, mut sleep: S) -> Result<()>
where
    C: FnMut() -> Result<bool>,
    S: FnMut(),
{
    let deadline = Instant::now() + timeout;
    loop {
        if check()? {
            return Ok(());
        }
        // If sleeping the interval would overshoot the deadline, give up now.
        if Instant::now() + interval > deadline {
            bail!("poll timed out after {:?}", timeout);
        }
        sleep();
    }
}

/// Build the argument vector for `cargo publish` for the crate whose manifest is
/// at `manifest_path`. Pure (no I/O) and **token-free** by construction — the
/// registry token is supplied only through the child environment, never argv
/// (`publish-crates-logic.md` §Inputs 2). Factored out so the command shape is
/// unit-testable.
fn publish_args(manifest_path: &Path) -> [&std::ffi::OsStr; 3] {
    [
        "publish".as_ref(),
        "--manifest-path".as_ref(),
        manifest_path.as_os_str(),
    ]
}

/// Parse the latest **published** version from `cargo info <name>` stdout.
///
/// Pure (no I/O). `cargo info` prints a `version: …` line reporting the crate's
/// version; this returns the first such version that resolves to a **registry**
/// publication.
///
/// Two annotations can follow the version value, and they mean opposite things:
///
/// - `version: 0.1.0 (from ./utils)` — a **local-source** annotation: `cargo
///   info`, when run *inside the workspace that contains the crate*, resolves to
///   the LOCAL workspace member, which is **not** a registry publication. Such a
///   line is **skipped** (we keep scanning), so an unpublished workspace crate
///   yields `None` rather than masquerading as published.
/// - `version: 1.0.0 (latest 1.0.102)` — a registry annotation pointing at the
///   newest release; this *is* a publication. We keep only the leading semver
///   token (`1.0.0`) and ignore the trailing `(latest …)`.
///
/// For each candidate `version:` field we therefore (a) reject it if its value
/// carries a local-source `(from …)` annotation, then (b) take the first
/// whitespace-delimited token and parse it as a [`semver::Version`]. Returns
/// `None` if no such registry version line is present (empty / unexpected output
/// / only a local-source line), which the caller maps to a genuine error only on
/// a *successful* `cargo info` whose absence wasn't already explained by
/// [`is_not_found_error`] on stderr. Field-name matching is case-insensitive;
/// prereleases (e.g. `1.0.0-rc.1`) parse through unchanged.
/// Remove ANSI CSI escape sequences (`ESC [ … <final byte>`, e.g. SGR color
/// codes) from `s`.
///
/// Pure. `cargo info` colorizes its output in some environments (notably GitHub
/// Actions), wrapping the `version:` line in SGR codes so that it no longer
/// starts with the literal `version:` prefix. We set `CARGO_TERM_COLOR=never` on
/// the `cargo info` invocations to prevent that at the source; stripping here as
/// well keeps the line parsing robust even if colored output slips through.
fn strip_ansi_csi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        // A CSI sequence is ESC '[' … terminated by a byte in '@'..='~'.
        if c == '\x1b' && chars.peek() == Some(&'[') {
            chars.next(); // consume '['
            for e in chars.by_ref() {
                if ('@'..='~').contains(&e) {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// The trimmed *value* of every `version:` field in `cargo info` stdout, ANSI
/// stripped and case-insensitive on the field name (so `rust-version:` and other
/// `*version:` fields are ignored). Shared by [`parse_max_version`] and
/// [`is_local_source_resolution`], which previously hand-rolled the same scan.
fn version_line_values(stdout: &str) -> Vec<String> {
    let stdout = strip_ansi_csi(stdout);
    stdout
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            line.strip_prefix("version:")
                .or_else(|| {
                    line.get(..8)
                        .filter(|p| p.eq_ignore_ascii_case("version:"))
                        .map(|_| &line[8..])
                })
                .map(|rest| rest.trim().to_string())
        })
        .collect()
}

fn parse_max_version(stdout: &str) -> Option<semver::Version> {
    for value in version_line_values(stdout) {
        // A `(from <path>)` annotation means cargo resolved to a LOCAL workspace
        // member (path source), not a registry publication — skip it.
        if is_local_source_version(&value) {
            continue;
        }
        // Keep only the leading semver token; drop any trailing annotation such
        // as `(latest 1.0.102)`.
        let Some(token) = value.split_whitespace().next() else {
            continue;
        };
        if let Ok(v) = semver::Version::parse(token) {
            return Some(v);
        }
    }
    None
}

/// True iff a `cargo info` version-line *value* carries a local-source
/// annotation — i.e. it contains `(from ` (e.g. `0.1.0 (from ./utils)` or
/// `0.1.0 (from /abs/path)`).
///
/// Pure. `cargo info` emits this `(from <path>)` suffix when it resolves the
/// crate to a LOCAL workspace member (a path source) rather than to a registry
/// release — which happens whenever `cargo info <name>` is run from *inside* the
/// workspace that defines `<name>`. Such a result is **not** evidence of a
/// registry publication, so callers must treat it as "not published".
fn is_local_source_version(value: &str) -> bool {
    value.contains("(from ")
}

/// True iff `cargo info` stdout shows the crate was resolved to a LOCAL
/// workspace member — i.e. its `version:` line carries a `(from <path>)`
/// annotation (see [`is_local_source_version`]).
///
/// Pure. Used by [`is_published`] to reject the in-workspace case where `cargo
/// info <name>@<version>` exits 0 against the local path source even though the
/// version was never published to the registry. Other `version:`-like fields
/// (e.g. `rust-version:`) are ignored.
fn is_local_source_resolution(stdout: &str) -> bool {
    version_line_values(stdout)
        .iter()
        .any(|v| is_local_source_version(v))
}

/// True iff `stderr` is `cargo info`'s "no such package/version" wording (as
/// opposed to a genuine query failure like a network or auth error).
fn is_not_found_error(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    s.contains("could not find")
        || s.contains("not found")
        || s.contains("no matching package")
        || s.contains("does not exist")
}

/// True iff `stderr` matches cargo's "already uploaded" / "already exists"
/// wording for a version that is already on the registry.
///
/// Pure and case-insensitive; recognizes the phrases cargo / the registry emit
/// when an upload loses the race. Unrelated failures (auth, network, dirty tree,
/// verification build) return `false`. Implements the success-on-race rule of
/// `publish-crates-logic.md` §"Not published, no tag" step 2.
pub fn is_already_uploaded_error(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    s.contains("already uploaded") || s.contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    // --- is_already_uploaded_error: the "already exists" race = success rule ---
    // (publish-crates-logic.md §"Not published, no tag" step 2)

    #[test]
    fn already_uploaded_matches_realistic_cargo_stderr() {
        // Phrasings cargo / the registry actually emit when a version already
        // exists on the index.
        let positives = [
            "error: failed to publish to registry at https://crates.io\n\nCaused by:\n  the remote server responded with an error (status 200 OK): crate version `0.3.1` is already uploaded",
            "error: crate version `1.0.0` is already uploaded",
            "    Uploading toy-kv-utils v0.3.1\nerror: api errors (status 200 OK): crate version `0.3.1` already exists on crates.io index",
            "the remote server responded with an error: A crate with the name already exists",
            // case-insensitive
            "CRATE VERSION `2.0.0` IS ALREADY UPLOADED",
        ];
        for s in positives {
            assert!(
                is_already_uploaded_error(s),
                "expected already-uploaded match for: {s}"
            );
        }
    }

    #[test]
    fn already_uploaded_rejects_unrelated_failures() {
        // Genuine failures must NOT be swallowed as success.
        let negatives = [
            "error: failed to verify package tarball\n\nCaused by:\n  Source directory was modified by build.rs during cargo publish",
            "error: failed to get a 200 OK response, got 403\nbody:\nmust be authenticated to access this resource",
            "error: failed to publish: network error: connection timed out",
            "error: 1 files in the working directory contain changes that were not yet committed",
            "error: no token found, please run `cargo login`",
            "",
        ];
        for s in negatives {
            assert!(
                !is_already_uploaded_error(s),
                "did not expect already-uploaded match for: {s}"
            );
        }
    }

    // --- is_not_found_error: drives is_published's Ok(false) vs Err split ---
    // (publish-crates-logic.md §"Idempotency scan" (b))

    #[test]
    fn not_found_matches_cargo_info_absence_wording() {
        let positives = [
            "error: could not find `toy-kv-utils@9.9.9` in registry `crates-io`",
            "error: package `nope@1.0.0` not found",
            "error: no matching package named `ghost` found",
            "error: version `0.0.1` does not exist",
        ];
        for s in positives {
            assert!(is_not_found_error(s), "expected not-found match for: {s}");
        }
    }

    #[test]
    fn not_found_rejects_genuine_query_failures() {
        // These must bubble up as Err from is_published, not become Ok(false).
        let negatives = [
            "error: failed to query registry: network error: connection refused",
            "error: failed to authenticate to registry",
            "",
        ];
        for s in negatives {
            assert!(
                !is_not_found_error(s),
                "did not expect not-found match for: {s}"
            );
        }
    }

    // --- parse_max_version: pure parse of `cargo info` stdout ---
    // (orchestrator.md §"Version validation" — current version = highest
    // published registry version; max_published_version feeds that lookup)

    #[test]
    fn parse_max_version_reads_version_line() {
        // Realistic `cargo info <crate>` stdout (header + fields).
        let stdout = "anyhow #error #error-handling\n\
            Flexible concrete Error type built on std::error::Error\n\
            version: 1.0.102\n\
            license: MIT OR Apache-2.0\n\
            rust-version: 1.68\n\
            documentation: https://docs.rs/anyhow\n";
        assert_eq!(
            parse_max_version(stdout),
            Some(semver::Version::new(1, 0, 102))
        );
    }

    #[test]
    fn parse_max_version_does_not_match_rust_version_field() {
        // `rust-version:` also ends in "version:" and is a bare MSRV like `1.68`,
        // which is NOT a valid semver — must be skipped, and the real `version:`
        // line picked instead.
        let stdout = "rust-version: 1.68\nversion: 0.3.1\n";
        assert_eq!(
            parse_max_version(stdout),
            Some(semver::Version::new(0, 3, 1))
        );
    }

    #[test]
    fn parse_max_version_handles_prerelease() {
        let stdout = "some-crate #tag\nversion: 2.0.0-rc.1\nlicense: MIT\n";
        let v = parse_max_version(stdout).expect("prerelease version should parse");
        assert_eq!(v, semver::Version::parse("2.0.0-rc.1").unwrap());
        assert!(!v.pre.is_empty(), "prerelease component must be preserved");
    }

    #[test]
    fn parse_max_version_is_case_insensitive_and_trims() {
        let stdout = "Version:    1.2.3   \n";
        assert_eq!(
            parse_max_version(stdout),
            Some(semver::Version::new(1, 2, 3))
        );
    }

    #[test]
    fn parse_max_version_none_on_empty_or_no_version_line() {
        // Empty output, and output that carries no parseable `version:` field,
        // both yield None (caller decides whether that's an error).
        assert_eq!(parse_max_version(""), None);
        assert_eq!(
            parse_max_version("name #tag\ndescription only\nlicense: MIT\n"),
            None
        );
        // A `version:` line whose value is not semver also yields None.
        assert_eq!(parse_max_version("version: not-a-version\n"), None);
    }

    #[test]
    fn parse_max_version_skips_local_source_line() {
        // In-workspace `cargo info <name>` resolves to the LOCAL workspace member
        // and prints `version: <v> (from ./path)`. That is NOT a registry
        // publication, so it must be skipped — here there is no registry line,
        // so the result is None (caller maps that to "never published").
        let stdout = "toy-kv-utils #key-value #wasm #toy\n\
            Foundational error, JSON, and key-validation helpers for toy-kv.\n\
            version: 0.1.0 (from ./utils)\n\
            license: MIT\n\
            rust-version: 1.85\n";
        assert_eq!(parse_max_version(stdout), None);
        // Absolute-path source annotation is treated identically.
        assert_eq!(parse_max_version("version: 0.1.0 (from /abs/path)\n"), None);
    }

    #[test]
    fn parse_max_version_keeps_only_leading_token() {
        // A genuine registry release at an exact version prints a trailing
        // `(latest …)` annotation — that IS published; keep just `1.0.0`.
        assert_eq!(
            parse_max_version("version: 1.0.0 (latest 1.0.102)\n"),
            Some(semver::Version::new(1, 0, 0))
        );
    }

    // --- is_local_source_version: the `(from …)` path-source marker ---

    #[test]
    fn local_source_version_detects_from_annotation() {
        assert!(is_local_source_version("0.1.0 (from ./utils)"));
        assert!(is_local_source_version("0.1.0 (from /abs/path)"));
        // A registry `(latest …)` annotation is NOT a local source.
        assert!(!is_local_source_version("1.0.0 (latest 1.0.102)"));
        // A bare registry version has no annotation.
        assert!(!is_local_source_version("1.4.2"));
    }

    // --- is_local_source_resolution: drives is_published's Ok(false) for the ---
    // --- in-workspace local-crate hazard (publish-crates-logic.md §scan (b)) ---

    #[test]
    fn local_source_resolution_true_for_workspace_member() {
        // `cargo info <name>@<version>` from inside the workspace exits 0 but
        // resolves to the local path source even for an unpublished version.
        let stdout = "toy-kv-utils #key-value #wasm #toy\n\
            Foundational error, JSON, and key-validation helpers for toy-kv.\n\
            version: 0.1.0 (from ./utils)\n\
            license: MIT\n\
            rust-version: 1.85\n";
        assert!(is_local_source_resolution(stdout));
    }

    #[test]
    fn local_source_resolution_false_for_registry_release() {
        // A real registry publication (exact-version lookup shows `(latest …)`,
        // a plain `version:` line shows nothing) is NOT a local source.
        assert!(!is_local_source_resolution(
            "version: 1.0.0 (latest 1.0.102)\n"
        ));
        assert!(!is_local_source_resolution(
            "anyhow #error\nversion: 1.0.102\nlicense: MIT\n"
        ));
        // `rust-version:` carrying nothing relevant must not be misread.
        assert!(!is_local_source_resolution("rust-version: 1.85\n"));
        assert!(!is_local_source_resolution(""));
    }

    // --- ANSI-color regression (the first-release CI failure) --------------------
    // In some environments (notably GitHub Actions) `cargo info` colorizes its
    // output, wrapping the `version:` line in SGR escape codes so it no longer
    // begins with the literal `version:` prefix. That silently broke BOTH parsers:
    // `is_local_source_resolution` returned false and `parse_max_version` returned
    // None, so an unpublished first-release workspace crate raised
    // "succeeded but no version line could be parsed" instead of Ok(None).
    // We force `CARGO_TERM_COLOR=never` on the `cargo info` calls; these tests pin
    // that the parsers are also robust if colored output slips through.

    /// The exact colored line `cargo info` emits for a local workspace member,
    /// reproduced from a `CARGO_TERM_COLOR=always` run.
    const COLORED_LOCAL_INFO: &str = concat!(
        "toy-kv-utils #key-value #wasm #toy\n",
        "Foundational error, JSON, and key-validation helpers for toy-kv.\n",
        "\x1b[1m\x1b[92mversion:\x1b[0m 0.1.0 \x1b[1m\x1b[94m(from ./utils)\x1b[0m\n",
        "\x1b[1m\x1b[92mlicense:\x1b[0m MIT\n",
    );

    #[test]
    fn strip_ansi_csi_removes_sgr_codes() {
        assert_eq!(
            strip_ansi_csi("\x1b[1m\x1b[92mversion:\x1b[0m 0.1.0"),
            "version: 0.1.0"
        );
        // No escapes → unchanged.
        assert_eq!(strip_ansi_csi("version: 1.2.3"), "version: 1.2.3");
    }

    #[test]
    fn parse_max_version_tolerates_ansi_color_codes() {
        // Colored local-source line is still recognized as local → None (not the
        // "unparseable" error that broke the first CI release).
        assert_eq!(parse_max_version(COLORED_LOCAL_INFO), None);
        // A colored registry line still parses to its version.
        assert_eq!(
            parse_max_version("\x1b[1m\x1b[92mversion:\x1b[0m 1.0.0\n"),
            Some(semver::Version::new(1, 0, 0))
        );
    }

    #[test]
    fn local_source_resolution_tolerates_ansi_color_codes() {
        assert!(is_local_source_resolution(COLORED_LOCAL_INFO));
    }

    // --- max_published_version success-branch decision (local source -> None) ---
    // A `cargo info <name>` that exits 0 but resolved to the LOCAL workspace
    // member is NOT a registry publication: the success branch must map it to
    // "no published version" (Ok(None)), NOT to the "unparseable" error.
    // (publish-crates-logic.md §"Idempotency scan" (b) — in-workspace hazard)

    #[test]
    fn max_published_version_treats_local_source_success_as_unpublished() {
        // Realistic in-workspace `cargo info toy-kv-utils` stdout: exits 0 but
        // the version line is a local path source.
        let stdout = "toy-kv-utils\nversion: 0.1.0 (from ./utils)\n";
        // The success-branch guard: a local-source resolution short-circuits to
        // Ok(None), so this must be detected as a local source...
        assert!(
            is_local_source_resolution(stdout),
            "in-workspace success must be recognized as a local source"
        );
        // ...and parse_max_version must NOT yield a (would-be-erroring) version
        // for it — confirming the only path left is Ok(None), never Err.
        assert_eq!(
            parse_max_version(stdout),
            None,
            "local-source line must not parse as a published version"
        );
    }

    #[test]
    fn max_published_version_registry_success_still_parses() {
        // A genuine registry release is NOT a local source, so the success
        // branch falls through to parse_max_version, which yields the version.
        let stdout = "anyhow #error\nversion: 1.0.102\nlicense: MIT\n";
        assert!(!is_local_source_resolution(stdout));
        assert_eq!(
            parse_max_version(stdout),
            Some(semver::Version::new(1, 0, 102))
        );
    }

    // --- publish_args: command construction, token never on argv ---
    // (publish-crates-logic.md §Inputs 2; §"Not published, no tag" step 1)

    #[test]
    fn publish_args_shape_and_no_token() {
        let manifest = Path::new("/work/utils/Cargo.toml");
        let args = publish_args(manifest);
        assert_eq!(args[0], "publish");
        assert_eq!(args[1], "--manifest-path");
        assert_eq!(args[2], manifest.as_os_str());
        // The token must never appear as a CLI argument; the only env-var name
        // we ever surface is the fixed CARGO_REGISTRY_TOKEN (set on the child
        // env, not argv). Assert no arg looks tokenish.
        for a in args {
            let s = a.to_string_lossy().to_lowercase();
            assert!(!s.contains("token"), "argv must not carry a token: {s}");
            assert!(
                !s.contains("--registry-token"),
                "must not pass token on CLI: {s}"
            );
        }
    }

    // --- poll_until: the generic poll loop (injected check + no-op sleep) ---
    // (publish-crates-logic.md §"Not published, no tag" step 3)

    #[test]
    fn poll_succeeds_when_check_turns_true() {
        // Flip false -> false -> true; must succeed on the 3rd call.
        let calls = Cell::new(0u32);
        let sleeps = Cell::new(0u32);
        let res = poll_until(
            Duration::from_secs(60),
            Duration::from_millis(1),
            || {
                let n = calls.get() + 1;
                calls.set(n);
                Ok(n >= 3)
            },
            || sleeps.set(sleeps.get() + 1),
        );
        assert!(res.is_ok());
        assert_eq!(calls.get(), 3, "should poll until the check turns true");
        assert_eq!(
            sleeps.get(),
            2,
            "should sleep between the two false results"
        );
    }

    #[test]
    fn poll_succeeds_immediately_when_already_true() {
        let calls = Cell::new(0u32);
        let res = poll_until(
            Duration::from_secs(60),
            Duration::from_millis(1),
            || {
                calls.set(calls.get() + 1);
                Ok(true)
            },
            || panic!("should not sleep when already published"),
        );
        assert!(res.is_ok());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn poll_times_out_when_check_never_true() {
        // Zero timeout with a non-trivial interval: the deadline is already past,
        // so the first false result must yield a timeout Err without sleeping.
        let sleeps = Cell::new(0u32);
        let res = poll_until(
            Duration::from_secs(0),
            Duration::from_secs(1),
            || Ok(false),
            || sleeps.set(sleeps.get() + 1),
        );
        assert!(res.is_err(), "expected a timeout error");
        assert_eq!(sleeps.get(), 0, "must not sleep past the deadline");
    }

    #[test]
    fn poll_propagates_check_error() {
        let res = poll_until(
            Duration::from_secs(60),
            Duration::from_millis(1),
            || Err(anyhow!("boom")),
            || {},
        );
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "boom");
    }
}
