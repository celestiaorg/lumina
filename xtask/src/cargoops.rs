//! Cargo/registry helpers for the crate-publishing step of `cargo xtask release`.
//! Everything here shells out to `cargo`.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

/// Env var `cargo publish` reads the registry token from. We always pass the
/// token to the child under this name, whatever env var the caller stored it in.
const CARGO_REGISTRY_TOKEN_ENV: &str = "CARGO_REGISTRY_TOKEN";

/// Interval between registry polls in [`wait_until_published`].
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Numeric exit code, or `"signal"` when the process was killed by a signal.
fn exit_code_str(status: &std::process::ExitStatus) -> String {
    status
        .code()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "signal".into())
}

/// Outcome of a `cargo info` query: found (its stdout), or cleanly absent.
enum CargoInfo {
    Found(String),
    NotFound,
}

/// Run `cargo info <args>` with color forced off, optionally from `cwd`.
///
/// Color is forced off because some environments (e.g. GitHub Actions) wrap the
/// `version:` line in ANSI SGR codes, which breaks the downstream line parsing.
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

/// Whether exactly `name@version` exists on the registry. A clean "not found" is
/// `Ok(false)`, not an error.
///
/// Run from the OS temp dir (outside the workspace): inside the workspace that
/// defines `name`, `cargo info` resolves to the LOCAL member and exits 0
/// regardless of the registry, so an unpublished crate would look "resolved" and
/// [`wait_until_published`] would never observe the registry. The local-source
/// guard stays as a fallback.
///
/// # Errors
/// Returns `Err` if `cargo` cannot be spawned, or if `cargo info` fails for a
/// reason other than "not found" (e.g. network/auth failure).
pub fn is_published(name: &str, version: &str) -> Result<bool> {
    let spec = format!("{name}@{version}");
    let outside = std::env::temp_dir();
    match cargo_info(&["info", &spec], Some(&outside))? {
        CargoInfo::Found(stdout) => Ok(!is_local_source_resolution(&stdout)),
        CargoInfo::NotFound => Ok(false),
    }
}

/// Highest version of `name` published on the registry, or `None` if it has never
/// been published.
///
/// A clean "not found" yields `Ok(None)`, mirroring [`is_published`]. So does a
/// local-source resolution (`version: <v> (from ./path)`): run inside the
/// workspace, `cargo info` resolved to the LOCAL member, which is not a
/// publication. Only a genuine query failure, or a non-local-source success whose
/// version line is unparseable, is `Err`.
///
/// # Errors
/// Returns `Err` if `cargo` cannot be spawned, if `cargo info` fails for a reason
/// other than "not found", or if it succeeds (and did *not* resolve to a local
/// workspace source) but no version line can be parsed.
pub fn max_published_version(name: &str) -> Result<Option<semver::Version>> {
    match cargo_info(&["info", name], None)? {
        CargoInfo::Found(stdout) => {
            // A local-source resolution is the workspace member, not a
            // publication — report "no published version".
            if is_local_source_resolution(&stdout) {
                return Ok(None);
            }
            parse_max_version(&stdout).map(Some).ok_or_else(|| {
                anyhow!("`cargo info {name}` succeeded but no version line could be parsed")
            })
        }
        CargoInfo::NotFound => Ok(None),
    }
}

/// Publish the crate at `manifest_dir` to the registry.
///
/// The token value is read from the env var named by `registry_token_env` and
/// passed to `cargo publish` via the child environment as `CARGO_REGISTRY_TOKEN`
/// — never on the command line, never logged. An "already uploaded" / "already
/// exists" failure is treated as success (the race won by another runner).
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
        // Version already on the registry: a successful publish for us.
        return Ok(());
    }

    Err(anyhow!(
        "`cargo publish` failed for {} (exit {}): {}",
        manifest_dir.display(),
        exit_code_str(&output.status),
        stderr.trim()
    ))
}

/// Preflight: package and verify-build every publishable workspace crate without
/// uploading anything.
///
/// Runs `cargo publish --workspace --dry-run --allow-dirty` from `repo_root`:
/// `--workspace` skips `publish = false` crates and resolves unpublished siblings
/// against a temporary local registry; `--dry-run` stops before the upload;
/// `--allow-dirty` allows the prepared-but-uncommitted tree. The verification
/// build surfaces problems like a yanked dependency with no non-yanked match.
/// Streams cargo's output. Returns `Err` if cargo cannot be spawned or the
/// dry-run exits non-zero.
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
/// # Errors
/// Returns `Err` if `timeout` elapses before the version appears (a re-run will
/// resume — the upload itself already succeeded), or if [`is_published`] errors.
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
/// tested without cargo or the network. `sleep` is injected (the real loop
/// sleeps; tests pass a no-op).
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

/// Build the `cargo publish` argv for the manifest at `manifest_path`. Token-free
/// by construction — the token is supplied only through the child environment.
fn publish_args(manifest_path: &Path) -> [&std::ffi::OsStr; 3] {
    [
        "publish".as_ref(),
        "--manifest-path".as_ref(),
        manifest_path.as_os_str(),
    ]
}

/// Latest published version from `cargo info <name>` stdout, skipping any
/// local-source line so an unpublished workspace crate yields `None`. Keeps only
/// the leading semver token; field-name matching is case-insensitive.
fn parse_max_version(stdout: &str) -> Option<semver::Version> {
    for value in version_line_values(stdout) {
        // `(from <path>)` = local workspace member, not a registry publication.
        if is_local_source_version(&value) {
            continue;
        }
        // Keep only the leading semver token (drop e.g. `(latest 1.0.102)`).
        let Some(token) = value.split_whitespace().next() else {
            continue;
        };
        if let Ok(v) = semver::Version::parse(token) {
            return Some(v);
        }
    }
    None
}

/// Remove ANSI CSI escape sequences (`ESC [ … <final byte>`) from `s`. `cargo
/// info` colorizes output in some environments (notably GitHub Actions), wrapping
/// the `version:` line in SGR codes so it no longer starts with `version:`;
/// stripping keeps the line parsing robust.
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

/// Trimmed value of every `version:` field in `cargo info` stdout, ANSI-stripped
/// and case-insensitive on the field name (so `rust-version:` and other
/// `*version:` fields are ignored).
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

/// True iff a `cargo info` version-line value carries a `(from <path>)`
/// annotation — cargo resolved to a LOCAL workspace member (a path source), not a
/// registry release, so it is not a publication.
fn is_local_source_version(value: &str) -> bool {
    value.contains("(from ")
}

/// True iff `cargo info` stdout resolved to a LOCAL workspace member (its
/// `version:` line carries a `(from <path>)` annotation). Used by
/// [`is_published`] to reject the in-workspace case where an exact-version lookup
/// exits 0 against the local path source for a never-published version.
fn is_local_source_resolution(stdout: &str) -> bool {
    version_line_values(stdout)
        .iter()
        .any(|v| is_local_source_version(v))
}

/// True iff `stderr` is `cargo info`'s "no such package/version" wording (not a
/// genuine query failure like a network or auth error).
fn is_not_found_error(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    s.contains("could not find")
        || s.contains("not found")
        || s.contains("no matching package")
        || s.contains("does not exist")
}

/// True iff `stderr` is cargo's "already uploaded" / "already exists" wording for
/// a version already on the registry. Case-insensitive; unrelated failures (auth,
/// network, dirty tree, verification build) return `false`.
pub fn is_already_uploaded_error(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    s.contains("already uploaded") || s.contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn already_uploaded_matches_realistic_cargo_stderr() {
        // Phrasings cargo / the registry emit when a version already exists.
        let positives = [
            "error: failed to publish to registry at https://crates.io\n\nCaused by:\n  the remote server responded with an error (status 200 OK): crate version `0.3.1` is already uploaded",
            "error: crate version `1.0.0` is already uploaded",
            "    Uploading lumina-utils v0.3.1\nerror: api errors (status 200 OK): crate version `0.3.1` already exists on crates.io index",
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

    #[test]
    fn not_found_matches_cargo_info_absence_wording() {
        let positives = [
            "error: could not find `lumina-utils@9.9.9` in registry `crates-io`",
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

    #[test]
    fn parse_max_version_reads_version_line() {
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
        // `rust-version:` also ends in "version:" but is a bare MSRV, not semver.
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
        // In-workspace `cargo info` resolves to the local member and prints
        // `version: <v> (from ./path)`; not a publication, so None.
        let stdout = "lumina-utils #key-value #wasm #toy\n\
            Foundational error, JSON, and key-validation helpers for lumina.\n\
            version: 0.1.0 (from ./utils)\n\
            license: MIT\n\
            rust-version: 1.85\n";
        assert_eq!(parse_max_version(stdout), None);
        // Absolute-path source annotation is treated identically.
        assert_eq!(parse_max_version("version: 0.1.0 (from /abs/path)\n"), None);
    }

    #[test]
    fn parse_max_version_keeps_only_leading_token() {
        // A genuine release prints a trailing `(latest …)`; keep just `1.0.0`.
        assert_eq!(
            parse_max_version("version: 1.0.0 (latest 1.0.102)\n"),
            Some(semver::Version::new(1, 0, 0))
        );
    }

    #[test]
    fn local_source_version_detects_from_annotation() {
        assert!(is_local_source_version("0.1.0 (from ./utils)"));
        assert!(is_local_source_version("0.1.0 (from /abs/path)"));
        // A registry `(latest …)` annotation is NOT a local source.
        assert!(!is_local_source_version("1.0.0 (latest 1.0.102)"));
        // A bare registry version has no annotation.
        assert!(!is_local_source_version("1.4.2"));
    }

    #[test]
    fn local_source_resolution_true_for_workspace_member() {
        // `cargo info <name>@<version>` from inside the workspace exits 0 but
        // resolves to the local path source even for an unpublished version.
        let stdout = "lumina-utils #key-value #wasm #toy\n\
            Foundational error, JSON, and key-validation helpers for lumina.\n\
            version: 0.1.0 (from ./utils)\n\
            license: MIT\n\
            rust-version: 1.85\n";
        assert!(is_local_source_resolution(stdout));
    }

    #[test]
    fn local_source_resolution_false_for_registry_release() {
        assert!(!is_local_source_resolution(
            "version: 1.0.0 (latest 1.0.102)\n"
        ));
        assert!(!is_local_source_resolution(
            "anyhow #error\nversion: 1.0.102\nlicense: MIT\n"
        ));
        // `rust-version:` must not be misread.
        assert!(!is_local_source_resolution("rust-version: 1.85\n"));
        assert!(!is_local_source_resolution(""));
    }

    // ANSI-color regression: colored `cargo info` output wraps the `version:`
    // line in SGR codes, which once broke both parsers. We force
    // `CARGO_TERM_COLOR=never`; these tests pin that the parsers are also robust
    // if colored output slips through.

    /// The colored line `cargo info` emits for a local workspace member.
    const COLORED_LOCAL_INFO: &str = concat!(
        "lumina-utils #key-value #wasm #toy\n",
        "Foundational error, JSON, and key-validation helpers for lumina.\n",
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
        // Colored local-source line is still recognized as local → None.
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

    #[test]
    fn max_published_version_treats_local_source_success_as_unpublished() {
        // In-workspace `cargo info` exits 0 with a local path source; the success
        // branch must map it to Ok(None), not the "unparseable" error.
        let stdout = "lumina-utils\nversion: 0.1.0 (from ./utils)\n";
        assert!(
            is_local_source_resolution(stdout),
            "in-workspace success must be recognized as a local source"
        );
        assert_eq!(
            parse_max_version(stdout),
            None,
            "local-source line must not parse as a published version"
        );
    }

    #[test]
    fn max_published_version_registry_success_still_parses() {
        // A genuine release is not a local source, so parse_max_version yields it.
        let stdout = "anyhow #error\nversion: 1.0.102\nlicense: MIT\n";
        assert!(!is_local_source_resolution(stdout));
        assert_eq!(
            parse_max_version(stdout),
            Some(semver::Version::new(1, 0, 102))
        );
    }

    #[test]
    fn publish_args_shape_and_no_token() {
        let manifest = Path::new("/work/utils/Cargo.toml");
        let args = publish_args(manifest);
        assert_eq!(args[0], "publish");
        assert_eq!(args[1], "--manifest-path");
        assert_eq!(args[2], manifest.as_os_str());
        // The token must never appear on argv.
        for a in args {
            let s = a.to_string_lossy().to_lowercase();
            assert!(!s.contains("token"), "argv must not carry a token: {s}");
            assert!(
                !s.contains("--registry-token"),
                "must not pass token on CLI: {s}"
            );
        }
    }

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
        // Zero timeout: the deadline is already past, so the first false result
        // yields a timeout Err without sleeping.
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
