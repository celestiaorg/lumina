//! npm / WASM primitives and the two release-time procedures.
//!
//! Owns the npm-side logic of the release tool: deriving the npm dist-tag from a
//! version, plus two procedures:
//!
//! - [`update_for_pr`] — prepare-time: mirror the workspace version into the JS
//!   wrapper and refresh its lockfile against a fresh `wasm-pack` build.
//!   Idempotent, publishes nothing, needs no credentials.
//! - [`publish`] — release-time: build + publish the wasm bindings and the
//!   wrapper, both at the same version under the derived dist-tag, repinning the
//!   wrapper's dependency to the concrete published version.
//!
//! The procedures shell out to `wasm-pack` and `npm`, so they are not exercised in
//! unit tests (the toolchain/registry/token may be absent in CI). The pure pieces —
//! [`dist_tag`] and the private version-suffix / command-argument helpers — are
//! unit-tested.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};

/// One configured npm component: a compiled wasm-bindgen package plus the JS
/// wrapper that depends on it.
#[derive(Debug, Clone)]
pub struct NpmComponent {
    /// npm/crate name of the compiled wasm-bindgen package, e.g. `"toy-kv-wasm"`.
    ///
    /// Used as the argument to `npm show <wasm_crate> version` (the publish
    /// idempotency guard) and as the `dependencies[...]` key repinned in the wrapper.
    pub wasm_crate: String,
    /// Path (relative to the repo root) to the JS wrapper package directory, e.g.
    /// `"wasm/js"` — the `package.json` that is version-set / dependency-repinned.
    pub package_dir: String,
}

impl NpmComponent {
    /// Construct a component from its two configured fields.
    pub fn new(wasm_crate: impl Into<String>, package_dir: impl Into<String>) -> Self {
        Self {
            wasm_crate: wasm_crate.into(),
            package_dir: package_dir.into(),
        }
    }

    /// The wasm crate directory `wasm-pack build` operates on, relative to the
    /// repo root: the **parent** of the JS wrapper's `package_dir`. `wasm-pack
    /// build <dir>` writes to `<dir>/pkg`, which the wrapper installs via
    /// `../pkg` — so the wasm dir is always the wrapper dir's parent (e.g.
    /// `node-wasm/js` → `node-wasm`, `wasm/js` → `wasm`).
    fn wasm_dir(&self) -> &str {
        Path::new(&self.package_dir)
            .parent()
            .and_then(|p| p.to_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(".")
    }
}

/// Derive the npm dist-tag from a version's prerelease suffix.
///
/// Pure and total: no I/O, never panics.
///
/// - No prerelease suffix (no `-`) → `"latest"`.
/// - Otherwise the leading alphabetic identifier of the prerelease part:
///   `0.2.0-rc.1` → `"rc"`, `0.2.0-alpha.2` → `"alpha"`, `0.2.0-beta.3` → `"beta"`.
///
/// Not an allow-list: any label other than `rc`/`alpha`/`beta` passes through
/// verbatim, e.g. `1.0.0-next.5` → `"next"`.
///
/// # Examples
///
/// ```
/// use xtask::npmops::dist_tag;
/// assert_eq!(dist_tag("0.2.0"), "latest");
/// assert_eq!(dist_tag("0.2.0-rc.1"), "rc");
/// assert_eq!(dist_tag("0.2.0-alpha.2"), "alpha");
/// assert_eq!(dist_tag("0.2.0-beta.3"), "beta");
/// ```
pub fn dist_tag(version: &str) -> String {
    match prerelease_label(version) {
        None => "latest".to_string(),
        Some(label) => label.to_string(),
    }
}

/// Extract the leading alphabetic prerelease label from a version string.
///
/// Pure. Returns `None` when the version has no prerelease suffix (no `-`), else
/// the substring of the prerelease part (everything after the first `-`) up to —
/// but not including — the first `.` or ASCII digit. Equivalent to the shell
/// `npm_tag="${local_version#*-}"; npm_tag="${npm_tag%%[.0-9]*}"`.
///
/// Degenerate inputs: a trailing `-` (empty suffix) yields `Some("")`.
fn prerelease_label(version: &str) -> Option<&str> {
    let suffix = version.split_once('-')?.1;
    let end = suffix
        .find(|c: char| c == '.' || c.is_ascii_digit())
        .unwrap_or(suffix.len());
    Some(&suffix[..end])
}

/// Prepare-time procedure.
///
/// Mirrors the resolved workspace `version` into the wrapper's `package.json`
/// (idempotently — skips when already at `version`), rebuilds the wasm package,
/// and refreshes the wrapper lockfile against the fresh build. Validates that the
/// wrapper still compiles.
///
/// Publishes nothing and needs no credentials. Leaves changed:
/// `<package_dir>/package.json`, `<package_dir>/package-lock.json`, the
/// regenerated declarations (`<package_dir>/index.d.ts`), and the regenerated
/// `<package_dir>/README.md` (typedoc) — all committed into the release PR, so
/// `release` can publish the committed wrapper source (`index.js`/`worker.js`)
/// as-is without a build step.
///
/// # Errors
///
/// Returns an error if any spawned command fails to start (e.g. `npm`/`wasm-pack`
/// absent) or exits non-zero, with context naming the failing step.
pub fn update_for_pr(component: &NpmComponent, version: &str) -> Result<()> {
    let pkg_dir = Path::new(&component.package_dir);

    // Set the wrapper version, idempotently: skip when already at target so the
    // step never produces an empty change.
    let current = npm_pkg_get_version(pkg_dir)?;
    if current == version {
        return Ok(());
    }
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .args(["version", version, "--no-git-tag-version"]),
        "npm version (set wrapper version)",
    )?;

    // Build the wasm package into <wasm_dir>/pkg/.
    run(
        Command::new("wasm-pack").args(["build", component.wasm_dir()]),
        "wasm-pack build",
    )?;

    // Refresh the lockfile against the fresh ../pkg build, then validate it.
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .args(["install", "--save", "../pkg"]),
        "npm install --save ../pkg",
    )?;
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .arg("clean-install"),
        "npm clean-install",
    )?;

    // Regenerate the committed type declarations (index.d.ts) from the JSDoc in the
    // hand-written wrapper, against the fresh wasm types. The wrapper's .js is source
    // (published as-is); tsc emits declarations only, so this is both the "refresh
    // committed .d.ts" step and a compile check.
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .args(["run", "tsc"]),
        "npm run tsc",
    )?;

    // Regenerate the wrapper README from its public API via typedoc, so the committed
    // README stays in sync with the wrapper on every release PR.
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .args(["run", "update-readme"]),
        "npm run update-readme",
    )?;

    Ok(())
}

/// Release-time procedure.
///
/// Publishes the wasm bindings (`component.wasm_crate`) and the JS wrapper, both at
/// `version` under `dist_tag`. The wasm crate is built + published with `wasm-pack`;
/// the wrapper is published from its committed hand-written source
/// (`index.js`/`worker.js` + the `index.d.ts` regenerated during `prepare`) with
/// only a dependency repin — no build or `npm install` at release time.
///
/// Idempotent per package: each of the two packages is published only if npm does
/// not already have this version, so a resumed run finishes whichever half is
/// missing (e.g. wasm already up, wrapper not).
///
/// `npm_token_env` is the NAME of the environment variable holding the npm auth
/// token (e.g. `"NPM_REGISTRY_TOKEN"`). The token value is read from the environment
/// and re-exported to the publish child processes under the *same* name, so the
/// committed `.npmrc` (`//registry.npmjs.org/:_authToken=${<name>}`) resolves it. It
/// is never placed on a command line, logged, or stored.
///
/// # Errors
///
/// Returns an error if a build/publish command is missing or exits non-zero, or if
/// `npm_token_env` is unset when a publish step needs it (the error names the
/// missing variable, never its value).
pub fn publish(
    component: &NpmComponent,
    version: &str,
    dist_tag: &str,
    npm_token_env: &str,
) -> Result<()> {
    let pkg_dir = Path::new(&component.package_dir);

    // Read the npm token from the env var *named* by npm_token_env. Resolve it once,
    // up front, so a missing token fails before we build/publish anything.
    let token = std::env::var(npm_token_env).map_err(|_| {
        anyhow!("npm token env var `{npm_token_env}` is not set (publish requires it)")
    })?;

    // The wasm bindings crate and the JS wrapper are two npm packages, published
    // independently: each step is guarded by its own `npm show` so a resumed run
    // (e.g. wasm already up, wrapper not) finishes the missing half.

    // Build + publish the wasm bindings package, unless npm already has it.
    if npm_show_version(&component.wasm_crate)?.as_deref() != Some(version) {
        run(
            Command::new("wasm-pack").args(["build", component.wasm_dir()]),
            "wasm-pack build",
        )?;
        run(
            Command::new("wasm-pack")
                .args([
                    "publish",
                    "--access",
                    "public",
                    "--tag",
                    dist_tag,
                    component.wasm_dir(),
                ])
                .env(npm_token_env, &token),
            "wasm-pack publish",
        )?;
    }

    // Publish the wrapper, unless npm already has it. The wrapper's npm name is read
    // from its own package.json (it differs from the wasm crate name).
    let wrapper = npm_pkg_get(pkg_dir, "name")?;
    if npm_show_version(&wrapper)?.as_deref() != Some(version) {
        // Repin the wrapper's wasm dependency from `file:../pkg` to the concrete
        // published version, so the published wrapper depends on the registry crate.
        run(
            Command::new("npm")
                .current_dir(pkg_dir)
                .arg("pkg")
                .arg("set")
                .arg(npm_pkg_set_dep_arg(&component.wasm_crate, version)),
            "npm pkg set (repin wasm dep)",
        )?;
        // Publish the wrapper. It ships its committed hand-written source
        // (index.js/worker.js) plus the index.d.ts regenerated during `prepare` —
        // no build or install at release time. The `files` allow-list bounds the
        // tarball to those files, so the repinned dependency is only a metadata
        // change.
        run(
            Command::new("npm")
                .current_dir(pkg_dir)
                .args(["publish", "--access", "public", "--tag", dist_tag])
                .env(npm_token_env, &token),
            "npm publish (wrapper)",
        )?;
    }

    Ok(())
}

/// Build the `dependencies[<crate>]=<version>` argument for `npm pkg set`.
///
/// Pure; factored out so the (quoting-sensitive) argument shape is unit-testable.
fn npm_pkg_set_dep_arg(wasm_crate: &str, version: &str) -> String {
    format!("dependencies[{wasm_crate}]={version}")
}

/// Read the wrapper's current `package.json` version via `npm pkg get version`.
///
/// `npm pkg get version` prints the value JSON-quoted (e.g. `"0.2.0"`); the quotes
/// are stripped.
fn npm_pkg_get_version(pkg_dir: &Path) -> Result<String> {
    npm_pkg_get(pkg_dir, "version")
}

/// Read a top-level `package.json` field via `npm pkg get <field>` (e.g. `name`,
/// `version`). npm prints the value JSON-quoted; the quotes are stripped.
fn npm_pkg_get(pkg_dir: &Path, field: &str) -> Result<String> {
    let out = Command::new("npm")
        .current_dir(pkg_dir)
        .args(["pkg", "get", field])
        .output()
        .with_context(|| format!("failed to run `npm pkg get {field}`"))?;
    if !out.status.success() {
        bail!(
            "`npm pkg get {field}` failed (status {}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(parse_npm_get_version(&String::from_utf8_lossy(&out.stdout)))
}

/// Parse the stdout of `npm pkg get version` into a bare version string.
///
/// Pure: trims surrounding whitespace and the JSON quotes npm wraps the value in.
fn parse_npm_get_version(stdout: &str) -> String {
    stdout.trim().trim_matches('"').to_string()
}

/// Resolve the version npm currently has published for `pkg`, via `npm show`.
///
/// Returns `Ok(None)` when the package has never been published (e.g. `npm show`
/// exits non-zero or prints nothing) so the caller treats it as "not yet
/// published" and proceeds. Returns `Ok(Some(v))` when a published version is
/// reported.
fn npm_show_version(pkg: &str) -> Result<Option<String>> {
    let out = Command::new("npm")
        .args(["show", pkg, "version"])
        .output()
        .context("failed to run `npm show <pkg> version`")?;
    if !out.status.success() {
        // Not-yet-published / unknown package: treat as no published version.
        return Ok(None);
    }
    let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
    Ok(if v.is_empty() { None } else { Some(v) })
}

/// Run a child command to completion, mapping a spawn failure or non-zero exit
/// into an `anyhow` error with `step` context. Stdout/stderr are inherited.
fn run(cmd: &mut Command, step: &str) -> Result<()> {
    let status = cmd
        .status()
        .with_context(|| format!("failed to spawn `{step}`"))?;
    if !status.success() {
        bail!("`{step}` failed with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // dist_tag: standard cases

    #[test]
    fn dist_tag_plain_is_latest() {
        assert_eq!(dist_tag("0.2.0"), "latest");
    }

    #[test]
    fn dist_tag_rc() {
        assert_eq!(dist_tag("0.2.0-rc.1"), "rc");
    }

    #[test]
    fn dist_tag_alpha() {
        assert_eq!(dist_tag("0.2.0-alpha.2"), "alpha");
    }

    #[test]
    fn dist_tag_beta() {
        assert_eq!(dist_tag("0.2.0-beta.3"), "beta");
    }

    // edge cases

    #[test]
    fn dist_tag_label_without_dot_number() {
        // `1.0.0-rc` (no `.N`) → `rc`.
        assert_eq!(dist_tag("1.0.0-rc"), "rc");
    }

    #[test]
    fn dist_tag_label_with_trailing_digit() {
        // `1.0.0-rc1` → digits stripped → `rc`.
        assert_eq!(dist_tag("1.0.0-rc1"), "rc");
    }

    #[test]
    fn dist_tag_non_standard_label_passes_through() {
        // Not an allow-list: any leading-alpha label is returned verbatim.
        assert_eq!(dist_tag("1.0.0-next.5"), "next");
    }

    #[test]
    fn dist_tag_trailing_dash_is_empty() {
        // Degenerate: trailing `-` with empty suffix → empty tag.
        assert_eq!(dist_tag("1.0.0-"), "");
    }

    #[test]
    fn dist_tag_splits_on_first_dash() {
        // Only the first `-` separates version from prerelease.
        assert_eq!(dist_tag("1.0.0-rc.1-extra"), "rc");
    }

    #[test]
    fn prerelease_label_none_when_no_suffix() {
        assert_eq!(prerelease_label("0.2.0"), None);
    }

    #[test]
    fn prerelease_label_stops_at_dot() {
        assert_eq!(prerelease_label("0.2.0-alpha.2"), Some("alpha"));
    }

    #[test]
    fn prerelease_label_stops_at_digit() {
        assert_eq!(prerelease_label("0.2.0-beta3"), Some("beta"));
    }

    #[test]
    fn prerelease_label_empty_suffix() {
        assert_eq!(prerelease_label("0.2.0-"), Some(""));
    }

    // pure command/arg builders

    #[test]
    fn npm_pkg_set_dep_arg_shape() {
        assert_eq!(
            npm_pkg_set_dep_arg("toy-kv-wasm", "0.2.0"),
            "dependencies[toy-kv-wasm]=0.2.0"
        );
    }

    #[test]
    fn npm_pkg_set_dep_arg_with_prerelease() {
        assert_eq!(
            npm_pkg_set_dep_arg("toy-kv-wasm", "0.2.0-rc.1"),
            "dependencies[toy-kv-wasm]=0.2.0-rc.1"
        );
    }

    #[test]
    fn parse_npm_get_version_strips_quotes_and_whitespace() {
        assert_eq!(parse_npm_get_version("\"0.2.0\"\n"), "0.2.0");
        assert_eq!(parse_npm_get_version("0.2.0\n"), "0.2.0");
        assert_eq!(parse_npm_get_version("  \"1.2.3-rc.1\"  "), "1.2.3-rc.1");
    }

    // struct construction

    #[test]
    fn npm_component_new_sets_fields() {
        let c = NpmComponent::new("toy-kv-wasm", "wasm/js");
        assert_eq!(c.wasm_crate, "toy-kv-wasm");
        assert_eq!(c.package_dir, "wasm/js");
    }
}
