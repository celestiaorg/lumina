//! npm / WASM primitives and the two release-time procedures.
//!
//! [`update_for_pr`] mirrors the workspace version into the JS wrapper and
//! refreshes its lockfile; [`publish`] builds and publishes the wasm bindings and
//! the wrapper. Both shell out to `wasm-pack`/`npm`, so only the pure helpers
//! ([`dist_tag`] and friends) are unit-tested.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};

/// One configured npm component: a compiled wasm-bindgen package plus its JS wrapper.
#[derive(Debug, Clone)]
pub struct NpmComponent {
    /// npm/crate name of the compiled wasm-bindgen package, e.g. `"lumina-node-wasm"`.
    pub wasm_crate: String,
    /// Repo-root-relative path to the JS wrapper package directory, e.g. `"wasm/js"`.
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

    /// Directory `wasm-pack build` operates on: the parent of the wrapper's
    /// `package_dir` (the wrapper installs the build via `../pkg`).
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
/// No prerelease suffix → `"latest"`; otherwise the leading alphabetic label of
/// the prerelease part (`0.2.0-rc.1` → `"rc"`). Not an allow-list: any other
/// label passes through verbatim (`1.0.0-next.5` → `"next"`).
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

/// Extract the leading alphabetic prerelease label, or `None` when there is no
/// prerelease suffix. Takes the prerelease part (after the first `-`) up to the
/// first `.` or ASCII digit; a trailing `-` yields `Some("")`.
fn prerelease_label(version: &str) -> Option<&str> {
    let suffix = version.split_once('-')?.1;
    let end = suffix
        .find(|c: char| c == '.' || c.is_ascii_digit())
        .unwrap_or(suffix.len());
    Some(&suffix[..end])
}

/// Prepare-time procedure: mirror the workspace `version` into the wrapper's
/// `package.json` (idempotent — skips when already at `version`), rebuild the
/// wasm package, and refresh the wrapper lockfile. Publishes nothing and needs
/// no credentials.
///
/// # Errors
///
/// Returns an error if any spawned command is missing or exits non-zero, with
/// context naming the failing step.
pub fn update_for_pr(component: &NpmComponent, version: &str) -> Result<()> {
    let pkg_dir = Path::new(&component.package_dir);

    // Idempotent: skip when already at the target version.
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

    // Regenerate the committed type declarations; tsc also acts as a compile check.
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .args(["run", "tsc"]),
        "npm run tsc",
    )?;

    // Regenerate the wrapper README from its public API.
    run(
        Command::new("npm")
            .current_dir(pkg_dir)
            .args(["run", "update-readme"]),
        "npm run update-readme",
    )?;

    Ok(())
}

/// Release-time procedure: publish the wasm bindings (`component.wasm_crate`) and
/// the JS wrapper, both at `version` under `dist_tag`. The wasm crate is built +
/// published with `wasm-pack`; the wrapper is published from its committed source
/// with only a dependency repin. Idempotent per package: each is published only
/// if npm does not already have this version.
///
/// `npm_token_env` is the NAME of the env var holding the npm auth token (e.g.
/// `"NPM_REGISTRY_TOKEN"`). The value is read from the environment and re-exported
/// to the publish child processes under the *same* name, so the committed `.npmrc`
/// resolves it. It is never placed on a command line, logged, or stored.
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

    // Resolve the token named by npm_token_env once, up front, so a missing token
    // fails before we build or publish anything.
    let token = std::env::var(npm_token_env).map_err(|_| {
        anyhow!("npm token env var `{npm_token_env}` is not set (publish requires it)")
    })?;

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

    // Publish the wrapper (its npm name comes from its own package.json, which
    // differs from the wasm crate name), unless npm already has it.
    let wrapper = npm_pkg_get(pkg_dir, "name")?;
    if npm_show_version(&wrapper)?.as_deref() != Some(version) {
        // Repin the wrapper's wasm dependency to the concrete published version.
        run(
            Command::new("npm")
                .current_dir(pkg_dir)
                .arg("pkg")
                .arg("set")
                .arg(npm_pkg_set_dep_arg(&component.wasm_crate, version)),
            "npm pkg set (repin wasm dep)",
        )?;
        // Publish the wrapper from its committed source; no build at release time.
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
fn npm_pkg_set_dep_arg(wasm_crate: &str, version: &str) -> String {
    format!("dependencies[{wasm_crate}]={version}")
}

/// Read the wrapper's `package.json` version via `npm pkg get version`.
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

/// Parse the stdout of `npm pkg get` into a bare string: trims whitespace and the
/// JSON quotes npm wraps the value in.
fn parse_npm_get_version(stdout: &str) -> String {
    stdout.trim().trim_matches('"').to_string()
}

/// Resolve the version npm currently has published for `pkg`, or `None` when the
/// package has never been published.
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
/// into an error with `step` context. Stdout/stderr are inherited.
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

    #[test]
    fn dist_tag_label_without_dot_number() {
        assert_eq!(dist_tag("1.0.0-rc"), "rc");
    }

    #[test]
    fn dist_tag_label_with_trailing_digit() {
        assert_eq!(dist_tag("1.0.0-rc1"), "rc");
    }

    #[test]
    fn dist_tag_non_standard_label_passes_through() {
        assert_eq!(dist_tag("1.0.0-next.5"), "next");
    }

    #[test]
    fn dist_tag_trailing_dash_is_empty() {
        assert_eq!(dist_tag("1.0.0-"), "");
    }

    #[test]
    fn dist_tag_splits_on_first_dash() {
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

    #[test]
    fn npm_pkg_set_dep_arg_shape() {
        assert_eq!(
            npm_pkg_set_dep_arg("lumina-node-wasm", "0.2.0"),
            "dependencies[lumina-node-wasm]=0.2.0"
        );
    }

    #[test]
    fn npm_pkg_set_dep_arg_with_prerelease() {
        assert_eq!(
            npm_pkg_set_dep_arg("lumina-node-wasm", "0.2.0-rc.1"),
            "dependencies[lumina-node-wasm]=0.2.0-rc.1"
        );
    }

    #[test]
    fn parse_npm_get_version_strips_quotes_and_whitespace() {
        assert_eq!(parse_npm_get_version("\"0.2.0\"\n"), "0.2.0");
        assert_eq!(parse_npm_get_version("0.2.0\n"), "0.2.0");
        assert_eq!(parse_npm_get_version("  \"1.2.3-rc.1\"  "), "1.2.3-rc.1");
    }

    #[test]
    fn npm_component_new_sets_fields() {
        let c = NpmComponent::new("lumina-node-wasm", "wasm/js");
        assert_eq!(c.wasm_crate, "lumina-node-wasm");
        assert_eq!(c.package_dir, "wasm/js");
    }
}
