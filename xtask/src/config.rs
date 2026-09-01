//! Release-tool configuration: parsing the optional repo-root `release.toml`.
//!
//! The file is optional: a crate-only workspace omits it and [`load`] returns a
//! default (empty) [`Config`].

use std::fs;
use std::io;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// The file name read, relative to the repository root.
const CONFIG_FILE_NAME: &str = "release.toml";

/// Full parsed `release.toml` configuration. Both fields default, so an empty,
/// partial, or absent file still yields a valid `Config`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Config {
    /// The `[defaults]` block; `Defaults::default()` when absent.
    #[serde(default)]
    pub defaults: Defaults,
    /// Zero or more `[[npm]]` publish-target entries, in file order.
    #[serde(default)]
    pub npm: Vec<NpmComponent>,
}

/// The `[defaults]` table — release defaults.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Defaults {
    /// Fallback for `prepare-release`'s `--branch-prefix` (e.g. `"release-"`);
    /// `None` when absent.
    #[serde(default)]
    pub branch_prefix: Option<String>,
}

/// One `[[npm]]` entry — a single published npm package (not a test-only build).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct NpmComponent {
    /// Name of the crate `wasm-pack` builds into the published wasm package
    /// (e.g. `"lumina-node-wasm"`). Required.
    pub wasm_crate: String,
    /// Repo-root-relative path of the JS wrapper package directory
    /// (e.g. `"wasm/js"`). Required.
    pub package_dir: String,
}

/// Loads and parses `<repo_root>/release.toml`.
///
/// A missing file is not an error: it yields `Config::default()`. Any other I/O
/// failure, or malformed/schema-invalid TOML, is returned as an error.
pub fn load(repo_root: &Path) -> Result<Config> {
    let path = repo_root.join(CONFIG_FILE_NAME);
    match fs::read_to_string(&path) {
        Ok(contents) => {
            from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Config::default()),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Parses an in-memory `release.toml` string into a [`Config`]. An empty string
/// yields `Config::default()`; malformed or schema-invalid TOML is an error.
pub fn from_str(s: &str) -> Result<Config> {
    let config: Config = toml::from_str(s).context("invalid release.toml")?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full example: `[defaults]` + one `[[npm]]`.
    #[test]
    fn parses_full_example() {
        let toml = r#"
            [defaults]
            branch_prefix = "release-"

            [[npm]]
            wasm_crate  = "lumina-node-wasm"
            package_dir = "wasm/js"
        "#;
        let cfg = from_str(toml).expect("valid config");
        assert_eq!(cfg.defaults.branch_prefix.as_deref(), Some("release-"));
        assert_eq!(cfg.npm.len(), 1);
        assert_eq!(cfg.npm[0].wasm_crate, "lumina-node-wasm");
        assert_eq!(cfg.npm[0].package_dir, "wasm/js");
    }

    /// A missing `release.toml` yields the default/empty config.
    #[test]
    fn missing_file_is_default() {
        let dir = std::env::temp_dir().join(format!(
            "m1-config-missing-{}-{}",
            std::process::id(),
            line!()
        ));
        let cfg = load(&dir).expect("missing file must not error");
        assert_eq!(cfg, Config::default());
        assert_eq!(cfg.defaults.branch_prefix, None);
        assert!(cfg.npm.is_empty());
    }

    /// `[defaults]` only, no `[[npm]]` -> empty npm vec.
    #[test]
    fn defaults_only() {
        let cfg = from_str("[defaults]\nbranch_prefix = \"rel/\"\n").expect("valid");
        assert_eq!(cfg.defaults.branch_prefix.as_deref(), Some("rel/"));
        assert!(cfg.npm.is_empty());
    }

    /// Several `[[npm]]` entries preserved in file order.
    #[test]
    fn multiple_npm_entries_in_order() {
        let toml = r#"
            [[npm]]
            wasm_crate  = "a-wasm"
            package_dir = "a/js"

            [[npm]]
            wasm_crate  = "b-wasm"
            package_dir = "b/js"
        "#;
        let cfg = from_str(toml).expect("valid");
        assert_eq!(cfg.npm.len(), 2);
        assert_eq!(cfg.npm[0].wasm_crate, "a-wasm");
        assert_eq!(cfg.npm[1].wasm_crate, "b-wasm");
        assert_eq!(cfg.defaults.branch_prefix, None);
    }

    /// Zero `[[npm]]` and empty string are both valid.
    #[test]
    fn zero_npm_and_empty_string() {
        let cfg = from_str("").expect("empty string is valid");
        assert_eq!(cfg, Config::default());
        assert!(cfg.npm.is_empty());
    }

    /// Malformed TOML is an error.
    #[test]
    fn malformed_toml_is_error() {
        assert!(from_str("this is not = = toml [[[").is_err());
    }

    /// A `[[npm]]` missing a required field is an error.
    #[test]
    fn npm_entry_missing_field_is_error() {
        let toml = r#"
            [[npm]]
            wasm_crate = "only-this"
        "#;
        assert!(from_str(toml).is_err());
    }

    /// `load` parses an existing well-formed file.
    #[test]
    fn load_reads_existing_file() {
        let dir =
            std::env::temp_dir().join(format!("m1-config-load-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(CONFIG_FILE_NAME);
        std::fs::write(&path, "[defaults]\nbranch_prefix = \"r-\"\n").unwrap();
        let cfg = load(&dir).expect("valid file");
        assert_eq!(cfg.defaults.branch_prefix.as_deref(), Some("r-"));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `load` on an existing-but-malformed file is an error.
    #[test]
    fn load_malformed_file_is_error() {
        let dir =
            std::env::temp_dir().join(format!("m1-config-bad-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(CONFIG_FILE_NAME), "= bad [[[").unwrap();
        assert!(load(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
