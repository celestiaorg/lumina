use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;

use super::Ops;
use super::*;
use crate::domain::types::UpdatedPackage;

// ── MockOps (Call enum + Ops impl) ──────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Call {
    RunCmd {
        program: String,
        args: Vec<String>,
        cwd: Option<PathBuf>,
    },
    CmdSuccess {
        program: String,
        args: Vec<String>,
    },
    ReadFile {
        path: PathBuf,
    },
    WriteFile {
        path: PathBuf,
        content: String,
    },
    GitCheckout {
        branch: String,
    },
    GitHasChanges {
        paths: Vec<String>,
    },
    CommitAndPush {
        branch: String,
        message: String,
        paths: Vec<String>,
    },
    GhaOutput {
        key: String,
        value: String,
    },
    GhaOutputMultiline {
        key: String,
        value: String,
    },
}

impl Call {
    fn label(&self) -> String {
        match self {
            Call::RunCmd { program, args, .. } => cmd_string(program, args),
            Call::CmdSuccess { program, args } => format!("[check] {}", cmd_string(program, args)),
            Call::ReadFile { path } => format!("[read_file] {}", path.display()),
            Call::WriteFile { path, .. } => format!("[write_file] {}", path.display()),
            Call::GitCheckout { branch } => format!("[git] checkout {branch}"),
            Call::GitHasChanges { .. } => "[git] has_changes".to_string(),
            Call::CommitAndPush { branch, .. } => format!("[git] commit_and_push {branch}"),
            Call::GhaOutput { key, .. } => format!("[gha] {key}"),
            Call::GhaOutputMultiline { key, .. } => format!("[gha:ml] {key}"),
        }
    }
}

fn cmd_string(program: &str, args: &[String]) -> String {
    if args.is_empty() {
        program.to_string()
    } else {
        format!("{program} {}", args.join(" "))
    }
}

struct MockOps {
    calls: RefCell<Vec<Call>>,
    labeled_cmd_outputs: RefCell<Vec<(String, String)>>,
    labeled_cmd_statuses: RefCell<Vec<(String, bool)>>,
    files: RefCell<HashMap<PathBuf, String>>,
    git_has_changes_results: RefCell<Vec<bool>>,
}

impl MockOps {
    fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            labeled_cmd_outputs: RefCell::new(Vec::new()),
            labeled_cmd_statuses: RefCell::new(Vec::new()),
            files: RefCell::new(HashMap::new()),
            git_has_changes_results: RefCell::new(Vec::new()),
        }
    }

    fn on_cmd(self, label: &str, output: &str) -> Self {
        self.labeled_cmd_outputs
            .borrow_mut()
            .push((label.to_string(), output.to_string()));
        self
    }

    fn on_cmd_success(self, label: &str, status: bool) -> Self {
        self.labeled_cmd_statuses
            .borrow_mut()
            .push((label.to_string(), status));
        self
    }

    fn with_file(self, path: &str, content: &str) -> Self {
        self.files
            .borrow_mut()
            .insert(PathBuf::from(path), content.to_string());
        self
    }

    fn with_git_has_changes(self, b: bool) -> Self {
        self.git_has_changes_results.borrow_mut().push(b);
        self
    }

    fn find_call(&self, pattern: &str) -> Call {
        let calls = self.calls.borrow();
        calls
            .iter()
            .find(|c| c.label().starts_with(pattern))
            .unwrap_or_else(|| {
                panic!(
                    "no call with label starting with `{pattern}`\nrecorded calls:\n{}",
                    format_call_list(&calls)
                )
            })
            .clone()
    }

    #[allow(dead_code)]
    fn assert_not_called(&self, pattern: &str) {
        let calls = self.calls.borrow();
        assert!(
            !calls.iter().any(|c| c.label().starts_with(pattern)),
            "expected no call matching `{pattern}` but found one\nrecorded calls:\n{}",
            format_call_list(&calls)
        );
    }

    fn assert_sequence(&self, expected: &[&str]) {
        let calls = self.calls.borrow();
        assert_eq!(
            calls.len(),
            expected.len(),
            "expected {} calls but got {}\nrecorded calls:\n{}",
            expected.len(),
            calls.len(),
            format_call_list(&calls)
        );
        for (i, (call, pattern)) in calls.iter().zip(expected.iter()).enumerate() {
            let label = call.label();
            assert!(
                label.starts_with(pattern),
                "call[{i}]: expected label starting with `{pattern}`, got `{label}`\nfull sequence:\n{}",
                format_call_list(&calls)
            );
        }
    }
}

fn format_call_list(calls: &[Call]) -> String {
    calls
        .iter()
        .enumerate()
        .map(|(i, c)| format!("  [{i}] {}", c.label()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn take_labeled<T>(entries: &mut Vec<(String, T)>, cmd_str: &str) -> Option<T> {
    let idx = entries
        .iter()
        .position(|(label, _)| cmd_str.starts_with(label.as_str()))?;
    Some(entries.remove(idx).1)
}

impl Ops for MockOps {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String> {
        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.calls.borrow_mut().push(Call::RunCmd {
            program: program.to_string(),
            args: args_owned.clone(),
            cwd: cwd.map(PathBuf::from),
        });
        let cmd_str = cmd_string(program, &args_owned);
        let output =
            take_labeled(&mut self.labeled_cmd_outputs.borrow_mut(), &cmd_str).unwrap_or_default();
        Ok(output)
    }

    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool> {
        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.calls.borrow_mut().push(Call::CmdSuccess {
            program: program.to_string(),
            args: args_owned.clone(),
        });
        let cmd_str = cmd_string(program, &args_owned);
        let status =
            take_labeled(&mut self.labeled_cmd_statuses.borrow_mut(), &cmd_str).unwrap_or(false);
        Ok(status)
    }

    fn read_file(&self, path: &Path) -> Result<String> {
        self.calls.borrow_mut().push(Call::ReadFile {
            path: path.to_path_buf(),
        });
        self.files
            .borrow()
            .get(path)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("mock: file not found: {}", path.display()))
    }

    fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::WriteFile {
            path: path.to_path_buf(),
            content: content.to_string(),
        });
        Ok(())
    }

    fn git_checkout(&self, branch: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::GitCheckout {
            branch: branch.to_string(),
        });
        Ok(())
    }

    fn git_has_changes(&self, paths: &[&str]) -> Result<bool> {
        self.calls.borrow_mut().push(Call::GitHasChanges {
            paths: paths.iter().map(|s| s.to_string()).collect(),
        });
        let mut results = self.git_has_changes_results.borrow_mut();
        Ok(if results.is_empty() {
            false
        } else {
            results.remove(0)
        })
    }

    fn commit_and_push(&self, branch: &str, message: &str, paths: &[&str]) -> Result<()> {
        self.calls.borrow_mut().push(Call::CommitAndPush {
            branch: branch.to_string(),
            message: message.to_string(),
            paths: paths.iter().map(|s| s.to_string()).collect(),
        });
        Ok(())
    }

    fn gha_output(&self, key: &str, value: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::GhaOutput {
            key: key.to_string(),
            value: value.to_string(),
        });
        Ok(())
    }

    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::GhaOutputMultiline {
            key: key.to_string(),
            value: value.to_string(),
        });
        Ok(())
    }
}

// ── Pure function tests ──────────────────────────────────────────────────

#[test]
fn rc_suffix_is_extracted_from_semver() {
    assert_eq!(extract_rc_suffix("1.2.3-rc.7"), Some("-rc.7"));
}

#[test]
fn missing_suffix_returns_none() {
    assert_eq!(extract_rc_suffix("1.2.3"), None);
}

#[test]
fn rc_suffix_empty_version() {
    assert_eq!(extract_rc_suffix(""), None);
}

#[test]
fn rc_suffix_no_dash() {
    assert_eq!(extract_rc_suffix("rc.1"), None);
}

#[test]
fn rc_suffix_double_rc() {
    assert_eq!(extract_rc_suffix("1.0.0-rc.1-rc.2"), Some("-rc.2"));
}

#[test]
fn rc_suffix_just_prefix() {
    assert_eq!(extract_rc_suffix("-rc."), None);
}

#[test]
fn rc_suffix_zero() {
    assert_eq!(extract_rc_suffix("1.0.0-rc.0"), Some("-rc.0"));
}

#[test]
fn find_uniffi_tag_valid() {
    let releases = json!([
        {"package_name": "lumina-node", "tag": "lumina-node-v1.0.0"},
        {"package_name": "lumina-node-uniffi", "tag": "lumina-node-uniffi-v1.0.0"}
    ]);
    assert_eq!(
        find_uniffi_tag(&releases),
        Some("lumina-node-uniffi-v1.0.0".to_string())
    );
}

#[test]
fn find_uniffi_tag_missing_package() {
    let releases = json!([
        {"package_name": "lumina-node", "tag": "lumina-node-v1.0.0"}
    ]);
    assert_eq!(find_uniffi_tag(&releases), None);
}

#[test]
fn find_uniffi_tag_empty_array() {
    assert_eq!(find_uniffi_tag(&json!([])), None);
}

#[test]
fn find_uniffi_tag_no_tag_field() {
    let releases = json!([{"package_name": "lumina-node-uniffi"}]);
    assert_eq!(find_uniffi_tag(&releases), None);
}

#[test]
fn command_display_empty_args() {
    assert_eq!(command_display("ls", &[]), "ls");
}

#[test]
fn command_display_multiple_args() {
    assert_eq!(
        command_display("git", &["commit", "-m", "msg"]),
        "git commit -m msg"
    );
}

// ── build_release_contract tests ─────────────────────────────────────────

#[test]
fn release_contract_with_pr_and_rc_version() {
    let report = ExecuteReport {
        head_branch: "release-plz-2025-01-01".to_string(),
        pr_url: Some("https://github.com/org/repo/pull/1".to_string()),
        updated_packages: vec![UpdatedPackage {
            package: "lumina-node".to_string(),
            version: "1.1.0-rc.1".to_string(),
        }],
    };
    let contract = build_release_contract(Some(&report), &json!([]));
    assert!(contract.prs_created);
    assert_eq!(contract.pr["head_branch"], "release-plz-2025-01-01");
    assert_eq!(
        contract.pr["html_url"],
        "https://github.com/org/repo/pull/1"
    );
    assert_eq!(contract.node_rc_prefix, "-rc.1");
    assert!(!contract.releases_created);
}

#[test]
fn release_contract_no_pr_with_releases() {
    let payload = json!([
        {"package_name": "lumina-node", "tag": "lumina-node-v1.0.0-rc.1"}
    ]);
    let contract = build_release_contract(None, &payload);
    assert!(!contract.prs_created);
    assert_eq!(contract.node_rc_prefix, "");
    assert!(contract.releases_created);
    assert_eq!(contract.releases.as_array().unwrap().len(), 1);
}

#[test]
fn release_contract_empty() {
    let contract = build_release_contract(None, &json!([]));
    assert!(!contract.prs_created);
    assert!(!contract.releases_created);
}

// ── npm-update-pr handler tests ──────────────────────────────────────────

fn npm_update_pr_args(pr_json: &str, rc_prefix: &str) -> GhaNpmUpdatePrArgs {
    GhaNpmUpdatePrArgs {
        pr_json: pr_json.to_string(),
        node_rc_prefix: rc_prefix.to_string(),
    }
}

fn npm_update_pr_base_mock() -> MockOps {
    MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.0.0"}"#)
        .on_cmd("npm version", "")
        .on_cmd("wasm-pack build", "")
        .on_cmd("npm install", "")
        .on_cmd("npm clean-install", "")
        .on_cmd("npm run tsc", "")
        .on_cmd("npm run update-readme", "")
        .with_git_has_changes(true)
}

#[test]
fn npm_update_pr_stable_full_flow() {
    let ops = npm_update_pr_base_mock();
    let args = npm_update_pr_args(r#"{"head_branch":"feature/branch"}"#, "");
    handle_gha_npm_update_pr_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "[git] checkout",
        "cargo pkgid",
        "[read_file]",
        "npm version",
        "wasm-pack build",
        "npm install",
        "npm clean-install",
        "npm run tsc",
        "npm run update-readme",
        "[git] has_changes",
        "[git] commit_and_push",
    ]);

    assert_eq!(
        ops.find_call("[git] checkout"),
        Call::GitCheckout {
            branch: "feature/branch".to_string()
        }
    );
    assert_eq!(
        ops.find_call("npm version"),
        Call::RunCmd {
            program: "npm".to_string(),
            args: vec![
                "version".to_string(),
                "1.2.3".to_string(),
                "--no-git-tag-version".to_string()
            ],
            cwd: Some(PathBuf::from("node-wasm/js")),
        }
    );
    assert_eq!(
        ops.find_call("[git] commit_and_push"),
        Call::CommitAndPush {
            branch: "feature/branch".to_string(),
            message: "update lumina-node npm package".to_string(),
            paths: vec![
                "node-wasm/js/package.json".to_string(),
                "node-wasm/js/package-lock.json".to_string(),
                "node-wasm/js/README.md".to_string(),
                "node-wasm/js/index.d.ts".to_string(),
            ],
        }
    );
}

#[test]
fn npm_update_pr_rc_version_flow() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3-rc.1")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.0.0"}"#)
        .on_cmd("npm version", "")
        .on_cmd("wasm-pack build", "")
        .on_cmd("npm install", "")
        .on_cmd("npm clean-install", "")
        .on_cmd("npm run tsc", "")
        .on_cmd("npm run update-readme", "")
        .with_git_has_changes(true);

    let args = npm_update_pr_args(r#"{"head_branch":"release/rc"}"#, "-rc.1");
    handle_gha_npm_update_pr_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "[git] checkout",
        "cargo pkgid",
        "[read_file]",
        "npm version",
        "wasm-pack build",
        "npm install",
        "npm clean-install",
        "npm run tsc",
        "npm run update-readme",
        "[git] has_changes",
        "[git] commit_and_push",
    ]);
    // RC prefix is appended to the base version for npm
    assert_eq!(
        ops.find_call("npm version"),
        Call::RunCmd {
            program: "npm".to_string(),
            args: vec![
                "version".to_string(),
                "1.2.3-rc.1".to_string(),
                "--no-git-tag-version".to_string()
            ],
            cwd: Some(PathBuf::from("node-wasm/js")),
        }
    );
}

#[test]
fn npm_update_pr_already_up_to_date() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3"}"#);

    let args = npm_update_pr_args(r#"{"head_branch":"feature/branch"}"#, "");
    handle_gha_npm_update_pr_impl(args, &ops).unwrap();

    // Early return: only checkout + version check, no build/push
    ops.assert_sequence(&["[git] checkout", "cargo pkgid", "[read_file]"]);
}

#[test]
fn npm_update_pr_no_changes_after_build() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.0.0"}"#)
        .on_cmd("npm version", "")
        .on_cmd("wasm-pack build", "")
        .on_cmd("npm install", "")
        .on_cmd("npm clean-install", "")
        .on_cmd("npm run tsc", "")
        .on_cmd("npm run update-readme", "")
        .with_git_has_changes(false);

    let args = npm_update_pr_args(r#"{"head_branch":"feature/branch"}"#, "");
    handle_gha_npm_update_pr_impl(args, &ops).unwrap();

    // No changes → stops after has_changes check, no commit/push
    ops.assert_sequence(&[
        "[git] checkout",
        "cargo pkgid",
        "[read_file]",
        "npm version",
        "wasm-pack build",
        "npm install",
        "npm clean-install",
        "npm run tsc",
        "npm run update-readme",
        "[git] has_changes",
    ]);
}

#[test]
fn npm_update_pr_invalid_json() {
    let ops = MockOps::new();
    let args = npm_update_pr_args("not json", "");
    assert!(handle_gha_npm_update_pr_impl(args, &ops).is_err());
}

#[test]
fn npm_update_pr_empty_head_branch() {
    let ops = MockOps::new();
    let args = npm_update_pr_args(r#"{"head_branch":""}"#, "");
    assert!(handle_gha_npm_update_pr_impl(args, &ops).is_err());
}

// ── npm-publish handler tests ────────────────────────────────────────────

#[test]
fn npm_publish_no_artifacts() {
    let ops = MockOps::new();
    let args = GhaNpmPublishArgs { no_artifacts: true };
    handle_gha_npm_publish_impl(args, &ops).unwrap();
    ops.assert_sequence(&[]);
}

#[test]
fn npm_publish_already_published() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3"}"#)
        .on_cmd_success("npm view", true)
        .on_cmd_success("npm view", true);

    let args = GhaNpmPublishArgs {
        no_artifacts: false,
    };
    handle_gha_npm_publish_impl(args, &ops).unwrap();

    // Early return after npm view confirms both versions exist
    ops.assert_sequence(&[
        "cargo pkgid",
        "[read_file]",
        "[check] npm view",
        "[check] npm view",
    ]);
}

#[test]
fn npm_publish_wasm_already_published_js_missing() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3"}"#)
        // wasm exists
        .on_cmd_success("npm view", true)
        // js missing
        .on_cmd_success("npm view", false)
        .on_cmd("npm pkg set", "")
        .on_cmd("npm publish", "");

    let args = GhaNpmPublishArgs {
        no_artifacts: false,
    };
    handle_gha_npm_publish_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "cargo pkgid",
        "[read_file]",
        "[check] npm view",
        "[check] npm view",
        "npm pkg set",
        "npm publish",
    ]);
    ops.assert_not_called("wasm-pack build");
    ops.assert_not_called("wasm-pack publish");
}

#[test]
fn npm_publish_wasm_missing_js_already_published() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3"}"#)
        // wasm missing
        .on_cmd_success("npm view", false)
        // js exists
        .on_cmd_success("npm view", true)
        .on_cmd("wasm-pack build", "")
        .on_cmd("wasm-pack publish", "");

    let args = GhaNpmPublishArgs {
        no_artifacts: false,
    };
    handle_gha_npm_publish_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "cargo pkgid",
        "[read_file]",
        "[check] npm view",
        "[check] npm view",
        "wasm-pack build",
        "wasm-pack publish",
    ]);
    ops.assert_not_called("npm pkg set");
    ops.assert_not_called("npm publish");
}

#[test]
fn npm_publish_stable_flow() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3"}"#)
        .on_cmd_success("npm view", false)
        .on_cmd_success("npm view", false)
        .on_cmd("wasm-pack build", "")
        .on_cmd("wasm-pack publish", "")
        .on_cmd("npm pkg set", "")
        .on_cmd("npm publish", "");

    let args = GhaNpmPublishArgs {
        no_artifacts: false,
    };
    handle_gha_npm_publish_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "cargo pkgid",
        "[read_file]",
        "[check] npm view",
        "[check] npm view",
        "wasm-pack build",
        "wasm-pack publish",
        "npm pkg set",
        "npm publish",
    ]);
    // Stable publish: no --tag flag
    assert_eq!(
        ops.find_call("wasm-pack publish"),
        Call::RunCmd {
            program: "wasm-pack".to_string(),
            args: vec![
                "publish".to_string(),
                "--access".to_string(),
                "public".to_string(),
                "node-wasm".to_string()
            ],
            cwd: None,
        }
    );
    assert_eq!(
        ops.find_call("npm publish"),
        Call::RunCmd {
            program: "npm".to_string(),
            args: vec![
                "publish".to_string(),
                "--access".to_string(),
                "public".to_string()
            ],
            cwd: Some(PathBuf::from("node-wasm/js")),
        }
    );
}

#[test]
fn npm_publish_rc_flow() {
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3-rc.1")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3-rc.1"}"#)
        .on_cmd_success("npm view", false)
        .on_cmd_success("npm view", false)
        .on_cmd("wasm-pack build", "")
        .on_cmd("wasm-pack publish", "")
        .on_cmd("npm pkg set", "")
        .on_cmd("npm publish", "");

    let args = GhaNpmPublishArgs {
        no_artifacts: false,
    };
    handle_gha_npm_publish_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "cargo pkgid",
        "[read_file]",
        "[check] npm view",
        "[check] npm view",
        "wasm-pack build",
        "wasm-pack publish",
        "npm pkg set",
        "npm publish",
    ]);
    // RC publish: --tag rc on both wasm-pack publish and npm publish
    assert_eq!(
        ops.find_call("wasm-pack publish"),
        Call::RunCmd {
            program: "wasm-pack".to_string(),
            args: vec![
                "publish".to_string(),
                "--access".to_string(),
                "public".to_string(),
                "--tag".to_string(),
                "rc".to_string(),
                "node-wasm".to_string()
            ],
            cwd: None,
        }
    );
    assert_eq!(
        ops.find_call("npm publish"),
        Call::RunCmd {
            program: "npm".to_string(),
            args: vec![
                "publish".to_string(),
                "--access".to_string(),
                "public".to_string(),
                "--tag".to_string(),
                "rc".to_string()
            ],
            cwd: Some(PathBuf::from("node-wasm/js")),
        }
    );
}

#[test]
fn npm_publish_rc_detection_uses_package_json() {
    // cargo version is stable, but package.json has rc → should publish with --tag rc
    let ops = MockOps::new()
        .on_cmd("cargo pkgid", "pkg@1.2.3")
        .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3-rc.1"}"#)
        .on_cmd_success("npm view", false)
        .on_cmd_success("npm view", false)
        .on_cmd("wasm-pack build", "")
        .on_cmd("wasm-pack publish", "")
        .on_cmd("npm pkg set", "")
        .on_cmd("npm publish", "");

    let args = GhaNpmPublishArgs {
        no_artifacts: false,
    };
    handle_gha_npm_publish_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "cargo pkgid",
        "[read_file]",
        "[check] npm view",
        "[check] npm view",
        "wasm-pack build",
        "wasm-pack publish",
        "npm pkg set",
        "npm publish",
    ]);
    // RC detection comes from package.json, not cargo version
    assert_eq!(
        ops.find_call("wasm-pack publish"),
        Call::RunCmd {
            program: "wasm-pack".to_string(),
            args: vec![
                "publish".to_string(),
                "--access".to_string(),
                "public".to_string(),
                "--tag".to_string(),
                "rc".to_string(),
                "node-wasm".to_string()
            ],
            cwd: None,
        }
    );
}

// ── uniffi-release handler tests ─────────────────────────────────────────

fn uniffi_releases_json() -> String {
    serde_json::to_string(&json!([
        {"package_name": "lumina-node-uniffi", "tag": "lumina-node-uniffi-v1.0.0"}
    ]))
    .unwrap()
}

#[test]
fn uniffi_release_no_artifacts() {
    let ops = MockOps::new();
    let args = GhaUniffiReleaseArgs {
        releases_json: "[]".to_string(),
        gha_output: false,
        no_artifacts: true,
    };
    handle_gha_uniffi_release_impl(args, &ops).unwrap();
    ops.assert_sequence(&[]);
}

#[test]
fn uniffi_release_full_flow() {
    let ops = MockOps::new()
        .on_cmd("./node-uniffi/build-ios.sh", "")
        .on_cmd("tar -cvzf lumina-node-uniffi-ios", "")
        .on_cmd("./node-uniffi/build-android.sh", "")
        .on_cmd("tar -cvzf lumina-node-uniffi-android", "")
        .on_cmd("shasum", "abc123  ios.tar.gz\ndef456  android.tar.gz");

    let args = GhaUniffiReleaseArgs {
        releases_json: uniffi_releases_json(),
        gha_output: true,
        no_artifacts: false,
    };
    handle_gha_uniffi_release_impl(args, &ops).unwrap();

    ops.assert_sequence(&[
        "./node-uniffi/build-ios.sh",
        "tar -cvzf lumina-node-uniffi-ios",
        "./node-uniffi/build-android.sh",
        "tar -cvzf lumina-node-uniffi-android",
        "shasum",
        "[write_file]",
        "[gha] uniffi_tag",
        "[gha:ml] uniffi_files",
    ]);

    assert_eq!(
        ops.find_call("[gha] uniffi_tag"),
        Call::GhaOutput {
            key: "uniffi_tag".to_string(),
            value: "lumina-node-uniffi-v1.0.0".to_string(),
        }
    );
}

#[test]
fn uniffi_release_gha_output_disabled() {
    let ops = MockOps::new()
        .on_cmd("./node-uniffi/build-ios.sh", "")
        .on_cmd("tar -cvzf lumina-node-uniffi-ios", "")
        .on_cmd("./node-uniffi/build-android.sh", "")
        .on_cmd("tar -cvzf lumina-node-uniffi-android", "")
        .on_cmd("shasum", "checksums");

    let args = GhaUniffiReleaseArgs {
        releases_json: uniffi_releases_json(),
        gha_output: false,
        no_artifacts: false,
    };
    handle_gha_uniffi_release_impl(args, &ops).unwrap();

    // Same build/tar/checksum steps, but no GHA output calls
    ops.assert_sequence(&[
        "./node-uniffi/build-ios.sh",
        "tar -cvzf lumina-node-uniffi-ios",
        "./node-uniffi/build-android.sh",
        "tar -cvzf lumina-node-uniffi-android",
        "shasum",
        "[write_file]",
    ]);
}

#[test]
fn uniffi_release_missing_uniffi_tag() {
    let ops = MockOps::new();
    let args = GhaUniffiReleaseArgs {
        releases_json: r#"[{"package_name":"lumina-node","tag":"v1.0.0"}]"#.to_string(),
        gha_output: false,
        no_artifacts: false,
    };
    assert!(handle_gha_uniffi_release_impl(args, &ops).is_err());
}

#[test]
fn uniffi_release_invalid_json() {
    let ops = MockOps::new();
    let args = GhaUniffiReleaseArgs {
        releases_json: "not json".to_string(),
        gha_output: false,
        no_artifacts: false,
    };
    assert!(handle_gha_uniffi_release_impl(args, &ops).is_err());
}

// ── is_published tests ────────────────────────────────────────────────

#[test]
fn is_published_with_releases() {
    assert!(is_published(&json!([{"name": "pkg", "version": "1.0.0"}])));
}

#[test]
fn is_published_empty_array() {
    assert!(!is_published(&json!([])));
}

#[test]
fn is_published_null() {
    assert!(!is_published(&serde_json::Value::Null));
}

#[test]
fn is_published_multiple_releases() {
    assert!(is_published(&json!([
        {"name": "core"},
        {"name": "sdk"},
        {"name": "cli"}
    ])));
}
