use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::adapters::github_output::{write_github_output, write_github_output_multiline};
use crate::application::gha_ops::{Ops, RealOps};
use crate::application::pipeline::{ExecuteArgs, ReleasePipeline};
use crate::domain::model::{RELEASE_PR_TITLE_PREFIX, RELEASE_PR_TITLE_RC};
use crate::domain::types::{
    AuthContext, BranchContext, CommonContext, ExecuteContext, ExecuteReport, PublishContext,
    ReleaseMode, ReleaseReport,
};

#[derive(Debug, Clone)]
pub struct GhaReleasePlzArgs {
    pub mode: ReleaseMode,
    pub compare_branch: Option<String>,
    pub default_branch: String,
    pub rc_branch_prefix: String,
    pub final_branch_prefix: String,
    pub gha_output: bool,
    pub no_artifacts: bool,
}

#[derive(Debug, Clone)]
pub struct GhaNpmUpdatePrArgs {
    pub pr_json: String,
    pub node_rc_prefix: String,
}

#[derive(Debug, Clone)]
pub struct GhaNpmPublishArgs {
    pub no_artifacts: bool,
}

#[derive(Debug, Clone)]
pub struct GhaUniffiReleaseArgs {
    pub releases_json: String,
    pub gha_output: bool,
    pub no_artifacts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseContract {
    pub pr: Value,
    pub prs_created: bool,
    pub releases: Value,
    pub releases_created: bool,
    pub node_rc_prefix: String,
}

impl Default for ReleaseContract {
    fn default() -> Self {
        Self {
            pr: json!({}),
            prs_created: false,
            releases: json!([]),
            releases_created: false,
            node_rc_prefix: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ReleaseDriverDecision {
    Execute(ReleaseMode),
    Publish(ReleaseMode),
}

#[derive(Debug, Deserialize)]
struct PullRequestPayload {
    head_branch: String,
}

pub async fn handle_gha_release_plz(
    pipeline: &ReleasePipeline,
    args: GhaReleasePlzArgs,
) -> Result<ReleaseContract> {
    let event_name = std::env::var("GITHUB_EVENT_NAME").unwrap_or_else(|_| "push".to_string());
    let cmd = release_command(&event_name, args.mode)?;
    let branch = args
        .compare_branch
        .clone()
        .unwrap_or_else(|| args.default_branch.clone());

    let contract = match cmd {
        ReleaseDriverDecision::Execute(mode) => {
            let report = pipeline
                .execute(ExecuteArgs {
                    ctx: ExecuteContext {
                        common: CommonContext {
                            mode,
                            default_branch: branch.clone(),
                            auth: AuthContext::from_env(),
                        },
                        branch: BranchContext { skip_pr: false },
                    },
                })
                .await?;
            contract_from_execute(&report)
        }
        ReleaseDriverDecision::Publish(mode) => {
            let report = pipeline
                .publish(PublishContext {
                    common: CommonContext {
                        mode,
                        default_branch: branch.clone(),
                        auth: AuthContext::from_env(),
                    },
                    rc_branch_prefix: args.rc_branch_prefix.clone(),
                    final_branch_prefix: args.final_branch_prefix.clone(),
                    no_artifacts: args.no_artifacts,
                })
                .await?;
            contract_from_publish(&report)
        }
    };

    if args.gha_output {
        write_contract_to_gha_output(&contract)?;
    }

    Ok(contract)
}

pub fn handle_gha_npm_update_pr(args: GhaNpmUpdatePrArgs) -> Result<()> {
    handle_gha_npm_update_pr_impl(args, &RealOps::new()?)
}

fn handle_gha_npm_update_pr_impl(args: GhaNpmUpdatePrArgs, ops: &dyn Ops) -> Result<()> {
    let pr_payload: PullRequestPayload =
        serde_json::from_str(&args.pr_json).context("failed to parse --pr-json payload")?;
    if pr_payload.head_branch.trim().is_empty() {
        bail!("--pr-json payload does not include non-empty `head_branch`");
    }

    ops.git_checkout(&pr_payload.head_branch)?;

    let node_wasm_version = cargo_manifest_version(ops, "node-wasm/Cargo.toml")?;
    let target_node_version = if args.node_rc_prefix.trim().is_empty() {
        node_wasm_version.clone()
    } else {
        format!(
            "{}{}",
            node_wasm_version
                .split('-')
                .next()
                .unwrap_or(node_wasm_version.as_str()),
            args.node_rc_prefix.trim()
        )
    };
    let current_node_version = package_json_version(ops, Path::new("node-wasm/js/package.json"))?;

    if current_node_version == target_node_version {
        info!(
            version = %target_node_version,
            "gha/npm-update-pr: npm package already at target version"
        );
        return Ok(());
    }

    let npm_js_dir = Path::new("node-wasm/js");
    info!(
        target_version = %target_node_version,
        "gha/npm-update-pr: running npm version without git tag"
    );
    ops.run_cmd(
        "npm",
        &["version", &target_node_version, "--no-git-tag-version"],
        Some(npm_js_dir),
    )?;
    info!("gha/npm-update-pr: running wasm-pack build for node-wasm/js");
    ops.run_cmd("wasm-pack", &["build", ".."], Some(npm_js_dir))?;
    info!("gha/npm-update-pr: installing generated wasm package in node-wasm/js");
    ops.run_cmd("npm", &["install", "--save", "../pkg"], Some(npm_js_dir))?;
    info!("gha/npm-update-pr: running npm clean-install in node-wasm/js");
    ops.run_cmd("npm", &["clean-install"], Some(npm_js_dir))?;
    info!("gha/npm-update-pr: running TypeScript build (npm run tsc)");
    ops.run_cmd("npm", &["run", "tsc"], Some(npm_js_dir))?;
    info!("gha/npm-update-pr: updating npm README output");
    ops.run_cmd("npm", &["run", "update-readme"], Some(npm_js_dir))?;

    let tracked_paths = [
        "node-wasm/js/package.json",
        "node-wasm/js/package-lock.json",
        "node-wasm/js/README.md",
        "node-wasm/js/index.d.ts",
    ];
    if !ops.git_has_changes(&tracked_paths)? {
        info!("gha/npm-update-pr: no tracked npm output changes detected");
        return Ok(());
    }

    let committed = ops.git_commit(&tracked_paths, "update lumina-node npm package")?;
    if !committed {
        info!("gha/npm-update-pr: nothing staged, skipping commit");
        return Ok(());
    }

    ops.git_push(&pr_payload.head_branch, false, false)?;
    Ok(())
}

pub fn handle_gha_npm_publish(args: GhaNpmPublishArgs) -> Result<()> {
    handle_gha_npm_publish_impl(args, &RealOps::new()?)
}

fn handle_gha_npm_publish_impl(args: GhaNpmPublishArgs, ops: &dyn Ops) -> Result<()> {
    if args.no_artifacts {
        info!("gha/npm-publish: --no-artifacts enabled, skipping npm publish");
        return Ok(());
    }

    let local_version = cargo_manifest_version(ops, "node-wasm/Cargo.toml")?;
    let npm_node_version = package_json_version(ops, Path::new("node-wasm/js/package.json"))?;
    if npm_version_exists(ops, "lumina-node-wasm", &local_version)? {
        info!(
            version = %local_version,
            "gha/npm-publish: lumina-node-wasm version already on npm, skipping"
        );
        return Ok(());
    }

    let is_rc = extract_rc_suffix(&npm_node_version).is_some();
    info!(
        version = %local_version,
        rc_channel = is_rc,
        "gha/npm-publish: running wasm-pack build"
    );
    ops.run_cmd("wasm-pack", &["build", "node-wasm"], None)?;

    if is_rc {
        info!("gha/npm-publish: publishing wasm package with rc tag");
        ops.run_cmd(
            "wasm-pack",
            &["publish", "--access", "public", "--tag", "rc", "node-wasm"],
            None,
        )?;
    } else {
        info!("gha/npm-publish: publishing wasm package with stable tag");
        ops.run_cmd(
            "wasm-pack",
            &["publish", "--access", "public", "node-wasm"],
            None,
        )?;
    }

    let dep_arg = format!("dependencies[lumina-node-wasm]={local_version}");
    info!(
        version = %local_version,
        "gha/npm-publish: setting js package dependency on lumina-node-wasm"
    );
    ops.run_cmd(
        "npm",
        &["pkg", "set", &dep_arg],
        Some(Path::new("node-wasm/js")),
    )?;
    if is_rc {
        info!("gha/npm-publish: publishing js package with rc tag");
        ops.run_cmd(
            "npm",
            &["publish", "--access", "public", "--tag", "rc"],
            Some(Path::new("node-wasm/js")),
        )?;
    } else {
        info!("gha/npm-publish: publishing js package with stable tag");
        ops.run_cmd(
            "npm",
            &["publish", "--access", "public"],
            Some(Path::new("node-wasm/js")),
        )?;
    }

    Ok(())
}

pub fn handle_gha_uniffi_release(args: GhaUniffiReleaseArgs) -> Result<()> {
    handle_gha_uniffi_release_impl(args, &RealOps::new()?)
}

fn handle_gha_uniffi_release_impl(args: GhaUniffiReleaseArgs, ops: &dyn Ops) -> Result<()> {
    if args.no_artifacts {
        info!(
            "gha/uniffi-release: --no-artifacts enabled, skipping uniffi build/release preparation"
        );
        return Ok(());
    }

    let releases: Value =
        serde_json::from_str(&args.releases_json).context("failed to parse --releases-json")?;
    let tag = find_uniffi_tag(&releases)
        .context("failed to find `lumina-node-uniffi` release tag in --releases-json payload")?;
    let files = vec![
        "lumina-node-uniffi-ios.tar.gz".to_string(),
        "lumina-node-uniffi-android.tar.gz".to_string(),
        "lumina-node-uniffi-sha256-checksums.txt".to_string(),
    ];

    ops.run_cmd("./node-uniffi/build-ios.sh", &[], None)?;
    ops.run_cmd(
        "tar",
        &[
            "-cvzf",
            "lumina-node-uniffi-ios.tar.gz",
            "-C",
            "node-uniffi",
            "ios",
        ],
        None,
    )?;
    ops.run_cmd("./node-uniffi/build-android.sh", &[], None)?;
    ops.run_cmd(
        "tar",
        &[
            "-cvzf",
            "lumina-node-uniffi-android.tar.gz",
            "-C",
            "node-uniffi",
            "app",
        ],
        None,
    )?;

    let checksums = ops.run_cmd(
        "shasum",
        &[
            "-a",
            "256",
            "lumina-node-uniffi-ios.tar.gz",
            "lumina-node-uniffi-android.tar.gz",
        ],
        None,
    )?;
    ops.write_file(
        Path::new("lumina-node-uniffi-sha256-checksums.txt"),
        &format!("{checksums}\n"),
    )?;

    if args.gha_output {
        ops.gha_output("uniffi_tag", &tag)?;
        ops.gha_output_multiline(
            "uniffi_files",
            &serde_json::to_string(&files).context("failed to serialize uniffi file list")?,
        )?;
    }

    Ok(())
}

fn release_command(event_name: &str, mode: ReleaseMode) -> Result<ReleaseDriverDecision> {
    if event_name == "workflow_dispatch" {
        return Ok(ReleaseDriverDecision::Execute(mode));
    }
    if event_name != "push" {
        warn!(
            event = %event_name,
            "gha/release-plz: unsupported event type, defaulting to execute rc"
        );
        return Ok(ReleaseDriverDecision::Execute(ReleaseMode::Rc));
    }

    // Push events: execute RC or publish (based on commit message).
    let commit_message = push_head_commit_message().unwrap_or_default();
    let commit_message_lc = commit_message.to_ascii_lowercase();
    if !commit_message_lc.contains(RELEASE_PR_TITLE_PREFIX) {
        return Ok(ReleaseDriverDecision::Execute(ReleaseMode::Rc));
    }
    if commit_message_lc.contains(RELEASE_PR_TITLE_RC) {
        return Ok(ReleaseDriverDecision::Publish(ReleaseMode::Rc));
    }
    Ok(ReleaseDriverDecision::Publish(ReleaseMode::Final))
}

fn push_head_commit_message() -> Result<String> {
    if let Ok(value) = std::env::var("GITHUB_HEAD_COMMIT_MESSAGE") {
        return Ok(value);
    }

    let event_path = std::env::var("GITHUB_EVENT_PATH")
        .context("missing GITHUB_EVENT_PATH for push event classification")?;
    let event_payload: Value = serde_json::from_str(
        &fs::read_to_string(&event_path)
            .with_context(|| format!("failed reading GitHub event payload: {event_path}"))?,
    )
    .with_context(|| format!("failed parsing GitHub event payload JSON: {event_path}"))?;

    if let Some(message) = event_payload
        .get("head_commit")
        .and_then(|head_commit| head_commit.get("message"))
        .and_then(Value::as_str)
    {
        return Ok(message.to_string());
    }

    if let Some(message) = event_payload
        .get("commits")
        .and_then(Value::as_array)
        .and_then(|commits| commits.last())
        .and_then(|commit| commit.get("message"))
        .and_then(Value::as_str)
    {
        return Ok(message.to_string());
    }

    Ok(String::new())
}

fn contract_from_execute(report: &ExecuteReport) -> ReleaseContract {
    let mut contract = ReleaseContract::default();
    if !report.submit.branch_name.trim().is_empty() {
        contract.prs_created = true;
        contract.pr = json!({
            "head_branch": report.submit.branch_name,
            "html_url": report.submit.pr_url.clone().unwrap_or_default(),
        });
    }

    let node_next_effective = report
        .versions
        .planned_versions
        .iter()
        .find(|plan| plan.package == "lumina-node")
        .map(|plan| plan.next_effective.as_str());
    if let Some(next_effective) = node_next_effective
        && let Some(rc_suffix) = extract_rc_suffix(next_effective)
    {
        contract.node_rc_prefix = rc_suffix.to_string();
    }

    contract
}

fn contract_from_publish(report: &ReleaseReport) -> ReleaseContract {
    let mut contract = ReleaseContract::default();
    let releases = report.payload.as_array().cloned().unwrap_or_else(Vec::new);
    contract.releases_created = !releases.is_empty();
    contract.releases = Value::Array(releases);
    contract
}

fn write_contract_to_gha_output(contract: &ReleaseContract) -> Result<()> {
    let pr_json = serde_json::to_string(&contract.pr).context("failed to serialize `pr` output")?;
    let releases_json = serde_json::to_string(&contract.releases)
        .context("failed to serialize `releases` output")?;
    write_github_output_multiline("pr", &pr_json)?;
    write_github_output(
        "prs_created",
        if contract.prs_created {
            "true"
        } else {
            "false"
        },
    )?;
    write_github_output_multiline("releases", &releases_json)?;
    write_github_output(
        "releases_created",
        if contract.releases_created {
            "true"
        } else {
            "false"
        },
    )?;
    write_github_output("node_rc_prefix", &contract.node_rc_prefix)?;
    Ok(())
}

fn extract_rc_suffix(version: &str) -> Option<&str> {
    let idx = version.rfind("-rc.")?;
    let suffix = &version[idx..];
    let rc_number = suffix.strip_prefix("-rc.")?;
    if !rc_number.is_empty() && rc_number.chars().all(|ch| ch.is_ascii_digit()) {
        Some(suffix)
    } else {
        None
    }
}

fn find_uniffi_tag(releases: &Value) -> Option<String> {
    releases.as_array()?.iter().find_map(|release| {
        let package_name = release.get("package_name").and_then(Value::as_str)?;
        if package_name != "lumina-node-uniffi" {
            return None;
        }
        release
            .get("tag")
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

fn package_json_version(ops: &dyn Ops, path: &Path) -> Result<String> {
    let payload = ops.read_file(path)?;
    let json: Value = serde_json::from_str(&payload)
        .with_context(|| format!("failed to parse package.json at {}", path.display()))?;
    let version = json
        .get("version")
        .and_then(Value::as_str)
        .context("package.json is missing `version` string")?;
    Ok(version.to_string())
}

fn cargo_manifest_version(ops: &dyn Ops, manifest_path: &str) -> Result<String> {
    let output = ops.run_cmd("cargo", &["pkgid", "--manifest-path", manifest_path], None)?;
    output
        .trim()
        .rsplit('@')
        .next()
        .map(str::to_string)
        .context("failed to parse version from `cargo pkgid` output")
}

fn npm_version_exists(ops: &dyn Ops, package: &str, version: &str) -> Result<bool> {
    let pkg_spec = format!("{package}@{version}");
    ops.cmd_success("npm", &["view", &pkg_spec, "version"])
}

pub(crate) fn run_checked(
    program: &str,
    args: &[&str],
    current_dir: Option<&Path>,
) -> Result<String> {
    let mut cmd = Command::new(program);
    cmd.args(args);
    if let Some(cwd) = current_dir {
        cmd.current_dir(cwd);
    }
    let output = cmd
        .output()
        .with_context(|| format!("failed to execute command `{program}`"))?;
    if !output.status.success() {
        bail!(
            "command `{}` failed with exit {}: stdout=`{}` stderr=`{}`",
            command_display(program, args),
            output.status,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(stdout)
}

fn command_display(program: &str, args: &[&str]) -> String {
    if args.is_empty() {
        return program.to_string();
    }
    format!("{program} {}", args.join(" "))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::application::gha_mock::{Call, MockOps};
    use crate::domain::types::{
        BranchState, PlannedVersion, PrepareReport, SubmitReport, VersionsReport,
    };

    // ── Pure function tests ──────────────────────────────────────────────

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

    // ── release_command tests ────────────────────────────────────────────

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn release_command_workflow_dispatch_rc() {
        let result = release_command("workflow_dispatch", ReleaseMode::Rc).unwrap();
        assert!(matches!(
            result,
            ReleaseDriverDecision::Execute(ReleaseMode::Rc)
        ));
    }

    #[test]
    fn release_command_workflow_dispatch_final() {
        let result = release_command("workflow_dispatch", ReleaseMode::Final).unwrap();
        assert!(matches!(
            result,
            ReleaseDriverDecision::Execute(ReleaseMode::Final)
        ));
    }

    #[test]
    fn release_command_unknown_event() {
        let result = release_command("pull_request", ReleaseMode::Rc).unwrap();
        assert!(matches!(
            result,
            ReleaseDriverDecision::Execute(ReleaseMode::Rc)
        ));
    }

    #[test]
    fn release_command_push_without_release_title() {
        let _lock = ENV_LOCK.lock().unwrap();
        // SAFETY: test-only env manipulation, serialized via ENV_LOCK.
        unsafe {
            std::env::remove_var("GITHUB_HEAD_COMMIT_MESSAGE");
            std::env::remove_var("GITHUB_EVENT_PATH");
        }
        let result = release_command("push", ReleaseMode::Rc).unwrap();
        assert!(matches!(
            result,
            ReleaseDriverDecision::Execute(ReleaseMode::Rc)
        ));
    }

    #[test]
    fn release_command_push_with_rc_title() {
        let _lock = ENV_LOCK.lock().unwrap();
        // SAFETY: test-only env manipulation, serialized via ENV_LOCK.
        unsafe {
            std::env::set_var("GITHUB_HEAD_COMMIT_MESSAGE", RELEASE_PR_TITLE_RC);
        }
        let result = release_command("push", ReleaseMode::Rc).unwrap();
        unsafe {
            std::env::remove_var("GITHUB_HEAD_COMMIT_MESSAGE");
        }
        assert!(matches!(
            result,
            ReleaseDriverDecision::Publish(ReleaseMode::Rc)
        ));
    }

    #[test]
    fn release_command_push_with_final_title() {
        let _lock = ENV_LOCK.lock().unwrap();
        // SAFETY: test-only env manipulation, serialized via ENV_LOCK.
        unsafe {
            std::env::set_var("GITHUB_HEAD_COMMIT_MESSAGE", "chore: test release final");
        }
        let result = release_command("push", ReleaseMode::Rc).unwrap();
        unsafe {
            std::env::remove_var("GITHUB_HEAD_COMMIT_MESSAGE");
        }
        assert!(matches!(
            result,
            ReleaseDriverDecision::Publish(ReleaseMode::Final)
        ));
    }

    // ── contract_from_execute tests ──────────────────────────────────────

    #[test]
    fn contract_from_execute_with_pr_and_planned_node_version() {
        let report = ExecuteReport {
            versions: VersionsReport {
                mode: ReleaseMode::Rc,
                previous_commit: "abc".to_string(),
                latest_release_tag: None,
                latest_non_rc_release_tag: None,
                current_commit: "def".to_string(),
                planned_versions: vec![PlannedVersion {
                    package: "lumina-node".to_string(),
                    current: "1.0.0".to_string(),
                    next_effective: "1.1.0-rc.1".to_string(),
                    publishable: true,
                }],
            },
            prepare: PrepareReport {
                mode: ReleaseMode::Rc,
                branch_name: "release/rc".to_string(),
                branch_state: BranchState::Missing,
                description: vec![],
            },
            submit: SubmitReport {
                mode: ReleaseMode::Rc,
                branch_name: "lumina/release-plz-rc".to_string(),
                commit_message: "chore(release): prepare rc release".to_string(),
                pushed: true,
                pr_url: Some("https://github.com/org/repo/pull/1".to_string()),
            },
        };
        let contract = contract_from_execute(&report);
        assert!(contract.prs_created);
        assert_eq!(contract.pr["head_branch"], "lumina/release-plz-rc");
        assert_eq!(
            contract.pr["html_url"],
            "https://github.com/org/repo/pull/1"
        );
        assert_eq!(contract.node_rc_prefix, "-rc.1");
    }

    #[test]
    fn contract_from_execute_empty_report() {
        let report = ExecuteReport {
            versions: VersionsReport {
                mode: ReleaseMode::Final,
                previous_commit: String::new(),
                latest_release_tag: None,
                latest_non_rc_release_tag: None,
                current_commit: String::new(),
                planned_versions: vec![],
            },
            prepare: PrepareReport {
                mode: ReleaseMode::Final,
                branch_name: String::new(),
                branch_state: BranchState::Missing,
                description: vec![],
            },
            submit: SubmitReport {
                mode: ReleaseMode::Final,
                branch_name: "  ".to_string(),
                commit_message: String::new(),
                pushed: false,
                pr_url: None,
            },
        };
        let contract = contract_from_execute(&report);
        assert!(!contract.prs_created);
        assert_eq!(contract.node_rc_prefix, "");
    }

    // ── contract_from_publish tests ──────────────────────────────────────

    #[test]
    fn contract_from_publish_with_releases() {
        let report = ReleaseReport {
            mode: ReleaseMode::Rc,
            published: true,
            payload: json!([
                {"package_name": "lumina-node", "tag": "lumina-node-v1.0.0-rc.1"}
            ]),
        };
        let contract = contract_from_publish(&report);
        assert!(contract.releases_created);
        assert_eq!(contract.releases.as_array().unwrap().len(), 1);
    }

    #[test]
    fn contract_from_publish_empty_releases() {
        let report = ReleaseReport {
            mode: ReleaseMode::Final,
            published: false,
            payload: json!([]),
        };
        let contract = contract_from_publish(&report);
        assert!(!contract.releases_created);
        assert_eq!(contract.releases.as_array().unwrap().len(), 0);
    }

    // ── npm-update-pr handler tests ──────────────────────────────────────

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
            .with_git_commit(true)
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
            "[git] commit",
            "[git] push",
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
            ops.find_call("[git] push"),
            Call::GitPush {
                branch: "feature/branch".to_string(),
                force: false,
                dry_run: false,
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
            .with_git_has_changes(true)
            .with_git_commit(true);

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
            "[git] commit",
            "[git] push",
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
    fn npm_update_pr_commit_returns_false() {
        let ops = MockOps::new()
            .on_cmd("cargo pkgid", "pkg@1.2.3")
            .with_file("node-wasm/js/package.json", r#"{"version":"1.0.0"}"#)
            .on_cmd("npm version", "")
            .on_cmd("wasm-pack build", "")
            .on_cmd("npm install", "")
            .on_cmd("npm clean-install", "")
            .on_cmd("npm run tsc", "")
            .on_cmd("npm run update-readme", "")
            .with_git_has_changes(true)
            .with_git_commit(false);

        let args = npm_update_pr_args(r#"{"head_branch":"feature/branch"}"#, "");
        handle_gha_npm_update_pr_impl(args, &ops).unwrap();

        // Commit returns false → no push
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
            "[git] commit",
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

    // ── npm-publish handler tests ────────────────────────────────────────

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
            .on_cmd_success("npm view", true);

        let args = GhaNpmPublishArgs {
            no_artifacts: false,
        };
        handle_gha_npm_publish_impl(args, &ops).unwrap();

        // Early return after npm view confirms version exists
        ops.assert_sequence(&["cargo pkgid", "[read_file]", "[check] npm view"]);
    }

    #[test]
    fn npm_publish_stable_flow() {
        let ops = MockOps::new()
            .on_cmd("cargo pkgid", "pkg@1.2.3")
            .with_file("node-wasm/js/package.json", r#"{"version":"1.2.3"}"#)
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

    // ── uniffi-release handler tests ─────────────────────────────────────

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
}
