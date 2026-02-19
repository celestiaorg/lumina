use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::adapters::git2_repo::Git2Repo;
use crate::adapters::github_output::{write_github_output, write_github_output_multiline};
use crate::application::pipeline::{ExecuteArgs, ReleasePipeline};
use crate::domain::model::{RELEASE_PR_TITLE_PREFIX, RELEASE_PR_TITLE_RC};
use crate::domain::types::{
    AuthContext, BranchContext, CommonContext, ExecuteContext, ExecuteReport, PublishContext,
    ReleaseMode, ReleaseReport,
};

#[derive(Debug, Clone)]
pub struct GhaReleasePlzArgs {
    pub compare_branch: Option<String>,
    pub default_branch: String,
    pub rc_branch_prefix: String,
    pub final_branch_prefix: String,
    pub gha_output: bool,
    pub json_out: Option<PathBuf>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct GhaNpmUpdatePrArgs {
    pub pr_json: String,
    pub node_rc_prefix: String,
}

#[derive(Debug, Clone)]
pub struct GhaNpmPublishArgs {
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct GhaUniffiReleaseArgs {
    pub releases_json: String,
    pub gha_output: bool,
    pub dry_run: bool,
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
    let classified = classify_release_decision()?;
    let decision = if args.dry_run {
        match classified {
            ReleaseDriverDecision::Publish(mode) => ReleaseDriverDecision::Execute(mode),
            other => other,
        }
    } else {
        classified
    };
    info!(decision=?decision, dry_run=args.dry_run, "gha/release-plz: selected flow");
    let compare_branch = args
        .compare_branch
        .clone()
        .unwrap_or_else(|| args.default_branch.clone());

    let contract = match decision {
        ReleaseDriverDecision::Execute(mode) => {
            let report = pipeline
                .execute(ExecuteArgs {
                    ctx: ExecuteContext {
                        common: CommonContext {
                            mode,
                            default_branch: compare_branch.clone(),
                            auth: AuthContext::from_env(),
                        },
                        branch: BranchContext { skip_pr: false },
                    },
                    dry_run: false,
                })
                .await?;
            contract_from_execute(&report)
        }
        ReleaseDriverDecision::Publish(mode) => {
            let report = pipeline
                .publish(PublishContext {
                    common: CommonContext {
                        mode,
                        default_branch: compare_branch.clone(),
                        auth: AuthContext::from_env(),
                    },
                    rc_branch_prefix: args.rc_branch_prefix.clone(),
                    final_branch_prefix: args.final_branch_prefix.clone(),
                })
                .await?;
            contract_from_publish(&report)
        }
    };

    if args.gha_output {
        write_contract_to_gha_output(&contract)?;
    }

    if let Some(path) = args.json_out {
        write_json_file(&path, &contract)?;
    }

    Ok(contract)
}

pub fn handle_gha_npm_update_pr(args: GhaNpmUpdatePrArgs) -> Result<()> {
    let pr_payload: PullRequestPayload =
        serde_json::from_str(&args.pr_json).context("failed to parse --pr-json payload")?;
    if pr_payload.head_branch.trim().is_empty() {
        bail!("--pr-json payload does not include non-empty `head_branch`");
    }

    let git =
        Git2Repo::new(std::env::current_dir().context("failed to resolve current workspace root")?);
    git.checkout_branch_from_origin(&pr_payload.head_branch)?;

    let node_wasm_version = cargo_manifest_version("node-wasm/Cargo.toml")?;
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
    let current_node_version = package_json_version(Path::new("node-wasm/js/package.json"))?;

    if current_node_version == target_node_version {
        info!(
            version = %target_node_version,
            "gha/npm-update-pr: npm package already at target version"
        );
        return Ok(());
    }

    let npm_js_dir = Path::new("node-wasm/js");
    run_checked(
        "npm",
        &["version", &target_node_version, "--no-git-tag-version"],
        Some(npm_js_dir),
    )?;
    run_checked("wasm-pack", &["build", ".."], Some(npm_js_dir))?;
    run_checked("npm", &["install", "--save", "../pkg"], Some(npm_js_dir))?;
    run_checked("npm", &["clean-install"], Some(npm_js_dir))?;
    run_checked("npm", &["run", "tsc"], Some(npm_js_dir))?;
    run_checked("npm", &["run", "update-readme"], Some(npm_js_dir))?;

    let tracked_paths = [
        "node-wasm/js/package.json",
        "node-wasm/js/package-lock.json",
        "node-wasm/js/README.md",
        "node-wasm/js/index.d.ts",
    ];
    if !git.paths_have_changes(&tracked_paths)? {
        info!("gha/npm-update-pr: no tracked npm output changes detected");
        return Ok(());
    }

    let committed = git.stage_paths_and_commit(&tracked_paths, "update lumina-node npm package")?;
    if !committed {
        info!("gha/npm-update-pr: nothing staged, skipping commit");
        return Ok(());
    }

    git.push_branch(&pr_payload.head_branch, false, false)?;
    Ok(())
}

pub fn handle_gha_npm_publish(args: GhaNpmPublishArgs) -> Result<()> {
    if args.dry_run {
        info!("gha/npm-publish: dry-run enabled, skipping npm publish");
        return Ok(());
    }

    let local_version = cargo_manifest_version("node-wasm/Cargo.toml")?;
    let npm_node_version = package_json_version(Path::new("node-wasm/js/package.json"))?;
    if npm_version_exists("lumina-node-wasm", &local_version)? {
        info!(
            version = %local_version,
            "gha/npm-publish: lumina-node-wasm version already on npm, skipping"
        );
        return Ok(());
    }

    let is_rc = extract_rc_suffix(&npm_node_version).is_some();
    run_checked("wasm-pack", &["build", "node-wasm"], None)?;

    if is_rc {
        run_checked(
            "wasm-pack",
            &["publish", "--access", "public", "--tag", "rc", "node-wasm"],
            None,
        )?;
    } else {
        run_checked(
            "wasm-pack",
            &["publish", "--access", "public", "node-wasm"],
            None,
        )?;
    }

    run_checked(
        "npm",
        &[
            "pkg",
            "set",
            &format!("dependencies[lumina-node-wasm]={local_version}"),
        ],
        Some(Path::new("node-wasm/js")),
    )?;
    if is_rc {
        run_checked(
            "npm",
            &["publish", "--access", "public", "--tag", "rc"],
            Some(Path::new("node-wasm/js")),
        )?;
    } else {
        run_checked(
            "npm",
            &["publish", "--access", "public"],
            Some(Path::new("node-wasm/js")),
        )?;
    }

    Ok(())
}

pub fn handle_gha_uniffi_release(args: GhaUniffiReleaseArgs) -> Result<()> {
    if args.dry_run {
        info!("gha/uniffi-release: dry-run enabled, skipping uniffi build/release preparation");
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

    run_checked("./node-uniffi/build-ios.sh", &[], None)?;
    run_checked(
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
    run_checked("./node-uniffi/build-android.sh", &[], None)?;
    run_checked(
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

    let checksums = run_checked(
        "shasum",
        &[
            "-a",
            "256",
            "lumina-node-uniffi-ios.tar.gz",
            "lumina-node-uniffi-android.tar.gz",
        ],
        None,
    )?;
    fs::write(
        "lumina-node-uniffi-sha256-checksums.txt",
        format!("{checksums}\n"),
    )
    .context("failed to write uniffi checksum file")?;

    if args.gha_output {
        write_github_output("uniffi_tag", &tag)?;
        write_github_output_multiline(
            "uniffi_files",
            &serde_json::to_string(&files).context("failed to serialize uniffi file list")?,
        )?;
    }

    Ok(())
}

fn classify_release_decision() -> Result<ReleaseDriverDecision> {
    let event_name = std::env::var("GITHUB_EVENT_NAME").unwrap_or_else(|_| "push".to_string());
    if event_name == "workflow_dispatch" {
        return Ok(ReleaseDriverDecision::Execute(ReleaseMode::Final));
    }
    if event_name != "push" {
        warn!(
            event = %event_name,
            "gha/release-plz: unsupported event type, defaulting to execute rc"
        );
        return Ok(ReleaseDriverDecision::Execute(ReleaseMode::Rc));
    }

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

fn write_json_file(path: &Path, payload: &ReleaseContract) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create output directory for release contract: {}",
                parent.display()
            )
        })?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(payload).context("failed to serialize release contract")?,
    )
    .with_context(|| format!("failed to write release contract JSON: {}", path.display()))?;
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

fn package_json_version(path: &Path) -> Result<String> {
    let payload = fs::read_to_string(path)
        .with_context(|| format!("failed to read package.json at {}", path.display()))?;
    let json: Value = serde_json::from_str(&payload)
        .with_context(|| format!("failed to parse package.json at {}", path.display()))?;
    let version = json
        .get("version")
        .and_then(Value::as_str)
        .context("package.json is missing `version` string")?;
    Ok(version.to_string())
}

fn cargo_manifest_version(manifest_path: &str) -> Result<String> {
    let output = run_checked("cargo", &["pkgid", "--manifest-path", manifest_path], None)?;
    output
        .trim()
        .rsplit('@')
        .next()
        .map(str::to_string)
        .context("failed to parse version from `cargo pkgid` output")
}

fn npm_version_exists(package: &str, version: &str) -> Result<bool> {
    let status = Command::new("npm")
        .args(["view", &format!("{package}@{version}"), "version"])
        .status()
        .context("failed to execute `npm view`")?;
    Ok(status.success())
}

fn run_checked(program: &str, args: &[&str], current_dir: Option<&Path>) -> Result<String> {
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
    use super::extract_rc_suffix;

    #[test]
    fn rc_suffix_is_extracted_from_semver() {
        assert_eq!(extract_rc_suffix("1.2.3-rc.7"), Some("-rc.7"));
    }

    #[test]
    fn missing_suffix_returns_none() {
        assert_eq!(extract_rc_suffix("1.2.3"), None);
    }
}
