use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::info;

use crate::adapters::github::{
    Git2Repo, parse_remote_repo_url, write_github_output, write_github_output_multiline,
};
use crate::adapters::release_plz::ReleasePlzAdapter;
use crate::domain::types::{AuthContext, ExecuteReport, PublishContext, ReleaseMode};

// ── Ops trait + RealOps ─────────────────────────────────────────────────

pub(crate) trait Ops {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String>;
    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool>;
    fn read_file(&self, path: &Path) -> Result<String>;
    fn write_file(&self, path: &Path, content: &str) -> Result<()>;
    fn git_checkout(&self, branch: &str) -> Result<()>;
    fn git_has_changes(&self, paths: &[&str]) -> Result<bool>;
    fn commit_and_push(&self, branch: &str, message: &str, paths: &[&str]) -> Result<()>;
    fn gha_output(&self, key: &str, value: &str) -> Result<()>;
    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()>;
}

pub(crate) struct RealOps {
    git: Git2Repo,
    root: std::path::PathBuf,
}

impl RealOps {
    pub(crate) fn new() -> Result<Self> {
        let root = std::env::current_dir().context("failed to resolve current workspace root")?;
        Ok(Self {
            git: Git2Repo::new(root.clone()),
            root,
        })
    }
}

impl Ops for RealOps {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String> {
        run_checked(program, args, cwd)
    }

    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool> {
        let status = Command::new(program)
            .args(args)
            .status()
            .with_context(|| format!("failed to execute `{program}`"))?;
        Ok(status.success())
    }

    fn read_file(&self, path: &Path) -> Result<String> {
        fs::read_to_string(path)
            .with_context(|| format!("failed to read file at {}", path.display()))
    }

    fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        fs::write(path, content)
            .with_context(|| format!("failed to write file at {}", path.display()))
    }

    fn git_checkout(&self, branch: &str) -> Result<()> {
        self.git.checkout_branch_from_origin(branch)
    }

    fn git_has_changes(&self, paths: &[&str]) -> Result<bool> {
        self.git.paths_have_changes(paths)
    }

    fn commit_and_push(&self, branch: &str, message: &str, paths: &[&str]) -> Result<()> {
        let repo_url = parse_remote_repo_url(&self.root)?
            .context("no origin remote URL found; cannot create GraphQL commit")?;

        let token =
            std::env::var("GITHUB_TOKEN").context("GITHUB_TOKEN is required for GraphQL commit")?;

        let git_client = release_plz_core::GitClient::new(release_plz_core::GitForge::Github(
            release_plz_core::GitHub::new(repo_url.owner, repo_url.name, token.into()),
        ))
        .context("failed to create GitClient for GraphQL commit")?;

        let root_str = self
            .root
            .to_str()
            .context("workspace root is not valid UTF-8")?;
        let git_repo = release_plz_core::git_cmd::Repo::new(root_str)
            .context("failed to open git_cmd::Repo for GraphQL commit")?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                release_plz_core::commit_specific_files(
                    &git_client,
                    &git_repo,
                    message,
                    branch,
                    paths,
                )
                .await
                .context("failed to create commit via GitHub GraphQL API")?;
                Ok(())
            })
        })
    }

    fn gha_output(&self, key: &str, value: &str) -> Result<()> {
        write_github_output(key, value)
    }

    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()> {
        write_github_output_multiline(key, value)
    }
}

// ── CLI ─────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Release process orchestrator")]
struct Cli {
    #[arg(long, default_value = ".")]
    workspace_root: std::path::PathBuf,

    #[command(subcommand)]
    command: CliCommands,
}

#[derive(Debug, Subcommand)]
enum CliCommands {
    Gha(GhaArgs),
}

#[derive(Debug, Clone, Args)]
struct GhaArgs {
    #[command(subcommand)]
    command: GhaCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum GhaCommands {
    /// Create/update release PR and publish crates+GitHub releases.
    Release(GhaReleaseArgs),
    NpmUpdatePr(GhaNpmUpdatePrArgs),
    NpmPublish(GhaNpmPublishArgs),
    UniffiRelease(GhaUniffiReleaseArgs),
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let adapter = ReleasePlzAdapter::new(cli.workspace_root.clone());

    match cli.command {
        CliCommands::Gha(args) => match args.command {
            GhaCommands::Release(cmd) => {
                info!(
                    gha_output = cmd.gha_output,
                    no_artifacts = cmd.no_artifacts,
                    "running gha release"
                );
                let contract = handle_gha_release(&adapter, cmd).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&contract)
                        .unwrap_or_else(|_| format!("{contract:?}"))
                );
            }
            GhaCommands::NpmUpdatePr(cmd) => {
                info!("running gha npm-update-pr");
                handle_gha_npm_update_pr(cmd)?;
            }
            GhaCommands::NpmPublish(cmd) => {
                info!("running gha npm-publish");
                handle_gha_npm_publish(cmd)?;
            }
            GhaCommands::UniffiRelease(cmd) => {
                info!("running gha uniffi-release");
                handle_gha_uniffi_release(cmd)?;
            }
        },
    }

    Ok(())
}

// ── Args ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Args)]
pub struct GhaReleaseArgs {
    #[arg(long, default_value = "rc", help = "Release mode (rc or final)")]
    pub mode: ReleaseMode,

    #[arg(long, help = "Write normalized contract fields to GITHUB_OUTPUT")]
    pub gha_output: bool,

    #[arg(long, help = "Skip publishing artifacts (crates, GitHub releases)")]
    pub no_artifacts: bool,
}

#[derive(Debug, Clone, Args)]
pub struct GhaNpmUpdatePrArgs {
    #[arg(long, help = "Release PR payload JSON from previous job output")]
    pub pr_json: String,

    #[arg(
        long,
        default_value = "",
        allow_hyphen_values = true,
        help = "Optional `-rc.N` suffix override"
    )]
    pub node_rc_prefix: String,
}

#[derive(Debug, Clone, Args)]
pub struct GhaNpmPublishArgs {
    #[arg(long, help = "Skip publishing npm artifacts")]
    pub no_artifacts: bool,
}

#[derive(Debug, Clone, Args)]
pub struct GhaUniffiReleaseArgs {
    #[arg(long, help = "Release payload JSON from previous job output")]
    pub releases_json: String,

    #[arg(long, help = "Write uniffi tag/files outputs to GITHUB_OUTPUT")]
    pub gha_output: bool,

    #[arg(long, help = "Skip uniffi build and release upload preparation")]
    pub no_artifacts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseContract {
    pub pr: Value,
    pub prs_created: bool,
    pub node_rc_prefix: String,
    pub releases: Value,
    pub releases_created: bool,
}

impl Default for ReleaseContract {
    fn default() -> Self {
        Self {
            pr: json!({}),
            prs_created: false,
            node_rc_prefix: String::new(),
            releases: json!([]),
            releases_created: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PullRequestPayload {
    head_branch: String,
}

pub async fn handle_gha_release(
    adapter: &ReleasePlzAdapter,
    args: GhaReleaseArgs,
) -> Result<ReleaseContract> {
    let auth = AuthContext::from_env();

    // 1. Create/update release PR
    let pr_report = adapter.release_pr(args.mode, &auth).await?;
    if let Some(ref r) = pr_report {
        info!(
            branch=%r.head_branch,
            pr_url=?r.pr_url,
            packages=r.updated_packages.len(),
            "release_pr completed"
        );
    } else {
        info!("release_pr completed: no updates needed");
    }

    // 2. Publish crates and GitHub releases
    let publish_ctx = PublishContext {
        auth,
        no_artifacts: args.no_artifacts,
    };
    info!("publish: invoking release adapter");
    let publish_payload = adapter.publish(&publish_ctx).await?;
    let releases_created = is_published(&publish_payload);
    info!(releases_created, "publish completed");

    let contract = build_release_contract(pr_report.as_ref(), &publish_payload);

    if args.gha_output {
        write_release_contract_to_gha_output(&contract)?;
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

    let node_wasm_version = cargo_manifest_version(ops, Path::new("node-wasm/Cargo.toml"))?;
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

    ops.commit_and_push(
        &pr_payload.head_branch,
        "update lumina-node npm package",
        &tracked_paths,
    )?;
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

    let local_version = cargo_manifest_version(ops, Path::new("node-wasm/Cargo.toml"))?;
    let npm_node_version = package_json_version(ops, Path::new("node-wasm/js/package.json"))?;
    let wasm_already_published = npm_version_exists(ops, "lumina-node-wasm", &local_version)?;
    let js_already_published = npm_version_exists(ops, "lumina-node", &npm_node_version)?;
    if wasm_already_published && js_already_published {
        info!(
            wasm_version = %local_version,
            js_version = %npm_node_version,
            "gha/npm-publish: wasm and js packages already on npm, skipping"
        );
        return Ok(());
    }

    // Determine RC channel from package.json version (which reflects the npm-update-pr step).
    // The Cargo.toml version (local_version) is used for the npm existence check above, but
    // package.json is the authoritative source for npm publishing since npm-update-pr may
    // apply an RC suffix independently of the Cargo version.
    let is_rc = extract_rc_suffix(&npm_node_version).is_some();
    if !wasm_already_published {
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
    } else {
        info!(
            version = %local_version,
            "gha/npm-publish: lumina-node-wasm version already on npm, skipping wasm publish"
        );
    }

    if !js_already_published {
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
    } else {
        info!(
            version = %npm_node_version,
            "gha/npm-publish: lumina-node version already on npm, skipping js publish"
        );
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

/// Returns `true` when the publish payload represents actual releases.
/// release-plz returns `null` or `[]` when nothing was published.
pub(crate) fn is_published(payload: &Value) -> bool {
    !payload.is_null() && !payload.as_array().is_some_and(|arr| arr.is_empty())
}

pub(crate) fn build_release_contract(
    pr_report: Option<&ExecuteReport>,
    publish_payload: &Value,
) -> ReleaseContract {
    let mut contract = ReleaseContract::default();

    // PR fields
    if let Some(report) = pr_report {
        if !report.head_branch.trim().is_empty() {
            contract.pr = json!({
                "head_branch": report.head_branch,
                "html_url": report.pr_url.clone().unwrap_or_default(),
            });
            contract.prs_created = report.pr_url.is_some();
        }

        // Extract RC suffix from lumina-node's updated version if present.
        let node_version = report
            .updated_packages
            .iter()
            .find(|pkg| pkg.package == "lumina-node")
            .map(|pkg| pkg.version.as_str());
        if let Some(version) = node_version
            && let Some(rc_suffix) = extract_rc_suffix(version)
        {
            contract.node_rc_prefix = rc_suffix.to_string();
        }
    }

    // Release fields
    let releases = publish_payload.as_array().cloned().unwrap_or_default();
    contract.releases_created = !releases.is_empty();
    contract.releases = Value::Array(releases);

    contract
}

fn write_release_contract_to_gha_output(contract: &ReleaseContract) -> Result<()> {
    let pr_json = serde_json::to_string(&contract.pr).context("failed to serialize `pr` output")?;
    write_github_output_multiline("pr", &pr_json)?;
    write_github_output(
        "prs_created",
        if contract.prs_created {
            "true"
        } else {
            "false"
        },
    )?;
    write_github_output("node_rc_prefix", &contract.node_rc_prefix)?;

    let releases_json = serde_json::to_string(&contract.releases)
        .context("failed to serialize `releases` output")?;
    write_github_output_multiline("releases", &releases_json)?;
    write_github_output(
        "releases_created",
        if contract.releases_created {
            "true"
        } else {
            "false"
        },
    )?;
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

fn cargo_manifest_version(ops: &dyn Ops, manifest_path: &Path) -> Result<String> {
    let manifest_str = manifest_path
        .to_str()
        .context("manifest path is not valid UTF-8")?;
    let output = ops.run_cmd("cargo", &["pkgid", "--manifest-path", manifest_str], None)?;
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
#[path = "tests.rs"]
mod tests;
