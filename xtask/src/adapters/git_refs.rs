use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use cargo_metadata::semver::Version;
use git2::{
    BranchType, DiffOptions, ObjectType, Oid, Repository, Sort, WorktreeAddOptions,
    WorktreePruneOptions,
};
use tempfile::TempDir;

#[derive(Debug)]
pub struct RepoSnapshot {
    pub temp_dir: TempDir,
    pub repo_root: PathBuf,
    source_repo_root: PathBuf,
    worktree_name: String,
    snapshot_branch: String,
}

impl Drop for RepoSnapshot {
    fn drop(&mut self) {
        let Ok(repo) = Repository::open(&self.source_repo_root) else {
            return;
        };

        if let Ok(worktree) = repo.find_worktree(&self.worktree_name) {
            let mut opts = WorktreePruneOptions::new();
            opts.valid(true).locked(true).working_tree(true);
            let _ = worktree.prune(Some(&mut opts));
        }

        if let Ok(mut branch) = repo.find_branch(&self.snapshot_branch, BranchType::Local) {
            let _ = branch.delete();
        }
    }
}

pub fn resolve_comparison_commit(
    workspace_root: &Path,
    default_branch: &str,
    base_commit: Option<&str>,
) -> Result<String> {
    let repo = Repository::open(workspace_root)
        .with_context(|| format!("failed to open repository at {}", workspace_root.display()))?;

    if let Some(commit) = base_commit {
        let object = repo
            .revparse_single(commit)
            .with_context(|| format!("failed to resolve base commit `{commit}`"))?;
        let commit_oid = object
            .peel_to_commit()
            .context("failed to peel base commit to a commit object")?
            .id();
        return Ok(commit_oid.to_string());
    }

    if let Some(oid) = resolve_branch_tip_oid(&repo, default_branch) {
        return Ok(oid.to_string());
    }

    bail!("failed to resolve comparison commit for default branch `{default_branch}`")
}

pub fn snapshot_workspace_to_temp(
    workspace_root: &Path,
    commit: &str,
    default_branch: &str,
) -> Result<RepoSnapshot> {
    let repo = Repository::open(workspace_root)
        .with_context(|| format!("failed to open repository at {}", workspace_root.display()))?;
    let object = repo
        .revparse_single(commit)
        .with_context(|| format!("failed to resolve commit `{commit}`"))?;
    let target_commit = object
        .peel_to_commit()
        .with_context(|| format!("failed to peel `{commit}` to commit"))?;

    let snapshot_id = unique_snapshot_id();
    let worktree_name = format!("xtask-snapshot-{snapshot_id}");
    let snapshot_branch = worktree_name.clone();

    repo.branch(&snapshot_branch, &target_commit, true)
        .with_context(|| format!("failed to create snapshot branch `{snapshot_branch}`"))?;

    let upstream = format!("origin/{default_branch}");
    if repo.find_branch(&upstream, BranchType::Remote).is_ok()
        && let Ok(mut local) = repo.find_branch(&snapshot_branch, BranchType::Local)
    {
        let _ = local.set_upstream(Some(&upstream));
    }

    let temp_dir = tempfile::tempdir().context("failed to create temporary directory")?;
    let repo_root = temp_dir.path().join("worktree");
    let mut opts = WorktreeAddOptions::new();
    opts.checkout_existing(true);

    repo.worktree(&worktree_name, &repo_root, Some(&opts))
        .with_context(|| {
            format!(
                "failed to create snapshot worktree `{}` in `{}` from `{}`",
                worktree_name,
                repo_root.display(),
                workspace_root.display()
            )
        })?;

    Ok(RepoSnapshot {
        temp_dir,
        repo_root,
        source_repo_root: workspace_root.to_path_buf(),
        worktree_name,
        snapshot_branch,
    })
}

pub fn remote_origin_url(workspace_root: &Path) -> Result<Option<String>> {
    let repo = Repository::open(workspace_root)
        .with_context(|| format!("failed to open repository at {}", workspace_root.display()))?;

    let remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => return Ok(None),
    };

    Ok(remote.url().map(|url| url.to_string()))
}

pub fn latest_release_tag_on_branch(
    workspace_root: &Path,
    default_branch: &str,
) -> Result<Option<String>> {
    latest_release_tag_with_filter(workspace_root, default_branch, ReleaseTagFilter::AnyRelease)
}

pub fn latest_non_rc_release_tag_on_branch(
    workspace_root: &Path,
    default_branch: &str,
) -> Result<Option<String>> {
    latest_release_tag_with_filter(workspace_root, default_branch, ReleaseTagFilter::NonRcOnly)
}

pub fn changed_files_since_tag(repo_root: &Path, tag: &str) -> Result<Vec<String>> {
    let repo = Repository::open(repo_root)
        .with_context(|| format!("failed to open repository at {}", repo_root.display()))?;

    let baseline_commit = resolve_tag_to_commit(&repo, tag)
        .with_context(|| format!("failed to resolve baseline tag `{tag}`"))?;
    let head_commit = repo
        .head()
        .context("failed to resolve HEAD reference")?
        .peel_to_commit()
        .context("failed to peel HEAD to commit")?;

    let baseline_tree = baseline_commit
        .tree()
        .context("failed to resolve baseline tree")?;
    let head_tree = head_commit.tree().context("failed to resolve HEAD tree")?;

    let mut diff_opts = DiffOptions::new();
    let diff = repo
        .diff_tree_to_tree(Some(&baseline_tree), Some(&head_tree), Some(&mut diff_opts))
        .context("failed to compute diff between baseline and HEAD")?;

    let mut files = BTreeSet::new();
    diff.foreach(
        &mut |delta, _| {
            if let Some(path) = delta.new_file().path().or(delta.old_file().path()) {
                files.insert(path.to_string_lossy().to_string());
            }
            true
        },
        None,
        None,
        None,
    )
    .context("failed to iterate changed files in diff")?;

    Ok(files.into_iter().collect())
}

pub fn commit_for_tag(repo_root: &Path, tag: &str) -> Result<String> {
    let repo = Repository::open(repo_root)
        .with_context(|| format!("failed to open repository at {}", repo_root.display()))?;
    let commit = resolve_tag_to_commit(&repo, tag)
        .with_context(|| format!("failed to resolve commit for tag `{tag}`"))?;
    Ok(commit.id().to_string())
}

#[derive(Debug, Clone, Copy)]
enum ReleaseTagFilter {
    AnyRelease,
    NonRcOnly,
}

fn latest_release_tag_with_filter(
    workspace_root: &Path,
    default_branch: &str,
    filter: ReleaseTagFilter,
) -> Result<Option<String>> {
    let repo = Repository::open(workspace_root)
        .with_context(|| format!("failed to open repository at {}", workspace_root.display()))?;

    let commit_tags = tags_by_commit(&repo)?;
    let start_oid = resolve_branch_tip_oid(&repo, default_branch).or_else(|| {
        repo.head()
            .ok()
            .and_then(|head| head.peel_to_commit().ok())
            .map(|commit| commit.id())
    });

    let Some(start_oid) = start_oid else {
        return Ok(None);
    };

    let mut revwalk = repo.revwalk().context("failed to initialize revwalk")?;
    revwalk
        .set_sorting(Sort::TOPOLOGICAL | Sort::TIME)
        .context("failed to configure revwalk sorting")?;
    revwalk
        .push(start_oid)
        .context("failed to seed revwalk with branch tip")?;

    for oid in revwalk {
        let oid = oid.context("failed while iterating commits on branch")?;
        let commit = repo
            .find_commit(oid)
            .context("failed to load commit while scanning release history")?;
        let message = commit.message().unwrap_or_default().to_ascii_lowercase();
        if !message.contains("release") {
            continue;
        }

        let Some(tags) = commit_tags.get(&oid) else {
            continue;
        };
        if tags.is_empty() {
            continue;
        }

        match filter {
            ReleaseTagFilter::AnyRelease => {
                if let Some(tag) = pick_best_semver_tag(tags, false) {
                    return Ok(Some(tag));
                }
            }
            ReleaseTagFilter::NonRcOnly => {
                if tags
                    .iter()
                    .any(|tag| tag.to_ascii_lowercase().contains("rc"))
                {
                    continue;
                }

                if let Some(tag) = pick_best_semver_tag(tags, true) {
                    return Ok(Some(tag));
                }
            }
        }
    }

    Ok(None)
}

fn tags_by_commit(repo: &Repository) -> Result<HashMap<Oid, Vec<String>>> {
    let mut tags = HashMap::<Oid, Vec<String>>::new();
    let references = repo
        .references_glob("refs/tags/*")
        .context("failed to enumerate tag references")?;

    for reference_result in references {
        let reference = reference_result.context("failed to read tag reference")?;
        let Some(name) = reference.shorthand().map(ToString::to_string) else {
            continue;
        };

        let tag_commit = match reference.peel_to_commit() {
            Ok(commit) => commit,
            Err(_) => continue,
        };

        tags.entry(tag_commit.id()).or_default().push(name);
    }

    Ok(tags)
}

fn resolve_branch_tip_oid(repo: &Repository, default_branch: &str) -> Option<Oid> {
    [
        format!("refs/remotes/origin/{default_branch}"),
        format!("refs/heads/{default_branch}"),
    ]
    .into_iter()
    .find_map(|reference| repo.refname_to_id(&reference).ok())
}

fn resolve_tag_to_commit<'repo>(repo: &'repo Repository, tag: &str) -> Result<git2::Commit<'repo>> {
    let object = match repo.find_reference(&format!("refs/tags/{tag}")) {
        Ok(reference) => reference
            .peel(ObjectType::Any)
            .with_context(|| format!("failed to peel tag reference `{tag}`"))?,
        Err(_) => repo
            .revparse_single(tag)
            .with_context(|| format!("failed to rev-parse tag `{tag}`"))?,
    };

    object
        .peel_to_commit()
        .with_context(|| format!("failed to resolve commit for tag `{tag}`"))
}

fn pick_best_semver_tag(tags: &[String], require_non_rc: bool) -> Option<String> {
    let mut parsed = tags
        .iter()
        .filter_map(|tag| extract_semver_from_tag(tag).map(|version| (tag, version)))
        .filter(|(_, version)| !require_non_rc || version.pre.is_empty())
        .collect::<Vec<_>>();

    parsed.sort_by(|(_, a), (_, b)| a.cmp(b));
    parsed.last().map(|(tag, _)| (*tag).clone())
}

fn extract_semver_from_tag(tag: &str) -> Option<Version> {
    let idx = tag.rfind('v')?;
    Version::parse(&tag[idx + 1..]).ok()
}

fn unique_snapshot_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{nanos}", std::process::id())
}
