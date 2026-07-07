//! Workspace discovery via `cargo metadata`: the single workspace version, the
//! member crates, and the topological publish order.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use cargo_metadata::{DependencyKind, MetadataCommand};

/// Re-export of `cargo_metadata`'s `semver` so dependents can name [`Version`].
pub use cargo_metadata::semver;

use semver::Version;

/// Facts about a single workspace member crate.
#[derive(Debug, Clone)]
pub struct CrateInfo {
    /// Package name, e.g. `lumina-utils`.
    pub name: String,
    /// The crate's version (equals the workspace version).
    pub version: Version,
    /// Absolute directory containing the crate's `Cargo.toml`.
    pub manifest_dir: PathBuf,
    /// Absolute path to the crate's `Cargo.toml`.
    pub manifest_path: PathBuf,
    /// `false` iff the manifest sets `publish = false`.
    pub is_publishable: bool,
}

/// The discovered workspace: its single version and every member crate.
#[derive(Debug, Clone)]
pub struct Workspace {
    /// The single workspace version, shared by every member.
    pub version: Version,
    /// Every workspace member, in `cargo metadata` order.
    pub crates: Vec<CrateInfo>,
    /// In-workspace dependency edges `(a, b)` meaning "`a` depends on `b`"
    /// (normal + build kinds only; dev-deps excluded).
    edges: Vec<(String, String)>,
}

/// A dependency cycle in the in-workspace dependency graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cycle {
    /// Names of the crates involved in the cycle (sorted, deterministic).
    pub crates: Vec<String>,
}

impl std::fmt::Display for Cycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dependency cycle among workspace crates: {}",
            self.crates.join(", ")
        )
    }
}

impl std::error::Error for Cycle {}

impl Workspace {
    /// Discover the workspace rooted at `repo_root` via `cargo metadata`.
    ///
    /// # Errors
    /// Errors if `cargo metadata` fails, the workspace has no members, or members
    /// disagree on their version (violates the single-version invariant).
    pub fn discover(repo_root: &Path) -> Result<Workspace> {
        let metadata = MetadataCommand::new()
            .manifest_path(repo_root.join("Cargo.toml"))
            .no_deps()
            .exec()
            .with_context(|| format!("running `cargo metadata` for workspace at {repo_root:?}"))?;

        // Restrict to actual workspace members (not transitive registry packages).
        let member_ids: BTreeSet<_> = metadata.workspace_members.iter().cloned().collect();

        // Names of all members, to filter dependency edges to in-workspace ones.
        let member_names: BTreeSet<&str> = metadata
            .packages
            .iter()
            .filter(|p| member_ids.contains(&p.id))
            .map(|p| p.name.as_str())
            .collect();

        let mut crates = Vec::new();
        let mut edges: Vec<(String, String)> = Vec::new();
        for pkg in &metadata.packages {
            if !member_ids.contains(&pkg.id) {
                continue;
            }
            let manifest_path: PathBuf = pkg.manifest_path.clone().into_std_path_buf();
            let manifest_dir = manifest_path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| manifest_path.clone());

            // Edge `pkg -> dep` for each normal/build in-workspace dependency
            // (dev-dependencies ignored).
            for dep in &pkg.dependencies {
                let is_in_workspace = member_names.contains(dep.name.as_str());
                let kind_counts =
                    matches!(dep.kind, DependencyKind::Normal | DependencyKind::Build);
                if is_in_workspace && kind_counts {
                    edges.push((pkg.name.clone(), dep.name.clone()));
                }
            }

            crates.push(CrateInfo {
                name: pkg.name.clone(),
                version: pkg.version.clone(),
                manifest_dir,
                manifest_path,
                is_publishable: is_publishable(pkg),
            });
        }

        if crates.is_empty() {
            bail!("no workspace members found at {repo_root:?}");
        }

        let version = single_version(&crates)?;

        Ok(Workspace {
            version,
            crates,
            edges,
        })
    }

    /// The publishable crates in topological dependency order: an edge `A → B`
    /// means "A depends on B", so `B` precedes `A`. Normal + build in-workspace
    /// deps only (dev-deps ignored); the sorted member graph is then filtered to
    /// publishable crates.
    ///
    /// # Errors
    /// Errors naming the involved crates if the dependency graph contains a cycle.
    pub fn publish_order(&self) -> Result<Vec<&CrateInfo>> {
        let names: Vec<String> = self.crates.iter().map(|c| c.name.clone()).collect();

        let ordered_names =
            topo_sort(&names, &self.edges).map_err(|cycle| anyhow!(cycle.to_string()))?;

        // Map names back to &CrateInfo, then filter to publishable crates only.
        let by_name: BTreeMap<&str, &CrateInfo> =
            self.crates.iter().map(|c| (c.name.as_str(), c)).collect();

        let order = ordered_names
            .iter()
            .filter_map(|n| by_name.get(n.as_str()).copied())
            .filter(|c| c.is_publishable)
            .collect();

        Ok(order)
    }
}

/// Whether a package is publishable: `false` iff `publish = false`, which
/// `cargo metadata` reports as an empty registry list.
fn is_publishable(pkg: &cargo_metadata::Package) -> bool {
    match &pkg.publish {
        // `publish = false` → `Some([])`; a non-empty registry list is publishable.
        Some(list) => !list.is_empty(),
        None => true,
    }
}

/// The single workspace version, erroring if members disagree (violates the
/// single-version invariant).
fn single_version(crates: &[CrateInfo]) -> Result<Version> {
    let mut iter = crates.iter();
    let first = iter.next().expect("crates is non-empty").version.clone();
    for c in iter {
        if c.version != first {
            bail!(
                "workspace members disagree on version: {} is {} but expected {}",
                c.name,
                c.version,
                first
            );
        }
    }
    Ok(first)
}

/// Pure topological sort over an abstract graph (unit-testable without `cargo`).
///
/// - `nodes`: node identifiers; also the deterministic tie-break order for nodes
///   with no ordering constraint.
/// - `edges`: `(a, b)` meaning "a depends on b" → `b` precedes `a`. Edges
///   referencing an unknown node are ignored.
///
/// Returns `Err(Cycle)` naming the crates in a cycle.
pub fn topo_sort(
    nodes: &[String],
    edges: &[(String, String)],
) -> std::result::Result<Vec<String>, Cycle> {
    let node_set: BTreeSet<&str> = nodes.iter().map(String::as_str).collect();

    // dependents[b] = set of a's that depend on b (so emitting b unblocks them).
    // remaining[a]  = count of b's that a still waits on (a's out-degree).
    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut remaining: BTreeMap<&str, usize> = nodes.iter().map(|n| (n.as_str(), 0)).collect();

    for (a, b) in edges {
        let (a, b) = (a.as_str(), b.as_str());
        if !node_set.contains(a) || !node_set.contains(b) || a == b {
            // Self-loop (a==b) is a degenerate cycle: keep it so the cycle
            // detector below catches it.
            if a == b && node_set.contains(a) {
                *remaining.get_mut(a).unwrap() += 1;
                dependents.entry(a).or_default().push(a);
            }
            continue;
        }
        *remaining.get_mut(a).unwrap() += 1;
        dependents.entry(b).or_default().push(a);
    }

    // Kahn's algorithm. Seed the ready set with nodes that depend on nothing,
    // visited in input (`nodes`) order for determinism.
    let mut ready: Vec<&str> = nodes
        .iter()
        .map(String::as_str)
        .filter(|n| remaining[n] == 0)
        .collect();

    let mut order: Vec<String> = Vec::with_capacity(nodes.len());
    while let Some(n) = ready.pop() {
        order.push(n.to_string());
        if let Some(deps) = dependents.get(n) {
            // Sort newly-freed nodes for a stable, input-order-ish result.
            let mut freed: Vec<&str> = Vec::new();
            for &a in deps {
                let r = remaining.get_mut(a).unwrap();
                *r -= 1;
                if *r == 0 {
                    freed.push(a);
                }
            }
            // Push in reverse of `nodes` order so `pop()` yields `nodes` order.
            freed.sort_by_key(|a| std::cmp::Reverse(node_index(nodes, a)));
            ready.extend(freed);
        }
    }

    if order.len() != nodes.len() {
        // Anything with a non-zero remaining count is in (or downstream of) a cycle.
        let mut cycle: Vec<String> = remaining
            .iter()
            .filter(|&(_, &r)| r > 0)
            .map(|(n, _)| n.to_string())
            .collect();
        cycle.sort();
        return Err(Cycle { crates: cycle });
    }

    Ok(order)
}

/// Index of `name` within `nodes` (for deterministic tie-breaking).
fn node_index(nodes: &[String], name: &str) -> usize {
    nodes.iter().position(|n| n == name).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn e(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    // Pure topo_sort (cargo-free)

    /// Topo sort over normal+build deps: a dependency precedes its dependent.
    #[test]
    fn topo_sort_simple_chain() {
        // c depends on b depends on a  =>  a, b, c
        let nodes = s(&["a", "b", "c"]);
        let edges = e(&[("c", "b"), ("b", "a")]);
        let order = topo_sort(&nodes, &edges).expect("acyclic");
        assert_eq!(order, s(&["a", "b", "c"]));
    }

    /// Diamond: a valid order; ties break by input order (determinism).
    #[test]
    fn topo_sort_diamond() {
        // d -> b, d -> c, b -> a, c -> a   (a is foundational, d is top)
        let nodes = s(&["a", "b", "c", "d"]);
        let edges = e(&[("d", "b"), ("d", "c"), ("b", "a"), ("c", "a")]);
        let order = topo_sort(&nodes, &edges).expect("acyclic");

        let pos = |name: &str| order.iter().position(|n| n == name).unwrap();
        assert!(pos("a") < pos("b"));
        assert!(pos("a") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
        assert_eq!(order.len(), 4);
        // Deterministic: a first, d last; b before c by input tie-break.
        assert_eq!(order, s(&["a", "b", "c", "d"]));
    }

    /// A dependency cycle errors, naming the involved crates.
    #[test]
    fn topo_sort_cycle_is_error_naming_crates() {
        // a -> b -> c -> a is a cycle.
        let nodes = s(&["a", "b", "c"]);
        let edges = e(&[("a", "b"), ("b", "c"), ("c", "a")]);
        let err = topo_sort(&nodes, &edges).expect_err("cycle must error");
        assert_eq!(err.crates, s(&["a", "b", "c"]));
        let msg = err.to_string();
        for name in ["a", "b", "c"] {
            assert!(msg.contains(name), "cycle message must name {name}: {msg}");
        }
    }

    /// A self-loop is a degenerate cycle and must be reported.
    #[test]
    fn topo_sort_self_loop_is_cycle() {
        let nodes = s(&["a", "b"]);
        let edges = e(&[("a", "a"), ("b", "a")]);
        let err = topo_sort(&nodes, &edges).expect_err("self-loop is a cycle");
        assert!(err.crates.contains(&"a".to_string()));
    }

    // Integration-style: discover() against the real worktree

    /// The repo root is the parent of this crate's manifest dir (`xtask/`).
    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask has a parent (repo root)")
            .to_path_buf()
    }

    /// Discover the single workspace version, the members, and `is_publishable`
    /// from `publish = false`.
    #[test]
    fn discover_finds_version_members_and_publishability() {
        let ws = Workspace::discover(&repo_root()).expect("discover real workspace");

        // Compare against xtask's own compile-time version (it inherits
        // `version.workspace`) so this needn't change on every release bump.
        assert_eq!(
            ws.version,
            Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
            "single workspace version"
        );

        let by = |name: &str| ws.crates.iter().find(|c| c.name == name);

        // `xtask` is a member and, being `publish = false`, is not publishable.
        assert!(!by("xtask").expect("xtask is a member").is_publishable);

        // A representative sample of publishable library crates (robust to churn).
        for n in [
            "lumina-utils",
            "lumina-node",
            "lumina-node-wasm",
            "celestia-types",
        ] {
            assert!(
                by(n)
                    .unwrap_or_else(|| panic!("{n} should be a member"))
                    .is_publishable,
                "{n} should be publishable"
            );
        }

        // Manifest dir is the crate's own directory.
        let utils = by("lumina-utils").unwrap();
        assert!(utils.manifest_dir.ends_with("utils"));
        assert!(utils.manifest_path.ends_with("Cargo.toml"));
    }

    /// Publish order is a *valid* topological sort (not an exact fixture): `xtask`
    /// is excluded and every dependency precedes its dependent.
    #[test]
    fn publish_order_is_valid_topological_order() {
        let ws = Workspace::discover(&repo_root()).expect("discover real workspace");
        let order: Vec<&str> = ws
            .publish_order()
            .expect("acyclic workspace")
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        // `xtask` (publish = false) is excluded from the publish order.
        assert!(!order.contains(&"xtask"), "xtask must not be published");

        // Every publishable member appears exactly once.
        let publishable: Vec<&str> = ws
            .crates
            .iter()
            .filter(|c| c.is_publishable)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            order.len(),
            publishable.len(),
            "publish order should list every publishable crate once"
        );
        for name in &publishable {
            assert!(order.contains(name), "{name} missing from publish order");
        }

        // Every in-workspace edge is respected: for "a depends on b", b precedes a.
        let pos = |name: &str| order.iter().position(|n| *n == name);
        for (a, b) in &ws.edges {
            if let (Some(pa), Some(pb)) = (pos(a), pos(b)) {
                assert!(pb < pa, "dependency `{b}` must precede dependent `{a}`");
            }
        }
    }
}
