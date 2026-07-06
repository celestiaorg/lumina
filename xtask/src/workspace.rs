//! Workspace discovery for the release tool.
//!
//! Shells out to `cargo metadata` to learn the facts the release process needs
//! about the workspace it is releasing: the single workspace version, the list of
//! member crates (name, version, manifest dir/path, publishability), and the
//! **topological publish order** — publishable crates ordered so each is published
//! only after every in-workspace crate it depends on.
//!
//! Implements `release-spec/orchestrator.md` §"Portability & configuration" (the
//! "Discovered" bullet) and `release-spec/publish-crates-logic.md` §"The
//! workspace" / §"Compute the publish order ourselves".

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use cargo_metadata::{DependencyKind, MetadataCommand};

/// Re-export of the `semver` crate that `cargo_metadata` parses versions with, so
/// dependents can name [`Version`] without adding their own `semver` dependency.
pub use cargo_metadata::semver;

use semver::Version;

/// Facts about a single workspace member crate.
#[derive(Debug, Clone)]
pub struct CrateInfo {
    /// Package name, e.g. `toy-kv-utils`.
    pub name: String,
    /// The crate's version (equals the workspace version under the
    /// single-workspace-version rule).
    pub version: Version,
    /// Absolute directory containing the crate's `Cargo.toml` (its own directory;
    /// used for changelog path-scoping by downstream modules).
    pub manifest_dir: PathBuf,
    /// Absolute path to the crate's `Cargo.toml`.
    pub manifest_path: PathBuf,
    /// `false` iff the manifest sets `publish = false`; `true` otherwise.
    pub is_publishable: bool,
}

/// The discovered workspace: its single version and every member crate.
#[derive(Debug, Clone)]
pub struct Workspace {
    /// The single workspace version (`[workspace.package].version`), shared by
    /// every member via `version.workspace = true`.
    pub version: Version,
    /// Every workspace member, in `cargo metadata` order (publishable or not).
    pub crates: Vec<CrateInfo>,
    /// In-workspace dependency edges `(a, b)` meaning "crate `a` depends on crate
    /// `b`" (normal + build kinds only; dev-deps excluded). Captured at discovery
    /// time because the raw dependency kinds are only available from
    /// `cargo metadata`. Not part of the public surface.
    edges: Vec<(String, String)>,
}

/// A dependency cycle in the in-workspace dependency graph.
///
/// Returned by the pure [`topo_sort`] helper so cycle behavior can be unit-tested
/// without running `cargo`. Carries the names of the crates participating in the
/// cycle.
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
    /// Discover the workspace rooted at `repo_root` by shelling out to
    /// `cargo metadata`.
    ///
    /// # Side effects
    /// Spawns one `cargo metadata` subprocess. No network, no filesystem writes.
    ///
    /// # Errors
    /// - `cargo metadata` fails to run or parse (cargo missing, unreadable or
    ///   invalid manifest).
    /// - The workspace has no members.
    /// - Members disagree on their version (violates the single-version invariant);
    ///   the error names the differing versions.
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

            // Edge `pkg -> dep` for each normal/build dependency that is also a
            // workspace member. Dev-dependencies are ignored.
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

    /// The publishable crates in topological dependency order.
    ///
    /// Edges are computed over **normal and build** in-workspace dependencies
    /// (dev-dependencies are ignored); an edge `A → B` means "A depends on B", so
    /// `B` precedes `A`. The full member graph is sorted, then filtered to
    /// publishable crates (relative order preserved).
    ///
    /// For the toy-kv fixture this is `[toy-kv-utils, toy-kv, toy-kv-wasm]`.
    ///
    /// # Errors
    /// Returns an error naming the involved crates if the in-workspace dependency
    /// graph contains a cycle (an unpublishable, malformed workspace).
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

/// Whether a package is publishable: `false` iff `publish = false`
/// (`cargo metadata` reports an empty registry list), `true` otherwise.
fn is_publishable(pkg: &cargo_metadata::Package) -> bool {
    match &pkg.publish {
        // `publish = false` → `Some([])`; an explicit non-empty registry list is
        // still publishable (to those registries).
        Some(list) => !list.is_empty(),
        // `publish` absent → publishable to any registry.
        None => true,
    }
}

/// Determine the single workspace version from the member versions, erroring if
/// they disagree (which would violate the single-workspace-version invariant).
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

/// Pure topological sort over an abstract graph, factored out so cycle behavior is
/// unit-testable **without** running `cargo`.
///
/// - `nodes`: the node identifiers (crate names). Defines membership and the
///   deterministic tie-break order for nodes with no ordering constraint.
/// - `edges`: `(a, b)` meaning "a depends on b" → `b` precedes `a` in the output.
///
/// Edges referencing a node not in `nodes` are ignored.
///
/// Returns `Ok(order)` with every dependency before its dependents, or
/// `Err(Cycle)` naming the crates that participate in a cycle.
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
            // self-loop a==b is a degenerate cycle; let it fall through to the
            // cycle detector below by NOT skipping it.
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
        // Whatever still has a non-zero remaining count is in (or downstream of) a
        // cycle.
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

    // --- Pure topo_sort (cargo-free) ----------------------------------------

    /// Rule: topo sort over normal+build deps; dependency precedes dependent.
    /// (publish-crates-logic.md §"Compute the publish order ourselves" steps 1–3.)
    #[test]
    fn topo_sort_simple_chain() {
        // c depends on b depends on a  =>  a, b, c
        let nodes = s(&["a", "b", "c"]);
        let edges = e(&[("c", "b"), ("b", "a")]);
        let order = topo_sort(&nodes, &edges).expect("acyclic");
        assert_eq!(order, s(&["a", "b", "c"]));
    }

    /// Rule: a diamond still yields a valid order where every dependency precedes
    /// its dependents (publish-crates-logic.md §"Compute…" step 3). Determinism is
    /// also checked: ties break by input order.
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

    /// Rule: a dependency cycle => error naming the involved crates
    /// (publish-crates-logic.md §"Compute…" step 4, cycle detection).
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

    /// A self-loop is a degenerate cycle and must be reported, not silently
    /// dropped (publish-crates-logic.md §"Compute…" step 4).
    #[test]
    fn topo_sort_self_loop_is_cycle() {
        let nodes = s(&["a", "b"]);
        let edges = e(&[("a", "a"), ("b", "a")]);
        let err = topo_sort(&nodes, &edges).expect_err("self-loop is a cycle");
        assert!(err.crates.contains(&"a".to_string()));
    }

    // --- Integration-style: discover() against the real worktree ------------

    /// The repo root is the parent of this crate's manifest dir (`xtask/`).
    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask has a parent (repo root)")
            .to_path_buf()
    }

    /// Rules: discover the single workspace version + the four members +
    /// is_publishable from `publish = false` (orchestrator.md §Portability
    /// "Discovered" bullet; publish-crates-logic.md §"The workspace").
    #[test]
    fn discover_finds_version_members_and_publishability() {
        let ws = Workspace::discover(&repo_root()).expect("discover real workspace");

        // The workspace version is a single inherited value; compare against
        // xtask's own compile-time version (it inherits `version.workspace`) so this
        // does not need updating on every release bump.
        assert_eq!(
            ws.version,
            Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
            "single workspace version"
        );

        let by = |name: &str| ws.crates.iter().find(|c| c.name == name);

        // `xtask` is a member and, being `publish = false`, is not publishable.
        assert!(!by("xtask").expect("xtask is a member").is_publishable);

        // A representative sample of lumina's publishable library crates. Asserting
        // a sample (rather than the full member list) keeps this test robust as
        // crates are added/removed.
        for n in ["lumina-utils", "lumina-node", "lumina-node-wasm", "celestia-types"] {
            assert!(
                by(n).unwrap_or_else(|| panic!("{n} should be a member")).is_publishable,
                "{n} should be publishable"
            );
        }

        // Manifest dir is the crate's own directory.
        let utils = by("lumina-utils").unwrap();
        assert!(utils.manifest_dir.ends_with("utils"));
        assert!(utils.manifest_path.ends_with("Cargo.toml"));
    }

    /// Rules: topological publish order over normal+build in-workspace deps, then
    /// filter to publishable crates dropping `publish = false`
    /// (publish-crates-logic.md §"Compute…", final filter paragraph). Asserts the
    /// order is a *valid* topological sort rather than an exact fixture: `xtask` is
    /// excluded, and every in-workspace dependency precedes its dependent.
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

        // The order respects every in-workspace edge: for `a depends on b`, `b`
        // must be published before `a`. (Edges to non-publishable crates are
        // irrelevant since such crates are absent from `order`.)
        let pos = |name: &str| order.iter().position(|n| *n == name);
        for (a, b) in &ws.edges {
            if let (Some(pa), Some(pb)) = (pos(a), pos(b)) {
                assert!(pb < pa, "dependency `{b}` must precede dependent `{a}`");
            }
        }
    }
}
