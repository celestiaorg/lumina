use std::collections::BTreeMap;

use cargo_metadata::semver::Version;

use super::{
    VersionBumpKind, apply_transitive_bump_policy, bump_version, classify_version_bump,
    compute_effective_versions,
};
use crate::domain::types::{PlannedVersion, ReleaseMode};

fn find_planned<'a>(versions: &'a [PlannedVersion], name: &str) -> &'a PlannedVersion {
    versions
        .iter()
        .find(|v| v.package == name)
        .unwrap_or_else(|| panic!("package `{name}` not found in planned versions"))
}

// ── Transitive bump policy ───────────────────────────────────────────

#[test]
fn direct_minor_bump_propagates_to_dependent() {
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("2.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([("b".into(), vec!["a".into()])]);
    let mut planned = BTreeMap::from([("a".into(), Version::parse("1.1.0").unwrap())]);

    let forced = apply_transitive_bump_policy(&current, &deps, &mut planned);

    assert_eq!(forced, 1);
    assert_eq!(planned["b"].to_string(), "2.1.0");
}

#[test]
fn transitive_major_bump_propagates_across_chain() {
    // a → b → c chain, a gets major bump.
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("1.2.0").unwrap()),
        ("c".into(), Version::parse("3.4.5").unwrap()),
    ]);
    let deps = BTreeMap::from([
        ("b".into(), vec!["a".into()]),
        ("c".into(), vec!["b".into()]),
    ]);
    let mut planned = BTreeMap::from([("a".into(), Version::parse("2.0.0").unwrap())]);

    let forced = apply_transitive_bump_policy(&current, &deps, &mut planned);

    assert_eq!(forced, 2);
    assert_eq!(planned["b"].to_string(), "2.0.0");
    assert_eq!(planned["c"].to_string(), "4.0.0");
}

#[test]
fn existing_higher_bump_is_not_downgraded() {
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("1.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([("b".into(), vec!["a".into()])]);
    let mut planned = BTreeMap::from([
        ("a".into(), Version::parse("1.0.1").unwrap()),
        ("b".into(), Version::parse("2.0.0").unwrap()),
    ]);

    let forced = apply_transitive_bump_policy(&current, &deps, &mut planned);

    assert_eq!(forced, 0);
    assert_eq!(planned["b"].to_string(), "2.0.0");
}

#[test]
fn dependent_gets_max_bump_from_multiple_deps() {
    // A depends on B(minor), C(patch), D(major) → A gets major.
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("1.0.0").unwrap()),
        ("c".into(), Version::parse("1.0.0").unwrap()),
        ("d".into(), Version::parse("1.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([("a".into(), vec!["b".into(), "c".into(), "d".into()])]);
    let mut planned = BTreeMap::from([
        ("b".into(), Version::parse("1.1.0").unwrap()), // minor
        ("c".into(), Version::parse("1.0.1").unwrap()), // patch
        ("d".into(), Version::parse("2.0.0").unwrap()), // major
    ]);

    let forced = apply_transitive_bump_policy(&current, &deps, &mut planned);

    assert_eq!(forced, 1);
    assert_eq!(planned["a"].to_string(), "2.0.0");
}

#[test]
fn bump_classifier_ignores_prerelease_for_comparison() {
    let current = Version::parse("1.2.3-rc.4").unwrap();
    let next = Version::parse("1.2.4-rc.1").unwrap();
    assert_eq!(
        classify_version_bump(&current, &next),
        VersionBumpKind::Patch
    );
}

#[test]
fn bump_version_resets_components() {
    let current = Version::parse("4.5.6-rc.3").unwrap();
    assert_eq!(
        bump_version(&current, VersionBumpKind::Minor).to_string(),
        "4.6.0"
    );
    assert_eq!(
        bump_version(&current, VersionBumpKind::Major).to_string(),
        "5.0.0"
    );
}

// ── compute_effective_versions: RC mode ──────────────────────────────

#[test]
fn rc_mode_single_package_patch_bump() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.0").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.1").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();

    assert_eq!(result.len(), 1);
    let v = find_planned(&result, "pkg-a");
    assert_eq!(v.current, "1.0.0");
    assert_eq!(v.next_effective, "1.0.1-rc.1");
    assert!(v.publishable);
}

#[test]
fn rc_mode_single_package_minor_bump() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("2.3.0").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("2.4.0").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(find_planned(&result, "pkg-a").next_effective, "2.4.0-rc.1");
}

#[test]
fn rc_mode_single_package_major_bump() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.9.9").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("2.0.0").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(find_planned(&result, "pkg-a").next_effective, "2.0.0-rc.1");
}

#[test]
fn rc_mode_existing_rc_increments_index() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.1-rc.1").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.1-rc.1").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(find_planned(&result, "pkg-a").next_effective, "1.0.1-rc.2");
}

#[test]
fn rc_mode_high_rc_index_increments() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("3.2.1-rc.15").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("3.2.1-rc.15").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(find_planned(&result, "pkg-a").next_effective, "3.2.1-rc.16");
}

#[test]
fn rc_mode_multiple_packages_different_bumps() {
    let current = BTreeMap::from([
        ("cli".into(), Version::parse("0.5.0").unwrap()),
        ("core".into(), Version::parse("1.0.0").unwrap()),
        ("sdk".into(), Version::parse("2.0.0").unwrap()),
    ]);
    let planned = BTreeMap::from([
        ("core".into(), Version::parse("1.0.1").unwrap()),
        ("sdk".into(), Version::parse("2.1.0").unwrap()),
        ("cli".into(), Version::parse("1.0.0").unwrap()),
    ]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].package, "cli");
    assert_eq!(result[0].next_effective, "1.0.0-rc.1");
    assert_eq!(result[1].package, "core");
    assert_eq!(result[1].next_effective, "1.0.1-rc.1");
    assert_eq!(result[2].package, "sdk");
    assert_eq!(result[2].next_effective, "2.1.0-rc.1");
}

// ── compute_effective_versions: Final mode ───────────────────────────

#[test]
fn final_mode_stable_version_passes_through() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.0").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.1").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Final).unwrap();
    assert_eq!(find_planned(&result, "pkg-a").next_effective, "1.0.1");
}

#[test]
fn final_mode_strips_prerelease_from_rc() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.1-rc.3").unwrap())]);
    let planned = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.1-rc.3").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Final).unwrap();
    assert_eq!(find_planned(&result, "pkg-a").next_effective, "1.0.1");
}

#[test]
fn final_mode_multiple_packages_mixed_rc_and_stable() {
    let current = BTreeMap::from([
        ("core".into(), Version::parse("1.0.1-rc.2").unwrap()),
        ("sdk".into(), Version::parse("2.0.0").unwrap()),
        ("cli".into(), Version::parse("0.6.0-rc.1").unwrap()),
    ]);
    let planned = BTreeMap::from([
        ("core".into(), Version::parse("1.0.1-rc.2").unwrap()),
        ("sdk".into(), Version::parse("2.1.0").unwrap()),
        ("cli".into(), Version::parse("0.6.0-rc.1").unwrap()),
    ]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Final).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(find_planned(&result, "cli").next_effective, "0.6.0");
    assert_eq!(find_planned(&result, "core").next_effective, "1.0.1");
    assert_eq!(find_planned(&result, "sdk").next_effective, "2.1.0");
}

// ── Edge cases ───────────────────────────────────────────────────────

#[test]
fn empty_planned_versions_returns_empty() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.0").unwrap())]);
    let result = compute_effective_versions(&current, BTreeMap::new(), ReleaseMode::Rc).unwrap();
    assert!(result.is_empty());
}

#[test]
fn planned_package_not_in_current_is_skipped() {
    let current = BTreeMap::from([("pkg-a".into(), Version::parse("1.0.0").unwrap())]);
    let planned = BTreeMap::from([
        ("pkg-a".into(), Version::parse("1.0.1").unwrap()),
        ("unknown-pkg".into(), Version::parse("0.1.0").unwrap()),
    ]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].package, "pkg-a");
}

#[test]
fn results_are_sorted_alphabetically_by_package_name() {
    let current = BTreeMap::from([
        ("zebra".into(), Version::parse("1.0.0").unwrap()),
        ("alpha".into(), Version::parse("1.0.0").unwrap()),
        ("middle".into(), Version::parse("1.0.0").unwrap()),
    ]);
    let planned = BTreeMap::from([
        ("zebra".into(), Version::parse("1.0.1").unwrap()),
        ("alpha".into(), Version::parse("1.0.1").unwrap()),
        ("middle".into(), Version::parse("1.0.1").unwrap()),
    ]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Final).unwrap();
    let names: Vec<&str> = result.iter().map(|v| v.package.as_str()).collect();
    assert_eq!(names, vec!["alpha", "middle", "zebra"]);
}

#[test]
fn all_planned_versions_are_marked_publishable() {
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("2.0.0").unwrap()),
    ]);
    let planned = BTreeMap::from([
        ("a".into(), Version::parse("1.0.1").unwrap()),
        ("b".into(), Version::parse("2.0.1").unwrap()),
    ]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert!(result.iter().all(|v| v.publishable));
}

#[test]
fn current_version_is_preserved_in_output() {
    let current = BTreeMap::from([("pkg".into(), Version::parse("1.2.3-rc.5").unwrap())]);
    let planned = BTreeMap::from([("pkg".into(), Version::parse("1.2.4").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(result[0].current, "1.2.3-rc.5");
    assert_eq!(result[0].next_effective, "1.2.4-rc.1");
}

// ── Full pipeline: transitive bumps + mode projection ────────────────

#[test]
fn full_pipeline_rc_transitive_patch_bump_with_projection() {
    let current = BTreeMap::from([
        ("pkg-core".into(), Version::parse("1.0.0").unwrap()),
        ("pkg-app".into(), Version::parse("2.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([("pkg-app".into(), vec!["pkg-core".into()])]);
    let mut planned = BTreeMap::from([("pkg-core".into(), Version::parse("1.0.1").unwrap())]);

    let forced = apply_transitive_bump_policy(&current, &deps, &mut planned);
    assert_eq!(forced, 1);
    assert_eq!(planned["pkg-app"].to_string(), "2.0.1");

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(
        find_planned(&result, "pkg-app").next_effective,
        "2.0.1-rc.1"
    );
    assert_eq!(
        find_planned(&result, "pkg-core").next_effective,
        "1.0.1-rc.1"
    );
}

#[test]
fn full_pipeline_final_transitive_minor_bump_strips_rc() {
    let current = BTreeMap::from([
        ("pkg-lib".into(), Version::parse("1.0.0-rc.2").unwrap()),
        ("pkg-bin".into(), Version::parse("3.0.0-rc.2").unwrap()),
    ]);
    let deps = BTreeMap::from([("pkg-bin".into(), vec!["pkg-lib".into()])]);
    let mut planned = BTreeMap::from([("pkg-lib".into(), Version::parse("1.1.0-rc.2").unwrap())]);

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Final).unwrap();

    assert_eq!(find_planned(&result, "pkg-lib").next_effective, "1.1.0");
    assert_eq!(find_planned(&result, "pkg-bin").next_effective, "3.1.0");
}

#[test]
fn full_pipeline_diamond_dependency_graph_rc() {
    //   A
    //  / \
    // B   C
    //  \ /
    //   D
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("1.0.0").unwrap()),
        ("c".into(), Version::parse("1.0.0").unwrap()),
        ("d".into(), Version::parse("1.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([
        ("b".into(), vec!["a".into()]),
        ("c".into(), vec!["a".into()]),
        ("d".into(), vec!["b".into(), "c".into()]),
    ]);
    let mut planned = BTreeMap::from([("a".into(), Version::parse("2.0.0").unwrap())]);

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();

    assert_eq!(result.len(), 4);
    assert_eq!(find_planned(&result, "a").next_effective, "2.0.0-rc.1");
    assert_eq!(find_planned(&result, "b").next_effective, "2.0.0-rc.1");
    assert_eq!(find_planned(&result, "c").next_effective, "2.0.0-rc.1");
    assert_eq!(find_planned(&result, "d").next_effective, "2.0.0-rc.1");
}

#[test]
fn full_pipeline_chain_propagation_preserves_existing_higher_bumps() {
    // a → b → c; a=patch, c already planned as major.
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("1.0.0").unwrap()),
        ("c".into(), Version::parse("1.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([
        ("b".into(), vec!["a".into()]),
        ("c".into(), vec!["b".into()]),
    ]);
    let mut planned = BTreeMap::from([
        ("a".into(), Version::parse("1.0.1").unwrap()),
        ("c".into(), Version::parse("2.0.0").unwrap()),
    ]);

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();

    assert_eq!(find_planned(&result, "a").next_effective, "1.0.1-rc.1");
    assert_eq!(find_planned(&result, "b").next_effective, "1.0.1-rc.1");
    assert_eq!(find_planned(&result, "c").next_effective, "2.0.0-rc.1");
}

#[test]
fn full_pipeline_rc_to_rc_same_stable_no_transitive() {
    // Second RC: packages at rc.1, planned stays rc.1.
    // Transitive compares stable parts (1.0.1 == 1.0.1) → no propagation.
    let current = BTreeMap::from([
        ("core".into(), Version::parse("1.0.1-rc.1").unwrap()),
        ("app".into(), Version::parse("2.0.1-rc.1").unwrap()),
    ]);
    let deps = BTreeMap::from([("app".into(), vec!["core".into()])]);
    let mut planned = BTreeMap::from([("core".into(), Version::parse("1.0.1-rc.1").unwrap())]);

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(find_planned(&result, "core").next_effective, "1.0.1-rc.2");
}

#[test]
fn full_pipeline_rc_second_iteration_new_patch_propagates() {
    // core at rc.1, release-plz plans 1.0.2 (new patch) → app gets transitive patch.
    let current = BTreeMap::from([
        ("core".into(), Version::parse("1.0.1-rc.1").unwrap()),
        ("app".into(), Version::parse("2.0.1-rc.1").unwrap()),
    ]);
    let deps = BTreeMap::from([("app".into(), vec!["core".into()])]);
    let mut planned = BTreeMap::from([("core".into(), Version::parse("1.0.2").unwrap())]);

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(find_planned(&result, "core").next_effective, "1.0.2-rc.1");
    assert_eq!(find_planned(&result, "app").next_effective, "2.0.2-rc.1");
}

#[test]
fn full_pipeline_no_bumps_produces_empty_result() {
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("2.0.0").unwrap()),
    ]);
    let deps = BTreeMap::from([("b".into(), vec!["a".into()])]);
    let mut planned = BTreeMap::new();

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert!(result.is_empty());
}

#[test]
fn full_pipeline_final_mode_complex_workspace() {
    let current = BTreeMap::from([
        ("lumina-node".into(), Version::parse("0.5.1-rc.3").unwrap()),
        ("lumina-sdk".into(), Version::parse("0.3.0-rc.3").unwrap()),
        ("lumina-cli".into(), Version::parse("0.2.0-rc.3").unwrap()),
        ("lumina-utils".into(), Version::parse("0.1.5-rc.3").unwrap()),
    ]);
    let deps = BTreeMap::from([
        (
            "lumina-sdk".into(),
            vec!["lumina-node".into(), "lumina-utils".into()],
        ),
        ("lumina-cli".into(), vec!["lumina-sdk".into()]),
    ]);
    let mut planned = BTreeMap::from([
        ("lumina-node".into(), Version::parse("0.5.1-rc.3").unwrap()),
        ("lumina-sdk".into(), Version::parse("0.3.0-rc.3").unwrap()),
        ("lumina-cli".into(), Version::parse("0.2.0-rc.3").unwrap()),
        ("lumina-utils".into(), Version::parse("0.1.5-rc.3").unwrap()),
    ]);

    apply_transitive_bump_policy(&current, &deps, &mut planned);
    let result = compute_effective_versions(&current, planned, ReleaseMode::Final).unwrap();

    assert_eq!(result.len(), 4);
    assert_eq!(find_planned(&result, "lumina-cli").next_effective, "0.2.0");
    assert_eq!(find_planned(&result, "lumina-node").next_effective, "0.5.1");
    assert_eq!(find_planned(&result, "lumina-sdk").next_effective, "0.3.0");
    assert_eq!(
        find_planned(&result, "lumina-utils").next_effective,
        "0.1.5"
    );
}

#[test]
fn full_pipeline_mixed_bumps_some_packages_unchanged() {
    let current = BTreeMap::from([
        ("a".into(), Version::parse("1.0.0").unwrap()),
        ("b".into(), Version::parse("2.0.0").unwrap()),
        ("c".into(), Version::parse("3.0.0").unwrap()),
    ]);
    let planned = BTreeMap::from([("a".into(), Version::parse("1.1.0").unwrap())]);

    let result = compute_effective_versions(&current, planned, ReleaseMode::Rc).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(find_planned(&result, "a").next_effective, "1.1.0-rc.1");
}
