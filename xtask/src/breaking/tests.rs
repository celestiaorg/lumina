//! Unit tests for the pure logic of `breaking`. The live `cargo-semver-checks` run
//! is not exercised — interpretation is tested via the seam with synthetic strings.

use super::*;
use crate::commit::ParsedCommit;

/// Build a `ParsedCommit` succinctly for tests.
fn pc(
    kind: Option<&str>,
    scope: Option<&str>,
    bang: bool,
    description: &str,
    footer: Option<&str>,
) -> ParsedCommit {
    ParsedCommit {
        kind: kind.map(str::to_string),
        scope: scope.map(str::to_string),
        breaking_bang: bang,
        description: description.to_string(),
        breaking_footer: footer.map(str::to_string),
    }
}

// intent detection

#[test]
fn intent_none_when_no_breaking_commits() {
    let commits = [
        pc(Some("feat"), Some("core"), false, "add thing", None),
        pc(Some("fix"), None, false, "fix bug", None),
        pc(None, None, false, "non conventional", None),
    ];
    assert_eq!(intent_signal(&commits), None);
}

#[test]
fn intent_detects_bang_marker() {
    // `feat(core)!: change get() signature` — `!` only, no footer.
    let commits = [pc(
        Some("feat"),
        Some("core"),
        true,
        "change get() signature",
        None,
    )];
    let reason = intent_signal(&commits).expect("bang marker should fire");
    assert_eq!(
        reason,
        r#"breaking commit: "feat(core)!: change get() signature""#
    );
}

#[test]
fn intent_detects_breaking_change_footer() {
    let commits = [pc(
        Some("refactor"),
        None,
        false,
        "rework api",
        Some("the get() return type changed"),
    )];
    let reason = intent_signal(&commits).expect("footer should fire");
    // Footer text preferred over a reconstructed subject.
    assert_eq!(reason, "the get() return type changed");
}

#[test]
fn intent_detects_breaking_dash_change_footer_spelling() {
    // Both `BREAKING CHANGE:` and `BREAKING-CHANGE:` normalize into
    // `breaking_footer`; either spelling yields the footer text here.
    let commits = [pc(Some("chore"), None, false, "bump", Some("dropped X"))];
    assert_eq!(intent_signal(&commits), Some("dropped X".to_string()));
}

#[test]
fn intent_footer_wins_over_bang_subject() {
    // A `!`-marked commit that ALSO has a footer: footer text is the reason.
    let commits = [pc(
        Some("feat"),
        Some("core"),
        true,
        "change get() signature",
        Some("get() now returns Option"),
    )];
    assert_eq!(
        intent_signal(&commits),
        Some("get() now returns Option".to_string())
    );
}

#[test]
fn intent_first_footer_wins() {
    let commits = [
        pc(Some("feat"), None, false, "a", Some("first footer")),
        pc(Some("fix"), None, false, "b", Some("second footer")),
    ];
    assert_eq!(intent_signal(&commits), Some("first footer".to_string()));
}

#[test]
fn intent_footer_preferred_even_when_bang_commit_comes_first() {
    // Precedence is by signal kind (footer > bang), not by position.
    let commits = [
        pc(Some("feat"), None, true, "bang first", None),
        pc(Some("fix"), None, false, "later", Some("footer text")),
    ];
    assert_eq!(intent_signal(&commits), Some("footer text".to_string()));
}

// strip_ansi

#[test]
fn strip_ansi_removes_sgr_color_codes() {
    let input = "\u{1b}[31m--- failure get_must_use\u{1b}[0m: items changed";
    assert_eq!(strip_ansi(input), "--- failure get_must_use: items changed");
}

#[test]
fn strip_ansi_handles_multi_param_codes() {
    let input = "\u{1b}[1;33mwarn\u{1b}[0m done";
    assert_eq!(strip_ansi(input), "warn done");
}

#[test]
fn strip_ansi_passes_plain_text_through() {
    assert_eq!(strip_ansi("no escapes here"), "no escapes here");
}

// result interpretation (the shell-out seam)

#[test]
fn interpret_exit_zero_is_compatible() {
    let (status, reason) = interpret_semver_checks(true, Some(0), "all good", "");
    assert_eq!(status, ApiStatus::Compatible);
    assert_eq!(reason, None);
}

#[test]
fn interpret_nonzero_with_report_is_incompatible_ansi_stripped() {
    let stderr = "\u{1b}[31m--- failure struct_missing\u{1b}[0m: Foo removed\n";
    let (status, reason) = interpret_semver_checks(false, Some(1), "", stderr);
    assert_eq!(status, ApiStatus::Incompatible);
    assert_eq!(
        reason.as_deref(),
        Some("--- failure struct_missing: Foo removed")
    );
}

#[test]
fn interpret_prefers_stderr_report_over_stdout() {
    let (status, reason) =
        interpret_semver_checks(false, Some(1), "stdout noise", "real report on stderr");
    assert_eq!(status, ApiStatus::Incompatible);
    assert_eq!(reason.as_deref(), Some("real report on stderr"));
}

#[test]
fn interpret_falls_back_to_stdout_when_stderr_empty() {
    let (status, reason) = interpret_semver_checks(false, Some(1), "stdout report", "   ");
    assert_eq!(status, ApiStatus::Incompatible);
    assert_eq!(reason.as_deref(), Some("stdout report"));
}

#[test]
fn interpret_nonzero_with_no_report_fails_open_to_unavailable() {
    let (status, reason) = interpret_semver_checks(false, Some(101), "", "");
    assert_eq!(status, ApiStatus::Unavailable);
    assert_eq!(reason, None);
}

// reconstruct_subject

#[test]
fn reconstruct_subject_with_scope_and_bang() {
    let c = pc(
        Some("feat"),
        Some("core"),
        true,
        "change get() signature",
        None,
    );
    assert_eq!(
        reconstruct_subject(&c),
        "feat(core)!: change get() signature"
    );
}

#[test]
fn reconstruct_subject_without_scope() {
    let c = pc(Some("fix"), None, true, "drop deprecated path", None);
    assert_eq!(reconstruct_subject(&c), "fix!: drop deprecated path");
}

#[test]
fn reconstruct_subject_non_conventional_is_description() {
    let c = pc(None, None, false, "merge stuff", None);
    assert_eq!(reconstruct_subject(&c), "merge stuff");
}

// combine: signal combination + reason precedence

#[test]
fn combine_neither_signal_is_not_breaking() {
    let r = combine(ApiStatus::Compatible, None, None);
    assert_eq!(
        r,
        BreakingReport {
            has_breaking: false,
            reason: None,
            api_status: ApiStatus::Compatible,
        }
    );
}

#[test]
fn combine_api_only() {
    let r = combine(
        ApiStatus::Incompatible,
        Some("Foo::bar removed".to_string()),
        None,
    );
    assert!(r.has_breaking);
    assert_eq!(r.reason.as_deref(), Some("Foo::bar removed"));
    assert_eq!(r.api_status, ApiStatus::Incompatible);
}

#[test]
fn combine_intent_only() {
    // API not Incompatible (e.g. Skipped/Unavailable) but intent fired.
    let r = combine(
        ApiStatus::Skipped,
        None,
        Some("behavioral break".to_string()),
    );
    assert!(r.has_breaking);
    assert_eq!(r.reason.as_deref(), Some("behavioral break"));
    assert_eq!(r.api_status, ApiStatus::Skipped);
}

#[test]
fn combine_both_includes_api_then_intent() {
    let r = combine(
        ApiStatus::Incompatible,
        Some("get() signature changed".to_string()),
        Some("author flagged break".to_string()),
    );
    assert!(r.has_breaking);
    assert_eq!(
        r.reason.as_deref(),
        Some("get() signature changed\n\nauthor note: author flagged break")
    );
    assert_eq!(r.api_status, ApiStatus::Incompatible);
}

#[test]
fn combine_unavailable_with_intent_is_breaking() {
    // Fail-open: API Unavailable, intent decides.
    let r = combine(ApiStatus::Unavailable, None, Some("dropped Y".to_string()));
    assert!(r.has_breaking);
    assert_eq!(r.reason.as_deref(), Some("dropped Y"));
    assert_eq!(r.api_status, ApiStatus::Unavailable);
}

#[test]
fn combine_unavailable_without_intent_is_not_breaking() {
    let r = combine(ApiStatus::Unavailable, None, None);
    assert!(!r.has_breaking);
    assert_eq!(r.reason, None);
}

// has_library_target

#[test]
fn library_target_explicit_lib_crate_type() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname=\"x\"\nversion=\"0.1.0\"\n[lib]\ncrate-type=[\"lib\"]\n",
    )
    .unwrap();
    assert!(has_library_target(&manifest));
}

#[test]
fn library_target_cdylib_only_is_not_a_library() {
    // The thin wasm cdylib (lumina-node-wasm) → Skipped.
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname=\"w\"\nversion=\"0.1.0\"\n[lib]\ncrate-type=[\"cdylib\"]\n",
    )
    .unwrap();
    assert!(!has_library_target(&manifest));
}

#[test]
fn library_target_lib_without_crate_type_defaults_to_library() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname=\"x\"\nversion=\"0.1.0\"\n[lib]\nname=\"x\"\n",
    )
    .unwrap();
    assert!(has_library_target(&manifest));
}

#[test]
fn library_target_no_lib_table_with_src_lib_rs() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(&manifest, "[package]\nname=\"x\"\nversion=\"0.1.0\"\n").unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    std::fs::write(dir.path().join("src").join("lib.rs"), "// lib\n").unwrap();
    assert!(has_library_target(&manifest));
}

#[test]
fn library_target_binary_only_has_no_library() {
    // No [lib] table and no src/lib.rs → binary-only → Skipped.
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(&manifest, "[package]\nname=\"b\"\nversion=\"0.1.0\"\n").unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    std::fs::write(dir.path().join("src").join("main.rs"), "fn main(){}\n").unwrap();
    assert!(!has_library_target(&manifest));
}

#[test]
fn library_target_missing_manifest_is_conservatively_false() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("does-not-exist").join("Cargo.toml");
    assert!(!has_library_target(&manifest));
}

// analyze: never-published short-circuit (no real tool run)

#[test]
fn analyze_no_baseline_is_first_release_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname=\"x\"\nversion=\"0.1.0\"\n[lib]\ncrate-type=[\"lib\"]\n",
    )
    .unwrap();
    let crate_info = workspace::CrateInfo {
        name: "x".to_string(),
        version: semver::Version::new(0, 1, 0),
        manifest_dir: dir.path().to_path_buf(),
        manifest_path: manifest,
        is_publishable: true,
    };
    // Even with a breaking commit present, no baseline ⇒ neither signal runs.
    let commits = [pc(Some("feat"), None, true, "break", None)];
    let r = analyze(dir.path(), &crate_info, None, &commits);
    assert_eq!(
        r,
        BreakingReport {
            has_breaking: false,
            reason: Some("first release — no prior version to compare".to_string()),
            api_status: ApiStatus::Skipped,
        }
    );
}

// analyze: API Skipped paths combine with intent only (no real tool run)

#[test]
fn analyze_cdylib_skips_api_and_uses_intent() {
    // cdylib crate → API Skipped; intent footer drives has_breaking. No tool run.
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname=\"w\"\nversion=\"0.1.0\"\n[lib]\ncrate-type=[\"cdylib\"]\n",
    )
    .unwrap();
    let crate_info = workspace::CrateInfo {
        name: "lumina-node-wasm".to_string(),
        version: semver::Version::new(0, 2, 0),
        manifest_dir: dir.path().to_path_buf(),
        manifest_path: manifest,
        is_publishable: true,
    };
    let baseline = semver::Version::new(0, 1, 0);
    let commits = [pc(
        Some("feat"),
        None,
        false,
        "x",
        Some("dropped wasm export"),
    )];
    let r = analyze(dir.path(), &crate_info, Some(&baseline), &commits);
    assert_eq!(r.api_status, ApiStatus::Skipped);
    assert!(r.has_breaking);
    assert_eq!(r.reason.as_deref(), Some("dropped wasm export"));
}

#[test]
fn analyze_no_changes_skips_api_and_is_not_breaking() {
    // Library crate but no commits since baseline → API Skipped, no intent → clean.
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname=\"x\"\nversion=\"0.2.0\"\n[lib]\ncrate-type=[\"lib\"]\n",
    )
    .unwrap();
    let crate_info = workspace::CrateInfo {
        name: "lumina-utils".to_string(),
        version: semver::Version::new(0, 2, 0),
        manifest_dir: dir.path().to_path_buf(),
        manifest_path: manifest,
        is_publishable: true,
    };
    let baseline = semver::Version::new(0, 1, 0);
    let r = analyze(dir.path(), &crate_info, Some(&baseline), &[]);
    assert_eq!(r.api_status, ApiStatus::Skipped);
    assert!(!r.has_breaking);
    assert_eq!(r.reason, None);
}
