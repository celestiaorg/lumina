//! Release-PR **title** and **body** generation.
//!
//! The body is a **Tera template** ported from release-plz
//! (`release_plz_core::pr::DEFAULT_PR_BODY_TEMPLATE`), rendered over one
//! [`ReleaseInfo`] per package — a faithful mirror of release-plz's PR body, minus
//! its tool-attribution footer. It emits the `## 🤖 New release` bullet list (with
//! inline API-status markers), per-package breaking-change blocks, and a
//! collapsible `<blockquote>` changelog, degrading gracefully under GitHub's
//! [`MAX_PR_BODY`] character cap exactly as release-plz does.
//!
//! Marker semantics differ from release-plz by design: the `⚠ / ✓` marker is driven
//! by toy-kv's **combined** breaking flag (API *or* intent), not the API-only
//! `cargo-semver-checks` result — see the caller ([`crate::breaking`]).
//!
//! Branch naming and the GitHub API call live in the orchestrator / `forge` module.

use anyhow::Context;
use serde::Serialize;

/// GitHub's cap on PR-body length, in **characters**. Enforced on
/// `str::chars().count()`, mirroring release-plz's `MAX_BODY_LEN`.
pub const MAX_PR_BODY: usize = 65_536;

/// The PR-body template, ported from release-plz's `DEFAULT_PR_BODY_TEMPLATE`
/// (same layout: `## 🤖 New release` bullets, per-package breaking blocks, and a
/// collapsible `<blockquote>` changelog) with two adaptations: the trailing
/// `---` / "generated with release-plz" attribution footer is removed (branding),
/// and the changelog section is gated on a precomputed `has_changelog` flag +
/// inlined loop instead of release-plz's `{% set changes %}…{% endset %}` capture
/// block, which Tera 1.x does not support. The rendered output is identical.
const DEFAULT_PR_BODY_TEMPLATE: &str = r#"
## 🤖 New release
{% for release in releases %}
* `{{ release.package }}`: {% if release.previous_version and release.previous_version != release.next_version %}{{ release.previous_version }} -> {% endif %}{{ release.next_version }}{% if release.semver_check == "incompatible" %} (⚠ API breaking changes){% elif release.semver_check == "compatible" %} (✓ API compatible changes){% endif %}
{%- endfor %}
{%- for release in releases %}{% if release.breaking_changes %}

### ⚠ `{{ release.package }}` breaking changes

```text
{{ release.breaking_changes }}
```{% endif %}{% endfor %}
{% if has_changelog %}
<details><summary><i><b>Changelog</b></i></summary><p>
{%- for release in releases %}
{%- if release.changelog %}{% if releases | length > 1 %}
## `{{ release.package }}`
{% endif %}
<blockquote>

{% if release.title %}## {{ release.title }}
{% endif %}
{{ release.changelog }}
</blockquote>{% endif %}
{% endfor %}
</p></details>
{% endif %}"#;

/// One record per workspace package, assembled by the prepare command before the
/// PR body is built.
///
/// All per-package data arrives pre-computed: `previous_version` / `new_version`
/// are validated version strings (M3), `breaking` / `breaking_reason` come from the
/// breaking-change diagnostic (M10), and `title` / `changelog_body` come from the
/// changelog step (M9). This module renders them verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageReport {
    /// Package name, e.g. `"toy-kv-utils"`. Rendered inside backticks.
    pub name: String,
    /// Last published/tagged version, or empty for a first release (which
    /// suppresses the `prev -> next` arrow, mirroring release-plz).
    pub previous_version: String,
    /// The single workspace version being released (same across all records).
    pub new_version: String,
    /// The new changelog entry's heading text **without** the `## ` prefix (e.g.
    /// `"[1.2.0] - 2026-06-14"`); empty when there is no entry.
    pub title: String,
    /// Diagnostic breaking-change flag (API *or* intent). Drives the `⚠ / ✓` marker
    /// and the breaking-detail block. Never influences the version.
    pub breaking: bool,
    /// Human-readable breaking reason, emitted verbatim inside a fenced `text`
    /// block. Only used when `breaking` is `true`.
    pub breaking_reason: String,
    /// Body-only changelog entry (no `## [..]` heading), possibly empty. Embedded
    /// in the collapsible Changelog section when non-empty.
    pub changelog_body: String,
}

/// The per-release context object the template renders over — the toy-kv twin of
/// release-plz's `ReleaseInfo`. Field names are the template's contract.
#[derive(Debug, Serialize)]
struct ReleaseInfo {
    package: String,
    /// New-entry heading (no `## `); `None` suppresses the in-blockquote heading.
    title: Option<String>,
    /// Body-only changelog notes; `None` omits this package's changelog block.
    changelog: Option<String>,
    /// Empty string suppresses the `prev ->` arrow.
    previous_version: String,
    next_version: String,
    /// `None` omits the breaking-changes block.
    breaking_changes: Option<String>,
    /// `"incompatible"` → `⚠`, `"compatible"` → `✓` (driven by the combined flag).
    semver_check: String,
}

/// Map a caller-supplied [`PackageReport`] into the template's [`ReleaseInfo`].
///
/// Empty `changelog_body` ⇒ no changelog block (and no title); empty
/// `previous_version` ⇒ no version arrow; the combined `breaking` flag selects the
/// `incompatible`/`compatible` marker and gates the breaking-changes block.
fn to_release_info(r: &PackageReport) -> ReleaseInfo {
    let changelog = {
        let trimmed = r.changelog_body.trim_matches('\n');
        (!trimmed.trim().is_empty()).then(|| trimmed.to_string())
    };
    let title = match &changelog {
        Some(_) if !r.title.trim().is_empty() => Some(r.title.clone()),
        _ => None,
    };
    let breaking_changes = (r.breaking && !r.breaking_reason.trim().is_empty())
        .then(|| r.breaking_reason.trim_end_matches('\n').to_string());
    let semver_check = if r.breaking {
        "incompatible"
    } else {
        "compatible"
    };

    ReleaseInfo {
        package: r.name.clone(),
        title,
        changelog,
        previous_version: r.previous_version.clone(),
        next_version: r.new_version.clone(),
        breaking_changes,
        semver_check: semver_check.to_string(),
    }
}

/// PR title: `chore: release v<version>`, where `<version>` is the `new_version`
/// of the first report.
///
/// On an empty slice the version segment is empty (`"chore: release v"`); the
/// caller is expected to pass at least one record.
pub fn title(reports: &[PackageReport]) -> String {
    let version = reports
        .first()
        .map(|r| r.new_version.as_str())
        .unwrap_or("");
    format!("chore: release v{version}")
}

/// Render the release-plz PR body over `reports`, with release-plz's length-safety
/// degradation applied so the result never exceeds [`MAX_PR_BODY`] characters:
///
/// 1. full render; if within the cap, use it;
/// 2. otherwise drop `changelog` + `title` from every release and re-render
///    (keeps the summary + breaking blocks, sheds the bulky changelog);
/// 3. `trim_pr_body` hard-truncates at a UTF-8 char boundary as a last resort.
pub fn body(reports: &[PackageReport]) -> anyhow::Result<String> {
    let mut releases: Vec<ReleaseInfo> = reports.iter().map(to_release_info).collect();

    // Step 1: full render.
    let full = render_template(&releases)?;
    if full.chars().count() <= MAX_PR_BODY {
        return Ok(full);
    }

    // Step 2: shed the bulky changelog (drop `changelog` + `title`) and re-render.
    // NOTE: release-plz's own code trims the *first* render before this length
    // check, so this degradation never actually fires there — it hard-truncates
    // instead. We keep the intended graceful behavior.
    for release in &mut releases {
        release.changelog = None;
        release.title = None;
    }
    let without_changelog = render_template(&releases)?;

    // Step 3: last resort — hard-truncate at a UTF-8 char boundary.
    Ok(trim_pr_body(without_changelog))
}

/// Render [`DEFAULT_PR_BODY_TEMPLATE`] against `releases` with a fresh Tera engine
/// (matching release-plz's `render_template`).
fn render_template(releases: &[ReleaseInfo]) -> anyhow::Result<String> {
    let has_changelog = releases.iter().any(|r| r.changelog.is_some());
    let mut tera = tera::Tera::default();
    tera.add_raw_template("pr_body", DEFAULT_PR_BODY_TEMPLATE)
        .context("failed to register the PR body template")?;
    let mut context = tera::Context::new();
    context.insert("releases", releases);
    context.insert("has_changelog", &has_changelog);
    tera.render("pr_body", &context)
        .context("failed to render the PR body")
}

/// Hard-truncate at a valid UTF-8 char boundary if still over the cap (mirrors
/// release-plz's `trim_pr_body`). A grapheme cluster may be split, but the result
/// is always valid UTF-8; no truncation marker is appended.
fn trim_pr_body(body: String) -> String {
    if body.chars().count() > MAX_PR_BODY {
        body.chars().take(MAX_PR_BODY).collect()
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a report with sensible defaults; override the fields each test cares
    /// about.
    fn report(name: &str, prev: &str, new: &str, breaking: bool) -> PackageReport {
        PackageReport {
            name: name.to_string(),
            previous_version: prev.to_string(),
            new_version: new.to_string(),
            title: String::new(),
            breaking,
            breaking_reason: String::new(),
            changelog_body: String::new(),
        }
    }

    // --- title ----------------------------------------------------------------

    #[test]
    fn title_uses_first_record_new_version() {
        let reports = vec![
            report("toy-kv-utils", "0.1.0", "0.2.0", false),
            report("toy-kv", "0.1.0", "0.2.0", false),
        ];
        assert_eq!(title(&reports), "chore: release v0.2.0");
    }

    #[test]
    fn title_empty_slice_has_no_version() {
        assert_eq!(title(&[]), "chore: release v");
    }

    // --- summary: 🤖 bullet list + inline markers -----------------------------

    #[test]
    fn summary_is_robot_bullet_list_with_markers() {
        let reports = vec![
            report("toy-kv-utils", "0.1.0", "0.2.0", false),
            report("toy-kv", "0.1.0", "0.2.0", true),
        ];
        let b = body(&reports).unwrap();

        assert!(b.contains("## 🤖 New release"));
        assert!(b.contains("* `toy-kv-utils`: 0.1.0 -> 0.2.0 (✓ API compatible changes)"));
        assert!(b.contains("* `toy-kv`: 0.1.0 -> 0.2.0 (⚠ API breaking changes)"));
        // Mirrors release-plz: no table.
        assert!(!b.contains("| Package | Version | Status |"));
    }

    #[test]
    fn arrow_omitted_when_no_previous_or_unchanged() {
        // First release: empty previous ⇒ just the new version, no arrow.
        let first = report("new-pkg", "", "0.1.0", false);
        // prev == next ⇒ no arrow either.
        let same = report("same-pkg", "0.2.0", "0.2.0", false);
        let b = body(&[first, same]).unwrap();

        assert!(b.contains("* `new-pkg`: 0.1.0 (✓ API compatible changes)"));
        assert!(!b.contains("-> 0.1.0"));
        assert!(b.contains("* `same-pkg`: 0.2.0 (✓ API compatible changes)"));
    }

    // --- breaking-change blocks -----------------------------------------------

    #[test]
    fn breaking_block_only_for_breaking_packages_fenced() {
        let mut breaks = report("toy-kv-utils", "0.1.0", "0.2.0", true);
        breaks.breaking_reason = "removed pub fn `from_str`".to_string();
        let ok = report("toy-kv", "0.1.0", "0.2.0", false);

        let b = body(&[breaks, ok]).unwrap();
        assert!(b.contains("### ⚠ `toy-kv-utils` breaking changes"));
        assert!(b.contains("```text\nremoved pub fn `from_str`\n```"));
        assert!(!b.contains("`toy-kv` breaking changes"));
    }

    #[test]
    fn no_breaking_block_when_reason_empty_even_if_flagged() {
        // Breaking flag set but no reason text ⇒ marker shows, but no detail block.
        let r = report("x", "0.1.0", "0.2.0", true);
        let b = body(&[r]).unwrap();
        assert!(b.contains("(⚠ API breaking changes)"));
        assert!(!b.contains("breaking changes\n\n```text"));
    }

    // --- collapsible changelog (blockquote, headings) -------------------------

    #[test]
    fn changelog_omitted_when_no_content() {
        let reports = vec![
            report("a", "0.1.0", "0.2.0", false),
            report("b", "0.1.0", "0.2.0", false),
        ];
        let b = body(&reports).unwrap();
        assert!(!b.contains("<details>"));
        assert!(!b.contains("Changelog"));
    }

    #[test]
    fn changelog_single_package_omits_package_heading_uses_blockquote() {
        // A single-release PR: mirrors release-plz's `releases | length > 1` gate,
        // so no per-package `## \`name\`` heading.
        let mut r = report("only", "0.1.0", "0.2.0", false);
        r.title = "[0.2.0] - 2026-06-14".to_string();
        r.changelog_body = "### Added\n\n- a thing".to_string();
        let b = body(&[r]).unwrap();

        assert!(b.contains("<details><summary><i><b>Changelog</b></i></summary><p>"));
        assert!(b.contains("<blockquote>"));
        // Entry heading rendered inside the blockquote.
        assert!(b.contains("## [0.2.0] - 2026-06-14"));
        assert!(b.contains("- a thing"));
        // Single release ⇒ no `## \`name\`` package heading.
        assert!(!b.contains("## `only`"));
    }

    #[test]
    fn changelog_multi_package_uses_package_headings_and_skips_empty() {
        let mut a = report("toy-kv-utils", "0.1.0", "0.2.0", false);
        a.changelog_body = "### Added\n\n- utils thing".to_string();
        let mut c = report("toy-kv", "0.1.0", "0.2.0", false);
        c.changelog_body = "### Added\n\n- core thing".to_string();
        let empty = report("toy-kv-wasm", "", "0.2.0", false);
        let b = body(&[a, empty, c]).unwrap();

        assert!(b.contains("## `toy-kv-utils`"));
        assert!(b.contains("## `toy-kv`"));
        assert!(b.contains("- utils thing"));
        assert!(b.contains("- core thing"));
        // Empty-changelog package gets no heading and no blockquote content.
        assert!(!b.contains("## `toy-kv-wasm`"));
    }

    // --- no branding ----------------------------------------------------------

    #[test]
    fn body_has_no_release_plz_branding() {
        let mut r = report("toy-kv", "0.1.0", "0.2.0", false);
        r.changelog_body = "### Added\n\n- a thing".to_string();
        let b = body(&[r]).unwrap();
        assert!(!b.to_lowercase().contains("release-plz"));
        assert!(!b.contains("This PR was generated"));
    }

    // --- length safety --------------------------------------------------------

    #[test]
    fn length_safety_normal_input_keeps_changelog() {
        let mut r = report("toy-kv", "0.1.0", "0.2.0", false);
        r.changelog_body = "### Added\n\n- a normal change".to_string();
        let b = body(&[r]).unwrap();
        assert!(b.contains("<details>"));
        assert!(b.chars().count() <= MAX_PR_BODY);
    }

    #[test]
    fn length_safety_oversized_changelog_drops_details_keeps_summary() {
        let mut r = report("toy-kv", "0.1.0", "0.2.0", false);
        r.changelog_body = "### Added\n".to_string() + &"- x\n".repeat(20_000);
        let b = body(&[r]).unwrap();

        assert!(b.chars().count() <= MAX_PR_BODY);
        // Step 2: the <details> changelog is dropped...
        assert!(!b.contains("<details>"));
        // ...but the summary bullet is kept.
        assert!(b.contains("## 🤖 New release"));
        assert!(b.contains("* `toy-kv`: 0.1.0 -> 0.2.0"));
    }

    #[test]
    fn length_safety_pathological_input_hard_truncates_at_char_boundary() {
        // Multi-byte breaking reason (un-droppable) large enough to force step 3.
        let mut r = report("toy-kv", "0.1.0", "0.2.0", true);
        r.breaking_reason = "⚠".repeat(70_000);
        let b = body(&[r]).unwrap();

        assert!(b.chars().count() <= MAX_PR_BODY, "over cap");
        // Valid UTF-8, no multi-byte char split.
        assert_eq!(String::from_utf8(b.clone().into_bytes()).unwrap(), b);
    }

    #[test]
    fn body_empty_slice_is_well_formed_and_does_not_panic() {
        let b = body(&[]).unwrap();
        assert!(b.contains("## 🤖 New release"));
        assert!(!b.contains("<details>"));
        assert!(b.chars().count() <= MAX_PR_BODY);
    }
}
