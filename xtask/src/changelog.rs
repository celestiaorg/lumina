//! Per-package `CHANGELOG.md` generation via the `git-cliff-core` crate.
//!
//! git-cliff renders the entry; this module adds the workspace-specific
//! responsibilities it does not handle:
//!
//! 1. Path-scoped commit selection ([`crate::commit::collect`]) so each package's
//!    changelog covers only its own directory.
//! 2. Header-preserving splice ([`prepend_into`]) that keeps the hand-edited
//!    `# Changelog` header (through `## [Unreleased]`) intact and inserts new
//!    entries newest-on-top.
//!
//! The version is always caller-supplied; nothing here computes one.

use anyhow::{Context, bail};
use git_cliff_core::{
    changelog::Changelog as GitCliffChangelog,
    commit::Commit as CliffCommit,
    config::{Bump, ChangelogConfig, CommitParser, Config, GitConfig, RemoteConfig},
    release::Release,
};
use regex::Regex;

use crate::commit;
use crate::workspace;

/// The two artifacts produced for one package by [`generate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageChangelog {
    /// Full rendered entry (heading + sections + bullets) for `CHANGELOG.md`.
    pub file_entry: String,
    /// The same sections + bullets without the `## [..]` heading, for the PR body.
    pub body_only: String,
}

/// Standard Keep-a-Changelog header, synthesized when none can be preserved. Ends
/// on the `## [Unreleased]` line; the new entry is spliced directly after it.
const DEFAULT_HEADER: &str = "\
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]";

/// git-cliff body template (Keep a Changelog): a dated `## [version]` heading, then
/// one `### <Group>` section per non-empty group with a `- <msg>` bullet per commit.
const BODY_TEMPLATE: &str = r#"
## [{{ version }}] - {{ timestamp | date(format="%Y-%m-%d") }}
{% for group, commits in commits | group_by(attribute="group") %}
### {{ group | upper_first }}

{% for commit in commits %}
{%- if commit.scope -%}
- *({{ commit.scope }})* {% if commit.breaking %}[**breaking**] {% endif %}{{ commit.message }}
{% else -%}
- {% if commit.breaking %}[**breaking**] {% endif %}{{ commit.message }}
{% endif -%}
{% endfor -%}
{% endfor %}"#;

/// Render the entry (git-cliff), splice it into the package's `CHANGELOG.md`, and
/// return the entry (with heading) plus the body-only copy.
///
/// `commits` is the package's path-scoped set; `date` is `YYYY-MM-DD` (pass
/// [`today_utc`] for the default); `version` is placed verbatim into the heading.
pub fn generate(
    crate_info: &workspace::CrateInfo,
    version: &str,
    date: &str,
    commits: &[commit::Commit],
) -> anyhow::Result<PackageChangelog> {
    let rendered = render_entry(version, date, commits)
        .with_context(|| format!("rendering changelog entry for `{}`", crate_info.name))?;

    let changelog_path = crate_info.manifest_dir.join("CHANGELOG.md");
    let existing = match std::fs::read_to_string(&changelog_path) {
        Ok(contents) => Some(contents),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(err)
                .with_context(|| format!("reading existing changelog {changelog_path:?}"));
        }
    };

    let new_contents = prepend_into(existing.as_deref(), &rendered.file_entry);
    std::fs::write(&changelog_path, new_contents)
        .with_context(|| format!("writing changelog {changelog_path:?}"))?;

    Ok(rendered)
}

/// Render one release entry with git-cliff from raw [`commit::Commit`]s. Returns the
/// entry (with heading) as `file_entry` and the same entry minus its heading as
/// `body_only`. Deterministic; no I/O.
pub fn render_entry(
    version: &str,
    date: &str,
    commits: &[commit::Commit],
) -> anyhow::Result<PackageChangelog> {
    let config = kac_config();
    let git_config = &config.git;

    let processed: Vec<CliffCommit> = commits
        .iter()
        .map(cliff_commit)
        .filter_map(|c| c.process(git_config).ok())
        .collect();

    let timestamp = date_to_timestamp(date)?;
    let release = Release {
        version: Some(version.to_string()),
        commits: processed,
        timestamp: Some(timestamp),
        ..Default::default()
    };

    let changelog = GitCliffChangelog::new(vec![release], config.clone(), None)
        .context("error while building git-cliff changelog")?;

    let mut out = Vec::new();
    changelog
        .generate(&mut out)
        .context("git-cliff failed to render the changelog entry")?;
    let entry = String::from_utf8(out).context("git-cliff produced non-UTF-8 output")?;
    let entry = entry.trim_matches('\n');

    // git-cliff renders nothing for a release with no commits, but an unchanged
    // package still needs a dated heading; synthesize the bare heading here.
    let file_entry = if entry.is_empty() {
        format!("## [{version}] - {date}\n")
    } else {
        format!("{entry}\n")
    };
    let body_only = body_from_entry(&file_entry);

    Ok(PackageChangelog {
        file_entry,
        body_only,
    })
}

/// Build a git-cliff [`CliffCommit`]. The message is subject plus body so
/// conventional-commit parsing sees `BREAKING CHANGE:` footers.
fn cliff_commit(c: &commit::Commit) -> CliffCommit<'static> {
    let message = if c.body.trim().is_empty() {
        c.subject.clone()
    } else {
        format!("{}\n\n{}", c.subject, c.body)
    };
    CliffCommit::new(c.hash.clone(), message)
}

/// Strip the leading `## [..]` heading (and the blank line after it) to get the
/// body-only sections. Returns the entry unchanged if it has no such heading.
fn body_from_entry(entry: &str) -> String {
    match entry.split_once('\n') {
        Some((first, rest)) if first.trim_start().starts_with("## [") => {
            rest.trim_start_matches('\n').to_string()
        }
        _ => entry.to_string(),
    }
}

/// The Keep-a-Changelog git-cliff [`Config`]: no header (we splice our own),
/// [`BODY_TEMPLATE`] body, `render_always`, and the KaC commit parsers.
fn kac_config() -> Config {
    Config {
        changelog: ChangelogConfig {
            header: None,
            body: BODY_TEMPLATE.to_string(),
            footer: None,
            trim: true,
            render_always: true,
            ..Default::default()
        },
        git: GitConfig {
            conventional_commits: true,
            filter_unconventional: false,
            filter_commits: false,
            split_commits: false,
            protect_breaking_commits: false,
            commit_parsers: kac_commit_parsers(),
            commit_preprocessors: vec![],
            link_parsers: vec![],
            sort_commits: "newest".to_string(),
            ..Default::default()
        },
        remote: RemoteConfig::default(),
        bump: Bump::default(),
    }
}

/// A git-cliff [`CommitParser`] mapping messages matching `regex` into `group`.
fn commit_parser(regex: &str, group: &str) -> CommitParser {
    CommitParser {
        message: Regex::new(regex).ok(),
        body: None,
        group: Some(group.to_string()),
        default_scope: None,
        scope: None,
        skip: None,
        field: None,
        pattern: None,
        sha: None,
        footer: None,
    }
}

/// Commit-type → group parsers based on
/// [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
fn kac_commit_parsers() -> Vec<CommitParser> {
    vec![
        commit_parser("^feat", "added"),
        commit_parser("^changed", "changed"),
        commit_parser("^deprecated", "deprecated"),
        commit_parser("^removed", "removed"),
        commit_parser("^fix", "fixed"),
        commit_parser("^security", "security"),
        commit_parser(".*", "other"),
    ]
}

/// Splice a new `entry` into an existing `CHANGELOG.md`, preserving everything
/// through the `## [Unreleased]` line and inserting the entry beneath it
/// (newest-on-top). Falls back to the default header when none is recognizable.
/// Always ends with a single trailing newline.
pub fn prepend_into(existing: Option<&str>, entry: &str) -> String {
    match existing.and_then(parse_header) {
        Some((header, old_body)) => compose(header, entry, old_body),
        None => compose(DEFAULT_HEADER, entry, ""),
    }
}

/// Concatenate `{header}\n\n{entry}{old_body}` with exactly one trailing newline.
/// `header` ends on the `## [Unreleased]` line (no trailing newline); `old_body`
/// keeps its own leading newlines.
fn compose(header: &str, entry: &str, old_body: &str) -> String {
    let mut out = String::new();
    out.push_str(header.trim_end_matches('\n'));
    out.push_str("\n\n");
    out.push_str(entry.trim_end_matches('\n'));
    out.push('\n');
    let body = old_body.trim_start_matches('\n');
    if !body.is_empty() {
        out.push('\n');
        out.push_str(body);
    }
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Parse `existing` into `(header, old_body)`, the header running from the start
/// through (and including) the `## [Unreleased]` line. Requires a case-insensitive
/// `# Changelog` title and an `## [Unreleased]` line; otherwise returns `None`.
fn parse_header(existing: &str) -> Option<(&str, &str)> {
    if existing.trim().is_empty() {
        return None;
    }

    let has_title = existing
        .lines()
        .any(|line| line.trim().eq_ignore_ascii_case("# changelog"));
    if !has_title {
        return None;
    }

    // Locate the `## [Unreleased]` line and split just after it.
    let mut offset = 0usize;
    for line in existing.split_inclusive('\n') {
        offset += line.len();
        let trimmed = line.strip_suffix('\n').unwrap_or(line).trim();
        if is_unreleased_line(trimmed) {
            let header_end = offset; // include this line (and its newline) in header
            let header = &existing[..header_end];
            let old_body = &existing[header_end..];
            return Some((header, old_body));
        }
    }
    None
}

/// `true` for the `## [Unreleased]` marker line (case-insensitive).
fn is_unreleased_line(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower == "## [unreleased]"
}

/// Convert `YYYY-MM-DD` into a Unix timestamp at midnight UTC for git-cliff's
/// `Release::timestamp`; git-cliff's `date` filter renders it back, so the date
/// round-trips.
fn date_to_timestamp(date: &str) -> anyhow::Result<i64> {
    let mut parts = date.split('-');
    let (Some(y), Some(m), Some(d), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("expected date as YYYY-MM-DD, got {date:?}");
    };
    let year: i64 = y.parse().with_context(|| format!("bad year in {date:?}"))?;
    let month: u32 = m
        .parse()
        .with_context(|| format!("bad month in {date:?}"))?;
    let day: u32 = d.parse().with_context(|| format!("bad day in {date:?}"))?;
    Ok(days_from_civil(year, month, day) * 86_400)
}

/// Days since the Unix epoch for a civil `(year, month, day)` in the proleptic
/// Gregorian calendar. Howard Hinnant's `days_from_civil`.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 }.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The current UTC date formatted `YYYY-MM-DD`.
pub fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        // Clock before the epoch is implausible; default to the epoch date.
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Convert days since the Unix epoch to a civil `(year, month, day)` in the
/// proleptic Gregorian calendar. Inverse of [`days_from_civil`].
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    // Shift the epoch to 0000-03-01 so leap days fall at the end of the era.
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::Commit;

    /// Build a `Commit` with the given subject and an empty body.
    fn subj(subject: &str) -> Commit {
        Commit {
            hash: "deadbeef".into(),
            subject: subject.into(),
            body: String::new(),
        }
    }

    /// feat + fix commits render a dated heading with `### Added` and `### Fixed`.
    #[test]
    fn render_entry_groups_and_headings() {
        let commits = [
            subj("feat: add a feature"),
            subj("fix: a fix"),
            subj("fix: another fix"),
        ];
        let out = render_entry("1.2.0", "2026-06-14", &commits).unwrap();

        assert!(
            out.file_entry.starts_with("## [1.2.0] - 2026-06-14"),
            "entry starts with dated heading: {:?}",
            out.file_entry
        );
        assert!(out.file_entry.contains("### Added"));
        assert!(out.file_entry.contains("- add a feature"));
        assert!(out.file_entry.contains("### Fixed"));
        assert!(out.file_entry.contains("- a fix"));
        assert!(out.file_entry.contains("- another fix"));
        assert!(out.file_entry.ends_with('\n'));
        // Added comes before Fixed.
        assert!(out.file_entry.find("### Added") < out.file_entry.find("### Fixed"));
    }

    /// `body_only` has the sections/bullets but no `## [..]` heading.
    #[test]
    fn render_entry_body_only_has_no_version_heading() {
        let out = render_entry("9.9.9", "2026-01-01", &[subj("feat: a feature")]).unwrap();
        assert!(
            !out.body_only.contains("## ["),
            "body-only must not contain a version heading: {:?}",
            out.body_only
        );
        assert!(out.body_only.contains("### Added"));
        assert!(out.body_only.contains("- a feature"));
    }

    /// A non-conventional subject falls into the `Other` group.
    #[test]
    fn render_entry_non_conventional_goes_to_other() {
        let out = render_entry("0.1.0", "2026-06-14", &[subj("just a plain message")]).unwrap();
        assert!(out.file_entry.contains("### Other"));
        assert!(out.file_entry.contains("- just a plain message"));
    }

    /// No commits still yields a dated heading; body-only is empty.
    #[test]
    fn render_entry_no_commits_yields_bare_heading() {
        let out = render_entry("0.1.0", "2026-06-14", &[]).unwrap();
        assert_eq!(out.file_entry, "## [0.1.0] - 2026-06-14\n");
        assert!(out.body_only.is_empty());
    }

    /// The header is preserved, the new entry spliced beneath it, and the old body
    /// retained below.
    #[test]
    fn prepend_preserves_header_and_old_body() {
        let existing = "\
# Changelog

Hand-edited preamble that must survive.

## [Unreleased]

## [1.0.0] - 2025-01-01

### Added

- old entry
";
        let entry = render_entry("1.1.0", "2026-06-14", &[subj("fix: new fix")])
            .unwrap()
            .file_entry;
        let out = prepend_into(Some(existing), &entry);

        // Header preserved verbatim through `## [Unreleased]`.
        assert!(out.starts_with(
            "# Changelog\n\nHand-edited preamble that must survive.\n\n## [Unreleased]\n"
        ));
        // New entry sits beneath the header, before the old entry.
        let new_idx = out.find("## [1.1.0] - 2026-06-14").unwrap();
        let old_idx = out.find("## [1.0.0] - 2025-01-01").unwrap();
        assert!(new_idx < old_idx, "new entry must precede the old one");
        assert!(out.contains("- old entry"));
        let unrel_idx = out.find("## [Unreleased]").unwrap();
        assert!(unrel_idx < new_idx);
    }

    /// The `# Changelog` title is matched case-insensitively.
    #[test]
    fn prepend_matches_title_case_insensitively() {
        let existing =
            "# CHANGELOG\n\n## [Unreleased]\n\n## [0.9.0] - 2025-01-01\n\n### Added\n\n- prior\n";
        let entry = render_entry("1.0.0", "2026-06-14", &[]).unwrap().file_entry;
        let out = prepend_into(Some(existing), &entry);
        assert!(out.starts_with("# CHANGELOG\n"));
        assert!(out.contains("## [1.0.0] - 2026-06-14"));
        assert!(out.contains("- prior"));
    }

    /// Missing file falls back to the default header + entry.
    #[test]
    fn prepend_fallback_default_header_when_none() {
        let entry = render_entry("0.1.0", "2026-06-14", &[subj("feat: initial")])
            .unwrap()
            .file_entry;
        let out = prepend_into(None, &entry);
        assert!(out.starts_with("# Changelog\n\nAll notable changes"));
        assert!(out.contains("## [Unreleased]"));
        let unrel = out.find("## [Unreleased]").unwrap();
        let entry_idx = out.find("## [0.1.0] - 2026-06-14").unwrap();
        assert!(unrel < entry_idx);
        assert!(out.contains("- initial"));
    }

    /// Whitespace-only existing content falls back to the default.
    #[test]
    fn prepend_fallback_on_empty_existing() {
        let entry = render_entry("0.1.0", "2026-06-14", &[]).unwrap().file_entry;
        let out = prepend_into(Some("   \n\n"), &entry);
        assert!(out.starts_with("# Changelog\n\nAll notable changes"));
    }

    /// A file lacking a `# Changelog` title falls back to the default.
    #[test]
    fn prepend_fallback_when_no_title() {
        let existing = "## [Unreleased]\n\n## [0.1.0]\n";
        let entry = render_entry("1.0.0", "2026-06-14", &[]).unwrap().file_entry;
        let out = prepend_into(Some(existing), &entry);
        assert!(out.starts_with("# Changelog\n\nAll notable changes"));
    }

    /// Output always ends with exactly one trailing newline.
    #[test]
    fn prepend_single_trailing_newline() {
        let entry = render_entry("1.0.0", "2026-06-14", &[]).unwrap().file_entry;
        let out = prepend_into(None, &entry);
        assert!(out.ends_with('\n'));
        assert!(!out.ends_with("\n\n"));
    }

    /// `date_to_timestamp` lands on midnight UTC and `civil_from_days` inverts it.
    #[test]
    fn date_to_timestamp_round_trips() {
        for date in ["1970-01-01", "2000-02-29", "2022-01-01", "2026-06-14"] {
            let ts = date_to_timestamp(date).unwrap();
            assert_eq!(ts % 86_400, 0, "midnight UTC for {date}");
            let (y, m, d) = civil_from_days(ts / 86_400);
            assert_eq!(format!("{y:04}-{m:02}-{d:02}"), date);
        }
    }

    #[test]
    fn date_to_timestamp_rejects_malformed() {
        assert!(date_to_timestamp("2026-06").is_err());
        assert!(date_to_timestamp("not-a-date-x").is_err());
    }

    #[test]
    fn civil_from_days_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1)); // Unix epoch
        assert_eq!(civil_from_days(31 + 28), (1970, 3, 1));
        assert_eq!(civil_from_days(18_993), (2022, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29)); // leap day
    }

    #[test]
    fn today_utc_is_well_formed() {
        let s = today_utc();
        assert_eq!(s.len(), 10);
        let parts: Vec<&str> = s.split('-').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].len(), 4);
        assert_eq!(parts[1].len(), 2);
        assert_eq!(parts[2].len(), 2);
        assert!(parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit())));
    }

    // Pathspec derivation is tested in `crate::commit`.
}
