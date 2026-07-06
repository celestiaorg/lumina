//! Per-package `CHANGELOG.md` generation, rendered by [`git-cliff-core`].
//!
//! This module used to hand-roll conventional-commit grouping and markdown
//! rendering. It now delegates that work to [`git-cliff-core`] — the same engine
//! release-plz uses — while keeping the two toy-kv-specific responsibilities that
//! git-cliff does *not* do:
//!
//! 1. **Path-scoped commit selection** — [`crate::commit::collect`] shells out to
//!    `git log -- <package dir>` so each package's changelog covers only its own
//!    directory. git-cliff renders whatever commit set we hand it; it does not
//!    scope by directory.
//! 2. **Header-preserving splice** — [`prepend_into`] keeps a hand-edited
//!    `# Changelog` header (through `## [Unreleased]`) intact across releases,
//!    inserting the new entry beneath it (newest-on-top).
//!
//! The **version is always caller-supplied** ([`generate`]'s `version` argument is
//! placed verbatim into the git-cliff `Release`); nothing here computes a version.
//!
//! [`git-cliff-core`]: https://crates.io/crates/git-cliff-core

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
    /// The full rendered entry (`## [version] - date` heading + `### Group`
    /// sections + bullets) spliced into `CHANGELOG.md`. Ends with a newline.
    pub file_entry: String,
    /// The same sections + bullets **without** the `## [..]` heading, for the PR
    /// body. Ends with a single trailing newline (empty when there are no groups).
    pub body_only: String,
}

/// The standard Keep-a-Changelog header, synthesized when no header can be
/// preserved. Terminates at the `## [Unreleased]` line; the new entry is spliced
/// directly after it.
const DEFAULT_HEADER: &str = "\
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]";

/// git-cliff body template (Keep a Changelog). Renders the `## [version] - date`
/// heading, then one `### <Group>` section per non-empty group with a `- <msg>`
/// bullet per commit. Mirrors release-plz's default (scope + breaking markers),
/// minus the remote/release-link context we don't populate.
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

/// Top-level per-package generator: render (git-cliff) → splice into the package's
/// `CHANGELOG.md` → return the entry (with heading) and the body-only copy.
///
/// `commits` is the package's path-scoped commit set, collected once by the caller
/// (via [`crate::commit::collect`]) and shared with breaking-change analysis. `date`
/// is explicit (`YYYY-MM-DD`); pass [`today_utc`] for the production default. The
/// `version` is placed verbatim into the entry heading — no version is computed.
///
/// Side effects: one write to `crate_info.manifest_dir/CHANGELOG.md`. No network,
/// no subprocess (the caller already collected the commits).
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

/// Render one release entry with git-cliff from raw [`commit::Commit`]s.
///
/// Converts each collected commit into a git-cliff [`CliffCommit`] (subject + body
/// as the message so `BREAKING CHANGE:` footers are detected), processes it under
/// the Keep-a-Changelog git config, and renders the [`BODY_TEMPLATE`] against a
/// single [`Release`] carrying the caller's `version` and `date`. Returns the entry
/// (with heading) as `file_entry` and the same entry minus its heading as
/// `body_only`.
///
/// Deterministic in (`version`, `date`, commits); performs no I/O.
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

    // git-cliff renders nothing for a release with no commits (even with
    // `render_always`), but we still want a dated heading for an unchanged
    // package. Synthesize the bare heading in that case.
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

/// Build a git-cliff [`CliffCommit`] from a collected commit. The message is the
/// subject plus the body (when present) so conventional-commit parsing sees the
/// full message, including `BREAKING CHANGE:` footers.
fn cliff_commit(c: &commit::Commit) -> CliffCommit<'static> {
    let message = if c.body.trim().is_empty() {
        c.subject.clone()
    } else {
        format!("{}\n\n{}", c.subject, c.body)
    };
    CliffCommit::new(c.hash.clone(), message)
}

/// Strip the leading `## [..]` heading line (and the blank line after it) from a
/// rendered entry, yielding the body-only sections. Returns the entry unchanged if
/// it does not start with a `## [` heading.
fn body_from_entry(entry: &str) -> String {
    match entry.split_once('\n') {
        Some((first, rest)) if first.trim_start().starts_with("## [") => {
            rest.trim_start_matches('\n').to_string()
        }
        _ => entry.to_string(),
    }
}

/// The Keep-a-Changelog git-cliff [`Config`]: no header (we splice our own),
/// [`BODY_TEMPLATE`] for each release, `render_always` so a package with no commits
/// still gets a dated heading, and the KaC commit parsers.
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

/// A single git-cliff [`CommitParser`] mapping commits whose message matches
/// `regex` into `group`.
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

/// Commit parsers based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
/// matching release-plz's mapping.
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

/// Header-preserving splice of a new `entry` into an existing `CHANGELOG.md`.
///
/// Preserves everything from the top of the file through the `## [Unreleased]`
/// line (the `# Changelog` title is matched case-insensitively) and inserts the
/// new entry directly beneath it, ahead of any prior entries (newest-on-top). When
/// the file is missing, empty, or has no recognizable header, the standard
/// Keep-a-Changelog header is synthesized instead. The result always ends with a
/// single trailing newline.
pub fn prepend_into(existing: Option<&str>, entry: &str) -> String {
    match existing.and_then(parse_header) {
        Some((header, old_body)) => compose(header, entry, old_body),
        None => compose(DEFAULT_HEADER, entry, ""),
    }
}

/// Concatenate `{header}\n\n{entry}{old_body}`, normalizing to exactly one
/// trailing newline. `header` carries no trailing newline (it ends on the
/// `## [Unreleased]` line); `old_body` retains its own leading newlines.
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

/// Parse `existing` into `(header, old_body)`, where the header runs from the start
/// of the file through (and including) the `## [Unreleased]` line.
///
/// Requires both a case-insensitive `# Changelog` title line and a `## [Unreleased]`
/// line; returns `None` (no recognizable header) otherwise, so the caller falls
/// back to the default header.
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
        // Strip the trailing newline (if any) for the comparison.
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

/// `true` for the `## [Unreleased]` marker line (case-insensitive on the word).
fn is_unreleased_line(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower == "## [unreleased]"
}

/// Convert a `YYYY-MM-DD` date into a Unix timestamp (seconds) at midnight UTC,
/// for git-cliff's `Release::timestamp`. git-cliff's `date` filter renders it back
/// to `YYYY-MM-DD`, so the round trip preserves the date.
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

/// Days since the Unix epoch (1970-01-01) for a civil `(year, month, day)` in the
/// proleptic Gregorian calendar. Howard Hinnant's `days_from_civil` (std-only).
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

/// The current UTC date formatted `YYYY-MM-DD`. Reads the system clock only.
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

/// Convert a count of days since the Unix epoch (1970-01-01) to a civil
/// `(year, month, day)` in the proleptic Gregorian calendar. Howard Hinnant's
/// `days_from_civil` inverse (std-only).
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

    /// Build a collected `Commit` with the given subject and an empty body.
    fn subj(subject: &str) -> Commit {
        Commit {
            hash: "deadbeef".into(),
            subject: subject.into(),
            body: String::new(),
        }
    }

    // render_entry (git-cliff): grouping, headings, body-only stripping

    /// A `feat:` and two `fix:` commits render a dated heading with `### Added`
    /// and `### Fixed` sections and the descriptions as bullets.
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

    /// `body_only` carries the same sections/bullets with NO `## [..]` heading.
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

    /// A non-conventional subject falls into the `Other` group verbatim.
    #[test]
    fn render_entry_non_conventional_goes_to_other() {
        let out = render_entry("0.1.0", "2026-06-14", &[subj("just a plain message")]).unwrap();
        assert!(out.file_entry.contains("### Other"));
        assert!(out.file_entry.contains("- just a plain message"));
    }

    /// A package with no commits still gets a dated heading; body-only is empty.
    #[test]
    fn render_entry_no_commits_yields_bare_heading() {
        let out = render_entry("0.1.0", "2026-06-14", &[]).unwrap();
        assert_eq!(out.file_entry, "## [0.1.0] - 2026-06-14\n");
        assert!(out.body_only.is_empty());
    }

    // prepend_into: header preservation, fallback default header

    /// The existing header (through `## [Unreleased]`) is preserved, the new entry
    /// is spliced beneath it, and the old body is retained below the new entry.
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

    /// The `# Changelog` title is matched case-insensitively (`# CHANGELOG` counts).
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

    /// Missing file → synthesize the default Keep-a-Changelog header + entry.
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

    /// Empty / whitespace-only existing content also falls back to the default.
    #[test]
    fn prepend_fallback_on_empty_existing() {
        let entry = render_entry("0.1.0", "2026-06-14", &[]).unwrap().file_entry;
        let out = prepend_into(Some("   \n\n"), &entry);
        assert!(out.starts_with("# Changelog\n\nAll notable changes"));
    }

    /// A file lacking a recognizable `# Changelog` title falls back to default.
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

    // date_to_timestamp / civil_from_days: date round-trip

    /// `date_to_timestamp` must land on midnight UTC of the requested day, and
    /// `civil_from_days` must invert it.
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

    // Package pathspec derivation now lives in `crate::commit::pathspec`; see its
    // unit tests there.
}
