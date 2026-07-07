//! Path-scoped git-log collection and Conventional-Commit parsing.
//!
//! [`collect`] gathers the commits touching a directory (via [`crate::gitops::Git`]);
//! [`parse`] turns a [`Commit`] into a [`ParsedCommit`]. A [`Commit`] keeps the hash,
//! subject, and body so one set serves both changelog generation and breaking-change
//! detection (which scans the body for a `BREAKING CHANGE:` footer).

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Result, bail};

use crate::gitops::Git;

/// Field separator emitted by `git log` (`%x1f`, ASCII unit separator).
const FIELD_SEP: char = '\u{1f}';
/// Record separator emitted by `git log` (`%x1e`, ASCII record separator).
const RECORD_SEP: char = '\u{1e}';

/// A raw commit collected from git; everything else is derived from it by [`parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Full commit hash (`%H`).
    pub hash: String,
    /// Commit subject, the first line of the message (`%s`).
    pub subject: String,
    /// Commit body after the subject (`%b`); may be empty. Carries `BREAKING
    /// CHANGE:` footers.
    pub body: String,
}

/// The result of parsing a [`Commit`]. Keep-a-Changelog grouping is not here; the
/// changelog generator groups via git-cliff (see [`crate::changelog`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommit {
    /// The bare, lowercased commit type (e.g. `"feat"`). `None` if non-conventional.
    pub kind: Option<String>,
    /// The scope inside `(...)`, verbatim (e.g. `"core"`). `None` if absent.
    pub scope: Option<String>,
    /// `true` if a `!` breaking marker appeared before the `:` in the type token.
    pub breaking_bang: bool,
    /// Trimmed bullet text: the description after the first `:`, or the whole
    /// subject for a non-conventional commit.
    pub description: String,
    /// Trimmed text following the first `BREAKING CHANGE:` / `BREAKING-CHANGE:`
    /// footer, if any.
    pub breaking_footer: Option<String>,
}

/// Collect the commits belonging to the package rooted at `dir`, newest-first,
/// tracking file identity across directory renames.
///
/// A plain `git log -- <dir>` drops a package's history the moment its directory is
/// renamed, so this lists the current tracked files, unions the commits touching
/// each (following renames via `--follow`), and re-lists them newest-first.
///
/// `range = Some("<ref>..HEAD")` restricts every step; `None` walks all history.
pub fn collect(git: &Git, range: Option<&str>, dir: &str) -> Result<Vec<Commit>> {
    let files = git.tracked_files(dir)?;
    if files.is_empty() {
        return Ok(Vec::new());
    }

    let mut wanted: HashSet<String> = HashSet::new();
    for file in &files {
        for hash in git.commits_touching(range, file)? {
            wanted.insert(hash);
        }
    }

    let ordered = all_commits(git, range)?;
    Ok(ordered
        .into_iter()
        .filter(|c| wanted.contains(&c.hash))
        .collect())
}

/// Derive the `git log` pathspec for a package: `manifest_dir` made relative to
/// `repo_root`. Falls back to `.` when the manifest dir is the root or outside it.
pub fn pathspec(repo_root: &Path, manifest_dir: &Path) -> String {
    match manifest_dir.strip_prefix(repo_root) {
        Ok(rel) if !rel.as_os_str().is_empty() => rel.to_string_lossy().into_owned(),
        _ => ".".to_string(),
    }
}

/// All commits in `range` across the repo, newest-first — the canonical ordering
/// used to sort the unioned per-file commit set.
fn all_commits(git: &Git, range: Option<&str>) -> Result<Vec<Commit>> {
    let pretty = format!("--pretty=format:%H{FIELD_SEP}%s{FIELD_SEP}%b{RECORD_SEP}");
    let stdout = git.log(range, &[&pretty])?;
    parse_log_output(&stdout)
}

/// Split raw `git log` output into [`Commit`]s. Pure; factored out for testing.
fn parse_log_output(stdout: &str) -> Result<Vec<Commit>> {
    let mut commits = Vec::new();
    for record in stdout.split(RECORD_SEP) {
        // Skip the empty record after the trailing separator (and any blank ones).
        if record.trim().is_empty() {
            continue;
        }
        let mut fields = record.splitn(3, FIELD_SEP);
        let hash = fields.next();
        let subject = fields.next();
        let body = fields.next();
        match (hash, subject, body) {
            (Some(hash), Some(subject), Some(body)) => commits.push(Commit {
                hash: hash.trim().to_string(),
                subject: subject.to_string(),
                body: body.trim().to_string(),
            }),
            _ => bail!("malformed `git log` record (expected 3 fields): {record:?}"),
        }
    }
    Ok(commits)
}

/// Parse a [`Commit`] into a [`ParsedCommit`]. Pure; no I/O.
///
/// Subject grammar: `<type>[(scope)][!]: <description>`. A non-conventional message
/// yields `kind = None` with the whole subject as the description. Body footers
/// (`BREAKING CHANGE:` / `BREAKING-CHANGE:`) are scanned separately.
pub fn parse(commit: &Commit) -> ParsedCommit {
    let breaking_footer = parse_breaking_footer(&commit.body);

    let Some((type_token, description)) = commit.subject.split_once(':') else {
        // Non-conventional: no `:` in the subject.
        return ParsedCommit {
            kind: None,
            scope: None,
            breaking_bang: false,
            description: commit.subject.trim().to_string(),
            breaking_footer,
        };
    };

    let description = description.trim().to_string();

    // Strip a trailing `!`, then a trailing `(scope)`, to recover the bare type
    // (tolerates both `feat!` and `feat(core)!`).
    let mut token = type_token.trim();
    let mut breaking_bang = false;
    if let Some(stripped) = token.strip_suffix('!') {
        breaking_bang = true;
        token = stripped;
    }

    let scope = if let (Some(open), true) = (token.find('('), token.ends_with(')')) {
        let scope = token[open + 1..token.len() - 1].to_string();
        token = &token[..open];
        Some(scope)
    } else {
        None
    };

    let kind = token.trim().to_lowercase();

    ParsedCommit {
        kind: Some(kind),
        scope,
        breaking_bang,
        description,
        breaking_footer,
    }
}

/// Whether the parsed commit declares a breaking change: the `!` marker or a
/// `BREAKING CHANGE:` footer, either alone.
pub fn is_breaking(parsed: &ParsedCommit) -> bool {
    parsed.breaking_bang || parsed.breaking_footer.is_some()
}

/// Scan a commit body for the first `BREAKING CHANGE:` / `BREAKING-CHANGE:` footer
/// and return its full trimmed value.
///
/// The value runs from the text after the token up to — but not including — a blank
/// line, the next footer token, or the end of the body. Continuation lines are kept
/// (joined by newlines), so a multi-line footer is captured in full.
fn parse_breaking_footer(body: &str) -> Option<String> {
    let mut lines = body.lines();
    // Advance to the footer's opening line, keeping the text after the token.
    let first = loop {
        let line = lines.next()?;
        if let Some(rest) = breaking_prefix(line.trim_start()) {
            break rest.trim();
        }
    };

    let mut value = vec![first.to_string()];
    for line in lines {
        let trimmed = line.trim_start();
        // The value ends at a blank line or the start of the next footer token.
        if trimmed.trim_end().is_empty() || is_footer_token_line(trimmed) {
            break;
        }
        value.push(trimmed.trim_end().to_string());
    }
    Some(value.join("\n"))
}

/// The text after a `BREAKING CHANGE:` / `BREAKING-CHANGE:` token if `line` opens
/// with one, else `None`.
fn breaking_prefix(line: &str) -> Option<&str> {
    ["BREAKING CHANGE:", "BREAKING-CHANGE:"]
        .into_iter()
        .find_map(|prefix| line.strip_prefix(prefix))
}

/// Whether `line` begins a footer token (`Token: value`, `Token #123`, or a
/// `BREAKING CHANGE:` token) — the boundary at which a preceding multi-line footer
/// value ends.
fn is_footer_token_line(line: &str) -> bool {
    if breaking_prefix(line).is_some() {
        return true;
    }
    if let Some((token, value)) = line.split_once(": ") {
        return is_footer_token(token) && !value.trim().is_empty();
    }
    if let Some((token, rest)) = line.split_once(" #") {
        return is_footer_token(token) && rest.starts_with(|c: char| c.is_ascii_digit());
    }
    false
}

/// A footer token: hyphen-joined alphanumeric words starting with a letter (e.g.
/// `Reviewed-by`). Rejects tokens with spaces so prose like `Note: something`
/// mid-sentence isn't mistaken for a footer.
fn is_footer_token(token: &str) -> bool {
    !token.is_empty()
        && token.starts_with(|c: char| c.is_ascii_alphabetic())
        && token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `Commit` with the given subject and an empty body.
    fn subj(subject: &str) -> Commit {
        Commit {
            hash: "deadbeef".into(),
            subject: subject.into(),
            body: String::new(),
        }
    }

    /// Build a `Commit` with the given subject and body.
    fn with_body(subject: &str, body: &str) -> Commit {
        Commit {
            hash: "deadbeef".into(),
            subject: subject.into(),
            body: body.into(),
        }
    }

    #[test]
    fn pathspec_relative_under_root() {
        let root = Path::new("/repo");
        assert_eq!(pathspec(root, Path::new("/repo/core")), "core");
        assert_eq!(pathspec(root, Path::new("/repo/wasm/js")), "wasm/js");
    }

    #[test]
    fn pathspec_root_or_outside_falls_back_to_dot() {
        let root = Path::new("/repo");
        assert_eq!(pathspec(root, Path::new("/repo")), ".");
        assert_eq!(pathspec(root, Path::new("/elsewhere")), ".");
    }

    #[test]
    fn feat_parses_type_and_description() {
        let p = parse(&subj("feat: add the thing"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert_eq!(p.scope, None);
        assert!(!p.breaking_bang);
        assert_eq!(p.description, "add the thing");
        assert!(!is_breaking(&p));
    }

    #[test]
    fn fix_parses_type_and_description() {
        let p = parse(&subj("fix: correct the bug"));
        assert_eq!(p.kind.as_deref(), Some("fix"));
        assert_eq!(p.description, "correct the bug");
    }

    // An arbitrary type is preserved verbatim (lowercased).
    #[test]
    fn arbitrary_type_is_preserved() {
        let p = parse(&subj("refactor: shuffle internals"));
        assert_eq!(p.kind.as_deref(), Some("refactor"));
        assert_eq!(p.description, "shuffle internals");
    }

    // Non-conventional (no `:`) -> kind None, whole subject as description.
    #[test]
    fn non_conventional_has_no_kind() {
        let p = parse(&subj("just a plain message"));
        assert_eq!(p.kind, None);
        assert_eq!(p.scope, None);
        assert!(!p.breaking_bang);
        assert_eq!(p.description, "just a plain message");
        assert!(!is_breaking(&p));
    }

    #[test]
    fn type_is_lowercased() {
        let p = parse(&subj("FEAT: shout"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
    }

    // Scope is stripped from the type and captured verbatim.
    #[test]
    fn scope_is_stripped_and_captured() {
        let p = parse(&subj("feat(core): scoped change"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert_eq!(p.scope.as_deref(), Some("core"));
        assert!(!p.breaking_bang);
    }

    // Description after the first `:` is trimmed; later colons stay.
    #[test]
    fn description_keeps_later_colons() {
        let p = parse(&subj("fix:  ratio is 1:2  "));
        assert_eq!(p.description, "ratio is 1:2");
    }

    // `feat!:` sets the bang and is breaking.
    #[test]
    fn bang_marker_is_breaking() {
        let p = parse(&subj("feat!: drop the old api"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert!(p.breaking_bang);
        assert!(is_breaking(&p));
    }

    // `feat(core)!:` — scope and bang together.
    #[test]
    fn scope_and_bang_together() {
        let p = parse(&subj("feat(core)!: change get() signature"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert_eq!(p.scope.as_deref(), Some("core"));
        assert!(p.breaking_bang);
        assert!(is_breaking(&p));
    }

    // Any type may carry `!` (e.g. refactor!:), still breaking.
    #[test]
    fn bang_on_any_type_is_breaking() {
        let p = parse(&subj("refactor!: rework module layout"));
        assert_eq!(p.kind.as_deref(), Some("refactor"));
        assert!(p.breaking_bang);
        assert!(is_breaking(&p));
    }

    // `!` present but no footer — still breaking via the marker alone.
    #[test]
    fn bang_without_footer_is_breaking() {
        let p = parse(&with_body(
            "fix!: behavior change",
            "Some normal body text.",
        ));
        assert!(p.breaking_bang);
        assert_eq!(p.breaking_footer, None);
        assert!(is_breaking(&p));
    }

    // `BREAKING CHANGE:` footer captured and breaking.
    #[test]
    fn breaking_change_footer_space_spelling() {
        let p = parse(&with_body(
            "feat: add config",
            "Body line.\n\nBREAKING CHANGE: config file is now required",
        ));
        assert!(!p.breaking_bang);
        assert_eq!(
            p.breaking_footer.as_deref(),
            Some("config file is now required")
        );
        assert!(is_breaking(&p));
    }

    // `BREAKING-CHANGE:` (hyphen) is equivalent to the space spelling.
    #[test]
    fn breaking_change_footer_hyphen_spelling() {
        let p = parse(&with_body(
            "feat: add config",
            "BREAKING-CHANGE: the env var was renamed",
        ));
        assert_eq!(
            p.breaking_footer.as_deref(),
            Some("the env var was renamed")
        );
        assert!(is_breaking(&p));
    }

    // A footer on a non-`feat` type still triggers the breaking signal.
    #[test]
    fn footer_on_non_feat_is_breaking() {
        let p = parse(&with_body(
            "chore: bump deps",
            "BREAKING CHANGE: minimum rust version raised",
        ));
        assert_eq!(p.kind.as_deref(), Some("chore"));
        assert!(is_breaking(&p));
    }

    // No marker and no footer -> not breaking.
    #[test]
    fn no_marker_no_footer_not_breaking() {
        let p = parse(&with_body("feat: add", "Just an ordinary body, no footer."));
        assert!(!p.breaking_bang);
        assert_eq!(p.breaking_footer, None);
        assert!(!is_breaking(&p));
    }

    // The first footer wins even when it is not on the first body line.
    #[test]
    fn first_footer_wins() {
        let p = parse(&with_body(
            "feat: x",
            "intro\nBREAKING CHANGE: first one\nBREAKING CHANGE: second one",
        ));
        assert_eq!(p.breaking_footer.as_deref(), Some("first one"));
    }

    // A footer wrapping across lines is captured in full.
    #[test]
    fn multiline_footer_is_captured_in_full() {
        let p = parse(&with_body(
            "feat!: rename the constant",
            "Body text.\n\nBREAKING CHANGE: the public constant `MAX_KEY_LEN` is renamed to\n`KEY_MAX_BYTES`. Update any references to the old name.",
        ));
        assert_eq!(
            p.breaking_footer.as_deref(),
            Some(
                "the public constant `MAX_KEY_LEN` is renamed to\n`KEY_MAX_BYTES`. Update any references to the old name."
            )
        );
    }

    // The footer stops at a blank line, so a following trailer isn't folded in.
    #[test]
    fn footer_stops_at_blank_line_before_trailer() {
        let p = parse(&with_body(
            "feat!: x",
            "BREAKING CHANGE: line one\nline two\n\nCo-Authored-By: Someone <s@e.com>",
        ));
        assert_eq!(p.breaking_footer.as_deref(), Some("line one\nline two"));
    }

    // The footer stops at the next footer token, even without a blank line.
    #[test]
    fn footer_stops_at_next_footer_token() {
        let p = parse(&with_body(
            "feat!: x",
            "BREAKING CHANGE: the reason\nReviewed-by: Someone\nRefs: #12",
        ));
        assert_eq!(p.breaking_footer.as_deref(), Some("the reason"));
    }

    // Records split on 0x1e, fields on 0x1f.
    #[test]
    fn parse_log_output_splits_records_and_fields() {
        let raw = "h1\u{1f}feat: a\u{1f}body one\u{1e}h2\u{1f}fix: b\u{1f}\u{1e}";
        let commits = parse_log_output(raw).unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].hash, "h1");
        assert_eq!(commits[0].subject, "feat: a");
        assert_eq!(commits[0].body, "body one");
        assert_eq!(commits[1].hash, "h2");
        assert_eq!(commits[1].subject, "fix: b");
        assert_eq!(commits[1].body, "");
    }

    #[test]
    fn parse_log_output_empty_is_empty() {
        assert!(parse_log_output("").unwrap().is_empty());
    }

    // collect() shells out without error and a real crate directory has history.
    #[test]
    fn collect_against_real_worktree() {
        // Tests run with cwd = crate dir (xtask/); the repo root is its parent.
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask has a parent dir");
        let git = Git::new(repo_root);
        let commits = collect(&git, None, "node/").expect("git log succeeds");
        assert!(
            !commits.is_empty(),
            "node/ has real history, expected commits"
        );
        // Every collected commit has a non-empty hash and subject.
        for c in &commits {
            assert!(!c.hash.is_empty());
            assert!(!c.subject.is_empty());
        }
        // Parsing each collected commit never panics.
        for c in &commits {
            let _ = parse(c);
        }
    }
}
