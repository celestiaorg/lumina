//! Path-scoped git-log collection and Conventional-Commit parsing.
//!
//! This module is the shared foundation for `changelog` (M9) and `breaking`
//! (M10). It has two responsibilities:
//!
//! 1. [`collect`] — shell out to `git log` to gather the commits touching a
//!    directory over a range. This is the only function that performs I/O.
//! 2. [`parse`] — a **pure** Conventional-Commit parser that turns a [`Commit`]
//!    into a [`ParsedCommit`] (type, scope, breaking markers, description,
//!    Keep-a-Changelog group, breaking footer).
//!
//! A collected [`Commit`] captures the hash, **subject**, and **body** so that a
//! single commit set serves both consumers: the changelog generator renders from
//! the subject, while breaking-change detection scans the body for a
//! `BREAKING CHANGE:` footer.
//!
//! Implements `release-spec/changelog-generation.md` (§"Collecting commits
//! (path-scoped git log)", §"Minimal Conventional Commits parser") and
//! `release-spec/breaking-change-detection.md` (§"Signal 2: intent breakage").

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// Field separator emitted by `git log` (`%x1f`, ASCII unit separator).
const FIELD_SEP: char = '\u{1f}';
/// Record separator emitted by `git log` (`%x1e`, ASCII record separator).
const RECORD_SEP: char = '\u{1e}';

/// A raw commit collected from git.
///
/// This is the only git-derived data in the module; everything else is derived
/// from it by [`parse`] (which is pure).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Full commit hash (`%H`).
    pub hash: String,
    /// Commit subject — the first line of the message (`%s`).
    pub subject: String,
    /// Commit body — everything after the subject (`%b`); may be empty.
    ///
    /// Carries footers such as `BREAKING CHANGE:` consumed by breaking-change
    /// detection.
    pub body: String,
}

/// The result of parsing a [`Commit`].
///
/// Note: the Keep-a-Changelog *grouping* (feat → Added, fix → Fixed, …) is not
/// represented here — the changelog generator does its own grouping through
/// git-cliff's `commit_parsers` table (see [`crate::changelog`]). This struct
/// carries only what the breaking-change detector and the parser's own callers
/// consume: the type, scope, breaking markers, and description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommit {
    /// The bare, lowercased commit type (e.g. `"feat"`). `None` for a
    /// non-conventional message (no `:` in the subject).
    pub kind: Option<String>,
    /// The scope inside `(...)`, preserved verbatim (e.g. `"core"`). `None` if
    /// absent.
    pub scope: Option<String>,
    /// `true` if a `!` breaking marker appeared before the `:` in the type token.
    pub breaking_bang: bool,
    /// The trimmed bullet text: the description after the first `:` for a
    /// conventional commit, or the whole subject line for a non-conventional one.
    pub description: String,
    /// The trimmed text following the first `BREAKING CHANGE:` /
    /// `BREAKING-CHANGE:` footer in the body, if any.
    pub breaking_footer: Option<String>,
}

/// Collect the commits that belong to the package rooted at `dir`, newest-first,
/// using **content/identity tracking that survives directory renames**.
///
/// A plain `git log -- <dir>` silently drops a package's history the moment its
/// directory is renamed (git does not follow directory renames), which produces
/// misleadingly empty changelogs. Instead this:
///
/// 1. lists the package's **current** tracked files (`git ls-files -- <dir>`);
/// 2. for each file, gathers the commits that touched it **following renames**
///    (`git log --follow -- <file>`), so history from the file's former locations
///    is included;
/// 3. unions those commit hashes and re-lists them in canonical newest-first order
///    with full messages.
///
/// `range = Some("<ref>..HEAD")` restricts every step to that range; `range = None`
/// walks all history (the never-released case). `--no-merges` is applied throughout.
///
/// # Side effects
///
/// Spawns `git` subprocesses that read the repository at `repo_root` (one
/// `ls-files`, one `git log --follow` per package file, one final `git log`). No
/// network access, no writes.
///
/// # Errors
///
/// Returns an error if git fails to spawn, any `git` invocation exits non-zero, or
/// its output is not valid UTF-8.
pub fn collect(repo_root: &Path, range: Option<&str>, dir: &str) -> Result<Vec<Commit>> {
    let files = tracked_files(repo_root, dir)?;
    if files.is_empty() {
        // No tracked files under `dir` (e.g. the package dir doesn't exist yet):
        // nothing to attribute.
        return Ok(Vec::new());
    }

    // Union of commit hashes that touched any of the package's current files,
    // following each file back through renames.
    let mut wanted: HashSet<String> = HashSet::new();
    for file in &files {
        for hash in commits_touching_file(repo_root, range, file)? {
            wanted.insert(hash);
        }
    }

    // Re-list the wanted commits in git's canonical newest-first order, with full
    // messages, by filtering a single whole-repo `git log` over the same range.
    let ordered = all_commits(repo_root, range)?;
    Ok(ordered
        .into_iter()
        .filter(|c| wanted.contains(&c.hash))
        .collect())
}

/// Derive the `git log` pathspec for a package from its absolute `manifest_dir`,
/// made relative to `repo_root`. Falls back to `.` (whole tree) for the degenerate
/// case where the manifest dir is the repo root or is not under it. Pure.
///
/// Callers pass the result as the `dir` argument to [`collect`].
pub fn pathspec(repo_root: &Path, manifest_dir: &Path) -> String {
    match manifest_dir.strip_prefix(repo_root) {
        Ok(rel) if !rel.as_os_str().is_empty() => rel.to_string_lossy().into_owned(),
        _ => ".".to_string(),
    }
}

/// Run `git -C <repo_root> <args>`, require a zero exit, and return stdout decoded
/// as UTF-8. The single spawn/check/decode seam for this module.
fn run_git(repo_root: &Path, args: &[&str]) -> Result<String> {
    let pretty = args.join(" ");
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()
        .with_context(|| format!("failed to spawn `git {pretty}` (is git installed and on PATH?)"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("`git {pretty}` failed: {}", stderr.trim());
    }
    String::from_utf8(output.stdout)
        .with_context(|| format!("`git {pretty}` produced non-UTF-8 output"))
}

/// Run `git log [range] --no-merges <tail…>` and return its raw stdout. Shared by
/// the per-file rename-following walk and the whole-repo ordering walk.
fn git_log(repo_root: &Path, range: Option<&str>, tail: &[&str]) -> Result<String> {
    let mut args: Vec<&str> = vec!["log"];
    if let Some(range) = range {
        args.push(range);
    }
    args.push("--no-merges");
    args.extend_from_slice(tail);
    run_git(repo_root, &args)
}

/// Repo-relative paths of every tracked file currently under `dir`
/// (`git ls-files -z -- <dir>`). NUL-delimited to tolerate unusual filenames.
fn tracked_files(repo_root: &Path, dir: &str) -> Result<Vec<String>> {
    let stdout = run_git(repo_root, &["ls-files", "-z", "--", dir])?;
    Ok(stdout
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

/// Commit hashes (newest-first) that touched `file`, following it across renames
/// (`git log [range] --no-merges --follow --format=%H -- <file>`).
fn commits_touching_file(repo_root: &Path, range: Option<&str>, file: &str) -> Result<Vec<String>> {
    let stdout = git_log(repo_root, range, &["--follow", "--format=%H", "--", file])?;
    Ok(stdout
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

/// All commits in `range` across the whole repo, newest-first, with full messages
/// — the canonical ordering used to sort the unioned per-file commit set.
fn all_commits(repo_root: &Path, range: Option<&str>) -> Result<Vec<Commit>> {
    let pretty = format!("--pretty=format:%H{FIELD_SEP}%s{FIELD_SEP}%b{RECORD_SEP}");
    let stdout = git_log(repo_root, range, &[&pretty])?;
    parse_log_output(&stdout)
}

/// Split raw `git log` output into [`Commit`]s. Pure; factored out for testing.
fn parse_log_output(stdout: &str) -> Result<Vec<Commit>> {
    let mut commits = Vec::new();
    for record in stdout.split(RECORD_SEP) {
        // The final record after the trailing separator is empty; skip records
        // that are blank (whitespace only) between/around the control bytes.
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

/// Parse a [`Commit`] into a [`ParsedCommit`]. **Pure** — no I/O.
///
/// Total: any input yields a `ParsedCommit` (a non-conventional message yields
/// `kind = None` with the whole subject as the description).
///
/// Subject grammar (`release-spec/changelog-generation.md`):
/// `<type>[(scope)][!]: <description>`. Body footers (`BREAKING CHANGE:` /
/// `BREAKING-CHANGE:`) are scanned per `release-spec/breaking-change-detection.md`
/// §"Signal 2".
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

    // Strip a trailing `!` (breaking marker), then a trailing `(scope)`, in
    // either order, to recover the bare type. This tolerates both `feat!` and
    // `feat(core)!`.
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

/// Whether the parsed commit declares a breaking change (intent signal).
///
/// Returns `true` if the `!` marker is present **or** a `BREAKING CHANGE:` /
/// `BREAKING-CHANGE:` footer was captured — per
/// `release-spec/breaking-change-detection.md` §"Signal 2" (either rule alone
/// marks a commit breaking). **Pure.**
pub fn is_breaking(parsed: &ParsedCommit) -> bool {
    parsed.breaking_bang || parsed.breaking_footer.is_some()
}

/// Scan a commit body for the first `BREAKING CHANGE:` / `BREAKING-CHANGE:`
/// footer and return its full trimmed value.
///
/// Per Conventional Commits, a footer value may wrap onto following lines; the
/// value runs from the text after the token up to — but not including — a blank
/// line, the next footer token (e.g. a trailing `Co-Authored-By:`), or the end of
/// the body. Continuation lines are preserved as newlines, so a multi-line
/// `BREAKING CHANGE:` footer is captured in full rather than truncated at the
/// first line. The two spellings are equivalent.
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

/// A git-trailer / Conventional-Commit footer token: hyphen-joined alphanumeric
/// words starting with a letter (e.g. `Reviewed-by`, `Closes`). Deliberately
/// rejects tokens containing spaces so prose like `Note: something` mid-sentence
/// is not mistaken for a footer.
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

    // --- package pathspec derivation ---------------------------------------

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

    // --- Subject parsing: type, scope, description -------------------------

    // Rule: conventional `feat:` — type/scope/description recovered.
    #[test]
    fn feat_parses_type_and_description() {
        let p = parse(&subj("feat: add the thing"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert_eq!(p.scope, None);
        assert!(!p.breaking_bang);
        assert_eq!(p.description, "add the thing");
        assert!(!is_breaking(&p));
    }

    // Rule: `fix:` type and description.
    #[test]
    fn fix_parses_type_and_description() {
        let p = parse(&subj("fix: correct the bug"));
        assert_eq!(p.kind.as_deref(), Some("fix"));
        assert_eq!(p.description, "correct the bug");
    }

    // Rule: an arbitrary type is preserved verbatim (lowercased).
    #[test]
    fn arbitrary_type_is_preserved() {
        let p = parse(&subj("refactor: shuffle internals"));
        assert_eq!(p.kind.as_deref(), Some("refactor"));
        assert_eq!(p.description, "shuffle internals");
    }

    // Rule: non-conventional (no `:`) -> kind None, whole subject as desc.
    #[test]
    fn non_conventional_has_no_kind() {
        let p = parse(&subj("just a plain message"));
        assert_eq!(p.kind, None);
        assert_eq!(p.scope, None);
        assert!(!p.breaking_bang);
        assert_eq!(p.description, "just a plain message");
        assert!(!is_breaking(&p));
    }

    // Rule: type token is lowercased.
    #[test]
    fn type_is_lowercased() {
        let p = parse(&subj("FEAT: shout"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
    }

    // Rule: scope is stripped from the type and captured verbatim.
    #[test]
    fn scope_is_stripped_and_captured() {
        let p = parse(&subj("feat(core): scoped change"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert_eq!(p.scope.as_deref(), Some("core"));
        assert!(!p.breaking_bang);
    }

    // Rule: description after the first `:` is trimmed; later colons stay.
    #[test]
    fn description_keeps_later_colons() {
        let p = parse(&subj("fix:  ratio is 1:2  "));
        assert_eq!(p.description, "ratio is 1:2");
    }

    // --- Breaking via `!` marker -------------------------------------------

    // Rule (breaking §Signal 2.1): `feat!:` sets the bang and is breaking.
    #[test]
    fn bang_marker_is_breaking() {
        let p = parse(&subj("feat!: drop the old api"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert!(p.breaking_bang);
        assert!(is_breaking(&p));
    }

    // Rule: `feat(core)!:` — scope AND bang together.
    #[test]
    fn scope_and_bang_together() {
        let p = parse(&subj("feat(core)!: change get() signature"));
        assert_eq!(p.kind.as_deref(), Some("feat"));
        assert_eq!(p.scope.as_deref(), Some("core"));
        assert!(p.breaking_bang);
        assert!(is_breaking(&p));
    }

    // Rule: any type may carry `!` (e.g. refactor!:), still breaking.
    #[test]
    fn bang_on_any_type_is_breaking() {
        let p = parse(&subj("refactor!: rework module layout"));
        assert_eq!(p.kind.as_deref(), Some("refactor"));
        assert!(p.breaking_bang);
        assert!(is_breaking(&p));
    }

    // Rule: `!` present but NO footer — still breaking via the marker alone.
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

    // --- Breaking via footer -----------------------------------------------

    // Rule (breaking §Signal 2.2): `BREAKING CHANGE:` footer captured + breaking.
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

    // Rule: `BREAKING-CHANGE:` (hyphen) is equivalent to the space spelling.
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

    // Rule: footer on a non-`feat` type still triggers the breaking signal.
    #[test]
    fn footer_on_non_feat_is_breaking() {
        let p = parse(&with_body(
            "chore: bump deps",
            "BREAKING CHANGE: minimum rust version raised",
        ));
        assert_eq!(p.kind.as_deref(), Some("chore"));
        assert!(is_breaking(&p));
    }

    // Rule: no marker and no footer -> not breaking.
    #[test]
    fn no_marker_no_footer_not_breaking() {
        let p = parse(&with_body("feat: add", "Just an ordinary body, no footer."));
        assert!(!p.breaking_bang);
        assert_eq!(p.breaking_footer, None);
        assert!(!is_breaking(&p));
    }

    // Rule: footer text is body-derived even when not on the first body line.
    #[test]
    fn first_footer_wins() {
        let p = parse(&with_body(
            "feat: x",
            "intro\nBREAKING CHANGE: first one\nBREAKING CHANGE: second one",
        ));
        assert_eq!(p.breaking_footer.as_deref(), Some("first one"));
    }

    // Rule: a footer value that wraps across lines is captured in full, not
    // truncated at the first line.
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

    // Rule: the footer value stops at a blank line, so a trailing trailer (e.g.
    // `Co-Authored-By:`) after a blank line is not folded into the value.
    #[test]
    fn footer_stops_at_blank_line_before_trailer() {
        let p = parse(&with_body(
            "feat!: x",
            "BREAKING CHANGE: line one\nline two\n\nCo-Authored-By: Someone <s@e.com>",
        ));
        assert_eq!(p.breaking_footer.as_deref(), Some("line one\nline two"));
    }

    // Rule: the footer value stops at the next footer token even without a blank
    // line between them.
    #[test]
    fn footer_stops_at_next_footer_token() {
        let p = parse(&with_body(
            "feat!: x",
            "BREAKING CHANGE: the reason\nReviewed-by: Someone\nRefs: #12",
        ));
        assert_eq!(p.breaking_footer.as_deref(), Some("the reason"));
    }

    // --- Raw git-log record splitting (pure part of `collect`) -------------

    // Rule (changelog §Collecting commits): records on 0x1e, fields on 0x1f.
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

    // --- Light integration: real `collect` against this worktree -----------

    // Rule (changelog §Collecting commits): collect() shells out without error
    // and a real crate directory has history (newest-first preserved).
    #[test]
    fn collect_against_real_worktree() {
        // Tests run with cwd = crate dir (xtask/); the repo root is its parent.
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask has a parent dir");
        let commits = collect(repo_root, None, "node/").expect("git log succeeds");
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
