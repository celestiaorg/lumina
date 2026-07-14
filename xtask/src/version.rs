//! Version logic: tag naming/parsing (`{crate}-v{version}`), prerelease detection,
//! and legal-successor validation.
//!
//! Successor rules: from a prerelease `X.Y.Z-rc.N` the legal next versions are
//! `X.Y.Z-rc.(N+1)` or `X.Y.Z`; from a stable `X.Y.Z` they are a single SemVer bump
//! that resets lower components to `0` (`(X+1).0.0` / `X.(Y+1).0` / `X.Y.(Z+1)`),
//! optionally with a `-rc.1` suffix. Anything else is rejected. Pure — no I/O.

use std::sync::LazyLock;

use regex::Regex;

pub use semver::Version;

/// The per-crate tag convention `{crate_name}-v{version}` (e.g. `lumina-utils-v0.2.0`).
/// [`parse_version_tag`] is its inverse.
pub fn tag_name(crate_name: &str, version: &str) -> String {
    format!("{crate_name}-v{version}")
}

/// Matches a version tag `<crate>-v<semver-core>[-<prerelease>]`, capturing the
/// version. The prerelease class excludes `+`, so build-metadata tags never match.
static VERSION_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^.+-v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$").unwrap());

/// Parse the version out of a `{crate}-v{version}` tag (splitting on the final `-v`),
/// or `None` if the tag doesn't match the convention or doesn't parse as semver.
pub fn parse_version_tag(tag: &str) -> Option<Version> {
    let caps = VERSION_TAG.captures(tag)?;
    Version::parse(&caps[1]).ok()
}

/// The highest version in `versions`, or `None` when empty.
pub fn max_version<'a>(versions: impl IntoIterator<Item = &'a Version>) -> Option<Version> {
    versions.into_iter().max().cloned()
}

/// Whether `version` carries a prerelease component (e.g. `1.2.0-rc.1`).
pub fn is_prerelease(version: &Version) -> bool {
    !version.pre.is_empty()
}

/// The kind of a *legal* transition, returned by [`classify_bump`] /
/// [`is_legal_successor`] only when the next version is an exact legal successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bump {
    /// Current is `X.Y.Z-rc.N`, next is `X.Y.Z-rc.(N+1)` — continue the rc series.
    RcContinue,
    /// Current is `X.Y.Z-rc.N`, next is `X.Y.Z` — promote the rc to a final release.
    RcPromote,
    /// Current stable, next is `(X+1).0.0` — a major bump.
    Major,
    /// Current stable, next is `X.(Y+1).0` — a minor bump.
    Minor,
    /// Current stable, next is `X.Y.(Z+1)` — a patch bump.
    Patch,
    /// Current stable, next is `(X+1).0.0-rc.1` — a major bump's first rc.
    MajorRc,
    /// Current stable, next is `X.(Y+1).0-rc.1` — a minor bump's first rc.
    MinorRc,
    /// Current stable, next is `X.Y.(Z+1)-rc.1` — a patch bump's first rc.
    PatchRc,
}

/// Everything that can go wrong while validating a version transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionError {
    /// The *current* version string failed to parse as SemVer.
    ParseCurrent { input: String, source: String },
    /// The *requested next* version string failed to parse as SemVer.
    ParseNext { input: String, source: String },
    /// The current version parsed but is not a modeled shape (a pre-release other
    /// than `rc.N` with `N >= 1`, or build metadata).
    UnsupportedCurrent { version: Version },
    /// Both parsed and the current shape is supported, but `next` is not a legal
    /// successor of `current`.
    IllegalSuccessor { current: Version, next: Version },
}

impl std::fmt::Display for VersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionError::ParseCurrent { input, source } => {
                write!(f, "current version `{input}` is not valid SemVer: {source}")
            }
            VersionError::ParseNext { input, source } => {
                write!(
                    f,
                    "requested next version `{input}` is not valid SemVer: {source}"
                )
            }
            VersionError::UnsupportedCurrent { version } => write!(
                f,
                "current version `{version}` has an unsupported shape \
                 (only stable `X.Y.Z` or release-candidate `X.Y.Z-rc.N` are supported)"
            ),
            VersionError::IllegalSuccessor { current, next } => {
                write!(f, "`{next}` is not a legal successor of `{current}`")
            }
        }
    }
}

impl std::error::Error for VersionError {}

/// Parse a SemVer string as the *current* version (parse errors tagged
/// [`VersionError::ParseCurrent`]).
pub fn parse_current(s: &str) -> Result<Version, VersionError> {
    Version::parse(s).map_err(|e| VersionError::ParseCurrent {
        input: s.to_owned(),
        source: e.to_string(),
    })
}

/// Parse a SemVer string as the *requested next* version (parse errors tagged
/// [`VersionError::ParseNext`]).
pub fn parse_next(s: &str) -> Result<Version, VersionError> {
    Version::parse(s).map_err(|e| VersionError::ParseNext {
        input: s.to_owned(),
        source: e.to_string(),
    })
}

/// Classify the transition `current -> next` on already-parsed versions, returning
/// the [`Bump`] iff `next` is a legal successor. Errors with
/// [`VersionError::UnsupportedCurrent`] (current is neither stable nor `rc.N`) or
/// [`VersionError::IllegalSuccessor`].
pub fn classify_bump(current: &Version, next: &Version) -> Result<Bump, VersionError> {
    let illegal = || VersionError::IllegalSuccessor {
        current: current.clone(),
        next: next.clone(),
    };

    match rc_number(current) {
        // Current is a modeled release candidate: X.Y.Z-rc.N.
        CurrentShape::Rc(n) => {
            // Continue the rc series: same core, rc.(N+1).
            if same_core(next, current) {
                if let CurrentShape::Rc(m) = rc_number(next)
                    && m == n + 1 {
                        return Ok(Bump::RcContinue);
                    }
                // Promote to final: same core, no pre-release, no build.
                if next.pre.is_empty() && next.build.is_empty() {
                    return Ok(Bump::RcPromote);
                }
            }
            Err(illegal())
        }
        // Current is stable: X.Y.Z.
        CurrentShape::Stable => {
            // Build metadata on next is never a legal successor.
            if !next.build.is_empty() {
                return Err(illegal());
            }
            let core_bump = stable_core_bump(current, next);
            match (&core_bump, next.pre.as_str()) {
                (Some(b), "") => Ok(*b),
                (Some(b), "rc.1") => Ok(rc_variant(*b)),
                _ => Err(illegal()),
            }
        }
        // Current carries an unmodeled pre-release or build metadata.
        CurrentShape::Unsupported => Err(VersionError::UnsupportedCurrent {
            version: current.clone(),
        }),
    }
}

/// Parse both strings then classify the transition.
///
/// Equivalent to [`parse_current`] then [`parse_next`] then [`classify_bump`],
/// short-circuiting on the first parse error.
pub fn is_legal_successor(current: &str, next: &str) -> Result<Bump, VersionError> {
    let current = parse_current(current)?;
    let next = parse_next(next)?;
    classify_bump(&current, &next)
}

/// The recognised shape of a version with respect to its pre-release.
enum CurrentShape {
    /// Stable: no pre-release, no build metadata.
    Stable,
    /// A modeled release candidate `rc.N` with `N >= 1`, no build metadata.
    Rc(u64),
    /// Anything else (foreign pre-release, `rc` without a number, `rc.0`,
    /// multi-part rc, or any build metadata).
    Unsupported,
}

/// Determine the [`CurrentShape`] of a version: stable, modeled `rc.N`, or
/// unsupported. Build metadata always renders a version unsupported.
fn rc_number(v: &Version) -> CurrentShape {
    if !v.build.is_empty() {
        return CurrentShape::Unsupported;
    }
    if v.pre.is_empty() {
        return CurrentShape::Stable;
    }
    // Pre-release present: accept only exactly `rc.<number>` with number >= 1.
    let mut parts = v.pre.as_str().split('.');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("rc"), Some(num), None) => match num.parse::<u64>() {
            // Reject leading-zero forms like `rc.01` (not canonical) and `rc.0`.
            Ok(n) if n >= 1 && !num.starts_with('0') => CurrentShape::Rc(n),
            _ => CurrentShape::Unsupported,
        },
        _ => CurrentShape::Unsupported,
    }
}

/// True iff `a` and `b` have the same `major.minor.patch` core (ignoring
/// pre-release and build).
fn same_core(a: &Version, b: &Version) -> bool {
    a.major == b.major && a.minor == b.minor && a.patch == b.patch
}

/// If `next`'s core (major.minor.patch) is exactly one legal stable bump of
/// `current`'s core — with lower components reset to 0 — return which one.
/// Ignores pre-release/build; the caller checks those.
fn stable_core_bump(current: &Version, next: &Version) -> Option<Bump> {
    let (cx, cy, cz) = (current.major, current.minor, current.patch);
    let (nx, ny, nz) = (next.major, next.minor, next.patch);
    if nx == cx + 1 && ny == 0 && nz == 0 {
        Some(Bump::Major)
    } else if nx == cx && ny == cy + 1 && nz == 0 {
        Some(Bump::Minor)
    } else if nx == cx && ny == cy && nz == cz + 1 {
        Some(Bump::Patch)
    } else {
        None
    }
}

/// Map a stable bump to its `-rc.1` counterpart.
fn rc_variant(bump: Bump) -> Bump {
    match bump {
        Bump::Major => Bump::MajorRc,
        Bump::Minor => Bump::MinorRc,
        Bump::Patch => Bump::PatchRc,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Version {
        Version::parse(s).unwrap()
    }

    #[test]
    fn tag_name_uses_crate_v_version_convention() {
        assert_eq!(tag_name("lumina-utils", "0.2.0"), "lumina-utils-v0.2.0");
        assert_eq!(tag_name("lumina", "0.2.0"), "lumina-v0.2.0");
        assert_eq!(tag_name("lumina", "1.0.0-rc.1"), "lumina-v1.0.0-rc.1");
    }

    #[test]
    fn parse_version_tag_round_trips_the_convention() {
        // crate names with internal hyphens split on the FINAL `-v`.
        assert_eq!(
            parse_version_tag("lumina-node-utils-v0.2.0"),
            Some(v("0.2.0"))
        );
        assert_eq!(parse_version_tag("lumina-v0.2.0"), Some(v("0.2.0")));
        assert_eq!(
            parse_version_tag("lumina-v1.0.0-rc.1"),
            Some(v("1.0.0-rc.1"))
        );
        let tag = tag_name("a-b-c", "3.4.5-rc.2");
        assert_eq!(parse_version_tag(&tag), Some(v("3.4.5-rc.2")));
    }

    #[test]
    fn parse_version_tag_rejects_non_matching() {
        assert_eq!(parse_version_tag("v0.2.0"), None); // nothing before `-v`
        assert_eq!(parse_version_tag("-v0.2.0"), None); // empty prefix
        assert_eq!(parse_version_tag("lumina-0.2.0"), None); // no `-v`
        assert_eq!(parse_version_tag("lumina-v"), None); // empty version
        assert_eq!(parse_version_tag("lumina-vlatest"), None); // not semver
        assert_eq!(parse_version_tag("lumina-v1.2"), None); // not X.Y.Z
        assert_eq!(parse_version_tag("lumina-v1.2.3+build"), None); // build metadata rejected
        assert_eq!(parse_version_tag("random-tag"), None);
    }

    #[test]
    fn max_version_picks_highest_and_none_when_empty() {
        let vs = [v("0.1.0"), v("0.3.0"), v("0.2.5")];
        assert_eq!(max_version(&vs), Some(v("0.3.0")));
        // A final release outranks its own rc per semver.
        let pre = [v("1.0.0-rc.1"), v("1.0.0"), v("1.0.0-rc.2")];
        assert_eq!(max_version(&pre), Some(v("1.0.0")));
        let empty: [Version; 0] = [];
        assert_eq!(max_version(&empty), None);
    }

    #[test]
    fn is_prerelease_detects_rc() {
        assert!(!is_prerelease(&v("1.2.0")));
        assert!(is_prerelease(&v("1.2.0-rc.1")));
    }

    /// Assert `next` is a legal successor of `current` with the expected bump.
    /// Exercises both `is_legal_successor` (string entry point) and the symmetry
    /// with `classify_bump` on the parsed values.
    fn ok(current: &str, next: &str, expected: Bump) {
        assert_eq!(
            is_legal_successor(current, next),
            Ok(expected),
            "{current} -> {next} should be {expected:?}"
        );
        let c = Version::parse(current).unwrap();
        let n = Version::parse(next).unwrap();
        assert_eq!(classify_bump(&c, &n), Ok(expected));
    }

    /// Assert `next` is rejected as an `IllegalSuccessor` of `current`.
    fn illegal(current: &str, next: &str) {
        match is_legal_successor(current, next) {
            Err(VersionError::IllegalSuccessor { .. }) => {}
            other => panic!("{current} -> {next} expected IllegalSuccessor, got {other:?}"),
        }
    }

    // prerelease current X.Y.Z-rc.N -> rc.(N+1)

    #[test]
    fn rc_continues_to_next_rc() {
        ok("1.2.0-rc.2", "1.2.0-rc.3", Bump::RcContinue);
        ok("0.1.0-rc.1", "0.1.0-rc.2", Bump::RcContinue);
        ok("2.0.0-rc.9", "2.0.0-rc.10", Bump::RcContinue);
    }

    // prerelease current X.Y.Z-rc.N -> X.Y.Z (promotion)

    #[test]
    fn rc_promotes_to_final() {
        ok("1.2.0-rc.2", "1.2.0", Bump::RcPromote);
        ok("0.1.0-rc.1", "0.1.0", Bump::RcPromote);
    }

    // stable current -> major/minor/patch with lower components reset

    #[test]
    fn stable_major_bump_resets_lower() {
        ok("1.4.2", "2.0.0", Bump::Major);
        ok("0.9.9", "1.0.0", Bump::Major);
    }

    #[test]
    fn stable_minor_bump_resets_patch() {
        ok("1.4.2", "1.5.0", Bump::Minor);
        ok("1.0.0", "1.1.0", Bump::Minor);
    }

    #[test]
    fn stable_patch_bump() {
        ok("1.4.2", "1.4.3", Bump::Patch);
        ok("0.0.0", "0.0.1", Bump::Patch);
    }

    // stable current -> any of the three bumps with a -rc.1 suffix

    #[test]
    fn stable_major_rc1() {
        ok("1.4.2", "2.0.0-rc.1", Bump::MajorRc);
    }

    #[test]
    fn stable_minor_rc1() {
        ok("1.4.2", "1.5.0-rc.1", Bump::MinorRc);
    }

    #[test]
    fn stable_patch_rc1() {
        ok("1.4.2", "1.4.3-rc.1", Bump::PatchRc);
    }

    // reject illegal jumps that skip a number

    #[test]
    fn reject_skipping_components() {
        illegal("1.4.2", "3.0.0"); // major jumps by 2
        illegal("1.4.2", "1.7.0"); // minor jumps by 2
        illegal("1.4.2", "1.4.5"); // patch jumps by 3
        illegal("1.2.0-rc.2", "1.2.0-rc.4"); // rc skips a number
    }

    // reject a higher component bumped without resetting the lower ones

    #[test]
    fn reject_non_reset() {
        illegal("1.4.2", "2.4.2"); // major bump but minor/patch not reset
        illegal("1.4.2", "2.0.2"); // major bump but patch not reset
        illegal("1.4.2", "2.4.0"); // major bump but minor not reset
        illegal("1.4.2", "1.5.2"); // minor bump but patch not reset
    }

    // reject bumping more than one component at once

    #[test]
    fn reject_multi_component_bump() {
        illegal("1.4.2", "2.1.0");
        illegal("1.0.0", "2.1.1");
    }

    // reject going backwards or staying put

    #[test]
    fn reject_backwards_or_same() {
        illegal("1.4.2", "1.4.2"); // identical
        illegal("1.4.2", "1.4.1"); // patch backwards
        illegal("1.4.2", "1.3.0"); // minor backwards
        illegal("2.0.0", "1.0.0"); // major backwards
        illegal("1.2.0-rc.3", "1.2.0-rc.2"); // rc backwards
    }

    // reject rc edge cases on the stable path

    #[test]
    fn reject_wrong_rc_on_stable() {
        illegal("1.4.2", "2.0.0-rc.2"); // must be rc.1, not rc.2
        illegal("1.4.2", "1.4.2-rc.1"); // rc.1 on a non-bumped core
        illegal("1.4.2", "2.0.0-beta.1"); // foreign pre-release
        illegal("1.4.2", "2.4.2-rc.1"); // rc.1 but lower not reset
    }

    // reject an rc current that is neither rc.(N+1) nor a promotion

    #[test]
    fn reject_illegal_rc_transitions() {
        illegal("1.2.0-rc.2", "1.3.0"); // promote to a different core
        illegal("1.2.0-rc.2", "1.2.0-rc.2"); // same rc
        illegal("1.2.0-rc.2", "1.2.1"); // wrong core on promote
        illegal("1.2.0-rc.2", "1.2.0-rc.1"); // rc decreases
    }

    // build metadata is never a legal successor

    #[test]
    fn reject_build_metadata_on_next() {
        illegal("1.4.2", "1.4.3+build.5");
        illegal("1.4.2", "2.0.0-rc.1+sha");
    }

    // current carries an unmodeled shape

    #[test]
    fn unsupported_current_shapes() {
        for (cur, next) in [
            ("1.2.0-beta.1", "1.2.0"),
            ("1.2.0-rc", "1.2.0"),     // rc with no number
            ("1.2.0-rc.0", "1.2.0"),   // rc.0 not modeled
            ("1.2.0-rc.1.2", "1.2.0"), // multi-part rc
            ("1.2.0-alpha", "1.2.0"),
        ] {
            match is_legal_successor(cur, next) {
                Err(VersionError::UnsupportedCurrent { .. }) => {}
                other => panic!("{cur} should be UnsupportedCurrent, got {other:?}"),
            }
        }
    }

    #[test]
    fn unsupported_current_with_build_metadata() {
        match is_legal_successor("1.2.0+meta", "1.2.1") {
            Err(VersionError::UnsupportedCurrent { .. }) => {}
            other => panic!("build metadata on current should be Unsupported, got {other:?}"),
        }
    }

    // garbage input is tagged to the right side

    #[test]
    fn parse_errors_tagged_per_side() {
        assert!(matches!(
            is_legal_successor("not-a-version", "1.0.0"),
            Err(VersionError::ParseCurrent { .. })
        ));
        assert!(matches!(
            is_legal_successor("1.0.0", "garbage"),
            Err(VersionError::ParseNext { .. })
        ));
        // Current is parsed first: a bad current short-circuits before next.
        assert!(matches!(
            is_legal_successor("bad", "also-bad"),
            Err(VersionError::ParseCurrent { .. })
        ));
    }

    #[test]
    fn parse_helpers_tag_correctly() {
        assert!(matches!(
            parse_current("x.y.z"),
            Err(VersionError::ParseCurrent { .. })
        ));
        assert!(matches!(
            parse_next("x.y.z"),
            Err(VersionError::ParseNext { .. })
        ));
        assert_eq!(parse_current("1.2.3").unwrap(), Version::new(1, 2, 3));
        assert_eq!(parse_next("1.2.3").unwrap(), Version::new(1, 2, 3));
    }

    #[test]
    fn error_implements_display_and_error_trait() {
        let e = is_legal_successor("1.4.2", "9.9.9").unwrap_err();
        let _: &dyn std::error::Error = &e;
        assert!(!e.to_string().is_empty());
    }
}
