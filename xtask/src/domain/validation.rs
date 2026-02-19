use crate::domain::types::{ReleaseMode, ValidationIssue, VersionEntry};
use crate::domain::versioning::validate_rc_conversion;

/// Aggregates all validation issues for computed version entries.
/// RC mode adds transition-shape checks on top of duplicate-version checks.
pub fn collect_validation_issues(
    mode: ReleaseMode,
    versions: &[VersionEntry],
    duplicate_publishable_versions: &[(String, Vec<String>)],
) -> Vec<ValidationIssue> {
    // Duplicate publishable package versions are always invalid.
    let mut issues = duplicates_to_issues(duplicate_publishable_versions);
    if matches!(mode, ReleaseMode::Rc) {
        // RC mode adds strict conversion-shape validation.
        issues.extend(find_invalid_rc_transitions(versions));
    }
    issues
}

/// Maps duplicate publishable package versions into structured validation issues.
fn duplicates_to_issues(duplicates: &[(String, Vec<String>)]) -> Vec<ValidationIssue> {
    duplicates
        .iter()
        .map(
            |(package, versions)| ValidationIssue::DuplicatePublishableResolvedVersions {
                package: package.clone(),
                versions: versions.clone(),
            },
        )
        .collect()
}

/// Verifies that RC effective versions are exactly derived from release-plz next versions.
fn find_invalid_rc_transitions(versions: &[VersionEntry]) -> Vec<ValidationIssue> {
    let mut invalid = Vec::new();
    for version in versions.iter().filter(|version| version.publishable) {
        // Validate that next_effective equals deterministic RC transform of next_release.
        if validate_rc_conversion(&version.next_release, &version.next_effective).is_err() {
            invalid.push(ValidationIssue::InvalidRcTransition {
                package: version.package.clone(),
                from: version.next_release.to_string(),
                to: version.next_effective.to_string(),
            });
        }
    }
    invalid
}
