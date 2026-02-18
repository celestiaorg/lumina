use crate::domain::types::{Plan, ReleaseMode, ValidationIssue};
use crate::domain::versioning::validate_rc_conversion;

pub fn collect_validation_issues(
    mode: ReleaseMode,
    plans: &[Plan],
    duplicate_publishable_versions: &[(String, Vec<String>)],
) -> Vec<ValidationIssue> {
    let mut issues = duplicates_to_issues(duplicate_publishable_versions);
    if matches!(mode, ReleaseMode::Rc) {
        issues.extend(find_invalid_rc_transitions(plans));
    }
    issues
}

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

fn find_invalid_rc_transitions(plans: &[Plan]) -> Vec<ValidationIssue> {
    let mut invalid = Vec::new();
    for plan in plans.iter().filter(|plan| plan.publishable) {
        if validate_rc_conversion(&plan.next_release, &plan.next_effective).is_err() {
            invalid.push(ValidationIssue::InvalidRcTransition {
                package: plan.package.clone(),
                from: plan.next_release.to_string(),
                to: plan.next_effective.to_string(),
            });
        }
    }
    invalid
}
