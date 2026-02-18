use anyhow::{Result, anyhow, bail};
use cargo_metadata::semver::{Prerelease, Version};

use crate::domain::types::RcTransformState;

/// Classifies a version as plain/stable input or as an existing `rc.N` variant.
pub fn rc_transform_state(version: &Version) -> Result<RcTransformState> {
    let pre = version.pre.as_str();
    if pre.is_empty() {
        return Ok(RcTransformState::NotRcInput);
    }

    let number = pre
        .strip_prefix("rc.")
        .ok_or_else(|| anyhow!("unsupported pre-release segment: {pre}"))?
        .parse::<u64>()
        .map_err(|e| anyhow!("invalid rc number in {pre}: {e}"))?;

    Ok(RcTransformState::ExistingRc { n: number })
}

#[allow(dead_code)]
/// Converts the *current* package version to the next RC candidate.
/// This increments patch for stable inputs and increments rc index for RC inputs.
pub fn convert_to_next_rc(version: &Version) -> Result<Version> {
    // Converts from current package version to next RC:
    // 0.5.0 -> 0.5.1-rc.1
    // 0.5.1-rc.1 -> 0.5.1-rc.2
    match rc_transform_state(version)? {
        RcTransformState::NotRcInput => {
            let mut next = version.clone();
            next.patch = next.patch.saturating_add(1);
            next.pre = Prerelease::new("rc.1")?;
            Ok(next)
        }
        RcTransformState::ExistingRc { n } => {
            let mut next = version.clone();
            next.pre = Prerelease::new(&format!("rc.{}", n + 1))?;
            Ok(next)
        }
    }
}

/// Converts release-plz computed next version into RC effective version.
/// Stable `X.Y.Z` -> `X.Y.Z-rc.1`; existing `X.Y.Z-rc.N` -> `X.Y.Z-rc.(N+1)`.
pub fn convert_release_version_to_rc(standard_release: &Version) -> Result<Version> {
    // Converts from a standard release-plz target version to RC:
    // 0.5.1 -> 0.5.1-rc.1
    // 0.5.1-rc.1 -> 0.5.1-rc.2
    match rc_transform_state(standard_release)? {
        RcTransformState::NotRcInput => {
            let mut next = standard_release.clone();
            next.pre = Prerelease::new("rc.1")?;
            Ok(next)
        }
        RcTransformState::ExistingRc { n } => {
            let mut next = standard_release.clone();
            next.pre = Prerelease::new(&format!("rc.{}", n + 1))?;
            Ok(next)
        }
    }
}

/// Returns true when the version pre-release segment starts with `rc.`.
pub fn is_rc(version: &Version) -> bool {
    version.pre.as_str().starts_with("rc.")
}

/// Strips prerelease/build metadata and returns stable variant when input is prerelease.
pub fn to_stable_if_prerelease(version: &Version) -> Option<Version> {
    if version.pre.is_empty() {
        return None;
    }

    let mut stable = version.clone();
    stable.pre = Prerelease::EMPTY;
    stable.build = Default::default();
    Some(stable)
}

/// Ensures RC conversion is deterministic and matches `convert_release_version_to_rc`.
pub fn validate_rc_conversion(next_release: &Version, next_effective: &Version) -> Result<()> {
    if !is_rc(next_effective) {
        bail!("next version must be an rc variant, got {next_effective}");
    }

    let expected = convert_release_version_to_rc(next_release)?;
    if &expected != next_effective {
        bail!("invalid rc conversion, expected {expected} but got {next_effective}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Stable input is promoted to next patch and first RC in current-version flow.
    fn plain_version_to_first_rc() {
        let current = Version::parse("0.5.0").unwrap();
        let next = convert_to_next_rc(&current).unwrap();
        assert_eq!(next.to_string(), "0.5.1-rc.1");
    }

    #[test]
    /// Existing RC input increments only RC index in current-version flow.
    fn rc_version_to_next_rc() {
        let current = Version::parse("0.5.1-rc.1").unwrap();
        let next = convert_to_next_rc(&current).unwrap();
        assert_eq!(next.to_string(), "0.5.1-rc.2");
    }

    #[test]
    /// release-plz target stable release maps to first RC in release-target flow.
    fn standard_release_to_first_rc() {
        let standard = Version::parse("0.5.1").unwrap();
        let next = convert_release_version_to_rc(&standard).unwrap();
        assert_eq!(next.to_string(), "0.5.1-rc.1");
    }
}
