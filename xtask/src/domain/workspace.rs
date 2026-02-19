use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Result, bail};
use cargo_metadata::{Metadata, semver::Version};

#[derive(Debug)]
pub struct Workspace {
    metadata: Metadata,
}

impl Workspace {
    /// Wraps cargo metadata with workspace-oriented helper queries used by validation.
    pub fn from_metadata(metadata: Metadata) -> Self {
        Self { metadata }
    }

    /// Detects publishable workspace crates that appear with multiple versions simultaneously.
    pub fn duplicate_publishable_versions(&self) -> Vec<(String, Vec<String>)> {
        // Restrict analysis to workspace members only.
        let workspace_members: HashSet<_> =
            self.metadata.workspace_members.iter().cloned().collect();

        // Track publishable package names that belong to this workspace.
        let publishable_workspace_crates: HashSet<String> = self
            .metadata
            .packages
            .iter()
            .filter(|package| workspace_members.contains(&package.id))
            .filter(|package| {
                package
                    .publish
                    .as_ref()
                    .is_none_or(|publish| !publish.is_empty())
            })
            .map(|package| package.name.to_string())
            .collect();

        let mut versions_by_name: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for package in &self.metadata.packages {
            // Aggregate all versions seen per publishable package name.
            if publishable_workspace_crates.contains(package.name.as_str()) {
                versions_by_name
                    .entry(package.name.to_string())
                    .or_default()
                    .insert(package.version.to_string());
            }
        }

        versions_by_name
            .into_iter()
            .filter_map(|(name, versions)| {
                if versions.len() > 1 {
                    Some((name, versions.into_iter().collect()))
                } else {
                    None
                }
            })
            .collect()
    }

    #[allow(dead_code)]
    /// Fails if any publishable package name resolves to more than one version.
    pub fn ensure_no_publishable_duplicates(&self, context: &str) -> Result<()> {
        let offenders = self.duplicate_publishable_versions();
        if offenders.is_empty() {
            return Ok(());
        }

        // Build a readable error payload listing duplicate package versions.
        let details = offenders
            .into_iter()
            .map(|(name, versions)| format!("{name}: {}", versions.join(", ")))
            .collect::<Vec<_>>()
            .join("\n");

        bail!(
            "{context} failed: multiple versions of publishable workspace crates were found:\n{details}"
        );
    }

    #[allow(dead_code)]
    /// Returns current workspace package versions keyed by package name.
    pub fn current_versions(&self) -> BTreeMap<String, Version> {
        self.metadata
            .workspace_packages()
            .iter()
            .map(|package| (package.name.to_string(), package.version.clone()))
            .collect()
    }
}
