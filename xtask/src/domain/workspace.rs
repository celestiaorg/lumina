use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Result, bail};
use cargo_metadata::{Metadata, semver::Version};

#[derive(Debug)]
pub struct Workspace {
    metadata: Metadata,
}

impl Workspace {
    pub fn from_metadata(metadata: Metadata) -> Self {
        Self { metadata }
    }

    pub fn duplicate_publishable_versions(&self) -> Vec<(String, Vec<String>)> {
        let workspace_members: HashSet<_> =
            self.metadata.workspace_members.iter().cloned().collect();

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
    pub fn ensure_no_publishable_duplicates(&self, context: &str) -> Result<()> {
        let offenders = self.duplicate_publishable_versions();
        if offenders.is_empty() {
            return Ok(());
        }

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
    pub fn current_versions(&self) -> BTreeMap<String, Version> {
        self.metadata
            .workspace_packages()
            .iter()
            .map(|package| (package.name.to_string(), package.version.clone()))
            .collect()
    }
}
