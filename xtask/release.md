# Release Process

Two modes: **PR** (creates a release PR) and **Publish** (publishes artifacts after merge).

## PR Mode

Creates a release PR with version bumps and changelog updates.

- Look at conventional commits and public API changes to figure out which packages need bumps and what kind (major/minor/patch).
- Apply transitive dependency bump policy so we don't end up with diamond dependency version conflicts. If A depends on both C directly and B which also depends on C, bumping C as minor and B as patch could produce two different versions of C in the tree. The policy keeps versions consistent (e.g. `pkg A -> pkg C`, `pkg A -> pkg B -> pkg C`. so if we bump C as minor and B as patch we can have `A -> C (v1 not bumped)`, `A -> B (v2 patch bump) -> C (v2 minor bump)`, so we have two versions of package C)
- Update Cargo.toml versions and changelogs.
- Update the npm library version if necessary.

### We support release candidates

RC versions bump the same way as regular versions, but if a package is already at e.g. `x.y.z-rc.1`, the next patch bump produces `x.y.z-rc.2`. Changelogs are always diffed against the last stable (non-RC) version.

## Publish Mode

After the release PR is merged:

- Publish updated crates to the registry.
- Create git tags and GitHub releases for each package.
- Publish npm package to the npm registry.
- Publish iOS and Android libraries as GitHub release assets.

## Trigger Logic

Every push creates a new release PR, unless the commit message is `chore: release rc` or `chore: release final` — those trigger publish instead.
