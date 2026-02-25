# Release Process

## Automatic Releases with release-plz

Releases are managed by [release-plz](https://release-plz.dev/), split into two GitHub Actions workflows:

- **`.github/workflows/release-plz.yml`** — Creates/updates the release PR on every push to `main`.
- **`.github/workflows/release-plz-release.yml`** — Publishes crates, npm packages and ios/android libraries when the release PR is merged.

### How it works

1. A push to `main` triggers the PR workflow, which runs `release-plz release-pr`.
2. release-plz compares the workspace version in `Cargo.toml` against what's published on crates.io. If they differ, it creates (or updates) a PR with changelog entries.
3. A follow-up job builds the WASM package, bumps the npm version in `package.json`, and pushes a commit to the release PR.
4. When the release PR is merged, the release workflow runs `release-plz release`, which publishes all crates to crates.io, npm packages to the npm registry, and builds UniFFI artifacts for iOS/Android.

### Version progression

After the initial `1.0.0-rc.1` release, the flow is:

1. release-plz automatically creates PRs proposing the next version bump (e.g. `1.0.0-rc.2`).
2. Merging these PRs triggers publication of the new RC.
3. To move from RC to a stable release (e.g. `1.0.0`), manually update the version in `[workspace.package]` and `[workspace.dependencies]` in the root `Cargo.toml` to the desired version in one of these release PRs before merging.

## Unified Workspace Versioning

All crates in the workspace share a single version, defined once in the root `Cargo.toml`:

```toml
[workspace.package]
version = "1.0.0-rc.1"
```

Each member crate inherits it with `version.workspace = true` instead of hardcoding its own version.

Internal dependencies use exact version pinning (`=1.0.0-rc.1`) so that each crate can only be used with the matching version of every other crate in the workspace.

## NPM Packages

The npm package `lumina-node` follows the same version as the Rust crate `lumina-node-wasm`. The release workflow automatically extracts the version from Cargo and applies it to `package.json`.

For pre-release versions (e.g. `1.0.0-rc.1`), npm publish uses `--tag rc` so that `npm install lumina-node` continues to install the last stable release. Users opt into the RC with `npm install lumina-node@rc` or `npm install lumina-node@1.0.0-rc.1`.

## UniFFI (iOS/Android)

When a release includes `lumina-node-uniffi`, the release workflow builds native libraries for iOS (`aarch64-apple-ios`, `aarch64-apple-ios-sim`) and Android (`aarch64`, `armv7`, `x86_64`, `i686`). The built artifacts are packaged as tarballs and uploaded to the corresponding GitHub release along with SHA256 checksums.
