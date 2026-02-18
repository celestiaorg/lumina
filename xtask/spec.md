# Xtask Release Spec

## Scope

This document defines the `xtask` release workflow as the source of truth for:

1. release planning and validation,
2. release branch preparation,
3. PR-safe submission,
4. publish execution.

All release behavior must be implemented in Rust under `xtask` and exposed through CLI commands.

## Commands

`xtask` provides these commands:

1. `check <rc|final>`
2. `prepare <rc|final>`
3. `submit <rc|final>`
4. `execute <rc|final>`
5. `publish <rc|final>`

`execute` orchestrates only `check + prepare + submit`.
`publish` is the only command allowed to perform registry/GitHub release publishing.

## Common CLI Arguments

All commands accept shared options:

1. `--base-commit <sha>`: optional comparison commit override.
2. `--default-branch <name>`: default `main`.
3. `--branch-name <name>`: optional explicit release branch name.
4. `--rc-branch-prefix <prefix>`: default `release/rc`.
5. `--final-branch-prefix <prefix>`: default `release`.
6. `--skip-pr`: run flow but skip open/close PR operations.
7. `--json`: print machine-readable report.

`submit` and `execute` also support `--dry-run`.

## Mode Semantics

## RC Mode

`check rc` rules:

1. If `--base-commit` is provided, compare against that commit.
2. If `--base-commit` is omitted, compare against tip of default branch.
3. Compute next versions using standard `release_plz_core::next_versions` rules.
4. Convert computed versions to RC form:
   - stable `X.Y.Z` becomes `X.Y.Z-rc.1`
   - existing `X.Y.Z-rc.N` becomes `X.Y.Z-rc.(N+1)`
5. Validate no duplicate publishable resolved versions.

## Final Mode

`check final` rules:

1. If `--base-commit` is provided, analyzed workspace is that commit.
2. If `--base-commit` is omitted, analyzed workspace is tip of default branch.
3. Baseline for final release diff is latest non-RC release.
4. Validate no duplicate publishable resolved versions.
5. At least one non-RC release tag must exist.

Final check/report should expose:

1. `comparison_commit`: actual analyzed workspace commit.
2. `baseline_commit`: latest non-RC release baseline commit.

## Command Contracts

## check

`check <mode>` must:

1. compute plans for the selected mode,
2. run strict duplicate simulation in a temp snapshot,
3. emit validation issues,
4. emit comparison metadata and planned effective versions.

## prepare

`prepare <mode>` must:

1. run `check <mode>` against default-branch tip (ignore `--base-commit`),
2. stop on validation issues,
3. ensure target release branch policy:
   - RC requires RC-prefixed branch naming policy,
   - final must not use RC prefix,
4. for existing branch, apply release-plz style update behavior:
   - stash local changes,
   - reset generated release commits,
   - rebase onto latest default branch,
   - regenerate artifacts,
   - force-push path unless contributor-safe override is needed,
5. if open release PR has external contributors:
   - do not force-push existing PR branch,
   - close old PR and recreate branch/PR,
6. regenerate versions/changelog using release-plz update flow,
7. apply mode-specific version normalization:
   - RC conversion for RC mode,
   - prerelease-to-stable normalization for final mode.

## submit

`submit <mode>` must:

1. commit prepared changes on release branch,
2. push branch (force for in-place update path),
3. ensure release PR exists unless `--skip-pr` is set,
4. use contributor-safe close/recreate strategy when required.

## execute

`execute <mode>` must orchestrate:

1. `check`,
2. `prepare`,
3. `submit`.

It must not perform publish actions.

## publish

`publish <mode>` must:

1. run release publishing via release-plz release request flow,
2. be the only command that creates GitHub releases / registry publishes.

## Workflow Expectations

Recommended CI split:

1. automatic flow runs `execute rc` to prepare/update RC release PRs,
2. publishing runs only through explicit `publish <mode>` step,
3. npm publish uses:
   - RC mode: `--tag rc`,
   - final mode: default `latest` tag behavior.

## JSON Output Expectations

`--json` output should include:

1. command stage marker (`checked`, `prepared`, `submitted`, `executed`, `released`),
2. selected mode (`rc` or `final`),
3. release plans with `current` and `next_effective`,
4. validation issues list,
5. branch/PR metadata for `prepare` and `submit`,
6. publish payload for `publish`.

## Local Usage

With cargo alias:

1. `cargo xtask check rc --json`
2. `cargo xtask check final --json`
3. `cargo xtask prepare rc --json`
4. `cargo xtask submit rc --json`
5. `cargo xtask execute rc --json`
6. `cargo xtask publish final --json`
