# Contributing to Lumina

First off, thanks for taking the time to contribute! ❤️ We appreciate all the efforts to help both Lumina and the Celestia ecosystem.


Below are a few requirements we have for incoming submissions.

## Signed Commits

All commits need to be signed before PR can be merged, to ensure integrity
and authenticity of the submitted code. If you don't have git signing set up,
see [github's guide](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits)
for reference.

## Conventional Commits

We follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
specification when writing commits messages. Consistency helps with readability
of git history, additionally we use it to generate our changelog.

## Pull Request Titles

PR titles are squashed into commit messages, so they must follow Conventional Commits and start the
subject with exactly one review level tag: `[high]`, `[medium]`, or `[low]`, e.g.
`fix(node)!: [high] reject invalid sample proofs` or `docs: [low] clarify setup`.
Include the same tag in the PR description with a brief reason, e.g. `Review level: [low] — documentation only.`

- `[high]`: Deep review for consensus, state transitions, security, or other changes with significant risk,
  e.g. header or sample verification, DAS, p2p/shwap protocol, `lumina-node-wasm` public API,
  cryptography or proof verification, and breaking changes.
- `[medium]`: Standard review for code changes with limited impact and well-understood behavior.
- `[low]`: Focused review for straightforward docs, tests, or tooling changes with low risk.

Choose the higher level when unsure. A small diff can still need deep review. CI checks the title format;
reviewers should confirm the chosen level. Release PRs opened by release-plz use `[medium]`. The tag is
stripped from changelog entries.

## Semantic versioning

When making a change, please make sure to check if it is SemVer compatible. Pull requests
that contain breaking changes should have an exclamation mark, e.g. `feat(types)!: remove some public method on Blob`.
This will ensure that the version of a crate will be updated accordingly upon the release.

## Adding / upgrading dependencies

We don't enforce the latest dependency versions to give more flexibility in dependency resolution to the dependant crates.
When adding or upgrading dependencies, try setting the minimal required version of a crate, e.g. if current latest version of
`serde-json` is `1.0.256`, start with version `1`. The CI job will then check what is the minimal required version and
suggest e.g. `1.0.101`, which you then can use in the respective `toml` file.

If a dependency is used in multiple crates of the workspace, set it in `workspace.dependencies` section
instead of repeating it in multiple crates.
