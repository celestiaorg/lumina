# CLAUDE.md

## Pull Requests

- Title format: `type(scope)!: [level] subject` (Conventional Commits; subject starts with exactly one of `[high]`, `[medium]`, `[low]`). CI enforces it.
- `[high]`: consensus, state transitions, security, or other significant risk (header/sample verification, DAS, p2p/shwap protocol, `lumina-node-wasm` public API, crypto/proof verification, breaking changes). `[medium]`: code changes with limited impact. `[low]`: straightforward docs, tests, or tooling.
- Pick the higher level when unsure. Add `Review level: [level] — <reason>` to the PR description.
- Details: CONTRIBUTING.md "Pull Request Titles".

## Security PRs

- For PRs that resolve Hacken bug bounty reports, do not include details about the bug in the PR description. Instead, link to a Linear issue that contains more details on the bug and the link to the Hacken bug bounty report.
