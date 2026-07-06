# `xtask` — the toy-kv release tool

`xtask` is the in-house release automation for this workspace, run as
`cargo xtask <command>`. It replaces third-party tooling (release-plz) with a
small, purpose-built, fully testable binary.

The **design spec** lives in [`../release-spec/`](../release-spec/); this file
describes the **implementation** — what each source file does and how the two
release flows run end to end.

---

## Commands

Two subcommands, defined in [`src/cli.rs`](src/cli.rs):

| Command | Flow | What it does |
|---|---|---|
| `cargo xtask prepare-release` | Flow 1 | Builds a single-commit **release PR** locally (version bump + changelogs + npm mirror), optionally pushes it and opens/updates the GitHub PR. |
| `cargo xtask release` | Flow 2 | **Publishes** crates to the registry, cuts per-crate git tags + GitHub releases, and optionally publishes npm packages. |

Both flows are **idempotent and resumable** — there is no state file. Re-running
converges on the desired end state by inspecting git tags and the registry each
time.

### Key flags

`prepare-release`: `--version` (prompts if missing), `--branch-prefix`
(config default → `release-`), `--push` (only then does it push + open a PR),
`--yes` (auto-confirm destructive prompts). GitHub token env var defaults to
`GH_TOKEN`.

`release`: `--sha` (commit to release, usually the merge commit; prompts if
missing), `--publish-timeout` (default 300s). Token env vars default to
`GITHUB_TOKEN`, `CARGO_REGISTRY_TOKEN`, and an optional npm token (npm step is
skipped if unset). Tokens are always read from env vars by name and never logged.

---

## File-by-file breakdown

The module layout is declared in [`src/lib.rs`](src/lib.rs), organized in "waves":
foundation primitives → feature logic → command orchestration → CLI.

### Entry points

- **`src/main.rs`** — thin binary; calls `cli::run()` and maps errors to exit codes.
- **`src/cli.rs`** — clap definitions (`Cli`, `PrepareArgs`, `ReleaseArgs`),
  `run()` dispatch, interactive prompting for missing inputs, and env-var name
  resolution for tokens.

### Command orchestration

- **`src/cmd_prepare.rs`** (M12) — orchestrates **Flow 1** in 10 steps: branch
  guard → discover current version → validate requested version → bump workspace
  version → per-crate breaking analysis → per-crate changelog → npm mirror →
  single commit → PR body assembly → push & open/update PR (only with `--push`).
  Also holds the tag-parser and the `toml_edit`-based version bumper.
- **`src/cmd_release.rs`** (M13) — orchestrates **Flow 2**: discover workspace,
  compute publish order, and for each crate run a `decide_action(tag_exists,
  is_published)` state table → **Skip** / **Publish** / **TagOnly**, then create
  the tag + GitHub release. Finishes with the optional npm step gated by
  `npm_gate(component_configured, token_supplied)`.

### Feature logic (pure where possible)

- **`src/version.rs`** (M3) — pure semver validation. `is_legal_successor(current,
  next)` classifies a transition into a `Bump` (RcContinue, RcPromote,
  Major/Minor/Patch, and `-rc.1` variants) or rejects it. No I/O.
- **`src/changelog.rs`** (M9) — per-package `CHANGELOG.md` generation. Groups
  parsed commits into Keep-a-Changelog sections, renders a `## [version] - date`
  entry, and splices it beneath the preserved header/`## [Unreleased]` line.
  Returns both the file entry and a heading-less `body_only` copy for the PR.
- **`src/commit.rs`** (M4) — collects `git log` (path-scoped, no-merges) and
  parses Conventional Commits (type, scope, `!` marker, `BREAKING CHANGE:`
  footer). Maps types to changelog groups. Parsing is pure.
- **`src/breaking.rs`** (M10) — diagnostic-only breaking-change detection (never
  changes the version). Combines an **API signal** (`cargo-semver-checks` vs the
  published baseline) with an **intent signal** (author's `!`/footer). Fail-open:
  if the tool is missing, falls back to intent.
- **`src/prbody.rs`** (M11) — pure PR title + body rendering from a
  `Vec<PackageReport>`: a summary table, per-crate breaking-detail blocks, and a
  collapsible changelog. Enforces a ≤65,536-char cap with graceful degradation
  (drop changelog, then hard-truncate at a UTF-8 boundary).

### Primitives

- **`src/workspace.rs`** (M2) — workspace discovery via `cargo metadata`;
  `publish_order()` topologically sorts publishable crates by intra-workspace deps.
- **`src/gitops.rs`** (M5) — typed git operations (branch/tag existence,
  create/reset branch, stage+commit, push with force-with-lease, stage-all +
  staged-change enumeration and push-commit-to-branch for the signed-commit path,
  create tag). No policy, just operations.
- **`src/cargoops.rs`** (M6) — registry/cargo ops: `is_published`,
  `max_published_version`, `publish`, and `wait_until_published` (polls until the
  new version indexes). Correctly treats in-workspace local-source resolution as
  *unpublished*.
- **`src/forge.rs`** (M7) — GitHub REST primitives: parse repo from remote URL,
  `open_or_update_pr` (idempotent), `create_release` (idempotent),
  `is_prerelease`, token-from-env.
- **`src/npmops.rs`** (M8) — npm/WASM build + publish. `dist_tag(version)` derives
  the npm tag (`latest`, or the leading prerelease label like `rc`).
  `update_for_pr` mirrors the version + refreshes the lockfile at prepare time;
  `publish` builds via `wasm-pack` and publishes both the wasm crate and the JS
  wrapper at release time.
- **`src/config.rs`** (M1) — parses the optional `release.toml`; a missing file
  yields defaults, not an error.

### Configuration file

- **[`../release.toml`](../release.toml)** — repo-root config. `[defaults]`
  supplies the `branch_prefix` fallback; each `[[npm]]` entry names a publishable
  npm component (`wasm_crate` + `package_dir`). For toy-kv: `toy-kv-wasm` (from
  `wasm-pack build wasm`) plus the `toy-kv` JS wrapper in `wasm/js/`. A
  crate-only workspace could omit the file entirely.

---

## Flow 1 — `prepare-release` (build the release PR)

1. **Branch guard** — check the release branch (e.g. `release-0.2.0`) locally and
   (with `--push`) on the remote; prompt to recreate; create/reset from the
   default branch.
2. **Current version** — max of git version tags (`{name}-v{semver}`) and the
   highest published registry version; `None` for a first release.
3. **Validate requested version** — must be a legal successor per `version.rs`
   (or any valid semver for a first release).
4. **Bump workspace version** — format-preserving `toml_edit` rewrite of
   `[workspace.package].version` plus any intra-workspace `=` pins.
5–6. **Per crate (topological order)** — collect commits since the last tag, run
   breaking analysis, and generate the `CHANGELOG.md` entry (keeping a body-only
   copy for the PR).
7. **npm mirror** — for each `[[npm]]` component, mirror the version and refresh
   the lockfile (no credentials needed).
8. **PR body** — render title/body from the per-crate reports; print to stdout.
9. **Single commit + PR** — with `--push`, the commit is created through GitHub's
   `createCommitOnBranch` API so it is **GitHub-signed ("Verified")**: push the
   release branch at its base commit, enumerate the staged change set as
   additions/deletions, create one signed commit `chore: release v<version>` on
   top, then open or update the GitHub PR. Without `--push`, fall back to a plain
   local commit (an unsigned preview — CI runners have no signing key).

## Flow 2 — `release` (publish + tag)

1. **Discovery** — workspace version, topological publish order, GitHub repo from
   the `origin` URL / workspace metadata.
2. **Per crate (topological order)** — check tag existence and registry state,
   then apply the decision table:

   | tag exists | published | action |
   |:---:|:---:|---|
   | yes | — | **Skip** |
   | no | yes | **TagOnly** (orphan-tag fix) |
   | no | no | **Publish** |

   *Publish* runs `cargo publish`, waits until the version indexes, then tags.
   Both *Publish* and *TagOnly* create the annotated tag `{name}-v{version}` and
   a GitHub release (prerelease flag derived from the semver), using the top
   `CHANGELOG.md` entry as the release body. All idempotent.
3. **npm** (optional) — gated on a configured component **and** a supplied token:
   `wasm-pack` build, publish the wasm crate and JS wrapper at the same version
   under the derived dist-tag, repinning the wrapper to the concrete version.

---

## Design principles

- **Pure core, thin I/O shell** — version validation, commit parsing, changelog
  rendering, and PR-body generation are pure and unit-tested; only the edges do
  git/cargo/network I/O.
- **Idempotent & resumable** — no state file; decisions are recomputed from git
  tags and the registry on every run.
- **Secrets by env-var name only** — tokens are read by name, never printed,
  never passed on the command line.
- **Fail-open on optional tools** — a missing `cargo-semver-checks` degrades to
  the author's intent signal rather than aborting.
- **Format-preserving edits** — `toml_edit` keeps comments, ordering, and layout
  in `Cargo.toml`.
- **Fixed conventions** — per-crate tag `{name}-v{version}`; PR title `chore:
  release v{version}`; npm dist-tag `latest` or the leading prerelease label.
