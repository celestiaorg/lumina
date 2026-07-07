# `xtask` — the lumina release tool

`xtask` is the in-house release automation for the `lumina` workspace, run as
`cargo xtask <command>`. It replaces third-party tooling (release-plz) with a
small, purpose-built, fully testable binary.

The tool is **fully non-interactive**: every input is a flag, and nothing ever
prompts.

---

## Commands

Two subcommands, defined in [`src/cli.rs`](src/cli.rs):

| Command | Flow | What it does |
|---|---|---|
| `cargo xtask prepare-release` | Flow 1 | Builds a single-commit **release PR** locally (version bump + changelogs + npm mirror), and — only with `--push` — pushes it and opens/updates the GitHub PR. |
| `cargo xtask release` | Flow 2 | **Publishes** crates to the registry, cuts per-crate git tags + GitHub releases, and optionally publishes npm packages. |

Both flows are **idempotent and resumable** — there is no state file. Re-running
converges on the desired end state by inspecting git tags and the registry each
time.

### Key flags

`prepare-release`:

- `--version` — **required**; the next version this release will carry.
- `--branch-prefix` — release-branch prefix (branch = `<prefix><version>`).
  Defaults to the config value, else `release-`.
- `--yes` — delete and recreate an existing release branch instead of erroring.
- `--push` — opt into the remote actions (push the branch + open/update the PR);
  without it the run is local-only.
- `--github-token-env` — name of the env var holding the GitHub token
  (default `GH_TOKEN`, a PAT so the branch commit re-triggers PR CI).
- `--pr-body-out FILE` — also write the rendered PR description to a file.
- `--no-verify` — skip the `cargo publish --workspace --dry-run` preflight.

`release`:

- `--sha` — commit to release from and place every tag on; **defaults to `HEAD`**.
- `--github-token-env` — GitHub token env-var name (default `GITHUB_TOKEN`).
- `--registry-token-env` — cargo registry token env-var name
  (default `CARGO_REGISTRY_TOKEN`).
- `--npm-token-env` — npm token env-var name; optional. Omitted means the npm
  step is skipped regardless of configuration.

Tokens are always referenced by env-var **name**, never passed as literals and
never logged.

---

## File-by-file breakdown

The module layout is declared in [`src/lib.rs`](src/lib.rs), layered as I/O
primitives → pure feature logic → command orchestration → CLI.

### Entry points

- **`src/main.rs`** — thin binary; calls `cli::run()` and maps errors to exit codes.
- **`src/cli.rs`** — clap definitions (`Cli`, `PrepareArgs`, `ReleaseArgs`),
  `run()` dispatch, token env-var name resolution, and outcome summaries.

### Command orchestration

- **`src/cmd_prepare.rs`** — orchestrates **Flow 1**: branch guard → current-version
  discovery → legal-successor validation → workspace version bump → publishability
  preflight → per-crate breaking analysis + changelogs → npm mirror → single commit
  → PR-body assembly → (under `--push`) push + open/update PR.
- **`src/cmd_release.rs`** — orchestrates **Flow 2**: discover workspace, compute
  publish order, and for each crate run `decide_action(tag_exists, is_published)`
  → **Skip** / **TagOnly** / **Publish**, then create the tag + GitHub release.
  Finishes with the optional npm step, gated on a configured component *and* a
  supplied token.

### Feature logic (pure where possible)

- **`src/version.rs`** — semver, tags, and prerelease. Tag naming/parsing
  (`{crate}-v{version}`), `is_prerelease`, and `is_legal_successor(current, next)`
  which classifies a transition into a `Bump` or rejects it.
- **`src/changelog.rs`** — per-package `CHANGELOG.md` generation. Groups parsed
  commits into Keep-a-Changelog sections, renders a `## [version] - date` entry,
  and returns both the file entry and a heading-less `body_only` copy for the PR.
- **`src/commit.rs`** — orchestrates path-scoped log collection (delegating the
  git calls to `gitops`) and parses Conventional Commits (type, scope, `!` marker,
  `BREAKING CHANGE:` footer). Parsing is pure.
- **`src/breaking.rs`** — diagnostic-only breaking-change detection (never changes
  the version). Combines an **API signal** (`cargo-semver-checks` vs the published
  baseline) with an **intent signal** (author's `!`/footer). Fail-open: a missing
  tool falls back to intent.
- **`src/prbody.rs`** — pure PR title + body rendering from a `Vec<PackageReport>`:
  a summary table, per-crate breaking-detail blocks, and a collapsible changelog.
  Enforces a ≤65,536-char cap with graceful degradation.

### Primitives

- **`src/workspace.rs`** — workspace discovery via `cargo metadata`;
  `publish_order()` topologically sorts publishable crates by intra-workspace deps.
- **`src/gitops.rs`** — all git subprocess calls: branch/tag existence,
  create/reset branch, stage + commit, push (with force-with-lease and the
  push-commit-to-branch path for signed commits), create tag, default-branch
  discovery, the `origin` URL, and the path-scoped `git log` that `commit.rs`
  orchestrates. No policy, just operations.
- **`src/cargoops.rs`** — registry/cargo ops: `is_published`,
  `max_published_version`, `publish`, and `wait_until_published`. Treats
  in-workspace local-source resolution as *unpublished*.
- **`src/forge.rs`** — GitHub REST/GraphQL primitives: derive the repo `owner/name`
  from the remote URL (or workspace `repository` metadata), `open_or_update_pr`
  (idempotent), `create_release` (idempotent), and token-from-env.
- **`src/npmops.rs`** — npm/WASM build + publish. `dist_tag(version)` derives the
  npm tag (`latest`, or the leading prerelease label like `rc`). `update_for_pr`
  mirrors the version + refreshes the lockfile at prepare time; `publish` builds
  via `wasm-pack` and publishes both the wasm crate and the JS wrapper at release
  time.
- **`src/config.rs`** — parses the optional `release.toml`; a missing file yields
  defaults, not an error.

### Configuration file

- **[`../release.toml`](../release.toml)** — repo-root config. `[defaults]`
  supplies the `branch_prefix` fallback; each `[[npm]]` entry names a publishable
  npm component (`wasm_crate` + `package_dir`). Lumina configures one component:
  `lumina-node-wasm` (from `wasm-pack build node-wasm`) plus the `lumina-node` JS
  wrapper in `node-wasm/js/`. A crate-only workspace could omit the file entirely.

---

## Flow 1 — `prepare-release` (build the release PR)

1. **Branch guard** — check the release branch (e.g. `release-0.2.0`) locally and,
   under `--push`, on the remote; with `--yes`, delete and recreate it (otherwise
   an existing branch is an error). Create/reset it from the default branch.
2. **Current version** — max of git version tags (`{crate}-v{semver}`) and the
   highest published registry version; `None` for a first release.
3. **Validate requested version** — must be a legal successor per `version.rs`
   (or any valid semver for a first release).
4. **Bump workspace version** — format-preserving `toml_edit` rewrite of
   `[workspace.package].version` plus any intra-workspace `=` pins.
5. **Per crate (topological order)** — collect commits since the last tag, run
   breaking analysis, and generate the `CHANGELOG.md` entry (keeping a body-only
   copy for the PR).
6. **npm mirror** — for each `[[npm]]` component, mirror the version and refresh
   the lockfile (no credentials needed).
7. **PR body** — render title/body from the per-crate reports; print to stdout.
8. **Single commit + PR** — with `--push`, the commit is created through GitHub's
   `createCommitOnBranch` API so it is **GitHub-signed ("Verified")**. Without
   `--push`, fall back to a plain local commit (an unsigned preview).

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
   Both *Publish* and *TagOnly* create the annotated tag `{crate}-v{version}` and
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
- **Non-interactive** — every input is a flag; nothing prompts.
- **Secrets by env-var name only** — tokens are read by name, never printed,
  never passed on the command line.
- **Fail-open on optional tools** — a missing `cargo-semver-checks` degrades to
  the author's intent signal rather than aborting.
- **Format-preserving edits** — `toml_edit` keeps comments, ordering, and layout
  in `Cargo.toml`.
- **Fixed conventions** — per-crate tag `{crate}-v{version}`; PR title `chore:
  release v{version}`; npm dist-tag `latest` or the leading prerelease label.
