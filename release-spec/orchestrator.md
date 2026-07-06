# Release Orchestrator

The top-level spec for an **in-house release process** for a multi-crate Rust
workspace, optionally also shipping WASM + npm packages. It is a **generic,
reusable tool**, not specific to this repository: **toy-kv** is only its test
fixture. The same tool must drop onto the real **Lumina** workspace — and other
workspaces — unchanged.

Because it is generic, **all repo-specific facts are discovered or configured, not
hardcoded** (see [Portability & configuration](#portability--configuration)). In
particular, the **npm/WASM side is optional**: a workspace that publishes only
crates and no npm package runs the crate flow and skips every npm/WASM step.
Throughout this doc and the sub-specs, concrete toy-kv values (`toy-kv-wasm`,
`wasm/js/`, …) are **illustrative examples** of what configuration resolves to,
not built-in constants.

This document is the **source of truth**. Every other process doc in this repo
is a *sub-spec* describing one step the orchestrator drives — see
[Sub-spec index](#sub-spec-index). Where a sub-spec disagrees with this file,
this file wins.

## Goals & principles

1. **Two operations, nothing else:**
   - **Prepare release PR** — a manual `workflow_dispatch` action that opens (or
     refreshes) a release PR.
   - **Release** — runs automatically when that PR is merged: publishes crates,
     tags, then publishes npm. Also can be a workflow_dispatch action that can be triggered manually.
2. **Don't reimplement `release-plz`.** The one hard rule: we do **not** depend on
   `release-plz`. The release *logic* — the two flows, the version rules, the
   orchestration — is **our own Rust code** in an `xtask` crate, not a wrapper
   around someone else's release tool. Beyond that:
   - **Normal helper crates are fine.** Use ordinary ecosystem crates for the
     plumbing — e.g. `clap` (CLI), `serde`/`toml`/`serde_json` (manifests,
     `package.json`), `semver` (version math), `anyhow` (errors), a git crate or
     shelling to `git`. No need to hand-roll parsers.
   - **Standalone tools are fine** where they do one job well: `cargo-semver-checks`
     for breaking-change detection (see step 5), plus the build chain we already
     use (`git`, `cargo`, `wasm-pack`, `npm`).
   - **We keep ownership of two things** rather than delegating them to the engines
     `release-plz` wraps: the **changelog generator** (our own — no `git-cliff`) and
     the **PR-body/orchestration logic**. cargo-semver-checks is allowed because it
     is a standalone diagnostic tool, not `release-plz`.
3. **`xtask` pattern.** Every Rust-side action is a subcommand of a workspace
   binary, run as `cargo xtask <subcommand>`. The GitHub workflows are thin: they
   set up the toolchain and call `cargo xtask …`. All real logic is local and
   runnable on a developer machine, which makes it testable without CI.
4. **Single workspace version.** One version in `[workspace.package]`; every
   crate inherits it via `version.workspace = true`; intra-workspace deps pin it
   with `{ version = "=<v>", path = "…" }`. There is never more than one version
   in flight. npm packages inherit the same version.
5. **Idempotent.** Both operations can be re-run safely. Prepare detects an
   existing release branch; Release detects already-published crates and existing
   tags and only does what is missing.
6. **One commit.** Prepare applies *all* of its changes (version bumps,
   changelogs, npm package updates) as a **single commit** on the release branch.
   With `--push`, that commit is created through GitHub's `createCommitOnBranch`
   API so it is **GitHub-signed ("Verified")**; a local run makes a plain
   (unsigned) commit for preview.
7. **Generic, not repo-specific.** No path, crate name, or package layout is baked
   into the tool. The crate set comes from `cargo metadata`; the optional npm
   component comes from configuration. toy-kv is a fixture, not a hardcoded target.
   See [Portability & configuration](#portability--configuration).

## The xtask surface

```
cargo xtask prepare-release [--branch-prefix <p>] [--version <v>] [--yes]
                            [--push] [--github-token-env <ENV>]
cargo xtask release         [--sha <commit>]
                            [--github-token-env <ENV>]
                            [--registry-token-env <ENV>]
                            [--npm-token-env <ENV>]
```

Both subcommands are **interactive by default** (they prompt for the inputs
described below). Flags pre-fill answers so CI can run them non-interactively;
`--yes` auto-confirms destructive prompts. Every credential is passed as the
**name of an environment variable** that holds the token (`--*-token-env`), never
as a literal token on the command line — see [Credentials](#credentials).

**`prepare-release` does nothing remote without `--push`.** By default it does all
the *local* work — create the release branch, bump versions, generate changelogs,
update the npm package, and make a single **local** commit — then **stops**: it
pushes nothing and opens no PR. This makes it safe to run locally to inspect
exactly what the release commit would contain. Only with `--push` does it go
remote — and there it does not push a local commit: it pushes the release branch
at its base commit and creates the single release commit through GitHub's
`createCommitOnBranch` API (a **signed, "Verified"** commit), then opens/updates
the PR (a GitHub token is required for this). The `prepare-release.yml` workflow
always passes `--push`.

| Subcommand | Trigger | Side effects |
| --- | --- | --- |
| `prepare-release` | `workflow_dispatch` (manual) | Creates/updates a release branch + PR. Publishes nothing. |
| `release` | Automatic on release-PR merge | Publishes crates (+ npm if configured), creates tags + GitHub releases. |

---

## Portability & configuration

The tool carries **no built-in knowledge of any repository**. Everything
repo-specific is either *discovered* or comes from the tool's own config file:

- **Discovered (no config):** the workspace version (`[workspace.package].version`)
  and the set of publishable crates, their directories, and their dependency order
  — all from `cargo metadata`. Changelog path-scoping uses each crate's own
  directory. This already works for any workspace, toy-kv or Lumina, with zero
  setup.
- **Configured:** the **optional npm component(s)** and any release defaults, in a
  dedicated config file the tool reads from the repo root.

### The config file (`release.toml`)

A single TOML file at the repo root is the tool's config. (Named `release.toml`
rather than a bare `config.toml` to avoid confusion with Cargo's own
`.cargo/config.toml`; rename if you prefer — it's just the path the xtask reads.)
It is **entirely optional**: a crate-only workspace can omit the file completely.

```toml
# release.toml — config for `cargo xtask` release tooling.

[defaults]
branch_prefix = "release-"   # optional; fallback for prepare-release's --branch-prefix

# Zero or more npm packages to PUBLISH. List ONLY packages that get published to
# npm — not every crate you happen to wasm-pack build for tests (see note below).
# Omit the [[npm]] list entirely for a crate-only workspace.

[[npm]]
wasm_crate  = "node-wasm"      # crate wasm-pack builds into the published wasm package
package_dir = "node-wasm/js"   # the JS wrapper package directory
#                                (Lumina: lumina-node-wasm + lumina-node)

# toy-kv's fixture uses one component:
#   [[npm]]
#   wasm_crate  = "toy-kv-wasm"
#   package_dir = "wasm/js"
```

`[[npm]]` is a **list**, so a workspace may declare zero, one, or several
published npm packages.

> **Publish targets only — not test-only wasm.** A workspace may `wasm-pack build`
> crates purely to run browser tests without ever publishing them. (Lumina's root
> `package.json` builds the `types` and `grpc` crates this way, but only
> `node-wasm` → `lumina-node-wasm` + `lumina-node` are published.) The `[[npm]]`
> list names **publish targets**; test-only wasm builds are out of scope for the
> release tool.

### The npm component is optional

If **no** `[[npm]]` entry is configured (or there is no `release.toml`), the
npm/WASM work is **skipped wholesale**:

- **Prepare** skips step 7 (build WASM + update npm package) — see
  [`npm-release-pr-steps.md`](npm-release-pr-steps.md).
- **Release** skips step 3 (publish npm) — see [`release-step.md`](release-step.md).
- No `wasm-pack`/`npm` toolchain is required, and the npm token
  (`--npm-token-env`) is not needed.

A crate-only workspace therefore exercises exactly the crate path and nothing
else. toy-kv and Lumina each configure one npm component, so they exercise both
paths — which is the point of using toy-kv as the fixture. The published npm
version always equals the single workspace version (resolved from the
`wasm_crate`), confirmed against Lumina where `cargo pkgid node-wasm` is the
source of the npm version.

---

## Flow 1 — Prepare release PR

Manual (`workflow_dispatch`). Produces a single-commit release PR. Publishes
nothing.

### Inputs (prompted)

1. **Branch prefix** — the release-PR branch is `"<prefix><version>"` (e.g.
   prefix `release-` + version `0.2.0` → branch `release-0.2.0`).
2. **GitHub token** — **only required with `--push`** (a purely local run needs no
   credentials). Read from the env var named by `--github-token-env`. Used to push
   the release-branch commit and to open/update the PR. A PAT (or GitHub App
   token), **not** the default Actions `GITHUB_TOKEN` — see
   [Credentials](#credentials). Needs `contents: write` + `pull-requests: write`.
3. **Next version** — the version this release will carry. Validated against the
   rules in [Version validation](#version-validation); rejected if it is not a
   legal successor of the current version.
4. **`--push`** (flag) — opt in to the remote actions. Without it, the run stops
   after the local commit (step 8) and does nothing remote (steps 9–10 are
   skipped). CI always passes it.

### Steps

1. **Idempotency / branch guard.** Compute the target branch
   `"<prefix><version>"`. Check whether an **unmerged** release branch already
   exists — locally always, and on the remote too when `--push` is set. If it
   does, **prompt to delete it** before continuing (`--yes` auto-confirms).
   Recreating from scratch keeps the "one commit" guarantee — we never stack
   commits on a stale release branch.
2. **Determine the current version** from existing **git tags + published
   registry versions** (not just `Cargo.toml`), so the successor check is against
   what actually exists. See [Version validation](#version-validation).
3. **Validate the requested version** is *exactly* a legal next version. Abort
   with a clear message if not.
4. **Bump the workspace version** to the requested value in
   `[workspace.package]`, and update the `=<version>` pins on intra-workspace
   dependencies. One version, applied everywhere.
5. **Breaking-change analysis (diagnostic only).** For each crate, run
   `cargo-semver-checks` against its last published version to detect public-API
   breakage, complemented by conventional-commit intent. **This does *not*
   influence the version** — the version is user-chosen and already validated. The
   result is recorded purely to annotate the PR description. See
   [`breaking-change-detection.md`](breaking-change-detection.md).
6. **Per-package changelogs.** For every package, generate/refresh its
   `CHANGELOG.md` entry from the commits since its last release, grouped
   Keep-a-Changelog style. See [`changelog-generation.md`](changelog-generation.md).
7. **npm package update — only if an npm component is configured.** For each
   configured npm package, build its WASM package, then update the npm wrapper
   (version, lockfile, regenerated types/README) so the npm side matches the new
   workspace version. **Skipped entirely** for crate-only workspaces. See
   [Portability & configuration](#portability--configuration) and
   [`npm-release-pr-steps.md`](npm-release-pr-steps.md).
8. **Generate the PR description** (local): each package with its **previous
   version → new version** and a **breaking-change diagnostic**. See
   [`pr-body-logic.md`](pr-body-logic.md).
9. **Single commit + Push & PR.**
    - **With `--push`:** the single commit is created **remotely and signed** by
      GitHub. Push the release branch at its base commit, stage every change from
      steps 4–7 and enumerate it as file additions/deletions, then create one
      commit `chore: release v<version>` via GitHub's `createCommitOnBranch` API
      (a **"Verified"** commit authored by the token owner), and finally open or
      update the PR with the generated body.
    - **Without `--push`, stop after a local commit:** stage every change from
      steps 4–7 and commit once onto the local release branch (unsigned) for
      inspection; the would-be PR body is printed to stdout, and nothing is pushed
      or opened. No GitHub token is touched on a non-push run.

### Output

- **With `--push`:** one release branch `"<prefix><version>"` (pushed) with exactly
  one commit, and an open PR whose description lists every package, its version
  transition, and breaking-change status.
- **Without `--push`:** the same single commit on the **local** branch only, and
  the PR body printed to stdout. Nothing pushed, no PR, no credentials used.

Nothing is published in either case.

---

## Flow 2 — Release

Runs automatically when the release PR is merged. Publishes crates, then npm.

### Inputs (prompted / from env)

1. **Commit SHA** — the commit to release *from* and to place tags *on*
   (normally the merge commit).
2. **Cargo registry token** — read from the env var named by
   `--registry-token-env` (e.g. `CARGO_REGISTRY_TOKEN`). Used by `cargo publish`.
3. **GitHub token** — read from the env var named by `--github-token-env`. Used
   to create GitHub releases and to resolve the commit/tag author (`GET /user`).
   toy-kv uses the **same `RELEASE_PAT`** as prepare so tags + releases are
   attributed to the PAT owner; the default `GITHUB_TOKEN` also works for
   tags/releases but has no associated user (author falls back to the runner's
   git identity).
4. **npm token** — **only needed if an npm component is configured.** Read from
   the env var named by `--npm-token-env` (e.g. `NPM_REGISTRY_TOKEN`). Used by
   `wasm-pack publish` / `npm publish`.

These are passed as env-var **names**, never literal tokens. A crate-only
workspace needs only the cargo and GitHub tokens. See [Credentials](#credentials).

### Steps

1. **Idempotency scan.** For each workspace crate, check what is **already
   published** on the registry and whether its **git tag** already exists. Build
   the work list of crates that still need releasing. Re-running after a partial
   release picks up exactly where it left off.
2. **Publish crates** in **dependency (topological) order**, skipping anything
   already published. For each crate that needs it: `cargo publish`, wait for the
   registry to index it, then create its git tag (and GitHub release). We
   explicitly handle the **"published but un-tagged" gap** — if a crate is already
   on the registry but missing its tag, we create the tag rather than skipping.
   See [`publish-crates-logic.md`](publish-crates-logic.md).
3. **Publish npm — only if an npm component is configured.** After *all* crates are
   published, build + publish each configured npm package (wasm bindings package,
   then the wrapper), idempotently. **Skipped entirely** for crate-only workspaces.
   See [Portability & configuration](#portability--configuration) and
   [`release-step.md`](release-step.md).

### Output

Every crate published + tagged and GitHub releases cut. If an npm component is
configured, its npm packages are also published at the same version; otherwise the
npm step is skipped.

---

## Version validation

The requested next version must be **exactly** a legal successor of the current
version. The current version is determined from **existing git tags and published
registry versions** — the highest version that actually exists.

Let the current version be the highest existing version.

- **If the current version is a prerelease** `X.Y.Z-rc.N`, the legal next versions
  are:
  - `X.Y.Z-rc.(N+1)` — continue the rc series; **or**
  - `X.Y.Z` — drop the suffix and ship the final (promote rc → final).
  - (e.g. `1.2.0-rc.2` → `1.2.0-rc.3` or `1.2.0`.)
- **If the current version is stable** `X.Y.Z`, the legal next versions are a
  standard SemVer bump — **a higher component bump resets the lower ones to 0**:
  - major → `(X+1).0.0`
  - minor → `X.(Y+1).0`
  - patch → `X.Y.(Z+1)`
  - **or** any one of those three with a `-rc.1` suffix appended (e.g. major-rc →
    `(X+1).0.0-rc.1`).
  - (e.g. from `1.4.2`: major → `2.0.0`, minor → `1.5.0`, patch → `1.4.3`, or
    `2.0.0-rc.1` / `1.5.0-rc.1` / `1.4.3-rc.1`.)

Anything else is rejected.

---

## Credentials

Every token the process needs, in one place. Tokens are **always** passed as the
*name* of an environment variable holding the secret (`--*-token-env`); the xtask
reads the value from the environment and never accepts a literal token on the
command line (so secrets don't land in shell history, process listings, or CI
logs).

| Credential | Default env var | Used in | What for | Required scope |
| --- | --- | --- | --- | --- |
| **GitHub token** | `GH_TOKEN` = `RELEASE_PAT` (same PAT for both flows) | Prepare (**only with `--push`**) + Release | Prepare: push the release branch at base, create the signed release commit via the `createCommitOnBranch` API, and open/update the PR. Release: push tags + create GitHub releases. Both attribute the commit/tags to the PAT owner. | `contents: write`, `pull-requests: write` |
| **Cargo registry token** | `CARGO_REGISTRY_TOKEN` | Release | `cargo publish` | publish (new + update) for the workspace crates |
| **npm token** | `NPM_REGISTRY_TOKEN` | Release | `wasm-pack publish` / `npm publish` | publish for `toy-kv-wasm` and `toy-kv` |

Read-only operations need **no** credentials: `cargo-semver-checks` and the
"current version" lookup read public registry data and git tags; `wasm-pack build`
and changelog generation are local. A **`prepare-release` run without `--push`**
therefore needs **no credentials at all** — it never touches the GitHub token.

### Why a PAT for *prepare* but not *release*

The release-branch commit in Prepare must be authored with a **PAT or GitHub App
token**, not the workflow's default `GITHUB_TOKEN`. GitHub deliberately does **not**
re-trigger workflows for commits made with `GITHUB_TOKEN`, so the PR would open
with no CI running. A PAT (or App token) re-triggers PR CI normally. For Release,
the default `GITHUB_TOKEN` is fine for pushing tags and cutting releases — upgrade
it to a PAT only if you need a tag push to itself trigger another workflow.

### Local (non-CI) runs

Both subcommands run on a developer machine. Credential resolution:

- The `--*-token-env` flags still apply — export the env var (e.g.
  `export GH_TOKEN=…`) before running, or let the subcommand fall back to the
  tool's own stored credentials when the env var is unset:
  - **GitHub**: `gh auth` / a git credential helper for the push.
  - **cargo**: `~/.cargo/credentials.toml` (`cargo login`).
  - **npm**: `~/.npmrc` (`npm login`).
- Interactive prompts cover the non-secret inputs (branch prefix, version, SHA);
  secrets are never prompted for inline — they come from the environment or the
  tool's stored credentials.

## GitHub Actions wiring

Two workflows, both thin wrappers over the xtask. Each maps a repo secret onto the
env var the xtask expects (see [Credentials](#credentials)):

- **`prepare-release.yml`** — `on: workflow_dispatch`, inputs `branch_prefix` and
  `version`. Sets up Rust + `cargo-semver-checks` + the WASM/npm toolchain, then
  runs
  `cargo xtask prepare-release --branch-prefix … --version … --yes --push --github-token-env GH_TOKEN`,
  with `GH_TOKEN` set to a **PAT/App-token secret** (not the default
  `GITHUB_TOKEN`) so PR CI re-triggers. The `--push` flag is what makes CI actually
  push the branch and open the PR; a local run omits it to stay offline.
- **`release.yml`** — `on: push` to the default branch **plus `workflow_dispatch`**
  (manual trigger / resume), guarded so a push only acts on a merged release PR
  while a manual dispatch is always allowed. Runs
  `cargo xtask release --sha "$GITHUB_SHA" --github-token-env GH_TOKEN --registry-token-env CARGO_REGISTRY_TOKEN --npm-token-env NPM_REGISTRY_TOKEN`,
  with `GH_TOKEN` set to the same `RELEASE_PAT` as prepare, plus
  `CARGO_REGISTRY_TOKEN` and `NPM_REGISTRY_TOKEN` from secrets. The built-in
  `GITHUB_TOKEN` still gets `contents: write` for the tag push (via checkout).

Commit/tag attribution follows release-plz: the xtask resolves the GitHub **token
owner** (`GET /user`) and authors the release commit and annotated tags as that
account (`git -c user.name/user.email`, using the `<id>+<login>@users.noreply.github.com`
email). A user PAT (e.g. `prepare-release`'s `RELEASE_PAT`) ⇒ that user authors the
commit; the default `GITHUB_TOKEN` (an installation token, no associated user) ⇒
`GET /user` 403s and the xtask falls back to the ambient git identity. The
workflows deliberately require a user `RELEASE_PAT` and set no ambient identity,
so if resolution ever returns no user the git tag step fails loudly rather than
silently attributing the release to a bot.

> Note: the repo currently has only `.github/workflows/ci.yml`. These two release
> workflows are **to be added** as part of this work.

---

## Sub-spec index

Each file below specifies one step the orchestrator drives. They are written as
specs for *our* in-house implementation, not as descriptions of any external
tool.

| Sub-spec | Owns | Used by |
| --- | --- | --- |
| [`breaking-change-detection.md`](breaking-change-detection.md) | Diagnosing breaking changes per crate (diagnostic only) | Prepare · step 5 |
| [`changelog-generation.md`](changelog-generation.md) | Per-package `CHANGELOG.md` generation | Prepare · step 6 |
| [`npm-release-pr-steps.md`](npm-release-pr-steps.md) | Building WASM + updating the npm package on the PR | Prepare · step 7 |
| [`pr-body-logic.md`](pr-body-logic.md) | Generating the release-PR description | Prepare · step 9 |
| [`publish-crates-logic.md`](publish-crates-logic.md) | Publishing crates in order, tagging, idempotency | Release · step 2 |
| [`release-step.md`](release-step.md) | Publishing the npm packages at release time | Release · step 3 |

## Status

This is a design spec. Implementation (`xtask` crate + the two workflows) does
not exist yet; the sub-specs define the behavior each xtask subcommand must
implement.
