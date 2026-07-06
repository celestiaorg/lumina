# Implementation Guide — how agents build the release tool

This guide tells a fleet of agents **how** to implement the release tool specified
by [`orchestrator.md`](orchestrator.md) and its sub-specs. It defines the module
decomposition, the dependency order, the per-module agent pipeline, and — most
importantly — the **artifact contract** that lets agents hand work off without
reading each other's source.

Read this together with [`orchestrator.md`](orchestrator.md) (the *what*); this
file is the *how*.

---

## 0. Hard rules (read first, never violate)

1. **The `release-spec/` folder is immutable.** Everything in this folder
   (`orchestrator.md`, every sub-spec, and this guide) is the **frozen source of
   truth**. No implementer agent may **create, edit, rename, move, or delete any
   file under `release-spec/`** — not to "fix a typo", not to "add a note", not for
   anything. If a spec seems wrong, ambiguous, or incomplete, an agent **must
   stop** and record the question in its artifact (see §4, "Spec questions"); it
   must never resolve the ambiguity by editing the spec. The spec changes only by
   explicit human action, outside this process.
2. **Code lives in `xtask/`. Artifacts live in `artifacts/`.** Both are outside
   `release-spec/`. See §2.
3. **Handoffs are artifact-only.** A downstream agent is given **only the
   `contract.md` artifacts** of the modules it depends on (plus its own module's
   `contract.md`). It must **not** read the *source code* of other modules. The
   contract is the interface; if the contract is insufficient, that is a contract
   bug to be raised, not worked around by peeking at source. (See §4–§5.)
4. **Obey the spec, not your instincts.** Where a sub-spec dictates behavior
   (version rules, idempotency, the orphan-tag fix, "publish targets only", etc.),
   implement it exactly. The orchestrator wins over a sub-spec on conflict.
5. **The constraints from the spec still apply:** no dependency on `release-plz`;
   our own changelog generator (no `git-cliff`); `cargo-semver-checks` is allowed;
   a single workspace version; the npm component is optional and read from
   `release.toml`.

---

## 1. The shape of the thing being built

A single `xtask` crate exposing two subcommands — `cargo xtask prepare-release` and
`cargo xtask release` — per [`orchestrator.md`](orchestrator.md). Internally it is
a set of small, single-responsibility **modules** with explicit public APIs. The
modules are built in **dependency waves**; within a wave they are built in
**parallel**.

---

## 2. Folder layout

```
<repo root>/
  release-spec/        ← IMMUTABLE. Specs + this guide. Implementers MUST NOT touch.
  xtask/               ← the crate being built (source code goes here)
    src/
    Cargo.toml
  artifacts/           ← agent handoff artifacts (mutable; see §4)
    INDEX.md           ← live status board for every module/step
    <module-id>/
      contract.md
      impl-report.md
      style-report.md
      test-report.md
  release.toml         ← the tool's config for THIS repo (toy-kv fixture)
```

- **`release-spec/`** — never modified by implementers (Hard rule 1).
- **`xtask/`** — all production code.
- **`artifacts/`** — every step leaves exactly one markdown artifact here; this is
  the *only* channel between steps (Hard rule 3).

---

## 3. The agent roles (per-module pipeline)

Each module goes through a fixed pipeline of single-purpose agents. **Each role
leaves one artifact and consumes only the artifacts named below.**

| # | Role | Consumes | Produces | Job |
|---|------|----------|----------|-----|
| A | **Contract** | the module's spec doc(s) + the `contract.md` of each upstream dependency | `artifacts/<m>/contract.md` | Design the module's public API (types, function signatures, errors, behavior, invariants) from the spec. **No implementation.** |
| B | **Builder** | its own `contract.md` + upstream `contract.md`s only | `xtask/` source + `artifacts/<m>/impl-report.md` | Implement the module to satisfy the contract exactly. The impl-report restates the final public API and lists any deviations + spec questions. |
| C | **Style** | the module source it just built + `impl-report.md` | `artifacts/<m>/style-report.md` | Enforce code style: `cargo fmt`, `cargo clippy -D warnings`, naming/doc-comment conventions, dead code. Fix in place; report what changed. |
| D | **Test** | `contract.md` + `impl-report.md` (+ source for the unit under test) | tests in `xtask/` + `artifacts/<m>/test-report.md` | Write unit tests that verify the **contract** (not the implementation's quirks). Cover happy paths, error cases, and every spec-mandated rule. Report results + coverage gaps. |

Order within a module: **A → B → C → D**. The Contract (A) for a module may run as
soon as its upstream contracts exist; Builder (B) waits for A; Style (C) and Test
(D) wait for B. C and D may run in parallel with each other.

> Why contract-first: it lets a downstream module's **Contract agent (A)** start as
> soon as the upstream **Contract (A)** is done — *before* the upstream is built —
> so design fans out early and implementation fans out behind it.

### Reviewer / integrator (per wave)

After all modules in a wave finish D, one **Integration agent** runs:
consumes the wave's `impl-report.md`s + `test-report.md`s, builds the whole crate,
runs the full test suite + clippy, and writes `artifacts/wave-<n>-integration.md`
recording green/red status and any cross-module contract mismatches. A wave is
**not done** until its integration artifact is green.

---

## 4. The artifact model

Every step leaves **exactly one** markdown artifact under `artifacts/<module-id>/`.
Artifacts are the **sole handoff medium** (Hard rule 3).

### `contract.md` (the load-bearing artifact)

The public interface of a module, precise enough that a dependent can code against
it without seeing the source. Must contain:

- **Public types** (structs/enums) with field names + types + meaning.
- **Public functions/methods** with full signatures, parameter semantics, return
  values, and the **error type + every error case**.
- **Behavioral contract**: pre/postconditions, invariants, idempotency guarantees,
  side effects (does it shell out? touch the network? write files?).
- **Spec traceability**: which section(s) of which spec doc each item implements.
- **Spec questions**: a list of anything ambiguous/contradictory in the spec.
  Raising it here is the *only* sanctioned response to a spec problem (Hard rule 1).

### `impl-report.md`

Produced by the Builder. Restates the **final** public API (must match
`contract.md`; if it had to deviate, the deviation + reason is called out loudly),
lists files added/changed under `xtask/`, notes any TODOs, and repeats unresolved
spec questions.

### `style-report.md` / `test-report.md`

What the Style/Test agents changed and found: commands run, results, fixes applied,
remaining gaps. The Test report must explicitly map tests → spec rules so coverage
of mandated behavior is auditable.

### `artifacts/INDEX.md`

A live status board: one row per module with its current pipeline stage
(`contract` / `built` / `styled` / `tested` / `integrated`) and the path to each
artifact. Every agent updates its row when it finishes. This is how the fleet knows
what is ready to start.

---

## 5. The contract-only handoff rule (worked example)

`changelog` (M9) depends on `workspace` (M2) and `commit` (M4).

- The **M9 Contract agent** is given `release-spec/changelog-generation.md` plus
  `artifacts/M2/contract.md` and `artifacts/M4/contract.md`. From those it designs
  M9's API. It does **not** open `xtask/src/workspace.rs` or `commit.rs`.
- The **M9 Builder** codes M9 against M9's contract + the M2/M4 contracts. If M4's
  contract doesn't expose, say, the parsed footer it needs, the Builder does **not**
  go read M4's source to find a workaround — it records "M4 contract gap: need
  parsed footers exposed" as a spec/contract question in `impl-report.md`, and the
  integrator routes it back to M4's Contract agent.

This keeps modules decoupled and forces interfaces to be real.

---

## 6. Module decomposition & dependency waves

Each module maps to one or more spec docs. `→` shows the spec it implements;
"deps" are the modules whose **contracts** it consumes.

### Wave 1 — foundation (no internal deps; only external crates)

| ID | Module | Implements | Notes |
|----|--------|-----------|-------|
| **M1** | `config` | [`orchestrator.md` §Portability](orchestrator.md#portability--configuration) | Parse `release.toml` → `Config { defaults, npm: Vec<NpmComponent> }`. Missing file ⇒ empty config (crate-only). |
| **M2** | `workspace` | [`orchestrator.md` §Portability](orchestrator.md#portability--configuration), [`publish-crates-logic.md`](publish-crates-logic.md) | `cargo metadata` → workspace version, crate list (name, dir, manifest), `is_publishable`, **topological publish order** (cycle = error). |
| **M3** | `version` | [`orchestrator.md` §Version validation](orchestrator.md#version-validation) | Pure version logic: parse, and **validate "is X exactly a legal successor of Y"** per the rc/stable rules. No I/O. |
| **M4** | `commit` | [`changelog-generation.md`](changelog-generation.md), [`breaking-change-detection.md`](breaking-change-detection.md) | Path-scoped `git log` collection + Conventional-Commit parse (type, scope, `!`, `BREAKING CHANGE` footer, subject). Shared by M9 + M10. |
| **M5** | `gitops` | [`orchestrator.md`](orchestrator.md) Flows | Git primitives: branch exists (local/remote), create/reset branch, stage-all + commit, push, `tag_exists`, create tag. |
| **M6** | `cargoops` | [`publish-crates-logic.md`](publish-crates-logic.md) | `is_published(pkg@ver)` (via `cargo`), `cargo publish` (token from env, "already uploaded" race = success), `wait_until_published` (poll). |
| **M7** | `forge` | [`pr-body-logic.md`](pr-body-logic.md), [`publish-crates-logic.md`](publish-crates-logic.md) | GitHub API: open/update PR (head/base/title/body, update-only-if-changed), create GitHub release. Token from env. |
| **M8** | `npmops` | [`npm-release-pr-steps.md`](npm-release-pr-steps.md), [`release-step.md`](release-step.md) | npm/WASM primitives + the two procedures: `update_for_pr(component, version)` and `publish(component, version, dist_tag)`; dist-tag derivation; `file:../pkg` repin. |

### Wave 2 — feature logic (depends on Wave 1 contracts)

| ID | Module | Implements | Deps (contracts) |
|----|--------|-----------|------------------|
| **M9** | `changelog` | [`changelog-generation.md`](changelog-generation.md) | M2, M4 |
| **M10** | `breaking` | [`breaking-change-detection.md`](breaking-change-detection.md) | M2, M4 (owns the `cargo-semver-checks` invocation) |
| **M11** | `prbody` | [`pr-body-logic.md`](pr-body-logic.md) | M3, M9, M10 |

### Wave 3 — command orchestration (depends on Waves 1–2 contracts)

| ID | Module | Implements | Deps (contracts) |
|----|--------|-----------|------------------|
| **M12** | `cmd_prepare` | [`orchestrator.md` Flow 1](orchestrator.md#flow-1--prepare-release-pr) | M1,M2,M3,M4,M5,M8,M9,M10,M11 |
| **M13** | `cmd_release` | [`orchestrator.md` Flow 2](orchestrator.md#flow-2--release), [`publish-crates-logic.md`](publish-crates-logic.md) | M1,M2,M5,M6,M7,M8 |

`cmd_prepare` must honor: branch guard + delete prompt, single commit, `--push`
gating (no remote actions without it), and "current version from tags+registry".
`cmd_release` must honor: idempotency scan, dependency-order publish, and the
**published-but-untagged → create the tag** fix.

### Wave 4 — entry point & CI

| ID | Module | Implements | Deps |
|----|--------|-----------|------|
| **M14** | `cli` (`xtask` main) | [`orchestrator.md` §xtask surface](orchestrator.md#the-xtask-surface), [§Credentials](orchestrator.md#credentials) | M12, M13 |
| **M15** | `workflows` | [`orchestrator.md` §GitHub Actions wiring](orchestrator.md#github-actions-wiring) | M14 (the CLI contract) |

M14: clap parsing, interactive prompts for non-secret inputs, `--*-token-env`
resolution (read value from the named env var; never accept a literal token),
`--push`, `--yes`. M15: `prepare-release.yml` (workflow_dispatch, passes `--push`,
sets up `cargo-semver-checks` + WASM/npm toolchain) and `release.yml`.

### Wave 5 — end-to-end on the toy-kv fixture

| ID | Step | Goal |
|----|------|------|
| **E1** | local prepare dry-run | `cargo xtask prepare-release --version <next>` **without** `--push`: a local commit + printed PR body, nothing remote. Verify the single commit contains version bump + changelogs + npm update. |
| **E2** | release dry-run | exercise the idempotency scan + dependency order + orphan-tag logic without publishing for real (mock/registry-sandbox or `--dry-run` path if added). |

Wave 5 produces `artifacts/e2e-report.md`.

---

## 7. Parallelization plan

```
Wave 1:  [M1 M2 M3 M4 M5 M6 M7 M8]   ← 8 module pipelines (A→B→C→D) in parallel
            │ (contracts ready)
Wave 2:  [M9 M10 M11]                ← start each module's Contract agent as soon
            │                          as its deps' CONTRACTS exist (not full build)
Wave 3:  [M12 M13]
Wave 4:  [M14] then [M15]
Wave 5:  E1, E2
```

- Within a wave, run every module's pipeline concurrently.
- A module's **Contract (A)** may begin once its dependencies' **contracts** are
  published — earlier than their builds. Builders (B) still wait for upstream
  builds to be integrated-green before a wave's integration step.
- After each wave: run the **Integration agent** and require a green
  `wave-<n>-integration.md` before the next wave's Builders start.

---

## 8. Version control — how agents commit the work

The build's own git history must be clean, attributable per step, and must never
touch `release-spec/`.

1. **Conventional Commits — we dogfood our own parser.** Every implementation
   commit follows `type(scope): subject`, because the tool being built parses
   exactly this format (for changelogs + breaking-change detection). Using it now
   keeps toy-kv's history consistent with its existing convention (see the repo
   `README.md` → "How it was built": *each step is a single conventional commit*)
   and gives the finished tool real history to run against. **Scope = the module
   name**, e.g. `feat(xtask-version): …`, `test(xtask-config): …`. Commit messages
   end with the repo's required `Co-Authored-By` trailer.

2. **One commit per pipeline step** (A/B/C/D), each bundling the step's code/test
   change **and** its artifact, so every artifact has a matching commit:
   - Contract (A): `docs(xtask-<m>): contract for <module>` → commits
     `artifacts/<m>/contract.md`.
   - Builder (B): `feat(xtask-<m>): implement <module>` → commits the `xtask/`
     source + `impl-report.md`.
   - Style (C): `style(xtask-<m>): …` (only if it changed anything) +
     `style-report.md`.
   - Test (D): `test(xtask-<m>): unit tests for <module>` → tests +
     `test-report.md`.

3. **One branch + worktree per module — never share a working tree across parallel
   agents.** Modules in the same wave run concurrently and would collide on shared
   files. Each module pipeline runs on its own branch `impl/M<n>-<name>` in its own
   git worktree. (When spawning a builder with the Agent tool, use
   `isolation: "worktree"`.) Parallel agents never operate in the same tree.

4. **The per-wave Integration agent owns the "stitch points."** Individual modules
   must **not** each rewrite the connective files — that guarantees merge
   conflicts. Instead, a builder *declares* what it needs in its `impl-report.md`
   (e.g. "needs `mod version;` in `lib.rs` and dep `semver = "1"` in
   `xtask/Cargo.toml`"), and the Integration agent applies all of them **once**
   when merging the wave's module branches into the integration branch
   (`impl/wave-<n>`). The stitch points are: `xtask/Cargo.toml`
   (`[dependencies]`, workspace `members`) and `xtask/src/lib.rs` (`mod …;`
   declarations).

5. **Never stage or commit anything under `release-spec/`** (Hard rule 1).
   `git status release-spec/` must be clean before every commit; if a diff appears
   there, reject the commit and revert.

6. **Commit the artifacts** — they are the audit trail / "how it was built" record.
   **Never** commit secrets: tokens live only in env vars (see the Credentials
   spec), never in code, config, or commit messages.

7. **No pushing and no PRs as part of the build** unless a human asks. The work
   lands on local branches; a human opens the PR for the whole tool when ready.
   (This is the build-time git workflow — distinct from the release tool's own
   runtime `--push`, which is a feature of the tool, not this process.)

---

## 9. Definition of done

**Per module:** contract.md exists and matches the final API; code compiles;
`cargo fmt --check` and `cargo clippy -D warnings` clean; unit tests pass and map
to every spec-mandated rule; all four artifacts present; INDEX.md row = `tested`.

**Per wave:** `wave-<n>-integration.md` is green (whole crate builds, full suite +
clippy pass, no unresolved cross-module contract mismatches).

**Overall:** both subcommands run on the toy-kv fixture per Wave 5; `release.toml`
exists; the two workflows exist; **nothing under `release-spec/` was modified**
(verify with `git status release-spec/`).

---

## 10. Quick checklist for any agent before it starts

- [ ] I have read [`orchestrator.md`](orchestrator.md) and my module's sub-spec(s).
- [ ] I am consuming **only** the artifacts my role is allowed to (Hard rule 3).
- [ ] I will write code under `xtask/` and my artifact under `artifacts/<m>/` —
      **never** under `release-spec/`.
- [ ] If the spec is ambiguous, I will record a **spec question** in my artifact and
      stop, rather than edit the spec or guess silently.
- [ ] I work on my module's branch/worktree `impl/M<n>-<name>`, and I will land my
      step as a single **Conventional Commit** (`type(xtask-<m>): …`) bundling its
      artifact — without touching the stitch files or `release-spec/` (§8).
- [ ] I will update my row in `artifacts/INDEX.md` when I finish.
