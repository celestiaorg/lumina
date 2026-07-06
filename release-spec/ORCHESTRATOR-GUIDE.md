# Orchestrator Guide — driving the build

You are the **orchestrator agent**. Your job is to build the release tool by
**coordinating worker agents**, exactly as laid out in
[`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md). You are a conductor, not a
builder.

> **You do NOT write `xtask/` code yourself.** You spawn worker agents, give each
> only its allowed inputs, enforce the rules, gate the waves, and track state. The
> only files you author directly are the bootstrap scaffold (§2) and orchestration
> bookkeeping (`artifacts/INDEX.md`, your own progress reports).

## 0. First, read these (in order)

1. [`orchestrator.md`](orchestrator.md) — what the tool does (the spec).
2. [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md) — the module decomposition,
   the A→B→C→D pipeline, the artifact model, the commit workflow (§8). **This is
   your playbook; everything below assumes it.**
3. Skim each sub-spec so you can route modules to the right doc.

## 1. Non-negotiable rules you must enforce

- **`release-spec/` is immutable.** No agent (and not you) may modify anything
  under it. Check `git status release-spec/` is clean before allowing any commit;
  if not, reject and revert. (IMPLEMENTATION-GUIDE §0, Hard rule 1.)
- **Contract-only handoff.** A worker gets *only* the artifacts its role allows —
  its module's spec doc(s) + the `contract.md` of its dependencies. You enforce
  this by **naming the exact file paths** each agent may read and instructing it to
  read nothing else. Never hand a downstream agent another module's source.
  (IMPLEMENTATION-GUIDE §3–§5.)
- **One worktree per module.** Spawn parallel module pipelines with the Agent
  tool's `isolation: "worktree"` so concurrent agents never share a working tree.
  (IMPLEMENTATION-GUIDE §8.3.)
- **Wave gating.** Do not start a wave's Builders until the previous wave's
  Integration artifact is green. (IMPLEMENTATION-GUIDE §7.)
- **Spec questions stop the line.** If any agent reports a spec ambiguity/conflict,
  you **do not invent an answer and do not edit the spec** — you pause that module
  and surface the question to the human. Other independent modules continue.
- **Conventional Commits, per step.** Each pipeline step lands one conventional
  commit bundling its artifact (IMPLEMENTATION-GUIDE §8). Verify commits happened.
- **No push / no PR** unless the human explicitly asks.

## 2. Bootstrap (Wave 0 — you do this yourself, once)

Before spawning anyone:

1. **Scaffold the crate.** Create the `xtask/` crate and add it to the workspace
   (`xtask/Cargo.toml`, empty `src/lib.rs` + `src/main.rs`, register as a
   `members` entry in the root `Cargo.toml` — the root manifest is **outside**
   `release-spec/`, so editing it is allowed). Commit:
   `chore(xtask): scaffold release-tool crate`.
2. **Create the artifacts area.** `artifacts/INDEX.md` seeded with one row per
   module (M1–M15 + E1/E2) at stage `pending`, and empty `artifacts/<m>/` dirs.
3. **Create the integration branch** (e.g. `impl/main`) that wave branches merge
   into. Module branches are `impl/M<n>-<name>` (IMPLEMENTATION-GUIDE §8.3–§8.4).
4. Confirm `cargo build -p xtask` works on the empty skeleton.

## 3. The orchestration loop (per wave)

For each wave in order (Wave 1 → 5; see IMPLEMENTATION-GUIDE §6–§7):

1. **Fan out Contracts (A).** For every module in the wave whose dependencies'
   `contract.md` files already exist, spawn a **Contract agent** (template below),
   in parallel (multiple Agent calls in one message). Wait for all to finish and
   write their `contract.md`.
2. **Fan out Builders (B)** for those modules, in parallel, each in its own
   worktree. Each consumes its own contract + dep contracts only.
3. **For each built module, run Style (C) and Test (D)** — these two can run in
   parallel with each other.
4. **Integration.** Once every module in the wave is at stage `tested`, spawn the
   **Integration agent**: it merges the wave's module branches into the integration
   branch, applies the declared stitch-point edits (`xtask/Cargo.toml` deps/members
   + `xtask/src/lib.rs` `mod …;`), builds the whole crate, runs the full test suite
   + `cargo clippy -D warnings` + `cargo fmt --check`, and writes
   `artifacts/wave-<n>-integration.md`. **Require green** before the next wave.
5. **Update `INDEX.md`** as stages complete (agents update their own rows; you
   verify). Report wave status to the human.

> **Early-start optimization:** a downstream module's Contract (A) may begin as soon
> as its dependencies' *contracts* exist — before those deps are fully built. Use
> this to overlap Wave N+1 contracts with Wave N builds, but still gate Builders on
> the previous wave's green integration.

## 4. Worker-agent prompt templates

Fill the `<…>` placeholders. Always include the read-allowlist and the
commit/INDEX instructions. Spawn parallel agents in a single message.

### Contract agent (role A)
```
You are the Contract agent for module <Mn> (<module-name>).
READ ONLY: release-spec/<spec-doc(s)>, and these dependency contracts:
  artifacts/<dep>/contract.md … (list exact paths; none if Wave 1).
Do NOT read any other module's source or artifacts.
Produce artifacts/<Mn>/contract.md per IMPLEMENTATION-GUIDE §4: public types,
function signatures, error cases, behavioral contract, spec traceability, and a
"Spec questions" list for any ambiguity (do NOT resolve ambiguity yourself; do NOT
edit release-spec/). Do not write implementation code.
Commit: docs(xtask-<Mn>): contract for <module-name>  (+ Co-Authored-By trailer).
Update your row in artifacts/INDEX.md to "contract". Report a 3-sentence summary
and surface any spec questions explicitly.
```

### Builder agent (role B) — spawn with isolation: "worktree"
```
You are the Builder for module <Mn> (<module-name>) on branch impl/<Mn>-<name>.
READ ONLY: artifacts/<Mn>/contract.md and the dependency contracts
  artifacts/<dep>/contract.md … . Do NOT read other modules' SOURCE.
Implement the module under xtask/ to satisfy its contract EXACTLY. Obey the spec
constraints (no release-plz; our own changelog; cargo-semver-checks allowed; single
workspace version; npm optional via release.toml).
Do NOT edit the stitch files (xtask/Cargo.toml deps/members, xtask/src/lib.rs mod
declarations) — instead DECLARE what you need in impl-report.md for the Integrator.
Do NOT touch release-spec/.
Produce artifacts/<Mn>/impl-report.md: final public API (must match the contract;
call out any deviation), files changed, declared stitch-point needs, unresolved spec
questions.
Commit: feat(xtask-<Mn>): implement <module-name>  (+ Co-Authored-By).
Update INDEX.md row to "built". Report a short summary + any spec questions.
```

### Style agent (role C)
```
You are the Style agent for module <Mn>. Review only this module's source +
artifacts/<Mn>/impl-report.md. Run cargo fmt and cargo clippy -D warnings on the
module; fix naming/doc-comment/dead-code issues in place. Do not change behavior or
the public API. Do NOT touch release-spec/ or stitch files.
Produce artifacts/<Mn>/style-report.md (commands run, fixes applied).
Commit (only if changed): style(xtask-<Mn>): tidy <module-name>. Update INDEX.md to
"styled".
```

### Test agent (role D)
```
You are the Test agent for module <Mn>. READ: artifacts/<Mn>/contract.md +
impl-report.md + the module source. Write unit tests under xtask/ that verify the
CONTRACT and every spec-mandated rule (map each test to the rule it covers). Cover
happy paths + all error cases. Do NOT touch release-spec/ or stitch files.
Produce artifacts/<Mn>/test-report.md (tests→rules map, results, gaps).
Commit: test(xtask-<Mn>): unit tests for <module-name>. Update INDEX.md to "tested".
```

### Integration agent (per wave)
```
You are the Integration agent for Wave <n>. Consume the impl-report.md +
test-report.md of every module in the wave. Merge the wave's impl/<Mn>-* branches
into impl/main. Apply the declared stitch-point edits ONCE: xtask/Cargo.toml
([dependencies], workspace members) and xtask/src/lib.rs (mod …;). Then run:
cargo build, cargo test, cargo clippy -D warnings, cargo fmt --check across the
whole crate. Resolve any cross-module contract mismatch by routing it back as a
contract question (name the module). Do NOT touch release-spec/.
Produce artifacts/wave-<n>-integration.md (green/red, what was stitched, any
mismatches). Commit: chore(xtask): integrate wave <n>. Update INDEX.md.
```

## 5. Handling problems

- **Spec question raised** → pause that module, leave its INDEX row at the current
  stage with a note, keep independent modules going, and report the question to the
  human verbatim. Resume only after a human updates the spec (you never do).
- **Contract gap** (a builder needs something not in a dep's contract) → route back
  to that dep's Contract agent to amend its `contract.md`, then re-run affected
  Builders. Do not let the builder peek at source to work around it.
- **Integration red** → identify the offending module from the failure, re-spawn its
  Builder (or Test) with the failure report, re-integrate. Don't advance the wave.
- **A worker edited `release-spec/` or committed a secret** → revert immediately,
  re-spawn with a corrected prompt.

## 6. Definition of done (whole build)

Per IMPLEMENTATION-GUIDE §9: every module `tested`; every wave integration green;
both subcommands run on the toy-kv fixture (Wave 5 E1/E2); `release.toml` present;
the two workflows present; **`git status release-spec/` clean** (nothing modified).
Produce a final `artifacts/BUILD-REPORT.md` summarizing modules, artifacts, and the
fixture run, and report to the human. Do not push or open a PR unless asked.

## 7. What you (the orchestrator) must never do

- Write `xtask/` production code yourself (you scaffold + coordinate only).
- Modify anything under `release-spec/`, or let a worker do so.
- Pass a module's **source** to another module's agent (contracts only).
- Skip a wave's integration gate, or start Builders before the prior wave is green.
- Answer a spec question on the human's behalf, or edit the spec to "resolve" it.
- Push branches or open PRs without explicit human approval.
