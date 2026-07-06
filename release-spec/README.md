# `release-spec/` — frozen specification for the release tool

> ## ⛔ DO NOT MODIFY ANYTHING IN THIS FOLDER
>
> This folder is the **immutable source of truth** for the in-house release tool.
> Implementer agents (and the humans driving them) **must never create, edit,
> rename, move, or delete any file under `release-spec/`** while implementing.
> Code goes in `xtask/`; handoff artifacts go in `artifacts/`. If a spec is wrong
> or ambiguous, **raise a spec question in an artifact and stop** — do not change
> the spec. The spec changes only by deliberate human action, outside the build
> process. See [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md) §0.

## What's here

| File | Role |
|------|------|
| [`orchestrator.md`](orchestrator.md) | **Master spec** — the two flows, version rules, credentials, config, portability. Source of truth; sub-specs defer to it. |
| [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md) | **How to build it** — module decomposition, dependency waves, agent pipeline, artifact contract, commit workflow, the immutability rule. |
| [`ORCHESTRATOR-GUIDE.md`](ORCHESTRATOR-GUIDE.md) | **How to drive the build** — operating manual for the orchestrator agent: bootstrap, the per-wave loop, worker-agent prompt templates, problem handling. Paste this to kick off the build. |
| [`breaking-change-detection.md`](breaking-change-detection.md) | Diagnostic-only breaking-change detection (`cargo-semver-checks` + intent). |
| [`changelog-generation.md`](changelog-generation.md) | In-house per-package `CHANGELOG.md` generation. |
| [`npm-release-pr-steps.md`](npm-release-pr-steps.md) | Build WASM + update the npm package during *prepare* (optional component). |
| [`pr-body-logic.md`](pr-body-logic.md) | Release-PR description generation. |
| [`publish-crates-logic.md`](publish-crates-logic.md) | Publishing crates in dependency order, idempotency, tagging. |
| [`release-step.md`](release-step.md) | Publishing the npm packages at release time (optional component). |

## Where to start

- **Driving the build?** Start at [`ORCHESTRATOR-GUIDE.md`](ORCHESTRATOR-GUIDE.md);
  it points you to the rest.
- **Understanding the tool?** Read [`orchestrator.md`](orchestrator.md).
- **A worker agent on a module?** Read [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md)
  and your module's sub-spec, then pick up your row from `artifacts/INDEX.md`.
