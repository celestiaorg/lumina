# Breaking-Change Detection (Diagnostic Only)

This is a sub-spec of the in-house release process. The master spec is
[`orchestrator.md`](orchestrator.md); where the two disagree, the orchestrator
wins. This file specifies how the `cargo xtask prepare-release` subcommand
decides, **per crate**, whether a release contains breaking changes.

## This does NOT influence the version

> **The breaking-change result has zero effect on the version.** Per
> [`orchestrator.md`](orchestrator.md) Flow 1, the next version is **chosen by the
> user** and validated independently (see the orchestrator's
> [Version validation](orchestrator.md#version-validation) section). Breaking-change
> detection never bumps, escalates, or vetoes a version.

Its only output is a **diagnostic** that annotates the release-PR description. For
each package we produce a boolean ("has breaking changes") plus a short
human-readable reason, and hand both to [`pr-body-logic.md`](pr-body-logic.md),
which renders them into the PR body. Nothing downstream of the PR consumes this
result.

Because the version is decoupled from this analysis, there are no
version-precedence or component-reset rules in this document — we don't bump at
all. We only report.

## Two complementary signals

We combine two independent signals; **either one alone** is enough to mark a crate
breaking:

1. **API-level breakage (primary)** — `cargo-semver-checks` diffs the crate's
   public API against its last published version.
2. **Intent-level breakage (secondary)** — author-declared breaking changes in
   Conventional Commits (`feat!:` / `BREAKING CHANGE:` footer).

The API signal is authoritative for libraries; the intent signal catches breakage
the author flagged that the API differ can't see (e.g. a behavioral break with an
unchanged signature, or a binary-only crate where there is no library API to
diff).

> Allowed because it is not `release-plz`: per the orchestrator's
> [guiding principle](orchestrator.md#goals--principles), the one hard rule is "no
> `release-plz`." `cargo-semver-checks` is a standalone diagnostic tool, so we use
> it directly.

## Baseline

The comparison baseline for a crate is its **last published version**, determined
exactly the way the orchestrator determines the current version: from a
combination of **published registry versions** and **git tags** (the highest
version that actually exists), not from the working-tree `Cargo.toml`. See
[`orchestrator.md`](orchestrator.md) Flow 1 step 2 and
[Version validation](orchestrator.md#version-validation). Reusing that same logic
keeps "what changed since the baseline" consistent across changelog generation,
versioning, and this diagnostic.

A crate that has **never been published** has no baseline; its diagnostic is
"first release — no prior version to compare" and `has_breaking = false`. Neither
signal runs for it.

## Signal 1: API breakage via `cargo-semver-checks`

For each crate that has a published baseline **and a library target**, we run
`cargo-semver-checks` to diff the local public API against the baseline:

```
cargo semver-checks check-release \
    --manifest-path <crate>/Cargo.toml \
    --baseline-version <last-published-version>
```

(`--baseline-version` lets the tool fetch the published baseline from the registry
itself; alternatively `--baseline-root <path>` points at an already-downloaded
baseline crate. Either is fine — pick one when implementing.)

Result interpretation:

- **exit 0** → API compatible.
- **non-zero with reported incompatibilities** → breaking; capture the tool's
  human-readable incompatibility list (ANSI-stripped) as the reason.
- **tool not installed / other error** → fail open: treat as "API signal
  unavailable" (do not crash the prepare step), and fall back to the intent signal
  alone. Log a warning so it's visible in CI.

When it applies:

- **Library crates only.** Binary-only crates and the thin `cdylib`
  (`toy-kv-wasm`) have no meaningful Rust library API to diff — they are
  `Skipped` for this signal and rely on the intent signal.
- **Only when there are changes** since the baseline (the same "should update"
  condition the changelog/diff step uses) — no point diffing an unchanged crate.

`cargo-semver-checks` is a normal external tool; ensure it is installed in the
prepare-release CI job (e.g. a setup step), same as `wasm-pack`.

## Signal 2: intent breakage from Conventional Commits

We also scan the commits that touch a crate since its baseline (the same commit
set the changelog step walks) and mark the crate breaking if **any** of those
commits is breaking by either Conventional Commits rule:

1. **The `!` marker** in the header, before the colon — e.g. `feat!: …`,
   `fix!: …`, or with a scope `feat(core)!: …`.
2. **A `BREAKING CHANGE:` or `BREAKING-CHANGE:` footer** in the commit body (the
   two spellings are equivalent per the Conventional Commits spec).

Any commit type may carry these markers (`refactor!:`, `chore!:` count too). A
commit without either marker contributes nothing to this signal.

Parsing is shared with the changelog generator (see
[`changelog-generation.md`](changelog-generation.md)) — we parse each commit's
header and footers once and reuse the result. A `semver`/conventional-commit
helper crate is fine here; the point is only that we own the logic, not that we
hand-roll the parse.

### Reason string (intent signal)

- If a commit has a `BREAKING CHANGE:`/`BREAKING-CHANGE:` footer, use that footer's
  text as the reason (first such footer wins).
- Otherwise, if a `!`-marked commit triggered it, cite the commit subject, e.g.
  `breaking commit: "feat(core)!: change get() signature"`.

## Combining the signals

For each crate, `has_breaking = api_breaking || intent_breaking`. The reason
prefers the most specific available text:

1. If the API signal found incompatibilities, lead with them (they name the exact
   incompatible items).
2. Otherwise use the intent reason (footer text or the breaking commit subject).
3. If both fired, include both — the API items and the author's note.

## Per-package result

For each package the step yields:

```text
{
  has_breaking: bool,        // api_breaking || intent_breaking
  reason: Option<String>,    // human-readable explanation when has_breaking
  api_status: ApiStatus,     // Compatible | Incompatible | Skipped | Unavailable
}
```

Example results for the toy-kv workspace:

| Package | Crate dir | Example outcome |
| --- | --- | --- |
| `toy-kv-utils` | `utils/` | `api_status = Compatible`, `has_breaking = false` |
| `toy-kv` | `core/` | `api_status = Incompatible`, `has_breaking = true` — `cargo-semver-checks` flags a changed `get()` signature (corroborated by `feat(core)!:`) |
| `toy-kv-wasm` | `wasm/` | `api_status = Skipped` (cdylib), `has_breaking = false` — no breaking commits |

These per-package results are passed to [`pr-body-logic.md`](pr-body-logic.md),
which is solely responsible for formatting them (e.g. a
`### ⚠️ \`toy-kv\` breaking changes` block). This spec defines only the *data*, not
its presentation.

## Summary

Breaking-change detection is **diagnostic-only** and combines two signals:
machine-verified **API diffing via `cargo-semver-checks`** against each crate's
last published baseline (library crates), plus **author-declared intent** from
Conventional Commits (`!` marker / `BREAKING CHANGE:` footer). Either signal marks
a crate breaking; the result is a per-crate `{ has_breaking, reason, api_status }`
consumed by [`pr-body-logic.md`](pr-body-logic.md). It never influences the
user-chosen version. If `cargo-semver-checks` is missing or errors, the step fails
open to the intent signal rather than aborting the release prep.
