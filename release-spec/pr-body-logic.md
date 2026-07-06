# Release-PR Description Generation

This is a sub-spec of the in-house release process. The master spec is
[`orchestrator.md`](orchestrator.md); where the two disagree, the orchestrator
wins. This file specifies how the `cargo xtask prepare-release` subcommand builds
the **title** and **markdown body** of the release PR — [`orchestrator.md`](orchestrator.md)
Flow 1 step 9.

It is **our own Rust code**. No `release-plz` and no template engine — the body is
assembled with plain Rust string building (`String::push_str` / `write!` into a
`String`). Ordinary helper crates are fine; we just don't delegate the PR layout
to a templating dependency.

## What the body must show

Per [`orchestrator.md`](orchestrator.md) Flow 1, the PR description lists, for
**every** package in the workspace:

- the package name,
- its **previous version → new version** (previous = the last published/tagged
  version for that package; new = the single workspace version being released),
- a **breaking-change diagnostic** for that package.

Breaking-change status is **diagnostic only**: it annotates the PR and has **zero
effect on the version** (the version is user-chosen and validated by the
orchestrator). See [`breaking-change-detection.md`](breaking-change-detection.md).

## Input

The generator runs after steps 4–8 of Flow 1, so all per-package data has already
been assembled in memory. It takes a slice of one record per workspace package:

```rust
struct PackageReport {
    name: String,            // e.g. "toy-kv-utils", "toy-kv", "toy-kv-wasm"
    previous_version: String,// last published/tagged version, or "—" for first release
    new_version: String,     // the single workspace version (same for all packages)
    breaking: bool,          // diagnostic flag from breaking-change-detection.md
    breaking_reason: String, // short human-readable reason (or "first release …")
    changelog_body: String,  // body-only changelog entry (no header/footer/links)
}
```

Notes on the fields:

- `previous_version` / `breaking` / `breaking_reason` come from the baseline logic
  in [`breaking-change-detection.md`](breaking-change-detection.md) (registry +
  git tags). A never-published crate has no baseline: `previous_version` renders as
  `—` and the diagnostic is "first release".
- `changelog_body` is the **body-only** new-entry text from
  [`changelog-generation.md`](changelog-generation.md) — the Keep-a-Changelog sections (`### Added`,
  `### Changed`, …) of the new entry, **without** the file header, the version
  heading, or the link-reference footer. It is *not* a copy of the whole
  `CHANGELOG.md`. May be empty if a package had no changes.

Because the workspace has a single version (orchestrator principle 4),
`new_version` is identical across all records; the generator reads it once for the
title from the first record.

## Output: the body layout

A fixed Markdown format we control. Concretely, in order:

### 1. Summary heading + per-package version table

```markdown
## Release v0.2.0

| Package | Version | Status |
| --- | --- | --- |
| `toy-kv-utils` | 0.1.0 -> 0.2.0 | ⚠ breaking |
| `toy-kv` | 0.1.0 -> 0.2.0 | ✓ compatible |
| `toy-kv-wasm` | — -> 0.2.0 | ✓ compatible |
```

- One row per package, **always**, even if `previous_version == new_version`.
- The `Status` cell is `⚠ breaking` when `breaking == true`, else `✓ compatible`.
  (A bullet list — `` * `pkg`: prev -> new (status) `` — is an equally valid
  rendering; the table is the chosen default.)
- For a first release, the previous column shows `—`.

This summary is the **mandatory core** of the body and is never dropped (see
[Length safety](#length-safety)).

### 2. Per-package breaking-change detail (only when breaking)

For each package whose `breaking` is `true`, emit a detail block carrying the
diagnostic reason verbatim inside a fenced block so it can't break Markdown:

```markdown
### ⚠ `toy-kv-utils` breaking changes

```text
removed pub fn `ToyKvError::from_str`; signature of `validate_key` changed
```
```

Packages with `breaking == false` contribute nothing here.

### 3. Collapsible Changelog section

A single collapsible `<details>` block embedding each package's body-only
changelog entry. Packages with an empty `changelog_body` are skipped. When more
than one package has changelog content, a per-package `#### \`pkg\`` sub-heading
precedes each entry; with a single package the sub-heading may be omitted.

```markdown
<details>
<summary><b>Changelog</b></summary>

#### `toy-kv-utils`

### Added
- shared `ToyKvError`, JSON helpers, and `validate_key`.

#### `toy-kv`

### Added
- BTreeMap-backed store with `set`/`get`/`delete`/… and `set_checked`.

</details>
```

If no package has changelog content, the whole `<details>` section is omitted.

There is **no** "generated with …" footer.

## Length safety

GitHub enforces a **65,536-character** limit on PR-body length. Treat this as a
real constraint and degrade gracefully, in this order:

1. **Full render.** Build summary + breaking-change details + changelog
   `<details>`. If it fits within the limit, use it.
2. **Drop changelogs.** If over the limit, re-render **without** the collapsible
   Changelog section (step 3 above), keeping the version summary table and the
   breaking-change detail blocks. The bulky changelog text is what overflows, so
   shedding it usually suffices while preserving the load-bearing information.
3. **Hard truncate.** As a last resort, if it is *still* over the limit,
   truncate to the limit at a **valid UTF-8 character boundary** (never split a
   multi-byte char — e.g. back off to the last `char_indices` boundary `<=` the
   cap) and append a short `…` / truncation marker.

The summary table is small and bounded by the package count, so step 2 reliably
brings normal releases under the cap; step 3 only guards pathological inputs.

## The title

A simple fixed format — no templates. Because the workspace ships a single
version (orchestrator principle 4):

```
chore: release v<version>
```

e.g. `chore: release v0.2.0`. `<version>` is the shared `new_version`. That is the
only form needed; there is no per-package title because every package moves
together at one version.

## Branch name and opening the PR

The release-PR **branch name is `<prefix><version>`** (e.g. `release-0.2.0`) and is
set by the **orchestrator**, *not* here — see [`orchestrator.md`](orchestrator.md)
Flow 1 (Inputs and step 1). This generator owns only the **title** and **body**
strings.

Opening/updating the PR is done by the prepare step **only when run with
`--push`** (orchestrator Flow 1 step 10): it posts `title`, `body`, `head` (= the
release branch), and `base` (= the default branch) to the GitHub API. On a refresh
of an existing release PR, it rewrites the title/body only when they changed.
Without `--push`, this generator still runs and the body is printed to stdout, but
nothing is posted to GitHub.

## Related sub-specs

- [`orchestrator.md`](orchestrator.md) — master spec; defines Flow 1, the single
  workspace version, the branch name, and where this step sits (step 9).
- [`breaking-change-detection.md`](breaking-change-detection.md) — source of the
  per-package `breaking` flag and `breaking_reason` (diagnostic only).
- [`changelog-generation.md`](changelog-generation.md) — source of each package's body-only changelog
  entry embedded in the Changelog section.
