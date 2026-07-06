# Per-package changelog generation

This sub-spec defines how **our own `xtask` code** generates and refreshes the
per-package `CHANGELOG.md` files during the Prepare flow. It is driven by
[`orchestrator.md`](orchestrator.md) — see Prepare Flow **step 6**. The
body-only output it produces is consumed by [`pr-body-logic.md`](pr-body-logic.md)
for the release-PR description.

This is a spec for **our in-house implementation**. The one rule is no
`release-plz` — and, specifically here, **no `git-cliff`**: we own the changelog
format rather than delegating to a template engine. We read commits via `git`,
parse them as Conventional Commits, and render with a fixed markdown layout.
Ordinary helper crates (e.g. `semver`, a conventional-commit parser) are fine;
what matters is that the generator is ours.

## Packages and their directories

`toy-kv` is a single-version workspace with three crates plus an npm wrapper.
Each has a directory the changelog scopes commits to:

| Package | Crate name | Directory |
| --- | --- | --- |
| utils | `toy-kv-utils` | `utils/` |
| core | `toy-kv` | `core/` |
| wasm bindings | `toy-kv-wasm` | `wasm/` |
| npm wrapper | `toy-kv` (npm) | `wasm/js/` |

Each package owns one `CHANGELOG.md` next to its manifest (e.g.
`utils/CHANGELOG.md`, `core/CHANGELOG.md`, `wasm/CHANGELOG.md`). We generate a
changelog **per package**, not one global file, so each crate's published notes
describe only the changes that touched *that* crate.

## End-to-end flow

For each package, in this order:

1. **Find the package's last release point** — the git ref of its last released
   version (its last tag, falling back to the start of history if the package
   has never been released).
2. **Collect the package's commits** — every commit since that point that
   touched the package's directory, using path-scoped `git log`.
3. **Parse + group** each commit message with our minimal Conventional Commits
   parser into Keep-a-Changelog groups.
4. **Render** the new entry with the fixed markdown layout (`## [version]`
   heading + `### Group` sections), newest-first.
5. **Prepend** the new entry to the existing `CHANGELOG.md`, preserving any
   hand-edited header.
6. **Emit a body-only copy** of the entry (no heading) for the PR description.

## Collecting commits (path-scoped `git log`)

We never load the whole repo history. Each package's entry contains **only the
commits that touched its directory**, so a change confined to `utils/` does not
appear in `core/CHANGELOG.md`.

We invoke `git log` with a stable, machine-readable format and a pathspec:

```
git log <last-release>..HEAD --no-merges --pretty=format:%H%x1f%s%x1e -- <dir>
```

- `<last-release>..HEAD` — only commits since the package's last release. When
  the package has no prior release, omit the left side and walk all of history
  (`HEAD -- <dir>`).
- `--no-merges` — merge commits carry no useful changelog message.
- `--pretty=format:%H%x1f%s%x1e` — hash and **subject** (first line only)
  separated by `0x1f` (unit separator), records terminated by `0x1e` (record
  separator). Using control bytes avoids ambiguity with any character that can
  appear in a commit subject.
- `-- <dir>` — the **pathspec** that scopes the log to the package directory
  (`utils/`, `core/`, `wasm/`, or `wasm/js/` for the npm wrapper). This is the
  whole point: path-scoping is what makes the changelog per-package.

`git log` already returns commits **newest-first**, which is the order we render
in, so no re-sorting is needed.

## Minimal Conventional Commits parser

We parse the **subject line only** (`%s`). The grammar we accept is the
[Conventional Commits](https://www.conventionalcommits.org/) prefix:

```
<type>[optional scope][!]: <description>
```

Parsing rules:

1. Split the subject on the first `:`. If there is no `:`, the message is
   **non-conventional**.
2. The text before `:` is the **type token**. Strip an optional `(scope)` and an
   optional trailing `!` (breaking marker) to get the bare type. Lowercase it.
3. The text after `:` (trimmed) is the **description** used as the bullet.
4. Map the bare type to a Keep-a-Changelog group (below). Any type that does not
   match a known prefix falls into **Other**.
5. **Non-conventional messages** (no `:`): take the first line of the subject
   verbatim and place it under **Other**.

We deliberately do not parse trailers, footers, or bodies — only the subject —
because that is all the rendered bullet needs.

### Type → group mapping

| Commit type | Changelog group |
| --- | --- |
| `feat` | Added |
| `changed` | Changed |
| `deprecated` | Deprecated |
| `removed` | Removed |
| `fix` | Fixed |
| `security` | Security |
| *(anything else, and non-conventional)* | Other |

Group **ordering** in the rendered entry is fixed: Added, Changed, Deprecated,
Removed, Fixed, Security, Other. A group with no commits is omitted entirely.
Within a group, commits keep `git log` order (newest-first).

## Fixed markdown render (no templates)

The new entry is assembled by direct string building — there is no template
engine. The layout is:

```markdown
## [<version>] - <YYYY-MM-DD>

### <Group>

- <description>
- <description>

### <Group>

- <description>
```

- The heading is `## [<version>] - <date>`, where `<version>` is the new
  workspace version and `<date>` is the release date (see below).
- Each non-empty group becomes a `### <Group>` section; each commit becomes a
  `- <description>` bullet.
- Sections and the entry as a whole are newest-first.

### Release date

The release date defaults to the **current UTC date**, formatted `YYYY-MM-DD`.
We compute it from the system clock in UTC. A caller may pass an
explicit date to override it (used for reproducible runs), but the default is
"today, UTC".

## Header-preserving prepend

We never blindly overwrite `CHANGELOG.md`. We keep whatever header the file
already has (which may have been hand-edited) and splice the new entry directly
beneath the `## [Unreleased]` line.

Algorithm:

1. **Read** the existing `CHANGELOG.md` (if any).
2. **Parse the header** — scan from the top up to **and including** the
   `## [Unreleased]` line. The leading `# Changelog` title is matched
   **case-insensitively** (`# Changelog`, `# CHANGELOG`, `# changelog` all
   count). Everything from the start of the file through the `## [Unreleased]`
   line is the *header*; everything after it is the *old body*.
3. **Compose** the result as the concatenation:

   ```
   {header}{new_entry}{old_body}
   ```

   The new entry is inserted between the preserved header and the previously
   existing entries, so the newest release is always on top and the
   hand-editable header survives untouched.

### Fallback when no header parses

If the file does not exist, is empty, or has no recognizable `# Changelog`
header / `## [Unreleased]` line, there is no header to preserve. In that case we
synthesize the file as:

```
{default_header}{new_entry}
```

using the default Keep-a-Changelog header text below. This guarantees a
well-formed changelog even on a package's very first release.

### Default header

When we have to create a header, we emit the standard Keep-a-Changelog text:

```markdown
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
```

## Body-only output

In addition to writing the file, we return the new entry **without its `## [...]`
heading** — just the `### Group` sections and their bullets. This header-less
body is what [`pr-body-logic.md`](pr-body-logic.md) embeds in the release-PR
description, so the PR shows the same notes that landed in each `CHANGELOG.md`
without duplicating the version heading (the PR lists the version transition
separately).

Concretely, generation yields two artifacts per package:

- **File entry** — heading + groups, spliced into `CHANGELOG.md`.
- **Body-only entry** — the same groups/bullets with the heading stripped, for
  the PR body.

## Summary

For each package we path-scope `git log` to the package's directory, parse each
commit subject as a Conventional Commit, group the
commits Keep-a-Changelog style (newest-first), render a fixed `## [version] -
date` markdown entry, and splice it beneath a preserved (or default) header in
the package's `CHANGELOG.md`. We also emit a heading-less copy of the entry for
the release PR. Our own generator — no `release-plz`, no `git-cliff` template
engine (helper crates are fine). Driven by [`orchestrator.md`](orchestrator.md) step 6;
body-only output consumed by [`pr-body-logic.md`](pr-body-logic.md).
