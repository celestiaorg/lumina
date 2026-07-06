# Publishing crates in order, tagging, idempotency

Sub-spec for the **crate-publishing step** of `cargo xtask release` — step 2 of
Flow 2 in [`orchestrator.md`](orchestrator.md). It publishes every publishable
workspace crate to the registry in dependency order, waits for each to index,
then creates its git tag and GitHub release. It is fully idempotent: re-running
after a partial or interrupted release does only the work that is still missing.

After all crates are published and tagged, the release proceeds to the npm step
— see [`release-step.md`](release-step.md).

This is a spec for **our own** `xtask` implementation. The orchestration logic
here is ours, not a wrapper around `release-plz`; we drive `git` and `cargo`
directly and may use ordinary helper crates (`semver`, `serde`/`toml`, a git
crate, etc.). The one hard rule is **no `release-plz`**.

## The workspace

`toy-kv` ships three crates with a single workspace version. Their in-workspace
dependency order is fixed:

| Crate | Path | Depends on (in-workspace) |
| --- | --- | --- |
| `toy-kv-utils` | `utils/` | — (foundational) |
| `toy-kv` | `core/` | `toy-kv-utils` |
| `toy-kv-wasm` | `wasm/` | `toy-kv` (with the `wasm-bindgen` feature) |

Intra-workspace dependencies pin the exact workspace version
(`{ version = "=<v>", path = "…" }`), so a dependent must never be published
before the dependency it pins.

## Inputs (from orchestrator Flow 2)

1. **Commit SHA** — the commit to release *from* and to place every git tag *on*
   (normally the merge commit of the release PR). All tags and GitHub releases
   created by this step point at this SHA.
2. **Registry token** — read from an **environment variable** whose name is
   passed to the subcommand (`--registry-token-env`, e.g. `CARGO_REGISTRY_TOKEN`).
   The token value is **never** accepted as a literal CLI argument and never
   logged. We pass it to `cargo publish` via the environment, not on the command
   line.
3. **GitHub token** — read from the env var named by `--github-token-env`. This is
   what authorizes **pushing the git tags and creating the GitHub releases** in the
   per-crate flow below; crate publishing itself uses the registry token, but the
   tag/release side needs repo write access (`contents: write`). The default
   Actions `GITHUB_TOKEN` is sufficient. See the orchestrator's
   [Credentials](orchestrator.md#credentials) section. Same env-var-name rule: no
   literal token on the CLI.

## Compute the publish order ourselves

We do not rely on any external tool to order the crates. We compute the order as
a **topological sort** of the workspace crates by their in-workspace
dependencies, so each crate is published only after every crate it depends on:

1. Read the workspace members and, for each member, the set of its dependencies
   that are **also workspace members**. We consider both **normal** and **build**
   dependencies (`[dependencies]` and `[build-dependencies]`). Dev-dependencies
   are ignored — they do not affect what must be on the registry to build a
   published crate — unless a feature pulls a normal/build dep in, in which case
   that dep is already captured as a normal/build edge.
2. Build a directed graph: an edge `A → B` means "A depends on B", i.e. B must be
   published before A.
3. Topologically sort it so every dependency precedes its dependents. For
   `toy-kv` this yields:

   ```
   toy-kv-utils → toy-kv → toy-kv-wasm
   ```

4. **Cycle detection.** If the graph contains a dependency cycle (no valid
   topological order exists), **abort with an error** naming the crates involved.
   A cycle is unpublishable and indicates a malformed workspace.

After ordering, we filter to **publishable** crates only — any crate whose
manifest sets `publish = false` is dropped from the work list (its dependents, if
publishable, still publish in the remaining order).

## Idempotency scan (per crate, before publishing)

Before doing any work for a crate we determine its current state with two checks:

- **(a) Does its git tag already exist?** Compute the crate's tag name and check
  whether that tag already exists (locally and on the remote).
- **(b) Is this version already on the registry?** Query via `cargo` — e.g.
  `cargo info <pkg>@<version>` (or equivalent registry/index lookup). A
  successful match means the exact `name@version` is already published.

These two booleans drive the per-crate decision below. The scan runs per crate as
we reach it in dependency order, so state observed for an earlier crate (e.g. one
we just published and indexed) is current by the time we evaluate a later one.

## Per-crate flow

For each publishable crate, in dependency order, branch on the idempotency scan:

### Corrected state table

| State when interrupted | Re-run behavior |
| --- | --- |
| **Tag exists (done)** | **skip** |
| **Not published, no tag** | **publish + tag** |
| **Published, no tag** | **CREATE the tag/release (do NOT skip)** |

The third row is the **critical fix** over the old behavior — see
[The orphan-tag fix](#the-orphan-tag-fix).

### "Not published, no tag" — full publish

1. **Publish.** Run `cargo publish` for the crate, with the registry token taken
   from the environment variable named in the inputs (passed via the environment,
   never on the command line). The manifest's `=<v>` pins guarantee the crate
   resolves the already-published versions of its in-workspace dependencies.
2. **Treat the "already exists" race as success.** If `cargo publish` fails but
   its output indicates the version was *already uploaded* / *already exists*
   (another runner or an earlier attempt won the race between our scan and our
   upload), treat that as a successful publish and continue — do not error.
3. **Wait for the registry to index the new version.** Publishing returns before
   the version is queryable. Poll the registry (the same `cargo info
   <pkg>@<version>` check as the scan) on a short interval until the new version
   appears, or until a **timeout** elapses. If the timeout is hit, abort with an
   error (a re-run will resume — the upload itself already succeeded). We must not
   create the tag before the version is indexed, otherwise a dependent crate could
   try to publish against a not-yet-resolvable dependency.
4. **Create the git tag (+ GitHub release)** on the input SHA. The tag points at
   the provided commit; the GitHub release is cut from that tag, with its body
   drawn from the crate's changelog entry and its pre-release flag derived from the
   semver (an `-rc.N` version is a pre-release).

### "Published, no tag" — tag only (the fix)

The crate's version is already on the registry but its tag is missing. We do
**not** skip it. We **create the missing git tag and GitHub release** on the
input SHA (step 4 above), exactly as if we had just published it. No upload is
attempted. This closes the orphan-tag gap.

### "Tag exists" — skip

If the git tag already exists, the crate is fully done (tag implies the version
was published before the tag was created). Skip it entirely and move on.

## The orphan-tag fix

The old `release-plz`-style logic had a genuine correctness gap. Within a single
crate the order is: `cargo publish` → wait for indexing → create tag/release. If
the process died **after the upload but before the tag was created**, the crate
was left on the registry with **no git tag and no GitHub release**. On the next
run its idempotency check saw "already published" and **skipped the crate
entirely**, so the tag and release were *never* created — the crate stayed
orphaned until a human fixed it by hand. There was no code path for "on the
registry but tag missing → just create the tag."

**Our design fixes this.** The two idempotency booleans are evaluated
*independently*, and "published" no longer short-circuits to "skip." When a crate
is **published but has no tag**, we fall into the tag-only branch and **create the
missing tag and release** rather than skipping. The only state that skips is *tag
exists*. Because tag creation is itself idempotent (creating an already-existing
tag is a no-op / handled gracefully), this is safe to re-run.

## Single-run abort & resume

Within one run, crates are published in dependency order and we **stop on the
first error**: if a crate fails to publish (a real failure, not the "already
exists" race) or its indexing times out, we abort the run and do not attempt
later crates *in that run* — a later crate may depend on the one that failed, so
continuing would be incorrect.

There is no checkpoint or state file. Resumability comes entirely from the
per-crate idempotency scan: on a re-run, crates that completed are skipped (tag
exists), crates that were published-but-not-tagged get their tag/release created,
and crates that were never reached get fully published. Each re-run therefore
makes **forward progress**, and repeated re-runs converge on "every crate
published and tagged."

## After this step

Once every publishable crate is published **and** tagged (with its GitHub release
cut), this step is complete and the orchestrator moves on to publishing the npm
packages — see [`release-step.md`](release-step.md). The master flow is in
[`orchestrator.md`](orchestrator.md) (Flow 2).
