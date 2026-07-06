# Building WASM + updating the npm package on the release PR

This is **Prepare · step 7** of the orchestrator (see
[`orchestrator.md`](orchestrator.md), Flow 1). It is implemented by our own
`xtask` (`cargo xtask prepare-release`), not by any third-party release tool.
It builds the WASM package and updates the npm wrapper so the npm side matches
the new workspace version.

> **Optional step.** This runs **once per configured npm component** and is
> **skipped entirely** when the workspace declares none — see
> [Portability & configuration](orchestrator.md#portability--configuration). The
> tool is generic; the concrete toy-kv values below (`toy-kv-wasm`, `wasm/`,
> `wasm/js/`, the `toy-kv` wrapper) are just what toy-kv's configured npm
> component resolves to. Another workspace supplies its own `wasm_crate` /
> `package_dir`; a crate-only workspace skips this file's steps and needs no
> `wasm-pack`/`npm` toolchain.

This step **produces changed files only**. It does **not** commit them: per the
orchestrator's "one commit" guarantee, the version bumps, changelogs, *and*
these npm files are all staged and committed together in a single commit by the
xtask (Prepare · step 8). There is no separate npm commit, no GraphQL
`createCommitOnBranch`, no PAT/curl/jq.

**No credentials are needed here.** Nothing is published during Prepare — we only
build WASM and edit local files. `npm install`/`npm clean-install` read the public
registry. The npm publish token is only used later, at release time
(see [`release-step.md`](release-step.md)). The GitHub token that pushes the
prepare commit is owned by the orchestrator (Flow 1), not this step.

## Layout

- `wasm/` — the `toy-kv-wasm` Rust crate (wasm-bindgen bindings).
- `wasm/pkg/` — the low-level wasm package produced by `wasm-pack build`
  (wasm binary + JS glue + `.d.ts`). Gitignored; never committed.
- `wasm/js/` — the ergonomic `toy-kv` npm wrapper, which depends on the
  generated `toy-kv-wasm` package via `file:../pkg`.

## Step by step

### 1. Resolve the target version (single source of truth)

The version is the **single workspace version** in `[workspace.package]`,
inherited by the `toy-kv-wasm` crate. Read it from cargo so there is no separate
npm version:

```bash
target_version="$(cargo pkgid --manifest-path=wasm/Cargo.toml | cut -d@ -f 2)"
```

This is the value the earlier prepare steps just bumped (Prepare · step 4). npm
inherits it.

### 2. Set the wrapper's `package.json` version (idempotent)

In `wasm/js`, set the `toy-kv` package version to `target_version`. **Skip if it
is already at the target** so the step never produces an empty change:

```bash
cd wasm/js
current="$(npm pkg get version | tr -d '"')"
if [ "$current" = "$target_version" ]; then
  echo "npm wrapper already at $target_version; nothing to do"
  exit 0
fi
npm version "$target_version" --no-git-tag-version
```

`npm version --no-git-tag-version` writes `version` into `package.json` (and
`package-lock.json`) without creating a git commit/tag — committing is the
orchestrator's job.

### 3. Build the wasm package

```bash
wasm-pack build wasm
```

Builds the `toy-kv-wasm` crate into `wasm/pkg/` (wasm binary + JS glue +
`.d.ts`). This is required so the next step resolves the freshly built
dependency at the new version. `wasm/pkg/` is gitignored and is **not**
committed here.

### 4. Update the lockfile against the fresh build

```bash
cd wasm/js
npm install --save ../pkg
npm clean-install
```

- `npm install --save ../pkg` installs the freshly built `../pkg` as the
  `toy-kv-wasm` dependency, updating `package-lock.json` so it resolves to the
  new version. The `package.json` entry stays `toy-kv-wasm: file:../pkg`.
- `npm clean-install` does a clean reinstall from the lockfile to validate that
  `package-lock.json` is consistent with the fresh build.

### 5. Regenerate the wrapper's type declarations

The `toy-kv` wrapper is hand-written plain JavaScript published as-is (mirroring
lumina's `node-wasm/js`): `index.js`/`worker.js` are the source. `tsc` is used
only to (re)generate the committed type declarations from their JSDoc:

```bash
cd wasm/js
npm run tsc
```

`tsc` runs with `emitDeclarationOnly` + `allowJs`, emitting `index.d.ts` (the
package's `types` output) from `index.js`'s JSDoc. This both regenerates the
committed declaration and validates that the wrapper still type-checks cleanly
against the freshly built `toy-kv-wasm` types.

### 6. Regenerate the wrapper README

The wrapper's `README.md` is generated from its public API by typedoc (mirroring
lumina's `npm run update-readme`):

```bash
cd wasm/js
npm run update-readme
```

`update-readme` runs `typedoc --plugin typedoc-plugin-markdown` over the wrapper
entry (`index.js`), using `../README.md` (the wasm crate's readme) as the intro,
then `concat-md` flattens the generated `docs/` tree into `README.md`. (lumina
points typedoc at its wasm `.d.ts` because its wrapper re-exports the wasm; the
toy-kv wrapper does not re-export it, so typedoc targets the wrapper's own
`spawnKv`/`ToyKvClient` surface instead.) The `docs/` output dir is gitignored.

## Files changed by this step

The files modified and picked up by the orchestrator's single commit:

- `wasm/js/package.json` — version set to the workspace version.
- `wasm/js/package-lock.json` — version + lockfile re-resolved against the fresh
  `../pkg` build.
- `wasm/js/index.d.ts` — declarations regenerated from the JSDoc by `tsc`.
- `wasm/js/README.md` — regenerated from the wrapper's public API by typedoc.

The wrapper's `.js` sources (`index.js`/`worker.js`) and hand-written
`types.d.ts` are already committed and are published as-is; the generated
`index.d.ts` and `README.md` may change here.

## What is deliberately NOT done here

- **No wasm binary committed.** `wasm/pkg/` is gitignored and is rebuilt at
  publish time. We only build it here to refresh the lockfile and validate the
  wrapper's compile.
- **No dependency repin.** `package.json` keeps `toy-kv-wasm: file:../pkg`. The
  swap to the concrete published version
  (`npm pkg set dependencies[toy-kv-wasm]=$version`) and the actual
  `npm publish` happen later, at release time — see
  [`release-step.md`](release-step.md).
- **No commit.** This step only leaves changed files in the working tree; the
  xtask stages and commits them together with the rest of the prepare changes.

## One-line mental model

Mirror the single workspace version into the `toy-kv` wrapper, regenerate its
lockfile from a fresh `toy-kv-wasm` build, validate the wrapper compiles, and
hand the changed `package.json` + `package-lock.json` to the orchestrator's
single commit — idempotently (skips if already at the target), publishing
nothing.
