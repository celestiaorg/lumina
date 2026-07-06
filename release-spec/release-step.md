# npm publish on release

Publishing the npm packages at **release** time, after all crates are
published. This is the last step the orchestrator drives during a release — see
[`orchestrator.md`](orchestrator.md) (Flow 2 — Release, step 3), which runs
after [`publish-crates-logic.md`](publish-crates-logic.md) has published every
Rust crate.

> **Optional step.** This runs **once per configured npm component** and is
> **skipped entirely** when the workspace declares none — see
> [Portability & configuration](orchestrator.md#portability--configuration). The
> tool is generic; the toy-kv package names below are an example instantiation. A
> crate-only workspace ends the release after the crates are tagged and never
> reaches this step (and needs no npm token).

Context (toy-kv's configured npm component): two npm packages, both published at
the same workspace version:

- **`toy-kv-wasm`** — the compiled wasm bindings, built from the `toy-kv-wasm`
  Rust crate in `wasm/`.
- **`toy-kv`** — the ergonomic JS wrapper in `wasm/js/`. It depends on
  `toy-kv-wasm`.

## Preconditions

- The Rust crates have already been published to the registry for this version
  (Flow 2 step 2).
- Toolchain available: Rust with the `wasm32-unknown-unknown` target,
  `wasm-pack`, and `npm`.
- An npm auth token with publish rights for `toy-kv-wasm` and `toy-kv` is present
  in the environment, under the var named by `--npm-token-env` (e.g.
  `NPM_REGISTRY_TOKEN`). It is read from the environment, never passed on the command
  line. See the orchestrator's [Credentials](orchestrator.md#credentials) section.

## Procedure

1. **Resolve published vs. local version.**
   ```bash
   published_version=$(npm show toy-kv-wasm version)
   local_version="$(cargo pkgid --manifest-path=wasm/Cargo.toml | cut -d@ -f 2)"
   ```

2. **Idempotency guard** — if npm already has this version for `toy-kv-wasm`,
   there is nothing to do, so stop:
   ```bash
   if [ "$published_version" == "$local_version" ]; then
     echo "Version already published to npm, skipping"
     exit
   fi
   ```

3. **Derive the npm dist-tag** from the version's prerelease suffix:
   ```bash
   if [[ "$local_version" == *-* ]]; then
     npm_tag="${local_version#*-}"    # "rc.1"
     npm_tag="${npm_tag%%[.0-9]*}"    # "rc"
   else
     npm_tag="latest"
   fi
   ```
   - `X.Y.Z-rc.1` → `rc`, `X.Y.Z-alpha.2` → `alpha`, plain `X.Y.Z` → `latest`.

4. **Build and publish the wasm bindings package (`toy-kv-wasm`):**
   ```bash
   wasm-pack build wasm
   wasm-pack publish --access public --tag "$npm_tag" wasm
   ```

5. **Publish the wrapper package (`toy-kv`).** First repin its dependency from
   the local path (`file:../pkg`) to the concrete published `toy-kv-wasm`
   version, then publish:
   ```bash
   cd wasm/js
   npm pkg set "dependencies[toy-kv-wasm]=$local_version"
   npm publish --access public --tag "$npm_tag"
   ```
   There is **no build or `npm install` at this step** (like lumina): the wrapper
   ships its **committed hand-written source** (`index.js`/`worker.js` plus the
   `index.d.ts` regenerated and committed into the release PR during Prepare — see
   `npm-release-pr-steps.md`), so `package.json` has no `prepublishOnly` hook and
   `npm publish` only packs the `files` allow-list (`index.js`, `worker.js`,
   `index.d.ts`, `types.d.ts`). The repin is a pure metadata change.

## Result

- `toy-kv-wasm` and `toy-kv` are both published to npm at `$local_version`
  under `$npm_tag`.
- The `toy-kv` wrapper's dependency on `toy-kv-wasm` is pinned to the concrete
  published version.
