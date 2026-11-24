# Releasing `@nanobpm/engine-wasm`

Publishes **`@nanobpm/engine-wasm`** — the wasm-pack (`--target web`) build of
the Rust in-browser engine — to npm.

> **The Bojtos framework packages moved.** `@nanobpm/bojtos-kit` and
> `@nanobpm/bojtos-react` were extracted to the standalone public repo
> [`nanobpm/bojtos`](https://github.com/nanobpm/bojtos) and are published from
> there (see that repo's `docs/releasing.md`). They consume `@nanobpm/engine-wasm`
> from npm (the console overrides that to its same-commit build). This repo only publishes `engine-wasm`, whose source lives here
> (built from the Rust engine via `make console-wasm`).

## How it works

- `@nanobpm/engine-wasm` ships the prebuilt wasm + generated `.d.ts`/`.js`. The
  publish is driven by **`scripts/bojtos-release.mjs`**, which packs and publishes
  the single `engine-wasm/pkg` package.
- Authentication is **npm OIDC trusted publishing** — there is no `NPM_TOKEN`.
  CI mints a short-lived publish token from GitHub OIDC; the package is bound to
  this repo + workflow on npmjs.com (one-time, below).
- Provenance is disabled (`NPM_CONFIG_PROVENANCE=false`) — it requires a public
  source repository, and nano-bpm is private.

## One-time setup

A Trusted Publisher can only be configured on a package that already exists;
`@nanobpm/engine-wasm` is already published, so this is done. For reference, on
npmjs.com for `@nanobpm/engine-wasm`: **Settings → Trusted Publisher → GitHub
Actions**:

| Field               | Value                     |
|---------------------|---------------------------|
| Organization / user | `nanobpm`                 |
| Repository          | `nano-bpm`                |
| Workflow filename   | `release-bojtos-npm.yml`  |
| Environment         | *(leave blank)*           |

After that, tagged releases publish automatically with no secret.

## One engine version (#1871)

`server vX.Y.Z` ⇔ `@nanobpm/engine-wasm X.Y.Z`. There is no separate
engine-wasm version or tag train:

- **Version.** `engine-wasm/pkg.package.json` carries no `version`.
  `make console-wasm` stamps it from `server/Cargo.toml`
  (`engine-wasm/scripts/stamp-pkg.mjs`) and records the build commit in
  `pkg/build-info.json` (exported as `@nanobpm/engine-wasm/build-info`).
- **Release.** Every gateway release tag `vX.Y.Z` triggers
  `release-bojtos-npm.yml` (the filename is kept: it is the npm Trusted
  Publisher). It builds the package from the tag, checks the stamped version
  equals the tag and the build commit equals the tagged commit
  (`scripts/engine-parity.mjs`), and publishes via OIDC. The old
  `bojtos-npm-v*` tags (last: `bojtos-npm-v0.11.2`) are retired.
- **Console.** The console depends on `file:../engine-wasm/pkg`, the build of
  its own commit, never an npm range. Its bundle records the embedded engine as
  `dist/engine-wasm.json`, which the gateway release checks against the server.
- **Discovery.** A running server reports its engine as
  `nano.engineWasmVersion` in `GET /v2/topology`; pin `@nanobpm/engine-wasm` to
  exactly that version to test against the engine it runs.

So cutting a release is just the gateway release in [`RELEASE.md`](../RELEASE.md):
bump `server/Cargo.toml`, merge, `make release-tags PUSH=1`. Verify with
<https://www.npmjs.com/package/@nanobpm/engine-wasm>.

> **Versioning note.** The first release under this scheme publishes
> `0.0.x` (the gateway's version) after the retired train's `0.11.2`. npm moves
> the `latest` dist-tag to the newest *publish*, so `latest` points at the
> gateway-versioned release; consumers on a `^0.11` range stay on 0.11.x until
> they pin the server's version.

## Guardrails

- **Staleness / skew (engine changed, wasm not republished or not embedded).**
  Impossible by construction: every server release publishes the engine at the
  same version from the same tag, and the console embeds the same-commit build.
  CI guards it: `scripts/engine-parity.mjs` runs on the CI-built package
  (`engine-wasm-ffi (dist + verify)`), on the console's installed package
  (`console (typecheck + test)`), on the release console bundle
  (`publish-c8ctl-binaries.yml`) and on the published package
  (`release-bojtos-npm.yml`), failing if version or commit differ from the
  server's.

- **Surface gap (engine gained a capability the wrapper doesn't expose).** A
  version match can't catch a *new* `engine-core` `Command` that should be
  surfaced through the `TestEngine` wasm facade but isn't — the wasm still
  compiles. `engine-wasm/src/surface_parity.rs` closes this with an exhaustive,
  wildcard-free `match` over every `Command` variant: adding a variant in
  `engine-core` makes the match non-exhaustive and **fails the `wasm32`
  type-check** (`make engine-wasm-check`, run in the `engine-wasm-ffi` CI job)
  until an author consciously surfaces it or records why it's excluded.

## Manual dry-run (local, no publish)

```bash
make console-wasm
node scripts/bojtos-release.mjs --dry-run
```

This runs `npm publish --dry-run` for `engine-wasm/pkg` and prints the tarball
contents without publishing.
