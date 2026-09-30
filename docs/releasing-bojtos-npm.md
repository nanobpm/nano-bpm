# Releasing `@nanobpm/engine-wasm`

Publishes **`@nanobpm/engine-wasm`** — the wasm-pack (`--target web`) build of
the Rust in-browser engine — to npm.

> **The Bojtos framework packages moved.** `@nanobpm/bojtos-kit` and
> `@nanobpm/bojtos-react` were extracted to the standalone public repo
> [`nanobpm/bojtos`](https://github.com/nanobpm/bojtos) and are published from
> there (see that repo's `docs/releasing.md`). They consume `@nanobpm/engine-wasm`
> from npm. This repo only publishes `engine-wasm`, whose source lives here
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

## Cutting a release

1. Bump the version in `engine-wasm/pkg.package.json` (the source of truth for
   `@nanobpm/engine-wasm`; `make console-wasm` copies it into
   `engine-wasm/pkg/package.json`).
2. `make console-wasm` — rebuilds the wasm so the committed artifact matches the
   new version.
3. Commit, open a PR, merge to `main`.
4. Tag from `main` and push with the helper. It is a thin wrapper over the
   canonical `scripts/release-tags.mjs` (see [`RELEASE.md`](../RELEASE.md)). It
   checks that the template and built versions agree, that the tag is new on the
   remote, and that `HEAD` is a clean `origin/main`. It then tags, pushes the tag
   on its own, and confirms `release-bojtos-npm` started:
   ```bash
   make release-engine-wasm
   ```
   When cutting several trains together, use `make release-tags PUSH=1` instead.
   Never push release tags in one batch: GitHub skips workflows for pushes of
   more than three tags.
5. The `release-bojtos-npm` workflow builds the wasm from source, verifies the
   tag matches the package version, and publishes via OIDC.
6. Verify on npmjs.com:
   - <https://www.npmjs.com/package/@nanobpm/engine-wasm>

## Guardrails (so a stale engine-wasm can't ship silently)

`@nanobpm/engine-wasm` is just a wasm build of `engine-core`, published *only* on
a hand-pushed `bojtos-npm-v*` tag. Two independent drift modes are guarded in CI
so neither can slip through unnoticed:

- **Staleness (engine changed, wasm not republished).** The
  `engine-wasm (release drift guard)` CI job fails a build when any wasm input
  — `engine-core/**`, `engine-wasm/src/**`, `engine-wasm/Cargo.toml`,
  `engine-wasm/Cargo.lock` or `engine-wasm/pkg.package.json` — has changed since
  the last `bojtos-npm-v*` tag but the version hasn't been bumped past it
  (`Cargo.toml`/`Cargo.lock` count because a dependency or feature change alters
  the produced wasm without touching `engine-core` or `engine-wasm/src`). In
  other words: once the engine moves, an *unreleased* version bump
  must always be pending — the pending `pkg.package.json` version must be
  **strictly greater** than the last published `bojtos-npm-v*` tag (a patch bump
  like `0.3.0 → 0.3.1` is enough; the guard compares with `sort -V`, so any
  higher version passes). The **first** engine change after a release must cross
  that threshold; later changes pass until that version is actually tagged and
  released, after which they must bump again. Cut the pending release with
  `make release-engine-wasm`.

- **Surface gap (engine gained a capability the wrapper doesn't expose).** A
  package bump can't catch a *new* `engine-core` `Command` that should be
  surfaced through the `TestEngine` wasm facade but isn't — the wasm still
  compiles. `engine-wasm/src/surface_parity.rs` closes this with an exhaustive,
  wildcard-free `match` over every `Command` variant: adding a variant in
  `engine-core` makes the match non-exhaustive and **fails the `wasm32`
  type-check** (`make engine-wasm-check`, run in the `engine-wasm-ffi` CI job)
  until an author consciously surfaces it or records why it's excluded.

> **New required checks:** consider adding `engine-wasm (release drift guard)` to
> branch protection so the staleness guard is merge-blocking, not just advisory.

## Manual dry-run (local, no publish)

```bash
make console-wasm
node scripts/bojtos-release.mjs --dry-run
```

This runs `npm publish --dry-run` for `engine-wasm/pkg` and prints the tarball
contents without publishing.
