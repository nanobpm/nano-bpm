# Releasing nano-bpm

This repo ships **four independent release trains**, each with its own tag scheme, workflow, and destination. Cutting a "complete" release means tagging every bumped train from the same merged commit on `main` with `make release-tags` (below). The script derives each tag from its train's version source and pushes **one tag per `git push`**.

| # | Product | Tag | Workflow | Destination |
|---|---------|-----|----------|-------------|
| 1 | Nano gateway (`nanobpm-gateway-rest-server`) + ProcessOS binaries | `v*` | `publish-c8ctl-binaries.yml`, `publish-processos-binaries.yml` | Gateway: the rolling `binaries` release of `jwulf/c8ctl-plugin-nano`, which then cuts its own plugin release. ProcessOS: this repo's `v*` release. Both: S3 mirror |
| 2 | `@nanobpm/nano-bernd` (embedded engine, npm) | `nano-bernd-npm-v*` | `release-nano-bernd-npm.yml` | npmjs.com |
| 3 | `io.github.jwulf:nano-bernd` (embedded engine, JVM) | `nano-bernd-jvm-v*` | `release-nano-bernd-jvm.yml` | Maven Central |
| 4 | `@nanobpm/engine-wasm` (in-browser engine, consumed by Bojtos) | `bojtos-npm-v*` | `release-bojtos-npm.yml` | npmjs.com (see [`docs/releasing-bojtos-npm.md`](docs/releasing-bojtos-npm.md)) |

The train table lives in code: `TRAINS` in [`scripts/release-tags.mjs`](scripts/release-tags.mjs). Its tests fail CI if a workflow's tag trigger drifts from it, or if a new tag-triggered release workflow isn't registered.

Details on each are in the per-package `RELEASING.md` (see below); this document is the orchestration checklist.

## Version streams

The trains share one `nano_engine` codebase (compiled to native for #1, wasm for #2–#4) but move at different cadences:

- **Gateway + ProcessOS** (`v*`) is the engine's own SemVer. Bump on any user-visible change to the gateway server or ProcessOS binary.
- **nano-bernd** (`nano-bernd-{npm,jvm}-v*`) is versioned by the **FFI ABI** its host wraps, not by the engine crate:
  - **Major**: ABI break.
  - **Minor**: additive ABI change (this is why the current release is `0.2.0` — ABI v1 → v2 added the job worker surface).
  - **Patch**: host-side fixes with the same ABI.
- The two `nano-bernd` packages (npm + JVM) **always share a version** — they're two hosts wrapping the same wasm blob. Never release one without the other.

- **engine-wasm** (`bojtos-npm-v*`) has its own SemVer. CI's `engine-wasm (release drift guard)` fails once `engine-core` changes after the last `bojtos-npm-v*` tag unless a bump is pending. A change to its JS/DTO surface (for example a renamed field) is breaking, so it gets a minor bump while the version is 0.x.

The versions are independent — bumping one does not require bumping another.

## Prerequisites (one-time)

Each train has its own credentials, documented in place:

- **Gateway/ProcessOS**: `C8CTL_PLUGIN_REPO_TOKEN`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — see the header comments in `.github/workflows/publish-{c8ctl,processos}-binaries.yml`.
- **npm nano-bernd**: no secret — publishing uses npm OIDC trusted publishing (a Trusted Publisher configured on npmjs.com, no `NPM_TOKEN`); see [`clients/nano-bernd/RELEASING.md`](clients/nano-bernd/RELEASING.md).
- **JVM nano-bernd**: `CENTRAL_USERNAME`, `CENTRAL_PASSWORD`, `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` — see [`clients/nano-bernd-jvm/RELEASING.md`](clients/nano-bernd-jvm/RELEASING.md).

Verify all secrets are set at <https://github.com/nanobpm/nano-bpm/settings/secrets/actions> before your first release.

## Cutting a coordinated release

Do this from a clean `main` after all release-worthy PRs have merged.

### 1. Bump versions in a single PR

- **Gateway/ProcessOS**: bump the workspace/`server/Cargo.toml` (and `processos/Cargo.toml`) `version = "…"` fields.
- **nano-bernd npm**: bump `clients/nano-bernd/package.json` `"version"`, then run `npm install` in that directory so `package-lock.json` stays in sync.
- **nano-bernd JVM**: bump `clients/nano-bernd-jvm/pom.xml` `<version>`.
- **engine-wasm**: bump `engine-wasm/pkg.package.json` `"version"`, then run `make console-wasm` so the committed `engine-wasm/pkg/` (wasm + `package.json`) is rebuilt at the new version.

If the FFI ABI changed on this train, also update `EXPECTED_ABI_VERSION` in `clients/nano-bernd/src/index.ts` and `EmbeddedEngine.EXPECTED_ABI_VERSION` in `clients/nano-bernd-jvm/src/main/java/io/github/jwulf/nano/bernd/EmbeddedEngine.java`, and bump `ABI_VERSION` in `engine-core/scripts/emit-dist.mjs` + `engine-core/scripts/verify-wasm-ffi.mjs`. CI enforces the match.

Open a PR titled e.g. `chore(release): gateway v0.0.24 + nano-bernd 0.3.0 + engine-wasm 0.10.0`, converge review, merge.

### 2. Tag every bumped train from the merge commit

```bash
git checkout main
git pull --ff-only

make release-tags          # dry run: lists each train's tag as NEW or exists
make release-tags PUSH=1   # tag HEAD and push each NEW tag, one push per tag
```

The script (`scripts/release-tags.mjs`):

- **Derives** each tag from the train's version source, so a tag can't disagree with the version it publishes. It refuses to run if the nano-bernd npm and JVM versions differ, or if the engine-wasm template and built versions differ.
- **Only cuts trains whose version isn't already tagged on the remote.** Trains you didn't bump are skipped.
- **Requires** a clean tree whose `HEAD` is `origin/main`.
- **Pushes each tag in its own `git push`.** Never batch release tags: **GitHub creates no push events when more than three tags are pushed at once**, so every workflow is silently skipped. v0.0.24 pushed four tags in one push and nothing ran (#1289).
- **Confirms each train's workflows actually started**, and exits non-zero naming any that didn't.

Don't hand-push release tags. If you must, push one tag per command.

### 3. Watch the workflows

- <https://github.com/nanobpm/nano-bpm/actions/workflows/publish-c8ctl-binaries.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/publish-processos-binaries.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/release-nano-bernd-npm.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/release-nano-bernd-jvm.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/release-bojtos-npm.yml>

Typical durations:
- npm nano-bernd: 3–5 min.
- JVM nano-bernd: 10–20 min (Central Portal validation dominates).
- engine-wasm: 5–10 min.
- Gateway + ProcessOS: 15–40 min (matrix cross-compilation).

### 4. Verify

- **Gateway binaries**: the workflow uploads them to the rolling [`binaries`](https://github.com/jwulf/c8ctl-plugin-nano/releases/tag/binaries) release of `jwulf/c8ctl-plugin-nano`. There is **no** `<tag>` release in that repo. It then merges a `fix(binary): bundle nanobpmn <version>` PR that bumps `nanobpmn-binary.json`, and that PR triggers the plugin's semantic-release to publish its own version (for example v0.0.24 shipped as plugin **v1.69.4**). Check: `gh api repos/jwulf/c8ctl-plugin-nano/contents/nanobpmn-binary.json -q .content | base64 -d`. S3 mirror at `s3://sitapati-storage/nanobpm-gateway/<tag>/`.
- **ProcessOS binaries**: <https://github.com/nanobpm/nano-bpm/releases> — assets on the tag. S3 at `s3://sitapati-storage/processos/<tag>/`.
- **npm**: `npm view @nanobpm/nano-bernd version` and `npm view @nanobpm/engine-wasm version` → should be the new versions. (Provenance is not published — it requires a public source repo, and nano-bpm is private.)
- **Maven Central**: <https://central.sonatype.com> shows the new version immediately; <https://search.maven.org/artifact/io.github.jwulf/nano-bernd> propagates within ~30 min. Test with `mvn dependency:get -Dartifact=io.github.jwulf:nano-bernd:0.2.0`.

## Releasing only some trains

`make release-tags` cuts only the trains whose version is bumped and not yet tagged, so a partial release needs no special handling. Just bump what you're releasing. To restrict explicitly, run `node scripts/release-tags.mjs --only engine-wasm,gateway --push`. Selecting just one nano-bernd host is rejected: name both or neither. `make release-engine-wasm` is shorthand for `--only engine-wasm --push`.

- Engine bugfix that affects the gateway only → bump the gateway only. If `engine-core` changed, CI also requires an engine-wasm bump.
- Host-side fix in `EmbeddedEngine` (either language) → bump the nano-bernd patch version in **both** hosts. They must stay in sync, and the script refuses to run if they differ.
- FFI-only change with no host update → bump nano-bernd minor and release every train (the gateway also embeds the FFI code path via the console feature).

## If something goes wrong

- **A tag was pushed but its workflow never started** (the script reports `… did not start for <tag>`). Nothing was published by the missing run. Re-trigger it by deleting the remote tag and pushing it **alone**: `git push origin --delete <tag> && git push origin refs/tags/<tag>`. The most common cause is more than three tags in one push.

- **npm publish fails after tag**: fix the underlying issue on `main`, delete the failed tag locally + on origin (`git push --delete origin nano-bernd-npm-v0.2.0`), then re-tag from the fixed commit. `npm publish` refuses to overwrite an already-published version; if it partially succeeded, cut a `.1` patch instead.
- **Maven Central deployment stuck at validation**: check the portal at <https://central.sonatype.com/publishing/deployments>. Common failures are missing GPG signature or a namespace not yet verified. The Central Portal keeps failed deployments — you can inspect them, drop them, and re-run the workflow.
- **Tag pushed to wrong commit**: delete on origin, re-tag correctly. Both workflows are idempotent up to the actual `publish`/`deploy` step, which is guarded by a `tag == version` check.
