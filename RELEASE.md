# Releasing nano-bpm

This repo ships **three release trains**, each with its own tag scheme, workflow, and destination. Cutting a "complete" release means tagging every bumped train from the same merged commit on `main` with `make release-tags` (below). The script derives each tag from its train's version source and pushes **one tag per `git push`**.

| # | Product | Tag | Workflow | Destination |
|---|---------|-----|----------|-------------|
| 1 | Nano gateway (`nanobpm-gateway-rest-server`) + ProcessOS binaries + `@nanobpm/engine-wasm` (in-browser engine, same version) | `v*` | `publish-c8ctl-binaries.yml`, `publish-processos-binaries.yml`, `release-bojtos-npm.yml` | Gateway: the rolling `binaries` release of `jwulf/c8ctl-plugin-nano`, which then cuts its own plugin release. ProcessOS: this repo's `v*` release. Both: S3 mirror. engine-wasm: npmjs.com (see [`docs/releasing-bojtos-npm.md`](docs/releasing-bojtos-npm.md)) |
| 2 | `@nanobpm/nano-bernd` (embedded engine, npm) | `nano-bernd-npm-v*` | `release-nano-bernd-npm.yml` | npmjs.com |
| 3 | `io.github.jwulf:nano-bernd` (embedded engine, JVM) | `nano-bernd-jvm-v*` | `release-nano-bernd-jvm.yml` | Maven Central |

The train table lives in code: `TRAINS` in [`scripts/release-tags.mjs`](scripts/release-tags.mjs). Its tests fail CI if a workflow's tag trigger drifts from it, or if a new tag-triggered release workflow isn't registered.

**Only a release tag on `main` publishes.** Every publishing job is gated on a tag ref and first runs [`assert-release-ref`](.github/actions/assert-release-ref/action.yml), which fails unless the tag matches its train's pattern and its commit is on `main`. A manual "Run workflow" from a branch builds but publishes nothing. (Such a run once put a side-branch server into the c8ctl plugin for seven weeks: #1830.) `scripts/release-ref-gate.test.mjs` fails CI if any publishing job, including one in a new workflow, lacks either gate.

Details on each are in the per-package `RELEASING.md` (see below); this document is the orchestration checklist.

## Version streams

The trains share one `nano_engine` codebase (compiled to native for #1, wasm for #2–#4) but move at different cadences:

- **Gateway + ProcessOS** (`v*`) is the engine's own SemVer. Bump on any user-visible change to the gateway server or ProcessOS binary.
- **nano-bernd** (`nano-bernd-{npm,jvm}-v*`) is versioned by the **FFI ABI** its host wraps, not by the engine crate:
  - **Major**: ABI break.
  - **Minor**: additive ABI change (this is why the current release is `0.2.0` — ABI v1 → v2 added the job worker surface).
  - **Patch**: host-side fixes with the same ABI.
- The two `nano-bernd` packages (npm + JVM) **always share a version** — they're two hosts wrapping the same wasm blob. Never release one without the other.

- **engine-wasm** shares the gateway version (#1871): `server vX.Y.Z` ⇔ `@nanobpm/engine-wasm X.Y.Z`, published from the same `v*` tag. Its version is stamped from `server/Cargo.toml` at build time; `engine-wasm/pkg.package.json` carries none. A running server reports it as `nano.engineWasmVersion` in `GET /v2/topology`.

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

- **Gateway/ProcessOS**: bump the workspace/`server/Cargo.toml` (and `processos/Cargo.toml`) `version = "…"` fields. This is also the `@nanobpm/engine-wasm` version (#1871); refresh `console/package-lock.json`'s recorded `../engine-wasm/pkg` version with `make console-wasm && (cd console && npm install)`.
- **nano-bernd npm**: bump `clients/nano-bernd/package.json` `"version"`, then run `npm install` in that directory so `package-lock.json` stays in sync.
- **nano-bernd JVM**: bump `clients/nano-bernd-jvm/pom.xml` `<version>`.

If the FFI ABI changed on this train, also update `EXPECTED_ABI_VERSION` in `clients/nano-bernd/src/index.ts` and `EmbeddedEngine.EXPECTED_ABI_VERSION` in `clients/nano-bernd-jvm/src/main/java/io/github/jwulf/nano/bernd/EmbeddedEngine.java`, and bump `ABI_VERSION` in `engine-core/scripts/emit-dist.mjs` + `engine-core/scripts/verify-wasm-ffi.mjs`. CI enforces the match.

**Upgrade boundaries (mandatory).** Every `upgrade-boundary` entry in
`engine-core/examples/determinism/divergence-allowlist.json` means the release
must **not** be rolled across a cluster (Raft members re-execute committed
commands, so a mixed-version window leaves replicas permanently divergent).
List them — the `prev-release` determinism gate prints each as an
`UPGRADE BOUNDARY` line (`scripts/determinism-ci.sh prev-release OUTDIR`) —
and paste the operator-facing warning from the "Release-note line" in
`engine-core/examples/determinism/README.md` into the release PR description
**and** the published GitHub release notes for the gateway. The console
changelog is generated from commit subjects and does not carry it. A release
with an unpublished boundary warning must not be distributed.

Open a PR titled e.g. `chore(release): gateway v0.0.24 + nano-bernd 0.3.0`, converge review, merge.

### 1a. Call out every determinism upgrade boundary in the release notes

Raft members **re-execute** committed commands (ADR 0066, "Rolling upgrade",
option C), so a release whose previous-release determinism allowlist
(`engine-core/examples/determinism/divergence-allowlist.json`) carries any
`"upgrade": "upgrade-boundary"` entry **forbids rolling upgrades across it**:
a mixed-version cluster spanning that release would journal different events
for the same command. Since #1702 the cluster-join handshake **enforces** this
— a member whose version straddles a declared boundary is refused at join with
a typed `JoinRejection::UpgradeBoundary` error naming both versions and the
boundary. The boundary table is derived at build time from the append-only
ledger `engine-core/examples/determinism/upgrade-boundaries.json`, and the build
fails if a live allowlist `upgrade-boundary` entry is missing from it — so
ratifying a boundary forces the ledger append, and the later (mandatory)
allowlist expiry never lifts the join restriction. A peer that advertises no
version (pre-#1702) is refused by every release build once any boundary exists.

Every `upgrade-boundary` entry **must be called out in the release notes** as:
"no rolling upgrade across this release; quiesce and upgrade all nodes". The
`determinism_corpus cross-release` CLI already prints the lines to source this
from — one per boundary entry:

```
determinism_corpus: UPGRADE BOUNDARY (no rolling upgrade across this release; call out in the release notes): <entry>
```

Run it (CI's previous-release determinism gate does) and copy each
`UPGRADE BOUNDARY` line into the release notes verbatim, with the entry's
issue link.

### 1b. Upgrade note: off-heap orphan settlement backfill (#1608)

Call this out in the release notes of the release that first ships #1608
(https://github.com/nanobpm/nano-bpm/issues/1608), next to the no-rolling-upgrade
warning above. A pre-#1608 snapshot carries no `settled_orphan_episodes`. On
every snapshot-based recovery — single- and multi-partition recovery, and Raft
snapshot adoption — the engine backfills that map from the retained covered
history (the cold archive plus the hot prefix) before it replays the tail,
using the live fold. Adoption needs it too: during a rolling upgrade a
pre-#1608 leader can send a snapshot with no settlement map, and the
receiver's retained prefix is the same replicated log, so the same fold
applies. **Residual
window:** a pre-#1608 snapshot, a pruned cold prefix, and a delayed duplicate
orphan report straddling the upgrade together raise **one extra, resolvable
`VariablesLost` incident** for that instance. Data is never lost. Resolve the
incident as usual.

### 2. Tag every bumped train from the merge commit

```bash
git checkout main
git pull --ff-only

make release-tags          # dry run: lists each train's tag as NEW or exists
make release-tags PUSH=1   # tag HEAD and push each NEW tag, one push per tag
```

The script (`scripts/release-tags.mjs`):

- **Derives** each tag from the train's version source, so a tag can't disagree with the version it publishes. It refuses to run if the nano-bernd npm and JVM versions differ.
- **Only cuts trains whose version isn't already tagged on the remote.** Trains you didn't bump are skipped.
- **Requires** a clean tree whose `HEAD` is `origin/main`.
- **Pushes each tag in its own `git push`.** Never batch release tags: **GitHub creates no push events when more than three tags are pushed at once**, so every workflow is silently skipped. v0.0.24 pushed four tags in one push and nothing ran (#1289).
- **Confirms each train's workflows actually started**, and exits non-zero naming any that didn't.

Don't hand-push release tags. If you must, push one tag per command.

### 2a. The gateway tag prunes the determinism allowlist itself (#1801)

The previous-release determinism gate expires every
`divergence-allowlist.json` entry whose `introduced_in` the previous release
has reached and **fails on the stale entries** — so an unpruned allowlist
turns `main` (and every open PR) red the moment a gateway tag lands. The
`publish-c8ctl-binaries` workflow therefore opens the pruning PR itself: its
`prune-allowlist` job runs at the `v*` tag push, drops every entry with
`introduced_in <= the new tag` (`scripts/prune-divergence-allowlist.mjs`),
opens `chore(determinism): expire allowlist entries shipped in <tag>` against
`main`, and comments `@mergifyio queue` so the red window is only as long as
one queued CI run. It is idempotent — re-running the tag's workflow re-queues
the existing PR instead of opening a duplicate, and a release with nothing to
prune opens nothing. The append-only `upgrade-boundaries.json` ledger is never
touched: expiring an allowlist entry never lifts the cluster-join restriction
it recorded (#1702).

The job authenticates with a GitHub App installation token minted by
`actions/create-github-app-token` from the `RELEASE_APP_ID` repository
variable and the `RELEASE_APP_PRIVATE_KEY` repository secret (the App needs
Contents + Pull requests write on this repo). `GITHUB_TOKEN` cannot be used:
GitHub Actions is not permitted to create pull requests here (#1907), and a
PR it opened would not trigger the required CI anyway. Without the App the job
fails at its first step, before pushing anything.
`scripts/no-github-token-pr-create.test.mjs` (CI) fails any workflow step that
runs `gh pr create` with `GITHUB_TOKEN`. A re-run after a failed
`gh pr create` reuses the already-pushed branch.

**If the pruning PR never appears** (the job failed or was skipped): run the
prune by hand from a clean `main` and open the PR yourself:

```bash
git checkout -b chore/prune-determinism-allowlist-<tag> origin/main
node scripts/prune-divergence-allowlist.mjs <tag> --write   # drops the expired entries
git commit -sam "chore(determinism): expire allowlist entries shipped in <tag>"
git push -u origin chore/prune-determinism-allowlist-<tag>
gh pr create --base main   # title as the commit; Refs #1801
```

Until that PR merges, every PR's required `determinism
(seed/profile/platform/previous-release)` check fails on the STALE entries —
queue the pruning PR first.

**Authoring new entries:** an entry's `introduced_in` is **derived, never
hand-guessed** — it must be the *next* gateway release (the version your
release PR will cut: the pending `server/Cargo.toml` bump). With no bump
committed yet (a normal feature PR — the release bump is its own PR), an entry
need only be labelled with an *unreleased* version (strictly after the latest
`v*` tag); an already-shipped label fails. The release-bump PR then fails until
**every** entry equals exactly the version it cuts, so a provisional label
(e.g. `v0.0.29` when the release becomes `v0.1.0`) is corrected before the tag
cuts and before the ledger can record a boundary under it. The
`check-allowlist-introduced-in` step of the determinism CI job
(`scripts/check-allowlist-introduced-in.mjs`) fails the PR otherwise. A
guessed future version is exactly how #1528's entries ended up labelled
`v0.0.29` while shipping in v0.0.28 — and the append-only ledger recorded the
spurious boundary permanently.

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

`make release-tags` cuts only the trains whose version is bumped and not yet tagged, so a partial release needs no special handling. Just bump what you're releasing. To restrict explicitly, run `node scripts/release-tags.mjs --only gateway --push` (this also publishes `@nanobpm/engine-wasm`). Selecting just one nano-bernd host is rejected: name both or neither.

- Engine bugfix → bump the gateway; `@nanobpm/engine-wasm` releases with it at the same version.
- Host-side fix in `EmbeddedEngine` (either language) → bump the nano-bernd patch version in **both** hosts. They must stay in sync, and the script refuses to run if they differ.
- FFI-only change with no host update → bump nano-bernd minor and release every train (the gateway also embeds the FFI code path via the console feature).

## If something goes wrong

- **A tag was pushed but its workflow never started** (the script reports `… did not start for <tag>`). Nothing was published by the missing run. Re-trigger it by deleting the remote tag and pushing it **alone**: `git push origin --delete <tag> && git push origin refs/tags/<tag>`. The most common cause is more than three tags in one push.

- **npm publish fails after tag**: fix the underlying issue on `main`, delete the failed tag locally + on origin (`git push --delete origin nano-bernd-npm-v0.2.0`), then re-tag from the fixed commit. `npm publish` refuses to overwrite an already-published version; if it partially succeeded, cut a `.1` patch instead.
- **Maven Central deployment stuck at validation**: check the portal at <https://central.sonatype.com/publishing/deployments>. Common failures are missing GPG signature or a namespace not yet verified. The Central Portal keeps failed deployments — you can inspect them, drop them, and re-run the workflow.
- **Tag pushed to wrong commit**: delete on origin, re-tag correctly. Both workflows are idempotent up to the actual `publish`/`deploy` step, which is guarded by a `tag == version` check.
