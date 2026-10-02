## No Such Thing as "Flaky Tests"

Intermittently failing tests must always be root-caused and addressed as a product defect (code) or a production-line defect (test). We do not acknowledge the existence of such a thing as "flaky tests".

## No Test Retries

Tests must pass on the first run. We do not configure test retries anywhere (nextest `retries`, CI re-run-on-fail, etc.) — a retry only masks a real defect (product or test) and lets it reach `main`. If a test only passes on a retry, that is a defect to root-cause, not to paper over.

## Red/Green Discipline

All bug fixes must have a test that reproduces the defect before modifying code. Red/Green—always.

## Fix the Failure Mode, Don't Just Squash the Bug

Whenever we detect an issue, reason broadly about the defect class and write a test guard for the defect class. Prefer securing surfaces — including suggesting an architectural refactor to eliminate the failure mode categorically — over squashing individual bugs.

## Feature Test Coverage

When adding new features, ensure test coverage over the new surface to prevent undetected regressions.

## Derivation Over Duplication: No Drift Surfaces

Identify and eliminate drift surfaces — duplicate sources of truth. Ensure that everything that can be derived is derived from a single source of truth and has a single canonical implementation. Do not introduce duplication.

## Zero Tolerance for Warnings, Errors, and Test Failures

We do not tolerate warnings, errors, or test failures in this project.

There are no pre-existing failures or warnings, and you will not allow any to enter the codebase. Thank you.

## Formatting: pinned nightly rustfmt (never bare `cargo fmt`)

The canonical Rust style relies on **unstable** rustfmt options — `rustfmt.toml`
sets `group_imports = "StdExternalCrate"`. Stable rustfmt **silently ignores**
that option (only a stderr warning), so a bare `cargo fmt` / `rustfmt` *appears*
to work but produces import-ordering drift that fails the
`rustfmt (pinned nightly)` CI gate. There is no `rust-toolchain.toml`, so the
default toolchain on your machine decides what `cargo fmt` does — never rely on
it. Always format through the pinned nightly:

- **Format:** `make fmt`
- **Verify (the CI gate):** `make fmt-check`

Both run `rustup run $(FMT_TOOLCHAIN) cargo fmt` with
`FMT_TOOLCHAIN := nightly-2026-06-26` (Makefile) over `engine-core`, `server`,
`processos`, `engine-wasm`, and `read-model` — the same invocation CI uses, so a
local `make fmt-check` pass is exactly what the gate checks. If the toolchain is
missing, install it once:
`rustup toolchain install nightly-2026-06-26 --component rustfmt`.

`cargo fmt` runs `cargo metadata` over the `server` workspace, so the gitignored
codegen inputs must exist on disk first. In a fresh (unbuilt) clone, generate
the throwaway parse-only stubs before formatting — `make fmt` / `make fmt-check`
do **not** do this for you:

```bash
./scripts/fmt-stub-codegen.sh           # create stubs (never clobbers real files)
./scripts/fmt-stub-codegen.sh --cleanup # remove the stubs it created
```

**Install the git hooks** so the same gate runs locally before every push —
catching drift before it reaches CI:

```bash
make install-hooks   # sets core.hooksPath -> .githooks (pre-push runs `make fmt-check`)
```

The pre-push hook creates and cleans up the codegen stubs automatically. Bypass
in an emergency with `git push --no-verify`.

## BPMN Models need DI

All BPMN Models need DI for rendering for humans.

## Adding Support for a New BPMN Element

A BPMN element type touches several layers. Because most of these are
compile-time exhaustive matches, the compiler will force some (but **not** all)
of the updates — the non-exhaustive ones (parsers, thin JSON pass-throughs,
regenerated artifacts) are silent and are the usual source of "it parsed but
didn't execute" or drift bugs. Work through **every** surface below. Reference
implementation: the `eventBasedGateway` support (`feat/event-based-gateway`).

**1. `engine-core` — the executable model (always required):**

- `engine-core/src/model.rs`
  - Add a variant to `enum ElementKind` (the engine's real element type), with
    any payload fields, and a doc comment describing its runtime semantics.
  - Add a `ProcessBuilder` constructor method (mirror `exclusive_gateway` /
    `parallel_gateway`) so the element can be built programmatically.
  - Some model transforms match on `ElementKind` (`inline_call_activities`,
    `remap_kind_ids`, …) — only relevant if the element carries embedded ids to
    remap; the compiler flags any exhaustive match.
- `engine-core/src/bpmn.rs` (the XML parser — **not** compiler-checked, easy to
  forget):
  - Add a variant to the parser-local `enum NodeKind`.
  - Add a tag-match arm recognizing the BPMN tag (near `exclusiveGateway` /
    `parallelGateway`). An unrecognized tag is silently dropped, so a flow into
    it fails deploy with a misleading "unknown target element" error at the
    *flow*, not the element.
  - Add the `NodeKind` → `ProcessBuilder` dispatch arm.
  - Update the `## Supported subset` doc comment at the top of the file.
- `engine-core/src/engine/mod.rs` (runtime execution):
  - Pass-through elements (gateways, none events) fall through the `Some(_)` arm
    in `run_activation_body` (activate → immediately `Complete`) and take their
    outgoing flow(s) in `finalize_completion`. Bespoke behaviour goes in
    `activate` / `complete` / `finalize_completion`.
  - `engine-core/src/engine/boundary.rs` only if the element is boundary-like.
- Tests: parser test in `bpmn.rs` (inline `#[cfg(test)]`), execution test in
  `engine/tests.rs`. Update `engine-core/README.md`'s supported-subset list.

**2. `processos` — the reversible IR / structural analysis (compiler-forced +
one parity test):**

- `processos/src/ir_spec.rs`: add a `KindSpec` to `ELEMENT_KIND_SPECS` (the
  canonical supported-element registry), a `sample_instances()` entry, and a
  `variant_witness` arm. The `specs_match_pretty_printer` parity test fails if
  these drift from `ElementKind`.
- `processos/src/model_ir.rs`: `kind_keyword`, `render_kind_attrs`, `build_kind`
  (exhaustive — will not compile until handled).
- `processos/src/bpmn_model.rs`: `kind_label`, `is_gateway`/`is_task` helpers,
  the XML emitter match, and `node_dims` (diagram footprint).
- Regenerate the grammar artifact: `cd processos && cargo run -- emit-gbnf --out
  assets/ir.gbnf` (commit the result — it is a checked-in derived artifact).

**3. `engine-wasm` — thin pass-through, but regenerate the committed
artifacts (silent):**

- `engine-core/src/ffi.rs` and `engine-wasm/src/lib.rs` enumerate no element
  types (JSON snapshots), so no code change — **but** the compiled wasm and its
  generated `.d.ts`/`.js` are committed. Rebuild: `make console-wasm`
  (regenerates `engine-wasm/pkg/`). The Bojtos framework packages that consume
  it (`@nanobpm/bojtos-kit` / `-react`) live in the separate `nanobpm/bojtos`
  repo and are element-type-agnostic, so they need no change here.

**4. `console` — only if the element needs modeller/palette support:**

- `console/src/components/BpmnModeler.tsx` / `console/src/lib/urbanComponents.ts`
  (element templates). Standard BPMN elements that `bpmn-js` already knows need
  no change; only bespoke `nano:` shapes do.

**Verify end-to-end** by deploying a model using the new element through the
freshly-built wasm (a Node probe against `@nanobpm/engine-wasm`'s `TestEngine`:
`engine.deploy(xml)` → `engine.createInstance(...)`), not just the Rust unit
tests — that is the surface the console (and Bojtos) actually consume.

## Adding or Changing an Event (event-frame replay-compatibility)

The `Event` enum (`engine-core/src/event.rs`) is a **persisted, replayed on-disk
shape**, not just an in-memory type. Migration-by-replay (#1071) rebuilds the
engine by replaying an old journal under the current binary, so every historical
record must still deserialize under new code — the property that broke in
incident #1065. The serde derives are feature-gated (`cfg_attr(feature =
"serde", …)`); **everything replay-related is built/tested with `--features
serde`** (the `engine-core (clippy + test)` CI job passes it — do not add a
second serde job). Two rules, enforced by the #1069 CI drift guard
(`engine-core/tests/golden_serde_drift.rs`), keep the frame replay-safe:

- **Additive change → safe, NO version bump.** Adding a field to an existing
  variant is forward-compatible **only if it carries `#[serde(default)]`** (an
  older record without the field then decodes with `None`/`0`). Adding a
  brand-new variant is likewise additive (old journals never contain it). After
  an additive change, refresh the golden corpus and commit the diff:
  `UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift`.
- **Breaking change → REQUIRES a version bump + migrator handling.** Renaming or
  removing a variant, retagging, changing a field's type, or reordering in a way
  that changes the serialized form is **not** rescued by serde defaults. It
  requires bumping `SNAPSHOT_FORMAT_VERSION` (`engine-core/src/engine/mod.rs`,
  #1068) — which the snapshot loader surfaces as a typed `SnapshotLoadError`:
  `FormatMismatch` when the on-disk snapshot is *newer* than this build supports
  (`check_format_version`), or `Corrupt` when an *older* snapshot no longer
  deserializes under the bumped format. `recover`/`recover_multi` trigger
  migration on **any** `SnapshotLoadError`, so both cases route to the replay
  path. You do **not** hand-write a per-version migration ladder:
  #1071's replay-migrator (`recover`/`recover_multi` in
  `server/crates/nano-server-storage/src/seglog.rs`) transparently rebuilds the
  engine by REPLAYING the event journal — the compacted prefix is kept in a
  bounded rolling **cold archive** (`compact`/`compact_multi` now archive it
  instead of hard-deleting) plus the surviving hot tail — under the new code and
  rewrites a fresh new-format snapshot. It falls back to fail-closed (#1066) only
  when replay genuinely cannot reconstruct the full `[0, total_events)` history (a
  pruned gap, or an unreadable `UnknownVariant` frame) — never a silent rewind
  (#1065). This means a breaking snapshot change is safe **iff** the event frame
  itself still replays under the new build; a breaking EVENT-frame change must
  follow the reject-don't-drop rule below. The #1069 drift guard fails CI until
  you make the version decision explicitly.

A **removed/renamed** variant also means an old journal may carry a variant this
build no longer knows. Replay must **reject it explicitly**, never drop it
silently (a silent skip is exactly the #1065 class): read journal records through
`nanobpmn_engine_core::decode_event_json`, which classifies an unrecognized
externally-tagged variant as the typed `EventDecodeError::UnknownVariant`
(operator-actionable, downcastable from the `io::Error` the storage reader
returns, routed to fail-closed #1066 / the migrator #1071). Regression coverage:
`engine-core/tests/golden_replay.rs` (decoder + whole-journal replay parity) and
the `read_segment_events*` rejection tests in
`server/crates/nano-server-storage/src/seglog.rs`.

## Claim Your Task Before You Start

Work here runs in **parallel worktrees across several agents** — an epic routinely
fans a dozen slices out at once. Your worktree is invisible to everyone else, so
the issue (or PR) is the only shared bus, and a claim signal on it is the only
thing preventing two agents from silently building the same slice and colliding
in the same files. Post the signal *before* writing code, not when you open the
PR — by then the duplicate work already happened.

- **Check first.** Before starting any planned task, look for an existing issue
  or PR covering it. If one is **already claimed** — a claim comment, an
  assignee, or an open PR — do **not** start. Stop and flag it to the user with a
  link. Never work a task in parallel with an untracked, unclaimed, or
  already-claimed item.
- **Then check the code, not just the issue.** An unclaimed issue does not mean
  unfinished work: at this velocity a slice can land while its issue stays open,
  and it is worse for a **cross-repo** slice, where the diff lands somewhere the
  issue does not live. So before claiming, confirm the work is actually absent —
  `git fetch` and look for the file, the field, the flag; check the other repo's
  `main`; check remote branches. Two of these have already been claimed after they
  were done. Claiming a finished task wastes an agent and, worse, tells everyone
  else the task is being handled when it needs nothing.
- **Nothing tracked yet → create it, then claim it.** Open the issue before
  writing any code, so the work is visible at the velocity this repo moves at.
- **Claim it by commenting your worktree name.** The comment is the canonical
  signal, because an agent is not always a repo collaborator and therefore cannot
  always assign or label. Lead with the marker word so claims are greppable
  (`gh issue view <n> --comments | grep Claimed`):

  ```
  Claimed — worktree `guided-journeys-contract`, branch `feat/guided-journeys-contract`.
  ```

  Name the worktree exactly as it appears in `git worktree list` (ours live in
  `~/workspace/nanobpmn-worktrees/<name>`) so a human can find the work in
  progress on disk. Add the PR link to the same thread once you open one, and
  assign yourself **if you have the permissions** — that reinforces the comment,
  it does not replace it. (There is deliberately no `in progress` label to
  maintain: one signal, in one place, cannot drift out of sync with itself.)
- **Release what you drop.** If you abandon or hand off a task, say so in the
  same thread (`Released — worktree <name>, <reason>`). A claim that outlives the
  work is worse than no claim: it deadlocks the slice behind an agent that is
  gone.
- **Reclaiming a stale claim.** A claim is stale when its worktree is absent from
  `git worktree list` **and** its branch has no unmerged commits. Say that in a
  comment, with what you checked, then claim it yourself. Never silently
  double-claim — if the evidence is ambiguous, ask the user rather than risk two
  agents in one file.
- **Cross-repo slices claim in the hub.** Work that lands in `nanobpm/nano-ide`,
  `jwulf/c8ctl-plugin-nano` or a demo app is still claimed on its
  `nanobpm/nano-bpm` issue, and the resulting PRs link back to it — one place
  to look, whatever repo the diff ends up in.

## Merging PRs

This repository does **not** auto-merge pull requests. Opening a PR is *not* the
same as committing to `main` — a PR sits open until a human or agent deliberately
merges it:

<!-- Machine-readable merge protocol (consumed by Merlin / any merge-driving agent —
     jwulf/urban-pr-review#43). This block is the single, AUTHORITATIVE source of truth
     automation reads to land a PR here; the prose below is a human-readable gloss that
     may lag and never overrides this block. Update this block first, then the prose. -->
```merge-protocol
{
  "autoMerge": false,
  "freshHeadRun": "ready-or-reopen",
  "waitForChecks": true,
  "land": { "method": "mergify-queue", "comment": "@mergifyio queue" },
  "requiredChecks": [
    { "name": "rustfmt (pinned nightly)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "engine-core (clippy + test)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "engine-wasm-ffi (dist + verify)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "server (clippy + test)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "nano-bernd (build + test)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "io.github.jwulf:nano-bernd (JVM, Chicory)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "processos (clippy + test)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "console (typecheck + test)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "console (journey e2e)", "acceptedConclusions": ["success", "skipped"] },
    { "name": "formal (tlc)", "acceptedConclusions": ["success", "skipped"] }
  ],
  "checksSemantics": "Every entry in requiredChecks gates the merge and must reach one of its acceptedConclusions. All ten checks are change-gated in ci.yml: the six code checks share the `code` filter (`*code_gate`), 'processos (clippy + test)' has its own `processos` filter, 'formal (tlc)' has its own `formal` filter (`formal/**`, `engine-core/src/**`, `engine-core/tests/**` — tests cited as Zeebe-parity evidence — `engine-core/examples/**` — the FEEL differential-fuzz harness the Lean layer diffs against — `engine-core/Cargo.toml` — a dependency or feature change there can alter compiled FEEL behaviour without touching `src/**`, so it must trigger the differential gate too — `engine-core/Cargo.lock` — a resolved-dependency version bump can alter compiled FEEL behaviour the differential-fuzz gate diffs without touching `Cargo.toml` — `rust-toolchain.toml` — the pinned Rust toolchain compiles that harness — and `.github/workflows/ci.yml`), and both console jobs ('console (typecheck + test)' and 'console (journey e2e)') are gated by the `console` filter (inputs `console/**`, `spec-console/**`, `spec-app/**`, `spec/**` — the console's generated Camunda REST subset client derives from it —, `.github/workflows/ci.yml`, `AGENTS.md`, and `.mergify.yml`; the typecheck job additionally runs on the `docs` filter for its bundled-docs guard), so each is SKIPPED for PRs (and merge_group batches) that don't touch its inputs (e.g. a Rust-only PR skips both console jobs; a console-only PR skips every code check) and RUNS when they do. A skipped check never reports success, so each accepts 'skipped' too (required-when-run, skip-tolerant). Without this, a merge orchestrator that treats a skipped required check as unsatisfied blocks console-only PRs that GitHub itself reports as CLEAN. 'console (typecheck + test)' is required because it is the only gate on the console `tsc -p tsconfig.test.json` type surface and the `src/gen` / `src/gen-c8` client-drift guard; leaving it non-required let a foundation slice that added a REQUIRED field to a console generated type (without updating the e2e fixture defaults in console/e2e/fixtures.ts) land on main red, forcing each following slice to repair the prior slice's break (epic #1119).",
  "doc": "AGENTS.md#merging-prs"
}
```

Required check names must not start with `@`: Mergify interprets
`@scope/name` as a GitHub App-qualified check, not an npm package name.
`console/scripts/merge-gates.test.mjs` guards required job names and Mergify's
success-or-skipped conditions against the canonical protocol above.

When required checks change, synchronize **live GitHub branch protection** too.
From the repository root, with console dependencies installed and an admin-capable
`gh` login, derive the update from this protocol rather than hand-copying names:

```bash
gh api repos/nanobpm/nano-bpm/branches/main/protection/required_status_checks > /tmp/nano-checks-before.json
node console/scripts/merge-gates.mjs --protection-update /tmp/nano-checks-before.json > /tmp/nano-checks-update.json
gh api --method PATCH repos/nanobpm/nano-bpm/branches/main/protection/required_status_checks --input /tmp/nano-checks-update.json
gh api repos/nanobpm/nano-bpm/branches/main/protection/required_status_checks > /tmp/nano-checks-after.json
node console/scripts/merge-gates.mjs --check-protection /tmp/nano-checks-after.json
```

The update preserves `strict` and pins every required check to GitHub Actions.
For a rename, wait for the replacement job to pass on the migration PR's fresh
head before atomically switching the required context; never remove a gate
without its replacement. Other open PRs must incorporate the renamed workflow
and produce fresh head CI before merging. Reading/updating branch protection
requires admin permission, so this live check is an explicit admin operation,
not a check run with the read-only CI token.

- Merge is a manual act. CI runs **once when the PR is opened**; follow-up pushes
  (review-fix commits) deliberately do **not** re-run CI, to keep review cycles
  cheap. So the recommended flow is: **open the PR as a draft** (`gh pr create
  --draft`) — the `opened` run gives early breakage signal — converge Copilot
  review, then **mark it ready at convergence** (`gh pr ready <n>`). Marking ready
  fires a fresh `pull_request` (`ready_for_review`) CI run on the head, and that
  run is what GitHub branch protection counts to allow the merge. Once those head
  checks are green (and review threads are resolved), merge via the UI **Merge**
  button or a **`@mergifyio queue`** comment.
- **Do not use `workflow_dispatch` as the merge finalizer** — its runs do NOT
  satisfy branch-protection required status checks (GitHub only counts the PR's
  own push/pull_request check suite). If a PR wasn't opened as a draft, produce
  the head run instead by **closing and reopening it** (`gh pr close <n> && gh pr
  reopen <n>` → `reopened` event). Symptom of getting this wrong: Mergify enters
  the queue, validates the batch, then dequeues with "N of N required status
  checks are expected". Nothing merges on its own.
- Because a PR stays open until merged, it is **safe to push follow-up commits**
  to an open PR (address review feedback, fix CI, iterate) before you merge it.
- Still keep each PR focused: land unrelated scope in its own PR rather than
  piling it onto an open one.
- Do not merge a branch you know to be in a broken intermediate state; merge only
  when the change is complete and correct.

### Converge Copilot review before merging

Every PR must be driven to **review convergence** before it is merged — use the
`pr-copilot-review-loop` skill (the "review convergence" loop) to do this:

- After opening a PR (and after each round of fixes), **re-request the GitHub
  Copilot review** and wait for its verdict.
- Triage and address each Copilot comment (fix, silently apply nitpicks, or push
  back with evidence on false positives), reply in-thread, then **re-request the
  review again**.
- **Keep looping** until Copilot's review comes back with **no actionable
  comments** — its PR-level summary reports nothing new (Copilot reviews are
  `COMMENTED`, never `APPROVED`, so the summary body is the verdict) — or the
  Copilot review is **exhausted** (it reiterates a point already addressed or
  pushed back on; two rounds of the same substantive point = converged).
- At convergence, **rebase the PR if it is behind `main`, resolve any conflicts
  and review threads**, then produce a fresh head CI run via a `pull_request`
  event — **mark a draft PR ready** (`gh pr ready <n>`) or **close+reopen** it —
  wait for green, and **merge** (UI button or `@mergifyio queue`).
- Stop early and sync with the user only if a comment genuinely **needs their
  input** (a design/product tradeoff you can't decide) — after resolving
  everything else in the round.

## Two web surfaces: the public site vs. the in-app console

There are **two** distinct HTML surfaces in this repo. Don't confuse them — a
change to one does **not** appear on the other.

1. **The public marketing/docs site — [`nanobpm.io`](https://nanobpm.io).**
   - Source: `website/`. Generated by `website/build.mjs` into `website/_site`
     and deployed to **GitHub Pages** by `.github/workflows/pages.yml` (on push
     to `main` touching `website/**`, `docs/**`, `USERGUIDE.md`, the schema
     sources, etc.).
   - Pages: landing `/` (`homePage()`), **`/architecture/`** (`architectureHtml()`
     — the *"One stack, four layers"* positioning page; its `ARCH_LAYERS` array is
     the single source of truth for the layer diagram + detail), `/demo/` (the
     Bojtos in-browser WASM demo), `/docs/` (rendered from `USERGUIDE.md` +
     `docs/*.md` via `console/scripts/build-docs.mjs`), `/whitepaper/`, and
     `/schemas/` (published JSON Schemas / OpenAPI). The shared header is
     `siteNav()` in `build.mjs`.
   - **This is where product positioning / "where Nano fits in the stack" lives.**
     Edit `architectureHtml()` (and its `ARCH_LAYERS`) rather than adding a new
     page, unless a genuinely separate page is wanted.

2. **The in-app console served by the gateway binary** (a running node at
   `:8080`). Self-contained pages under `server/src/console/*.html`, each
   `include_str!`'d and routed in `server/src/console/mod.rs`: `/`
   (`landing.html`), `/features`, `/optimization`, `/stack`, plus the `/console`
   SPA, `/swagger`, `/docs`. These ship *inside* the binary and are only visible
   on a running node — **not** on nanobpm.io.

**The landscape positioning table is shared, not duplicated.** The "Where Nano
sits in the landscape" comparison table appears on *both* surfaces (public
`/architecture#landscape` and the console `/stack` page), so it has a single
source of truth: `website/data/landscape.json`. `website/build.mjs` renders it
into `/architecture` **and** writes `server/src/console/landscape.gen.html`, a
checked-in derived artifact the server splices into `stack.html` at the
`<!--LANDSCAPE_TABLE-->` marker (`STACK_PAGE` in `mod.rs`). Edit the JSON, then
run `node website/build.mjs` to regenerate — never hand-edit either rendered
table. The `schemas` CI job runs the build and `git diff --exit-code`s the
artifact, so a forgotten regeneration fails the build instead of shipping drift.
