# ADR 0045 — Code-first workflows as a RAD authoring surface

Status: **Proposed.**
Date: 2026-07-29.

> **Decision update (2026-07-30).** The code-first surface this RAD integration exposes is the
> **declarative `defineFlow`** builder only — the single code-first authoring surface per ADR 0044's
> 2026-07-30 update. The imperative `defineWorkflow` replay function is experimental/internal and is
> **not** scaffolded. The scaffold example authors a `defineFlow` with `w.run` (local step) and
> `w.signal` (durable human-in-the-loop wait), and ships a `scripts/approve.ts` demonstrating the
> signal correlation. The third verb, `w.task` (external-worker step), is documented in the README
> and supported by the SDK but is intentionally left out of the runnable example so the scaffold
> completes end-to-end without an external worker. References to "imperative orchestration
> function" below are historical; the shipped scaffold is declarative.
Relates to:
ADR 0044 (`0044-code-first-durable-orchestration.md`, the `@nanobpm/workflow` SDK this surfaces),
ADR 0022 (`0022-nano-rad-application.md`, Urban — the model-first RAD App this sits beside),
ADR 0007 (`0007-rad-extension-system.md`, the npm-installable pack/template contract this rides),
ADR 0009 (`0009-gui-application-projects.md`, the output-kind axis a code-first project extends),
ADR 0040 (`0040-fused-domain-model.md`, the derived domain model a code-first surface projects into),
and `server/src/console/projects.rs` (`TEMPLATES`, `create_project`, the scaffolder + run supervisor),
`server/src/console/extensions.rs` (`ExtKind`, `template_source`, `copy_tree`).

## Context

The RAD (Urban, ADR 0022) is **model-first**: a developer authors a BPMN diagram in the modeller,
and the system *derives* the wiring — worker I/O from the data envelope (ADR 0040), the domain
model, forms, pages — stamping it into `nano-generated/` so `deno.json`-mapped specifiers
(`@nanobpm/worker`, `@nanobpm/domain`, …) resolve. The authoring act is *drawing the model*; the
code is derived.

ADR 0044 shipped the **code-first dual**: `@nanobpm/workflow`, where the developer writes a
declarative flow (`defineFlow`) and the SDK *derives the executable BPMN
model*, the job types, and the message/correlation wiring. The authoring act is *writing the code*;
the model is derived.

These are two ends of one axis — **both emit the same executable model onto the same engine** — and
the single-user SDLC beachhead (agents/PRs/tests, the `urban-pr-review` context) is exactly the
audience that reaches for code-first first. Today, however, code-first is only reachable as a
hand-rolled npm dependency; it is not a **surface** in the RAD. A developer who opens the console
and clicks *New Project* is offered only model-first scaffolds. This ADR makes code-first a
first-class RAD authoring surface.

Two questions this ADR must answer explicitly, because they came up directly:

1. **Where does the SDK live** — nano-bpm or the `nanobpm/nano-ide` monorepo (where the IDE packs
   live)?
2. **How is it surfaced in the RAD** so a user can develop with it end-to-end (scaffold → edit →
   run → see the model)?

## Decision

### 1. The SDK stays in nano-bpm; the RAD integration is a separate pack/template

`@nanobpm/workflow` is a **runtime SDK**, not an IDE pack. Its three couplings are all to this
repository, so it must co-evolve here:

- **BPMN emission** depends on engine semantics — the declarative builder emits service tasks,
  message catch events, and (next increment) gateways that `engine-core` must support.
- **The client** targets the gateway's REST v2 API (`server/`).
- **Its integration tests boot the sibling `server/` binary** and prove crash-resume in this repo's
  CI.

This matches precedent: `@nanobpm/engine-wasm`, `@nanobpm/bojtos-kit`, and `@nanobpm/bojtos-react`
are all runtime packages published *from* nano-bpm. The only runtime SDK in a separate repo is
`nano-sdk-js`, a generic transport client with far less engine coupling. Moving `workflow` to
nano-ide would divorce it from the engine it tracks and the CI that proves its durability claim.

What genuinely belongs where the `nano-ide-*` packs live (ADR 0007; the IDE monorepo) is the **RAD
integration** — a scaffolder/template that *depends on* `@nanobpm/workflow`. Clean split:

> **The SDK ships from nano-bpm. The IDE pack that scaffolds projects using it ships as a
> `nano-ide-app-workflow` pack (per ADR 0007), whose home is the IDE monorepo.**

### 2. Surface it via the ADR 0007 pack/template seam

A code-first project is a **different shape** from a model-first one: it has *no authored BPMN* and
*none of the model-first `nano-generated/` machinery*, because the executable model is derived from
the code at deploy time. So it must not reuse the model-first built-in scaffold (which
unconditionally stamps `resources/processes/`, the derived-worker `nano-generated/` tree, the domain
stubs, etc.). It stamps a lean tree:

```
<project>/
  deno.json          import map: @nanobpm/workflow → npm:@nanobpm/workflow ; start task
  package.json       node identity + @nanobpm/workflow dependency
  tsconfig.json      authoring-time types
  main.ts            standalone worker-host service: deploy the workflow(s) + Worker.start(), run forever
  workflows/
    pr-review.ts     a defineFlow example (the durable SDLC flow: w.run/w.signal/w.task)
  scripts/
    start-instance.ts  example client that starts one instance (kept out of the host)
    approve.ts         example client that correlates the `humanApproval` signal
  README.md
```

- **Editor**: TypeScript is already supported (Monaco `ts` worker is bundled); no new grammar.
- **Run supervisor**: reuse the existing Deno run profile — a code-first project is a TS project
  whose `start` task runs `main.ts` (`deploy` + `Worker.start()`, then runs forever). No new
  toolchain. "Run" in the console/IDE executes the same `main.ts`, so standalone and in-IDE are
  identical (the app is a true standalone runnable, like the model-first scaffolds).
- **New-Project picker**: the scaffolder's template menu (`project_templates()`) auto-surfaces it,
  so it appears alongside the model-first scaffolds with provenance.

**Prototype landing (this repo):** ship it first as a **built-in `workflow-starter` template** in
`projects.rs` — an early, lean scaffold that returns before the model-first built-ins. This proves
the whole loop out-of-the-box (no pack install needed) and gives the pack a reference tree to lift.
The richer, versioned form is the `nano-ide-app-workflow` pack in the IDE monorepo, which the
built-in can later delegate to (a builtin pack, or a first-party marketplace entry).

### 3. Round-trip visibility is the RAD payoff

The point of the RAD is that authoring and the model stay in sync. For code-first, the model is
*derived*, so the surface renders the **derived** BPMN (`toBpmn(workflow)`) **read-only** in the
modeller/explorer — the developer sees the diagram their code produced — and feeds the derived job
types (`${workflowId}:${step}`) into the console's worker/domain views (the same envelope-derived
worker-I/O plumbing model-first already uses). This is deliberately **one-way** (code → model, for
inspection): there is no model → code round-trip. The code is the source of truth; the diagram is a
lens.

### 4. App kind

The prototype records `app: "console"` (a Deno console app the run supervisor already knows how to
drive). A dedicated `app: "workflow"` kind — with a supervisor profile that runs the Worker as a
long-lived host and a workspace toolbar that shows the derived model — is a follow-on, not required
to land the surface.

## Consequences

- A developer can go *New Project → Code-first workflow → edit `workflows/*.ts` → Run* and get a
  durable orchestration on the engine, with the derived diagram visible — no diagram drawn, no
  task-type wiring, no correlation plumbing authored by hand.
- The model-first (Urban) and code-first surfaces coexist as peers on the same engine; a team can
  pick per project.
- The SDK/IDE boundary is explicit and matches the existing package topology, so releases stay
  independent (`@nanobpm/workflow` from nano-bpm; the pack from the IDE monorepo).

### Non-goals / risks

- **Not the default.** Model-first Urban remains the headline RAD surface; code-first is an
  additional axis for the developer-automation audience.
- **At-least-once idempotency (ADR 0044)** binds each `w.run` step handler; the example handlers are
  written to be idempotent and say so. (Declarative steps run as ordinary engine jobs, so the
  imperative replay-determinism constraint does not apply to this surface.)
- **Round-trip is one-way.** Editing the derived diagram is not supported; the modeller renders it
  read-only for code-first projects.
- **Versioning / version-skew** for redeploys of a changed orchestration is punted here (tracked on
  ADR 0044's checklist), same as the model-first redeploy story.
