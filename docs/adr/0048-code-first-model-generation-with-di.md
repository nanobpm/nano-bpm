# ADR 0048 — Code-first model generation with diagram layout (on-disk BPMN)

Status: **Proposed.**
Date: 2026-07-30.

Relates to:
ADR 0044 (`0044-code-first-durable-orchestration.md`, the `@nanobpm/workflow` SDK),
ADR 0045 (`0045-code-first-workflows-rad-surface.md`, the RAD surface + the in-memory Derived Model
viewer this complements),
ADR 0047 (`0047-declarative-flow-control-and-typed-envelopes.md`, the typed data-envelope carrier the
generated models now carry to disk),
ADR 0040 (`0040-fused-domain-model.md`, the envelope scan the generated models feed),
ADR 0033 §6 increment 12 (`envelope_scan::scan_project` — the `resources/processes/*.bpmn` scan surface),
and the code: `workflow/src/layout.ts` (`layoutBpmn` / `declarativeToLayoutedBpmn`, the DI generator),
`server/src/console/projects.rs` (`generate_models`, the server-side generation path that mirrors
`derive_models`), `server/src/console/mod.rs` (`is_workflow_source`, `regenerate_workflow_models`),
`server/src/console_api.rs` (the save + create hooks).

## Context

Code-first workflow projects (ADR 0045) have **no authored `.bpmn`**. The executable model is derived
from the `workflows/*.ts` by `@nanobpm/workflow`. That derivation happened in two places, both
**in-memory** and both emitting **DI-less semantic BPMN** (process, flow nodes, sequence flows,
messages — no `bpmndi:` diagram):

1. **Deploy/run time** — `client.deploy(wf)` → `toBpmn(flow)` → POST to the engine as an in-memory
   blob. Never touches the project tree.
2. **The console "Derived model" viewer** (ADR 0045) — `GET /derived-models` → `derive_models()` runs
   a sandboxed Deno driver that imports the flows, runs `toBpmn`, and returns `[{id, kind, xml}]` as
   JSON. In-memory, no disk write.

The emitter (`declarativeToBpmn`) produces **zero DI** on purpose: the engine runs the *semantic*
model, and a derived flow has no diagram of its own. But that leaves two gaps:

- **Nothing renders.** DI-less XML opens as a blank canvas in bpmn-js / any modeller, so a code-first
  flow is not visually inspectable, reviewable, or round-trippable.
- **No unification with the model-first surface.** Model-first Urban projects keep authored
  `resources/processes/*.bpmn` on disk — the surface `envelope_scan::scan_project` reads to derive the
  Fused Domain Model (worker/message I/O + custom headers). Code-first projects had nothing there, so
  their contracts (now carried as `nano:shape` / `io.nanobpm.dataEnvelope` per ADR 0047) never reached
  the domain-model derivation, and there was no single artifact bridging the two ends of the authoring
  axis.

PR #376 added `layoutBpmn` / `declarativeToLayoutedBpmn` (semantic BPMN → bpmn-io's `bpmn-auto-layout`
→ a laid-out model *with* DI), gated on the optional `bpmn-auto-layout` peer dependency. But nothing in
the project pipeline called it: the deploy and derive paths still used plain `toBpmn`.

## Decision

**On save of a `workflows/*.ts` file, (re)generate the corresponding laid-out `.bpmn` model(s) — with
DI — onto the model-first scan surface `resources/processes/<id>.bpmn`.** The workflow *code* stays the
single source of truth; the generated `.bpmn` are derived, always-regenerable artifacts that unify the
code-first project with the model-first world.

Concretely:

- **Generator (`projects::generate_models`).** Mirrors `derive_models`: a least-privilege Deno driver
  imports the project's `workflows/*.ts`, and for each exported Workflow runs
  `layoutBpmn(toBpmn(flow))` — deriving the semantic model then adding a `bpmndi:` diagram. The driver
  prints `[{id, kind, xml}]`; the *server* writes each model to `resources/processes/<id>.bpmn`
  (filenames sanitized so a hostile id can never escape the directory). The sandbox reads the project,
  fetches the SDK + layout dep from the registry, and writes **only** to the Deno cache — the model
  files are written by the server, not the sandbox. No `--allow-env`.
- **Layout resolution.** `layoutBpmn` lazily `import()`s the optional `bpmn-auto-layout` peer dep, and
  the layout helpers ship in `@nanobpm/workflow >= 0.3.0`. Neither is guaranteed in an older project's
  `deno.json`, so generation runs under a **synthesized import map** (`--import-map` + `--no-config`,
  since Deno forbids a map from both a discovered `deno.json` and the flag). The map merges the
  project's own imports and *defaults in* the SDK (`^0.3.0`) and `bpmn-auto-layout`
  (`^2.0.0-alpha.2`) when absent, never overriding an explicit project pin.
- **Trigger (`is_workflow_source` + `regenerate_workflow_models`).** The code-first inverse of
  `is_model_resource`: a `.ts` directly under `workflows/` re-generates the models on save. Because the
  output lands on the scan surface, generation then refreshes the derived domain/worker types from the
  new models — exactly as an authored `.bpmn` save does. The scaffold also kicks an initial generation
  on project create (fire-and-forget) so a brand-new project opens with a rendered model.
- **Envelope fidelity.** `layoutBpmn` preserves `zeebe:` extension elements and the `nano:shape` /
  `io.nanobpm.dataEnvelope` carrier (ADR 0047) through the round-trip, so the generated models are a
  valid scan surface: the typed envelopes authored in code flow straight into the Fused Domain Model.
- **Scaffold pins.** The `workflow-starter` scaffold pins `@nanobpm/workflow@^0.3.0` and adds
  `bpmn-auto-layout` (deno.json import + package.json devDependency) so local `deno task`/`npm` layout
  works too.

Best-effort throughout: a missing Deno toolchain, an unpublished SDK, or a layout failure surface as a
logged skip, never a failed save — the in-memory semantic derivation (deploy + viewer) still works.

## Consequences

- **Code-first flows are now visually inspectable and round-trippable.** `resources/processes/*.bpmn`
  open rendered in a modeller/viewer, and reviewers see a diagram in a diff.
- **One scan surface.** Code-first and model-first projects both derive their domain model from
  `resources/processes/*.bpmn`; the ADR 0047 typed envelopes finally reach the derivation.
- **Derived, not authored.** The generated `.bpmn` are regenerated on every `workflows/*.ts` save, so a
  hand-edit in the modeller is clobbered on the next code save (a leading provenance comment says so).
  Code is the source of truth; ejecting to model-first means *taking over* the file (removing the flow),
  a follow-up affordance.
- **Runtime prerequisite.** The layout helpers live in `@nanobpm/workflow@0.3.0`, published on
  2026-07-30 (tag `workflow-npm-v0.3.0`); generation resolves the SDK from the registry. (Historically,
  before that publish, generation degraded gracefully to a logged skip.)
- **The Derived Model viewer renders.** `derive_models` now overlays each model's XML with the on-disk
  auto-laid-out `resources/processes/<id>.bpmn` (matched by `PROVENANCE_MARKER`), so the console panel
  shows a real diagram instead of a blank canvas. It also falls back to those on-disk models when the
  live Deno derivation is unavailable (no toolchain), fails transiently, or yields nothing — so an
  already-generated project always renders. The live derivation stays authoritative for `id`/`kind`;
  the on-disk read only supplies DI (and, in the fallback, the whole model with `kind: "generated"`).
- **Follow-up.** An explicit "eject to model-first" action (take over a generated `.bpmn`, remove the
  flow from `workflows/`).
