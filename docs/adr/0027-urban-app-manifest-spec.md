# ADR 0027 — Urban App manifest (`nano.app.json`) — the binding, spec-first

Status: **Proposed.**
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md`, **Urban** — §A first sketched
`nano.app.json`; this ADR turns that example into a validated, spec-first schema and the console
**App** project type),
ADR 0024 / 0025 / 0026 (the `data` / `triggers` / `surfaces` blocks this manifest binds),
ADR 0007 (`0007-rad-extension-system.md`, **declared data, no `eval`** — the manifest is data the
host drives; and the pack axes the manifest references),
ADR 0009 (`0009-gui-application-projects.md`, the `app` output-kind axis — Urban is a new `app`
value, not a replacement),
`server/src/console/projects.rs` (`ProjectConfig` / **`nanobpm.project.json`** — the *IDE/toolchain*
descriptor whose ownership boundary with `nano.app.json` this ADR draws, `:191-265`),
`spec-console/console-api.yaml` + `scripts/generate-console.sh` (the **spec-first + codegen**
precedent this ADR follows — one schema generating both the Rust and TS types, so the two never
drift).

## Context

ADR 0022 §A shows `nano.app.json` as a JSONC *example* and says it "references the models the
editors already produce and binds them to triggers, surfaces, data, and workers" — that `app`
becomes a value of the existing output axis, and that the console gains an **App** project type
whose tabs are the three editors plus Triggers/Data/Surfaces panels editing *this manifest*. What is
missing is everything needed to actually build against it:

1. **A real schema** — types, required vs. optional, cross-reference rules — not a prose example.
2. **Validation** — where it runs and how it fails, under the ADR 0007 declared-data (no-`eval`)
   discipline.
3. **Its relationship to the file that already exists.** A console project *already* persists
   `nanobpm.project.json` (`ProjectConfig`: `name`, `deployTarget`, `main`, `lang`, `app`,
   `autoDeploy`, `env`, `toolchain`, …). The manifest must not duplicate or fight that file; the
   ownership boundary has to be explicit.
4. **The console App project type** — how it is created and how its panels edit the manifest while
   Run/Compile keeps flowing through the existing supervisor.

The repo already has the right pattern for (1)+(2): the console API is **spec-first** —
`spec-console/console-api.yaml` generates *both* the server Rust models and the console TS models
via `generate-console.sh`, precisely so hand-written DTOs can't drift from the wire (see the console
codegen discipline). The App manifest is the same problem — one document consumed by the Rust
runtime *and* the TS console — and takes the same solution.

## Decision (proposed)

### 1. Two files, one boundary

Keep **two** files with disjoint ownership; neither duplicates the other.

| File | Owns | Read by | Lifecycle |
|---|---|---|---|
| **`nanobpm.project.json`** (`ProjectConfig`, exists) | *How the project builds/runs in the IDE*: `lang`/`app` pack, `toolchain` argv, `deployTarget`, run configs, `env` | the console **supervisor** | IDE/tooling concern |
| **`nano.app.json`** (this ADR) | *What the App **is** at runtime*: models, data, triggers, surfaces, workers, llm | the **compiled App** at boot + the console **App panels** | ships inside the binary |

**Boundary rule:** anything the *supervisor* needs to spawn/compile a process lives in
`nanobpm.project.json`; anything the *running App* needs to behave lives in `nano.app.json`. The
`deployTarget` (dev-loop engine URL, ADR 0026 §4) stays project config; `runtime.engine`
(embedded|remote|cluster, the *shipping* topology) is manifest. An `app: "urban"` project carries
both.

The **entrypoint** is the one field that lives on both sides of the boundary, by design (#957). The
supervisor's run/compile entrypoint defaults to `main.ts`; an app may declare its own `entrypoint`
in `nano.app.json` so the run contract *travels with the app* (e.g. a code-first app whose bootstrap
is `src/main.ts`) rather than depending on a console-generated `nanobpm.project.json`. Precedence,
default-preserving: an **explicit** project-config `main` > the manifest `entrypoint` > the `main.ts`
default. The declared path resolves relative to the app root and Run fails with the resolved path
named if it is missing.

### 2. The manifest is declared data, versioned

`nano.app.json` is pure declared data (ADR 0007 — the host drives it; no `eval`, no code in the
manifest; handlers/workers are referenced *files*, not inline code). It carries a top-level
`schemaVersion` for forward-compat. Top-level shape (each block is specified by the ADR named):

```jsonc
{
  "schemaVersion": 1,
  "id": "home-heating",              // required, slug
  "name": "Home Heating",            // required
  "codename": "Urban",               // optional, informational (App.CODENAME)
  "entrypoint": "src/main.ts",       // optional, #957; overrides the main.ts run/compile default
  "runtime": { "engine": "embedded", "node": "single" },  // ADR 0005; ship topology

  "models":   { "processes": [...], "decisions": [...], "forms": [...] }, // glob refs
  "data":     { "default": "app", "sources": { ... } },   // ADR 0024
  "triggers": [ ... ], "connections": { ... },            // ADR 0025
  "surfaces": { "taskInbox": {...}, "chat": {...} },       // ADR 0026
  "security": { "mode": "none", "providers": [...], "roles": [...], "rules": {...} }, // ADR 0028
  "workers":  [ ... ],                                     // ADR 0022 §E (files or llm)
  "llm":      { ... }                                      // ADR 0022 §E
}
```

Every value may use `${VAR:-default}` substitution (§5). Every block's detailed schema is owned by
its ADR (0024/0025/0026/**0028**/0022 §E); this ADR owns the *envelope*, the *cross-reference rules*,
and the *codegen*.

### 3. Spec-first: one schema, generated types (the anti-drift decision)

The canonical source of truth is a **JSON Schema** at `spec-app/nano-app.schema.json`, a sibling of
`spec-console/`. A generator (`scripts/generate-app-manifest.sh`) emits, from that one schema, the
**TypeScript** `AppManifest` types.

Both consumers of the manifest are TypeScript: the console **App panels** (authoring) and the Deno
**App loader** (running). The manifest is *not* read by the Rust server — per §1 it is read by "the
compiled App at boot + the console App panels", and the server handles project files as opaque bytes.
So this is a **TS-only** codegen: unlike `console-api.yaml`, whose Rust side is the server itself,
`nano.app.json` has no Rust consumer, and adding a Rust emitter would only invite drift against a type
nothing reads. (Should a future Rust consumer appear — e.g. a server-side pre-flight validator — a
`typify` Rust emitter can be added from the *same* schema without changing the source of truth.)

This applies the `console-api.yaml → generate-console.sh` spec-first discipline to the manifest, so a
new manifest field cannot be silently dropped by a hand-written DTO. The schema is also publishable as
the `$schema` a maker's editor uses for `nano.app.json` autocompletion.

### 4. Validation — fail-closed at three gates

The generated types give *shape* validation; the manifest additionally has **cross-reference** rules
enforced by a shared validator (one implementation, surfaced at all three gates):

- **Console save/load** — inline diagnostics in the App panels (JSON-pointer to the offending node).
- **`deno compile`** — the build **fails closed** on an invalid manifest (no shipping a broken App).
- **App boot** — validate before starting the engine/triggers/surfaces; refuse to start with a clear
  error rather than half-run.

Cross-reference rules (beyond shape): every `models.*` glob resolves to ≥1 file; every `triggers[]`
`action.start` names a deployed process and `action.message` a declared message; every
`data`/`triggers[].connection`/`workers[]`/`llm` reference resolves; `surfaces.chat.agent` names a
declared `llm`. These are the errors that otherwise surface as a confusing runtime drop (cf. ADR 0025
§5's silent `CorrelateMessage` no-op).

**Authoring-time enumeration.** ADR 0029 (`0029-urban-bindings-domain-model.md`) turns these
after-the-fact rules into *pickers*: a project **symbol index** enumerates the valid ids so a maker
selects a process/message/form/decision instead of typing one, and the §4 validator collapses into
"the referenced id ∈ the index." It also adds the compile-time **domain type** layer the envelope
binds against.

### 5. Env & secret substitution — resolved at boot, never persisted

`${VAR}` / `${VAR:-default}` are resolved **at App boot** (and at IDE Run) from the environment,
never written back into the file. Consequence: the `nano.app.json` a maker commits and shares carries
**no secrets** — connection strings, LLM keys, and webhook secrets are env references (the resolution
of ADR 0024's and ADR 0025's "secrets" open questions at the manifest layer). The validator checks
*shape* of a reference, not the resolved value (which may be absent at author time).

### 6. The console App project type

Urban is a new **`app`** pack value (ADR 0009 `lang × app`) — e.g. `app: "urban"` — not a change to
`console`/`deno-gui`. Creating an App project scaffolds a `nano.app.json` (+ the model dirs, `db/`,
`public/`) and a `nanobpm.project.json` whose `toolchain` compiles the App binary. The workspace
tabs are the **existing** editors (BPMN/DMN/Form, unchanged) plus **Triggers**, **Data**, and
**Surfaces** panels that are typed editors over `nano.app.json` blocks (using the §3 generated TS
types). Run/Compile flow through the **existing supervisor** unchanged — the App project type adds
*manifest-editing surfaces*, not a new build path.

## Phased plan

1. **manifest-schema** — author `spec-app/nano-app.schema.json` (envelope + the 0024/0025/0026/§E
   blocks) and `generate-app-manifest.sh` emitting the TypeScript `AppManifest` types; wire into
   `make generate` (via `make generate-app-manifest`).
2. **manifest-validator** — the shared cross-reference validator + the three fail-closed gates (§4);
   the boot gate first (it protects the runtime).
3. **app-project-type** — the `app: "urban"` scaffold + the Triggers/Data/Surfaces typed panels over
   the generated types; Run/Compile bridges to the existing supervisor.
4. **env-substitution** — `${VAR:-default}` resolution at boot/Run + the "no secrets in the bundle"
   guarantee (shared with ADR 0024/0025 secrets).

## Consequences

- The manifest becomes the **keystone** ADR 0022 called it — a validated contract every Urban
  subsystem (0024/0025/0026, §E) binds through, with one generated type on both the Rust and TS
  sides so authoring and running share one schema.
- The `nanobpm.project.json` / `nano.app.json` split keeps IDE/toolchain concerns out of the shipping
  App and vice-versa; the existing supervisor and project config are untouched.
- Fail-closed validation turns whole classes of "silent runtime no-op" (a mistyped trigger target, a
  missing form) into an authoring-time error with a pointer.
- No secrets in the shared bundle — env-reference substitution at boot makes the committed manifest
  safe to distribute.
- One new spec dir (`spec-app/`) + one generator, consistent with `spec-console/`.

## Open questions

- **Generator choice** — Rust from JSON Schema via `typify`/`schemars`, TS via
  `json-schema-to-typescript`? Or reuse the OpenAPI toolchain by expressing the manifest as an
  OpenAPI `components.schemas` fragment for symmetry with `console-api.yaml`?
- **One file vs. includes** — does a larger App want `nano.app.json` to `$ref`/include per-concern
  files (e.g. `triggers.json`) so the panels edit smaller documents, or stay single-file for
  shareability?
- **Model binding granularity** — globs (`processes/*.bpmn`) vs. explicit ids with a
  processId↔file map; how does deploy (auto-deploy dirs in `nanobpm.project.json`) reconcile with
  `models` in the manifest so a resource isn't declared twice?
- **Runtime-topology validation** — should `runtime.engine: cluster` force a stricter subset (e.g.
  the ADR 0025 multi-node source-ownership question), rejected at compile for a single-file App?
- **Schema evolution** — migration policy when `schemaVersion` bumps (auto-migrate on load in the
  console, like the read-model schema version, vs. an explicit `nano app upgrade`)?
- **Manifest as MCP descriptor** — ADR 0022 §"Open questions" asks whether an App exposes itself as
  an MCP server; if so, is that a `surfaces.mcp` block here or a separate descriptor?
  **Settled (2026-11-11, epic #1499):** an App is exposed via Camunda's MCP contract
  (`/mcp/cluster`, `/mcp/processes`, tools marked with `io.camunda.tool:*` properties in the
  BPMN); any Urban-specific manifest extras are additive over it.
