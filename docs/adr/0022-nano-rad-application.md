# ADR 0022 — Nano RAD Application (the App bundle: triggers, process, forms, decisions, data — embedded by default)

Status: **Proposed.**
Date: 2026-07-21.
Relates to: ADR 0005 (`0005-embedded-u-nano.md`, **Bernd** — the embedded engine and the
"single application source, choose deployment by config" position this ADR completes),
ADR 0007 (`0007-rad-extension-system.md`, the npm-installable pack contract),
ADR 0008 (`0008-polyglot-language-packs.md`, language axis),
ADR 0009 (`0009-gui-application-projects.md`, the served-UI binary this ADR generalizes),
ADR 0011 (`0011-editable-model-workbench.md`, human authoring surfaces),
ADR 0021 (`0021-process-sla-as-a-first-class-abstraction.md`),
`console/src/components/{BpmnModeler,DmnModeler,FormEditor}.tsx` (the three model editors
that already exist), `packages/app-deno-gui/` in `nanobpm/nano-ide` (the served-UI pack this
ADR grows into an App), `server/src/console/projects.rs` (scaffold + run/compile supervisor),
`engine-core/src/bpmn.rs` (`is_adhoc` — today's collapse-ad-hoc-to-one-job behavior, the §E.1
parity gap), and — for Camunda agentic switch-over parity — `~/workspace/camunda/zeebe`
(`protocol-impl/.../adhocsubprocess/AdHocSubProcessActivateElementInstruction.java`,
`protocol-impl/.../job/JobResult.java` `activateElements`), plus the Camunda AI Agent connector +
MCP Client connector (job-worker connectors, portable via the ADR 0005 transport seam).

## Signature — why "Urban"

The RAD application model is named **Urban**, after **Adam Urban** — a Camunda engineer whose
career reaches back to **Turbo Pascal**, and who has therefore carried the Rapid Application
Development lineage *in person* across its whole arc: from Borland's form-painter-plus-native-
compiler loop, through the years of enterprise workflow tooling, to the Camunda engine whose
distillation Nano is. Delphi's contribution was never the compiler; it was the *binding* — a
form painter, an event model, data-aware controls, and a native compiler unified into one tight
loop where you dropped a control, wrote the handler, and shipped a single `.exe`. That loop is
what turned a business problem into a running program in an afternoon, and it is what a
developer who started on Turbo Pascal and stayed through to Camunda has been building, in one
form or another, the entire time.

Nano-as-RAD sits directly on that inheritance. It could have offered "build a bespoke frontend,
or run Operate + Tasklist" — the Camunda-8 default — but that is not RAD; it is integration
work. Urban is the position that the *binding* is the product: an **App** ties triggers,
processes, decisions, forms, and data into one artifact that compiles to a single binary and
runs with no external server. Naming it for a Camunda engineer who has lived that lineage keeps
the ADR 0015 discipline — Nano is a distillation of Camunda engineering, so its RAD face is
signed by one of the people who carried the idea here. We sign the work.

Naming conventions that follow (same discipline as ADR 0005 §"Signature — why 'Bernd'"):

- **Manifest / bundle**: the `nano.app.json` file and the directory it heads are a **Nano App**.
  The *feature* is Urban; the *noun a user types* is "App".
- **Codename constant**: `App.CODENAME` (public, `"Urban"`).
- **Boot banner**: `nano app [urban] "<name>" v<version>` on startup of a compiled App.
- **Env-var prefix**: `NANO_APP_*` for App-runtime config (`NANO_APP_DB_PATH`,
  `NANO_APP_BIND`, …).
- **ADR references**: `Urban` (proper noun) for the feature; `App` / `nano.app.json` for the
  concrete artifact; `Bernd`/`EmbeddedEngine` for the engine it embeds.

The rule, as with Bernd: a user reads `nano.app.json` and understands what it is immediately;
they eventually ask *"who's Urban?"* — which is when the signature does its work.

## Context

We want a person to install Nano on their computer and use it to **make useful applications the
way Borland Delphi let them** — to automate a task on their machine, in their business, or in
their home. The pieces to do this already exist or are in flight, but they are not yet *bound*:

### What already exists (the load-bearing facts)

- **Three model editors ship in the console** today: `BpmnModeler.tsx` (processes),
  `DmnModeler.tsx` (decisions, dmn-js), and `FormEditor.tsx` (forms). The authoring surfaces
  the user asked for are largely present, not missing.
- **Bernd (ADR 0005)** already delivers a *single application source* that runs against remote
  Nano or Camunda **or** cross-compiles into a self-contained native binary with a **single-node
  engine embedded** — no external server. The "Embed Bernd" button is the mechanism; this ADR
  makes it the **default** for Apps.
- **The pack system (ADR 0007) + `app-deno-gui` (ADR 0009)** already scaffold a `Deno.serve`
  binary that serves a UI and drives the engine, and `deno compile` already emits a
  self-contained binary with the frontend bundled.
- **Projects are self-contained dirs** with a config file and a template-driven scaffolder;
  run/compile is a supervisor over the user's on-machine toolchain (`projects.rs`).

### What is missing (the gap this ADR closes)

1. **The binding.** There is no first-class object that says *"these triggers start these
   processes, which raise these user tasks rendered by these forms, evaluate these decisions,
   and read/write this database."* Delphi's magic was this binding; Nano has all the parts and
   none of the glue. Today `app-deno-gui` is a hand-wired proxy, not a declared App.
2. **Triggers — the Zapier primitive.** Nothing lets an installed Nano *do something on its own*
   in response to the world: a cron tick, an inbound webhook, a watched file/folder, an IMAP
   message, an MQTT topic. This is the single largest new runtime primitive and the thing that
   makes an installed App feel alive rather than a website you must open.
3. **A default data layer.** Automations need durable local state and small relational data.
   There is no App-owned database or DB manager.
4. **Generic human surfaces.** RAD's promise is *not* "hand-build a frontend." An App needs a
   **generic task inbox** (a Tasklist-lite that renders the App's forms) and a **chat surface**
   — so the maker gets a working UI for free.
5. **An LLM seam.** Two concrete roles (below) are wanted but unspecified.

### The strategic fork this ADR settles

Nano's throughput identity is a Raft cluster (RF3, group-commit, admission control). The RAD
persona — a person automating their laptop, business, or home — wants the **opposite defaults**:
zero-ops, single-node, embedded, one binary. Those personas want different defaults, and trying
to serve both from one default is what makes "install it like Delphi" die on setup friction.

> **Decision driver:** For Nano **Apps**, *development* always targets the **engine already running in
> the IDE** (ADR 0026 §4) — a maker never stands up an engine to build; they deploy into the console's
> live Nano. *Shipping* then offers **two targets, chosen by config, never a rewrite** (ADR 0005): a
> **self-contained single-file binary** with the engine embedded ("look — no server"), or a **hosted
> frontend against a remote Nano cluster** (single- or multi-node). The self-contained binary is the
> **on-ramp** — the fastest way to hand someone a working App — and the cluster is where an App **grows
> up** for real workloads. Embedded is a *ship mode and the on-ramp*, not a default *runtime*.

## Decision (proposed)

Introduce the **App** as a first-class RAD artifact — an `app` **kind** that *composes* the
existing model editors, packs, and Bernd — described by a single declarative manifest
`nano.app.json`, run and compiled by the existing supervisor, and defaulting to embedded Bernd.

Five parts: **(A)** the App manifest (the binding), **(B)** the trigger runtime, **(C)** the
human surfaces (task inbox + chat), **(D)** the data layer (SQLite), **(E)** the LLM seam. Each
is additive over an existing seam; none is a rewrite.

### (A) The App manifest — `nano.app.json` (the binding)

A single declared-data manifest (no `eval`; the host drives declared data, exactly as ADR 0007
established for packs). It *references* the models the editors already produce and *binds* them
to triggers, surfaces, data, and workers.

```jsonc
{
  "id": "home-heating",
  "name": "Home Heating",
  "codename": "Urban",               // App.CODENAME, informational
  "runtime": { "engine": "embedded", "node": "single" },  // default; "remote"|"cluster" opt-in

  "models": {
    "processes": ["processes/*.bpmn"],
    "decisions": ["decisions/*.dmn"],
    "forms":     ["forms/*.form"]     // form-js schemas from FormEditor
  },

  "data": {                           // (D) — App-owned SQLite
    "driver": "sqlite",
    "path":   "${NANO_APP_DB_PATH:-./app.db}",
    "migrations": "db/migrations"
  },

  "triggers": [                       // (B) — the Zapier primitive
    { "id": "morning", "type": "cron",    "spec": "0 6 * * *",
      "action": { "start": "heating-cycle" } },
    { "id": "sensor",  "type": "webhook", "path": "/hooks/temp",
      "action": { "message": "temp-reading", "correlationKey": "= body.room" } },
    { "id": "inbox",   "type": "imap",    "connection": "mailbox",
      "action": { "start": "triage-email" } }
  ],

  "surfaces": {                       // (C) — free UI
    "taskInbox": { "enabled": true, "path": "/tasks" },
    "chat":      { "enabled": true, "path": "/chat", "agent": "concierge" }
  },

  "workers": [                        // service tasks; language via ADR 0008 packs
    { "taskType": "read-thermostat", "handler": "workers/thermostat.ts" },
    { "taskType": "classify",        "llm": "classifier" }   // (E) LLM-as-worker
  ],

  "llm": {                            // (E)
    "classifier": { "provider": "env", "model": "${NANO_APP_LLM_MODEL}",
                    "output": { "decision": "email-triage" } },  // DMN-constrained output
    "concierge":  { "provider": "env", "model": "${NANO_APP_LLM_MODEL}",
                    "tools": ["start-process", "complete-task", "query-data"] }
  }
}
```

`app` becomes a value of the existing `kind`/output axis (ADR 0007/0009), so `lang × app`
still holds: Deno+App now, Rust/Java+App later. The console gains an **App** project type whose
workspace tabs are exactly the existing editors (BPMN/DMN/Form) plus new **Triggers**, **Data**,
and **Surfaces** panels that edit *this manifest*.

**Specified in ADR 0027** (`0027-urban-app-manifest-spec.md`): `nano.app.json` as a validated,
**spec-first** schema (`spec-app/nano-app.schema.json` generating both Rust and TS types), its
ownership boundary with the existing `nanobpm.project.json` (IDE/toolchain) file, fail-closed
validation at console/compile/boot, `${VAR:-default}` secret substitution at boot, and the console
**App** project type (`app: "urban"`).

### (B) Trigger runtime

A trigger source produces events; each trigger's `action` maps the event to **start a process**
or **publish a message** (correlation via a FEEL expression over the event body — the same
message-correlation Nano already has). Ship a small, extensible set first:

- `cron` — schedule (in-process scheduler).
- `webhook` — an HTTP path the App's server exposes (reuses the `Deno.serve` backend).
- `file` — watch a path/glob for create/modify.
- `imap` / `mqtt` — poll/subscribe (connection defined once, referenced by id).

Triggers are **declared data**; the runtime owns the loop. New trigger *types* arrive as
`nano-ide-trigger-*` packs under the ADR 0007 contract (a fourth specialization beside
`lang`/`app`), so the community can add sources without touching the core. This is the "Zapier
for yourself" wedge and should be built **first** — it is what makes an installed App act.

### (C) Human surfaces — task inbox + chat (Tasklist/Operate, replaced)

Instead of "build a frontend" or "run Operate + Tasklist," an App gets two generic surfaces for
free, both rendering the **same** form-js schemas the FormEditor produces:

- **Task inbox** (`/tasks`): a Tasklist-lite that lists the App's open user tasks and renders
  their forms to claim/complete. This is the default UI; the maker writes zero frontend.
- **Chat** (`/chat`): a conversational surface. *Chat is not a new subsystem — it is another
  trigger plus another form renderer.* A chat turn can start a process, complete a user task, or
  query data; a user task can be surfaced as a chat prompt instead of a form. This dissolves the
  "how do LLMs fit the UI" uncertainty: the chat agent (E) drives the same human-task API.

Hand-built bespoke frontends remain fully supported (the `app-deno-gui` path); the inbox + chat
are the *batteries-included default* so a maker is productive on day one.

**Expanded in ADR 0026** (`0026-urban-human-surfaces-and-run-model.md`): the **App action API** (the
UI trigger surface, with the attended-sync vs. unattended-inbox split against ADR 0025), the three
UI-generation tiers, the form-js **viewer** as an App-side runtime dependency (distinct from the
console's editor), and the develop/run loop (IDE engine on `:8080`, app under test on its own port,
embedded-vs-remote as a compile-time choice).

### (D) Data layer — SQLite, App-owned

Automations need durable local relational state. The App owns a **SQLite** database
(`data` block above), with a migrations dir and a console **Data** panel (a lightweight DB
manager: browse tables, run queries, edit the schema). Deno's SQLite bindings back it; the file
lives beside the App's journal. Workers and the chat agent read/write it through a thin
App-data API (also exposed to FEEL for decisions). This is the "data-aware controls" of the
Delphi analogy.

**Expanded in ADR 0024** (`0024-urban-data-layer-datasource-abstraction.md`): the single SQLite
`data` block above is generalized into named, env-swappable **datasources** (the BDE-alias seam),
so an App develops on embedded SQLite and deploys on Postgres by config alone, with a
`nano-ide-data-*` driver pack axis and the binding contract for forms/workers/FEEL.

### (E) LLM seam — two roles, both already-shaped slots

1. **LLM-as-job-worker.** A service task whose handler is an LLM call. Structured output is
   **constrained by a referenced DMN decision or JSON schema** — the decision layer is exactly
   how an LLM's output is kept on rails (`workers[].llm` + `llm.<id>.output.decision`). This is
   shippable now and needs no new engine concept.
   **Status: shipped (Tier-1 batteries-included runtime).** `server/src/console/llm_worker.ts` is
   materialised into every project as `nano-generated/llm-worker.ts` (alias `@nanobpm/llm`) and booted by
   the App entrypoint via `startLlmWorkers()`: for each `workers[]` entry with an `llm` binding it
   registers a Falcon worker on that `taskType` whose handler builds a chat request from the job
   (`prompt`/`messages`/`system`), calls an OpenAI-compatible endpoint (`provider: "env"` →
   `NANO_APP_LLM_BASE_URL` default Ollama, `NANO_APP_LLM_API_KEY`, `NANO_APP_LLM_MODEL`), and — when
   `output` is set — parses JSON, optionally routing it through `output.decision`'s DMN evaluation as
   the rails. No handler code and no external connector runtime required; works fully offline.
2. **Chat agent.** The `/chat` surface's agent, whose *tools* are App capabilities
   (`start-process`, `complete-task`, `query-data`). It is the human surface (C) wired to an LLM
   rather than to forms.

Provider is configuration (`provider: "env"` → local/remote model via env), so no vendor is
baked in and a fully-local model works.

### (E.1) Switch-over parity with Camunda's agentic orchestration

The ADR 0005 promise is *one application source, choose the engine by config* — it must hold for
**agentic** apps too, or Nano forks the ecosystem exactly where Camunda is investing. The good
news, verified against the current Camunda source (`~/workspace/camunda/zeebe`, HEAD 2026-07-21)
and docs: **Camunda's agentic orchestration is not a new engine concept.** It is three portable
pieces plus one engine primitive:

1. **Engine primitive — the ad-hoc sub-process.** The only engine-level addition. The agent
   dynamically activates *inner* elements of an `adHocSubProcess` as real token flow. The
   contract is small and concrete:
   - `AdHocSubProcessActivateElementInstruction` = `{ elementId, variables }` (see
     `protocol-impl/.../adhocsubprocess/AdHocSubProcessActivateElementInstruction.java`).
   - Job completion carries these back via **`JobResult.activateElements[]`**, plus
     **`isCompletionConditionFulfilled`** and **`isCancelRemainingInstances`**
     (`protocol-impl/.../job/JobResult.java`). The completion condition is a standard BPMN
     `<completionCondition>` FEEL expression.
2. **The AI Agent connector is *just a job worker*.** It reads the ad-hoc container's inner
   activities as the available *tools*, calls the LLM, and returns `activateElements` to run the
   chosen tools; it loops, keeping short-term memory in process variables. Two variants:
   **AI Agent Sub-process** (autonomous tool-sequencing over an ad-hoc container) and **AI Agent
   Task** (a single LLM invocation) — which map exactly onto our two roles above (chat agent /
   LLM-as-job-worker). Per ADR 0005, job workers are already portable across engines via the
   transport seam, so the **unmodified Camunda connector runs against Nano** *iff* the engine
   honors the ad-hoc contract.
3. **Tools = BPMN activities + MCP.** A tool is any callable inner element (service/user/connector
   task); additionally the **MCP Client connector** discovers tools at runtime over the **Model
   Context Protocol** (Anthropic's open standard — *not* "multi-cluster"; a common web
   mischaracterization). MCP is the interop point for "LLMs as task workers or in chat."

**Where Nano stands today (the parity gap).** Nano's engine parses `adHocSubProcess` but
**collapses it into a single opaque service job and prunes the inner tools**
(`engine-core/src/bpmn.rs:881` `is_adhoc`, `:1044` prune-tools comment). That yields *deploy
parity* (the model deploys and an external agent worker can drive tools out-of-band) but **not
execution parity**: the engine never activates individual tools, so there is no per-tool token,
no audit trail in the read model, no `completionCondition`, no `cancelRemainingInstances`, and
Nano's job-completion result has **no `activateElements` field** (grep confirms: absent in
`engine-core`/`server`).

**Parity strategy — adopt the contract, don't reinvent it.** Urban's LLM seam (E) is defined as a
*batteries-included implementation of Camunda's agent contract*, not a divergent mechanism, along
three tiers that mirror ADR 0005's deployment modes:

- **Tier 0 — deploy parity (today).** Ad-hoc model deploys; an agent worker drives tools
  out-of-band. Works now; no engine-visible tool execution.
- **Tier 1 — execution parity (the target).** Implement ad-hoc **activate-element** semantics in
  the engine and add `activateElements[] / isCompletionConditionFulfilled /
  isCancelRemainingInstances` to Nano's job result. Then the **unmodified Camunda AI Agent
  connector** runs identically on embedded Bernd and remote Nano, with tools visible in the audit
  trail — true switch-over.
- **Tier 2 — Urban-native default.** The embedded LLM seam ships a built-in agent (provider via
  env, **MCP** tools, optional **DMN-/schema-constrained output** as a Nano value-add layered on
  the connector's FEEL response mapping) so an App is agentic **offline with no external connector
  runtime** — yet the BPMN, the ad-hoc container, the variable shapes, and the MCP tool wiring are
  **byte-identical to Camunda's**, so lifting the App onto Camunda 8 (or a Camunda agentic process
  onto Nano) "just works."

The rule: **any Urban-specific agent nicety (DMN rails, embedded provider, local MCP) must be
additive over Camunda's ad-hoc + AI-Agent-connector + MCP contract, never a substitute for it.**
That is what keeps switch-over parity for Camunda users.

### Runtime & packaging

The dev loop and the ship targets are distinct decisions (this refines an earlier framing that named
embedded the *default runtime*; per ADR 0026 §4 and the run-model correction, dev is always the IDE
engine and embedded is one of two *ship* modes):

- **Develop against the IDE's running engine** (ADR 0026 §4). A maker deploys into the console's live
  Nano (default `:8080`); there is no per-App engine to start. This is the fast RAD loop, and it is the
  same regardless of which target the App will eventually ship to.
- **Ship A — self-contained binary (the on-ramp).** `deno compile --include public --include db
  main.ts` emits one file: engine (embedded Bernd, ADR 0005) + triggers + surfaces + data + frontend.
  Install = copy one file and run — "look, no server." The fastest way to hand someone a working App,
  and the emotional core of the Delphi lineage.
- **Ship B — hosted frontend on a remote cluster (where it grows up).** `runtime.engine: "remote" |
  "cluster"` points the **same App source** at a remote Nano — single-node or multi-node — via the ADR
  0005 transport seam, with **no source change**. Where production workloads live once an App outgrows
  the single file.

Both ship targets are the *same App source*; embedded-vs-hosted is a compile/config choice, not a
rewrite. RAD makers never *start* by operating a cluster — they develop in the IDE and ship the
on-ramp binary first — but growth to a hosted cluster is a one-line change when it comes.

## Consequences

- Nano gains a coherent **product persona** (Delphi-for-automation) distinct from, and layered
  cleanly over, the throughput-cluster persona — with an explicit default fork so neither
  compromises the other.
- The three existing editors (BPMN/DMN/Form) get a *reason to be bound together*; the App
  manifest is the missing keystone, not new modeling surface.
- One genuinely new runtime subsystem — the **trigger runtime** — plus a packable
  `nano-ide-trigger-*` axis; everything else composes existing seams (packs, supervisor,
  `deno compile`, Bernd, human-task API, FEEL correlation).
- A maker can dogfood a real automation end-to-end (release pipeline, soak-summarizer, home
  sensor loop) with no hand-written frontend and no server to operate — the fastest path to
  discovering the use cases that are currently hard to name.

## Open questions

- **Trigger delivery semantics** under embedded single-node: at-least-once with an idempotency
  key on `action`? How do we survive App restart with a webhook mid-flight (durable inbox table
  in the App SQLite)?
- **Form runtime**: confirm form-js as both the FormEditor schema *and* the inbox/chat renderer,
  so authoring and running share one schema.
- **Chat agent boundaries**: which App capabilities are safe as LLM tools by default, and how is
  consent/guardrail expressed in the manifest?
- **Ad-hoc execution parity (§E.1)**: what is the minimum engine work to reach Tier 1 — implement
  ad-hoc `activateElements` + `completionCondition` + `cancelRemainingInstances` so the unmodified
  Camunda AI Agent connector runs on Bernd — and does embedded Bernd need the full read-model
  audit trail of tool activations to match Operate, or a reduced one?
- **MCP surface**: does an Urban App only *consume* MCP tool servers, or also *expose* its own
  processes/tasks as an MCP server (making the App itself an agent tool for other systems)?
  **Settled (2026-11-11, epic #1499): yes — Nano exposes Camunda's MCP gateway contract
  (`/mcp/cluster`, `/mcp/processes`); any Urban-specific extras are additive over it.**
- **SQLite vs the engine journal**: two files, or does App data ride the engine's varstore? Keep
  them separate for a clean DB-manager story, or unify for one durability model?
- **Trigger packs vs first-party set**: which sources are core (cron/webhook/file) vs pack-only
  (imap/mqtt/cloud)?
- **Distribution**: is the compiled App the unit a maker shares, and do we want an App registry
  later (beyond the pack marketplace)?
