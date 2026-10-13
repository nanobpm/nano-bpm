# ADR 0029 — Urban bindings & the domain model (typed references, compile-time types over untyped runtime)

Status: **Proposed.**
Date: 2026-07-21.
Relates to:
ADR 0030 (`0030-domain-process-duality.md`, the **charter** this ADR implements — matter/motion/
director as co-first-class primitives, the authoring-symmetry test, and "the method became a process";
0029 is the *how* beneath 0030's *why*),
ADR 0031 (`0031-process-relational-mapper.md`, the **mapper** that turns this ADR's declared types into
three coherent projections — face/motion/rest — over Drizzle; §4's spine + registry are the material it
maps),
ADR 0022 (`0022-nano-rad-application.md`, **Urban** — §A's manifest references models by id; §E's
workers/llm reference the same symbols),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the datasource whose `schema()` is
already annotated *"powers both the DB Manager and form data-binding"* — the spine this ADR builds
the domain model on),
ADR 0025 (`0025-urban-trigger-runtime.md`, `action.start` names a process / `action.message` a
message — the references a maker must not hand-type),
ADR 0026 (`0026-urban-human-surfaces-and-run-model.md`, forms rendered for the inbox/chat surfaces),
ADR 0027 (`0027-urban-app-manifest-spec.md`, the manifest envelope + **§4 cross-reference rules** and
`spec-app/nano-app.schema.json` — this ADR turns those post-hoc rules into authoring-time
enumeration and adds the type layer the envelope binds against),
ADR 0028 (`0028-...auth...`, the `security.rules` roles that also reference symbols),
`engine-core` (process **variables are untyped JSON/MessagePack** by design — Zeebe's model; the
engine will not enforce types),
`console/src/components/FormEditor.tsx` (`@bpmn-io/form-js` — a form is `{ id, components[] }` where
each component carries a `type` and a dotted `key`),
`engine-core/tests/fixtures/adhoc-agent/fraud-detection/fraud-detection-process-enter-tax-form.form`
(a real form whose fields are `taxSubmission.fullName`, `taxSubmission.dob`,
`taxSubmission.emailAddress` — forms *already emit nested domain records*).

## Context

The manifest (ADR 0027) is full of hand-typed string identifiers that reference the models the
editors produce: `triggers[].action.start` (a process), `action.message` (a message name),
`surfaces.chat.agent` (an `llm` binding), a user task → its `.form`, `workers[].taskType`,
`llm[].output.decision` (a DMN decision), and ADR 0028's `security.rules` role targets. ADR 0027 §4
already enumerates these as **cross-reference rules** — but only as *validation after the fact*
("every `triggers[].action.start` names a deployed process"). A maker still types the id by hand and
learns of a typo as a red squiggle, or worse, as ADR 0025 §5's silent `CorrelateMessage` no-op at
runtime. This is the "spaghetti-wiring of hand-written ids" to eliminate.

Underneath the id problem is a deeper one. Process **variables are untyped JSON** — that is a
deliberate Zeebe design choice (flexibility + MessagePack performance), and Nano inherits it: the
engine will not, and should not, enforce a type system. Yet a maker wiring forms → variables → DMN →
workers → a SQLite table wants **Delphi-grade typed data-binding**, not stringly-typed keys that
happen to line up. Two problems, two answers.

Crucially, the raw material for both already exists in the models. A form is not an opaque blob: the
`enter-tax-form` fixture binds fields to `taxSubmission.fullName` (textfield), `taxSubmission.dob`
(datetime), `taxSubmission.emailAddress` (textfield) — i.e. it *already declares* a nested record
`taxSubmission { fullName: string; dob: date; emailAddress: string }`. And ADR 0024's `DataSource`
already exposes `schema(): Promise<TableMeta[]>` "to power form data-binding." The pieces are there;
they are just not reified or wired.

## Decision (proposed)

### 1. The project symbol index — the one enumeration source

Introduce a **project symbol index**: a TypeScript pass that parses a project's BPMN/DMN/form files
(via the moddle/form-js parsers the editors already use) and emits a typed symbol table:

- **processes** — `bpmn:process/@id` + `@name`, `isExecutable`; their **message-start subscriptions**,
  **user tasks** (+ referenced form), and service-task **`taskType`s**;
- **messages** — `bpmn:message/@name` (for `action.message`);
- **decisions** — `decision/@id` + `@name`, input/output `typeRef`s;
- **forms** — the form `id` **and its fields** (`key` + `type`, incl. dotted paths).

**One index, two consumers, no drift:**
- the console **App panels** bind pickers to it — a maker *chooses* a process/message/form/decision
  from a dropdown, never types an id;
- the ADR 0027 §4 validator **collapses into the index**: every cross-reference rule becomes
  "the referenced id ∈ the index" (deploy/compile/boot all query the same table).

The index is TypeScript, consistent with the TS-only manifest decision (ADR 0027 §3): forms/BPMN/DMN
parsing lives entirely in the TS ecosystem, and the open editors already hold the parsed models.

### 2. Typed references replace free-string ids (the binding)

The manifest still *stores* the id (a stable slug is the durable wire value), but **authoring never
hand-types it** — the Triggers/Data/Surfaces panels resolve every reference through the §1 index.
An unresolved reference (a model was deleted/renamed) is a first-class diagnostic with a
JSON-pointer, at the same three gates ADR 0027 §4 defines. This is the id half of "no spaghetti."

### 3. The domain model is compile-time types, erased at runtime

The type layer is an **authoring/compile/boot-time contract, erased at runtime** — TS-over-JS applied
to process data. The engine stays untyped JSON; Urban adds a *typed projection* over it that is
checked while authoring, at `deno compile`, and at App boot, then compiled away. Prior art is
Camunda's own **element templates**, which type task I/O at design time over untyped runtime
variables; Urban generalizes that from one task to the whole App. This keeps the engine pure and the
runtime fast while giving the maker Delphi-strength guarantees where they are actually enforceable.

### 4. The datasource schema is the spine; a manifest registry covers the rest

Types come from two sources, in priority order:

1. **The datasource schema (ADR 0024) — the spine.** `DataSource.schema()`'s `TableMeta` reifies a
   table into a named **record type** (columns → typed fields). Importing a table yields a domain
   type; this is the literal Delphi move — a `TField` bound to a DB column — and 0024 already
   annotated `schema()` as powering form data-binding, so this is the seam it anticipated.

   > **Reifier — implemented (spike).** `server/src/console/domain_types.ts` is the concrete §4.1
   > reifier: `emitDomainDts(tables)` turns `schema()`'s `TableMeta[]` into a `domain-rows.d.ts` — one
   > `export interface` per table plus a `DomainTables` lookup keyed by the raw table name. The
   > datasource CLI's `domaintypes` op (`data_cli.ts`) runs `schema()` → emit → write.
   > `sqliteAffinityToTs`
   > applies SQLite's type-affinity rules with two maker-facing overrides the DB Manager's type list
   > implies (`BOOLEAN → boolean`, date/time → ISO `string`); a nullable column widens with `| null`,
   > a `NOT NULL`/primary-key column does not. It is a pure emitter (type-only SDK import),
   > Node+Deno-portable (ADR 0036) and unit- +
   > roundtrip-tested (`domain_types_test.ts`, run under Deno). *This is the "generate a models
   > directory" answer — but generated, never hand-edited, so it cannot drift from the DB.*
   >
   > **File naming — distinct stem (required for `tsconfig.json` `paths`).** The reified type
   > files are named `domain-rows.d.ts` and `worker-io.d.ts`, *not* `domain.d.ts`/`workers.d.ts`.
   > TypeScript pairs a `foo.d.ts` as the *declaration of* a sibling `foo.ts`, so with the runtime
   > accessors `domain.ts`/`workers.ts` present, a project `tsconfig.json` `paths` map made the
   > accessor importing its own row types read as a circular self-import (TS2303/TS2459), breaking
   > type resolution under standard tooling (VS Code/`tsc`). Distinct stems avoid the pairing; the
   > scaffolder emits a `tsconfig.json` (paths mirroring the `deno.json` map) + `package.json`
   > (`@types/node`) so `@nanobpm/*` and `node:*` resolve natively without the console's Monaco
   > extra-libs (ADR 0038 Node-first tooling parity).
2. **A manifest type registry** — a small `types` block for **transient, non-persisted** shapes
   (an event body, a worker payload, a computed context) that no table backs.

   > **Revised by ADR 0040 (the Fused Domain Model).** This *authored* `types` block is superseded: the
   > registry becomes **derived** — a fusion of three authoring sources (models carry motion shapes,
   > the datasource carries rest, external contracts cover the rest), so it cannot drift from its
   > sources. Motion shapes move into the models (authored in the Modeller); any persisted `types`
   > becomes a fuse cache, not a source of truth. The nominal id-matching and the reifier (§6) below
   > are unchanged. See `0040-fused-domain-model.md`.

Everything then **references a type by name**: form fields bind to a type's fields; a process gets an
optional declared `variables` type; DMN `typeRef`s map to the same registry; worker payloads are
typed against it. Matching is **nominal** — a reference resolves against the type's stable id (the
`types` map key), not its shape — consistent with the id-based reference pickers (§1). Two records
with identical fields but different ids are different types; a `structural` matching mode is reserved
in the schema (`domainType.match`) as an escape hatch for later shape-based reuse, but is not yet
honoured (see resolved open question 3). The `enter-tax-form` fixture shows the on-ramp: the index *infers* a candidate
`taxSubmission` record from the form keys, and the maker either **promotes** it into the registry or
**binds** it to a `taxSubmission` table — one gesture connects form, variable, and datasource. Turning
a referenced type into its three coherent shapes — the form field (face), the process variable
(motion), and the table row (rest) — is the **Process-Relational Mapper**'s job (ADR 0031); this ADR
supplies the type material, 0031 maps it.

### 5. Variable-path autocomplete (the other half of "no spaghetti")

FEEL-expression fields — `action.variables`, `action.correlationKey` (`= body.room`), form default
values, DMN inputs — autocomplete **variable paths** from the resolved type in scope (the declared
process `variables` type, plus the trigger event's shape). A maker picks `body.room` from a list, not
by remembering it. Wrong-path references become diagnostics instead of runtime `null`s.

**The scope binding (implemented).** A trigger event's shape is supplied by an optional typed
reference `trigger.bodyType`, whose value is a `types` registry id (validated to resolve, like every
other reference). With it declared, the action's FEEL fields complete `body.<field>` from that domain
type, walking nested declared types (`body.sensor.id`); list fields are leaves (FEEL indexes them).
Absent a `bodyType`, only the `body` root is offered. This is the same "typed reference replaces a
free-string id" move as §2, applied to the FEEL scope.

**Wrong-path diagnostics (implemented).** The validator shares the completer's path walker (`feel.ts`),
so a `body.<path>` that doesn't exist in the trigger's `bodyType` is flagged (`unknown-path`) instead
of failing as a runtime `null`. It is conservative — it only flags a segment that is definitively
absent from a *declared* type, and stays silent through `json`/undeclared shapes and complex FEEL
(indexing, calls) it cannot verify, so it never raises a false error.

**Binding a type to a form / decision (implemented).** A trigger declares its scope inline
(`bodyType`), but a form and a decision are separate model artifacts referenced by glob, so their
scope is declared in a top-level `bindings[]` list: each entry binds one model — `{ form: <id> }` or
`{ decision: <id> }` — to a `type` (a declared domain type id). The bound model's id resolves against
the project symbol index (`unknown-form` / `unknown-decision`) and the type against the registry
(`unknown-type`); the manifest editor autocompletes all three. This is the same typed-reference move,
and it is the type-in-scope prerequisite for the in-editor step below.

**DMN input-expression variables (implemented).** The FEEL *inside* a decision's input expressions —
edited in dmn-js, which carries its own FEEL engine — is fed the bound type's fields as variable
suggestions. `feel.ts` exposes `scopeVarsForType` / `decisionScope` (an editor-agnostic scope tree that
recurses declared types, cycle-guarded); the console maps that tree into dmn-js's `variableResolver`
via a registered provider (per-view `additionalModules`), scoped to the decision shown in the active
view. The bound scope reuses the same resolved domain-type view as validation, so autocomplete and
diagnostics can't disagree. Form-js default-value FEEL is **deferred**: form-js self-supplies its own
component keys as FEEL variables (`getSchemaVariables`) with no external-injection seam and no
plain-defaultValue-as-FEEL concept, so domain-scope injection there is low-value and fragile.

### 6. Codegen — the domain types flow into TypeScript

The ADR 0027 generator (`generate-app-manifest.sh`) additionally emits **TypeScript types for the
domain records**, so worker handlers and the Deno App loader are typed against the same registry the
panels edit. Types are erased at `deno compile`; the shipped App is still untyped JSON on the wire.

> **Codegen + typed SDK — implemented (spike).** The reifier (§4.1) emits a **gitignored**
> `nano-generated/domain-rows.d.ts` under the maker's project — same materialize-and-ignore pattern as the
> generated `data-cli.ts`/`data-sdk.ts`, so there is never a committed, hand-editable `models/` dir
> to drift. Generation is wired to two real triggers: a `domaintypes` datasource op
> (`data_cli.ts`, exposed through the same `run_data_op` gateway) that emits + writes the file, fired
> automatically after any **schema change** through the Data panel (a `script` rebuild, a DDL
> `exec`, or a `migrate`); and an **App-boot hook** (`projects.rs` run path) that refreshes the
> types before the App starts. Both are best-effort and type-erased — a failure never blocks the
> maker's DDL or the App. The emitter itself is a pure module (`domain_types.ts`, type-only import)
> so it materialises next to `data-cli.ts` as `nano-generated/domain-types.ts`. The worker SDK
> (`worker_sdk.ts`) carries authoring-only generics that consume it:
> `defineWorker<In, Out>({ handle(job) })` types `job.variables` as `In` and `job.complete(out)` as
> `Out`, and `ctx.data().query<T>()` returns `T[]` — e.g.
> `query<DomainTables["customers"]>(...)`. Generics default to the prior untyped shapes, so existing
> single-/two-arg `defineWorker({...})` calls compile unchanged; the constraint is `extends object`
> (not `Record<string, unknown>`) precisely so a generated `interface` is assignable. The internals
> are type-agnostic (`enrich`/`dispatch` unchanged) — `defineWorker` casts to the base options — so
> types add zero runtime cost and are fully erased at `deno compile`.
>
> **Follow-ups (all now implemented).** A maker-facing "Regenerate domain types" affordance exists: the
> `POST /projects/{name}/data/{source}/domaintypes` route (operationId `regenerateDomainTypes`)
> and a "⟳ Types" button in the Data panel's Tables sidebar both drive the same op. The op
> **unions every declared datasource** into one `domain-rows.d.ts`: `emitDomainDtsForSources` emits
> source-prefixed interfaces (e.g. `AppCustomers`) under a `DomainSources` map keyed by alias then
> table, with `DomainTables` aliased to the default source so the single-source
> `DomainTables["customers"]` convention keeps working (byte-identical output for one source). The
> **manifest `types` registry (§4.2) is folded in** too: `emitDomainTypeRegistry` emits a `DomainTypes`
> map keyed by type id (`DomainTypes["taxSubmission"]`), mapping primitives + nominal references
> (`DomainTypes["taxLine"]`), with `optional` widening the key and `list` wrapping in `[]` —
> `emitDomainModel` composes the table spine + registry into the single file. Regeneration runs at
> **App boot, on schema change, on the explicit button, and at export/`deno compile`** (the compile
> and export handlers refresh the file before packaging, so a shipped App carries current types —
> best-effort and type-erased at compile).

### 6.1 The typed data-object layer — records, not SQL strings

The §6 codegen types *read* rows (`query<DomainTables["customers"]>(...)`), but a worker still hand-wrote
every `INSERT`/`UPDATE` as a SQL string — the domain model described the data without being the thing the
code *manipulated*. This step closes that gap with a **typed table gateway** — the RAD "TTable" / Delphi
data-module idea: the code binds to a record-oriented object, not a query.

Two pieces, split so codegen stays minimal and the Node fallback (ADR 0036) is never at risk:

- **Generic runtime — `Table<T>` in `data-sdk.ts`.** A single dual-runtime class exposing
  `insert` / `get` / `all` / `find` / `findOne` / `update` / `delete` / `count`, building parameterised SQL
  from a typed row object's own keys. It is schema-agnostic (`T` is a caller-supplied type), so it lives in
  the hand-written SDK, not codegen. `DataSource.table<T>(name, pk?)` opens one; `pk` defaults to `id`.
- **Generated bindings — `nano-generated/domain.ts`.** The reifier (`emitDomainBindings`) additionally emits a
  typed accessor next to `domain-rows.d.ts`, written by the **same `domaintypes` op** that writes the `.d.ts`:
  `openDomain()` → `Domain`, an object with one `Table<Row>` per table bound to its **real primary key**
  (`db.orders.insert({...})`, `db.orders.get(id)`), plus `db.raw` as the raw-SQL escape hatch. It imports
  only the sibling SDK (relative) and a **type-only** `domain-rows.d.ts`, so it carries no runtime dependency and
  erases at `deno compile`.

The **degrade-to-Node invariant (ADR 0036) is load-bearing here.** The worker runtime runs Node-first
(`node --experimental-strip-types --import node-register.mjs`), whose *strip-only* mode transforms nothing —
so **TypeScript parameter properties** (`constructor(private readonly x)`) are rejected even though Deno
accepts them. `Table` therefore uses plain field declarations + a JS-private `#src`, and the console test
harness (`run_data_op` over the Node fallback) is the guard that keeps it that way. A stub `domain.ts` (raw
accessor only) is seeded at scaffold so `@nanobpm/domain` resolves before the first reification, and is
re-seeded — never clobbering the reified per-table file — on SDK re-materialisation.

A worker is now SQL-free and typed end-to-end:

```ts
import { defineWorker } from "@nanobpm/worker";
import { openDomain } from "@nanobpm/domain";

defineWorker({
  type: "save-order",
  async handle(job) {
    const { customerId, item, qty } = job.variables as { customerId: number; item: string; qty: number };
    const db = await openDomain();
    const orderId = await db.orders.insert({ customer_id: customerId, item, qty, status: "received" });
    return { orderId: Number(orderId), qty };
  },
});
```

### 6.2 Typed workers by task type — the model informs the worker types

§6.1's worker still hand-casts `job.variables as { … }`: the domain model typed the *data at rest*, but
the *variable payload in motion* (a job's `variables`, the `complete` result) stayed stringly-cast. This
step closes the last gap so the **process model informs the worker type system**, keyed by the one join
that already ties template↔worker (§ADR 0033 §3): the **task type** (`zeebe:taskDefinition:type` ↔
`workers[].taskType`).

Two symmetric declarations on a worker name its motion shapes, both `$ref`-ing the `types` registry
(§4.2) and both fail-closed validated (`unknown-type`) in the schema package:

- `workers[].inputType` — the type of the job's incoming `variables` (the payload the engine hands the
  worker). New in this step; the symmetric partner of…
- `workers[].outputType` — the type of the variables the worker writes back (already present, §ADR 0033
  §3, where it also types output-mapped process variables for the next component's FEEL scope).

The reifier emits a **gitignored `nano-generated/worker-io.d.ts`** next to `domain-rows.d.ts` (same materialize-and-ignore
pattern), a `taskType → DomainTypes[…]` map for each direction (`WorkerInputs` / `WorkerOutputs`), plus a
**static `nano-generated/workers.ts`** SDK wrapper that re-exports the worker SDK and overrides `defineWorker` with
a task-type-driven overload:

```ts
// workers.ts (static SDK, type-erased)
export function defineWorker<K extends string>(
  opts: { type: K } & WorkerOptions<InFor<K> & object, OutFor<K> & object>,
): void;
// InFor<K>  = K extends keyof WorkerInputs  ? WorkerInputs[K]  : WorkerVars
// OutFor<K> = K extends keyof WorkerOutputs ? WorkerOutputs[K] : WorkerVars
```

TypeScript **infers `K` from the `type:` string literal**, so a worker is fully typed with **zero manual
generics** — and an undeclared task type falls back to the untyped `WorkerVars`, so nothing breaks:

```ts
import { defineWorker } from "@nanobpm/worker";        // → nano-generated/workers.ts
import { openDomain } from "@nanobpm/domain";

defineWorker({
  type: "save-order",                                   // K = "save-order"
  async handle(job) {
    const { customerId, item, qty } = job.variables;    // typed off WorkerInputs["save-order"] — no cast
    const db = await openDomain();
    const orderId = await db.orders.insert({ customer_id: customerId, item, qty, status: "received" });
    return { orderId: Number(orderId), qty };           // checked against WorkerOutputs["save-order"]
  },
});
```

The **degrade-to-Node invariant (ADR 0036)** holds: `workers.ts` uses only erasable type-level constructs
(conditional types, generics, `import type`, `export *`, a single `as unknown as` cast). The explicit local
`defineWorker` export shadows the star-export of the same name under both TS and Node ESM, so the typed
wrapper is a strict superset — every `@nanobpm/worker` alias repoints to `workers.ts` with no runtime change.
`workers.ts` is the static SDK (written on every `ensure_project_sdk`); `worker-io.d.ts` is reified by the
`domaintypes` op (seeded-if-absent, refreshed on schema change / boot / the explicit route), and imports
`DomainTypes` **only when ≥1 mapping references it** (the registry is omitted from `domain-rows.d.ts` when empty).

**The modeler closes the loop (the Delphi Object Inspector).** So the *model informs the types* without
hand-editing the manifest, the BPMN properties panel shows an **"Urban domain type"** group on every service
task carrying a literal `zeebe:taskDefinition:type`, with **Input / Output domain type** dropdowns populated
from the manifest `types` registry. Picking a type writes the matching `workers[].inputType/outputType` back
to `nano.app.json` (creating the `workers[]` entry if absent), which — after the next reification — flows
straight into `WorkerInputs`/`WorkerOutputs`. The edit is **out-of-band from the BPMN document** (the type
lives in the manifest, not the model XML), so it never touches the command stack (no false dirty); an
optimistic overlay + an `elements.changed` refire keeps the panel in sync before the manifest reload lands.
This is the RAD triangle: **declare the type in the modeler → the worker's `job.variables` is typed by task
type → the same registry scopes the model's FEEL.**

## Consequences

**Positive.**
- No hand-typed ids; every reference is a picker over the §1 index, and §4 validation is that same
  index — one source of truth for both authoring and validation.
- Delphi-grade continuity: **form field → process variable → DMN → worker → SQLite column** all name
  the *same* type, resolvable and autocompletable end to end.
- **Both motion and matter become first-class** (ADR 0030 §2): the domain type is the shared spine
  that makes the process (motion) the *type's dynamics* and the type (matter) the *process's phase
  space*. The **authoring-symmetry** test of ADR 0030 §3 lands here — the §1 index enables
  *process-first* authoring (the type accretes from what tasks touch, §5's on-ramp), and the type
  registry (§4) enables *domain-first* authoring (declare the type, bind the models to it); both
  directions resolve against one index.
- **Domain-shaped observability.** Because the domain is reified, a read model can be shaped by the
  *business noun* ("Orders awaiting payment") rather than the *engine noun* ("instances parked at
  `Task_3`") — the most visible payoff of making matter first-class (ADR 0030 §5).
- The engine is untouched — untyped, fast, Zeebe-faithful. Typing is a tooling layer that adds no
  runtime cost and cannot destabilize the hot path.
- The datasource-as-spine reuses ADR 0024's `schema()` exactly as intended; nothing new in the data
  layer.

**Costs / honest gaps.**
- **Types are not enforced at runtime.** Like TS over JS, a worker can still write off-type JSON; the
  guarantees are design/compile/boot-time only. A malicious or careless remote worker bypasses them.
- **No engine-level cross-process variable typing** — the contract is Urban's, not the engine's; two
  processes agreeing on a `taxSubmission` shape is a tooling convention.
- The index must **rebuild on model change** to stay honest (a stale index yields false diagnostics or
  false completions); the console must invalidate it on save.
- form-js `key`s are free strings, so record **inference is heuristic** — the maker confirms the
  promotion; Urban does not silently invent a schema.

## Open questions

1. **Table → type import: explicit or automatic?** Does every table auto-appear as a type, or must a
   maker import it (keeping the type surface curated)?
2. **Process `variables`: required or optional schema?** Optional preserves the untyped ergonomics;
   required maximizes safety. Likely optional, opt-in per process.
3. **Nominal vs structural** typing in the registry — ~~does `taxSubmission` match by name or by
   shape~~ **Resolved (2026-07): nominal**, with a reserved structural escape hatch. A type is
   identified by its stable `types` map key; references resolve by id, matching the id-based pickers
   (§1). The schema carries a `domainType.match` enum (`nominal` default, `structural` reserved) so
   shape-based reuse can be added later without a breaking change; the validator and the future PRM
   honour only `nominal` for now. Implemented in the type registry (spec-app `types` block + validator,
   PR #173). Shared resolution with ADR 0031 open question 6.
4. **Relationship to FEEL's own type system** (context/list types) — does the registry generate FEEL
   type hints, or only TS?
5. **Where the index lives** — a shared TS package imported by both the console and the Deno App's
   boot validator, or console-only with the App re-deriving at boot?
6. **DMN `typeRef` mapping** — how the registry's records map onto DMN's primitive/`typeRef` model.

## Phased plan

1. **symbol-index** — the §1 index over BPMN/DMN/form + typed reference **pickers** in the panels;
   fold the ADR 0027 §4 cross-reference validator into "id ∈ index." (Kills hand-typed ids; no type
   system yet.)
2. **domain-types** — datasource-`schema()` → type import (ADR 0024) + the manifest `types` registry
   + **variable-path autocomplete** (§5); forms/variables/DMN reference types by name.
3. **type-codegen** — emit the domain-record TypeScript from `generate-app-manifest.sh` (§6) and
   scaffold **typed worker signatures** against it.
4. **data-objects** — the typed table gateway (§6.1): `Table<T>` in `data-sdk.ts` + a generated
   `nano-generated/domain.ts` (`openDomain()` → `db.<table>.insert/get/find/update/delete`), so workers
   manipulate typed records instead of hand-writing SQL.
