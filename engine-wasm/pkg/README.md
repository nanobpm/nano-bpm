# @nanobpm/engine-wasm

The nanobpmn engine (`engine-core`) compiled to **WebAssembly** for in-browser
BPMN execution. Deploy a diagram, start instances, activate/complete jobs, drive
the virtual clock, and read live snapshots/traces — all client-side, with no
gateway. It is the substrate for the [Bojtos](https://github.com/nanobpm/bojtos)
demo framework (ADR 0043) and the console test-run panel.

The package ships **two independent engines** from one install, selectable by
**import path**:

| Import | Engine | Wire size (brotli) | Use it for |
| --- | --- | ---: | --- |
| `@nanobpm/engine-wasm` | **lean** — primary state only | ~252 KB | demos, the modeler, debuggers, anything that reads via `snapshot()` / `events()` |
| `@nanobpm/engine-wasm/readmodel` | **read-model** — lean surface **+** the gateway's C8-style REST read methods | ~561 KB | full-surface app testing in CI that wants gateway read parity |
| `@nanobpm/engine-wasm/readmodel-types` | **types only** — the DTOs the read-model `search*` / `get*ByKey` methods return | 0 KB (erased) | typing read-model query results in a consumer (e.g. `@nanobpm/bojtos-kit`) without hand-mirroring the shapes |

## Why two binaries (not a runtime toggle)

Each variant is its own `wasm-pack --target web` JS glue + `_bg.wasm`. A bundler
follows the static module graph, so it only emits the wasm for the subpath you
actually import — wasm **cannot** be tree-shaken out of a single fat build. So:

- Lean-only pages pay **zero** read-model weight (the +309 KB brotli never lands).
- You can `await import("@nanobpm/engine-wasm/readmodel")` to code-split the
  heavy engine behind a runtime decision.
- You can even instantiate **both** on one page.

The read-model variant is the lean engine **plus** an in-memory wasm SQLite read
model (the same projection the gateway serves from), exposing the REST read
surface the lean engine lacks.

## Install

```sh
npm install @nanobpm/engine-wasm
```

## Usage — lean (default)

```js
import init, { TestEngine } from "@nanobpm/engine-wasm";

await init();                       // instantiate the wasm module
const engine = new TestEngine();

engine.deploy(bpmnXml);             // deploy a BPMN diagram (or a DMN decision)
const handle = engine.createInstance("my-process", "{}");
const snap = JSON.parse(engine.snapshot());   // live primary state
const trace = JSON.parse(engine.events());    // the event log
engine.reset();                     // back to a clean engine
```

The lean surface covers primary state and execution: `deploy`, `deployDecision`,
`evaluateDecision`, `createInstance`,
`snapshot`, `events`, `replayEvents`, job ops (`activateJobs`, `completeJob`, `failJob`,
`updateRetries`, `updateTimeout`, `throwError`), user-task ops (`completeUserTask`,
`assignUserTask`, …), messaging/signals, the virtual clock (`advanceTime`,
`tickNow`), and `reset`. It links **no** SQLite / read-model code.

`deploy` accepts **either** a BPMN process **or** a DMN decision resource: a
document with no BPMN `<process>` is routed to decision deployment (equivalent to
calling `deployDecision`), so a `businessRuleTask` carrying a
`zeebe:calledDecision` can resolve its decision. `evaluateDecision(decisionId,
variablesJson)` evaluates a deployed decision standalone.

### Optional job leasing and agent history

`activateJobs(type, maxJobs, timeoutMs, worker, withLease?)` defaults to nonleasing
for **every** job kind and agent marker. Activated jobs always contain
`jobLeaseToken`: `null` without a lease, otherwise an opaque string. Never parse it
as a number. Once a job is leased, subsequent activations must also opt in;
nonleasing workers skip it after failures and timeouts.

Pass the token as the final optional argument to `completeJob`, `completeAgentJob`,
`failJob`, or `throwError`. Leased lifecycle commands require the current token.
`updateRetries` and `updateTimeout` also accept a final token; it is optional, but
a supplied stale value is rejected. Timeout updates accept signed millisecond
durations; zero and negative updates make the activation immediately eligible for
expiry on the next clock tick without discarding its current lease.

Agent APIs follow the vendored stable/8.10 contract. Requests take `jobLeaseToken`
(the Camunda 8.10 name); the pre-8.10 name `jobLease` is still accepted as a
deprecated alias (#1283):

- `createAgentInstance(JSON.stringify({elementInstanceKey, jobKey, jobLeaseToken, history}))`
  requires nonempty history establishing CONFIGURATION. `jobLeaseToken` is the activated
  job's `jobLeaseToken`, unchanged. Definition, limits, tools, and metrics are history
  fields, not top-level request properties.
- `updateAgentInstance(agentInstanceKey, JSON.stringify({elementInstanceKey, jobKey,
  jobLeaseToken, status?, history?}))` requires all three attribution fields even without
  history.
- Both return canonical results containing positionally correlated `createdHistory`;
  CREATE also returns `agentInstanceKey`. Duplicate CREATE rejects rather than upserting.
- Failure and timeout leave history PENDING. Job completion commits the winning
  attempt and discards superseded attempts; cancellation or a business error
  discards pending history. Searches default to COMMITTED. Timestamps are RFC-3339
  strings and loop iterations start at 1.
- System prompts are typed content-block arrays. Persisted historical text remains
  one TEXT block, even when the text resembles JSON.
- Metrics preserve the difference between an omitted object and an object with
  nullable counters. Zero and signed values remain observations, never sentinels.

Agent searches currently support exact scalar filters and `$eq`, plus `$in` for
history commit status. Other advanced operators are rejected; pagination and sorting
are not implemented by the simulation API.

`replayEvents(engine.events())` restores a complete exported trace through core
replay, including opaque lease state. It rejects unknown event types and missing
prefixes rather than silently dropping records.

## Usage — read-model

```js
import init, { TestEngine } from "@nanobpm/engine-wasm/readmodel";

await init();
const engine = new TestEngine();

engine.deploy(bpmnXml);
engine.createInstance("my-process", "{}");

// Same lean surface as above, PLUS the gateway's REST read channel:
const form  = JSON.parse(engine.getFormByKey("2251799813685250"));
const open  = JSON.parse(engine.searchUserTasks(JSON.stringify({ state: "CREATED" })));
const insts = JSON.parse(engine.searchProcessInstances("{}"));
const res   = JSON.parse(engine.getResourceByKey("2251799813685251"));
const vars  = JSON.parse(engine.searchVariables("{}"));
```

### Read methods (read-model subpath only)

Each returns the same JSON shapes the Camunda v2 REST surface returns; each
delegates to the in-memory read model, kept current after every command and
cleared by `reset()`.

- **`getFormByKey(formKey)`** → the latest deployed form for `formKey`
  (`{ tenantId, formId, schema, version, formKey }`), or `null`.
  Mirrors `GET /forms/{formKey}`.
- **`searchUserTasks(filterJson)`** → `{ items, page }`. Honours an optional
  `{ state? }` filter (e.g. `"CREATED"`) through the read model.
  Mirrors `POST /user-tasks/search`.
- **`searchProcessInstances(filterJson)`** → `{ items, page }`. Body is
  shape-validated; filter/sort/page fields are not yet honoured (returns every
  instance). Mirrors `POST /process-instances/search`.
- **`getResourceByKey(resourceKey)`** → the generic resource, or `null`.
  Mirrors `GET /resources/{resourceKey}`.
- **`searchVariables(filterJson)`** → `{ items, page }`. Body is shape-validated;
  long values are truncated with `isTruncated: true`.
  Mirrors `POST /variables/search`.

### Typing the results — `@nanobpm/engine-wasm/readmodel-types`

The methods above hand off opaque JSON strings (the wasm boundary is strings), so
the subpath `@nanobpm/engine-wasm/readmodel-types` ships the **DTO types** for
those results — `UserTaskSearchQueryResult`, `ProcessInstanceSearchQueryResult`,
`VariableSearchQueryResult`, `FormResult`, `ResourceResult` — so a consumer can
type them without hand-mirroring the shapes:

```ts
import type { UserTaskSearchQueryResult } from "@nanobpm/engine-wasm/readmodel-types";

const open = JSON.parse(
  engine.searchUserTasks(JSON.stringify({ state: "CREATED" })),
) as UserTaskSearchQueryResult;
```

These are **derived** from the single source of truth — the Camunda-parity REST
OpenAPI in `spec/` — by `engine-wasm/readmodel-types` (`@hey-api/openapi-ts`); a
CI drift guard regenerates and fails on any stale artifact. The subpath is
types-only (its runtime module is empty), so importing it adds zero wire weight.

## Choosing a subpath

- **Reach for lean** for demos, the modeler, debuggers/adapters, and anything
  that only inspects `snapshot()` / `events()`. Keeping these paths lean is the
  whole point of the split — the +309 KB brotli must never ship in a demo page.
- **Reach for `/readmodel`** when you need gateway read parity — full-surface app
  testing in CI — instead of hand-rolling shadow read stores in JS/TS.

## References

- Epic [#796](https://github.com/Magikcraft/nano-bpm/issues/796) — the read
  channel via a wasm SQLite read model (size table, toolchain note, downstream
  consumers).
- ADR 0043 — Bojtos (the demo framework this engine backs).
