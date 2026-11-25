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

Agent searches (read-model subpath) accept the spec's full
[`AgentInstanceSearchQuery`](../spec/agent-instances.yaml) /
`AgentInstanceHistorySearchQuery` — every filter operator, sort, and limit/cursor
paging — through the same search pipeline the gateway serves; see the generated
[API reference](#api-reference).

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
const open  = JSON.parse(engine.searchUserTasks(JSON.stringify({ filter: { state: "CREATED" } })));
const insts = JSON.parse(engine.searchProcessInstances("{}"));
const res   = JSON.parse(engine.getResourceByKey("2251799813685251"));
const vars  = JSON.parse(engine.searchVariables("{}"));
```

### Read methods (read-model subpath only)

Every read method runs the SAME spec-derived pipeline the gateway serves (the
shared `nanobpmn-search` crate): the request body is the spec's `*SearchQuery`
(`{ filter?, sort?, page? }` — every filter field and operator, `$or`,
variable filters, sort fields and limit/offset/cursor paging; an empty string
means "no query"), parsed strictly — a property or enum value the spec does not
define throws, it is never silently ignored — and the result is the generated
`*SearchQueryResult` / `*Result`, byte-for-byte what the gateway returns. A
server/WASM parity guard (`src/search_parity_tests.rs`) enforces this for every
operation. Each reads the in-memory read model, kept current after every
command and cleared by `reset()`.

- **`searchUserTasks(queryJson)`** → `UserTaskSearchQueryResult`.
  Mirrors `POST /user-tasks/search`.
- **`searchProcessInstances(queryJson)`** → `ProcessInstanceSearchQueryResult`.
  Mirrors `POST /process-instances/search`.
- **`searchVariables(queryJson, truncateValues?)`** → `VariableSearchQueryResult`.
  `truncateValues` (default `true`) is the endpoint's query parameter: long
  values are cut to the preview length and flagged `isTruncated: true`.
  Mirrors `POST /variables/search`.
- **`searchAgentInstances(queryJson)`** / **`searchAgentInstanceHistory(key,
  queryJson)`** → the agent search results. Mirror `POST /agent-instances/search`
  and `POST /agent-instances/{key}/history/search`.
- **`getFormByKey(formKey)`** → `FormResult`, or `null` where the gateway
  answers 404. Mirrors `GET /forms/{formKey}`.
- **`getResourceByKey(resourceKey)`** → `ResourceResult`, or `null` where the
  gateway answers 404. Mirrors `GET /resources/{resourceKey}`.
- **`getAgentInstance(agentInstanceKey)`** → `AgentInstanceResult`, `null` where
  the gateway answers 404, and throws where it answers 400 (a malformed key).
  Mirrors `GET /agent-instances/{agentInstanceKey}`.

### Typing the results — `@nanobpm/engine-wasm/readmodel-types`

The methods above hand off opaque JSON strings (the wasm boundary is strings), so
the subpath `@nanobpm/engine-wasm/readmodel-types` ships the **DTO types** for
those results — `UserTaskSearchQueryResult`, `ProcessInstanceSearchQueryResult`,
`VariableSearchQueryResult`, `FormResult`, `ResourceResult` — so a consumer can
type them without hand-mirroring the shapes:

```ts
import type { UserTaskSearchQueryResult } from "@nanobpm/engine-wasm/readmodel-types";

const open = JSON.parse(
  engine.searchUserTasks(JSON.stringify({ filter: { state: "CREATED" } })),
) as UserTaskSearchQueryResult;
```

These are **derived** from the single source of truth — the Camunda-parity REST
OpenAPI in `spec/` — by `engine-wasm/readmodel-types` (`@hey-api/openapi-ts`); a
CI drift guard regenerates and fails on any stale artifact. The subpath is
types-only (its runtime module is empty), so importing it adds zero wire weight.

## API reference

<!-- BEGIN GENERATED API (engine-wasm/scripts/gen-readme-api.mjs) -->

_Generated by `node engine-wasm/scripts/gen-readme-api.mjs` from the `#[wasm_bindgen]` exports in
`engine-wasm/src/lib.rs` and the REST spec in [`spec/`](../spec). Do not edit by hand; CI fails
when this section is stale._

Every argument and result that carries structured data is a JSON **string**. The **REST operation**
column names the spec operation the method mirrors, and the REST request/response columns name
that operation's schemas. They are **REST counterparts for orientation, not the WASM payload
contract**: some methods take or return a different shape (for example `deploy` returns
`{ processIds, snapshot }`, and `evaluateDecision` takes a plain variables object). The method's
doc comment in [`src/lib.rs`](src/lib.rs) is authoritative for its WASM JSON. `—` marks an
embedded-only method.

### Lean surface (both subpaths)

| Method | Returns | REST operation | REST request schema | REST response schema | Description |
| --- | --- | --- | --- | --- | --- |
| `new TestEngine()` | TestEngine | — | — | — | Create a fresh, empty simulated engine. |
| `now` | property: number | — | — | — | The current virtual clock (milliseconds). |
| `reset()` | void | — | — | — | Discard all engine state (deployed definitions, instances, jobs, timers, the event log and the virtual clock), returning the engine to the same pristine state as a freshly constructed one. |
| `deploy(xml: string)` | string (throws) | [`createDeployment`](../spec/deployments.yaml) (`POST /deployments`) | inline `multipart/form-data` schema | `DeploymentResult` | Parse and deploy a BPMN **or** DMN resource — the one entry point accepts both (issue #1158). |
| `deployDecision(xml: string)` | string (throws) | [`createDeployment`](../spec/deployments.yaml) (`POST /deployments`) | inline `multipart/form-data` schema | `DeploymentResult` | Parse and deploy a DMN decision-requirements resource explicitly. |
| `evaluateDecision(decisionId: string, variablesJson: string)` | string (throws) | [`evaluateDecision`](../spec/decision-definitions.yaml) (`POST /decision-definitions/evaluation`) | `DecisionEvaluationInstruction` | `EvaluateDecisionResult` | Evaluate a deployed decision by id against the given variables — the standalone counterpart to a business rule task's in-line evaluation (the engine's `EvaluateDecision` command, exactly as the gateway runs it). |
| `deployForm(schema: string)` | string (throws) | [`createDeployment`](../spec/deployments.yaml) (`POST /deployments`) | inline `multipart/form-data` schema | `DeploymentResult` | Deploy a single `form-js` `.form` resource (the verbatim form-js JSON document). |
| `deployResource(resourceName: string, content: string)` | string (throws) | [`createDeployment`](../spec/deployments.yaml) (`POST /deployments`) | inline `multipart/form-data` schema | `DeploymentResult` | Deploy a single generic resource (any deployed file that is not a BPMN/DMN/form — e.g. a Markdown agent prompt) under `resource_name` with the given verbatim `content`. |
| `createInstance(processId: string, variablesJson: string, version: number?)` | string (throws) | [`createProcessInstance`](../spec/process-instances.yaml) (`POST /process-instances`) | `ProcessInstanceCreationInstruction` | `CreateProcessInstanceResult` | Start a new instance of `process_id`, seeding it with the given variables (a JSON object string; pass `"{}"` or `""` for none). |
| `debugCreateInstance(processId: string, variablesJson: string, breakpointsJson: string)` | string (throws) | — | — | — | Begin a **debug** run of a `CreateInstance` command: deploy first (as usual), then call this to start the instance under the stepping executor, pausing at the first breakpoint (or running to completion if none match). |
| `debugResume()` | string (throws) | — | — | — | Resume a paused debug run until the next breakpoint or completion. |
| `debugStep()` | string (throws) | — | — | — | Advance a paused debug run by exactly one step, then pause again (unless that step drained the command). |
| `debugIsPaused` | property: boolean | — | — | — | Whether a debug run is currently paused at a breakpoint. |
| `debugClear()` | void | — | — | — | Stop debugging, keeping the state the run produced. |
| `completeJob(jobKey: string, variablesJson: string, leaseToken: string?)` | string (throws) | [`completeJob`](../spec/jobs.yaml) (`POST /jobs/{jobKey}/completion`) | `JobCompletionRequest` | — | Complete a waiting job by key, merging `variables_json` (a JSON object string) into the instance. |
| `completeAgentJob(jobKey: string, variablesJson: string, agentResultJson: string, leaseToken: string?)` | string (throws) | [`completeJob`](../spec/jobs.yaml) (`POST /jobs/{jobKey}/completion`) | `JobCompletionRequest` | — | Complete an ad-hoc sub-process **agent** job — the container's JOB_WORKER job (Camunda's agentic `aiagent-job-worker`) — carrying the agent's activate-element instructions so the engine runs the selected inner "tools" this turn (ADR 0023 seam 2/3; Camunda `JobResult` / `activateElements[]`). |
| `failJob(jobKey: string, retries: number, message: string, leaseToken: string?)` | string (throws) | [`failJob`](../spec/jobs.yaml) (`POST /jobs/{jobKey}/failure`) | `JobFailRequest` | — | Fail a waiting job by key with the given remaining `retries` and message. |
| `correlateMessage(messageName: string, correlationKey: string, variablesJson: string)` | string (throws) | [`correlateMessage`](../spec/messages.yaml) (`POST /messages/correlation`) | `MessageCorrelationRequest` | `MessageCorrelationResult` | Correlate a message to any instance waiting on it: publishes `message_name` with the given `correlation_key` (the value the waiting subscription's `correlationKey` expression resolved to) and merges `variables_json` (a JSON object string) into each correlated instance. |
| `advanceTime(byMs: number)` | string (throws) | — | — | — | Advance the virtual clock by `by_ms` milliseconds, firing any timers that become due and expiring any lapsed job locks. |
| `tickNow(nowMs: number)` | string (throws) | — | — | — | Set the engine clock to a wall-clock instant (ms), then trigger due timers and expire lapsed job locks. |
| `activateJobs(jobType: string, maxJobs: number, timeoutMs: number, worker: string, withLease: boolean?)` | string (throws) | [`activateJobs`](../spec/jobs.yaml) (`POST /jobs/activation`) | `JobActivationRequest` | `JobActivationResult` | Activate up to `max_jobs` `Created` jobs of `job_type`, locking them to `worker` until `now + timeout_ms`. |
| `throwError(jobKey: string, errorCode: string, errorMessage: string, leaseToken: string?)` | string (throws) | [`throwJobError`](../spec/jobs.yaml) (`POST /jobs/{jobKey}/error`) | `JobErrorRequest` | — | Throw a BPMN business error from a waiting job by key. |
| `updateRetries(jobKey: string, retries: number, leaseToken: string?)` | string (throws) | [`updateJob`](../spec/jobs.yaml) (`PATCH /jobs/{jobKey}`) | `JobUpdateRequest` | — | Set a job's remaining retries by key. |
| `updateTimeout(jobKey: string, timeoutMs: number, leaseToken: string?)` | string (throws) | [`updateJob`](../spec/jobs.yaml) (`PATCH /jobs/{jobKey}`) | `JobUpdateRequest` | — | Update an activated job's timeout, optionally checking its opaque lease. |
| `resolveIncident(incidentKey: string)` | string (throws) | [`resolveIncident`](../spec/incidents.yaml) (`POST /incidents/{incidentKey}/resolution`) | `IncidentResolutionRequest` | — | Resolve an open incident by key, retrying the work that failed (a job incident returns the parked job — which must have retries left — to the activatable pool; a gateway incident re-evaluates; an uncaught-error incident re-creates the service-task job). |
| `setVariables(scopeKey: string, variablesJson: string, local: boolean)` | string (throws) | [`createElementInstanceVariables`](../spec/element-instances.yaml) (`PUT /element-instances/{elementInstanceKey}/variables`) | `SetVariableRequest` | — | Merge variables into a scope (a process-instance key or an element-instance key). |
| `broadcastSignal(signalName: string, variablesJson: string)` | string (throws) | [`broadcastSignal`](../spec/signals.yaml) (`POST /signals/broadcast`) | `SignalBroadcastRequest` | `SignalBroadcastResult` | Broadcast a signal by name to **every** open subscription that matches, across all instances, merging `variables_json` into each correlated instance. |
| `cancelInstance(instanceKey: string)` | string (throws) | [`cancelProcessInstance`](../spec/process-instances.yaml) (`POST /process-instances/{processInstanceKey}/cancellation`) | `CancelProcessInstanceRequest` | — | Cancel (terminate) a running process instance by key. |
| `modify(instanceKey: string, activateInstructionsJson: string, terminateInstructionsJson: string)` | string (throws) | [`modifyProcessInstance`](../spec/process-instances.yaml) (`POST /process-instances/{processInstanceKey}/modification`) | `ProcessInstanceModificationInstruction` | — | Modify a running process instance (Zeebe "modify process instance"): move tokens by terminating existing element instances and/or activating new ones. |
| `migrate(instanceKey: string, targetProcessDefinitionKey: string, mappingInstructionsJson: string)` | string (throws) | [`migrateProcessInstance`](../spec/process-instances.yaml) (`POST /process-instances/{processInstanceKey}/migration`) | `ProcessInstanceMigrationInstruction` | — | Migrate a running process instance to a target process definition (Zeebe "migrate process instance"): re-point every active element instance at the mapped element of `target_process_definition_key` and re-home the instance. |
| `completeUserTask(userTaskKey: string, variablesJson: string)` | string (throws) | [`completeUserTask`](../spec/user-tasks.yaml) (`POST /user-tasks/{userTaskKey}/completion`) | `UserTaskCompletionRequest` | — | Complete a waiting user task by key, merging `variables_json` into the instance before the parked token resumes. |
| `assignUserTask(userTaskKey: string, assignee: string, allowOverride: boolean)` | string (throws) | [`assignUserTask`](../spec/user-tasks.yaml) (`POST /user-tasks/{userTaskKey}/assignment`) | `UserTaskAssignmentRequest` | — | Assign a user task to `assignee`. |
| `unassignUserTask(userTaskKey: string)` | string (throws) | [`unassignUserTask`](../spec/user-tasks.yaml) (`DELETE /user-tasks/{userTaskKey}/assignee`) | — | — | Clear a user task's assignee. |
| `updateUserTask(userTaskKey: string, changesetJson: string)` | string (throws) | [`updateUserTask`](../spec/user-tasks.yaml) (`PATCH /user-tasks/{userTaskKey}`) | `UserTaskUpdateRequest` | — | Update a user task's attributes from a JSON changeset object. |
| `snapshot()` | string (throws) | — | — | — | The current simulation state as a JSON [`Snapshot`]. |
| `events()` | string (throws) | — | — | — | The full ordered event log emitted so far, as a JSON array of `{ seq, now, type, ...payload }`. |
| `replayEvents(eventsJson: string)` | string (throws) | — | — | — | Restore a complete trace returned by `events()`, using the core journal decoder and replay engine. |
| `createAgentInstance(requestJson: string)` | string (throws) | [`createAgentInstance`](../spec/agent-instances.yaml) (`POST /agent-instances`) | `AgentInstanceCreationRequest` | `AgentInstanceCreationResult` | Create an agent using `{ elementInstanceKey, jobKey, jobLeaseToken, history }`. |
| `updateAgentInstance(agentInstanceKey: string, requestJson: string)` | string (throws) | [`updateAgentInstance`](../spec/agent-instances.yaml) (`PATCH /agent-instances/{agentInstanceKey}`) | `AgentInstanceUpdateRequest` | `AgentInstanceUpdateResult` | Update an agent with `{ elementInstanceKey, jobKey, jobLeaseToken, status?, history? }`. |
| `completeAgentInstance(agentInstanceKey: string)` | string (throws) | — | — | — | Legacy embedded-only completion extension. |

### Read-model surface (`@nanobpm/engine-wasm/readmodel` only)

| Method | Returns | REST operation | REST request schema | REST response schema | Description |
| --- | --- | --- | --- | --- | --- |
| `getFormByKey(formKey: string)` | string (throws) | [`getFormByKey`](../spec/forms.yaml) (`GET /forms/{formKey}`) | — | `FormResult` | `getFormByKey` — mirrors `GET /forms/{formKey}`. |
| `getResourceByKey(resourceKey: string)` | string (throws) | [`getResource`](../spec/deployments.yaml) (`GET /resources/{resourceKey}`) | — | `ResourceResult` | `getResourceByKey` — mirrors `GET /resources/{resourceKey}`. |
| `getAgentInstance(agentInstanceKey: string)` | string (throws) | [`getAgentInstance`](../spec/agent-instances.yaml) (`GET /agent-instances/{agentInstanceKey}`) | — | `AgentInstanceResult` | `getAgentInstance` — mirrors `GET /agent-instances/{agentInstanceKey}`. |
| `searchUserTasks(queryJson: string)` | string (throws) | [`searchUserTasks`](../spec/user-tasks.yaml) (`POST /user-tasks/search`) | `UserTaskSearchQuery` | `UserTaskSearchQueryResult` | `searchUserTasks` — mirrors `POST /user-tasks/search`. |
| `searchProcessInstances(queryJson: string)` | string (throws) | [`searchProcessInstances`](../spec/process-instances.yaml) (`POST /process-instances/search`) | `ProcessInstanceSearchQuery` | `ProcessInstanceSearchQueryResult` | `searchProcessInstances` — mirrors `POST /process-instances/search`: the spec's `ProcessInstanceSearchQuery` through the shared pipeline, returning the generated `ProcessInstanceSearchQueryResult`. |
| `searchVariables(queryJson: string, truncateValues: boolean?)` | string (throws) | [`searchVariables`](../spec/variables.yaml) (`POST /variables/search`) | `VariableSearchQuery` | `VariableSearchQueryResult` | `searchVariables` — mirrors `POST /variables/search`: the spec's `VariableSearchQuery` through the shared pipeline, returning the generated `VariableSearchQueryResult`. |
| `searchAgentInstances(queryJson: string)` | string (throws) | [`searchAgentInstances`](../spec/agent-instances.yaml) (`POST /agent-instances/search`) | `AgentInstanceSearchQuery` | `AgentInstanceSearchQueryResult` | `searchAgentInstances` — mirrors `POST /agent-instances/search`. |
| `searchAgentInstanceHistory(agentInstanceKey: string, queryJson: string)` | string (throws) | [`searchAgentInstanceHistory`](../spec/agent-instances.yaml) (`POST /agent-instances/{agentInstanceKey}/history/search`) | `AgentInstanceHistorySearchQuery` | `AgentInstanceHistorySearchQueryResult` | `searchAgentInstanceHistory` — mirrors `POST /agent-instances/{agentInstanceKey}/history/search`. |

<!-- END GENERATED API -->

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
