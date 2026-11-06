# ADR 0037 — Execution listeners (and task listeners): BPMN lifecycle-hook parity

Status: **Proposed.**
Date: 2026-07-23.

Relates to: ADR 0023 (`0023-adhoc-subprocess-execution-parity.md` — the precedent
for a bounded, honestly-scoped `engine-core` execution-parity feature that threads
new job semantics through both completion transports), ADR 0016
(`0016-falcon-protocol.md` — the complete-job transport that must carry the listener
job kind/event-type), ADR 0022 (`0022-nano-rad-application.md` §E — the
switch-over-parity discipline). Engine hot path:
`engine-core/src/bpmn.rs` (extension-element parse), `engine-core/src/model.rs`
(`ElementKind`, `Element`), `engine-core/src/{state,command,event}.rs`,
`engine-core/src/engine/mod.rs` (the `ElementActivating→ElementActivated` and
`ElementCompleting→ElementCompleted` transitions), `server/src/main.rs`
(`JobKindEnum`/`JobListenerEventTypeEnum` on the jobs read model, complete-job REST),
`server/src/falcon.rs`. Camunda reference (`~/workspace/camunda/zeebe`):
`engine/.../deployment/model/element/ExecutionListener.java`,
`engine/.../deployment/model/transformer/zeebe/ExecutionListenerTransformer.java`,
`engine/.../bpmn/behavior/BpmnJobBehavior.java` (`createNewExecutionListenerJob`,
`fromExecutionListenerEventType` → `START`/`END`),
`qa/.../command/ExecutionListenerJobTest.java` (behavioural contract).

## Context

A BPMN **execution listener** (`zeebe:executionListeners` → `zeebe:executionListener
eventType="start|end" type="…" retries="…"`) is a **job** that fires at a flow node's
lifecycle boundary: `start` listeners run while the element is **activating** (before
its own behaviour — before a service task creates its worker job, before a gateway
routes), `end` listeners run while it is **completing** (before the token leaves). A
**task listener** (`zeebe:taskListeners`, `eventType` incl. `completing`) is the
user-task-only cousin that can additionally **deny** the transition and return
corrections.

### The Camunda contract we must satisfy (verified against source)

- Each listener is a job with `jobWorkerProperties` (a `type`, optional `retries`) and
  an `eventType`. `fromExecutionListenerEventType` maps `start → START`, `end → END`
  (`BpmnJobBehavior.java:494`); the job is stamped `JobListenerEventType` +
  `JobKind = EXECUTION_LISTENER` so a worker subscribed to that type/event receives it.
- Listeners of a given event type run **sequentially in declared order**, one job at a
  time: the engine creates listener *i*, waits for its completion, then creates
  *i+1*. Only after the **last `start`** listener completes does the element run its
  own behaviour; only after the **last `end`** listener completes does the element
  reach `COMPLETED` and the token advance.
- **Variables** a listener returns on completion merge into the element scope and are
  visible to subsequent listeners and to the element.
- Execution listeners **cannot deny** (that is a task-listener-only capability). A
  listener job that **fails / exhausts retries raises an incident**, exactly like any
  job; the transition stays parked until resolved.
- Listeners are valid on **most flow nodes** (tasks, gateways, events, sub-processes,
  the process itself, call/ad-hoc containers, multi-instance bodies).

### Nano before this ADR (the gap that motivated it)

> **Historical.** This section records the pre-implementation state that
> motivated this ADR — it is **not** current behaviour. As of #1197 both
> `zeebe:executionListeners` and `zeebe:taskListeners` are parsed and run (see
> the "Subset (honestly stated…)" section below for the implemented surface);
> `grep -i listener engine-core/src` now returns many matches. It is retained as
> the "before" picture, not a live limitation.

- **Parse:** `engine-core/src/bpmn.rs` handled a large set of `zeebe:` extension
  elements (`taskDefinition`, `calledDecision`, `calledElement`, `subscription`,
  `assignmentDefinition`, `taskSchedule`, `priorityDefinition`, `adHoc`, `ioMapping`,
  `loopCharacteristics`, `script`) but **not** `executionListeners`/`taskListeners`.
  A model that declared them **deployed and ran**, with the listeners **silently
  never firing** — no jobs were created, so a subscribed worker was never activated.
  `grep -i listener engine-core/src` → zero matches.
- **Lifecycle:** the engine already emits the four lifecycle events
  (`ElementActivating`, `ElementActivated`, `ElementCompleting`, `ElementCompleted`
  — `event.rs:147-176`), **but atomically**: `activate_element` pushes
  `ElementActivating` and `ElementActivated` back-to-back in one synchronous step
  (`engine/mod.rs:2598-2609`); completion likewise. There is **no state in which an
  element rests mid-transition** awaiting external work.
- **Transport:** the jobs read model already carries `JobKindEnum` and
  `JobListenerEventTypeEnum`, but every engine job is hardcoded
  `JobKindEnum::BpmnElement` + `JobListenerEventTypeEnum::Unspecified`
  (`server/src/main.rs:13774-13775, 13908-13909`). The REST spec surface
  (`spec/jobs.yaml`, enum `EXECUTION_LISTENER`/`TASK_LISTENER`) is the copied C8 API
  contract — schema only, no engine behaviour behind it.

**The core engine change is therefore to break lifecycle atomicity**: let an element
instance *rest* in `ACTIVATING` (resp. `COMPLETING`) while a chain of listener jobs
runs, then finalise the transition when the chain drains.

## Decision (proposed)

Implement **execution listeners first** (they apply to every flow node and cannot
deny, so they are the smaller, cleaner change); specify **task listeners** as a
follow-on §6 (user-task-only, adds denial + corrections, reuses the same machinery).
Match the Camunda job shape byte-for-byte so an **unmodified** listener worker runs.
Everything is gated on the element actually declaring listeners, so models without
them emit **byte-identical journals** (replay-safe; the hot path is unchanged).

### 1. Model (`bpmn.rs`, `model.rs`) — parse and attach listeners

- Parse `zeebe:executionListeners`/`zeebe:executionListener` into
  `ExecutionListener { event_type: ListenerEvent::{Start,End}, job_type: String,
  retries: Option<String> }` (retries a literal-or-FEEL raw expression, resolved at
  job creation exactly like `taskDefinition` retries).
- Add `start_listeners: Vec<ExecutionListener>` / `end_listeners: Vec<ExecutionListener>`
  to `struct Element` (`model.rs:571`), preserving declaration order. Empty vecs
  (the overwhelmingly common case) ⇒ no behaviour change. This is orthogonal to
  `ElementKind`, so it applies uniformly to tasks, gateways, events, and containers.

### 2. Runtime state (`state.rs`) — a rest point mid-transition

- Add a per-element-instance **listener cursor**: `{ phase: Start|End, index: usize }`
  recording which listener in the chain is currently outstanding. Present only while a
  listener job is in flight; absent otherwise.
- Add a job kind discriminator so a created job knows it is an execution listener
  (carries `event_type` Start/End). Reuse the existing job store; the discriminator
  drives (a) the read-model `JobKind`/`JobListenerEventType` mapping and (b) the
  completion router in §3.

### 3. Commands/events + transitions (`command.rs`, `event.rs`, `engine/mod.rs`)

The heart of the change. Split the two atomic transitions:

- **Activation.** In `activate_element`: emit `ElementActivating`. If the element has
  `start_listeners`, **stop** — create listener job `#0` (type + resolved retries,
  `event_type=Start`), set the listener cursor, and rest. Do **not** yet emit
  `ElementActivated` or run the element behaviour. If there are none, proceed exactly
  as today (emit `ElementActivated`, apply input mappings, create the service job /
  route the gateway / …).
- **Listener completion** (a `CompleteJob` whose job is an execution listener): merge
  the returned variables into the element scope; advance the cursor. If another
  same-phase listener remains, create it. If the `Start` chain is **drained**, emit
  `ElementActivated` and run the element's **normal activation behaviour** (the code
  path currently inlined right after `ElementActivated`). If the `End` chain is
  drained, emit `ElementCompleted` and take the outgoing flow.
- **Completion.** Wherever the engine emits `ElementCompleting`→`ElementCompleted`
  today (service-job complete, gateway, pass-through events, sub-process end — many
  sites, `engine/mod.rs`), interpose: emit `ElementCompleting`, and if the element has
  `end_listeners`, create end-listener `#0` and rest instead of emitting
  `ElementCompleted`. A shared helper (`begin_end_listeners_or_complete`) keeps the
  many completion sites consistent.
- **Failure/incident.** A listener job that exhausts retries raises an incident like
  any job (existing machinery); the element stays parked in `ACTIVATING`/`COMPLETING`.
  A listener whose job *properties* fail to evaluate (e.g. a `=type` expression,
  `EXTRACT_VALUE_ERROR`) parks the element on an incident; resolving it re-runs the
  lifecycle step exactly as Zeebe does (#1406): the chain **restarts at listener 0**,
  and the element's input (start chain) or output (end chain) mappings are
  re-applied. A split gateway keeps the route journaled at activation.
- **Interruption.** If an interrupting boundary event / terminate fires while a
  listener chain is outstanding, cancel the in-flight listener job and drop the cursor
  as part of the element's termination (must be handled so a pending listener can't
  resurrect a terminated element).

### 4. Job kind plumbing (`server/src/main.rs`, generated models, `falcon.rs`)

- Map the listener discriminator onto the jobs read model: replace the hardcoded
  `JobKindEnum::BpmnElement` / `JobListenerEventTypeEnum::Unspecified` with
  `ExecutionListener` + `Start`/`End` **for listener jobs only** (non-listener jobs
  unchanged). Both the REST jobs projection and the activate-jobs path.
- Activation and complete-job transports are otherwise **unchanged** — a listener job
  activates and completes through the same REST/Falcon paths as any job (that is the
  point: stock workers work). Completion just routes into the §3 advance instead of
  the element's own complete.

### 5. Read model + metrics

- Listener jobs are real jobs ⇒ they appear in the jobs read model / trace with the
  correct kind + event type (observability parity with Operate's listener jobs).
- Add `nanobpm_execution_listener_jobs_total{event_type=start|end,outcome=created|completed|failed}`.

### 6. Task listeners (implemented — engine-core runtime)

Task listeners apply **only to user tasks** and add two things execution listeners
lack: the `assigning`/`updating`/`completing` events can **deny** the transition
(`denied=true` + `deniedReason`) and every event may return **corrections**
(assignee, candidate groups/users, due/follow-up date, priority). They fire on the
five user-task lifecycle transitions — `creating`, `assigning`, `updating`,
`completing`, `canceling` — reusing the §2/§3 machinery (a `JobKind::TaskListener`,
a sequential listener cursor, and an `AdvanceTaskListener` step) hooked at the
**user-task lifecycle** rather than the element lifecycle.

**What is implemented (engine-core):**

- **Parsing** (`bpmn.rs`, `model.rs`): `zeebe:taskListeners`/`zeebe:taskListener`
  (`eventType` defaulting to `creating`) parse into `Element.task_listeners`,
  mirroring execution-listener parsing. A user task carrying no task listeners
  parses and runs **byte-identically** to the pre-task-listener engine.
- **Deferred transitions** (`state.rs`): a user-task transition whose payload is
  not re-derivable from resident state (the target assignee, the update changeset,
  the captured completion variables) is persisted durably on `UserTask.pending`
  (`PendingUserTaskTransition`) via a `UserTaskTransitionDeferred` event, so replay
  reconstructs the in-flight transition exactly.
- **Runtime** (`engine/mod.rs`): each of `AssignUserTask`/`UnassignUserTask`/
  `UpdateUserTask`/`CompleteUserTask` checks for listeners of the matching event;
  if any exist it emits `UserTaskTransitionDeferred` + the first
  `TaskListenerJobCreated` and does **not** apply the transition yet. `creating`
  listeners gate the task at element `ACTIVATING`, so it is not `Created` (and not
  assignable/completable) until the chain drains. Completing a task-listener job
  advances the chain (`AdvanceTaskListener`); when the last one drains,
  `commit_user_task_transition` applies accumulated corrections and emits the real
  lifecycle event(s) plus `UserTaskTransitionResolved`.
- **Deny + corrections** are carried on the complete-job result
  (`TaskListenerJobResult` on `Command::CompleteJob`). Validation (Zeebe parity):
  task-listener jobs may not carry variables; `denied` is honoured only on
  `assigning`/`updating`/`completing`; `denied` and `corrections` are mutually
  exclusive; a `creating` listener may not correct the assignee when the task
  already declares an initial assignee. A denial emits
  `UserTaskTransitionResolved{denied:Some(reason)}`, clearing `pending` and
  returning the task to its prior available state.
- **Canceling (deferred termination):** `CancelInstance` runs each `Created` task's
  `canceling` chain before termination. When any chain must run it emits
  `ProcessInstanceTerminating` (a new non-terminal `ProcessInstanceState`) instead
  of `ProcessInstanceTerminated`; the last canceling chain to drain emits
  `ProcessInstanceTerminated`. An instance with no canceling listeners terminates
  synchronously, **byte-identically** to before.

**Boundaries (deferred to follow-ups):** the `TaskListenerJobResult` (deny +
corrections) plumbing stops at the engine boundary — extending it through the
server command-stream transport and the client SDKs is a follow-on. Read-model
projection of the new events and behaviour under process-instance **migration**/
**modification** are likewise out of scope here.


### Subset (honestly stated, per the whitepaper discipline)

State each boundary in `PERFORMANCE.md` / the feature matrix:

- **v1 (this ADR, as implemented):** `zeebe:executionListeners`
  parsed on any task/container that carries a `zeebe:ioMapping` attach point
  (service/script/business-rule/user tasks, call activity, (sub)process,
  ad-hoc/multi-instance) — **and, since #1197, on the non-activity flow nodes
  enumerated in the "Non-activity flow-node listeners" note below** (gateways,
  start events, boundary events), with placements where a listener could never
  fire rejected at deploy. Sequential in-order execution; literal/FEEL `retries`;
  incident on failure; forward variable merge; correct
  `JobKind`/`JobListenerEventType` in the read model and on the activated job.
  - **`start` listeners fire for every element reached through the common
    `activate()` path** (all of the above), before the element enacts its own
    behaviour (job creation / routing / scope open). The multi-instance **body**
    is a special case: it early-returns from `activate()` before the shared start
    gate, so it carries its **own** start gate — its `start` listeners fire once
    on the body, before any child is instantiated, and child fan-out is deferred
    to `advance_listener` (which re-derives it via `spawn_multi_instance_children`
    once the chain drains). They do not double-fire per child.
  - **`end` listeners fire for every element that completes**, across both the
    shared `complete()` path (service/script/business-rule/user tasks,
    pass-through events) and the previously-deferred structural completion sites,
    each of which now parks in `COMPLETING` while its end chain runs and finalises
    through a dedicated `finalize_*` tail dispatched from `advance_listener`:
    the **exclusive gateway** (routing deferred until the chain drains, then
    re-selected), the **embedded sub-process** (and, since a **call activity** is
    spliced into a sub-process before deploy, call activities too), the
    **multi-instance body** (fires once, after every child completes), and the
    **ad-hoc sub-process container** (natural, no-further-activations completion).
    Each `finalize_*` re-derives its structural tail from still-resident state
    (the parked element stays active, its scope resident until `ElementCompleted`),
    so a node restart between park and drain is replay-safe.
- **Documented boundaries (not gaps in coverage, but noted nuances):**
  - An exclusive gateway's `end` listener runs **before** the routing decision, so
    a listener that mutates a condition variable is observed by the post-listener
    re-selection. If re-selection then matches no flow (and there is no default),
    the gateway raises a no-matching-flow incident and keeps its token, exactly as
    its non-listener path does — it does not silently complete and drop the token.
  - The ad-hoc container's `end` listeners fire on the **natural** completion path
    (the agent returns no further activations and no tools remain). The
    agent-signalled *cancel-remaining-instances* and *completion-condition-
    fulfilled* completions cancel any still-running tools and stay inline (the
    `cancelled` flag cannot be re-derived at drain), so they do not run end
    listeners — treated as an aborted, not a clean, completion.
- **Task listeners (§6) are implemented in engine-core** (user-task
  `creating`/`assigning`/`updating`/`completing`/`canceling` denial + corrections);
  their transport/SDK plumbing and read-model projection are the follow-ups.
- **Deferred:** listener behaviour under process-instance **migration** and **modification**;
  interaction subtleties with non-interrupting boundary events firing mid-chain.
- **Non-activity flow-node listeners (#1197):** `start`/`end` execution listeners
  on **gateways** (exclusive/parallel/inclusive/event-based), **start events**, and
  **boundary events** are parsed (`engine-core/src/bpmn.rs` pushes these nodes onto
  the `io_stack`; a boundary event, buffered rather than live on the stack, carries
  its listeners on the pending boundary and re-attaches them by id at build) and
  fire through the shared activation/completion listener gate — closing the silent
  drop / mis-attachment gap. Placements where a listener could never fire are
  **rejected at deploy** rather than silently stored
  (`UnsupportedExecutionListener`): a **multi-incoming parallel gateway** (a *join*
  that synchronises tokens and completes without running the activation body or the
  end-listener chain — so *neither* phase fires; a single-incoming split is
  supported), the **`start` listener of a multi-incoming inclusive gateway** (the
  join is synchronised on activation and short-circuits the activation body — but
  its **`end` listener IS supported**, since an accepted join runs the end-listener
  chain before routing, so an inclusive-join `end` listener is *not* rejected), a
  **compensation boundary event** (a passive structural marker never entered by
  token flow), a **tool of an ad-hoc sub-process** (a leaf tool is pruned into the
  non-executable catalog and a retained embedded tool is activated/completed with
  direct lifecycle events, both bypassing the listener gate), a
  **process-level listener** (a listener on the `<process>`'s own
  `extensionElements` — the process element has no activation/completion lifecycle
  and is never ridden by a token, so *neither* phase fires), and a
  **sequence-flow ("take") listener** (a sequence flow is
  modelled as an edge, not an `Element`, so it has no lifecycle to run a listener
  on). One more placement is rejected: **any execution listener on a
  surplus (non-designated) signal start event** (a process may have only one real
  start, so extra signal starts are demoted to an inert `IntermediateThrowEvent`
  with no incoming flow that is never activated or completed, so *neither* phase
  could fire). (A terminate end event's listeners are both supported since #985:
  like Zeebe's `TerminateEndEventBehavior`, the terminate end rests in COMPLETING
  while its `end` listener chain runs, and the scope-wide teardown runs only once
  the chain drains.) **Sequence-flow ("take") listeners
  remain deferred:** firing a take listener between source completion and target
  activation needs a model + runtime design and is tracked separately in #1198 —
  until then a listener nested in a `<sequenceFlow>` is rejected at deploy (naming
  the offending flow), never silently dropped or hoisted onto the enclosing node.
  A **`zeebe:taskListener` on a non-user-task element** is likewise rejected at
  deploy (`UnsupportedTaskListener`, naming the element): task-listener jobs are
  created only on the user-task runtime path, so a task listener attached to any
  other element (e.g. a `receiveTask`, which rides the `io_stack` for its
  *execution* listeners) could never fire — reject-don't-drop rather than store a
  dead task listener.

## Phased plan

1. **Contract fixtures.** Import a Camunda golden BPMN with start+end execution
   listeners on a service task + a gateway; capture the exact create-listener-job and
   complete-job REST bodies a stock worker exchanges (mirror `ExecutionListenerJobTest`).
2. **Model** (seam 1): parse + attach `start/end_listeners`; unit tests over fixtures;
   assert a listener-free model's parse is byte-unchanged.
3. **Transport** (seam 4): `JobKind`/`JobListenerEventType` mapping (no runtime yet).
4. **Runtime** (seams 2–3): the transition split + listener cursor + completion router;
   engine tests for ordering, variable merge, retries→incident, and interruption
   mid-chain. **Guard:** a listener-free run emits the identical journal.
5. **Read model + metrics** (seam 5).
6. **E2E parity test:** run the golden BPMN under an unmodified listener worker on
   embedded Bernd; assert listener jobs appear in the read model in order and the run
   matches Camunda's outcome. Update the feature matrix + `PERFORMANCE.md` subset note.
7. **Task listeners** (seam 6): implemented in engine-core (all five events,
   deny + corrections, deferred termination); transport/SDK plumbing and read-model
   projection tracked as follow-ups.

## Consequences

- **First feature to break element-lifecycle atomicity.** Every existing completion
  site must route through the shared "begin end-listeners or complete" helper; the
  invariant "an element can now rest in `ACTIVATING`/`COMPLETING`" ripples into
  interruption, boundary-event, and termination handling. This is the main risk and
  the reason for the strict listener-free-is-byte-identical guard.
- Unmodified Zeebe listener workers run on Nano — the ADR 0022 switch-over-parity
  target for a widely-used authoring feature that today silently no-ops (a
  correctness cliff: a model relying on an `end` listener to, e.g., write an audit
  record simply skips it on Nano).
- Listener jobs are first-class in the read model/trace, extending the same
  audit-trail parity ADR 0023 established for ad-hoc tools.

## Open questions

- **Ordering vs. IO mappings — RESOLVED (pinned against Zeebe
  `BpmnStreamProcessor`).** `start` listeners run **after** `zeebe:input`
  mappings are applied and **before** the element's job/behaviour + `ACTIVATED`;
  `end` listeners run **after** `zeebe:output` mappings (and any script/DMN
  result merge) and **before** `COMPLETED` + the outgoing flows. Nano matches
  this: the listener job's own FEEL attributes resolve against the
  input-/output-mapped variable view. (The early `ElementActivated` marker this
  note originally called out was removed by #1429: the element now stays
  `ACTIVATING` until the `start` chain drains, exactly as Zeebe writes
  `ELEMENT_ACTIVATED` only after the last start listener completes.)
- **Container start/end semantics:** for a sub-process/multi-instance body, do
  `start`/`end` listeners fire on the container boundary only, or also interleave with
  child activation? Confirm against Camunda for the container processors.
- **Retry/incident UX:** a parked element with a failed listener — surface it in the
  console incidents view identically to a failed service job (it already is a job), or
  distinguish listener incidents?
- **Guarding runaway/expensive chains:** any engine cap on listener count or a
  per-listener timeout, or leave it purely worker/incident-driven (as Zeebe does)?
