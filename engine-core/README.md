# nanobpmn-engine-core

A minimal, **embeddable** BPMN execution engine.

It follows the **Camunda 8 (Zeebe)** execution architecture — a deterministic
`command → event → applier` state machine driven by a single sequential writer —
with all of the distributed-systems machinery deliberately removed. There is no
Raft, no partitioning, no exporters, no gRPC and no RocksDB. What's left is the
part that makes a process engine correct, testable and portable.

## Why the Camunda 8 model (not the Camunda 7 PVM)?

Two execution architectures were considered:

| | Camunda 7 PVM (`camunda-bpm-platform`) | Camunda 8 / Zeebe |
| --- | --- | --- |
| Execution | Token walks the graph via `ActivityBehavior` + atomic operations | Element-lifecycle state machine driven by a stream of commands/events |
| State mutation | Mutable entities, in-place + dirty-check flush | `command → event → applier`; events are the source of truth |
| Persistence | Relational DB via MyBatis (ORM, schema, migrations) | Append-only log + key-value store; **replay to recover** |
| Concurrency | Multi-threaded, guarded by **optimistic locking** + retries | **Single writer** — no locks, no races, no retries |
| Async work | Background `JobExecutor` polling locked job rows | Just the next record to process |

For a *nano*, embeddable engine the Camunda 8 model wins decisively:

1. **It removes the hardest parts.** Single-writer sequential processing makes
   optimistic locking, transaction managers and a job-acquisition protocol
   unnecessary — an entire class of concurrency bugs cannot occur.
2. **`command → event → applier` is trivially testable and deterministic.** Feed
   commands, assert the event stream; replay the events to reconstruct state
   (see the `should_be_deterministic_and_replayable` test).
3. **Persistence is optional and pluggable.** The engine holds state in memory
   and emits a replayable event log; back it with a WAL, `redb`, `sled` or
   SQLite — or nothing at all. `Engine::replay(events)` reconstructs state (and
   the key generator) from a recorded log; enable the off-by-default `serde`
   feature to (de)serialize events for an on-disk journal. The server ships one
   (set `NANOBPMN_DATA_DIR`); see the repo `README.md`.
4. **It matches the rest of nanobpmn.** The generated REST layer *is* the
   Camunda 8 v2 API, so an engine speaking C8 semantics wires straight behind it.

Camunda 7 remains a useful **per-element semantic reference** (its BPMN behaviour
catalogue is mature and readable); it is the *execution architecture* we don't
copy.

## Why it runs on phones (and in the browser)

`engine-core` has **zero dependencies and uses only `std`** by default (the
optional `serde` feature adds `serde` only when a host opts into event
(de)serialization). The same crate compiles for:

- native servers (`x86_64` / `aarch64`),
- **iOS** (`aarch64-apple-ios`) and **Android** (`aarch64-linux-android`),
  embedded in-process and called from Swift/Kotlin — generate the bindings with
  [UniFFI](https://mozilla.github.io/uniffi-rs/),
- **`wasm32`** (verified: `make engine-wasm`), for a mobile web view or a
  JS/React Native layer.

This is only possible because the engine is a single owned state machine with no
threads, no locks, no networking and no database. On a phone you embed
`engine-core` directly; you do **not** run the HTTP `server/` crate there. Keep
FFI surfaces coarse (submit a command, drain the events) rather than chatty, and
build with `opt-level = "z"` + LTO to keep the binary small.

### The FFI surface

`src/ffi.rs` (behind the off-by-default **`ffi`** feature) is exactly such a
coarse C-ABI: `nbpmn_engine_new`/`_free`, `nbpmn_alloc`/`_free` for passing
bytes across the boundary, and a handful of operations — `nbpmn_deploy_bpmn`,
`nbpmn_create_instance`, `nbpmn_correlate_message`, `nbpmn_trigger_timers`,
`nbpmn_is_completed`, `nbpmn_instance_count`. Each submits one command and
returns a scalar summary (a key, an event count, a flag); none can unwind across
the boundary. The same functions back a UniFFI layer on mobile and the wasm
exports in a browser.

The crate is `crate-type = ["lib", "cdylib"]`, so building with `--features ffi`
emits a loadable artifact. `make engine-wasm-ffi` builds it for
`wasm32-unknown-unknown` and runs `scripts/verify-wasm-ffi.mjs`, which asserts
every `nbpmn_*` symbol is exported and then instantiates the `.wasm` (no imports
needed) to drive a real deploy → create → complete cycle — proving the FFI wasm
build works end to end.

## The execution model

Every BPMN element instance walks the same lifecycle, mirroring Zeebe:

```text
ACTIVATING -> ACTIVATED -> COMPLETING -> COMPLETED --(take outgoing flow)--> ACTIVATING(next)
```

- **Pass-through elements** (start/end events) traverse it in one burst.
- **Execution order matches Zeebe** (#1244, pinned by the `formal/traces`
  scenarios `parallel-fork-join-tasks` and `inclusive-join-pass-through-race`).
  An element's outgoing flows are taken (and an exclusive/inclusive gateway's
  conditions evaluated) in **reverse document order**, as Zeebe's `getOutgoing()`
  lists them; the declared order is still what is persisted (flow ordinals).
  Gateways complete inside their own activation, ahead of work already queued;
  other elements complete as a queued step, after it.
- A **join** (parallel or inclusive gateway with several incoming flows) writes
  no element-instance record until it is ready: an early arrival parks a token
  (`ParallelJoinOpened`) with no `ACTIVATING`/`ACTIVATED`, just as Zeebe rejects
  the join's `ACTIVATE_ELEMENT` command until it can fire. The arrival that makes
  it ready activates and completes it once.
- A **service task** rests in `ACTIVATED` after creating a job, and only advances
  to `COMPLETING` when a `CompleteJob` command arrives. This is how asynchronous
  work is modelled with no background thread. A task's job type may be a FEEL
  expression (`type="=jobType"`, `type='="worker-" + region'`); it is evaluated
  against the instance variables at job-creation time. As in Zeebe, an
  expression that cannot be evaluated (or yields the wrong type) raises an
  `EXTRACT_VALUE_ERROR` incident; resolving it re-evaluates. The same holds for
  every other element expression (retries, priority, user-task assignment and
  dates, timers, message names / correlation keys, multi-instance and ad-hoc
  collections and conditions) — there is no silent fallback.
- A **user task** (`<bpmn:userTask>` with a `<zeebe:userTask/>`, i.e. a native
  Zeebe user task) rests in `ACTIVATED` after the engine creates the task, and
  advances only when a `CompleteUserTask` command arrives. Unlike jobs there is
  no worker lock or retries: a task is `Created`, then `Completed` (or
  `Canceled` when its instance is terminated). The assignment, scheduling and
  priority expressions declared on the element (`zeebe:assignmentDefinition`,
  `zeebe:taskSchedule`, `zeebe:priorityDefinition`) are resolved against the
  instance variables (FEEL or literal) at creation. Its form linkage
  (`zeebe:formDefinition`) is also captured at creation: a `formId` is resolved
  against the currently-deployed forms (latest version) to a numeric `form_key`,
  and an `externalReference` is carried verbatim, so the v2 user-task search can
  surface `formKey`/`externalFormReference`. While `Created` the task may
  be assigned (`AssignUserTask`, honouring `allowOverride`), unassigned
  (`UnassignUserTask`) and have its candidate groups/users, due/follow-up date
  and priority changed (`UpdateUserTask`). Completion merges its variables and
  resumes the token, exactly like `CompleteJob`. (Task listeners and their
  transient states — `ASSIGNING`/`UPDATING`/`COMPLETING` etc. — are not modelled:
  without listener job workers the transitions are atomic.)
- **Job activation** mirrors Camunda 8: a worker activates available jobs of a
  type (`ActivateJobs`), locking each until `now + timeout`. A job must be
  activated before it can be completed. Locks expire — either lazily on the next
  activation or via an explicit `ExpireJobs` tick — making the job activatable
  again. `JobActivationOptions.with_lease` defaults to false; setting it true
  gives every job kind an opaque string token. Leased complete/fail/throw-error
  commands require the matching token (`Command::with_job_lease`); retries/timeout
  updates may omit it, but a supplied stale token is rejected. Failure and timeout
  retain the token: late completion is allowed until another leasing activation
  supersedes it, and non-leasing workers skip previously leased jobs. Unleased
  jobs retain first-completion-wins behavior. The engine is **clock-free**: the
  caller supplies `now` (a logical instant) on `ActivateJobs`/`ExpireJobs`.
- **Job failure** (`FailJob`) sets a job's remaining retries. With retries left
  the job returns to the activatable pool; with none it parks (`JobState::Failed`)
  and an **incident** is raised on the instance — the same incident mechanism an
  exclusive gateway uses when no flow matches. Jobs start with a default retry
  count and, like completion, failing requires prior activation.
- **Business errors** (`ThrowJobError`) let a worker raise a named error from a
  job. If the job's service task has an **error boundary event** with a matching
  `errorCode`, the task is interrupted and the boundary's outgoing flow runs the
  error-handling path; an unmatched error raises an incident instead. The job is
  consumed either way (`JobState::Errored`) and, like the other transitions,
  throwing requires prior activation.
- **Incidents** are first-class records with a key (`State::incidents`) and a
  `created_at` timestamp, raised when a token cannot proceed: a job exhausted its
  retries, an exclusive gateway matched no flow, a sequence-flow condition failed
  to evaluate, or a thrown error went uncaught (`IncidentKind`). A job-incident links back to its job. Recovery mirrors
  Camunda: `ResolveIncident` **retries the failed work** rather than merely
  clearing the record. A job-incident returns the parked job to the activatable
  pool (after `UpdateJobRetries` restores its retries; resolution is rejected
  while it still has none). A gateway incident re-evaluates the gateway against
  the current variables. An uncaught-error incident re-creates the service-task
  job. If the retry fails again, a fresh incident is raised by the same code
  paths that raised the original.
- **Incident lifecycle** is auditable: incidents carry an `IncidentState`
  (`Active`/`Resolved`). Resolving transitions the record to `Resolved` —
  stamping `resolved_at` (from the host clock) and any `operation_reference` —
  and **retains it** rather than deleting it, so the read APIs expose a full
  history. Only `Active` incidents can be resolved (re-resolving a `Resolved`
  one is rejected). `Engine::incidents()` returns the whole history;
  `Engine::active_incidents()` filters to open ones, and the per-instance active
  index (`ProcessInstance::incidents`) drives `hasIncident`.
- **Link intermediate events** wire a throw to a matching catch **within the same
  scope** by name instead of by an explicit sequence flow — the modeller's
  "go-to" for keeping a diagram uncluttered. An `intermediateThrowEvent` carrying a
  `linkEventDefinition` completes its incoming flow and then hands its token
  **directly** to the `intermediateCatchEvent` whose `linkEventDefinition` shares
  the same `name` **in the same scope** (link events never cross a (sub)process
  boundary), which resumes as a pass-through out its own outgoing flow. No
  synthetic sequence flow is emitted (the handoff never appears in
  `takenSequenceFlows`). Many throws may target one catch; two catches sharing a
  link name, a throw/catch pair split across scopes, an empty link `name`, or a
  throw with an outgoing (or a catch with an incoming) sequence flow are all
  rejected at deploy (Zeebe `verifyLinkIntermediateEvents` parity). Before
  this an unrecognised link throw silently swallowed the token and the catch never
  fired.
- **Timer intermediate catch events** park a token mid-flow until a deadline.
  Reaching one arms a `Timer` (`due_at = now + duration`) and rests in
  `ACTIVATED`; a host-driven `TriggerTimers { now }` tick fires every due timer,
  releasing its token along the event's outgoing flow. Like job-lock expiry the
  engine stays **clock-free** — the host supplies `now` on the tick. Timers are
  durable (`TimerCreated`/`TimerTriggered` are journaled), so a parked timer
  survives a restart and fires on the next due tick; a fired timer is retained so
  it never re-fires.
- **Interrupting timer boundary events** attach a deadline to a service task. When
  the task activates, its boundary timer is armed (`due_at = now + duration`); if
  the timer fires before the job is done, the engine **cancels the job**
  (`JobState::Canceled`, terminal — no longer activatable/completable), interrupts
  the activity, and routes the token out the boundary's outgoing flow. If the job
  instead completes (or the task is interrupted another way, e.g. an error
  boundary) first, the armed boundary timer is **disarmed** (`TimerState::Canceled`)
  so it never fires. `JobCanceled`/`TimerCanceled` are journaled, so both outcomes
  survive a restart. A **non-interrupting** timer boundary
  (`cancelActivity="false"`) instead leaves the activity (and its job) running and
  spawns a new parallel token along the boundary's outgoing flow when it fires. A
  **cycle** non-interrupting timer boundary (`timeCycle` `R/PT…`) **re-arms**
  itself for the next interval on every fire (`due_at += interval`), spawning a
  parallel token each time, until the activity completes (which disarms the
  pending timer).
- **Message intermediate catch events** park a token mid-flow until a matching
  message is correlated. Reaching one opens a `MessageSubscription` keyed by the
  message name and a **correlation value** — the stringified value of the named
  instance variable captured at open time. A host-driven `CorrelateMessage
  { message_name, correlation_key, variables }` (the engine's `correlate_message`
  helper, or the REST `publishMessage`/`correlateMessage` endpoints) releases the
  token of every matching open subscription along the event's outgoing flow,
  merging the message's variables into the instance first. Like timers the engine
  stays **clock-free** and the message flow is durable
  (`MessagePublished`/`MessageSubscriptionCreated`/`MessageCorrelated` are
  journaled), so a parked subscription survives a restart and a settled one is
  retained so it never re-correlates. Messages are **not buffered** — with no
  matching open subscription the message is simply dropped (no TTL/dedup).
- **Interrupting message boundary events** attach a subscription to a service
  task. When the task activates a subscription is opened; if a matching message is
  correlated before the job is done, the engine **cancels the job**, interrupts
  the activity, and routes the token out the boundary's outgoing flow. If the job
  instead completes (or the task is interrupted another way) first, the open
  subscription is **cancelled** (`MessageSubscriptionState::Canceled`) so it never
  correlates. Both outcomes are journaled and survive a restart. A
  **non-interrupting** message boundary (`cancelActivity="false"`) instead leaves
  the activity (and its job) running and spawns a new parallel token along the
  boundary's outgoing flow for **every** matching message — its subscription stays
  open rather than settling.
- **Compensation** lets a completed activity be undone by a dedicated handler. A
  `boundaryEvent` carrying a `compensateEventDefinition`, wired by an
  `<association>` to an `isForCompensation` handler activity, marks its attached
  activity **compensable**: when that activity completes a durable
  `CompensationSubscription` is recorded (`CompensationSubscriptionCreated`,
  journaled in completion order). A `compensateEventDefinition` on an
  `intermediateThrowEvent`/`endEvent` is a **compensation throw**: activating it
  triggers the handlers of the compensable activities in scope in **reverse
  completion order** (`CompensationTriggered`), running each handler activity as
  a normal job; the throw rests until every handler completes
  (`CompensationHandlerCompleted`) before routing its own token onward. With
  nothing to compensate the throw is a pass-through. This covers the
  single-activity path (one completed task → its handler); whole-scope and nested
  compensation are follow-ups.
- **Terminate end events** (`<endEvent>` with a `<terminateEventDefinition>`)
  don't merely consume their own token: reaching one **kills every other active
  token in its enclosing scope** — parallel-split sibling branches, pending
  timers, open jobs and subscriptions — and then completes that scope. A
  terminate end in the top-level process ends the whole instance
  (`ProcessInstanceTerminated`); a terminate end inside an embedded sub-process
  ends only that sub-process scope, and the parent instance continues on the
  sub-process's outgoing flow.
- **Message start events** create a new process instance when a matching message
  arrives. Deploying a process whose start event carries a
  `messageEventDefinition` opens a **process-level** `MessageStartSubscription`
  keyed by the message name (not a per-instance subscription). A `CorrelateMessage`
  whose name matches then **creates a fresh instance**, seeding it with the
  message's variables, and runs it from the start event. The subscription is
  journaled (`MessageStartSubscriptionCreated`), so it survives a restart and keeps
  starting instances; the REST `correlateMessage` reports the new instance's key.
- **Timer start events** create instances on a schedule, with **no triggering
  command**. Deploying a process whose start event carries a `timerEventDefinition`
  arms a process-level `StartTimer` (`due_at = now + interval`, journaled as
  `ProcessStartTimerArmed`). A host-driven `TriggerTimers { now }` tick fires every
  due start timer, **creating a new instance** and running it from the start event.
  A **cycle** (`timeCycle` `R/PT…`) **re-arms** for the next interval
  (`ProcessStartTimerFired { next_due_at: Some(_) }`); a **one-shot**
  (`timeDuration`) is retained with no due time (`next_due_at: None`) so it never
  fires again. Like all timers the engine stays **clock-free** and the schedule is
  durable across restarts.
- **Variables** can be merged into a scope with `SetVariables` (the scope key may
  be a process instance or any active element instance — nano keeps a single
  instance-level scope). Typically used to correct the data behind a gateway
  incident before resolving it, so the re-evaluation on resolution succeeds.
- The engine reads **no wall clock**: the host supplies `now` to
  `apply_command_at`, and timestamped events (e.g. a raised incident) carry it,
  so replay reconstructs identical timestamps.
- A process **instance completes** when its last token is consumed (its set of
  active element instances becomes empty).
- An **embedded sub-process** opens a token *scope*: activating it activates its
  inner start event inside the scope, and the sub-process element instance rests
  while the inner flow runs. When the inner scope drains (its last inner token is
  consumed) the sub-process completes and routes along its outgoing flow. An
  interrupting **error boundary event** attached to the sub-process catches an
  error thrown by any inner activity (the error propagates up enclosing scopes),
  terminates the whole inner scope (cancelling its jobs/timers/subscriptions) and
  routes the token out the boundary's outgoing flow. **Timer and message boundary
  events** can also attach to a sub-process (not just a service task): they arm
  when the sub-process activates, and an **interrupting** one tears down the whole
  inner scope (like the error boundary) before routing out the boundary, while a
  **non-interrupting** one spawns a parallel token and leaves the inner scope
  running.
- An **event sub-process** (`<subProcess triggeredByEvent="true">`, #1555) is a
  container like an embedded sub-process but **never entered by token flow**:
  while its enclosing scope (the process, or an embedded sub-process) is active,
  its single typed start event's trigger is armed, and the event's occurrence
  activates the sub-process. The parser models it as
  `ElementKind::EventSubProcess` carrying its start event's trigger — message,
  timer, signal, error, escalation or conditional — and its `isInterrupting`
  flag (default `true`; an error start is always interrupting). At runtime
  the message / timer / signal / conditional start triggers of every event
  sub-process directly contained in a scope **arm when that scope activates**
  and **disarm when it completes or is terminated**. An **interrupting**
  trigger terminates the scope's other active children and disarms its other
  event sub-process triggers, then runs the event sub-process; the scope
  completes when it does. A **non-interrupting** trigger runs the event
  sub-process concurrently and **repeatedly** (message / signal stay armed, a
  timer cycle re-arms — the `Rn` repetition count is not yet honoured, #1596);
  the scope waits for running ones before completing. **Error** (job error by
  code, or catch-all without a code — always interrupting) and **escalation**
  (by code or catch-all, interrupting or non-interrupting) start events catch
  throws from within the scope, resolved outward scope by scope with Zeebe's
  precedence: an exact code beats a catch-all, and an event sub-process beats
  a boundary event of equal match; an uncaught error raises an
  `UNHANDLED_ERROR` incident. Event payload variables (message / signal /
  error) propagate from the host scope. Error *end* events do not throw yet
  (#1592). `ProcessDefinition::event_subprocesses_in_scope` enumerates the
  event sub-processes directly contained in a scope.
- A **call activity** invokes another deployed process as a distinct **child
  process instance** of its `calledElement` / `zeebe:calledElement processId`
  (Zeebe/C8 parity), linked back to the caller via `parentProcessInstanceKey` /
  `parentElementInstanceKey`; the call activity's token parks until the child
  completes, then routes along its outgoing flow. Variables cross the boundary per
  Zeebe semantics: `zeebe:calledElement` `propagateAllParentVariables` /
  `propagateAllChildVariables` both default to `true` — a bare call activity
  copies all variables visible in its scope into the child at spawn and merges all
  of the child's final variables back into the parent scope on completion (the
  child's value wins on a name collision); setting either to `="false"` narrows
  that direction (input-mapping results only in, output-mappings only back), and
  `zeebe:ioMapping` input/output mappings always apply on top. Cancelling the
  parent cancels the in-flight child; a missing or misdeployed callee raises a
  recoverable `CalledElementError` incident.
- **Bounded hot state (optional eviction).** By default the engine retains
  completed instances forever — `is_completed`, `instance`, and the read APIs all
  keep working — which is ideal for an embedder that queries the engine directly.
  A host that instead projects history into a separate read model can call
  `Engine::evict_instance(key)` (drops a *completed* instance and everything it
  owns: jobs, timers, subscriptions, incidents) or `Engine::evict_completed()`
  (sweeps all completed instances and `shrink`s the maps) to keep the resident
  footprint tracking only in-flight work. Eviction is always opt-in and never
  touches active instances or non-instance state (deployed definitions, message-
  start and timer-start subscriptions). The server uses this behind its SQLite
  read model; see the repo `README.md`.

### Pieces

| File | Responsibility |
| --- | --- |
| `model.rs` | `ProcessDefinition` / `Element` / `ElementKind` and the `ProcessBuilder`. |
| `command.rs` | `Command` — the only way to drive the engine. |
| `event.rs` | `Event` — immutable facts; a replayable log. |
| `state.rs` | `State` and `apply()` — the **sole** mutator of state. |
| `engine.rs` | `Engine::apply_command` — the single-writer loop and the processor. |
| `ffi.rs` | Coarse C-ABI surface for FFI/wasm embedders (feature `ffi`). |

> **Scope.** This is a POC. The model supports start/end events, service tasks,
> **error boundary events** (a worker `throwError` caught by a matching boundary,
> interrupting the task and routing to its error-handling path),
> **interrupting timer boundary events** (a deadline on a service task that, when
> it fires first, cancels the job and routes the token out the boundary),
> **exclusive (XOR) gateways** (FEEL condition-based routing with a default flow,
> raising an incident when nothing matches or a condition fails to evaluate),
> **parallel (AND) gateways**
> (split takes all branches; join synchronises them: as in Zeebe, it fires once
> every incoming flow has delivered a token, consuming one per flow and keeping
> any surplus for a later firing),
> **inclusive (OR) gateways** (split takes every outgoing flow whose FEEL
> condition holds, falling back to the default flow; as in Zeebe, the join is
> evaluated when a token arrives: it fires once every incoming flow has been
> taken, or when no active element or in-transit token can still reach it over
> an untaken flow, then consumes one token per taken flow and keeps any surplus
> for a later firing; it is never re-evaluated otherwise, so a join whose
> remaining path diverges elsewhere waits, exactly as in Zeebe), **timer intermediate
> catch events** (a token parks until its `timeDuration` elapses, fired by a
> host clock tick) and **message events** — **message intermediate catch events**
> (a token parks until a matching message is correlated) and **interrupting
> message boundary events** (a message that, correlated first, cancels the job and
> routes the token out the boundary) — **non-interrupting timer and message
> boundary events** (`cancelActivity="false"`: the activity keeps running and a
> parallel token is spawned out the boundary on each fire, and a `timeCycle`
> timer boundary **re-arms** for the next interval on every fire), and
> **event-triggered instance creation**:
> **message start events** (a matching message creates a new instance) and
> **timer start events** (a one-shot `timeDuration` or recurring `timeCycle`
> creates instances on a host clock tick), and **embedded sub-processes** (a
> token scope whose inner flow runs to its own end before the sub-process routes
> on, with **error, timer and message boundary events** attached to the
> sub-process — an interrupting one terminates the whole inner scope and routes to
> its handler), and **terminate end events** (an `endEvent` with a
> `terminateEventDefinition` that kills the remaining tokens in its enclosing
> scope, then completes that scope), and **escalation events** (an
> `escalationEventDefinition` on an intermediate throw event or an escalation
> **end event** raises an escalation that propagates up the scope hierarchy; an
> **escalation boundary event** on a sub-process whose `escalationCode` matches —
> or a catch-all with no code — catches it, interrupting the sub-process when
> `cancelActivity="true"` or spawning a parallel token when `false`; an uncaught
> escalation is ignored, per BPMN/Zeebe semantics, and the thrower continues).
> Instances carry JSON-like variables (`null`, booleans, numbers,
> strings, lists and contexts) evaluated by an in-house FEEL engine ([`feel`])
> for gateway conditions, job types and message correlation. It also supports
> **persisted AgentInstance state** (Camunda stable/8.10 parity): agent markers
> classify ordinary job-worker elements rather than replacing their behavior.
> Both `agentType="aiAgentTask"` and `agentType="external"` service tasks create
> **normal jobs**, retaining their headers, linked resources, priority, retries,
> input/output mappings, and multi-instance behavior. The worker explicitly
> registers the `AgentInstance`; none is automatically minted on activation.
> Advancing BPMN requires ordinary job completion. Process completion/termination
> cleans up its agents. Repeated CREATE is a conflict, not an upsert. Canonical
> REST CREATE/UPDATE require job attribution; CREATE derives its definition from
> CONFIGURATION history. New history remains pending until job completion commits
> the winning activation's items and discards superseded attempts. UPDATE configuration
> applies at commit; usage metrics accumulate when new history is recorded.
> System prompts are typed content-block arrays. Persistence decoders convert
> every historical string into one literal TEXT block, never interpreting it as
> JSON. Per-turn metrics preserve absent objects and null counters separately
> from every submitted integer, including `-1` and zero.
> History-free jobless calls and per-agent completion are legacy embedded extensions,
> not the canonical REST contract.
> A co-located `zeebe:taskDefinition type="..."` supplies the agent's
> job type (literal or FEEL, evaluated after input mappings); without one it
> defaults to the element id. Adding the external marker therefore preserves
> an existing worker's task-definition-based routing.
> This replaces Nano's earlier no-job `aiAgentTask` behavior: workers must now
> register the agent explicitly. Historical `AgentTask` deployment frames and
> snapshots remain readable and are normalized to the shared service-task model;
> replay preserves existing runtime state and does not fabricate agent records
> or jobs for already-active elements. Existing jobless native activations cannot
> acquire a job lease after restoration; cancel/restart those instances against
> a redeployed model to use the job-backed lifecycle. Redeploy models to recover
> metadata that older parsers discarded, such as linked prompt resources.
> Placement mirrors Camunda's `AgentDefinitionValidator` (`aiAgentTask` only on a
> `serviceTask`, `aiAgentSubProcess` only on an `adHocSubProcess`); the wrong
> placement is rejected at deploy. Processes can be
> built programmatically with [`ProcessBuilder`] or parsed from BPMN 2.0 XML for
> that same subset (including `subProcess`, `boundaryEvent`/`errorEventDefinition`,
> `boundaryEvent`/`escalationEventDefinition`, `intermediateThrowEvent`/`escalationEventDefinition`,
> `intermediateCatchEvent`/`timerEventDefinition`, `messageEventDefinition`/
> `zeebe:subscription` and `startEvent` message/timer definitions) via the
> [`bpmn`] module (`bpmn::parse_bpmn`), a tiny dependency-free scanner.
> `zeebe:executionListeners` (`start`/`end`, ADR 0037) are parsed on tasks
> (including `receiveTask`, which pushes onto the io_stack so its listener lands
> on the receive task itself rather than hoisting onto the enclosing scope),
> sub-processes, ad-hoc/call containers **and** on gateways
> (exclusive/parallel/inclusive/event-based), **start events**, and **boundary
> events** (#1197) — each attaches to its own element and fires through the shared
> activation/completion listener gate. A listener that could never fire is
> rejected at deploy (`UnsupportedExecutionListener`): a multi-incoming
> parallel gateway (a *join* — both phases; a single-incoming split is
> supported), a multi-incoming inclusive gateway's `start` listener (its `end`
> listener IS supported — the join defers behind the end-listener chain at
> quiescence), a compensation boundary event, a terminate end event's `end`
> listener (its `start` listener IS supported — it fires through the activation
> start-listener gate), a listener on a surplus signal start event (demoted to
> an inert throw event that is never activated), a **tool of an ad-hoc
> sub-process** (a leaf tool is pruned and a retained embedded tool is
> activated/completed with direct lifecycle events, both bypassing the listener
> gate), and a sequence-flow ("take") listener. A listener on the `<process>`
> itself is likewise rejected (the process element has no activation/completion
> lifecycle and is never ridden by a token, so neither phase could fire).
> Sequence-flow ("take") listeners
> are not yet modelled (sequence flows are edges, not elements), so a listener
> nested in a `<sequenceFlow>` is rejected at deploy rather than silently
> dropped — tracked in #1198. A `zeebe:taskListener` on a non-user-task element
> is likewise rejected (`UnsupportedTaskListener`), since task-listener jobs run
> only on the user-task path.
> Deployments assign a per-id **version** and a unique process-definition key.
> A `sendTask` is parsed as a job-backed **service task** (Zeebe models
> send tasks as ordinary job workers). Constructs the engine does not execute
> are **rejected at deploy** with an `UnsupportedElement` error that
> names the construct, rather than being silently dropped.
> Deeper sub-process nesting is an intended extension point — new element kinds
> plug into `process_step` without touching the
> architecture.

## Usage

```rust
use nanobpmn_engine_core::{Command, Engine, ProcessBuilder};

let mut engine = Engine::new();

let process = ProcessBuilder::new("order")
    .start_event("start")
    .service_task("charge", "payment")
    .end_event("end")
    .connect("start", "charge")
    .connect("charge", "end")
    .build()
    .unwrap();

engine.apply_command(Command::DeployProcess(process)).unwrap();
let events = engine
    .apply_command(Command::CreateInstance { process_id: "order".into() })
    .unwrap();

let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
assert!(!engine.is_completed(instance_key)); // parked on the service task

// A worker activates the job (locking it for 30s) before completing it.
let jobs = engine.activate_jobs("payment", "worker-1", 10, 30_000, 0);
engine.apply_command(Command::complete_job(jobs[0].key)).unwrap();
assert!(engine.is_completed(instance_key)); // token resumed, instance done
```

## Build & test

```bash
cargo test                              # unit + integration + doctests
cargo test --features ffi               # also exercise the C-ABI surface
cargo build --target wasm32-unknown-unknown

# Build the FFI cdylib for wasm32 and verify its exports + a round-trip
# (from the repo root; needs the wasm32 target and node):
make engine-wasm-ffi
```
