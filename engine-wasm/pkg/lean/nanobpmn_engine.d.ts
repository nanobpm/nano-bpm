/* tslint:disable */
/* eslint-disable */

/**
 * A simulated engine instance bound to one modeler session.
 */
export class TestEngine {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Activate up to `max_jobs` `Created` jobs of `job_type`, locking them to
     * `worker` until `now + timeout_ms`. Returns a JSON array of activated jobs
     * (key, type, instance/element, retries, variables) for the dispatch loop to
     * hand to worker handlers. The host owns the wall clock via `tickNow`.
     */
    activateJobs(job_type: string, max_jobs: number, timeout_ms: number, worker: string, with_lease?: boolean | null): string;
    /**
     * Advance the virtual clock by `by_ms` milliseconds, firing any timers that
     * become due and expiring any lapsed job locks.
     */
    advanceTime(by_ms: number): string;
    /**
     * Assign a user task to `assignee`. When `allow_override` is false and the
     * task already has an assignee the command is rejected (it must be
     * unassigned first). Returns the snapshot.
     */
    assignUserTask(user_task_key: string, assignee: string, allow_override: boolean): string;
    /**
     * Broadcast a signal by name to **every** open subscription that matches,
     * across all instances, merging `variables_json` into each correlated
     * instance. Signals correlate by name only and are not buffered. Returns
     * the snapshot.
     */
    broadcastSignal(signal_name: string, variables_json: string): string;
    /**
     * Cancel (terminate) a running process instance by key. Every token is
     * discarded, pending jobs are canceled, and the instance transitions to
     * `Terminated`. Returns the snapshot.
     */
    cancelInstance(instance_key: string): string;
    /**
     * Legacy embedded-only completion extension. It neither commits history nor
     * advances BPMN; canonical workers complete their owning job instead.
     * Returns the snapshot.
     */
    completeAgentInstance(agent_instance_key: string): string;
    /**
     * Complete an ad-hoc sub-process **agent** job — the container's
     * JOB_WORKER job (Camunda's agentic `aiagent-job-worker`) — carrying the
     * agent's activate-element instructions so the engine runs the selected
     * inner "tools" this turn (ADR 0023 seam 2/3; Camunda `JobResult` /
     * `activateElements[]`). This is the browser seam that the plain
     * `completeJob` deliberately omits (it always sends
     * `adhoc_result: None`).
     *
     * `agent_result_json` shape (camelCase, mirroring Camunda's agentic
     * `JobResult`):
     * ```json
     * { "activateElements": [{ "elementId": "toolA", "variables": { "q": 1 } }],
     *   "completionConditionFulfilled": false,
     *   "cancelRemainingInstances": false }
     * ```
     * Empty/whitespace ⇒ a no-op result, so the container completes this turn
     * (no tool activated). `variables_json` merges instance variables exactly
     * like `completeJob` — e.g. the agent's final
     * decision when it signals `completionConditionFulfilled`.
     */
    completeAgentJob(job_key: string, variables_json: string, agent_result_json: string, lease_token?: string | null): string;
    /**
     * Complete a waiting job by key, merging `variables_json` (a JSON object
     * string) into the instance. The job is activated first if it has not been
     * already, so the UI can complete a freshly-created job directly.
     */
    completeJob(job_key: string, variables_json: string, lease_token?: string | null): string;
    /**
     * Complete a waiting user task by key, merging `variables_json` into the
     * instance before the parked token resumes. The task must be in the
     * `Created` state. Returns the snapshot.
     */
    completeUserTask(user_task_key: string, variables_json: string): string;
    /**
     * Correlate a message to any instance waiting on it: publishes `message_name`
     * with the given `correlation_key` (the value the waiting subscription's
     * `correlationKey` expression resolved to) and merges `variables_json` (a
     * JSON object string) into each correlated instance. This unblocks a message
     * intermediate catch / receive task without an external broker — the
     * in-browser equivalent of an app publishing a message. Returns the snapshot.
     */
    correlateMessage(message_name: string, correlation_key: string, variables_json: string): string;
    /**
     * Create an agent using `{ elementInstanceKey, jobKey, jobLeaseToken, history }`.
     * History must establish its CONFIGURATION. Returns the canonical creation
     * result with `agentInstanceKey` and positionally correlated `createdHistory`.
     */
    createAgentInstance(request_json: string): string;
    /**
     * Start a new instance of `process_id`, seeding it with the given variables
     * (a JSON object string; pass `"{}"` or `""` for none). `version` selects a
     * specific process **version** number (Zeebe by-id semantics); pass
     * `undefined`/`null` (or a non-positive value) for the latest version.
     * Returns the post-run [`Snapshot`] with a top-level `created` field holding
     * the new instance key.
     */
    createInstance(process_id: string, variables_json: string, version?: number | null): string;
    /**
     * Stop debugging, keeping the state the run produced. If the run is paused
     * mid-command, that in-flight command is first **finished normally** (its
     * breakpoints are cleared and it is resumed once) so the engine lands on the
     * same run-to-completion (RTC) quiescent state a plain command would produce
     * — never a partial, non-RTC intermediate one that later mutators could
     * build on (the hole [`TestEngine::guard_paused`] exists to prevent). To
     * discard the run's state entirely instead, use [`TestEngine::reset`].
     */
    debugClear(): void;
    /**
     * Begin a **debug** run of a `CreateInstance` command: deploy first (as
     * usual), then call this to start the instance under the stepping executor,
     * pausing at the first breakpoint (or running to completion if none match).
     *
     * `breakpoints_json` is a JSON array of `{ kind, id? }`:
     * ```json
     * [{ "kind": "elementActivated", "id": "Task_Charge" },
     *  { "kind": "elementCompleted", "id": "Gateway_1" },
     *  { "kind": "processCompleted" },
     *  { "kind": "everyStep" }]
     * ```
     * Returns the debug state JSON (see [`TestEngine::debug_state`]). Starting a
     * new debug run replaces any previous *finished* session; it is rejected
     * while a run is still paused (`debugIsPaused`) — resume or clear that run
     * first, so its intermediate state isn't stranded live in the engine.
     */
    debugCreateInstance(process_id: string, variables_json: string, breakpoints_json: string): string;
    /**
     * Resume a paused debug run until the next breakpoint or completion. No-op if
     * no session is active or it has already finished. Returns the debug state.
     */
    debugResume(): string;
    /**
     * Advance a paused debug run by exactly one step, then pause again (unless
     * that step drained the command). No-op if no session is active or it has
     * already finished. Returns the debug state.
     */
    debugStep(): string;
    /**
     * Parse and deploy a BPMN **or** DMN resource — the one entry point accepts
     * both (issue #1158). A BPMN process resource deploys as before and returns
     * `{ "processIds": [...], "snapshot": {...} }`. A DMN decision resource (no
     * `<process>` element) is routed to decision deployment and returns the same
     * shape as the `deployDecision` method
     * (`{ "decisionRequirementsId": ..., "decisionRequirementsKey": ..., "version": N, "decisions": [{ "decisionId", "decisionName", "decisionKey", "version" }], "snapshot": {...} }`),
     * so a `zeebe:calledDecision` on a business rule task can finally resolve.
     * On failure it throws a JS error carrying the parse/deploy message for the
     * format the document most resembles.
     */
    deploy(xml: string): string;
    /**
     * Parse and deploy a DMN decision-requirements resource explicitly. Registers
     * every `<decision>` it contains (by id) so a business rule task's
     * `zeebe:calledDecision` resolves and `evaluateDecision` can run it. Returns a
     * JSON object
     * `{ "decisionRequirementsId": ..., "decisionRequirementsKey": ..., "version": N, "decisions": [{ "decisionId", "decisionName", "decisionKey", "version" }], "snapshot": {...} }`
     * on success, or throws a JS error carrying the parse/deploy failure message.
     */
    deployDecision(xml: string): string;
    /**
     * Deploy a single `form-js` `.form` resource (the verbatim form-js JSON
     * document). Mirrors the native deploy decomposition, which registers a
     * `.form` resource as a [`Command::DeployForms`] (stored, not executed) so
     * `getFormByKey` can serve its schema. The form-js document's `id` is the
     * form identifier used for versioning/lookup — a body without a non-empty
     * string `id` is a client error (native parity). The deploy resource name is
     * derived as `<id>.form`.
     *
     * Returns a JSON object
     * `{ "formKey": "...", "formId": "...", "version": N, "resourceName": "...", "snapshot": {...} }`
     * on success, or throws a JS error carrying the parse/deploy failure message.
     */
    deployForm(schema: string): string;
    /**
     * Deploy a single generic resource (any deployed file that is not a
     * BPMN/DMN/form — e.g. a Markdown agent prompt) under `resource_name` with
     * the given verbatim `content`. Mirrors the native deploy decomposition,
     * which registers such a file as a [`Command::DeployGenericResources`]
     * (stored, not executed) so `getResourceByKey` can serve its content. The
     * `resource_id` is the filename (`resource_name`), matching Zeebe's default
     * resource transformer.
     *
     * Returns a JSON object
     * `{ "resourceKey": "...", "resourceId": "...", "version": N, "resourceName": "...", "snapshot": {...} }`
     * on success, or throws a JS error carrying the deploy failure message.
     */
    deployResource(resource_name: string, content: string): string;
    /**
     * Evaluate a deployed decision by id against the given variables — the
     * standalone counterpart to a business rule task's in-line evaluation. The
     * decision must already be deployed (via `deploy`/`deployDecision`). Read-only:
     * it evaluates and returns the result without mutating engine state or
     * recording a decision instance. `variables_json` is a JSON object string
     * (`"{}"` / `""` for none). Returns
     * `{ "decisionId": ..., "decisionKey": ..., "output": <value> }` on success,
     * or throws a JS error carrying an "unknown decision" or evaluation-failure
     * message.
     */
    evaluateDecision(decision_id: string, variables_json: string): string;
    /**
     * The full ordered event log emitted so far, as a JSON array of
     * `{ seq, now, type, ...payload }`. Useful for a step-through / trace view.
     */
    events(): string;
    /**
     * Fail a waiting job by key with the given remaining `retries` and message.
     * With no retries left this raises an incident (visible in the snapshot).
     */
    failJob(job_key: string, retries: number, message: string, lease_token?: string | null): string;
    /**
     * Migrate a running process instance to a target process definition (Zeebe
     * "migrate process instance"): re-point every active element instance at the
     * mapped element of `target_process_definition_key` and re-home the instance.
     * `mapping_instructions_json` is a JSON array of
     * `{ sourceElementId: string, targetElementId: string }`. Returns the
     * snapshot.
     */
    migrate(instance_key: string, target_process_definition_key: string, mapping_instructions_json: string): string;
    /**
     * Modify a running process instance (Zeebe "modify process instance"): move
     * tokens by terminating existing element instances and/or activating new
     * ones. `activate_instructions_json` is a JSON array of
     * `{ elementId: string, variables?: object }` (variables are merged into the
     * instance's root scope before the token is placed);
     * `terminate_instructions_json` is a JSON array of element-instance keys,
     * each a decimal string or a `{ elementInstanceKey: string }` object.
     * Activations run at the process root scope. Returns the snapshot.
     */
    modify(instance_key: string, activate_instructions_json: string, terminate_instructions_json: string): string;
    /**
     * Create a fresh, empty simulated engine. The virtual clock starts at 0.
     */
    constructor();
    /**
     * Restore a complete trace returned by `events()`, using the core journal
     * decoder and replay engine. Replaces this simulation only after decoding succeeds.
     */
    replayEvents(events_json: string): string;
    /**
     * Discard all engine state (deployed definitions, instances, jobs, timers,
     * the event log and the virtual clock), returning the engine to the same
     * pristine state as a freshly constructed one. Callers redeploy afterwards
     * to start a clean run — this is what makes a re-run start from zero
     * completed instances rather than accumulating across runs.
     */
    reset(): void;
    /**
     * Resolve an open incident by key, retrying the work that failed (a job
     * incident returns the parked job — which must have retries left — to the
     * activatable pool; a gateway incident re-evaluates; an uncaught-error
     * incident re-creates the service-task job). Returns the snapshot.
     */
    resolveIncident(incident_key: string): string;
    /**
     * Merge variables into a scope (a process-instance key or an element-instance
     * key). When `local` is true the values are written strictly into the target
     * scope; otherwise they propagate upward to the nearest ancestor scope that
     * defines each name (Zeebe `SetVariables` semantics). Returns the snapshot.
     */
    setVariables(scope_key: string, variables_json: string, local: boolean): string;
    /**
     * The current simulation state as a JSON [`Snapshot`].
     */
    snapshot(): string;
    /**
     * Throw a BPMN business error from a waiting job by key. If the job's
     * activity has a matching error boundary/event-subprocess catch it is
     * interrupted and the error-handling path runs; otherwise an incident is
     * raised. The job is activated first if needed, so the UI can throw an
     * error directly from a freshly-created job. Returns the snapshot.
     */
    throwError(job_key: string, error_code: string, error_message: string, lease_token?: string | null): string;
    /**
     * Set the engine clock to a wall-clock instant (ms), then trigger due timers
     * and expire lapsed job locks. The embedded host calls this with `Date.now()`
     * so `engine-core` stays clock-free while running as a real runtime. The
     * clock never moves backwards. Returns the snapshot.
     */
    tickNow(now_ms: number): string;
    /**
     * Clear a user task's assignee. The task must be in the `Created` state.
     * Returns the snapshot.
     */
    unassignUserTask(user_task_key: string): string;
    /**
     * Update an agent with `{ elementInstanceKey, jobKey, jobLeaseToken, status?, history? }`.
     * Configuration and metrics are submitted through history, never top-level fields.
     * Returns `createdHistory`; the engine owns pending/commit/discard semantics.
     */
    updateAgentInstance(agent_instance_key: string, request_json: string): string;
    /**
     * Set a job's remaining retries by key. Used to recover a job parked on a
     * no-retries incident before resolving that incident; does not by itself
     * unblock the job. Returns the snapshot.
     */
    updateRetries(job_key: string, retries: number, lease_token?: string | null): string;
    /**
     * Update an activated job's timeout, optionally checking its opaque lease.
     */
    updateTimeout(job_key: string, timeout_ms: number, lease_token?: string | null): string;
    /**
     * Update a user task's attributes from a JSON changeset object. Recognised
     * keys (all optional): `candidateGroups` / `candidateUsers` (string arrays),
     * `dueDate` / `followUpDate` (ISO-8601 string, or `null`/`""` to clear),
     * `priority` (0..=100). Only present keys are changed. The task must be in
     * the `Created` state. Returns the snapshot.
     */
    updateUserTask(user_task_key: string, changeset_json: string): string;
    /**
     * Whether a debug run is currently paused at a breakpoint.
     */
    readonly debugIsPaused: boolean;
    /**
     * The current virtual clock (milliseconds).
     */
    readonly now: number;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_testengine_free: (a: number, b: number) => void;
    readonly testengine_activateJobs: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => void;
    readonly testengine_advanceTime: (a: number, b: number, c: number) => void;
    readonly testengine_assignUserTask: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly testengine_broadcastSignal: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_cancelInstance: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_completeAgentInstance: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_completeAgentJob: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => void;
    readonly testengine_completeJob: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
    readonly testengine_completeUserTask: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_correlateMessage: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
    readonly testengine_createAgentInstance: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_createInstance: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly testengine_debugClear: (a: number) => void;
    readonly testengine_debugCreateInstance: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
    readonly testengine_debugIsPaused: (a: number) => number;
    readonly testengine_debugResume: (a: number, b: number) => void;
    readonly testengine_debugStep: (a: number, b: number) => void;
    readonly testengine_deploy: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_deployDecision: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_deployForm: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_deployResource: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_evaluateDecision: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_events: (a: number, b: number) => void;
    readonly testengine_failJob: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => void;
    readonly testengine_migrate: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
    readonly testengine_modify: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
    readonly testengine_new: () => number;
    readonly testengine_now: (a: number) => number;
    readonly testengine_replayEvents: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_reset: (a: number) => void;
    readonly testengine_resolveIncident: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_setVariables: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly testengine_snapshot: (a: number, b: number) => void;
    readonly testengine_throwError: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => void;
    readonly testengine_tickNow: (a: number, b: number, c: number) => void;
    readonly testengine_unassignUserTask: (a: number, b: number, c: number, d: number) => void;
    readonly testengine_updateAgentInstance: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly testengine_updateRetries: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly testengine_updateTimeout: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly testengine_updateUserTask: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
    readonly __wbindgen_export: (a: number, b: number) => number;
    readonly __wbindgen_export2: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_export3: (a: number, b: number, c: number) => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
