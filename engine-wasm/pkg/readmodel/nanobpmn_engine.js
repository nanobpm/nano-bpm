/* @ts-self-types="./nanobpmn_engine.d.ts" */

/**
 * A simulated engine instance bound to one modeler session.
 */
export class TestEngine {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        TestEngineFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_testengine_free(ptr, 0);
    }
    /**
     * Activate up to `max_jobs` `Created` jobs of `job_type`, locking them to
     * `worker` until `now + timeout_ms`. Returns a JSON array of activated jobs
     * (key, type, instance/element, retries, variables) for the dispatch loop to
     * hand to worker handlers. The host owns the wall clock via `tickNow`.
     * @param {string} job_type
     * @param {number} max_jobs
     * @param {number} timeout_ms
     * @param {string} worker
     * @param {boolean | null} [with_lease]
     * @returns {string}
     */
    activateJobs(job_type, max_jobs, timeout_ms, worker, with_lease) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_type, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(worker, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_activateJobs(retptr, this.__wbg_ptr, ptr0, len0, max_jobs, timeout_ms, ptr1, len1, isLikeNone(with_lease) ? 0xFFFFFF : with_lease ? 1 : 0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Advance the virtual clock by `by_ms` milliseconds, firing any timers that
     * become due and expiring any lapsed job locks.
     * @param {number} by_ms
     * @returns {string}
     */
    advanceTime(by_ms) {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.testengine_advanceTime(retptr, this.__wbg_ptr, by_ms);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Assign a user task to `assignee`. When `allow_override` is false and the
     * task already has an assignee the command is rejected (it must be
     * unassigned first). Returns the snapshot.
     * @param {string} user_task_key
     * @param {string} assignee
     * @param {boolean} allow_override
     * @returns {string}
     */
    assignUserTask(user_task_key, assignee, allow_override) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(user_task_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(assignee, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_assignUserTask(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, allow_override);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Broadcast a signal by name to **every** open subscription that matches,
     * across all instances, merging `variables_json` into each correlated
     * instance. Signals correlate by name only and are not buffered. Returns
     * the snapshot.
     * @param {string} signal_name
     * @param {string} variables_json
     * @returns {string}
     */
    broadcastSignal(signal_name, variables_json) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(signal_name, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_broadcastSignal(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Cancel (terminate) a running process instance by key. Every token is
     * discarded, pending jobs are canceled, and the instance transitions to
     * `Terminated`. Returns the snapshot.
     * @param {string} instance_key
     * @returns {string}
     */
    cancelInstance(instance_key) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(instance_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_cancelInstance(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Legacy embedded-only completion extension. It neither commits history nor
     * advances BPMN; canonical workers complete their owning job instead.
     * Returns the snapshot.
     * @param {string} agent_instance_key
     * @returns {string}
     */
    completeAgentInstance(agent_instance_key) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(agent_instance_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_completeAgentInstance(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
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
     * @param {string} job_key
     * @param {string} variables_json
     * @param {string} agent_result_json
     * @param {string | null} [lease_token]
     * @returns {string}
     */
    completeAgentJob(job_key, variables_json, agent_result_json, lease_token) {
        let deferred6_0;
        let deferred6_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(agent_result_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len2 = WASM_VECTOR_LEN;
            var ptr3 = isLikeNone(lease_token) ? 0 : passStringToWasm0(lease_token, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            var len3 = WASM_VECTOR_LEN;
            wasm.testengine_completeAgentJob(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr5 = r0;
            var len5 = r1;
            if (r3) {
                ptr5 = 0; len5 = 0;
                throw takeObject(r2);
            }
            deferred6_0 = ptr5;
            deferred6_1 = len5;
            return getStringFromWasm0(ptr5, len5);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred6_0, deferred6_1, 1);
        }
    }
    /**
     * Complete a waiting job by key, merging `variables_json` (a JSON object
     * string) into the instance. The job is activated first if it has not been
     * already, so the UI can complete a freshly-created job directly.
     * @param {string} job_key
     * @param {string} variables_json
     * @param {string | null} [lease_token]
     * @returns {string}
     */
    completeJob(job_key, variables_json, lease_token) {
        let deferred5_0;
        let deferred5_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            var ptr2 = isLikeNone(lease_token) ? 0 : passStringToWasm0(lease_token, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            var len2 = WASM_VECTOR_LEN;
            wasm.testengine_completeJob(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr4 = r0;
            var len4 = r1;
            if (r3) {
                ptr4 = 0; len4 = 0;
                throw takeObject(r2);
            }
            deferred5_0 = ptr4;
            deferred5_1 = len4;
            return getStringFromWasm0(ptr4, len4);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred5_0, deferred5_1, 1);
        }
    }
    /**
     * Complete a waiting user task by key, merging `variables_json` into the
     * instance before the parked token resumes. The task must be in the
     * `Created` state. Returns the snapshot.
     * @param {string} user_task_key
     * @param {string} variables_json
     * @returns {string}
     */
    completeUserTask(user_task_key, variables_json) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(user_task_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_completeUserTask(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Correlate a message to any instance waiting on it: publishes `message_name`
     * with the given `correlation_key` (the value the waiting subscription's
     * `correlationKey` expression resolved to) and merges `variables_json` (a
     * JSON object string) into each correlated instance. This unblocks a message
     * intermediate catch / receive task without an external broker — the
     * in-browser equivalent of an app publishing a message. Returns the snapshot.
     * @param {string} message_name
     * @param {string} correlation_key
     * @param {string} variables_json
     * @returns {string}
     */
    correlateMessage(message_name, correlation_key, variables_json) {
        let deferred5_0;
        let deferred5_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(message_name, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(correlation_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len2 = WASM_VECTOR_LEN;
            wasm.testengine_correlateMessage(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr4 = r0;
            var len4 = r1;
            if (r3) {
                ptr4 = 0; len4 = 0;
                throw takeObject(r2);
            }
            deferred5_0 = ptr4;
            deferred5_1 = len4;
            return getStringFromWasm0(ptr4, len4);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred5_0, deferred5_1, 1);
        }
    }
    /**
     * Create an agent using `{ elementInstanceKey, jobKey, jobLeaseToken, history }`.
     * History must establish its CONFIGURATION. Returns the canonical creation
     * result with `agentInstanceKey` and positionally correlated `createdHistory`.
     * @param {string} request_json
     * @returns {string}
     */
    createAgentInstance(request_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(request_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_createAgentInstance(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Start a new instance of `process_id`, seeding it with the given variables
     * (a JSON object string; pass `"{}"` or `""` for none). `version` selects a
     * specific process **version** number (Zeebe by-id semantics); pass
     * `undefined`/`null` (or a non-positive value) for the latest version.
     * Returns the post-run [`Snapshot`] with a top-level `created` field holding
     * the new instance key.
     * @param {string} process_id
     * @param {string} variables_json
     * @param {number | null} [version]
     * @returns {string}
     */
    createInstance(process_id, variables_json, version) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(process_id, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_createInstance(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, isLikeNone(version) ? Number.MAX_SAFE_INTEGER : (version) >> 0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Stop debugging, keeping the state the run produced. If the run is paused
     * mid-command, that in-flight command is first **finished normally** (its
     * breakpoints are cleared and it is resumed once) so the engine lands on the
     * same run-to-completion (RTC) quiescent state a plain command would produce
     * — never a partial, non-RTC intermediate one that later mutators could
     * build on (the hole [`TestEngine::guard_paused`] exists to prevent). To
     * discard the run's state entirely instead, use [`TestEngine::reset`].
     */
    debugClear() {
        wasm.testengine_debugClear(this.__wbg_ptr);
    }
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
     * @param {string} process_id
     * @param {string} variables_json
     * @param {string} breakpoints_json
     * @returns {string}
     */
    debugCreateInstance(process_id, variables_json, breakpoints_json) {
        let deferred5_0;
        let deferred5_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(process_id, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(breakpoints_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len2 = WASM_VECTOR_LEN;
            wasm.testengine_debugCreateInstance(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr4 = r0;
            var len4 = r1;
            if (r3) {
                ptr4 = 0; len4 = 0;
                throw takeObject(r2);
            }
            deferred5_0 = ptr4;
            deferred5_1 = len4;
            return getStringFromWasm0(ptr4, len4);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred5_0, deferred5_1, 1);
        }
    }
    /**
     * Whether a debug run is currently paused at a breakpoint.
     * @returns {boolean}
     */
    get debugIsPaused() {
        const ret = wasm.testengine_debugIsPaused(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Resume a paused debug run until the next breakpoint or completion. No-op if
     * no session is active or it has already finished. Returns the debug state.
     * @returns {string}
     */
    debugResume() {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.testengine_debugResume(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Advance a paused debug run by exactly one step, then pause again (unless
     * that step drained the command). No-op if no session is active or it has
     * already finished. Returns the debug state.
     * @returns {string}
     */
    debugStep() {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.testengine_debugStep(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred2_0, deferred2_1, 1);
        }
    }
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
     * @param {string} xml
     * @returns {string}
     */
    deploy(xml) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(xml, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_deploy(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Parse and deploy a DMN decision-requirements resource explicitly. Registers
     * every `<decision>` it contains (by id) so a business rule task's
     * `zeebe:calledDecision` resolves and `evaluateDecision` can run it. Returns a
     * JSON object
     * `{ "decisionRequirementsId": ..., "decisionRequirementsKey": ..., "version": N, "decisions": [{ "decisionId", "decisionName", "decisionKey", "version" }], "snapshot": {...} }`
     * on success, or throws a JS error carrying the parse/deploy failure message.
     * @param {string} xml
     * @returns {string}
     */
    deployDecision(xml) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(xml, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_deployDecision(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
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
     * @param {string} schema
     * @returns {string}
     */
    deployForm(schema) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(schema, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_deployForm(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
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
     * @param {string} resource_name
     * @param {string} content
     * @returns {string}
     */
    deployResource(resource_name, content) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(resource_name, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(content, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_deployResource(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Evaluate a deployed decision by id against the given variables — the
     * standalone counterpart to a business rule task's in-line evaluation (the
     * engine's `EvaluateDecision` command, exactly as the gateway runs it). The
     * decision must already be deployed (via `deploy`/`deployDecision`). The
     * evaluation is recorded as a decision instance with its own minted
     * `decisionEvaluationKey` and no process instance — a failed one too
     * (Zeebe parity, #1292). `variables_json` is a JSON object string (`"{}"` /
     * `""` for none). Returns
     * `{ "decisionId": ..., "decisionKey": ..., "decisionEvaluationKey": ..., "output": <value> }`
     * on success, or throws a JS error carrying an "unknown decision" or
     * evaluation-failure message.
     * @param {string} decision_id
     * @param {string} variables_json
     * @returns {string}
     */
    evaluateDecision(decision_id, variables_json) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(decision_id, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_evaluateDecision(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * The full ordered event log emitted so far, as a JSON array of
     * `{ seq, now, type, ...payload }`. Useful for a step-through / trace view.
     * @returns {string}
     */
    events() {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.testengine_events(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Fail a waiting job by key with the given remaining `retries` and message.
     * With no retries left this raises an incident (visible in the snapshot).
     * @param {string} job_key
     * @param {number} retries
     * @param {string} message
     * @param {string | null} [lease_token]
     * @returns {string}
     */
    failJob(job_key, retries, message, lease_token) {
        let deferred5_0;
        let deferred5_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(message, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            var ptr2 = isLikeNone(lease_token) ? 0 : passStringToWasm0(lease_token, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            var len2 = WASM_VECTOR_LEN;
            wasm.testengine_failJob(retptr, this.__wbg_ptr, ptr0, len0, retries, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr4 = r0;
            var len4 = r1;
            if (r3) {
                ptr4 = 0; len4 = 0;
                throw takeObject(r2);
            }
            deferred5_0 = ptr4;
            deferred5_1 = len4;
            return getStringFromWasm0(ptr4, len4);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred5_0, deferred5_1, 1);
        }
    }
    /**
     * The latest deployed form for `form_key`, as a `FormResult` JSON object
     * (`{ tenantId, formId, schema, version, formKey }`), or JSON `null` when no
     * form with that key exists. Mirrors `GET /forms/{formKey}`.
     * @param {string} form_key
     * @returns {string}
     */
    getFormByKey(form_key) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(form_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_getFormByKey(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * The generic resource for `resource_key`, as a `ResourceResult` JSON object,
     * or JSON `null` when no resource with that key exists. Mirrors
     * `GET /resources/{resourceKey}`.
     * @param {string} resource_key
     * @returns {string}
     */
    getResourceByKey(resource_key) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(resource_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_getResourceByKey(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Migrate a running process instance to a target process definition (Zeebe
     * "migrate process instance"): re-point every active element instance at the
     * mapped element of `target_process_definition_key` and re-home the instance.
     * `mapping_instructions_json` is a JSON array of
     * `{ sourceElementId: string, targetElementId: string }`. Returns the
     * snapshot.
     * @param {string} instance_key
     * @param {string} target_process_definition_key
     * @param {string} mapping_instructions_json
     * @returns {string}
     */
    migrate(instance_key, target_process_definition_key, mapping_instructions_json) {
        let deferred5_0;
        let deferred5_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(instance_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(target_process_definition_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(mapping_instructions_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len2 = WASM_VECTOR_LEN;
            wasm.testengine_migrate(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr4 = r0;
            var len4 = r1;
            if (r3) {
                ptr4 = 0; len4 = 0;
                throw takeObject(r2);
            }
            deferred5_0 = ptr4;
            deferred5_1 = len4;
            return getStringFromWasm0(ptr4, len4);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred5_0, deferred5_1, 1);
        }
    }
    /**
     * Modify a running process instance (Zeebe "modify process instance"): move
     * tokens by terminating existing element instances and/or activating new
     * ones. `activate_instructions_json` is a JSON array of
     * `{ elementId: string, variables?: object }` (variables are merged into the
     * instance's root scope before the token is placed);
     * `terminate_instructions_json` is a JSON array of element-instance keys,
     * each a decimal string or a `{ elementInstanceKey: string }` object.
     * Activations run at the process root scope. Returns the snapshot.
     * @param {string} instance_key
     * @param {string} activate_instructions_json
     * @param {string} terminate_instructions_json
     * @returns {string}
     */
    modify(instance_key, activate_instructions_json, terminate_instructions_json) {
        let deferred5_0;
        let deferred5_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(instance_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(activate_instructions_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(terminate_instructions_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len2 = WASM_VECTOR_LEN;
            wasm.testengine_modify(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr4 = r0;
            var len4 = r1;
            if (r3) {
                ptr4 = 0; len4 = 0;
                throw takeObject(r2);
            }
            deferred5_0 = ptr4;
            deferred5_1 = len4;
            return getStringFromWasm0(ptr4, len4);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred5_0, deferred5_1, 1);
        }
    }
    /**
     * Create a fresh, empty simulated engine. The virtual clock starts at 0.
     */
    constructor() {
        const ret = wasm.testengine_new();
        this.__wbg_ptr = ret;
        TestEngineFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * The current virtual clock (milliseconds).
     * @returns {number}
     */
    get now() {
        const ret = wasm.testengine_now(this.__wbg_ptr);
        return ret;
    }
    /**
     * Restore a complete trace returned by `events()`, using the core journal
     * decoder and replay engine. Replaces this simulation only after decoding succeeds.
     * @param {string} events_json
     * @returns {string}
     */
    replayEvents(events_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(events_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_replayEvents(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Discard all engine state (deployed definitions, instances, jobs, timers,
     * the event log and the virtual clock), returning the engine to the same
     * pristine state as a freshly constructed one. Callers redeploy afterwards
     * to start a clean run — this is what makes a re-run start from zero
     * completed instances rather than accumulating across runs.
     */
    reset() {
        wasm.testengine_reset(this.__wbg_ptr);
    }
    /**
     * Resolve an open incident by key, retrying the work that failed (a job
     * incident returns the parked job — which must have retries left — to the
     * activatable pool; a gateway incident re-evaluates; an uncaught-error
     * incident re-creates the service-task job). Returns the snapshot.
     * @param {string} incident_key
     * @returns {string}
     */
    resolveIncident(incident_key) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(incident_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_resolveIncident(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * The AgentHistory turns for `agent_instance_key`, as an
     * `AgentInstanceHistorySearchQueryResult` JSON object (`{ items, page }`),
     * in the engine's canonical `(loopIteration, producedAt, historyItemKey)`
     * order. `filter_json` is `{ commitStatus? }` where `commitStatus` is a REST
     * spelling or array of them; **omitting it defaults to COMMITTED only**
     * (PENDING/DISCARDED surface only when asked for explicitly). Accepts the
     * canonical REST envelope `{ "filter": { commitStatus? } }` as well as the
     * top-level shorthand. Mirrors
     * `POST /agent-instances/{agentInstanceKey}/history/search`.
     * @param {string} agent_instance_key
     * @param {string} filter_json
     * @returns {string}
     */
    searchAgentInstanceHistory(agent_instance_key, filter_json) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(agent_instance_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(filter_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_searchAgentInstanceHistory(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * The AgentInstances matching an optional filter, as an
     * `AgentInstanceSearchQueryResult` JSON object (`{ items, page }`). The
     * filter is `{ agentInstanceKey?, agentDefinitionKey?, processInstanceKey?,
     * rootProcessInstanceKey?, processDefinitionKey?, status?, elementId?,
     * tenantId? }` (`status` a REST spelling, e.g. `"INITIALIZING"`); an
     * empty/absent filter returns every instance. Accepts the canonical REST
     * envelope `{ "filter": { … } }` as well as the top-level shorthand.
     * Mirrors `POST /agent-instances/search`.
     * @param {string} filter_json
     * @returns {string}
     */
    searchAgentInstances(filter_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(filter_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_searchAgentInstances(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * All process instances, as a `ProcessInstanceSearchQueryResult` JSON object
     * (`{ items: [...], page: {...} }`). The request body is shape-validated like
     * `POST /process-instances/search`, but filter/sort/page fields are not yet
     * honoured — every instance is returned. Do not rely on gateway-side
     * filtering through this method yet.
     * @param {string} filter_json
     * @returns {string}
     */
    searchProcessInstances(filter_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(filter_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_searchProcessInstances(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * The user tasks matching an optional `{ state? }` filter, as a
     * `UserTaskSearchQueryResult` JSON object (`{ items: [...], page: {...} }`).
     * The `state` filter (e.g. `"CREATED"`) is honoured through the read model,
     * not hardcoded. Mirrors `POST /user-tasks/search`.
     * @param {string} filter_json
     * @returns {string}
     */
    searchUserTasks(filter_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(filter_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_searchUserTasks(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * All variables, as a `VariableSearchQueryResult` JSON object
     * (`{ items: [...], page: {...} }`). The request body is shape-validated like
     * `POST /variables/search`, but filter/sort/page fields are not yet honoured —
     * every variable is returned. Values longer than `VARIABLE_VALUE_PREVIEW_LEN`
     * are truncated with `isTruncated: true`; shorter values pass through intact
     * with `isTruncated: false`. There is no `truncateValues` opt-out yet.
     * @param {string} filter_json
     * @returns {string}
     */
    searchVariables(filter_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(filter_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_searchVariables(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Merge variables into a scope (a process-instance key or an element-instance
     * key). When `local` is true the values are written strictly into the target
     * scope; otherwise they propagate upward to the nearest ancestor scope that
     * defines each name (Zeebe `SetVariables` semantics). Returns the snapshot.
     * @param {string} scope_key
     * @param {string} variables_json
     * @param {boolean} local
     * @returns {string}
     */
    setVariables(scope_key, variables_json, local) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(scope_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(variables_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_setVariables(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, local);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * The current simulation state as a JSON [`Snapshot`].
     * @returns {string}
     */
    snapshot() {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.testengine_snapshot(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Throw a BPMN business error from a waiting job by key. If the job's
     * activity has a matching error boundary/event-subprocess catch it is
     * interrupted and the error-handling path runs; otherwise an incident is
     * raised. The job is activated first if needed, so the UI can throw an
     * error directly from a freshly-created job. Returns the snapshot.
     * @param {string} job_key
     * @param {string} error_code
     * @param {string} error_message
     * @param {string | null} [lease_token]
     * @returns {string}
     */
    throwError(job_key, error_code, error_message, lease_token) {
        let deferred6_0;
        let deferred6_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(error_code, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(error_message, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len2 = WASM_VECTOR_LEN;
            var ptr3 = isLikeNone(lease_token) ? 0 : passStringToWasm0(lease_token, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            var len3 = WASM_VECTOR_LEN;
            wasm.testengine_throwError(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr5 = r0;
            var len5 = r1;
            if (r3) {
                ptr5 = 0; len5 = 0;
                throw takeObject(r2);
            }
            deferred6_0 = ptr5;
            deferred6_1 = len5;
            return getStringFromWasm0(ptr5, len5);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred6_0, deferred6_1, 1);
        }
    }
    /**
     * Set the engine clock to a wall-clock instant (ms), then trigger due timers
     * and expire lapsed job locks. The embedded host calls this with `Date.now()`
     * so `engine-core` stays clock-free while running as a real runtime. The
     * clock never moves backwards. Returns the snapshot.
     * @param {number} now_ms
     * @returns {string}
     */
    tickNow(now_ms) {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.testengine_tickNow(retptr, this.__wbg_ptr, now_ms);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Clear a user task's assignee. The task must be in the `Created` state.
     * Returns the snapshot.
     * @param {string} user_task_key
     * @returns {string}
     */
    unassignUserTask(user_task_key) {
        let deferred3_0;
        let deferred3_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(user_task_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            wasm.testengine_unassignUserTask(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr2 = r0;
            var len2 = r1;
            if (r3) {
                ptr2 = 0; len2 = 0;
                throw takeObject(r2);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Update an agent with `{ elementInstanceKey, jobKey, jobLeaseToken, status?, history? }`.
     * Configuration and metrics are submitted through history, never top-level fields.
     * Returns `createdHistory`; the engine owns pending/commit/discard semantics.
     * @param {string} agent_instance_key
     * @param {string} request_json
     * @returns {string}
     */
    updateAgentInstance(agent_instance_key, request_json) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(agent_instance_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(request_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_updateAgentInstance(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Set a job's remaining retries by key. Used to recover a job parked on a
     * no-retries incident before resolving that incident; does not by itself
     * unblock the job. Returns the snapshot.
     * @param {string} job_key
     * @param {number} retries
     * @param {string | null} [lease_token]
     * @returns {string}
     */
    updateRetries(job_key, retries, lease_token) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            var ptr1 = isLikeNone(lease_token) ? 0 : passStringToWasm0(lease_token, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            var len1 = WASM_VECTOR_LEN;
            wasm.testengine_updateRetries(retptr, this.__wbg_ptr, ptr0, len0, retries, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Update an activated job's timeout, optionally checking its opaque lease.
     * @param {string} job_key
     * @param {number} timeout_ms
     * @param {string | null} [lease_token]
     * @returns {string}
     */
    updateTimeout(job_key, timeout_ms, lease_token) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(job_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            var ptr1 = isLikeNone(lease_token) ? 0 : passStringToWasm0(lease_token, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            var len1 = WASM_VECTOR_LEN;
            wasm.testengine_updateTimeout(retptr, this.__wbg_ptr, ptr0, len0, timeout_ms, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
    /**
     * Update a user task's attributes from a JSON changeset object. Recognised
     * keys (all optional): `candidateGroups` / `candidateUsers` (string arrays),
     * `dueDate` / `followUpDate` (ISO-8601 string, or `null`/`""` to clear),
     * `priority` (0..=100). Only present keys are changed. The task must be in
     * the `Created` state. Returns the snapshot.
     * @param {string} user_task_key
     * @param {string} changeset_json
     * @returns {string}
     */
    updateUserTask(user_task_key, changeset_json) {
        let deferred4_0;
        let deferred4_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(user_task_key, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(changeset_json, wasm.__wbindgen_export2, wasm.__wbindgen_export3);
            const len1 = WASM_VECTOR_LEN;
            wasm.testengine_updateUserTask(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
            var ptr3 = r0;
            var len3 = r1;
            if (r3) {
                ptr3 = 0; len3 = 0;
                throw takeObject(r2);
            }
            deferred4_0 = ptr3;
            deferred4_1 = len3;
            return getStringFromWasm0(ptr3, len3);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred4_0, deferred4_1, 1);
        }
    }
}
if (Symbol.dispose) TestEngine.prototype[Symbol.dispose] = TestEngine.prototype.free;
function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg___wbindgen_throw_bb96b2010945f0bc: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbg_error_c101d80a635fe63e: function(arg0, arg1) {
            console.error(getStringFromWasm0(arg0, arg1));
        },
        __wbg_getDate_b0ac858991e80b2e: function(arg0) {
            const ret = getObject(arg0).getDate();
            return ret;
        },
        __wbg_getDay_d65e35285ba489ae: function(arg0) {
            const ret = getObject(arg0).getDay();
            return ret;
        },
        __wbg_getFullYear_d2ecaf3ecbfaddf9: function(arg0) {
            const ret = getObject(arg0).getFullYear();
            return ret;
        },
        __wbg_getHours_521ab343a38cb962: function(arg0) {
            const ret = getObject(arg0).getHours();
            return ret;
        },
        __wbg_getMinutes_27257e5640ea2e7c: function(arg0) {
            const ret = getObject(arg0).getMinutes();
            return ret;
        },
        __wbg_getMonth_9f2f20dab3150834: function(arg0) {
            const ret = getObject(arg0).getMonth();
            return ret;
        },
        __wbg_getRandomValues_896855b3a5f89999: function() { return handleError(function (arg0, arg1) {
            globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
        }, arguments); },
        __wbg_getSeconds_0071b90906b363f2: function(arg0) {
            const ret = getObject(arg0).getSeconds();
            return ret;
        },
        __wbg_getTime_63fb0332e6c4ec17: function(arg0) {
            const ret = getObject(arg0).getTime();
            return ret;
        },
        __wbg_getTimezoneOffset_4baa793e0d3962a8: function(arg0) {
            const ret = getObject(arg0).getTimezoneOffset();
            return ret;
        },
        __wbg_new_0_f117d868b403dc07: function() {
            const ret = new Date();
            return addHeapObject(ret);
        },
        __wbg_new_f9d6489212f3b2b3: function(arg0) {
            const ret = new Date(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_new_with_year_month_day_cc812ef210488bf4: function(arg0, arg1, arg2) {
            const ret = new Date(arg0 >>> 0, arg1, arg2);
            return addHeapObject(ret);
        },
        __wbg_now_8b265300afd5f2b9: function() {
            const ret = Date.now();
            return ret;
        },
        __wbg_random_b0d98802be10ff20: function() {
            const ret = Math.random();
            return ret;
        },
        __wbindgen_cast_0000000000000001: function(arg0) {
            // Cast intrinsic for `F64 -> Externref`.
            const ret = arg0;
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return addHeapObject(ret);
        },
        __wbindgen_object_drop_ref: function(arg0) {
            takeObject(arg0);
        },
    };
    return {
        __proto__: null,
        "./nanobpmn_engine_bg.js": import0,
    };
}

const TestEngineFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_testengine_free(ptr, 1));

function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
}

function dropObject(idx) {
    if (idx < 1028) return;
    heap[idx] = heap_next;
    heap_next = idx;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function getStringFromWasm0(ptr, len) {
    return decodeText(ptr >>> 0, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function getObject(idx) { return heap[idx]; }

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        wasm.__wbindgen_export(addHeapObject(e));
    }
}

let heap = new Array(1024).fill(undefined);
heap.push(undefined, null, true, false);

let heap_next = heap.length;

function isLikeNone(x) {
    return x === undefined || x === null;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

let wasmModule, wasmInstance, wasm;
function __wbg_finalize_init(instance, module) {
    wasmInstance = instance;
    wasm = instance.exports;
    wasmModule = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (!module.ok) {
            throw new Error(`failed to fetch Wasm: ${module.status} ${module.statusText} fetching '${module.url}'`);
        }

        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('nanobpmn_engine_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };

// --- nanobpm: use-after-free DX guard (scripts/inject-free-guard.mjs) ---
// wasm-bindgen throws the opaque "null pointer passed to rust" when a method is
// called on a freed handle (__wbg_ptr === 0). Re-describe that as a clear
// use-after-free so a host lifecycle bug is diagnosable at the call site. The
// call is still refused — this only changes the message, not the behaviour.
for (const __name of Object.getOwnPropertyNames(TestEngine.prototype)) {
    if (__name === "constructor" || __name === "free" || __name === "__destroy_into_raw") continue;
    const __desc = Object.getOwnPropertyDescriptor(TestEngine.prototype, __name);
    if (!__desc || typeof __desc.value !== "function") continue;
    const __orig = __desc.value;
    const __guarded = {
        [__name](...__args) {
            if (this.__wbg_ptr === 0) {
                throw new Error("TestEngine used after free(): '" + __name + "' called on a released engine handle");
            }
            return __orig.apply(this, __args);
        },
    }[__name];
    Object.defineProperty(TestEngine.prototype, __name, { ...__desc, value: __guarded });
}
// --- end nanobpm use-after-free DX guard ---
