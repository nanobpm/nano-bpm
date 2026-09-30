//! In-browser test-execution wrapper around `nanobpmn-engine-core`.
//!
//! This crate exists purely to give the web modeler a way to *run* a BPMN
//! process before deploying it. It compiles `engine-core` (zero-dependency,
//! deterministic, clock-injected) to `wasm32-unknown-unknown` and exposes a
//! tiny JSON-string API over [`wasm_bindgen`].
//!
//! Design constraints:
//!   * `engine-core` stays lean and untouched — all the binding glue lives here.
//!   * The engine never reads a wall clock; we drive a *virtual* clock so the
//!     simulation is fully deterministic and timers can be "fast-forwarded".
//!   * Execution mutators return a [`Snapshot`] for rendering; agent CREATE/UPDATE
//!     return their canonical REST result with `createdHistory`.

use std::collections::HashMap;

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, form_id_of, ActivateElementInstruction, AdHocActivateElement, AdHocJobResult,
    AgentDefinition, AgentHistoryContent, AgentHistoryContentType, AgentHistoryRole,
    AgentHistoryTurn, AgentInstanceLimits, AgentInstanceMetricsDelta, AgentInstanceStatus,
    AgentTool, BreakCondition, Command, DebugSession, Engine, Event, FormResource, GenericResource,
    IncidentKind, IncidentState, JobActivationOptions, JobState, MessageSubscriptionKind,
    MessageSubscriptionState, ProcessInstanceState, TimerState, UserTaskChangeset, UserTaskState,
    Value,
};
/// The shared read-model surface, compiled with its in-memory wasm SQLite
/// backend. Behind the off-by-default `read-model` feature so the baseline engine
/// links none of the read-model/SQLite deps and keeps its lean baseline size;
/// everything it touches is `#[cfg(feature = "read-model")]`.
#[cfg(feature = "read-model")]
use nanobpmn_read_model::{
    AgentHistoryFilter, AgentHistoryRow, AgentInstanceFilter, AgentInstanceRow, FormRow,
    ProcessInstanceRow, ReadStore, ResourceRow, RootResolver, UserTaskRow, VariableRow,
    VARIABLE_VALUE_PREVIEW_LEN,
};
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// `console.error` binding used to surface (never abort on) best-effort read-model
/// projection failures, so a broken projection is debuggable instead of silently
/// serving stale/empty read results. Zero new crate deps — a direct JS binding.
///
/// The JS import only exists on the `wasm32` target; the host build (used by
/// `cargo test --features read-model`) has no JS runtime, so calling the import
/// there would abort. A native `eprintln!` fallback keeps the same
/// never-abort contract off-wasm.
#[cfg(all(feature = "read-model", target_arch = "wasm32"))]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error(msg: &str);
}

/// Native fallback for the `console.error` binding on non-wasm targets (host
/// tests), where the JS import is absent. Logs to stderr so the never-abort
/// contract holds off-wasm too.
#[cfg(all(feature = "read-model", not(target_arch = "wasm32")))]
fn console_error(msg: &str) {
    eprintln!("{msg}");
}

/// Compile-time parity gate: an exhaustive match over `engine-core`'s `Command`
/// surface that fails the build when a new engine capability is added without a
/// conscious decision about whether to expose it here. See the module docs.
///
/// Its twin `classify_read` (same module) is the read-surface analogue: an
/// exhaustive match over `engine-core`'s `ReadQuery` enum — the single canonical
/// read surface — that fails the build when a newly served read is recorded
/// without a conscious `TestEngine` decision.
mod surface_parity;

/// A simulated engine instance bound to one modeler session.
#[wasm_bindgen]
pub struct TestEngine {
    engine: Engine,
    now: u64,
    seq: u64,
    log: Vec<LogEntry>,
    /// Cumulative history aggregates folded incrementally as events are applied,
    /// so `snapshot` stays O(live state) instead of re-scanning the whole event
    /// log on every (frequent) Play poll.
    history: HistoryAggregates,
    /// The in-flight debug run, if the host is stepping a command (see the
    /// `debug*` methods). `None` when no debug session is active. Held out of the
    /// engine so the engine's production path is untouched.
    debug: Option<DebugSession>,
    /// How many of the active [`DebugSession`]'s events have already been folded
    /// into `log`, so each `debugResume`/`debugStep` mirrors only the new tail.
    debug_folded: usize,
    /// The virtual clock captured when the active debug run was started. A single
    /// command is applied at a single `now` (see [`TestEngine::apply`]), so every
    /// event folded from the session is stamped with this value rather than the
    /// live `self.now` at fold time — keeping the mirrored log identical to a
    /// plain command even if the clock were to move between steps.
    debug_now: u64,
    /// The in-memory read model (CQRS query side), fed the same events as `log`
    /// after every applied command so the REST read methods answer from the SAME
    /// projection the gateway serves. Behind the `read-model` feature; absent
    /// (and unlinked) in the baseline engine.
    #[cfg(feature = "read-model")]
    read_model: ReadStore,
}

/// The log-derived, monotonically-growing parts of a snapshot, accumulated once
/// per emitted event rather than recomputed per poll: cumulative per-element
/// completion counts, the set of traversed sequence flows, and the evaluated
/// decision instances.
#[derive(Default)]
struct HistoryAggregates {
    completed_counts: HashMap<String, u64>,
    taken_flows: std::collections::BTreeSet<(String, String)>,
    decisions: Vec<DecisionInstanceDto>,
}

struct LogEntry {
    seq: u64,
    now: u64,
    event: Event,
}

#[wasm_bindgen]
impl TestEngine {
    /// Create a fresh, empty simulated engine. The virtual clock starts at 0.
    #[wasm_bindgen(constructor)]
    pub fn new() -> TestEngine {
        TestEngine {
            engine: Engine::new(),
            now: 0,
            seq: 0,
            log: Vec::new(),
            history: HistoryAggregates::default(),
            debug: None,
            debug_folded: 0,
            debug_now: 0,
            #[cfg(feature = "read-model")]
            read_model: open_read_model(),
        }
    }

    /// The current virtual clock (milliseconds).
    #[wasm_bindgen(getter)]
    pub fn now(&self) -> f64 {
        self.now as f64
    }

    /// Discard all engine state (deployed definitions, instances, jobs, timers,
    /// the event log and the virtual clock), returning the engine to the same
    /// pristine state as a freshly constructed one. Callers redeploy afterwards
    /// to start a clean run — this is what makes a re-run start from zero
    /// completed instances rather than accumulating across runs.
    pub fn reset(&mut self) {
        self.engine = Engine::new();
        self.now = 0;
        self.seq = 0;
        self.log.clear();
        self.history = HistoryAggregates::default();
        self.debug = None;
        self.debug_folded = 0;
        self.debug_now = 0;
        #[cfg(feature = "read-model")]
        {
            self.read_model = open_read_model();
        }
    }

    /// Parse and deploy a BPMN **or** DMN resource — the one entry point accepts
    /// both (issue #1158). A BPMN process resource deploys as before and returns
    /// `{ "processIds": [...], "snapshot": {...} }`. A DMN decision resource (no
    /// `<process>` element) is routed to decision deployment and returns the same
    /// shape as the `deployDecision` method
    /// (`{ "decisionRequirementsId": ..., "decisionRequirementsKey": ..., "version": N, "decisions": [{ "decisionId", "decisionName", "decisionKey", "version" }], "snapshot": {...} }`),
    /// so a `zeebe:calledDecision` on a business rule task can finally resolve.
    /// On failure it throws a JS error carrying the parse/deploy message for the
    /// format the document most resembles.
    pub fn deploy(&mut self, xml: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        // Try BPMN first (the common case). A DMN document carries no BPMN
        // `<process>`, so it fails the BPMN parse and falls back to the DMN
        // parser — no fragile up-front content sniffing on the happy path.
        match parse_bpmn(xml) {
            Ok(defs) => {
                let ids: Vec<String> = defs.iter().map(|d| d.id.clone()).collect();
                self.apply(Command::DeployResources(defs))
                    .map_err(|e| js_err(&format!("deploy error: {e}")))?;
                let snapshot = self.snapshot_value(None);
                to_json(&serde_json::json!({
                    "processIds": ids,
                    "snapshot": snapshot,
                }))
            }
            Err(bpmn_err) => match nanobpmn_engine_core::dmn::parse_dmn(xml) {
                Ok(drg) => to_json(&self.deploy_decision_drg(drg)?),
                // Neither parser accepted it: report the error for the format the
                // document most resembles (a DMN-namespaced doc gets the DMN
                // error), so the message is actionable rather than misleading.
                Err(dmn_err) => {
                    if resource_looks_like_dmn(xml) {
                        Err(js_err(&format!("parse error: {dmn_err}")))
                    } else {
                        Err(js_err(&format!("parse error: {bpmn_err}")))
                    }
                }
            },
        }
    }

    /// Parse and deploy a DMN decision-requirements resource explicitly. Registers
    /// every `<decision>` it contains (by id) so a business rule task's
    /// `zeebe:calledDecision` resolves and `evaluateDecision` can run it. Returns a
    /// JSON object
    /// `{ "decisionRequirementsId": ..., "decisionRequirementsKey": ..., "version": N, "decisions": [{ "decisionId", "decisionName", "decisionKey", "version" }], "snapshot": {...} }`
    /// on success, or throws a JS error carrying the parse/deploy failure message.
    #[wasm_bindgen(js_name = deployDecision)]
    pub fn deploy_decision(&mut self, xml: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let drg = nanobpmn_engine_core::dmn::parse_dmn(xml)
            .map_err(|e| js_err(&format!("parse error: {e}")))?;
        to_json(&self.deploy_decision_drg(drg)?)
    }

    /// Register a parsed DRG as a decision deployment and build the response
    /// object. Shared by [`Self::deploy`] (DMN auto-route) and
    /// [`Self::deploy_decision`]. Resolves the assigned identities from post-apply
    /// state rather than the emitted events, so an idempotent redeploy of the
    /// identical latest DRG (which emits no `DecisionDeployed` event) still returns
    /// the existing identity (native parity — the server builds responses from
    /// resolved state, not events).
    fn deploy_decision_drg(
        &mut self,
        drg: nanobpmn_engine_core::dmn::DecisionRequirementsGraph,
    ) -> Result<serde_json::Value, JsValue> {
        let drg_id = drg.id.clone();
        self.apply(Command::DeployDecisionRequirements(vec![drg]))
            .map_err(|e| js_err(&format!("deploy error: {e}")))?;
        let (drg_key, version, decisions) = {
            let state = self.engine.state();
            let deployed = state.decision_requirements.get(&drg_id).ok_or_else(|| {
                js_err("deploy error: decision requirements not found after deploy")
            })?;
            let decisions: Vec<serde_json::Value> = deployed
                .drg
                .decisions
                .iter()
                .map(|d| {
                    // Every <decision> in a successfully-deployed DRG must be
                    // registered; a missing identity is a deploy bug, so fail
                    // loudly rather than silently emitting null key/version.
                    let identity = state.decisions.get(&d.id).ok_or_else(|| {
                        js_err(&format!(
                            "deploy error: decision '{}' was not registered after deploy",
                            d.id
                        ))
                    })?;
                    Ok(serde_json::json!({
                        "decisionId": d.id,
                        "decisionName": d.name,
                        "decisionKey": identity.key.to_string(),
                        "version": identity.version,
                    }))
                })
                .collect::<Result<Vec<_>, JsValue>>()?;
            (deployed.key, deployed.version, decisions)
        };
        let snapshot = self.snapshot_value(None);
        Ok(serde_json::json!({
            "decisionRequirementsId": drg_id,
            "decisionRequirementsKey": drg_key.to_string(),
            "version": version,
            "decisions": decisions,
            "snapshot": snapshot,
        }))
    }

    /// Evaluate a deployed decision by id against the given variables — the
    /// standalone counterpart to a business rule task's in-line evaluation. The
    /// decision must already be deployed (via `deploy`/`deployDecision`). Read-only:
    /// it evaluates and returns the result without mutating engine state or
    /// recording a decision instance. `variables_json` is a JSON object string
    /// (`"{}"` / `""` for none). Returns
    /// `{ "decisionId": ..., "decisionKey": ..., "output": <value> }` on success,
    /// or throws a JS error carrying an "unknown decision" or evaluation-failure
    /// message.
    #[wasm_bindgen(js_name = evaluateDecision)]
    pub fn evaluate_decision(
        &mut self,
        decision_id: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let variables = parse_vars(variables_json)?;
        let deployed = self
            .engine
            .state()
            .decisions
            .get(decision_id)
            .ok_or_else(|| {
                js_err(&format!(
                    "evaluate error: no deployed decision with id '{decision_id}'"
                ))
            })?;
        let result = nanobpmn_engine_core::dmn::evaluate(&deployed.drg, decision_id, &variables);
        if let Some(failure) = &result.failure {
            return Err(js_err(&format!(
                "evaluate error: failed to evaluate decision '{}': {}",
                failure.failed_decision_id, failure.message
            )));
        }
        to_json(&serde_json::json!({
            "decisionId": decision_id,
            "decisionKey": deployed.key.to_string(),
            "output": value_to_json(&result.decision_output),
        }))
    }

    /// Deploy a single `form-js` `.form` resource (the verbatim form-js JSON
    /// document). Mirrors the native deploy decomposition, which registers a
    /// `.form` resource as a [`Command::DeployForms`] (stored, not executed) so
    /// `getFormByKey` can serve its schema. The form-js document's `id` is the
    /// form identifier used for versioning/lookup — a body without a non-empty
    /// string `id` is a client error (native parity). The deploy resource name is
    /// derived as `<id>.form`.
    ///
    /// Returns a JSON object
    /// `{ "formKey": "...", "formId": "...", "version": N, "resourceName": "...", "snapshot": {...} }`
    /// on success, or throws a JS error carrying the parse/deploy failure message.
    #[wasm_bindgen(js_name = deployForm)]
    pub fn deploy_form(&mut self, schema: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let id = form_id_of(schema).ok_or_else(|| {
            js_err(
                "invalid form: not a valid form-js document (expected a JSON object \
                 with a non-empty string \"id\")",
            )
        })?;
        let resource_name = format!("{id}.form");
        self.apply(Command::DeployForms(vec![FormResource {
            id: id.clone(),
            resource_name: resource_name.clone(),
            schema: schema.to_string(),
        }]))
        .map_err(|e| js_err(&format!("deploy error: {e}")))?;
        // Resolve from post-apply state rather than the emitted events: an
        // idempotent redeploy of the identical latest form emits no
        // `FormDeployed` event, but the deploy still succeeds and must return
        // the existing identity (native parity — the server builds responses
        // from resolved state, not events).
        let (form_key, version) = self
            .engine
            .state()
            .forms
            .get(&id)
            .map(|f| (f.key, f.version))
            .ok_or_else(|| js_err("deploy error: form not found after deploy"))?;
        let snapshot = self.snapshot_value(None);
        to_json(&serde_json::json!({
            "formKey": form_key.to_string(),
            "formId": id,
            "version": version,
            "resourceName": resource_name,
            "snapshot": snapshot,
        }))
    }

    /// Deploy a single generic resource (any deployed file that is not a
    /// BPMN/DMN/form — e.g. a Markdown agent prompt) under `resource_name` with
    /// the given verbatim `content`. Mirrors the native deploy decomposition,
    /// which registers such a file as a [`Command::DeployGenericResources`]
    /// (stored, not executed) so `getResourceByKey` can serve its content. The
    /// `resource_id` is the filename (`resource_name`), matching Zeebe's default
    /// resource transformer.
    ///
    /// Returns a JSON object
    /// `{ "resourceKey": "...", "resourceId": "...", "version": N, "resourceName": "...", "snapshot": {...} }`
    /// on success, or throws a JS error carrying the deploy failure message.
    #[wasm_bindgen(js_name = deployResource)]
    pub fn deploy_resource(
        &mut self,
        resource_name: &str,
        content: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        self.apply(Command::DeployGenericResources(vec![GenericResource {
            resource_id: resource_name.to_string(),
            resource_name: resource_name.to_string(),
            content: content.to_string(),
        }]))
        .map_err(|e| js_err(&format!("deploy error: {e}")))?;
        // Resolve from post-apply state rather than the emitted events: an
        // idempotent redeploy of the identical latest resource emits no
        // `GenericResourceDeployed` event, but the deploy still succeeds as a
        // no-op and must return the existing identity (native parity — the
        // server builds responses from resolved state, not events).
        let (resource_key, version) = self
            .engine
            .state()
            .resources
            .get(resource_name)
            .map(|r| (r.key, r.version))
            .ok_or_else(|| js_err("deploy error: resource not found after deploy"))?;
        let snapshot = self.snapshot_value(None);
        to_json(&serde_json::json!({
            "resourceKey": resource_key.to_string(),
            "resourceId": resource_name,
            "version": version,
            "resourceName": resource_name,
            "snapshot": snapshot,
        }))
    }

    /// Start a new instance of `process_id`, seeding it with the given variables
    /// (a JSON object string; pass `"{}"` or `""` for none). `version` selects a
    /// specific process **version** number (Zeebe by-id semantics); pass
    /// `undefined`/`null` (or a non-positive value) for the latest version.
    /// Returns the post-run [`Snapshot`] with a top-level `created` field holding
    /// the new instance key.
    #[wasm_bindgen(js_name = createInstance)]
    pub fn create_instance(
        &mut self,
        process_id: &str,
        variables_json: &str,
        version: Option<i32>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let variables = parse_vars(variables_json)?;
        let events = self
            .apply(Command::CreateInstance {
                process_id: process_id.to_string(),
                variables,
                tags: Vec::new(),
                business_id: None,
                process_definition_key: None,
                version,
            })
            .map_err(|e| js_err(&format!("create error: {e}")))?;
        let created = events.iter().find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        });
        to_json(&self.snapshot_value(created))
    }

    /// Begin a **debug** run of a `CreateInstance` command: deploy first (as
    /// usual), then call this to start the instance under the stepping executor,
    /// pausing at the first breakpoint (or running to completion if none match).
    ///
    /// `breakpoints_json` is a JSON array of `{ kind, id? }`:
    /// ```json
    /// [{ "kind": "elementActivated", "id": "Task_Charge" },
    ///  { "kind": "elementCompleted", "id": "Gateway_1" },
    ///  { "kind": "processCompleted" },
    ///  { "kind": "everyStep" }]
    /// ```
    /// Returns the debug state JSON (see [`TestEngine::debug_state`]). Starting a
    /// new debug run replaces any previous *finished* session; it is rejected
    /// while a run is still paused (`debugIsPaused`) — resume or clear that run
    /// first, so its intermediate state isn't stranded live in the engine.
    #[wasm_bindgen(js_name = debugCreateInstance)]
    pub fn debug_create_instance(
        &mut self,
        process_id: &str,
        variables_json: &str,
        breakpoints_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let variables = parse_vars(variables_json)?;
        let breakpoints = parse_breakpoints(breakpoints_json)?;
        // Start fresh: nothing of this session has been folded into `log` yet.
        self.debug = None;
        self.debug_folded = 0;
        // A command is applied at a single virtual clock; capture it now so every
        // folded event carries the command's `now`, not the clock at fold time.
        self.debug_now = self.now;
        let session = self
            .engine
            .debug_command_at(
                Command::CreateInstance {
                    process_id: process_id.to_string(),
                    variables,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: None,
                    version: None,
                },
                self.now,
                breakpoints,
            )
            .map_err(|e| js_err(&format!("debug create error: {e}")))?;
        self.debug = Some(session);
        self.fold_debug_delta();
        to_json(&self.debug_state())
    }

    /// Resume a paused debug run until the next breakpoint or completion. No-op if
    /// no session is active or it has already finished. Returns the debug state.
    #[wasm_bindgen(js_name = debugResume)]
    pub fn debug_resume(&mut self) -> Result<String, JsValue> {
        if let Some(mut session) = self.debug.take() {
            self.engine.debug_resume(&mut session);
            self.debug = Some(session);
            self.fold_debug_delta();
        }
        to_json(&self.debug_state())
    }

    /// Advance a paused debug run by exactly one step, then pause again (unless
    /// that step drained the command). No-op if no session is active or it has
    /// already finished. Returns the debug state.
    #[wasm_bindgen(js_name = debugStep)]
    pub fn debug_step(&mut self) -> Result<String, JsValue> {
        if let Some(mut session) = self.debug.take() {
            self.engine.debug_step(&mut session);
            self.debug = Some(session);
            self.fold_debug_delta();
        }
        to_json(&self.debug_state())
    }

    /// Whether a debug run is currently paused at a breakpoint.
    #[wasm_bindgen(getter, js_name = debugIsPaused)]
    pub fn debug_is_paused(&self) -> bool {
        self.debug.as_ref().is_some_and(DebugSession::is_paused)
    }

    /// Stop debugging, keeping the state the run produced. If the run is paused
    /// mid-command, that in-flight command is first **finished normally** (its
    /// breakpoints are cleared and it is resumed once) so the engine lands on the
    /// same run-to-completion (RTC) quiescent state a plain command would produce
    /// — never a partial, non-RTC intermediate one that later mutators could
    /// build on (the hole [`TestEngine::guard_paused`] exists to prevent). To
    /// discard the run's state entirely instead, use [`TestEngine::reset`].
    #[wasm_bindgen(js_name = debugClear)]
    pub fn debug_clear(&mut self) {
        if let Some(mut session) = self.debug.take() {
            if session.is_paused() {
                // Drain the in-flight command to its natural RTC quiescence.
                session.set_breakpoints(Vec::new());
                self.engine.debug_resume(&mut session);
                self.debug = Some(session);
                self.fold_debug_delta();
            }
        }
        self.debug = None;
        self.debug_folded = 0;
        self.debug_now = 0;
    }

    /// Complete a waiting job by key, merging `variables_json` (a JSON object
    /// string) into the instance. The job is activated first if it has not been
    /// already, so the UI can complete a freshly-created job directly.
    #[wasm_bindgen(js_name = completeJob)]
    pub fn complete_job(
        &mut self,
        job_key: &str,
        variables_json: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(job_key)?;
        let variables = parse_vars(variables_json)?;
        if lease_token.is_none() {
            self.ensure_activated(key)?;
        }
        self.apply(Command::CompleteJob {
            job_key: key,
            lease_token,
            variables,
            adhoc_result: None,
            task_listener_result: None,
        })
        .map_err(|e| js_err(&format!("complete error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Complete an ad-hoc sub-process **agent** job — the container's
    /// JOB_WORKER job (Camunda's agentic `aiagent-job-worker`) — carrying the
    /// agent's activate-element instructions so the engine runs the selected
    /// inner "tools" this turn (ADR 0023 seam 2/3; Camunda `JobResult` /
    /// `activateElements[]`). This is the browser seam that the plain
    /// `completeJob` deliberately omits (it always sends
    /// `adhoc_result: None`).
    ///
    /// `agent_result_json` shape (camelCase, mirroring Camunda's agentic
    /// `JobResult`):
    /// ```json
    /// { "activateElements": [{ "elementId": "toolA", "variables": { "q": 1 } }],
    ///   "completionConditionFulfilled": false,
    ///   "cancelRemainingInstances": false }
    /// ```
    /// Empty/whitespace ⇒ a no-op result, so the container completes this turn
    /// (no tool activated). `variables_json` merges instance variables exactly
    /// like `completeJob` — e.g. the agent's final
    /// decision when it signals `completionConditionFulfilled`.
    #[wasm_bindgen(js_name = completeAgentJob)]
    pub fn complete_agent_job(
        &mut self,
        job_key: &str,
        variables_json: &str,
        agent_result_json: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(job_key)?;
        let variables = parse_vars(variables_json)?;
        let adhoc_result = parse_adhoc_result(agent_result_json)?;
        if lease_token.is_none() {
            self.ensure_activated(key)?;
        }
        self.apply(Command::CompleteJob {
            job_key: key,
            lease_token,
            variables,
            adhoc_result: Some(adhoc_result),
            task_listener_result: None,
        })
        .map_err(|e| js_err(&format!("complete agent job error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Fail a waiting job by key with the given remaining `retries` and message.
    /// With no retries left this raises an incident (visible in the snapshot).
    #[wasm_bindgen(js_name = failJob)]
    pub fn fail_job(
        &mut self,
        job_key: &str,
        retries: i32,
        message: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(job_key)?;
        if lease_token.is_none() {
            self.ensure_activated(key)?;
        }
        self.apply(Command::FailJob {
            job_key: key,
            lease_token,
            retries,
            error_message: message.to_string(),
        })
        .map_err(|e| js_err(&format!("fail error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Correlate a message to any instance waiting on it: publishes `message_name`
    /// with the given `correlation_key` (the value the waiting subscription's
    /// `correlationKey` expression resolved to) and merges `variables_json` (a
    /// JSON object string) into each correlated instance. This unblocks a message
    /// intermediate catch / receive task without an external broker — the
    /// in-browser equivalent of an app publishing a message. Returns the snapshot.
    #[wasm_bindgen(js_name = correlateMessage)]
    pub fn correlate_message(
        &mut self,
        message_name: &str,
        correlation_key: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let variables = parse_vars(variables_json)?;
        self.apply(Command::CorrelateMessage {
            message_name: message_name.to_string(),
            correlation_key: correlation_key.to_string(),
            variables,
        })
        .map_err(|e| js_err(&format!("correlate error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Advance the virtual clock by `by_ms` milliseconds, firing any timers that
    /// become due and expiring any lapsed job locks.
    #[wasm_bindgen(js_name = advanceTime)]
    pub fn advance_time(&mut self, by_ms: f64) -> Result<String, JsValue> {
        self.guard_paused()?;
        let delta = if by_ms.is_finite() && by_ms > 0.0 {
            by_ms as u64
        } else {
            0
        };
        self.now = self.now.saturating_add(delta);
        let now = self.now;
        self.apply(Command::TriggerTimers { now })
            .map_err(|e| js_err(&format!("timer error: {e}")))?;
        self.apply(Command::ExpireJobs { now })
            .map_err(|e| js_err(&format!("expire error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Set the engine clock to a wall-clock instant (ms), then trigger due timers
    /// and expire lapsed job locks. The embedded host calls this with `Date.now()`
    /// so `engine-core` stays clock-free while running as a real runtime. The
    /// clock never moves backwards. Returns the snapshot.
    #[wasm_bindgen(js_name = tickNow)]
    pub fn tick_now(&mut self, now_ms: f64) -> Result<String, JsValue> {
        self.guard_paused()?;
        let now = if now_ms.is_finite() && now_ms > 0.0 {
            now_ms as u64
        } else {
            0
        };
        if now > self.now {
            self.now = now;
        }
        let now = self.now;
        self.apply(Command::TriggerTimers { now })
            .map_err(|e| js_err(&format!("timer error: {e}")))?;
        self.apply(Command::ExpireJobs { now })
            .map_err(|e| js_err(&format!("expire error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Activate up to `max_jobs` `Created` jobs of `job_type`, locking them to
    /// `worker` until `now + timeout_ms`. Returns a JSON array of activated jobs
    /// (key, type, instance/element, retries, variables) for the dispatch loop to
    /// hand to worker handlers. The host owns the wall clock via `tickNow`.
    #[wasm_bindgen(js_name = activateJobs)]
    pub fn activate_jobs(
        &mut self,
        job_type: &str,
        max_jobs: u32,
        timeout_ms: f64,
        worker: &str,
        with_lease: Option<bool>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let now = self.now;
        let timeout = if timeout_ms.is_finite() && timeout_ms > 0.0 {
            timeout_ms as u64
        } else {
            30_000
        };
        let events = self
            .apply(Command::activate_jobs_with_options(
                job_type,
                worker,
                (max_jobs.max(1)) as usize,
                timeout,
                now,
                JobActivationOptions {
                    with_lease: with_lease.unwrap_or(false),
                    ..Default::default()
                },
            ))
            .map_err(|e| js_err(&format!("activate error: {e}")))?;
        // Derive the returned job keys from the `JobActivated` events *this*
        // call emitted — not by scanning all `Activated` jobs, which would also
        // surface jobs locked by earlier `activateJobs` calls and could exceed
        // `max_jobs`. Each is then projected through the engine's canonical
        // `ActivatedJob` snapshot so the console TestEngine surfaces the same
        // Zeebe field set as the server/FFI paths (custom headers,
        // process-definition identity, tags, priority).
        let keys: Vec<_> = events
            .iter()
            .filter_map(|ev| match ev {
                Event::JobActivated { job_key, .. } => Some(*job_key),
                _ => None,
            })
            .collect();
        let out: Vec<serde_json::Value> = keys
            .iter()
            .filter_map(|k| self.engine.activated_job(*k))
            .map(|j| {
                serde_json::json!({
                    "key": j.key.to_string(),
                    "type": j.job_type,
                    "instanceKey": j.instance_key.to_string(),
                    "elementInstanceKey": j.element_instance_key.to_string(),
                    "elementId": j.element_id,
                    "bpmnProcessId": j.bpmn_process_id,
                    "processDefinitionKey": j.process_definition_key.to_string(),
                    "processDefinitionVersion": j.process_definition_version,
                    "worker": j.worker,
                    "retries": j.retries,
                    "deadline": j.deadline,
                    "priority": j.priority,
                    "customHeaders": j.custom_headers,
                    "tags": j.tags,
                    "businessId": j.business_id,
                    "variables": vars_to_json(&j.variables),
                    "jobLeaseToken": j.lease_token,
                })
            })
            .collect();
        to_json(&serde_json::Value::Array(out))
    }

    /// Throw a BPMN business error from a waiting job by key. If the job's
    /// activity has a matching error boundary/event-subprocess catch it is
    /// interrupted and the error-handling path runs; otherwise an incident is
    /// raised. The job is activated first if needed, so the UI can throw an
    /// error directly from a freshly-created job. Returns the snapshot.
    #[wasm_bindgen(js_name = throwError)]
    pub fn throw_error(
        &mut self,
        job_key: &str,
        error_code: &str,
        error_message: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(job_key)?;
        if lease_token.is_none() {
            self.ensure_activated(key)?;
        }
        self.apply(Command::ThrowJobError {
            job_key: key,
            error_code: error_code.to_string(),
            error_message: error_message.to_string(),
            variables: HashMap::new(),
            lease_token,
        })
        .map_err(|e| js_err(&format!("throw error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Set a job's remaining retries by key. Used to recover a job parked on a
    /// no-retries incident before resolving that incident; does not by itself
    /// unblock the job. Returns the snapshot.
    #[wasm_bindgen(js_name = updateRetries)]
    pub fn update_retries(
        &mut self,
        job_key: &str,
        retries: i32,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(job_key)?;
        self.apply(Command::UpdateJobRetries {
            job_key: key,
            retries,
            lease_token,
            operation_reference: None,
        })
        .map_err(|e| js_err(&format!("update retries error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Update an activated job's timeout, optionally checking its opaque lease.
    #[wasm_bindgen(js_name = updateTimeout)]
    pub fn update_timeout(
        &mut self,
        job_key: &str,
        timeout_ms: f64,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let timeout = update_timeout_value(timeout_ms).map_err(|e| js_err(&e))?;
        self.apply(Command::UpdateJobTimeout {
            job_key: parse_key(job_key)?,
            timeout,
            lease_token,
            operation_reference: None,
        })
        .map_err(|e| js_err(&format!("update timeout error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Resolve an open incident by key, retrying the work that failed (a job
    /// incident returns the parked job — which must have retries left — to the
    /// activatable pool; a gateway incident re-evaluates; an uncaught-error
    /// incident re-creates the service-task job). Returns the snapshot.
    #[wasm_bindgen(js_name = resolveIncident)]
    pub fn resolve_incident(&mut self, incident_key: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(incident_key)?;
        self.apply(Command::ResolveIncident {
            incident_key: key,
            operation_reference: None,
        })
        .map_err(|e| js_err(&format!("resolve incident error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Merge variables into a scope (a process-instance key or an element-instance
    /// key). When `local` is true the values are written strictly into the target
    /// scope; otherwise they propagate upward to the nearest ancestor scope that
    /// defines each name (Zeebe `SetVariables` semantics). Returns the snapshot.
    #[wasm_bindgen(js_name = setVariables)]
    pub fn set_variables(
        &mut self,
        scope_key: &str,
        variables_json: &str,
        local: bool,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(scope_key)?;
        let variables = parse_vars(variables_json)?;
        self.apply(Command::SetVariables {
            scope_key: key,
            variables,
            local,
        })
        .map_err(|e| js_err(&format!("set variables error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Broadcast a signal by name to **every** open subscription that matches,
    /// across all instances, merging `variables_json` into each correlated
    /// instance. Signals correlate by name only and are not buffered. Returns
    /// the snapshot.
    #[wasm_bindgen(js_name = broadcastSignal)]
    pub fn broadcast_signal(
        &mut self,
        signal_name: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let variables = parse_vars(variables_json)?;
        self.apply(Command::BroadcastSignal {
            signal_name: signal_name.to_string(),
            variables,
        })
        .map_err(|e| js_err(&format!("broadcast signal error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Cancel (terminate) a running process instance by key. Every token is
    /// discarded, pending jobs are canceled, and the instance transitions to
    /// `Terminated`. Returns the snapshot.
    #[wasm_bindgen(js_name = cancelInstance)]
    pub fn cancel_instance(&mut self, instance_key: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(instance_key)?;
        self.apply(Command::CancelInstance { instance_key: key })
            .map_err(|e| js_err(&format!("cancel instance error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Modify a running process instance (Zeebe "modify process instance"): move
    /// tokens by terminating existing element instances and/or activating new
    /// ones. `activate_instructions_json` is a JSON array of
    /// `{ elementId: string, variables?: object }` (variables are merged into the
    /// instance's root scope before the token is placed);
    /// `terminate_instructions_json` is a JSON array of element-instance keys,
    /// each a decimal string or a `{ elementInstanceKey: string }` object.
    /// Activations run at the process root scope. Returns the snapshot.
    #[wasm_bindgen(js_name = modify)]
    pub fn modify(
        &mut self,
        instance_key: &str,
        activate_instructions_json: &str,
        terminate_instructions_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(instance_key)?;
        let activate_instructions = parse_activate_instructions(activate_instructions_json)?;
        let terminate_instructions = parse_terminate_instructions(terminate_instructions_json)?;
        self.apply(Command::ModifyInstance {
            instance_key: key,
            activate_instructions,
            terminate_instructions,
        })
        .map_err(|e| js_err(&format!("modify instance error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Migrate a running process instance to a target process definition (Zeebe
    /// "migrate process instance"): re-point every active element instance at the
    /// mapped element of `target_process_definition_key` and re-home the instance.
    /// `mapping_instructions_json` is a JSON array of
    /// `{ sourceElementId: string, targetElementId: string }`. Returns the
    /// snapshot.
    #[wasm_bindgen(js_name = migrate)]
    pub fn migrate(
        &mut self,
        instance_key: &str,
        target_process_definition_key: &str,
        mapping_instructions_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(instance_key)?;
        let target_key = parse_key(target_process_definition_key)?;
        let mapping_instructions = parse_mapping_instructions(mapping_instructions_json)?;
        self.apply(Command::MigrateInstance {
            instance_key: key,
            target_process_definition_key: target_key,
            mapping_instructions,
        })
        .map_err(|e| js_err(&format!("migrate instance error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Complete a waiting user task by key, merging `variables_json` into the
    /// instance before the parked token resumes. The task must be in the
    /// `Created` state. Returns the snapshot.
    #[wasm_bindgen(js_name = completeUserTask)]
    pub fn complete_user_task(
        &mut self,
        user_task_key: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(user_task_key)?;
        let variables = parse_vars(variables_json)?;
        self.apply(Command::CompleteUserTask {
            user_task_key: key,
            variables,
        })
        .map_err(|e| js_err(&format!("complete user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Assign a user task to `assignee`. When `allow_override` is false and the
    /// task already has an assignee the command is rejected (it must be
    /// unassigned first). Returns the snapshot.
    #[wasm_bindgen(js_name = assignUserTask)]
    pub fn assign_user_task(
        &mut self,
        user_task_key: &str,
        assignee: &str,
        allow_override: bool,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(user_task_key)?;
        self.apply(Command::AssignUserTask {
            user_task_key: key,
            assignee: assignee.to_string(),
            allow_override,
        })
        .map_err(|e| js_err(&format!("assign user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Clear a user task's assignee. The task must be in the `Created` state.
    /// Returns the snapshot.
    #[wasm_bindgen(js_name = unassignUserTask)]
    pub fn unassign_user_task(&mut self, user_task_key: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(user_task_key)?;
        self.apply(Command::UnassignUserTask { user_task_key: key })
            .map_err(|e| js_err(&format!("unassign user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// Update a user task's attributes from a JSON changeset object. Recognised
    /// keys (all optional): `candidateGroups` / `candidateUsers` (string arrays),
    /// `dueDate` / `followUpDate` (ISO-8601 string, or `null`/`""` to clear),
    /// `priority` (0..=100). Only present keys are changed. The task must be in
    /// the `Created` state. Returns the snapshot.
    #[wasm_bindgen(js_name = updateUserTask)]
    pub fn update_user_task(
        &mut self,
        user_task_key: &str,
        changeset_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(user_task_key)?;
        let changeset = parse_user_task_changeset(changeset_json)?;
        self.apply(Command::UpdateUserTask {
            user_task_key: key,
            changeset,
        })
        .map_err(|e| js_err(&format!("update user task error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }

    /// The current simulation state as a JSON [`Snapshot`].
    pub fn snapshot(&self) -> Result<String, JsValue> {
        to_json(&self.snapshot_value(None))
    }

    /// The full ordered event log emitted so far, as a JSON array of
    /// `{ seq, now, type, ...payload }`. Useful for a step-through / trace view.
    pub fn events(&self) -> Result<String, JsValue> {
        let arr: Vec<serde_json::Value> = self
            .log
            .iter()
            .map(|e| {
                let mut v = serde_json::to_value(&e.event).unwrap_or(serde_json::Value::Null);
                // Event serializes as an externally-tagged object `{ "Type": {..} }`;
                // flatten it to `{ "type": "Type", seq, now, ...fields }`.
                let (ty, body) = match v {
                    serde_json::Value::Object(ref mut m) if m.len() == 1 => {
                        let k = m.keys().next().cloned().unwrap();
                        let b = m.remove(&k).unwrap();
                        (k, b)
                    }
                    serde_json::Value::String(s) => (s, serde_json::Value::Null),
                    other => ("Unknown".to_string(), other),
                };
                let mut out = serde_json::Map::new();
                out.insert("seq".into(), e.seq.into());
                out.insert("now".into(), e.now.into());
                out.insert("type".into(), serde_json::Value::String(ty));
                if let serde_json::Value::Object(fields) = body {
                    for (k, val) in fields {
                        out.insert(k, val);
                    }
                }
                serde_json::Value::Object(out)
            })
            .collect();
        to_json(&arr)
    }

    /// Restore a complete trace returned by `events()`, using the core journal
    /// decoder and replay engine. Replaces this simulation only after decoding succeeds.
    #[wasm_bindgen(js_name = replayEvents)]
    pub fn replay_events(&mut self, events_json: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let records = decode_trace(events_json).map_err(|e| js_err(&e))?;
        let events: Vec<_> = records.iter().map(|record| record.event.clone()).collect();
        let mut restored = Self::new();
        restored.engine = Engine::replay(events);
        for record in records {
            restored.now = record.now;
            restored.seq = record.seq;
            restored.fold_history(&record.event);
            #[cfg(feature = "read-model")]
            restored.project_read_model(std::slice::from_ref(&record.event));
            restored.log.push(record);
        }
        *self = restored;
        to_json(&self.snapshot_value(None))
    }

    // --- Engine-native AgentInstance drivers (Camunda 8.10 parity, Stage 3) ---
    //
    // These expose the AgentInstance CREATE/UPDATE/COMPLETE lifecycle commands
    // through the wasm `TestEngine`. Each accepts a small camelCase JSON request
    // mirroring the Camunda v2 `/agent-instances` wire shape (status/role are the
    // REST spellings, e.g. `"THINKING"`, `"ASSISTANT"`), applies the command,
    // and returns its canonical result. The projected instance/history state is read
    // back through `searchAgentInstances` / `searchAgentInstanceHistory` (the
    // read-model surface), which serialise the same REST shapes.

    /// Create an agent using `{ elementInstanceKey, jobKey, jobLeaseToken, history }`.
    /// History must establish its CONFIGURATION. Returns the canonical creation
    /// result with `agentInstanceKey` and positionally correlated `createdHistory`.
    #[wasm_bindgen(js_name = createAgentInstance)]
    pub fn create_agent_instance(&mut self, request_json: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let req: CreateAgentInstanceReq = serde_json::from_str(request_json)
            .map_err(|e| js_err(&format!("createAgentInstance: invalid request JSON: {e}")))?;
        let element_instance_key = parse_key(&req.element_instance_key)?;
        let job_key = parse_key(&req.job_key)?;
        let job_lease = reconcile_agent_lease_token(req.job_lease_token, req.job_lease)
            .map_err(|e| js_err(e.message()))?;
        let history = agent_turns_from(req.history, job_key, &job_lease)?;
        let history_ids: Vec<_> = history
            .iter()
            .map(|t| t.history_item_id.clone().unwrap_or_default())
            .collect();
        let events = self
            .apply(Command::CreateAgentInstance {
                element_instance_key,
                job_key,
                job_lease,
                definition: AgentDefinition::default(),
                limits: None,
                history,
            })
            .map_err(|e| js_err(&format!("create agent instance error: {e}")))?;
        let key = events
            .iter()
            .find_map(|event| match event {
                Event::AgentInstanceCreated { agent_instance, .. } => {
                    Some(agent_instance.agent_instance_key)
                }
                _ => None,
            })
            .ok_or_else(|| js_err("create agent instance produced no record"))?;
        to_json(&serde_json::json!({
            "agentInstanceKey": key.to_string(),
            "createdHistory": self.created_history_result(key, &history_ids, &events)?,
        }))
    }

    /// Update an agent with `{ elementInstanceKey, jobKey, jobLeaseToken, status?, history? }`.
    /// Configuration and metrics are submitted through history, never top-level fields.
    /// Returns `createdHistory`; the engine owns pending/commit/discard semantics.
    #[wasm_bindgen(js_name = updateAgentInstance)]
    pub fn update_agent_instance(
        &mut self,
        agent_instance_key: &str,
        request_json: &str,
    ) -> Result<String, JsValue> {
        self.guard_paused()?;
        let req: UpdateAgentInstanceReq = serde_json::from_str(request_json)
            .map_err(|e| js_err(&format!("updateAgentInstance: invalid request JSON: {e}")))?;
        let agent_instance_key = parse_key(agent_instance_key)?;
        let element_instance_key = parse_key(&req.element_instance_key)?;
        let (element_id, process_instance_key) = self
            .engine
            .agent_instance_ownership(agent_instance_key)
            .ok_or_else(|| js_err("no such agent instance"))?;
        reject_non_updatable_status(req.status.as_deref()).map_err(|m| js_err(&m))?;
        let status = match req.status.as_deref() {
            Some(s) => Some(parse_agent_status(s)?),
            None => None,
        };
        let job_key = parse_key(&req.job_key)?;
        let job_lease = reconcile_agent_lease_token(req.job_lease_token, req.job_lease)
            .map_err(|e| js_err(e.message()))?;
        let history = agent_turns_from(req.history.unwrap_or_default(), job_key, &job_lease)?;
        let history_ids: Vec<_> = history
            .iter()
            .map(|t| t.history_item_id.clone().unwrap_or_default())
            .collect();
        let events = self
            .apply(Command::UpdateAgentInstance {
                agent_instance_key,
                element_instance_key,
                element_id,
                process_instance_key,
                job_key,
                job_lease,
                status,
                metrics: AgentInstanceMetricsDelta::default(),
                tools: None,
                history,
            })
            .map_err(|e| js_err(&format!("update agent instance error: {e}")))?;
        to_json(&serde_json::json!({
            "createdHistory": self.created_history_result(agent_instance_key, &history_ids, &events)?,
        }))
    }

    /// Legacy embedded-only completion extension. It neither commits history nor
    /// advances BPMN; canonical workers complete their owning job instead.
    /// Returns the snapshot.
    #[wasm_bindgen(js_name = completeAgentInstance)]
    pub fn complete_agent_instance(&mut self, agent_instance_key: &str) -> Result<String, JsValue> {
        self.guard_paused()?;
        let key = parse_key(agent_instance_key)?;
        self.apply(Command::CompleteAgentInstance {
            agent_instance_key: key,
        })
        .map_err(|e| js_err(&format!("complete agent instance error: {e}")))?;
        to_json(&self.snapshot_value(None))
    }
}

/// The REST read channel: the gateway's read-side queries served from the SAME
/// shared `nanobpmn-read-model` projection the server uses, so downstream
/// consumers (e.g. the console/testkit) can drop their bespoke shadow stores and
/// read forms, user tasks, process instances, resources and variables straight
/// off the in-browser test engine. Every method delegates to the in-memory
/// [`ReadStore`] and serialises the read-model `*Row` types into the same JSON
/// shapes the Camunda v2 REST surface returns.
///
/// Behind the off-by-default `read-model` feature — the baseline engine build
/// links none of the read-model/SQLite deps and keeps its lean baseline size.
#[cfg(feature = "read-model")]
#[wasm_bindgen]
impl TestEngine {
    /// The latest deployed form for `form_key`, as a `FormResult` JSON object
    /// (`{ tenantId, formId, schema, version, formKey }`), or JSON `null` when no
    /// form with that key exists. Mirrors `GET /forms/{formKey}`.
    #[wasm_bindgen(js_name = getFormByKey)]
    pub fn get_form_by_key(&self, form_key: &str) -> Result<String, JsValue> {
        let key = parse_key(form_key)?;
        match self.read_model.form_by_key(key) {
            Some(row) => to_json(&form_result(&row)),
            None => Ok("null".to_string()),
        }
    }

    /// The user tasks matching an optional `{ state? }` filter, as a
    /// `UserTaskSearchQueryResult` JSON object (`{ items: [...], page: {...} }`).
    /// The `state` filter (e.g. `"CREATED"`) is honoured through the read model,
    /// not hardcoded. Mirrors `POST /user-tasks/search`.
    #[wasm_bindgen(js_name = searchUserTasks)]
    pub fn search_user_tasks(&self, filter_json: &str) -> Result<String, JsValue> {
        let want = match parse_state_filter(filter_json, "state")? {
            Some(s) => Some(user_task_state_from_rest(&s).ok_or_else(|| {
                js_err(&format!(
                    "filter `state` must be one of {}; got {s:?}",
                    user_task_state_spellings()
                ))
            })?),
            None => None,
        };
        // One resolver spans the whole search so tasks sharing a parent chain
        // walk it once, mirroring `search_process_instances` (issues #977/#1095).
        let roots = RootResolver::new(|k| self.read_model.process_instance(k));
        let items: Vec<serde_json::Value> = self
            .read_model
            .user_tasks()
            .iter()
            .filter(|row| match want {
                Some(w) => row.state == w,
                None => true,
            })
            .map(|row| {
                // `businessId` is the owning instance's (as the gateway's
                // `business_id_of`), not inherited from the root.
                let business_id = self
                    .read_model
                    .process_instance(row.instance_key)
                    .and_then(|pi| pi.business_id);
                user_task_result(row, &roots, business_id)
            })
            .collect();
        to_json(&search_result(items))
    }

    /// All process instances, as a `ProcessInstanceSearchQueryResult` JSON object
    /// (`{ items: [...], page: {...} }`). The request body is shape-validated like
    /// `POST /process-instances/search`, but filter/sort/page fields are not yet
    /// honoured — every instance is returned. Do not rely on gateway-side
    /// filtering through this method yet.
    #[wasm_bindgen(js_name = searchProcessInstances)]
    pub fn search_process_instances(&self, filter_json: &str) -> Result<String, JsValue> {
        validate_search_filter_body(filter_json)?;
        // One resolver spans the whole search so instances sharing a parent chain
        // walk it once (issue #977 review: avoid O(rows × chain-depth) root walks).
        let roots = RootResolver::new(|k| self.read_model.process_instance(k));
        let items: Vec<serde_json::Value> = self
            .read_model
            .process_instances()
            .iter()
            .map(|row| process_instance_result(row, &roots))
            .collect();
        to_json(&search_result(items))
    }

    /// The generic resource for `resource_key`, as a `ResourceResult` JSON object,
    /// or JSON `null` when no resource with that key exists. Mirrors
    /// `GET /resources/{resourceKey}`.
    #[wasm_bindgen(js_name = getResourceByKey)]
    pub fn get_resource_by_key(&self, resource_key: &str) -> Result<String, JsValue> {
        let key = parse_key(resource_key)?;
        match self.read_model.resource_by_key(key) {
            Some(row) => to_json(&resource_result(&row)),
            None => Ok("null".to_string()),
        }
    }

    /// All variables, as a `VariableSearchQueryResult` JSON object
    /// (`{ items: [...], page: {...} }`). The request body is shape-validated like
    /// `POST /variables/search`, but filter/sort/page fields are not yet honoured —
    /// every variable is returned. Values longer than `VARIABLE_VALUE_PREVIEW_LEN`
    /// are truncated with `isTruncated: true`; shorter values pass through intact
    /// with `isTruncated: false`. There is no `truncateValues` opt-out yet.
    #[wasm_bindgen(js_name = searchVariables)]
    pub fn search_variables(&self, filter_json: &str) -> Result<String, JsValue> {
        validate_search_filter_body(filter_json)?;
        let items: Vec<serde_json::Value> = self
            .read_model
            .variables()
            .iter()
            .map(variable_search_result)
            .collect();
        to_json(&search_result(items))
    }

    /// The AgentInstances matching an optional filter, as an
    /// `AgentInstanceSearchQueryResult` JSON object (`{ items, page }`). The
    /// filter is `{ agentInstanceKey?, agentDefinitionKey?, processInstanceKey?,
    /// rootProcessInstanceKey?, processDefinitionKey?, status?, elementId?,
    /// tenantId? }` (`status` a REST spelling, e.g. `"INITIALIZING"`); an
    /// empty/absent filter returns every instance. Accepts the canonical REST
    /// envelope `{ "filter": { … } }` as well as the top-level shorthand.
    /// Mirrors `POST /agent-instances/search`.
    #[wasm_bindgen(js_name = searchAgentInstances)]
    pub fn search_agent_instances(&self, filter_json: &str) -> Result<String, JsValue> {
        let filter = parse_agent_instance_filter(filter_json)?;
        let items: Vec<serde_json::Value> = self
            .read_model
            .try_agent_instances(&filter, None)
            .map_err(|e| js_err(&e.to_string()))?
            .iter()
            .map(agent_instance_result)
            .collect::<Result<_, _>>()
            .map_err(|e| js_err(&e))?;
        to_json(&search_result(items))
    }

    /// The AgentHistory turns for `agent_instance_key`, as an
    /// `AgentInstanceHistorySearchQueryResult` JSON object (`{ items, page }`),
    /// in the engine's canonical `(loopIteration, producedAt, historyItemKey)`
    /// order. `filter_json` is `{ commitStatus? }` where `commitStatus` is a REST
    /// spelling or array of them; **omitting it defaults to COMMITTED only**
    /// (PENDING/DISCARDED surface only when asked for explicitly). Accepts the
    /// canonical REST envelope `{ "filter": { commitStatus? } }` as well as the
    /// top-level shorthand. Mirrors
    /// `POST /agent-instances/{agentInstanceKey}/history/search`.
    #[wasm_bindgen(js_name = searchAgentInstanceHistory)]
    pub fn search_agent_instance_history(
        &self,
        agent_instance_key: &str,
        filter_json: &str,
    ) -> Result<String, JsValue> {
        let key = parse_key(agent_instance_key)?;
        let filter = parse_agent_history_filter(key, filter_json)?;
        let items: Vec<serde_json::Value> = self
            .read_model
            .try_agent_history(&filter, None)
            .map_err(|e| js_err(&e.to_string()))?
            .iter()
            .map(agent_history_result)
            .collect::<Result<_, _>>()
            .map_err(|e| js_err(&e))?;
        to_json(&search_result(items))
    }
}

#[cfg(feature = "read-model")]
impl TestEngine {
    /// Fold the newly-emitted `events` into the in-memory read model so the REST
    /// read methods stay consistent with `self.log`. Best-effort: a projection
    /// error must never abort a simulation step (the read channel is auxiliary),
    /// so it is surfaced to `console.error` for debuggability rather than
    /// propagated.
    fn project_read_model(&self, events: &[Event]) {
        if events.is_empty() {
            return;
        }
        let refs: Vec<&Event> = events.iter().collect();
        if let Err(e) = self.read_model.export(&refs) {
            console_error(&format!("nano read-model projection failed: {e}"));
        }
    }
}

/// Open a fresh in-memory read model for the test engine. `ReadStore::open(None)`
/// opens an ephemeral `:memory:` SQLite on every target — backed by the wasm
/// MemoryVFS on `wasm32` and by the native (bundled) C SQLite on host builds (the
/// backend is selected per target in `Cargo.toml`). Opening a pristine in-RAM
/// database cannot fail in practice, so a failure here is an unrecoverable
/// environment bug and is surfaced as a panic.
#[cfg(feature = "read-model")]
fn open_read_model() -> ReadStore {
    ReadStore::open(None).expect("open in-memory read model")
}

/// The REST string spelling of a [`UserTaskState`] (Camunda v2 enum), used to
/// honour `searchUserTasks`' `state` filter through the read model.
#[cfg(feature = "read-model")]
fn user_task_state_rest(state: UserTaskState) -> &'static str {
    match state {
        UserTaskState::Created => "CREATED",
        UserTaskState::Completed => "COMPLETED",
        UserTaskState::Canceled => "CANCELED",
    }
}

/// Every [`UserTaskState`] variant, in enum order. Single source of truth for the
/// set of valid REST `state` filter spellings — each spelling is derived from
/// [`user_task_state_rest`], so the two never drift. A variant added to the enum
/// makes `user_task_state_rest`'s match fail to compile; the
/// `all_user_task_states_is_exhaustive` test additionally asserts this list keeps
/// enumerating them all.
#[cfg(feature = "read-model")]
const ALL_USER_TASK_STATES: [UserTaskState; 3] = [
    UserTaskState::Created,
    UserTaskState::Completed,
    UserTaskState::Canceled,
];

/// Parse a REST `state` filter spelling (e.g. `"CREATED"`) back into a
/// [`UserTaskState`], or `None` when it is not a valid enum spelling. The gateway
/// rejects unknown `UserTaskStateEnum` spellings during request deserialization,
/// so `searchUserTasks` does too rather than silently returning an empty set.
#[cfg(feature = "read-model")]
fn user_task_state_from_rest(s: &str) -> Option<UserTaskState> {
    ALL_USER_TASK_STATES
        .into_iter()
        .find(|st| user_task_state_rest(*st) == s)
}

/// The comma-separated list of valid REST `state` spellings, for error messages.
#[cfg(feature = "read-model")]
fn user_task_state_spellings() -> String {
    ALL_USER_TASK_STATES
        .iter()
        .map(|st| user_task_state_rest(*st))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Extract an optional string field (e.g. `state`) from a search filter argument.
/// Empty/whitespace input, a missing field, or an explicit JSON `null` all yield
/// `None` (no filter); a non-string value is rejected. The filter body must be a
/// JSON object (or root `null`): a non-object root (`[]`, `"x"`, `42`, …) is
/// rejected rather than silently treated as "no filter", matching the gateway's
/// request parsing.
///
/// The field is looked up under a nested `filter` object when present
/// (`{ "filter": { "state": … } }`, the canonical REST body shape for
/// `UserTaskSearchQuery`) as well as at the top level (`{ "state": … }`, the
/// convenience shorthand). A present-but-non-object `filter` is a malformed body
/// and is rejected rather than silently ignored.
#[cfg(feature = "read-model")]
fn parse_state_filter(filter_json: &str, field: &str) -> Result<Option<String>, JsValue> {
    parse_state_filter_inner(filter_json, field).map_err(|e| js_err(&e))
}

/// Shape-validate a `search*` filter body against the gateway's REST contract:
/// accept empty/whitespace, JSON `null`, or a JSON object (with an optional nested
/// `filter` object); reject a non-object root (`[]`, `"x"`, `42`, …) or a
/// present-but-non-object `filter`, and reject syntactically invalid JSON. The
/// `searchProcessInstances` / `searchVariables` methods don't filter on any field
/// yet, but still call this so a malformed body is rejected exactly as the gateway
/// rejects it at request deserialization, instead of being silently accepted.
#[cfg(feature = "read-model")]
fn validate_search_filter_body(filter_json: &str) -> Result<(), JsValue> {
    validate_search_filter_body_inner(filter_json).map_err(|e| js_err(&e))
}

/// Pure core of [`validate_search_filter_body`] (host-testable: constructs no
/// `JsValue`). Delegates to [`parse_state_filter_inner`] with a field that is
/// never present, so the body-shape contract has a single implementation shared
/// with the field-reading callers (`searchUserTasks`) and the two can't drift.
#[cfg(feature = "read-model")]
fn validate_search_filter_body_inner(filter_json: &str) -> Result<(), String> {
    parse_state_filter_inner(filter_json, "\0__nano_validate_shape_only__").map(|_| ())
}

/// Select the map that actually carries filter fields, honouring the canonical
/// REST envelope `{ "filter": { … } }` (matching the `*SearchQuery` request
/// bodies) as well as the top-level `{ … }` shorthand. A present-but-non-object
/// `filter` is malformed and is rejected. Shared by every search-filter parser
/// (`searchUserTasks`, `searchAgentInstances`, `searchAgentInstanceHistory`) so
/// the envelope contract has a single implementation and cannot drift.
#[cfg(feature = "read-model")]
fn rest_filter_target(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<&serde_json::Map<String, serde_json::Value>, String> {
    match obj.get("filter") {
        None | Some(serde_json::Value::Null) => Ok(obj),
        Some(serde_json::Value::Object(nested)) => Ok(nested),
        Some(_) => Err("the `filter` field must be a JSON object".to_string()),
    }
}

/// Pure core of [`parse_state_filter`] (host-testable: constructs no `JsValue`).
#[cfg(feature = "read-model")]
fn parse_state_filter_inner(filter_json: &str, field: &str) -> Result<Option<String>, String> {
    let t = filter_json.trim();
    if t.is_empty() {
        return Ok(None);
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| format!("invalid filter JSON: {e}"))?;
    let obj = match &json {
        serde_json::Value::Null => return Ok(None),
        serde_json::Value::Object(map) => map,
        _ => return Err("filter body must be a JSON object".to_string()),
    };
    let target = rest_filter_target(obj)?;
    match target.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("filter `{field}` must be a string")),
    }
}

/// Wrap a list of result items in the Camunda search-query envelope
/// `{ items, page: { totalItems, hasMoreTotalItems, startCursor, endCursor } }`.
/// The in-browser engine returns every match in one page (no cursoring), so
/// `totalItems == items.len()` and the cursors are null.
#[cfg(feature = "read-model")]
fn search_result(items: Vec<serde_json::Value>) -> serde_json::Value {
    let total = items.len();
    serde_json::json!({
        "items": items,
        "page": {
            "totalItems": total,
            "hasMoreTotalItems": false,
            "startCursor": serde_json::Value::Null,
            "endCursor": serde_json::Value::Null,
        },
    })
}

/// An ISO-8601 / RFC-3339 UTC timestamp (`YYYY-MM-DDThh:mm:ss.sssZ`) from epoch
/// milliseconds, matching the `date-time` strings the gateway emits — computed
/// without `chrono` to avoid pulling a date crate into the wasm engine.
#[cfg(feature = "read-model")]
fn iso8601_from_ms(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let millis = ms % 1000;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (hour, min, sec) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    // days since 1970-01-01 -> civil (y, m, d), Howard Hinnant's algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

/// Serialise a [`FormRow`] as the gateway's `FormResult` JSON shape.
#[cfg(feature = "read-model")]
fn form_result(row: &FormRow) -> serde_json::Value {
    serde_json::json!({
        "tenantId": row.tenant_id,
        "formId": row.form_id,
        "schema": row.schema,
        "version": row.version as i64,
        "formKey": row.form_key.to_string(),
    })
}

/// Serialise a [`ResourceRow`] as the gateway's `ResourceResult` JSON shape.
#[cfg(feature = "read-model")]
fn resource_result(row: &ResourceRow) -> serde_json::Value {
    serde_json::json!({
        "resourceName": row.resource_name,
        "version": row.version,
        "versionTag": row.version_tag,
        "resourceId": row.resource_id,
        "tenantId": row.tenant_id,
        "resourceKey": row.resource_key.to_string(),
    })
}

/// Serialise a [`ProcessInstanceRow`] as the gateway's `ProcessInstanceResult`
/// JSON shape. The call-activity hierarchy (issue #977) is surfaced exactly as
/// the gateway projects it: `parentProcessInstanceKey` / `parentElementInstanceKey`
/// carry the engine-tracked linkage for a child spawned by a call activity (both
/// null for a top-level instance), and `rootProcessInstanceKey` is resolved by
/// walking the parent chain via `roots` (a per-search [`RootResolver`] memoising
/// the whole walked chain, so a page of co-located descendants walks each parent
/// once) to the top-level ancestor — a top-level instance reports its own key.
/// Fields the engine does not retain (definition name, version tag) stay null.
#[cfg(feature = "read-model")]
fn process_instance_result(row: &ProcessInstanceRow, roots: &RootResolver) -> serde_json::Value {
    let state = match row.state {
        ProcessInstanceState::Active => "ACTIVE",
        ProcessInstanceState::Suspended => "SUSPENDED",
        ProcessInstanceState::Completed => "COMPLETED",
        ProcessInstanceState::Terminated | ProcessInstanceState::Terminating => "TERMINATED",
    };
    let parent_process_instance_key = match row.parent_process_instance_key {
        Some(k) => serde_json::Value::String(k.to_string()),
        None => serde_json::Value::Null,
    };
    let parent_element_instance_key = match row.parent_element_instance_key {
        Some(k) => serde_json::Value::String(k.to_string()),
        None => serde_json::Value::Null,
    };
    // `suspendedDate` is always present (a required, nullable field): an ISO
    // datetime while the instance is suspended, `null` otherwise. Derived from
    // the same `suspended_date_ms` record the `state` projection reads.
    let suspended_date = match row.suspended_date_ms {
        Some(ms) => serde_json::Value::String(iso8601_from_ms(ms)),
        None => serde_json::Value::Null,
    };
    serde_json::json!({
        "processDefinitionId": row.process_definition_id,
        "processDefinitionName": serde_json::Value::Null,
        "processDefinitionVersion": row.version,
        "processDefinitionVersionTag": serde_json::Value::Null,
        "startDate": iso8601_from_ms(row.start_date_ms),
        "endDate": serde_json::Value::Null,
        "state": state,
        "suspendedDate": suspended_date,
        "hasIncident": row.has_incident,
        "tenantId": "<default>",
        "processInstanceKey": row.key.to_string(),
        "processDefinitionKey": row.process_definition_key,
        "parentProcessInstanceKey": parent_process_instance_key,
        "parentElementInstanceKey": parent_element_instance_key,
        "rootProcessInstanceKey": roots.root_of_row(row).to_string(),
        "tags": row.tags,
        "businessId": row.business_id,
    })
}

/// Serialise a [`VariableRow`] as the gateway's `VariableSearchResult` JSON shape.
/// Long values are truncated to [`VARIABLE_VALUE_PREVIEW_LEN`] (the shared
/// canonical preview length, imported from `nanobpmn-read-model` so the gateway
/// and TestEngine can't drift) on a char boundary and `isTruncated` is set,
/// mirroring the gateway, whose `truncateValues` defaults to on. (A
/// single-variable get returns the full untruncated value there and here.)
#[cfg(feature = "read-model")]
fn variable_search_result(v: &VariableRow) -> serde_json::Value {
    let (value, is_truncated) = if v.value.len() > VARIABLE_VALUE_PREVIEW_LEN {
        let mut end = VARIABLE_VALUE_PREVIEW_LEN;
        while !v.value.is_char_boundary(end) {
            end -= 1;
        }
        (v.value[..end].to_string(), true)
    } else {
        (v.value.clone(), false)
    };
    serde_json::json!({
        "name": v.name,
        "tenantId": "<default>",
        "variableKey": v.key.to_string(),
        "scopeKey": v.scope_key.to_string(),
        "processInstanceKey": v.instance_key.to_string(),
        "rootProcessInstanceKey": serde_json::Value::Null,
        "value": value,
        "isTruncated": is_truncated,
    })
}

/// Parse the `searchAgentInstances` filter body:
/// `{ agentInstanceKey?, agentDefinitionKey?, processInstanceKey?,
/// rootProcessInstanceKey?, processDefinitionKey?, status?, elementId?,
/// tenantId? }`. Accepts the canonical REST envelope `{ "filter": { … } }`
/// (matching `AgentInstanceSearchQuery`) as well as this top-level shorthand.
/// Empty/whitespace ⇒ an unfiltered search. Keys are decimal strings; `status`
/// is a REST spelling.
#[cfg(feature = "read-model")]
fn parse_agent_instance_filter(filter_json: &str) -> Result<AgentInstanceFilter, JsValue> {
    let t = filter_json.trim();
    let mut filter = AgentInstanceFilter::default();
    if t.is_empty() {
        return Ok(filter);
    }
    let json: serde_json::Value = serde_json::from_str(t)
        .map_err(|e| js_err(&format!("searchAgentInstances: invalid filter JSON: {e}")))?;
    let map = match &json {
        serde_json::Value::Object(m) => m,
        serde_json::Value::Null => return Ok(filter),
        _ => return Err(js_err("searchAgentInstances filter must be a JSON object")),
    };
    let map = rest_filter_target(map).map_err(|e| js_err(&e))?;
    filter.agent_instance_key = agent_filter_key(map, "agentInstanceKey")?;
    filter.agent_definition_key = agent_filter_key(map, "agentDefinitionKey")?;
    filter.process_instance_key = agent_filter_key(map, "processInstanceKey")?;
    filter.root_process_instance_key = agent_filter_key(map, "rootProcessInstanceKey")?;
    filter.process_definition_key = agent_filter_key(map, "processDefinitionKey")?;
    if let Some(v) = agent_filter_scalar(map.get("status"))? {
        if !v.is_null() {
            let s = v
                .as_str()
                .ok_or_else(|| js_err("searchAgentInstances filter `status` must be a string"))?;
            filter.status = Some(parse_agent_status(s)?);
        }
    }
    filter.element_id = agent_filter_string(map, "elementId")?;
    filter.tenant_id = agent_filter_string(map, "tenantId")?;
    filter.process_definition_id = agent_filter_string(map, "processDefinitionId")?;
    filter.process_definition_version_tag =
        agent_filter_string(map, "processDefinitionVersionTag")?;
    filter.process_definition_version = agent_filter_integer(map, "processDefinitionVersion")?;
    filter.creation_date_ms = agent_filter_date(map, "creationDate")?;
    filter.last_updated_date_ms = agent_filter_date(map, "lastUpdatedDate")?;
    filter.completion_date_ms = agent_filter_date(map, "completionDate")?;
    if let Some(value) = map.get("elementInstanceKeys").filter(|v| !v.is_null()) {
        let values = value
            .as_array()
            .ok_or_else(|| js_err("elementInstanceKeys must be an array"))?;
        filter.element_instance_keys = values
            .iter()
            .map(|value| {
                let value = agent_filter_scalar(Some(value))?
                    .ok_or_else(|| js_err("elementInstanceKeys cannot contain null"))?;
                parse_key(value.as_str().ok_or_else(|| {
                    js_err("elementInstanceKeys must contain decimal string keys")
                })?)
            })
            .collect::<Result<_, _>>()?;
    }
    Ok(filter)
}

/// Parse the `searchAgentInstanceHistory` filter body: `{ commitStatus? }` where
/// `commitStatus` is a REST spelling or an array of them. Accepts the canonical
/// REST envelope `{ "filter": { commitStatus? } }` as well as this top-level
/// shorthand. Omitted/empty ⇒ the COMMITTED default (enforced by the read
/// model). The `agent_instance_key` is bound from the method argument, not the
/// body.
#[cfg(feature = "read-model")]
fn parse_agent_history_filter(
    agent_instance_key: u64,
    filter_json: &str,
) -> Result<AgentHistoryFilter, JsValue> {
    let mut filter = AgentHistoryFilter {
        agent_instance_key: Some(agent_instance_key),
        ..Default::default()
    };
    let t = filter_json.trim();
    if t.is_empty() {
        return Ok(filter);
    }
    let json: serde_json::Value = serde_json::from_str(t).map_err(|e| {
        js_err(&format!(
            "searchAgentInstanceHistory: invalid filter JSON: {e}"
        ))
    })?;
    let map = match &json {
        serde_json::Value::Object(m) => m,
        serde_json::Value::Null => return Ok(filter),
        _ => {
            return Err(js_err(
                "searchAgentInstanceHistory filter must be a JSON object",
            ));
        }
    };
    let map = rest_filter_target(map).map_err(|e| js_err(&e))?;
    filter.history_item_key = agent_filter_key(map, "historyItemKey")?;
    filter.element_instance_key = agent_filter_key(map, "elementInstanceKey")?;
    filter.job_key = agent_filter_key(map, "jobKey")?;
    filter.loop_iteration = agent_filter_integer(map, "loopIteration")?;
    filter.produced_at_ms = agent_filter_date(map, "producedAt")?;
    if let Some(role) = agent_filter_string(map, "role")? {
        filter.role = Some(parse_agent_role(&role)?);
    }
    if let Some(v) = map.get("commitStatus") {
        let v = match v {
            serde_json::Value::Object(map) if map.len() == 1 => map
                .get("$eq")
                .or_else(|| map.get("$in"))
                .ok_or_else(|| js_err("unsupported commitStatus filter operator"))?,
            _ => v,
        };
        let spellings: Vec<&str> = match v {
            serde_json::Value::Null => Vec::new(),
            serde_json::Value::String(s) => vec![s.as_str()],
            serde_json::Value::Array(items) => items
                .iter()
                .map(|it| {
                    it.as_str()
                        .ok_or_else(|| js_err("`commitStatus` array entries must be strings"))
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => {
                return Err(js_err(
                    "`commitStatus` must be a string or an array of strings",
                ));
            }
        };
        if !spellings.is_empty() {
            let statuses = spellings
                .into_iter()
                .map(parse_agent_commit_status)
                .collect::<Result<Vec<_>, _>>()?;
            filter.commit_status = Some(statuses);
        }
    }
    Ok(filter)
}

/// Parse a REST `AgentHistoryItemStateEnum` spelling into the commit status.
#[cfg(feature = "read-model")]
fn parse_agent_commit_status(
    s: &str,
) -> Result<nanobpmn_engine_core::AgentHistoryCommitStatus, JsValue> {
    use nanobpmn_engine_core::AgentHistoryCommitStatus as Cs;
    match s {
        "COMMITTED" => Ok(Cs::Committed),
        "PENDING" => Ok(Cs::Pending),
        "DISCARDED" => Ok(Cs::Discarded),
        other => Err(js_err(&format!(
            "invalid agent commit status {other:?}; expected one of COMMITTED, PENDING, DISCARDED"
        ))),
    }
}

/// Read an optional decimal-string key from an agent-search filter object.
#[cfg(feature = "read-model")]
fn agent_filter_key(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<u64>, JsValue> {
    match agent_filter_scalar(map.get(field))? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(parse_key(s)?)),
        Some(_) => Err(js_err(&format!(
            "agent-search filter `{field}` must be a decimal string key"
        ))),
    }
}

/// Read an optional string field from an agent-search filter object.
#[cfg(feature = "read-model")]
fn agent_filter_string(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>, JsValue> {
    agent_filter_scalar(map.get(field))?
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| js_err(&format!("{field} must be a string")))
        })
        .transpose()
}

#[cfg(feature = "read-model")]
fn agent_filter_scalar(
    value: Option<&serde_json::Value>,
) -> Result<Option<&serde_json::Value>, JsValue> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Object(map)) if map.len() == 1 && map.contains_key("$eq") => {
            Ok(map.get("$eq").filter(|v| !v.is_null()))
        }
        Some(serde_json::Value::Object(_)) => Err(js_err("unsupported agent filter operator")),
        value => Ok(value),
    }
}

#[cfg(feature = "read-model")]
fn agent_filter_integer(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<i32>, JsValue> {
    agent_filter_scalar(map.get(field))?
        .map(|v| {
            v.as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| js_err(&format!("{field} must be an integer")))
        })
        .transpose()
}

#[cfg(feature = "read-model")]
fn agent_filter_date(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<u64>, JsValue> {
    agent_filter_string(map, field)?
        .map(|v| ms_from_rfc3339(&v).map_err(|e| js_err(&e)))
        .transpose()
}

/// Serialise an [`AgentInstanceRow`] as the gateway's `AgentInstanceResult` JSON
/// shape (Camunda v2 `/agent-instances`). Keys are decimal strings; dates are
/// RFC-3339; `tools` is decoded from the row's projected JSON.
#[cfg(feature = "read-model")]
fn agent_instance_result(row: &AgentInstanceRow) -> Result<serde_json::Value, String> {
    let tools: Vec<AgentTool> = projected_agent_json(&row.tools_json)?;
    let completion_date = match row.completion_date_ms {
        Some(ms) => serde_json::Value::String(iso8601_from_ms(ms)),
        None => serde_json::Value::Null,
    };
    let version_tag = match &row.process_definition_version_tag {
        Some(tag) => serde_json::Value::String(tag.clone()),
        None => serde_json::Value::Null,
    };
    Ok(serde_json::json!({
        "agentInstanceKey": row.agent_instance_key.to_string(),
        "agentDefinitionKey": row.agent_definition_key.to_string(),
        "status": row.status.as_str(),
        "definition": {
            "model": row.model.as_deref().ok_or("agent definition is missing its required model")?,
            "provider": row.provider.as_deref().ok_or("agent definition is missing its required provider")?,
            "systemPrompt": agent_prompt_result(row.system_prompt.as_deref())?,
        },
        "metrics": {
            "inputTokens": row.input_tokens,
            "outputTokens": row.output_tokens,
            "modelCalls": row.model_calls,
            "toolCalls": row.tool_calls,
            "reasoningTokenCount": row.reasoning_token_count,
            "cacheCreationTokenCount": row.cache_creation_token_count,
            "cacheReadTokenCount": row.cache_read_token_count,
        },
        "limits": {
            "maxTokens": row.max_tokens,
            "maxModelCalls": row.max_model_calls,
            "maxToolCalls": row.max_tool_calls,
        },
        "tools": tools.iter().map(agent_tool_result).collect::<Vec<_>>(),
        "elementId": row.element_id,
        "processInstanceKey": row.process_instance_key.to_string(),
        "rootProcessInstanceKey": row.root_process_instance_key.to_string(),
        "processDefinitionKey": row.process_definition_key.to_string(),
        "processDefinitionId": row.process_definition_id,
        "processDefinitionVersion": row.process_definition_version,
        "processDefinitionVersionTag": version_tag,
        "tenantId": row.tenant_id,
        "creationDate": iso8601_from_ms(row.creation_date_ms),
        "lastUpdatedDate": iso8601_from_ms(row.last_updated_date_ms),
        "completionDate": completion_date,
        "elementInstanceKeys": row.element_instance_keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
    }))
}

#[cfg(feature = "read-model")]
use nanobpmn_engine_core::AgentHistoryToolCall;

#[cfg(feature = "read-model")]
fn projected_agent_json<T: serde::de::DeserializeOwned>(json: &str) -> Result<T, String> {
    serde_json::from_str(json).map_err(|e| format!("invalid projected agent JSON: {e}"))
}

/// Serialise an [`AgentHistoryRow`] as the gateway's `AgentInstanceHistoryItemResult`
/// JSON shape. Per-call `metrics` retain their recorded presence and null values;
/// `content`, `toolCalls` and `tools`
/// are decoded from the row's projected JSON columns and re-serialised into the
/// REST wire shape (camelCase keys, REST enum spellings, and the opaque
/// `object`/`arguments` JSON strings re-parsed to structured JSON) exactly as the
/// gateway does — never dumped in the engine's internal snake_case layout.
#[cfg(feature = "read-model")]
fn agent_history_result(row: &AgentHistoryRow) -> Result<serde_json::Value, String> {
    let content = projected_agent_json::<Vec<AgentHistoryContent>>(&row.content_json)?
        .iter()
        .map(agent_content_result)
        .collect::<Result<Vec<_>, _>>()?;
    let tool_calls = projected_agent_json::<Vec<AgentHistoryToolCall>>(&row.tool_calls_json)?
        .iter()
        .map(agent_tool_call_result)
        .collect::<Result<Vec<_>, _>>()?;
    let tools = projected_agent_json::<Vec<AgentTool>>(&row.tools_json)?
        .iter()
        .map(agent_tool_result)
        .collect::<Vec<_>>();
    let metrics = row
        .metrics_json
        .as_deref()
        .map(projected_agent_json::<Option<nanobpmn_engine_core::AgentHistoryMetrics>>)
        .transpose()?
        .flatten();
    let limits = row
        .limits_json
        .as_deref()
        .map(projected_agent_json::<AgentInstanceLimits>)
        .transpose()?
        .unwrap_or_default();
    Ok(serde_json::json!({
        "historyItemKey": row.agent_history_key.to_string(),
        "historyItemId": row.history_item_id.clone().unwrap_or_default(),
        "agentInstanceKey": row.agent_instance_key.to_string(),
        "elementInstanceKey": row.element_instance_key.to_string(),
        "jobKey": row.job_key.to_string(),
        "jobLeaseToken": row.job_lease,
        "loopIteration": row.loop_iteration,
        "role": row.role.as_str(),
        "content": content,
        "toolCalls": tool_calls,
        "metrics": agent_metrics_result(&metrics),
        "commitStatus": row.commit_status.as_str(),
        "producedAt": iso8601_from_ms(row.produced_at_ms),
        "tools": tools,
        "model": row.model,
        "provider": row.provider,
        "limits": {
            "maxTokens": limits.max_tokens, "maxModelCalls": limits.max_model_calls,
            "maxToolCalls": limits.max_tool_calls,
        },
        "systemPrompt": agent_prompt_result(row.system_prompt.as_deref())?,
    }))
}

#[cfg(any(feature = "read-model", test))]
fn agent_metrics_result(
    metrics: &Option<nanobpmn_engine_core::AgentHistoryMetrics>,
) -> serde_json::Value {
    metrics
        .as_ref()
        .map(|metrics| {
            serde_json::json!({
                "inputTokens": metrics.input_tokens,
                "outputTokens": metrics.output_tokens,
                "durationMs": metrics.duration_ms,
            })
        })
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(feature = "read-model")]
fn agent_prompt_result(
    prompt: Option<&[AgentHistoryContent]>,
) -> Result<Vec<serde_json::Value>, String> {
    prompt
        .unwrap_or_default()
        .iter()
        .map(agent_content_result)
        .collect()
}

/// Map an engine [`AgentHistoryContent`] block to the gateway's
/// `AgentInstanceMessageContent` JSON shape (camelCase keys, REST `contentType`
/// enum spelling). The engine stores `object` as an opaque JSON string; it is
/// re-parsed into structured JSON so the wire carries JSON, not a JSON-in-a-string
/// — mirroring the server's `agent_message_content`.
#[cfg(feature = "read-model")]
fn agent_content_result(c: &AgentHistoryContent) -> Result<serde_json::Value, String> {
    Ok(match c.content_type {
        AgentHistoryContentType::Text => serde_json::json!({
            "contentType": "TEXT", "text": c.text.as_deref().ok_or("TEXT content is missing text")?,
        }),
        AgentHistoryContentType::Document => serde_json::json!({
            "contentType": "DOCUMENT",
            "documentReference": projected_agent_json::<serde_json::Map<String, serde_json::Value>>(
                c.document_reference.as_deref().ok_or("DOCUMENT content is missing documentReference")?)?,
        }),
        AgentHistoryContentType::Object => serde_json::json!({
            "contentType": "OBJECT",
            "object": projected_agent_json::<serde_json::Value>(
                c.object.as_deref().ok_or("OBJECT content is missing object")?)?,
        }),
    })
}

/// Map an engine [`AgentHistoryToolCall`] to the gateway's
/// `AgentInstanceToolCall` JSON shape (camelCase keys). `arguments` is stored as
/// an opaque JSON string and re-parsed into structured JSON (mirroring the
/// server's `agent_tool_call_result`); an absent one becomes `null`.
#[cfg(feature = "read-model")]
fn agent_tool_call_result(c: &AgentHistoryToolCall) -> Result<serde_json::Value, String> {
    let arguments = c
        .arguments
        .as_deref()
        .map(projected_agent_json::<serde_json::Map<String, serde_json::Value>>)
        .transpose()?;
    Ok(serde_json::json!({
        "toolCallId": c.tool_call_id,
        "toolName": c.tool_name,
        "elementId": c.element_id,
        "arguments": arguments,
    }))
}

/// Map an engine [`AgentTool`] to the gateway's `AgentTool` JSON shape (camelCase
/// keys), mirroring the server's `agent_tool_result`.
#[cfg(feature = "read-model")]
fn agent_tool_result(t: &AgentTool) -> serde_json::Value {
    serde_json::json!({
        "name": t.name,
        "description": t.description,
        "elementId": t.element_id,
    })
}

/// Coerce an optional user-task date (`followUpDate` / `dueDate`) to the JSON the
/// gateway would emit: the string when it is a valid RFC-3339 `date-time`, else
/// JSON `null`. Mirrors the gateway's `parse_date`, which parses these as
/// `chrono::DateTime<Utc>` and coerces any unparseable value to `null` rather
/// than forwarding an invalid date string to clients.
#[cfg(feature = "read-model")]
fn rfc3339_or_null(value: &Option<String>) -> serde_json::Value {
    match value.as_deref() {
        Some(s) if is_rfc3339_date_time(s) => serde_json::Value::String(s.to_string()),
        _ => serde_json::Value::Null,
    }
}

/// Number of days in `month` (1-12) of `year`, honouring the proleptic Gregorian
/// leap-year rule chrono uses (divisible by 4, except centuries not divisible by
/// 400). Callers must pass a `month` already validated into `1..=12`; any other
/// value falls through to 31 and is rejected by the surrounding range check.
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 => {
            if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// True when `s` is a valid RFC-3339 `date-time` — the shape
/// `str::parse::<chrono::DateTime<Utc>>()` accepts on the gateway:
/// `YYYY-MM-DDThh:mm:ss`, an optional `.fraction`, and a `Z`/`±hh:mm` offset.
/// Component ranges are checked so out-of-range values (month 13, hour 25, …) are
/// rejected the same way chrono rejects them, including the day against the
/// actual length of the given month/year (so `2026-02-31` is rejected and leap
/// days like `2024-02-29` are accepted). Validation only (no date crate).
#[cfg(feature = "read-model")]
fn is_rfc3339_date_time(s: &str) -> bool {
    let b = s.as_bytes();
    // Shortest valid form "1970-01-01T00:00:00Z" is 20 bytes.
    if b.len() < 20 {
        return false;
    }
    let digit = |c: u8| c.is_ascii_digit();
    let num = |slice: &[u8]| -> u32 {
        slice
            .iter()
            .fold(0u32, |acc, &c| acc * 10 + u32::from(c - b'0'))
    };
    let fixed = digit(b[0])
        && digit(b[1])
        && digit(b[2])
        && digit(b[3])
        && b[4] == b'-'
        && digit(b[5])
        && digit(b[6])
        && b[7] == b'-'
        && digit(b[8])
        && digit(b[9])
        && (b[10] == b'T' || b[10] == b't')
        && digit(b[11])
        && digit(b[12])
        && b[13] == b':'
        && digit(b[14])
        && digit(b[15])
        && b[16] == b':'
        && digit(b[17])
        && digit(b[18]);
    if !fixed {
        return false;
    }
    let year = num(&b[0..4]);
    let month = num(&b[5..7]);
    let day = num(&b[8..10]);
    let hour = num(&b[11..13]);
    let min = num(&b[14..16]);
    let sec = num(&b[17..19]);
    // `sec == 60` is intentionally accepted: chrono's `DateTime<Utc>` parse (the
    // gateway's `parse_date`) accepts leap seconds at any hh:mm (e.g. `12:30:60Z`)
    // and rejects `61`, so `sec > 60` mirrors it exactly. Do not tighten to `> 59`.
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || min > 59
        || sec > 60
    {
        return false;
    }
    let mut i = 19;
    // Optional fractional seconds: a dot followed by at least one digit.
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let start = i;
        while i < b.len() && digit(b[i]) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    // Mandatory timezone: `Z`/`z` or a `±hh:mm` offset.
    match b.get(i) {
        Some(&c) if c == b'Z' || c == b'z' => i + 1 == b.len(),
        Some(&c) if c == b'+' || c == b'-' => {
            i + 6 == b.len()
                && digit(b[i + 1])
                && digit(b[i + 2])
                && b[i + 3] == b':'
                && digit(b[i + 4])
                && digit(b[i + 5])
                && num(&b[i + 1..i + 3]) <= 23
                && num(&b[i + 4..i + 6]) <= 59
        }
        _ => false,
    }
}

/// Serialise a [`UserTaskRow`] as the gateway's `UserTaskResult` JSON shape. The
/// `state` is the Camunda v2 enum spelling; dates are ISO-8601 UTC; fields the
/// engine does not retain (name, process name, completion date) are null;
/// `priority` is clamped to `0..=100` exactly as the gateway does.
///
/// `rootProcessInstanceKey` (issue #1095) is resolved by walking the task's
/// `processInstanceKey` up the call-activity parent chain via `roots` — the same
/// [`RootResolver`] the process-instance projection uses (issue #977), so there
/// is one canonical root-walk and no duplicate derivation. A top-level
/// (self-rooted) task reports its own `processInstanceKey`.
#[cfg(feature = "read-model")]
fn user_task_result(
    task: &UserTaskRow,
    roots: &RootResolver,
    business_id: Option<String>,
) -> serde_json::Value {
    serde_json::json!({
        "name": serde_json::Value::Null,
        "state": user_task_state_rest(task.state),
        "assignee": task.assignee,
        "elementId": task.element_id,
        "candidateGroups": task.candidate_groups,
        "candidateUsers": task.candidate_users,
        "processDefinitionId": task.process_definition_id,
        "creationDate": iso8601_from_ms(task.created_at_ms),
        "completionDate": serde_json::Value::Null,
        "followUpDate": rfc3339_or_null(&task.follow_up_date),
        "dueDate": rfc3339_or_null(&task.due_date),
        "tenantId": "<default>",
        "externalFormReference": task.external_form_reference,
        "processDefinitionVersion": task.process_definition_version,
        "customHeaders": serde_json::Map::new(),
        "userTaskKey": task.key.to_string(),
        "elementInstanceKey": task.element_instance_key.to_string(),
        "processName": serde_json::Value::Null,
        "processDefinitionKey": task.process_definition_key,
        "processInstanceKey": task.instance_key.to_string(),
        "rootProcessInstanceKey": roots.root_process_instance_key(task.instance_key).to_string(),
        "formKey": task.form_key.map(|k| k.to_string()),
        "priority": task.priority.clamp(0, 100),
        "tags": Vec::<String>::new(),
        "businessId": business_id,
    })
}

impl Default for TestEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn decode_trace(json: &str) -> Result<Vec<LogEntry>, String> {
    let records: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_str(json).map_err(|e| format!("invalid event trace: {e}"))?;
    records
        .into_iter()
        .enumerate()
        .map(|(index, mut record)| {
            let seq = record
                .remove("seq")
                .and_then(|v| v.as_u64())
                .filter(|seq| *seq == index as u64 + 1)
                .ok_or_else(|| "event trace must be complete and sequential".to_string())?;
            let now = record
                .remove("now")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "event trace requires an unsigned clock".to_string())?;
            let kind = record
                .remove("type")
                .and_then(|v| v.as_str().map(str::to_owned))
                .ok_or_else(|| "event trace requires an event type".to_string())?;
            let event = nanobpmn_engine_core::decode_event_json(
                &serde_json::json!({kind: record}).to_string(),
            )
            .map_err(|e| format!("invalid replay event: {e}"))?;
            Ok(LogEntry { seq, now, event })
        })
        .collect()
}

impl TestEngine {
    /// Reject a state-mutating call while a debug run is paused mid-command. The
    /// paused engine holds an *intermediate* state — a partially-applied command
    /// that a normal run-to-completion (RTC) engine can never observe. Letting
    /// another command (`completeJob`, `advanceTime`, `tickNow`, …) build on it
    /// would produce states the production engine cannot represent, so the
    /// contract is: while `debugIsPaused`, the only permitted mutations are
    /// `debugResume`, `debugStep`, `debugClear`, and `reset`. Everything else
    /// must first finish or discard the run.
    fn guard_paused(&self) -> Result<(), JsValue> {
        self.check_not_paused().map_err(js_err)
    }

    /// Native-testable core of [`TestEngine::guard_paused`]: `Err(msg)` while a
    /// debug run is paused, `Ok` otherwise. Kept `&str`-typed (not `JsValue`) so
    /// unit tests can assert the rejection without constructing a `JsValue`,
    /// which aborts off the wasm target.
    fn check_not_paused(&self) -> Result<(), &'static str> {
        if self.debug_is_paused() {
            return Err(
                "cannot mutate the engine while a debug run is paused; call debugResume, debugStep, debugClear, or reset first",
            );
        }
        Ok(())
    }

    /// Apply a command at the current virtual clock, recording the emitted
    /// events in the log.
    fn apply(&mut self, command: Command) -> Result<Vec<Event>, nanobpmn_engine_core::EngineError> {
        let now = self.now;
        let events = self.engine.apply_command_at(command, now)?;
        for ev in &events {
            self.seq += 1;
            self.fold_history(ev);
            self.log.push(LogEntry {
                seq: self.seq,
                now,
                event: ev.clone(),
            });
        }
        #[cfg(feature = "read-model")]
        self.project_read_model(&events);
        Ok(events)
    }

    /// Mirror the newly-emitted tail of the active [`DebugSession`]'s event log
    /// into `self.log` (with seq/now stamps + history folding), so `events()` and
    /// `snapshot()` reflect the paused partial run exactly as a normal command
    /// would. Idempotent per call: only events past `debug_folded` are appended.
    fn fold_debug_delta(&mut self) {
        let Some(session) = self.debug.as_ref() else {
            return;
        };
        let full = session.log();
        if self.debug_folded >= full.len() {
            return;
        }
        // Copy the new tail out of the session so the immutable `&self.debug`
        // borrow ends before we mutate `self.seq`/`self.log`/`self.history`
        // below; the events are then *moved* (not re-cloned) into `self.log`.
        let now = self.debug_now;
        let new_events: Vec<Event> = full[self.debug_folded..].to_vec();
        self.debug_folded = full.len();
        #[cfg(feature = "read-model")]
        self.project_read_model(&new_events);
        for ev in new_events {
            self.seq += 1;
            self.fold_history(&ev);
            self.log.push(LogEntry {
                seq: self.seq,
                now,
                event: ev,
            });
        }
    }

    /// The debug state DTO returned by every `debug*` method: whether the run is
    /// paused, the current event count, and the elements currently active (an
    /// `ElementActivated` with no matching `ElementCompleted`) — the set a diagram
    /// view highlights while paused.
    fn debug_state(&self) -> serde_json::Value {
        serde_json::json!({
            "paused": self.debug_is_paused(),
            "seq": self.seq,
            "activeElements": self.active_elements(),
        })
    }

    /// Elements with a live token, read straight from the engine's authoritative
    /// per-instance active-token map (the same source [`TestEngine::snapshot_value`]
    /// uses) rather than re-scanning the event log. Deduplicated, ordered by
    /// element-instance key (creation order) — the set a diagram view highlights
    /// while paused.
    fn active_elements(&self) -> Vec<String> {
        let mut tokens: Vec<(String, String)> = self
            .engine
            .state()
            .instances
            .values()
            .flat_map(|inst| {
                inst.active
                    .iter()
                    .map(|(k, eid)| (k.to_string(), eid.clone()))
            })
            .collect();
        tokens.sort_by(|a, b| cmp_key(&a.0, &b.0));
        let mut seen = std::collections::HashSet::new();
        tokens
            .into_iter()
            .filter_map(|(_, id)| seen.insert(id.clone()).then_some(id))
            .collect()
    }

    /// Fold one emitted event into the cumulative history aggregates that back
    /// the snapshot's `elementStats.completed`, `takenSequenceFlows` and
    /// `decisionInstances` fields, so `snapshot` never re-scans the full log.
    fn fold_history(&mut self, ev: &Event) {
        match ev {
            Event::ElementCompleted { element_id, .. } => {
                *self
                    .history
                    .completed_counts
                    .entry(element_id.clone())
                    .or_default() += 1;
            }
            Event::SequenceFlowTaken { from, to, .. } => {
                self.history.taken_flows.insert((from.clone(), to.clone()));
            }
            Event::DecisionEvaluated {
                instance_key,
                element_id,
                decision_key,
                decision_id,
                decision_output,
                evaluated_at,
                ..
            } => {
                self.history.decisions.push(DecisionInstanceDto {
                    instance_key: instance_key.to_string(),
                    element_id: element_id.clone(),
                    decision_key: decision_key.to_string(),
                    decision_id: decision_id.clone(),
                    output: value_to_json(decision_output),
                    evaluated_at: *evaluated_at,
                });
            }
            _ => {}
        }
    }

    fn created_history_result(
        &self,
        agent_key: u64,
        ids: &[String],
        events: &[Event],
    ) -> Result<Vec<serde_json::Value>, JsValue> {
        let mut created: std::collections::HashSet<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::AgentHistoryCreated { record, .. } => Some(record.agent_history_key),
                _ => None,
            })
            .collect();
        let history = self
            .engine
            .state()
            .instances
            .values()
            .find_map(|instance| instance.agent_history.get(&agent_key));
        ids.iter()
            .map(|id| {
                let record = history
                    .and_then(|records| {
                        records
                            .iter()
                            .rev()
                            .find(|record| record.history_item_id.as_deref() == Some(id))
                    })
                    .ok_or_else(|| js_err("history response record missing"))?;
                Ok(serde_json::json!({
                    "historyItemId": id,
                    "historyItemKey": record.agent_history_key.to_string(),
                    "isDuplicate": !created.remove(&record.agent_history_key),
                }))
            })
            .collect()
    }

    /// Activate the job's type so a `Created` job can be completed/failed. A job
    /// that has already been activated is left as-is.
    fn ensure_activated(&mut self, job_key: u64) -> Result<(), JsValue> {
        let (state, leased) = self
            .engine
            .state()
            .jobs
            .get(&job_key)
            .map(|j| (j.state, j.lease_token.is_some()))
            .ok_or_else(|| js_err(&format!("no such job: {job_key}")))?;
        // Already activated (or terminal) — nothing to do; completion is by key.
        if state != JobState::Created || leased {
            return Ok(());
        }
        let now = self.now;
        self.apply(Command::ActivateJobsByKey {
            job_keys: vec![job_key],
            worker: "modeler".to_string(),
            timeout: u64::MAX / 4,
            now,
            fetch_variables: Vec::new(),
            with_lease: false,
        })
        .map_err(|e| js_err(&format!("activate error: {e}")))?;
        Ok(())
    }

    fn snapshot_value(&self, created: Option<u64>) -> serde_json::Value {
        let state = self.engine.state();

        let mut instances: Vec<InstanceDto> = state
            .instances
            .values()
            .map(|inst| {
                let mut active: Vec<ActiveEl> = inst
                    .active
                    .iter()
                    .map(|(k, eid)| ActiveEl {
                        key: k.to_string(),
                        element_id: eid.clone(),
                    })
                    .collect();
                active.sort_by(|a, b| cmp_key(&a.key, &b.key));
                InstanceDto {
                    key: inst.key.to_string(),
                    process_id: inst.process_id.clone(),
                    state: instance_state(&inst.state),
                    completed: !matches!(
                        inst.state,
                        ProcessInstanceState::Active | ProcessInstanceState::Suspended
                    ),
                    active_elements: active,
                    variables: vars_to_json(&inst.variables),
                }
            })
            .collect();
        instances.sort_by(|a, b| cmp_key(&a.key, &b.key));

        let mut jobs: Vec<JobDto> = state
            .jobs
            .values()
            .filter(|j| matches!(j.state, JobState::Created | JobState::Activated))
            .map(|j| JobDto {
                key: j.key.to_string(),
                instance_key: j.instance_key.to_string(),
                element_id: j.element_id.clone(),
                job_type: j.job_type.clone(),
                state: job_state(&j.state),
                retries: j.retries,
            })
            .collect();
        jobs.sort_by(|a, b| cmp_key(&a.key, &b.key));

        let mut incidents: Vec<IncidentDto> = state
            .incidents
            .values()
            .filter(|i| i.state == IncidentState::Active)
            .map(|i| IncidentDto {
                key: i.key.to_string(),
                instance_key: i.instance_key.to_string(),
                element_id: i.element_id.clone(),
                kind: incident_kind_tag(&i.kind).to_string(),
                reason: i.reason.clone(),
            })
            .collect();
        incidents.sort_by(|a, b| cmp_key(&a.key, &b.key));

        let mut timers: Vec<TimerDto> = state
            .timers
            .values()
            .filter(|t| t.state == TimerState::Created)
            .map(|t| TimerDto {
                key: t.key.to_string(),
                instance_key: t.instance_key.to_string(),
                element_id: t.element_id.clone(),
                due_at: t.due_at,
                due_in_ms: t.due_at.saturating_sub(self.now),
            })
            .collect();
        timers.sort_by_key(|a| a.due_at);

        // User tasks parked on `userTask` elements (Created = waiting for a human;
        // Completed/Canceled retained for audit). Play renders Created tasks in
        // its task panel and lets the user complete/assign them.
        let mut user_tasks: Vec<UserTaskDto> = state
            .user_tasks
            .values()
            .map(|t| UserTaskDto {
                key: t.key.to_string(),
                instance_key: t.instance_key.to_string(),
                element_instance_key: t.element_instance_key.to_string(),
                element_id: t.element_id.clone(),
                state: user_task_state(&t.state),
                assignee: t.assignee.clone(),
                candidate_groups: t.candidate_groups.clone(),
                candidate_users: t.candidate_users.clone(),
                due_date: t.due_date.clone(),
                follow_up_date: t.follow_up_date.clone(),
                priority: t.priority,
            })
            .collect();
        user_tasks.sort_by(|a, b| cmp_key(&a.key, &b.key));

        // Open message subscriptions (waiting catch/boundary events). Play's
        // message-correlation panel needs the name + resolved correlation key.
        let mut message_subscriptions: Vec<MessageSubscriptionDto> = state
            .message_subscriptions
            .values()
            .filter(|s| {
                matches!(
                    s.state,
                    MessageSubscriptionState::Open | MessageSubscriptionState::Opening
                )
            })
            .map(|s| MessageSubscriptionDto {
                key: s.key.to_string(),
                instance_key: s.instance_key.to_string(),
                element_id: s.element_id.clone(),
                message_name: s.message_name.clone(),
                correlation_key: s.correlation_key.clone(),
                kind: subscription_kind_tag(&s.kind).to_string(),
            })
            .collect();
        message_subscriptions.sort_by(|a, b| cmp_key(&a.key, &b.key));

        // Open signal subscriptions (waiting signal catch/boundary events). Play's
        // signal-broadcast panel lists the signal names currently awaited.
        let mut signal_subscriptions: Vec<SignalSubscriptionDto> = state
            .signal_subscriptions
            .values()
            .filter(|s| {
                matches!(
                    s.state,
                    MessageSubscriptionState::Open | MessageSubscriptionState::Opening
                )
            })
            .map(|s| SignalSubscriptionDto {
                key: s.key.to_string(),
                instance_key: s.instance_key.to_string(),
                element_id: s.element_id.clone(),
                signal_name: s.signal_name.clone(),
                kind: subscription_kind_tag(&s.kind).to_string(),
            })
            .collect();
        signal_subscriptions.sort_by(|a, b| cmp_key(&a.key, &b.key));

        // Per-element token statistics for diagram overlays. `active` is the
        // current live token count (from instance active elements); `completed`
        // and `incidents` are cumulative counts. `completed` comes from the
        // incrementally-maintained history aggregate (no per-poll log scan).
        let mut stats: HashMap<String, ElementStat> = HashMap::new();
        for inst in &instances {
            for el in &inst.active_elements {
                stats.entry(el.element_id.clone()).or_default().active += 1;
            }
        }
        for (element_id, count) in &self.history.completed_counts {
            stats.entry(element_id.clone()).or_default().completed += *count;
        }
        // Cumulative sequence flows taken, as (from, to) element-id pairs, for
        // highlighting traversed connections (Play's `fetchSequenceFlows`). The
        // set is maintained incrementally and already deduplicated + sorted.
        let taken_sequence_flows: Vec<SequenceFlowDto> = self
            .history
            .taken_flows
            .iter()
            .map(|(from, to)| SequenceFlowDto {
                from: from.clone(),
                to: to.clone(),
            })
            .collect();
        for inc in &incidents {
            stats.entry(inc.element_id.clone()).or_default().incidents += 1;
        }
        let mut element_stats: Vec<ElementStatDto> = stats
            .into_iter()
            .map(|(element_id, s)| ElementStatDto {
                element_id,
                active: s.active,
                completed: s.completed,
                incidents: s.incidents,
            })
            .collect();
        element_stats.sort_by(|a, b| a.element_id.cmp(&b.element_id));

        // Evaluated decision instances (from `businessRuleTask`/DMN), collected
        // incrementally from the `DecisionEvaluated` audit events (Play's
        // `fetchDecisionInstances`). Not part of live engine state.
        let mut decision_instances: Vec<DecisionInstanceDto> = self.history.decisions.clone();
        decision_instances.sort_by(|a, b| cmp_key(&a.decision_key, &b.decision_key));

        // Unions for one-shot diagram highlighting.
        let mut active_element_ids: Vec<String> = instances
            .iter()
            .flat_map(|i| i.active_elements.iter().map(|e| e.element_id.clone()))
            .collect();
        active_element_ids.sort();
        active_element_ids.dedup();
        let mut incident_element_ids: Vec<String> =
            incidents.iter().map(|i| i.element_id.clone()).collect();
        incident_element_ids.sort();
        incident_element_ids.dedup();

        let total_instances = instances.len();
        let completed_instances = instances.iter().filter(|i| i.completed).count();

        let snap = Snapshot {
            now: self.now,
            event_count: self.seq,
            created: created.map(|k| k.to_string()),
            total_instances,
            completed_instances,
            instances,
            jobs,
            incidents,
            timers,
            user_tasks,
            message_subscriptions,
            signal_subscriptions,
            element_stats,
            taken_sequence_flows,
            decision_instances,
            active_element_ids,
            incident_element_ids,
        };
        serde_json::to_value(&snap).unwrap_or(serde_json::Value::Null)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    now: u64,
    event_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,
    total_instances: usize,
    completed_instances: usize,
    instances: Vec<InstanceDto>,
    jobs: Vec<JobDto>,
    incidents: Vec<IncidentDto>,
    timers: Vec<TimerDto>,
    user_tasks: Vec<UserTaskDto>,
    message_subscriptions: Vec<MessageSubscriptionDto>,
    signal_subscriptions: Vec<SignalSubscriptionDto>,
    element_stats: Vec<ElementStatDto>,
    taken_sequence_flows: Vec<SequenceFlowDto>,
    decision_instances: Vec<DecisionInstanceDto>,
    active_element_ids: Vec<String>,
    incident_element_ids: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstanceDto {
    key: String,
    process_id: String,
    state: String,
    completed: bool,
    active_elements: Vec<ActiveEl>,
    variables: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActiveEl {
    key: String,
    element_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobDto {
    key: String,
    instance_key: String,
    element_id: String,
    job_type: String,
    state: String,
    retries: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IncidentDto {
    key: String,
    instance_key: String,
    element_id: String,
    kind: String,
    reason: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimerDto {
    key: String,
    instance_key: String,
    element_id: String,
    due_at: u64,
    due_in_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UserTaskDto {
    key: String,
    instance_key: String,
    element_instance_key: String,
    element_id: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee: Option<String>,
    candidate_groups: Vec<String>,
    candidate_users: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    due_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    follow_up_date: Option<String>,
    priority: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageSubscriptionDto {
    key: String,
    instance_key: String,
    element_id: String,
    message_name: String,
    correlation_key: String,
    kind: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignalSubscriptionDto {
    key: String,
    instance_key: String,
    element_id: String,
    signal_name: String,
    kind: String,
}

/// Cumulative per-element counters accumulated while building a snapshot.
#[derive(Default)]
struct ElementStat {
    active: u64,
    completed: u64,
    incidents: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ElementStatDto {
    element_id: String,
    active: u64,
    completed: u64,
    incidents: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SequenceFlowDto {
    from: String,
    to: String,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DecisionInstanceDto {
    instance_key: String,
    element_id: String,
    decision_key: String,
    decision_id: String,
    output: serde_json::Value,
    evaluated_at: u64,
}

/// A stable camelCase discriminant for an incident kind, exposed to the UI
/// instead of Rust `Debug` output so variant renames or formatting changes
/// don't become breaking JSON-API changes.
fn incident_kind_tag(kind: &IncidentKind) -> &'static str {
    match kind {
        IncidentKind::JobNoRetries => "jobNoRetries",
        IncidentKind::NoMatchingSequenceFlow => "noMatchingSequenceFlow",
        IncidentKind::ExpressionEvaluation => "expressionEvaluation",
        IncidentKind::UnhandledError => "unhandledError",
        IncidentKind::DecisionEvaluation => "decisionEvaluation",
        IncidentKind::CalledElementError => "calledElementError",
        // Every `zeebe:ioMapping` failure (input or output, any element type)
        // shares the one external `ioMapping` taxonomy (REST `IO_MAPPING_ERROR`);
        // the internal resolution re-drive is phase-driven off the incident's
        // `redrive`, not a distinct kind (#946).
        IncidentKind::IoMapping => "ioMapping",
    }
}

/// A stable, camelCase discriminant string for a message/signal subscription
/// kind, exposed to the UI instead of Rust `Debug` output (which is brittle and
/// leaks struct fields). Play only distinguishes intermediate-catch from
/// boundary subscriptions, so the boundary element id is intentionally omitted.
fn subscription_kind_tag(kind: &MessageSubscriptionKind) -> &'static str {
    match kind {
        MessageSubscriptionKind::IntermediateCatch => "intermediateCatch",
        MessageSubscriptionKind::InterruptingBoundary { .. } => "interruptingBoundary",
        MessageSubscriptionKind::NonInterruptingBoundary { .. } => "nonInterruptingBoundary",
    }
}

/// Compare two decimal-string entity keys numerically (they are `u64` rendered
/// as decimal), falling back to lexicographic order for any non-numeric key so
/// snapshot ordering stays stable once keys grow past a single digit.
fn cmp_key(a: &str, b: &str) -> std::cmp::Ordering {
    match (a.parse::<u64>(), b.parse::<u64>()) {
        (Ok(x), Ok(y)) => x.cmp(&y),
        _ => a.cmp(b),
    }
}

fn instance_state(s: &ProcessInstanceState) -> String {
    match s {
        ProcessInstanceState::Active => "Active",
        ProcessInstanceState::Suspended => "Suspended",
        ProcessInstanceState::Terminating => "Terminating",
        ProcessInstanceState::Completed => "Completed",
        ProcessInstanceState::Terminated => "Terminated",
    }
    .to_string()
}

fn job_state(s: &JobState) -> String {
    match s {
        JobState::Created => "Created",
        JobState::Activated => "Activated",
        JobState::Completed => "Completed",
        JobState::Failed => "Failed",
        JobState::Errored => "Errored",
        JobState::Canceled => "Canceled",
    }
    .to_string()
}

fn user_task_state(s: &UserTaskState) -> String {
    match s {
        UserTaskState::Created => "Created",
        UserTaskState::Completed => "Completed",
        UserTaskState::Canceled => "Canceled",
    }
    .to_string()
}

fn js_err(msg: &str) -> JsValue {
    JsValue::from_str(msg)
}

fn to_json<T: Serialize>(v: &T) -> Result<String, JsValue> {
    serde_json::to_string(v).map_err(|e| js_err(&format!("serialize error: {e}")))
}

/// Parse a decimal key string into a `u64`.
fn parse_key(s: &str) -> Result<u64, JsValue> {
    s.trim()
        .parse::<u64>()
        .map_err(|_| js_err(&format!("invalid key: {s}")))
}

/// Whether an XML resource that failed *both* the BPMN and DMN parsers most
/// resembles DMN, used only to attribute the error message in [`TestEngine::deploy`]
/// (the happy path routes by successful parse, never by this heuristic). A DMN
/// document declares the DMN MODEL namespace; a BPMN document declares the BPMN
/// one — so the namespace substring cleanly distinguishes the two even when the
/// body is too malformed for either parser to accept.
fn resource_looks_like_dmn(xml: &str) -> bool {
    xml.contains("/spec/DMN/")
}

fn update_timeout_value(timeout_ms: f64) -> Result<i64, String> {
    if !timeout_ms.is_finite()
        || timeout_ms < i64::MIN as f64
        || timeout_ms.fract() != 0.0
        || timeout_ms >= i64::MAX as f64
    {
        return Err("update timeout must be an int64 number".into());
    }
    Ok(timeout_ms as i64)
}

// --- AgentInstance driver request shapes (Camunda 8.10 REST-parity JSON) ------
//
// The wasm agent drivers accept camelCase JSON mirroring the Camunda v2
// `/agent-instances` wire shape, with status/role/commit-status carried as their
// canonical REST spellings (e.g. `"INITIALIZING"`, `"ASSISTANT"`, `"COMMITTED"`).
// These request structs deserialize that JSON and convert into the engine-core
// agent types the lifecycle commands take.

/// A supplied canonical `limits` block requires all three members.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentLimitsReq {
    max_tokens: i64,
    max_model_calls: i32,
    max_tool_calls: i32,
}

impl From<AgentLimitsReq> for AgentInstanceLimits {
    fn from(r: AgentLimitsReq) -> Self {
        AgentInstanceLimits {
            max_tokens: r.max_tokens,
            max_model_calls: r.max_model_calls.into(),
            max_tool_calls: r.max_tool_calls.into(),
        }
    }
}

/// A `tools[]` entry of an update request: `{ name, description?, elementId? }`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentToolReq {
    name: String,
    #[serde(deserialize_with = "required_nullable")]
    description: Option<String>,
    #[serde(deserialize_with = "required_nullable")]
    element_id: Option<String>,
}

fn required_nullable<'de, T: serde::Deserialize<'de>, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<Option<T>, D::Error> {
    serde::Deserialize::deserialize(de)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentToolCallReq {
    tool_call_id: String,
    tool_name: String,
    #[serde(deserialize_with = "required_nullable")]
    element_id: Option<String>,
    #[serde(deserialize_with = "required_nullable")]
    arguments: Option<serde_json::Map<String, serde_json::Value>>,
}

impl From<AgentToolReq> for AgentTool {
    fn from(r: AgentToolReq) -> Self {
        AgentTool {
            name: r.name,
            description: r.description,
            element_id: r.element_id,
        }
    }
}

/// Parse an RFC-3339 `date-time` string into epoch milliseconds, mirroring the
/// grammar `is_rfc3339_date_time` validates (and the gateway's chrono parse):
/// `YYYY-MM-DDThh:mm:ss`, optional `.fraction` (rounded down to millis), and a
/// mandatory `Z`/`±hh:mm` offset. Computed without `chrono` to keep the wasm
/// engine date-crate-free. A leap second (`:60`) is clamped to `:59` for the
/// epoch arithmetic, matching how chrono normalises it. Returns the error message
/// as a `String` — kept pure so it is unit-testable on the host, where `JsValue`
/// errors cannot be constructed.
fn ms_from_rfc3339(s: &str) -> Result<u64, String> {
    let err = || {
        format!(
            "invalid producedAt {s:?}; expected an RFC-3339 \
             date-time string (e.g. \"2026-01-02T03:04:05Z\")"
        )
    };
    let b = s.as_bytes();
    // Shortest valid form "1970-01-01T00:00:00Z" is 20 bytes.
    if b.len() < 20 {
        return Err(err());
    }
    let digit = |c: u8| c.is_ascii_digit();
    let ok_fixed = digit(b[0])
        && digit(b[1])
        && digit(b[2])
        && digit(b[3])
        && b[4] == b'-'
        && digit(b[5])
        && digit(b[6])
        && b[7] == b'-'
        && digit(b[8])
        && digit(b[9])
        && (b[10] == b'T' || b[10] == b't')
        && digit(b[11])
        && digit(b[12])
        && b[13] == b':'
        && digit(b[14])
        && digit(b[15])
        && b[16] == b':'
        && digit(b[17])
        && digit(b[18]);
    if !ok_fixed {
        return Err(err());
    }
    let num = |slice: &[u8]| -> i64 {
        slice
            .iter()
            .fold(0i64, |a, &c| a * 10 + i64::from(c - b'0'))
    };
    let year = num(&b[0..4]);
    let month = num(&b[5..7]);
    let day = num(&b[8..10]);
    let hour = num(&b[11..13]);
    let min = num(&b[14..16]);
    let sec = num(&b[17..19]);
    if !(1..=12).contains(&month)
        || day < 1
        || day > i64::from(days_in_month(year as u32, month as u32))
        || hour > 23
        || min > 59
        || sec > 60
    {
        return Err(err());
    }
    let mut i = 19usize;
    let mut millis = 0i64;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let start = i;
        let mut scale = 0;
        while i < b.len() && digit(b[i]) {
            if scale < 3 {
                millis = millis * 10 + i64::from(b[i] - b'0');
                scale += 1;
            }
            i += 1;
        }
        if i == start {
            return Err(err());
        }
        while scale < 3 {
            millis *= 10;
            scale += 1;
        }
    }
    let (offset_min, consumed) = match b.get(i) {
        Some(&c) if c == b'Z' || c == b'z' => (0i64, i + 1),
        Some(&c)
            if (c == b'+' || c == b'-')
                && i + 6 <= b.len()
                && digit(b[i + 1])
                && digit(b[i + 2])
                && b[i + 3] == b':'
                && digit(b[i + 4])
                && digit(b[i + 5]) =>
        {
            let oh = num(&b[i + 1..i + 3]);
            let om = num(&b[i + 4..i + 6]);
            if oh > 23 || om > 59 {
                return Err(err());
            }
            let mag = oh * 60 + om;
            (if c == b'-' { -mag } else { mag }, i + 6)
        }
        _ => return Err(err()),
    };
    if consumed != b.len() {
        return Err(err());
    }
    let sec = sec.min(59);
    let days = days_from_civil(year, month, day);
    let total_secs = days * 86_400 + hour * 3600 + min * 60 + sec - offset_min * 60;
    let total_ms = total_secs * 1000 + millis;
    if total_ms < 0 {
        return Err(err());
    }
    Ok(total_ms as u64)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian civil date,
/// via Howard Hinnant's `days_from_civil`. Inverse of the civil-from-days
/// algorithm `iso8601_from_ms` uses; callers pass an already-validated date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[derive(serde::Deserialize)]
#[serde(
    tag = "contentType",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
enum AgentContentReq {
    Text {
        text: String,
    },
    Document {
        #[serde(rename = "documentReference")]
        document_reference: serde_json::Map<String, serde_json::Value>,
    },
    Object {
        object: serde_json::Value,
    },
}

/// A single history turn of a create/update request. Only the turn-specific
/// fields are accepted; the instance-derived context and the minted history key
/// are filled in engine-side.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentTurnReq {
    #[serde(deserialize_with = "positive_loop_iteration")]
    loop_iteration: i32,
    produced_at: String,
    role: String,
    content: Vec<AgentContentReq>,
    #[serde(default)]
    system_prompt: Option<Vec<AgentContentReq>>,
    history_item_id: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    tools: Option<Vec<AgentToolReq>>,
    #[serde(default)]
    limits: Option<AgentLimitsReq>,
    #[serde(default)]
    tool_calls: Option<Vec<AgentToolCallReq>>,
    #[serde(default)]
    metrics: Option<AgentMetricsReq>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentMetricsReq {
    #[serde(deserialize_with = "required_nullable")]
    input_tokens: Option<i64>,
    #[serde(deserialize_with = "required_nullable")]
    output_tokens: Option<i64>,
    #[serde(deserialize_with = "required_nullable")]
    duration_ms: Option<i64>,
}

impl From<AgentMetricsReq> for nanobpmn_engine_core::AgentHistoryMetrics {
    fn from(metrics: AgentMetricsReq) -> Self {
        Self {
            input_tokens: metrics.input_tokens,
            output_tokens: metrics.output_tokens,
            duration_ms: metrics.duration_ms,
            ..Self::default()
        }
    }
}

fn positive_loop_iteration<'de, D: serde::Deserializer<'de>>(de: D) -> Result<i32, D::Error> {
    let value = <i32 as serde::Deserialize>::deserialize(de)?;
    if value < 1 {
        return Err(serde::de::Error::custom("loopIteration must be positive"));
    }
    Ok(value)
}

/// Why reconciling the canonical `jobLeaseToken` with its deprecated legacy
/// alias `jobLease` failed. A plain enum (not a `JsValue`) so the reconcile rule
/// is unit-testable on the native target; the call sites map it to a `js_err`.
#[derive(Debug, PartialEq, Eq)]
enum LeaseReconcileError {
    /// Both names were supplied with differing values.
    Conflict,
    /// Neither name was supplied — the lease is required.
    Missing,
}

impl LeaseReconcileError {
    fn message(&self) -> &'static str {
        match self {
            Self::Conflict => {
                "jobLeaseToken and the deprecated legacy alias jobLease were both supplied with \
                 different values; supply only jobLeaseToken"
            }
            Self::Missing => "a non-empty jobLeaseToken is required",
        }
    }
}

/// Reconcile the canonical `jobLeaseToken` with its deprecated pre-8.10 legacy
/// alias `jobLease` at the WASM request boundary, mirroring the gateway's REST
/// reconciliation (#1283): prefer the canonical name, still honor a legacy-only
/// request (the deprecation window), accept both names when they agree, and
/// reject a request that supplies both with *differing* values loudly rather
/// than fencing on a stale token. A request supplying neither is rejected — the
/// lease is required. Unlike a `#[serde(alias)]` (which folds both spellings
/// onto one field and so rejects an equal dual-send as a duplicate field), this
/// accepts an equal pair, keeping the WASM window consistent with REST.
fn reconcile_agent_lease_token(
    canonical: Option<String>,
    legacy: Option<String>,
) -> Result<String, LeaseReconcileError> {
    match (canonical, legacy) {
        (Some(c), Some(l)) if c != l => Err(LeaseReconcileError::Conflict),
        (Some(c), _) => Ok(c),
        (None, Some(l)) => Ok(l),
        (None, None) => Err(LeaseReconcileError::Missing),
    }
}

/// The `createAgentInstance` request body.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateAgentInstanceReq {
    element_instance_key: String,
    job_key: String,
    #[serde(default)]
    job_lease_token: Option<String>,
    #[serde(default)]
    job_lease: Option<String>,
    #[serde(deserialize_with = "nonempty_agent_history")]
    history: Vec<AgentTurnReq>,
}

fn nonempty_agent_history<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<Vec<AgentTurnReq>, D::Error> {
    let history = <Vec<AgentTurnReq> as serde::Deserialize>::deserialize(de)?;
    if history.is_empty() {
        return Err(serde::de::Error::custom(
            "history must contain at least one item",
        ));
    }
    Ok(history)
}

/// The `updateAgentInstance` request body.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateAgentInstanceReq {
    element_instance_key: String,
    #[serde(default)]
    status: Option<String>,
    job_key: String,
    #[serde(default)]
    job_lease_token: Option<String>,
    #[serde(default)]
    job_lease: Option<String>,
    #[serde(default)]
    history: Option<Vec<AgentTurnReq>>,
}

/// Convert a batch of request turns into engine [`AgentHistoryTurn`]s, resolving
/// each REST role/content-type spelling. `job_key`/`job_lease` are the request's
/// job attribution, stamped onto every turn in the batch — mirroring the
/// gateway, which attributes the activation's `jobKey`/`jobLease` to each turn.
fn agent_turns_from(
    turns: Vec<AgentTurnReq>,
    job_key: u64,
    job_lease: &str,
) -> Result<Vec<AgentHistoryTurn>, JsValue> {
    turns
        .into_iter()
        .map(|t| agent_turn_from(t, job_key, job_lease))
        .collect()
}

fn agent_turn_from(
    t: AgentTurnReq,
    job_key: u64,
    job_lease: &str,
) -> Result<AgentHistoryTurn, JsValue> {
    let role = parse_agent_role(&t.role)?;
    let content = t
        .content
        .into_iter()
        .map(agent_content_from)
        .collect::<Result<Vec<_>, _>>()?;
    let mut changed_attributes = Vec::new();
    for (name, supplied) in [
        ("systemPrompt", t.system_prompt.is_some()),
        ("tools", t.tools.is_some()),
        ("model", t.model.is_some()),
        ("provider", t.provider.is_some()),
        ("maxTokens", t.limits.is_some()),
        ("maxModelCalls", t.limits.is_some()),
        ("maxToolCalls", t.limits.is_some()),
    ] {
        if supplied {
            changed_attributes.push(name.into());
        }
    }
    let system_prompt = t
        .system_prompt
        .map(|blocks| {
            blocks
                .into_iter()
                .map(agent_content_from)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    Ok(AgentHistoryTurn {
        changed_attributes,
        loop_iteration: t.loop_iteration,
        produced_at: ms_from_rfc3339(&t.produced_at).map_err(|e| js_err(&e))?,
        role,
        content,
        system_prompt,
        history_item_id: Some(t.history_item_id),
        model: t.model,
        provider: t.provider,
        job_key,
        tools: t
            .tools
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect(),
        limits: t.limits.map(Into::into),
        metrics: t.metrics.map(Into::into),
        tool_calls: t
            .tool_calls
            .unwrap_or_default()
            .into_iter()
            .map(|call| nanobpmn_engine_core::AgentHistoryToolCall {
                tool_call_id: call.tool_call_id,
                tool_name: call.tool_name,
                element_id: call.element_id,
                arguments: call
                    .arguments
                    .map(|v| serde_json::Value::Object(v).to_string()),
            })
            .collect(),
        job_lease: job_lease.to_owned(),
        ..Default::default()
    })
}

fn agent_content_from(c: AgentContentReq) -> Result<AgentHistoryContent, JsValue> {
    Ok(match c {
        AgentContentReq::Text { text } => AgentHistoryContent {
            content_type: AgentHistoryContentType::Text,
            text: Some(text),
            document_reference: None,
            object: None,
        },
        AgentContentReq::Document { document_reference } => AgentHistoryContent {
            content_type: AgentHistoryContentType::Document,
            text: None,
            document_reference: Some(serde_json::Value::Object(document_reference).to_string()),
            object: None,
        },
        AgentContentReq::Object { object } => AgentHistoryContent {
            content_type: AgentHistoryContentType::Object,
            text: None,
            document_reference: None,
            object: Some(object.to_string()),
        },
    })
}

/// Validate the narrower update-status enum before converting it to core state.
fn reject_non_updatable_status(status: Option<&str>) -> Result<(), String> {
    match status {
        None | Some("IDLE" | "THINKING" | "TOOL_CALLING" | "TOOL_DISCOVERY") => Ok(()),
        Some("COMPLETED") => Err(
            "updateAgentInstance: status COMPLETED is not settable via UPDATE; \
             complete the owning job with completeJob"
                .to_string(),
        ),
        Some(status) => Err(format!(
            "updateAgentInstance: status {status} is not settable via UPDATE"
        )),
    }
}

/// Parse a REST `AgentInstanceStatusEnum` spelling into the engine status.
fn parse_agent_status(s: &str) -> Result<AgentInstanceStatus, JsValue> {
    match s {
        "INITIALIZING" => Ok(AgentInstanceStatus::Initializing),
        "TOOL_DISCOVERY" => Ok(AgentInstanceStatus::ToolDiscovery),
        "THINKING" => Ok(AgentInstanceStatus::Thinking),
        "TOOL_CALLING" => Ok(AgentInstanceStatus::ToolCalling),
        "IDLE" => Ok(AgentInstanceStatus::Idle),
        "COMPLETED" => Ok(AgentInstanceStatus::Completed),
        other => Err(js_err(&format!(
            "invalid agent status {other:?}; expected one of \
             INITIALIZING, TOOL_DISCOVERY, THINKING, TOOL_CALLING, IDLE, COMPLETED"
        ))),
    }
}

/// Parse a REST `AgentHistoryItemRoleEnum` spelling into the engine role.
fn parse_agent_role(s: &str) -> Result<AgentHistoryRole, JsValue> {
    match s {
        "USER" => Ok(AgentHistoryRole::User),
        "ASSISTANT" => Ok(AgentHistoryRole::Assistant),
        "TOOL_RESULT" => Ok(AgentHistoryRole::ToolResult),
        "CONFIGURATION" => Ok(AgentHistoryRole::Configuration),
        other => Err(js_err(&format!(
            "invalid agent history role {other:?}; expected one of \
             USER, ASSISTANT, TOOL_RESULT, CONFIGURATION"
        ))),
    }
}

/// Parse the `activate_instructions_json` argument of [`TestEngine::modify`]: a
/// JSON array of `{ elementId: string, variables?: object }`. Empty/whitespace
/// ⇒ no activations.
fn parse_activate_instructions(s: &str) -> Result<Vec<ActivateElementInstruction>, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value = serde_json::from_str(t)
        .map_err(|e| js_err(&format!("invalid activate instructions JSON: {e}")))?;
    let serde_json::Value::Array(items) = json else {
        return Err(js_err("activate instructions must be a JSON array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let serde_json::Value::Object(map) = item else {
            return Err(js_err("each activate instruction must be a JSON object"));
        };
        let element_id = match map.get("elementId") {
            Some(serde_json::Value::String(id)) if !id.is_empty() => id.clone(),
            _ => {
                return Err(js_err(
                    "activate instruction requires a non-empty elementId",
                ));
            }
        };
        let variables = match map.get("variables") {
            None | Some(serde_json::Value::Null) => HashMap::new(),
            Some(serde_json::Value::Object(vars)) => vars
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
            Some(_) => {
                return Err(js_err(
                    "activate instruction variables must be a JSON object",
                ));
            }
        };
        out.push(ActivateElementInstruction {
            element_id,
            variables,
        });
    }
    Ok(out)
}

/// Parse the `agent_result_json` of [`TestEngine::complete_agent_job`] into an
/// [`AdHocJobResult`]. Shape (camelCase, mirroring Camunda's agentic
/// `JobResult`):
/// `{ "activateElements": [{ "elementId": string, "variables"?: object }],
///    "completionConditionFulfilled"?: bool, "cancelRemainingInstances"?: bool }`.
/// Empty/whitespace ⇒ an empty (no-op) result, so it degrades to a plain
/// completion (the container ends its turn with nothing activated).
fn parse_adhoc_result(s: &str) -> Result<AdHocJobResult, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(AdHocJobResult::default());
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| js_err(&format!("invalid agent result JSON: {e}")))?;
    let serde_json::Value::Object(map) = json else {
        return Err(js_err("agent result must be a JSON object"));
    };
    let activate_elements = match map.get("activateElements") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let serde_json::Value::Object(obj) = item else {
                    return Err(js_err("each activateElements entry must be a JSON object"));
                };
                let element_id = match obj.get("elementId") {
                    Some(serde_json::Value::String(id)) if !id.is_empty() => id.clone(),
                    _ => {
                        return Err(js_err(
                            "activateElements entry requires a non-empty elementId",
                        ));
                    }
                };
                let variables = match obj.get("variables") {
                    None | Some(serde_json::Value::Null) => HashMap::new(),
                    Some(serde_json::Value::Object(vars)) => vars
                        .iter()
                        .map(|(k, v)| (k.clone(), json_to_value(v)))
                        .collect(),
                    Some(_) => {
                        return Err(js_err(
                            "activateElements entry variables must be a JSON object",
                        ));
                    }
                };
                out.push(AdHocActivateElement {
                    element_id,
                    variables,
                });
            }
            out
        }
        Some(_) => return Err(js_err("activateElements must be a JSON array")),
    };
    let flag = |k: &str| -> Result<bool, JsValue> {
        match map.get(k) {
            None | Some(serde_json::Value::Null) => Ok(false),
            Some(serde_json::Value::Bool(b)) => Ok(*b),
            Some(_) => Err(js_err(&format!("agent result `{k}` must be a boolean"))),
        }
    };
    Ok(AdHocJobResult {
        activate_elements,
        completion_condition_fulfilled: flag("completionConditionFulfilled")?,
        cancel_remaining_instances: flag("cancelRemainingInstances")?,
    })
}

/// Parse the `terminate_instructions_json` argument of [`TestEngine::modify`]: a
/// JSON array of element-instance keys, each a decimal string, a number, or a
/// `{ elementInstanceKey: string|number }` object. Empty/whitespace ⇒ none.
fn parse_terminate_instructions(s: &str) -> Result<Vec<u64>, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value = serde_json::from_str(t)
        .map_err(|e| js_err(&format!("invalid terminate instructions JSON: {e}")))?;
    let serde_json::Value::Array(items) = json else {
        return Err(js_err("terminate instructions must be a JSON array"));
    };
    items.iter().map(json_to_element_instance_key).collect()
}

/// Coerce one terminate-instruction entry (string, number, or
/// `{ elementInstanceKey }` object) into an element-instance key.
fn json_to_element_instance_key(v: &serde_json::Value) -> Result<u64, JsValue> {
    match v {
        serde_json::Value::String(s) => parse_key(s),
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| js_err(&format!("invalid element instance key: {n}"))),
        serde_json::Value::Object(map) => match map.get("elementInstanceKey") {
            Some(inner) => json_to_element_instance_key(inner),
            None => Err(js_err(
                "terminate instruction requires an elementInstanceKey",
            )),
        },
        _ => Err(js_err(
            "terminate instruction must be a key string, number or object",
        )),
    }
}

/// Parse a JSON array of `{ sourceElementId, targetElementId }` objects into the
/// `(source, target)` element-id pairs a `MigrateInstance` command carries.
/// Empty/whitespace ⇒ no mappings.
fn parse_mapping_instructions(s: &str) -> Result<Vec<(String, String)>, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value = serde_json::from_str(t)
        .map_err(|e| js_err(&format!("invalid mapping instructions JSON: {e}")))?;
    let serde_json::Value::Array(items) = json else {
        return Err(js_err("mapping instructions must be a JSON array"));
    };
    items
        .iter()
        .map(|v| {
            let serde_json::Value::Object(map) = v else {
                return Err(js_err(
                    "mapping instruction must be a { sourceElementId, targetElementId } object",
                ));
            };
            let source = map.get("sourceElementId").and_then(|x| x.as_str());
            let target = map.get("targetElementId").and_then(|x| x.as_str());
            match (source, target) {
                (Some(source), Some(target)) => Ok((source.to_string(), target.to_string())),
                _ => Err(js_err(
                    "mapping instruction requires string sourceElementId and targetElementId",
                )),
            }
        })
        .collect()
}

/// Parse a JSON object string into engine variables. Empty/whitespace ⇒ none.
fn parse_vars(s: &str) -> Result<HashMap<String, Value>, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(HashMap::new());
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| js_err(&format!("invalid variables JSON: {e}")))?;
    match json {
        serde_json::Value::Object(map) => Ok(map
            .into_iter()
            .map(|(k, v)| (k, json_to_value(&v)))
            .collect()),
        _ => Err(js_err("variables must be a JSON object")),
    }
}

/// Parse the `breakpoints_json` argument of the `debug*` methods: a JSON array of
/// `{ kind, id? }` objects into [`BreakCondition`]s. Empty/whitespace ⇒ no
/// breakpoints (the run drains to completion). `kind` is one of
/// `elementActivated`, `elementCompleted`, `processCompleted`, `everyStep`; the
/// first two require an `id`.
fn parse_breakpoints(s: &str) -> Result<Vec<BreakCondition>, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| js_err(&format!("invalid breakpoints JSON: {e}")))?;
    let serde_json::Value::Array(items) = json else {
        return Err(js_err("breakpoints must be a JSON array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let serde_json::Value::Object(obj) = item else {
            return Err(js_err("each breakpoint must be a JSON object"));
        };
        let kind = match obj.get("kind") {
            Some(serde_json::Value::String(k)) => k.as_str(),
            _ => return Err(js_err("breakpoint `kind` must be a string")),
        };
        let id = || match obj.get("id") {
            Some(serde_json::Value::String(id)) => Ok(id.clone()),
            _ => Err(js_err(&format!(
                "breakpoint kind `{kind}` requires a string `id`"
            ))),
        };
        out.push(match kind {
            "elementActivated" => BreakCondition::ElementActivated(id()?),
            "elementCompleted" => BreakCondition::ElementCompleted(id()?),
            "processCompleted" => BreakCondition::ProcessCompleted,
            "everyStep" => BreakCondition::EveryStep,
            other => return Err(js_err(&format!("unknown breakpoint kind: {other}"))),
        });
    }
    Ok(out)
}

/// Parse a JSON changeset object for `updateUserTask` into a [`UserTaskChangeset`].
/// Only keys present in the object become `Some`; absent keys leave that
/// attribute unchanged. `dueDate`/`followUpDate` accept a string, or `null`/`""`
/// to clear the attribute.
fn parse_user_task_changeset(s: &str) -> Result<UserTaskChangeset, JsValue> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(UserTaskChangeset::default());
    }
    let json: serde_json::Value =
        serde_json::from_str(t).map_err(|e| js_err(&format!("invalid changeset JSON: {e}")))?;
    let obj = match json {
        serde_json::Value::Object(map) => map,
        _ => return Err(js_err("changeset must be a JSON object")),
    };

    let string_list = |v: &serde_json::Value| -> Result<Vec<String>, JsValue> {
        match v {
            serde_json::Value::Array(items) => items
                .iter()
                .map(|i| {
                    i.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| js_err("candidate list entries must be strings"))
                })
                .collect(),
            _ => Err(js_err("candidate groups/users must be a JSON array")),
        }
    };
    // A nullable date field: `Some(None)` clears, `Some(Some(s))` sets, absent leaves alone.
    let opt_date = |v: &serde_json::Value| -> Result<Option<String>, JsValue> {
        match v {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(s) if s.is_empty() => Ok(None),
            serde_json::Value::String(s) => Ok(Some(s.clone())),
            _ => Err(js_err("date fields must be a string or null")),
        }
    };

    let mut changeset = UserTaskChangeset::default();
    if let Some(v) = obj.get("candidateGroups") {
        changeset.candidate_groups = Some(string_list(v)?);
    }
    if let Some(v) = obj.get("candidateUsers") {
        changeset.candidate_users = Some(string_list(v)?);
    }
    if let Some(v) = obj.get("dueDate") {
        changeset.due_date = Some(opt_date(v)?);
    }
    if let Some(v) = obj.get("followUpDate") {
        changeset.follow_up_date = Some(opt_date(v)?);
    }
    if let Some(v) = obj.get("priority") {
        let p = v
            .as_i64()
            .ok_or_else(|| js_err("priority must be an integer"))?;
        if !(0..=100).contains(&p) {
            return Err(js_err("priority must be in the range 0..=100"));
        }
        changeset.priority = Some(p as i32);
    }
    Ok(changeset)
}

/// Convert engine variables to a natural JSON object (not the tagged enum form).
fn vars_to_json(vars: &HashMap<String, Value>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    let mut keys: Vec<&String> = vars.keys().collect();
    keys.sort();
    for k in keys {
        map.insert(k.clone(), value_to_json(&vars[k]));
    }
    serde_json::Value::Object(map)
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let mut m = serde_json::Map::new();
            for (k, val) in entries {
                m.insert(k.clone(), value_to_json(val));
            }
            serde_json::Value::Object(m)
        }
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::Int(u as i64)
            } else {
                Value::Double(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        serde_json::Value::Object(map) => Value::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value as J;

    use super::*;

    fn parse(s: &str) -> J {
        serde_json::from_str(s).expect("valid JSON")
    }

    const USER_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    const SERVICE_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="do-work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="b" sourceRef="work" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    fn only_user_task_key(snap: &J) -> String {
        let tasks = snap["userTasks"].as_array().expect("userTasks array");
        assert_eq!(tasks.len(), 1, "expected one user task: {snap}");
        tasks[0]["key"].as_str().unwrap().to_string()
    }

    #[test]
    fn update_timeout_accepts_signed_int64_values() {
        for value in [-5, 0, 5] {
            assert_eq!(update_timeout_value(value as f64).unwrap(), value);
        }
        assert_eq!(update_timeout_value(i64::MIN as f64).unwrap(), i64::MIN);
        for invalid in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.5,
            i64::MAX as f64,
            (i64::MIN as f64) * 2.0,
        ] {
            assert!(update_timeout_value(invalid).is_err());
        }
    }

    #[test]
    fn signed_update_timeout_preserves_the_current_lease() {
        for timeout in [-5, 0, 5] {
            let mut eng = TestEngine::new();
            eng.deploy(include_str!(
                "../../engine-core/tests/fixtures/external-agent-job-type.bpmn"
            ))
            .unwrap();
            eng.create_instance("external-agent-routing", "{}", None)
                .unwrap();
            eng.advance_time(10.0).unwrap();
            let jobs = parse(
                &eng.activate_jobs("senior:rebase", 1, 100.0, "W", Some(true))
                    .unwrap(),
            );
            let key = jobs[0]["key"].as_str().unwrap();
            let lease = jobs[0]["jobLeaseToken"].as_str().unwrap();
            eng.update_timeout(key, timeout as f64, Some(lease.into()))
                .unwrap();
            assert_eq!(
                eng.engine.state().jobs[&key.parse::<u64>().unwrap()].deadline,
                Some((10 + timeout) as u64)
            );
            eng.advance_time(0.0).unwrap();
            let job = &eng.engine.state().jobs[&key.parse::<u64>().unwrap()];
            assert_eq!(
                job.state,
                if timeout <= 0 {
                    JobState::Created
                } else {
                    JobState::Activated
                }
            );
            assert_eq!(job.lease_token.as_deref(), Some(lease));
            eng.complete_job(key, "{}", Some(lease.into())).unwrap();
        }
    }

    #[test]
    fn implicit_activation_does_not_lock_other_jobs_of_the_same_type() {
        let mut eng = TestEngine::new();
        eng.deploy(include_str!(
            "../../engine-core/tests/fixtures/external-agent-job-type.bpmn"
        ))
        .unwrap();
        for _ in 0..2 {
            eng.create_instance("external-agent-routing", "{}", None)
                .unwrap();
        }
        let keys: Vec<_> = eng.engine.state().jobs.keys().copied().collect();
        assert_eq!(keys.len(), 2);
        eng.complete_job(&keys[1].to_string(), "{}", None).unwrap();
        assert_eq!(eng.engine.state().jobs[&keys[0]].state, JobState::Created);
    }

    #[test]
    fn user_task_lifecycle_and_snapshot() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());

        // A Created user task is surfaced, parked on the `review` element.
        let task = &snap["userTasks"][0];
        assert_eq!(task["state"], "Created");
        assert_eq!(task["elementId"], "review");
        assert_eq!(task["priority"], 50);
        // Its element is highlighted as active and the entry flow is recorded.
        assert!(snap["activeElementIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "review"));
        assert!(snap["takenSequenceFlows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["from"] == "s" && f["to"] == "review"));

        let key = only_user_task_key(&snap);

        // Assign / unassign flow.
        let snap = parse(&eng.assign_user_task(&key, "alice", false).unwrap());
        assert_eq!(snap["userTasks"][0]["assignee"], "alice");
        let snap = parse(&eng.unassign_user_task(&key).unwrap());
        assert!(snap["userTasks"][0].get("assignee").is_none());

        // Update attributes from a JSON changeset.
        let snap = parse(
            &eng.update_user_task(
                &key,
                r#"{"candidateGroups":["ops"],"priority":80,"dueDate":"2026-01-01"}"#,
            )
            .unwrap(),
        );
        assert_eq!(snap["userTasks"][0]["candidateGroups"][0], "ops");
        assert_eq!(snap["userTasks"][0]["priority"], 80);
        assert_eq!(snap["userTasks"][0]["dueDate"], "2026-01-01");

        // Completing resumes the token, completes the instance, and records the
        // exit sequence flow + element-completed statistic.
        let snap = parse(
            &eng.complete_user_task(&key, r#"{"approved":true}"#)
                .unwrap(),
        );
        assert_eq!(snap["userTasks"][0]["state"], "Completed");
        assert_eq!(snap["completedInstances"], 1);
        assert!(snap["takenSequenceFlows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["from"] == "review" && f["to"] == "e"));
        let review_stat = snap["elementStats"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["elementId"] == "review")
            .expect("review stat");
        assert_eq!(review_stat["completed"], 1);
    }

    #[test]
    fn generic_lease_options_are_sticky_and_survive_trace_replay() {
        let source = include_str!("../../engine-core/tests/fixtures/external-agent-job-type.bpmn");
        for marker in [
            None,
            Some("external"),
            Some("aiAgentTask"),
            Some("listener"),
        ] {
            let mut xml = source.replace(
                "<zeebe:agentDefinition agentType=\"external\"/>",
                &marker
                    .filter(|m| *m != "listener")
                    .map(|m| format!("<zeebe:agentDefinition agentType=\"{m}\"/>"))
                    .unwrap_or_default(),
            );
            let job_type = if marker == Some("listener") {
                xml = xml.replace("<zeebe:taskDefinition type=\"senior:rebase\"/>",
                    "<zeebe:taskDefinition type=\"senior:rebase\"/><zeebe:executionListeners><zeebe:executionListener eventType=\"start\" type=\"before\"/></zeebe:executionListeners>");
                "before"
            } else {
                "senior:rebase"
            };
            let mut engine = TestEngine::new();
            engine.deploy(&xml).unwrap();
            engine
                .create_instance("external-agent-routing", "{}", None)
                .unwrap();
            let jobs = parse(&engine.activate_jobs(job_type, 1, 100.0, "W", None).unwrap());
            assert!(jobs[0].get("jobLeaseToken").unwrap().is_null());
            assert!(jobs[0].get("jobLease").is_none());
            let key = jobs[0]["key"].as_str().unwrap();
            engine.fail_job(key, 2, "retry", None).unwrap();
            let jobs = parse(
                &engine
                    .activate_jobs(job_type, 1, 100.0, "W", Some(true))
                    .unwrap(),
            );
            let first = jobs[0]["jobLeaseToken"].as_str().unwrap();
            engine
                .fail_job(key, 2, "retry", Some(first.into()))
                .unwrap();
            let mut replayed = TestEngine::new();
            replayed.replay_events(&engine.events().unwrap()).unwrap();
            assert_eq!(
                parse(
                    &engine
                        .activate_jobs(job_type, 1, 100.0, "W", Some(false))
                        .unwrap()
                ),
                serde_json::json!([])
            );
            assert_eq!(
                parse(
                    &replayed
                        .activate_jobs(job_type, 1, 100.0, "W", None)
                        .unwrap()
                ),
                serde_json::json!([])
            );
            let jobs = parse(
                &engine
                    .activate_jobs(job_type, 1, 100.0, "W", Some(true))
                    .unwrap(),
            );
            let replayed_jobs = parse(
                &replayed
                    .activate_jobs(job_type, 1, 100.0, "W", Some(true))
                    .unwrap(),
            );
            assert_eq!(jobs[0]["jobLeaseToken"], replayed_jobs[0]["jobLeaseToken"]);
            assert!(
                jobs[0]["jobLeaseToken"].is_string(),
                "{marker:?}: sticky activation lost lease: {jobs}"
            );
            let second = jobs[0]["jobLeaseToken"].as_str().unwrap();
            assert_ne!(first, second);
            engine.complete_job(key, "{}", Some(second.into())).unwrap();
            replayed
                .complete_job(key, "{}", Some(second.into()))
                .unwrap();
        }
    }

    #[test]
    fn trace_replay_rejects_unknown_events_and_missing_prefix() {
        assert!(decode_trace(r#"[{"seq":1,"now":0,"type":"UnknownFutureEvent"}]"#).is_err());
        assert!(decode_trace(r#"[{"seq":2,"now":0,"type":"UnknownFutureEvent"}]"#).is_err());
    }

    #[test]
    fn throw_error_without_boundary_raises_incident() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let job_key = snap["jobs"][0]["key"].as_str().unwrap().to_string();

        // Throwing an uncaught business error consumes the job and raises an
        // incident visible on the `work` element.
        let snap = parse(&eng.throw_error(&job_key, "BOOM", "kaboom", None).unwrap());
        let incidents = snap["incidents"].as_array().unwrap();
        assert_eq!(incidents.len(), 1, "expected one incident: {snap}");
        assert_eq!(incidents[0]["elementId"], "work");
        assert!(snap["incidentElementIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "work"));
    }

    #[test]
    fn fail_update_retries_resolve_incident_recovers_job() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let job_key = snap["jobs"][0]["key"].as_str().unwrap().to_string();

        // Fail with no retries left → incident.
        let snap = parse(&eng.fail_job(&job_key, 0, "nope", None).unwrap());
        let incident_key = snap["incidents"][0]["key"].as_str().unwrap().to_string();

        // Give the job a retry, then resolve the incident to re-create the job.
        eng.update_retries(&job_key, 1, None).unwrap();
        let snap = parse(&eng.resolve_incident(&incident_key).unwrap());
        assert!(
            snap["incidents"].as_array().unwrap().is_empty(),
            "incident should be resolved: {snap}"
        );
        assert!(
            !snap["jobs"].as_array().unwrap().is_empty(),
            "job should be activatable again: {snap}"
        );
    }

    #[test]
    fn activate_jobs_returns_only_this_calls_jobs_bounded_by_max() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        // Two instances → two `do-work` jobs waiting to be activated.
        eng.create_instance("p", "{}", None).unwrap();
        eng.create_instance("p", "{}", None).unwrap();

        // First activation with the same worker locks exactly one job.
        let first: Vec<J> = serde_json::from_str(
            &eng.activate_jobs("do-work", 1, 30_000.0, "w1", None)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            first.len(),
            1,
            "first call must respect max_jobs=1: {first:?}"
        );
        let first_key = first[0]["key"].as_str().unwrap().to_string();

        // Second activation with the *same* worker must return only the job it
        // newly locks — not the one already locked by the first call. Scanning
        // all `Activated` jobs for the worker would leak `first_key` back and
        // exceed `max_jobs`.
        let second: Vec<J> = serde_json::from_str(
            &eng.activate_jobs("do-work", 1, 30_000.0, "w1", None)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            second.len(),
            1,
            "second call must respect max_jobs=1 and not re-return earlier jobs: {second:?}"
        );
        let second_key = second[0]["key"].as_str().unwrap().to_string();
        assert_ne!(
            first_key, second_key,
            "each activation call must return distinct, freshly-locked jobs"
        );
    }

    #[test]
    fn set_variables_merges_into_instance() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", r#"{"a":1}"#, None).unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();

        let snap = parse(
            &eng.set_variables(&instance_key, r#"{"b":2}"#, false)
                .unwrap(),
        );
        let vars = &snap["instances"][0]["variables"];
        assert_eq!(vars["a"], 1);
        assert_eq!(vars["b"], 2);
    }

    #[test]
    fn cancel_instance_terminates() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();

        let snap = parse(&eng.cancel_instance(&instance_key).unwrap());
        assert_eq!(snap["instances"][0]["state"], "Terminated");
        assert!(snap["userTasks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["state"] != "Created"));
    }

    const TWO_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="a">
            <bpmn:extensionElements><zeebe:taskDefinition type="ja" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:serviceTask id="b">
            <bpmn:extensionElements><zeebe:taskDefinition type="jb" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="a" />
          <bpmn:sequenceFlow id="f2" sourceRef="a" targetRef="b" />
          <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    fn active_eik(snap: &J, element_id: &str) -> String {
        snap["instances"][0]["activeElements"]
            .as_array()
            .expect("activeElements array")
            .iter()
            .find(|el| el["elementId"] == element_id)
            .unwrap_or_else(|| panic!("no active element {element_id}: {snap}"))["key"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn modify_moves_a_token_between_elements() {
        let mut eng = TestEngine::new();
        eng.deploy(TWO_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();
        let a_eik = active_eik(&snap, "a");

        // Terminate the token on `a` and activate one on `b`, merging a variable.
        let snap = parse(
            &eng.modify(
                &instance_key,
                r#"[{"elementId":"b","variables":{"approved":true}}]"#,
                &format!("[\"{a_eik}\"]"),
            )
            .unwrap(),
        );

        assert_eq!(snap["instances"][0]["state"], "Active");
        // Token now rests on `b` (a fresh `jb` job), not `a`.
        assert!(snap["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["elementId"] == "b" && j["jobType"] == "jb"));
        assert!(snap["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|j| j["elementId"] != "a"));
        assert_eq!(snap["instances"][0]["variables"]["approved"], true);
    }

    #[test]
    fn modify_terminating_last_token_terminates_instance() {
        let mut eng = TestEngine::new();
        eng.deploy(SERVICE_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();
        let work_eik = active_eik(&snap, "work");

        let snap = parse(
            &eng.modify(&instance_key, "[]", &format!("[\"{work_eik}\"]"))
                .unwrap(),
        );
        assert_eq!(snap["instances"][0]["state"], "Terminated");
        assert!(snap["jobs"].as_array().unwrap().is_empty());
    }

    const ADHOC_AGENT_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    fn job_key(snap: &J, element_id: &str, job_type: &str) -> String {
        snap["jobs"]
            .as_array()
            .expect("jobs array")
            .iter()
            .find(|j| j["elementId"] == element_id && j["jobType"] == job_type)
            .unwrap_or_else(|| panic!("no {job_type} job for {element_id}: {snap}"))["key"]
            .as_str()
            .unwrap()
            .to_string()
    }

    // The browser seam for Camunda agentic ad-hoc sub-processes: `completeAgentJob`
    // carries the agent's `activateElements[]` so the engine runs the chosen inner
    // tools, loops, and completes the container — behaviour the plain `completeJob`
    // (which always sends `adhoc_result: None`) cannot drive. Mirrors the engine-core
    // test `adhoc_agent_activates_tools_loops_and_completes_with_output_collection`.
    #[test]
    fn complete_agent_job_activates_tools_loops_and_completes() {
        let mut eng = TestEngine::new();
        eng.deploy(ADHOC_AGENT_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());

        // The container emitted its agent job; no tools are active yet.
        let agent = job_key(&snap, "agent", "agent-worker");
        assert!(
            !snap["jobs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|j| j["jobType"] == "tool"),
            "no tool jobs before the first agent turn: {snap}"
        );

        // Turn 1: the agent activates both tools → each becomes a real `tool` job.
        let snap = parse(
            &eng.complete_agent_job(
                &agent,
                "{}",
                r#"{"activateElements":[{"elementId":"toolA"},{"elementId":"toolB"}]}"#,
                None,
            )
            .unwrap(),
        );
        assert_eq!(snap["instances"][0]["state"], "Active", "container parks");
        let tool_a = job_key(&snap, "toolA", "tool");
        let tool_b = job_key(&snap, "toolB", "tool");

        // Drain both tool jobs, each producing a `result` captured via outputElement.
        eng.complete_job(&tool_a, r#"{"result":"A"}"#, None)
            .unwrap();
        let snap = parse(
            &eng.complete_job(&tool_b, r#"{"result":"B"}"#, None)
                .unwrap(),
        );
        assert!(
            !snap["jobs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|j| j["jobType"] == "tool"),
            "all tools drained: {snap}"
        );

        // The agent job re-emitted for the next turn.
        let agent2 = job_key(&snap, "agent", "agent-worker");

        // Turn 2: the agent signals completion → the container completes, writing
        // its `outputCollection`, and the instance finishes.
        let snap = parse(
            &eng.complete_agent_job(
                &agent2,
                "{}",
                r#"{"completionConditionFulfilled":true}"#,
                None,
            )
            .unwrap(),
        );
        assert!(
            !snap["instances"]
                .as_array()
                .unwrap()
                .iter()
                .any(|i| i["state"] == "Active"),
            "no active instance remains after the agent completes: {snap}"
        );
        assert!(
            snap["jobs"].as_array().unwrap().is_empty(),
            "no jobs remain: {snap}"
        );
    }

    // A whitespace/empty agent result degrades to a plain completion: the container
    // ends its turn with nothing activated and finishes.
    #[test]
    fn complete_agent_job_empty_result_completes_container() {
        let mut eng = TestEngine::new();
        eng.deploy(ADHOC_AGENT_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let agent = job_key(&snap, "agent", "agent-worker");

        let snap = parse(&eng.complete_agent_job(&agent, "{}", "", None).unwrap());
        assert!(
            !snap["instances"]
                .as_array()
                .unwrap()
                .iter()
                .any(|i| i["state"] == "Active"),
            "empty result completes the container: {snap}"
        );
    }

    const DEBUG_TWO_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="first">
            <bpmn:extensionElements><zeebe:taskDefinition type="t1" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:serviceTask id="second">
            <bpmn:extensionElements><zeebe:taskDefinition type="t2" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="first" />
          <bpmn:sequenceFlow id="b" sourceRef="first" targetRef="second" />
          <bpmn:sequenceFlow id="c" sourceRef="second" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    /// A breakpoint on the start event pauses the debug run before the first job
    /// is parked, and the reported `activeElements` names the paused element.
    #[test]
    fn debug_breakpoint_pauses_and_reports_active_element() {
        let mut eng = TestEngine::new();
        eng.deploy(DEBUG_TWO_TASK_XML).unwrap();

        let state = parse(
            &eng.debug_create_instance("p", "{}", r#"[{"kind":"elementActivated","id":"s"}]"#)
                .unwrap(),
        );
        assert_eq!(
            state["paused"], true,
            "paused at the start-event breakpoint"
        );
        assert!(eng.debug_is_paused());
        let active = state["activeElements"].as_array().unwrap();
        assert!(
            active.iter().any(|e| e == "s"),
            "start event is highlighted while paused: {state}"
        );
        // No job has been parked yet (we stopped before the service task).
        let snap = parse(&eng.snapshot().unwrap());
        assert!(
            snap["jobs"]
                .as_array()
                .map(|j| j.is_empty())
                .unwrap_or(true),
            "no job parked at the start-event breakpoint: {snap}"
        );
    }

    /// Resuming with no further breakpoints drains to quiescence; the mirrored
    /// event log then matches a plain (non-debug) `createInstance` run — RTC parity
    /// across the wasm boundary.
    #[test]
    fn debug_resume_matches_plain_create_instance() {
        let plain = {
            let mut eng = TestEngine::new();
            eng.deploy(DEBUG_TWO_TASK_XML).unwrap();
            eng.create_instance("p", "{}", None).unwrap();
            parse(&eng.events().unwrap())
        };

        let mut eng = TestEngine::new();
        eng.deploy(DEBUG_TWO_TASK_XML).unwrap();
        eng.debug_create_instance("p", "{}", r#"[{"kind":"elementActivated","id":"s"}]"#)
            .unwrap();
        assert!(eng.debug_is_paused());
        let state = parse(&eng.debug_resume().unwrap());
        assert_eq!(state["paused"], false, "resumed to quiescence");
        assert!(!eng.debug_is_paused());

        let debugged = parse(&eng.events().unwrap());
        assert_eq!(
            plain, debugged,
            "RTC parity across wasm: debug run == plain createInstance"
        );
        // Parked on the first job, exactly as the plain run leaves it.
        let snap = parse(&eng.snapshot().unwrap());
        let jobs = snap["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["jobType"], "t1");
    }

    /// Single-stepping from the first step to quiescence yields the same event log
    /// as the atomic run — the wasm surface drives the real steps, one at a time.
    #[test]
    fn debug_single_step_to_completion() {
        let plain = {
            let mut eng = TestEngine::new();
            eng.deploy(DEBUG_TWO_TASK_XML).unwrap();
            eng.create_instance("p", "{}", None).unwrap();
            parse(&eng.events().unwrap())
        };

        let mut eng = TestEngine::new();
        eng.deploy(DEBUG_TWO_TASK_XML).unwrap();
        eng.debug_create_instance("p", "{}", r#"[{"kind":"everyStep"}]"#)
            .unwrap();
        assert!(eng.debug_is_paused(), "pauses after the first step");

        let mut guard = 0;
        while eng.debug_is_paused() {
            eng.debug_step().unwrap();
            guard += 1;
            assert!(guard < 1000, "single-stepping must terminate");
        }
        let debugged = parse(&eng.events().unwrap());
        assert_eq!(
            plain, debugged,
            "RTC parity across wasm: single-stepped run == plain createInstance"
        );
    }

    /// While a debug run is paused mid-command the engine holds an intermediate
    /// state; other mutating calls must be rejected so they can't build on a
    /// state a normal RTC engine can't represent. `debugClear` lifts the block by
    /// *finishing* the in-flight command (RTC parity), not by stranding the
    /// partial state. (Asserted against `check_not_paused`, the `&str`-typed core
    /// every mutator funnels through via `guard_paused` — the `JsValue` wrapper
    /// aborts off the wasm target, so the mutators' own reject paths aren't
    /// native-testable.)
    #[test]
    fn paused_debug_run_rejects_other_mutators() {
        // Reference: the RTC state a plain createInstance leaves (parked on t1).
        let plain = {
            let mut eng = TestEngine::new();
            eng.deploy(DEBUG_TWO_TASK_XML).unwrap();
            eng.create_instance("p", "{}", None).unwrap();
            parse(&eng.events().unwrap())
        };

        let mut eng = TestEngine::new();
        eng.deploy(DEBUG_TWO_TASK_XML).unwrap();
        eng.debug_create_instance("p", "{}", r#"[{"kind":"elementActivated","id":"s"}]"#)
            .unwrap();
        assert!(
            eng.debug_is_paused(),
            "paused at the start-event breakpoint"
        );
        assert!(
            eng.check_not_paused().is_err(),
            "mutators are rejected while a debug run is paused"
        );

        // Clearing while paused finishes the command (RTC parity) and lifts the block.
        eng.debug_clear();
        assert!(!eng.debug_is_paused());
        assert_eq!(
            plain,
            parse(&eng.events().unwrap()),
            "debugClear drains the in-flight command to the plain-createInstance RTC state, \
             not a stranded partial one"
        );
        assert!(
            eng.check_not_paused().is_ok(),
            "mutators allowed again once the debug run is cleared"
        );
        assert!(
            eng.advance_time(1000.0).is_ok(),
            "a real mutator succeeds once unblocked"
        );
    }

    #[test]
    fn update_agent_instance_rejects_completed_status() {
        let msg = reject_non_updatable_status(Some("COMPLETED"))
            .expect_err("status COMPLETED must be rejected");
        assert!(
            msg.contains("COMPLETED") && msg.contains("completeJob"),
            "the rejection names the terminal status and points at job completion: {msg}"
        );
        // Non-terminal statuses (and absence) are settable via UPDATE.
        assert!(reject_non_updatable_status(Some("THINKING")).is_ok());
        assert!(reject_non_updatable_status(None).is_ok());
    }

    #[test]
    fn agent_update_status_is_limited_to_canonical_enum() {
        for status in ["UNKNOWN", "INITIALIZING", "COMPLETED", "not-a-status"] {
            assert!(
                reject_non_updatable_status(Some(status)).is_err(),
                "{status}"
            );
        }
        for status in ["IDLE", "THINKING", "TOOL_CALLING", "TOOL_DISCOVERY"] {
            assert!(
                reject_non_updatable_status(Some(status)).is_ok(),
                "{status}"
            );
        }
    }

    #[test]
    fn agent_configuration_attributes_name_individual_limits() {
        let request = serde_json::json!({
            "historyItemId":"limits", "loopIteration":1, "producedAt":"2026-01-02T03:04:05Z",
            "role":"CONFIGURATION", "content":[], "tools":[],
            "limits":{"maxTokens":20,"maxModelCalls":2,"maxToolCalls":3},
        });
        let turn = agent_turn_from(serde_json::from_value(request).unwrap(), 42, "opaque").unwrap();
        assert_eq!(
            turn.changed_attributes,
            ["tools", "maxTokens", "maxModelCalls", "maxToolCalls"]
        );
    }

    // `producedAt` accepts the REST RFC-3339 `date-time` spelling. `object`
    // content accepts arbitrary JSON (the REST wire shape), stored as the engine's
    // opaque JSON string.
    #[test]
    fn agent_turn_from_parses_rest_produced_at_and_json_object() {
        let iso = agent_turn_from(
            serde_json::from_str::<AgentTurnReq>(
                r#"{"historyItemId":"object-test","loopIteration":1,"producedAt":"2026-01-02T03:04:05.250Z","role":"USER",
                    "content":[{"contentType":"OBJECT","object":{"a":1,"b":[2,3]}}]}"#,
            )
            .unwrap(),
            42,
            "opaque:lease/0009",
        )
        .unwrap();
        assert_eq!(
            iso.produced_at, 1_767_323_045_250,
            "RFC-3339 producedAt parses to epoch millis (incl. fractional seconds)"
        );
        assert_eq!(
            iso.content[0].object.as_deref(),
            Some(r#"{"a":1,"b":[2,3]}"#),
            "a JSON object is stored as the engine's opaque JSON string"
        );

        assert_eq!(
            (iso.job_key, iso.job_lease),
            (42, "opaque:lease/0009".into()),
            "the request job attribution is stamped onto the turn"
        );
    }

    #[test]
    fn agent_prompt_request_serializes_as_content_array() {
        let request = serde_json::json!({
            "historyItemId":"configuration", "loopIteration":1,
            "producedAt":"2026-01-02T03:04:05Z", "role":"CONFIGURATION", "content":[],
            "systemPrompt":[{"contentType":"TEXT","text":"[not JSON]"},
                {"contentType":"OBJECT","object":{"nested":true}}],
        });
        let turn = agent_turn_from(serde_json::from_value(request).unwrap(), 42, "opaque").unwrap();
        let encoded = serde_json::to_value(turn).unwrap();
        assert!(
            encoded["system_prompt"].is_array(),
            "new prompts must not be double encoded"
        );
        assert_eq!(encoded["system_prompt"][0]["text"], "[not JSON]");
    }

    #[test]
    #[cfg(feature = "read-model")]
    fn agent_content_corruption_does_not_panic() {
        for content_type in [
            AgentHistoryContentType::Document,
            AgentHistoryContentType::Object,
        ] {
            let content = AgentHistoryContent {
                content_type,
                text: None,
                document_reference: Some("broken JSON".into()),
                object: Some("broken JSON".into()),
            };
            assert!(agent_content_result(&content).is_err());
        }
    }

    #[test]
    fn agent_history_metrics_preserve_nullable_values() {
        let request = serde_json::json!({
            "historyItemId":"metrics", "loopIteration":1, "producedAt":"2026-01-02T03:04:05Z",
            "role":"ASSISTANT", "content":[],
            "metrics":{"inputTokens":2,"outputTokens":null,"durationMs":7},
        });
        let turn = agent_turn_from(
            serde_json::from_value(request.clone()).unwrap(),
            42,
            "opaque",
        )
        .unwrap();
        assert_eq!(agent_metrics_result(&turn.metrics), request["metrics"]);
        let mut zero = request.clone();
        zero["metrics"] = serde_json::json!({"inputTokens":0,"outputTokens":0,"durationMs":0});
        let turn =
            agent_turn_from(serde_json::from_value(zero.clone()).unwrap(), 42, "opaque").unwrap();
        assert_eq!(agent_metrics_result(&turn.metrics), zero["metrics"]);
        for metrics in [
            serde_json::json!({"inputTokens":null,"outputTokens":null,"durationMs":null}),
            serde_json::json!({"inputTokens":-1,"outputTokens":-2,"durationMs":-1}),
        ] {
            let mut present = request.clone();
            present["metrics"] = metrics.clone();
            let turn =
                agent_turn_from(serde_json::from_value(present).unwrap(), 42, "opaque").unwrap();
            assert_eq!(agent_metrics_result(&turn.metrics), metrics);
        }
        let mut omitted = request.clone();
        omitted.as_object_mut().unwrap().remove("metrics");
        let turn = agent_turn_from(serde_json::from_value(omitted).unwrap(), 42, "opaque").unwrap();
        assert_eq!(agent_metrics_result(&turn.metrics), serde_json::Value::Null);
        for missing in ["inputTokens", "outputTokens", "durationMs"] {
            let mut invalid = request.clone();
            invalid["metrics"].as_object_mut().unwrap().remove(missing);
            assert!(
                serde_json::from_value::<AgentTurnReq>(invalid).is_err(),
                "{missing}"
            );
        }
    }

    #[test]
    fn agent_history_requests_enforce_required_fields_and_content_union() {
        let valid = serde_json::json!({
            "historyItemId":"id", "loopIteration":1, "producedAt":"2026-01-02T03:04:05Z",
            "role":"USER", "content":[{"contentType":"TEXT","text":"hello"}],
        });
        for field in [
            "historyItemId",
            "loopIteration",
            "producedAt",
            "role",
            "content",
        ] {
            let mut request = valid.clone();
            request.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<AgentTurnReq>(request).is_err(),
                "{field}"
            );
        }
        for bad in [
            serde_json::json!({"contentType":"TEXT","text":"hello","object":{}}),
            serde_json::json!({"contentType":"OBJECT"}),
            serde_json::json!({"contentType":"DOCUMENT","documentReference":"not-an-object"}),
        ] {
            let mut request = valid.clone();
            request["content"] = serde_json::json!([bad]);
            assert!(serde_json::from_value::<AgentTurnReq>(request).is_err());
        }
        let mut request = valid.clone();
        request["loopIteration"] = serde_json::json!(0);
        assert!(serde_json::from_value::<AgentTurnReq>(request).is_err());
        let mut request = valid.clone();
        request["loopIteration"] = serde_json::json!(u32::MAX);
        assert!(serde_json::from_value::<AgentTurnReq>(request).is_err());
        let mut request = valid;
        request["producedAt"] = serde_json::json!(100);
        assert!(serde_json::from_value::<AgentTurnReq>(request).is_err());
    }

    #[test]
    fn ms_from_rfc3339_handles_offsets_fractions_and_rejects_garbage() {
        assert_eq!(ms_from_rfc3339("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            ms_from_rfc3339("2026-01-02T03:04:05Z").unwrap(),
            1_767_323_045_000
        );
        // A +02:00 offset is subtracted to reach UTC.
        assert_eq!(
            ms_from_rfc3339("2026-01-02T05:04:05+02:00").unwrap(),
            1_767_323_045_000
        );
        // Fractional seconds are truncated to millisecond precision.
        assert_eq!(
            ms_from_rfc3339("2026-01-02T03:04:05.2509Z").unwrap(),
            1_767_323_045_250
        );
        for bad in [
            "not-a-date",
            "2026-13-01T00:00:00Z",
            "2026-02-30T00:00:00Z",
            "2026-01-02T03:04:05",
            "2026-01-02 03:04:05Z",
        ] {
            assert!(
                ms_from_rfc3339(bad).is_err(),
                "an invalid RFC-3339 string is rejected: {bad}"
            );
        }
    }

    // --- DMN deploy / evaluate surface (issue #1158) -------------------------

    const GREETING_DMN: &str = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg" name="drg">
      <decision id="greeting" name="Greeting">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="o1" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="ie1"><text>"en"</text></inputEntry>
            <outputEntry id="oe1"><text>"hello"</text></outputEntry></rule>
          <rule id="r2"><inputEntry id="ie2"><text>"de"</text></inputEntry>
            <outputEntry id="oe2"><text>"hallo"</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;

    const BUSINESS_RULE_TASK_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="rules" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:businessRuleTask id="decide">
      <bpmn:extensionElements>
        <zeebe:calledDecision decisionId="greeting" resultVariable="score" />
      </bpmn:extensionElements>
    </bpmn:businessRuleTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="decide" />
    <bpmn:sequenceFlow id="f2" sourceRef="decide" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn deploy_decision_registers_the_drg_and_its_decisions() {
        let mut eng = TestEngine::new();
        let out = parse(&eng.deploy_decision(GREETING_DMN).unwrap());
        assert_eq!(out["decisionRequirementsId"], "drg");
        assert_eq!(out["version"], 1);
        let decisions = out["decisions"].as_array().unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0]["decisionId"], "greeting");
        assert_eq!(decisions[0]["decisionName"], "Greeting");
        assert_eq!(decisions[0]["version"], 1);
        assert!(decisions[0]["decisionKey"].as_str().is_some());
        // The decision is now resolvable in engine state.
        assert!(eng.engine.state().decisions.contains_key("greeting"));
    }

    #[test]
    fn deploy_auto_detects_dmn_and_routes_to_decision_deployment() {
        // The one deploy() entry point accepts DMN — the exact repro from #1158,
        // where `deploy(<dmn>)` used to fail with "no <process> element found".
        let mut eng = TestEngine::new();
        let out = parse(&eng.deploy(GREETING_DMN).unwrap());
        assert_eq!(out["decisionRequirementsId"], "drg");
        assert!(out.get("processIds").is_none());
        assert!(eng.engine.state().decisions.contains_key("greeting"));
    }

    #[test]
    fn deploy_still_returns_process_ids_for_bpmn() {
        let mut eng = TestEngine::new();
        let out = parse(&eng.deploy(SERVICE_TASK_XML).unwrap());
        assert_eq!(out["processIds"], serde_json::json!(["p"]));
        assert!(out.get("decisionRequirementsId").is_none());
    }

    #[test]
    fn business_rule_task_resolves_called_decision_end_to_end() {
        // Deploy the DMN and a process whose businessRuleTask calls it, run an
        // instance, and confirm the decision output is bound and a decision
        // instance is recorded in the snapshot — the whole point of #1158.
        let mut eng = TestEngine::new();
        eng.deploy_decision(GREETING_DMN).unwrap();
        eng.deploy(BUSINESS_RULE_TASK_XML).unwrap();
        let snap = parse(
            &eng.create_instance("rules", r#"{"lang":"de"}"#, None)
                .unwrap(),
        );

        // The instance ran straight through to completion (no incident).
        assert!(
            snap["incidents"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true),
            "unexpected incident: {snap}"
        );
        // A decision instance was recorded with the evaluated output.
        let decisions = snap["decisionInstances"].as_array().unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0]["decisionId"], "greeting");
        assert_eq!(decisions[0]["output"], "hallo");
    }

    #[test]
    fn evaluate_decision_runs_a_deployed_decision() {
        let mut eng = TestEngine::new();
        eng.deploy_decision(GREETING_DMN).unwrap();
        let out = parse(
            &eng.evaluate_decision("greeting", r#"{"lang":"en"}"#)
                .unwrap(),
        );
        assert_eq!(out["decisionId"], "greeting");
        assert_eq!(out["output"], "hello");
        assert!(out["decisionKey"].as_str().is_some());
    }

    #[test]
    fn resource_dmn_detection_distinguishes_dmn_from_bpmn() {
        // Error attribution in deploy() relies on this: a DMN-namespaced document
        // is DMN, a BPMN-namespaced one is not. (Only reached when both parsers
        // reject; the happy path routes by successful parse.)
        assert!(resource_looks_like_dmn(GREETING_DMN));
        assert!(resource_looks_like_dmn(
            r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="e" />"#
        ));
        assert!(!resource_looks_like_dmn(SERVICE_TASK_XML));
        assert!(!resource_looks_like_dmn(BUSINESS_RULE_TASK_XML));
    }
}

/// Acceptance tests for the feature-gated REST read channel: proves the shared
/// `nanobpmn-read-model` projection, fed the same events as `self.log`, answers
/// the gateway's readstore-shaped queries from inside the in-browser test engine.
///
/// Runs on the host (`cargo test -p nanobpmn-engine-wasm --features read-model`)
/// where the read model links the platform SQLite; the same code path is the one
/// compiled to `wasm32` with the in-memory MemoryVFS backend.
#[cfg(all(test, feature = "read-model"))]
mod read_channel_tests {
    use nanobpmn_engine_core::{Command, Event, FormResource};
    use serde_json::Value as J;

    use super::*;

    fn parse(s: &str) -> J {
        serde_json::from_str(s).expect("valid JSON")
    }

    const USER_TASK_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    fn form_key_of(events: &[Event]) -> u64 {
        events
            .iter()
            .find_map(|e| match e {
                Event::FormDeployed { form_key, .. } => Some(*form_key),
                _ => None,
            })
            .expect("a FormDeployed event")
    }

    #[test]
    fn get_form_by_key_returns_the_latest_schema() {
        let mut eng = TestEngine::new();

        // Deploy two versions of the same form id; each version gets its own key.
        let v1 = eng
            .apply(Command::DeployForms(vec![FormResource {
                id: "greeting".into(),
                resource_name: "greeting.form".into(),
                schema: r#"{"components":[],"v":1}"#.into(),
            }]))
            .expect("deploy form v1");
        let v1_key = form_key_of(&v1);
        let v2 = eng
            .apply(Command::DeployForms(vec![FormResource {
                id: "greeting".into(),
                resource_name: "greeting.form".into(),
                schema: r#"{"components":[],"v":2}"#.into(),
            }]))
            .expect("deploy form v2");
        let v2_key = form_key_of(&v2);
        assert_ne!(v1_key, v2_key, "each form version gets a distinct key");

        // The latest version's key resolves to the latest schema, in the gateway's
        // FormResult JSON shape.
        let latest = parse(&eng.get_form_by_key(&v2_key.to_string()).unwrap());
        assert_eq!(latest["formId"], "greeting");
        assert_eq!(latest["version"], 2);
        assert_eq!(latest["schema"], r#"{"components":[],"v":2}"#);
        assert_eq!(latest["formKey"], v2_key.to_string());
        assert_eq!(latest["tenantId"], "<default>");

        // The older key still resolves to its own (v1) schema.
        let old = parse(&eng.get_form_by_key(&v1_key.to_string()).unwrap());
        assert_eq!(old["version"], 1);
        assert_eq!(old["schema"], r#"{"components":[],"v":1}"#);

        // An unknown key is JSON null (the gateway 404 has no body to mirror).
        assert_eq!(eng.get_form_by_key("999999").unwrap(), "null");
    }

    // Round-trip the JS deploy entry points added in #815: deploying a form /
    // generic resource through the #[wasm_bindgen] `deployForm` / `deployResource`
    // methods must populate the in-browser read model so `getFormByKey` /
    // `getResourceByKey` resolve — the whole point of closing the write-side gap.
    #[test]
    fn deploy_form_js_entry_round_trips_through_get_form_by_key() {
        let mut eng = TestEngine::new();

        let schema = r#"{"id":"greeting","components":[],"v":1}"#;
        let deployed = parse(&eng.deploy_form(schema).expect("deployForm succeeds"));
        assert_eq!(deployed["formId"], "greeting");
        assert_eq!(deployed["version"], 1);
        assert_eq!(deployed["resourceName"], "greeting.form");
        let form_key = deployed["formKey"].as_str().expect("a formKey string");

        // getFormByKey now resolves the just-deployed form (non-null in-browser).
        let got = parse(&eng.get_form_by_key(form_key).unwrap());
        assert_eq!(got["formId"], "greeting");
        assert_eq!(got["version"], 1);
        assert_eq!(got["schema"], schema);
        assert_eq!(got["formKey"], form_key);

        // A second deploy of the same id bumps the version and gets its own key.
        let v2 = parse(
            &eng.deploy_form(r#"{"id":"greeting","components":[],"v":2}"#)
                .expect("deployForm v2 succeeds"),
        );
        assert_eq!(v2["version"], 2);
        assert_ne!(v2["formKey"], deployed["formKey"]);
    }

    #[test]
    fn deploy_form_is_idempotent_when_redeploying_the_identical_latest_form() {
        // Redeploying the identical latest form emits no `FormDeployed` event
        // (the engine dedupes it), but the deploy must still succeed and return
        // the existing identity — resolved from post-apply state, not events.
        let mut eng = TestEngine::new();
        let schema = r#"{"id":"greeting","components":[],"v":1}"#;
        let first = parse(&eng.deploy_form(schema).expect("first deployForm succeeds"));
        let again = parse(
            &eng.deploy_form(schema)
                .expect("redeploying the identical latest form succeeds"),
        );
        assert_eq!(again["formKey"], first["formKey"]);
        assert_eq!(again["version"], first["version"]);
        assert_eq!(again["resourceName"], first["resourceName"]);
    }

    #[test]
    fn form_id_of_requires_a_non_empty_string_id() {
        assert_eq!(
            form_id_of(r#"{"id":"greeting","components":[]}"#),
            Some("greeting".to_string())
        );
        // A body without a string `id`, an empty id, or non-JSON is rejected
        // (native parity: Zeebe requires a form id).
        assert_eq!(form_id_of(r#"{"components":[]}"#), None);
        assert_eq!(form_id_of(r#"{"id":""}"#), None);
        assert_eq!(form_id_of(r#"{"id":42}"#), None);
        assert_eq!(form_id_of("not json"), None);
    }

    #[test]
    fn deploy_resource_js_entry_round_trips_through_get_resource_by_key() {
        let mut eng = TestEngine::new();

        let content = "# Agent prompt\nBe helpful.";
        let deployed = parse(
            &eng.deploy_resource("agent-prompt.md", content)
                .expect("deployResource succeeds"),
        );
        assert_eq!(deployed["resourceId"], "agent-prompt.md");
        assert_eq!(deployed["resourceName"], "agent-prompt.md");
        assert_eq!(deployed["version"], 1);
        let resource_key = deployed["resourceKey"]
            .as_str()
            .expect("a resourceKey string");

        // getResourceByKey now resolves the just-deployed generic resource.
        let got = parse(&eng.get_resource_by_key(resource_key).unwrap());
        assert_eq!(got["resourceId"], "agent-prompt.md");
        assert_eq!(got["resourceName"], "agent-prompt.md");
        assert_eq!(got["version"], 1);

        // An unknown key is JSON null.
        assert_eq!(eng.get_resource_by_key("999999").unwrap(), "null");
    }

    #[test]
    fn deploy_resource_is_idempotent_when_redeploying_the_identical_latest_resource() {
        // Redeploying the identical latest resource (same name and content)
        // emits no `GenericResourceDeployed` event, but the deploy is a
        // successful no-op that must return the existing identity — resolved
        // from post-apply state, not events.
        let mut eng = TestEngine::new();
        let content = "# Agent prompt\nBe helpful.";
        let first = parse(
            &eng.deploy_resource("agent-prompt.md", content)
                .expect("first deployResource succeeds"),
        );
        let again = parse(
            &eng.deploy_resource("agent-prompt.md", content)
                .expect("redeploying the identical latest resource succeeds"),
        );
        assert_eq!(again["resourceKey"], first["resourceKey"]);
        assert_eq!(again["version"], first["version"]);
        assert_eq!(again["resourceName"], first["resourceName"]);
    }

    #[test]
    fn search_user_tasks_honours_the_state_filter() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let key = snap["userTasks"][0]["key"].as_str().unwrap().to_string();

        // An open task shows up under CREATED (state filtered via the read model)…
        let created = parse(&eng.search_user_tasks(r#"{"state":"CREATED"}"#).unwrap());
        assert_eq!(created["items"].as_array().unwrap().len(), 1);
        assert_eq!(created["items"][0]["elementId"], "review");
        assert_eq!(created["items"][0]["state"], "CREATED");
        assert_eq!(created["items"][0]["userTaskKey"], key);
        assert_eq!(created["page"]["totalItems"], 1);

        // …and NOT under COMPLETED.
        let completed = parse(&eng.search_user_tasks(r#"{"state":"COMPLETED"}"#).unwrap());
        assert_eq!(completed["items"].as_array().unwrap().len(), 0);

        // No filter returns every task.
        let all = parse(&eng.search_user_tasks("").unwrap());
        assert_eq!(all["items"].as_array().unwrap().len(), 1);

        // The canonical nested REST body shape filters identically to the shorthand.
        let nested = parse(
            &eng.search_user_tasks(r#"{"filter":{"state":"CREATED"}}"#)
                .unwrap(),
        );
        assert_eq!(nested["items"].as_array().unwrap().len(), 1);
        assert_eq!(nested["items"][0]["userTaskKey"], key);

        // After completion the task leaves the CREATED (open) set entirely.
        eng.complete_user_task(&key, r#"{"approved":true}"#)
            .unwrap();
        let open = parse(&eng.search_user_tasks(r#"{"state":"CREATED"}"#).unwrap());
        assert_eq!(
            open["items"].as_array().unwrap().len(),
            0,
            "a completed task is no longer CREATED"
        );
        let done = parse(&eng.search_user_tasks(r#"{"state":"COMPLETED"}"#).unwrap());
        assert_eq!(done["items"].as_array().unwrap().len(), 1);
    }

    /// `required` property names of the spec schema `name`, following `allOf`
    /// `$ref`s. Loads every `spec/*.yaml` component schema (refs across files are
    /// `other.yaml#/components/schemas/X`, so the last path segment names it).
    fn spec_required(name: &str) -> std::collections::BTreeSet<String> {
        use serde_yaml::Value as Y;
        fn schemas() -> std::collections::BTreeMap<String, Y> {
            let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../spec");
            let mut all = std::collections::BTreeMap::new();
            for entry in std::fs::read_dir(&dir).expect("spec/ dir") {
                let path = entry.unwrap().path();
                if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                    continue;
                }
                let doc: Y = serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap())
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                if let Some(Y::Mapping(m)) = doc.get("components").and_then(|c| c.get("schemas")) {
                    for (k, v) in m {
                        all.insert(k.as_str().unwrap().to_string(), v.clone());
                    }
                }
            }
            all
        }
        fn walk(
            all: &std::collections::BTreeMap<String, Y>,
            s: &Y,
            out: &mut std::collections::BTreeSet<String>,
        ) {
            if let Some(r) = s.get("$ref").and_then(Y::as_str) {
                let target = r.rsplit('/').next().unwrap();
                walk(
                    all,
                    all.get(target).unwrap_or_else(|| panic!("unresolved {r}")),
                    out,
                );
            }
            if let Some(Y::Sequence(req)) = s.get("required") {
                out.extend(req.iter().filter_map(Y::as_str).map(str::to_string));
            }
            if let Some(Y::Sequence(parts)) = s.get("allOf") {
                for p in parts {
                    walk(all, p, out);
                }
            }
        }
        let all = schemas();
        let mut out = std::collections::BTreeSet::new();
        walk(
            &all,
            all.get(name).unwrap_or_else(|| panic!("no schema {name}")),
            &mut out,
        );
        assert!(
            !out.is_empty(),
            "{name} declares no required fields — wrong schema?"
        );
        out
    }

    /// Guard (defect class: hand-built read DTOs drifting from the spec the
    /// published `@nanobpm/engine-wasm/readmodel-types` are generated from). The
    /// facade builds each read result with `json!`, so nothing forces it to
    /// track a spec re-sync: #1291's upstream bump made `UserTaskResult.businessId`
    /// required while the facade kept omitting it — a type that lies to TS
    /// consumers. Every exported read is exercised against real engine state and
    /// must carry every spec-`required` key (present, possibly `null`).
    #[test]
    fn read_results_carry_every_spec_required_field() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        eng.create_instance("p", r#"{"amount":1}"#, None).unwrap();
        let form = parse(&eng.deploy_form(r#"{"id":"f","components":[]}"#).unwrap());
        let resource = parse(&eng.deploy_resource("r.md", "x").unwrap());

        let first = |v: J| v["items"][0].clone();
        let cases = [
            (
                "UserTaskResult",
                first(parse(&eng.search_user_tasks("").unwrap())),
            ),
            (
                "ProcessInstanceResult",
                first(parse(&eng.search_process_instances("").unwrap())),
            ),
            (
                "VariableSearchResult",
                first(parse(&eng.search_variables("").unwrap())),
            ),
            (
                "FormResult",
                parse(
                    &eng.get_form_by_key(form["formKey"].as_str().unwrap())
                        .unwrap(),
                ),
            ),
            (
                "ResourceResult",
                parse(
                    &eng.get_resource_by_key(resource["resourceKey"].as_str().unwrap())
                        .unwrap(),
                ),
            ),
        ];
        for (schema, got) in cases {
            let obj = got
                .as_object()
                .unwrap_or_else(|| panic!("{schema}: not an object: {got}"));
            let missing: Vec<_> = spec_required(schema)
                .into_iter()
                .filter(|k| !obj.contains_key(k))
                .collect();
            assert!(
                missing.is_empty(),
                "{schema} is missing spec-required {missing:?}: {got}"
            );
        }
    }

    /// `UserTaskResult.businessId` is the owning instance's business id (the
    /// same source as the gateway's `business_id_of`), `null` when unset.
    #[test]
    fn search_user_tasks_surfaces_the_instance_business_id() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        eng.apply(Command::CreateInstance {
            process_id: "p".into(),
            variables: Default::default(),
            tags: Vec::new(),
            business_id: Some("order-42".into()),
            process_definition_key: None,
            version: None,
        })
        .unwrap();
        eng.create_instance("p", "{}", None).unwrap();
        let items = parse(&eng.search_user_tasks("").unwrap())["items"].clone();
        let mut ids: Vec<J> = items
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["businessId"].clone())
            .collect();
        ids.sort_by_key(|v| v.to_string());
        assert_eq!(ids, vec![J::String("order-42".into()), J::Null]);
    }

    #[test]
    fn reset_clears_the_read_model() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        eng.create_instance("p", "{}", None).unwrap();
        assert_eq!(
            parse(&eng.search_user_tasks("").unwrap())["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        eng.reset();
        assert_eq!(
            parse(&eng.search_user_tasks("").unwrap())["items"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "reset re-opens a fresh, empty read model"
        );
    }

    #[test]
    fn state_filter_rejects_a_non_object_body() {
        // A non-object JSON root must be rejected rather than silently broadening
        // the query (which is what `json.get(field)` on a non-object would do).
        for body in [r#"[]"#, r#""CREATED""#, r#"42"#, r#"true"#] {
            let err = parse_state_filter_inner(body, "state").unwrap_err();
            assert!(
                err.contains("must be a JSON object"),
                "body {body:?} should be rejected as non-object, got {err:?}"
            );
        }
        // Root `null`, an empty body, and a missing field are all "no filter".
        assert_eq!(parse_state_filter_inner("null", "state").unwrap(), None);
        assert_eq!(parse_state_filter_inner("", "state").unwrap(), None);
        assert_eq!(parse_state_filter_inner("{}", "state").unwrap(), None);
        assert_eq!(
            parse_state_filter_inner(r#"{"state":null}"#, "state").unwrap(),
            None
        );
        // A well-formed object yields the value; a non-string value is rejected.
        assert_eq!(
            parse_state_filter_inner(r#"{"state":"CREATED"}"#, "state").unwrap(),
            Some("CREATED".to_string())
        );
        assert!(parse_state_filter_inner(r#"{"state":42}"#, "state").is_err());
        // Malformed JSON is rejected too.
        assert!(parse_state_filter_inner(r#"{"state":"#, "state").is_err());
    }

    #[test]
    fn search_instances_and_variables_reject_malformed_filter_bodies() {
        // `searchProcessInstances` / `searchVariables` don't filter on any field,
        // but they must still reject a malformed/non-object body exactly as the
        // gateway rejects it at deserialization, rather than silently accepting it.
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        eng.create_instance("p", "{}", None).unwrap();

        // Well-formed "no filter" bodies (empty, root null, empty/nested object)
        // are accepted by both surfaces.
        for body in ["", "  ", "null", "{}", r#"{"filter":{}}"#] {
            assert!(
                eng.search_process_instances(body).is_ok(),
                "searchProcessInstances should accept {body:?}"
            );
            assert!(
                eng.search_variables(body).is_ok(),
                "searchVariables should accept {body:?}"
            );
        }

        // A non-object root, a non-object nested `filter`, and syntactically
        // invalid JSON are all rejected. Exercise the rejection through the shared
        // host-testable validator the search methods call (the `search*` methods
        // surface the error as a `JsValue`, which can't be constructed off-wasm).
        for body in [
            r#"[]"#,
            r#""x""#,
            r#"42"#,
            r#"true"#,
            r#"{"filter":[]}"#,
            r#"{"#,
        ] {
            assert!(
                validate_search_filter_body_inner(body).is_err(),
                "filter body {body:?} should be rejected"
            );
        }
    }

    const CA_CHILD_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="ca-child">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="child-work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="b" sourceRef="work" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    const CA_PARENT_XML: &str = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="ca-parent">
          <bpmn:startEvent id="s" />
          <bpmn:callActivity id="c1">
            <bpmn:extensionElements>
              <zeebe:calledElement processId="ca-child" />
            </bpmn:extensionElements>
          </bpmn:callActivity>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="c1" />
          <bpmn:sequenceFlow id="b" sourceRef="c1" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;

    #[test]
    fn search_process_instances_surfaces_call_activity_parent_and_root_keys() {
        // Issue #977 (Zeebe/C8 parity): the `engine-wasm` read model — the surface
        // nano-ide#473 / the console consume via `@nanobpm/engine-wasm` — must
        // surface the native call-activity hierarchy, not hard-code the linkage to
        // null. A `ca-parent` → `ca-child` run parks the child on a service-task
        // job so both instances stay ACTIVE and are projected.
        let mut eng = TestEngine::new();
        eng.deploy(CA_CHILD_XML).unwrap();
        eng.deploy(CA_PARENT_XML).unwrap();
        let snap = parse(&eng.create_instance("ca-parent", "{}", None).unwrap());
        let parent_key = snap["instances"]
            .as_array()
            .and_then(|xs| xs.iter().find(|i| i["processId"] == "ca-parent"))
            .map(|i| i["key"].as_str().unwrap().to_string())
            .expect("parent instance created");

        let result = parse(&eng.search_process_instances("").unwrap());
        let items = result["items"].as_array().expect("items array");
        let by_id = |id: &str| -> J {
            items
                .iter()
                .find(|i| i["processDefinitionId"] == id)
                .unwrap_or_else(|| panic!("no {id} instance projected: {result}"))
                .clone()
        };

        // Top-level parent: no parent linkage, self-rooted (no-regression guard).
        let parent = by_id("ca-parent");
        assert_eq!(parent["processInstanceKey"], serde_json::json!(parent_key));
        assert_eq!(parent["parentProcessInstanceKey"], J::Null);
        assert_eq!(parent["parentElementInstanceKey"], J::Null);
        assert_eq!(
            parent["rootProcessInstanceKey"],
            serde_json::json!(parent_key),
            "a top-level instance roots to its own key, not null"
        );

        // Call-activity child: parent linkage populated, roots to the parent.
        let child = by_id("ca-child");
        assert_eq!(
            child["parentProcessInstanceKey"],
            serde_json::json!(parent_key),
            "the child reports its calling instance, not null"
        );
        assert_ne!(
            child["parentElementInstanceKey"],
            J::Null,
            "the child reports the spawning call-activity element instance"
        );
        assert_eq!(
            child["rootProcessInstanceKey"],
            serde_json::json!(parent_key),
            "the child roots to the top-level parent"
        );
    }

    /// Issue #1095 (mirrors #977 for user tasks): a `searchUserTasks` on a
    /// call-activity child-cell run must surface the resolved
    /// `rootProcessInstanceKey` — the top-level parent for a child-instance task,
    /// and its own key for a self-rooted top-level task — never the old
    /// hard-coded null. This is the engine-wasm surface the typed `urban-testkit`
    /// seam passes through, so a null here blocks child-cell escalation → epic
    /// correlation (nanobpm/nano-workforce#646).
    #[test]
    fn search_user_tasks_surfaces_call_activity_root_key() {
        // A child process that parks a user task, called from a parent, so the
        // child instance stays ACTIVE with an open task projected under a
        // non-self root.
        const UT_CHILD_XML: &str = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="ut-child">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;
        const UT_PARENT_XML: &str = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="ut-parent">
              <bpmn:startEvent id="s" />
              <bpmn:callActivity id="c1">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="ut-child" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="c1" />
              <bpmn:sequenceFlow id="b" sourceRef="c1" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let mut eng = TestEngine::new();
        eng.deploy(UT_CHILD_XML).unwrap();
        eng.deploy(UT_PARENT_XML).unwrap();
        let snap = parse(&eng.create_instance("ut-parent", "{}", None).unwrap());
        let parent_key = snap["instances"]
            .as_array()
            .and_then(|xs| xs.iter().find(|i| i["processId"] == "ut-parent"))
            .map(|i| i["key"].as_str().unwrap().to_string())
            .expect("parent instance created");
        let child_key = snap["instances"]
            .as_array()
            .and_then(|xs| xs.iter().find(|i| i["processId"] == "ut-child"))
            .map(|i| i["key"].as_str().unwrap().to_string())
            .expect("child instance created");
        assert_ne!(parent_key, child_key, "child is a distinct instance");

        let result = parse(&eng.search_user_tasks("").unwrap());
        let items = result["items"].as_array().expect("items array");
        assert_eq!(items.len(), 1, "exactly the child's user task is projected");
        let task = &items[0];
        assert_eq!(task["elementId"], "review");
        assert_eq!(
            task["processInstanceKey"],
            serde_json::json!(child_key),
            "the task lives on the child instance"
        );
        assert_eq!(
            task["rootProcessInstanceKey"],
            serde_json::json!(parent_key),
            "the child-cell task roots to the top-level parent, not null"
        );
    }

    /// Issue #1095 no-regression guard: a top-level (self-rooted) user task
    /// reports `rootProcessInstanceKey == processInstanceKey`, never null.
    #[test]
    fn search_user_tasks_self_roots_a_top_level_task() {
        let mut eng = TestEngine::new();
        eng.deploy(USER_TASK_XML).unwrap();
        let snap = parse(&eng.create_instance("p", "{}", None).unwrap());
        let instance_key = snap["instances"][0]["key"].as_str().unwrap().to_string();

        let result = parse(&eng.search_user_tasks("").unwrap());
        let task = &result["items"][0];
        assert_eq!(task["processInstanceKey"], serde_json::json!(instance_key));
        assert_eq!(
            task["rootProcessInstanceKey"],
            serde_json::json!(instance_key),
            "a top-level task roots to its own key, not null"
        );
    }

    #[test]
    fn call_activity_propagates_variables_both_ways_through_the_shipped_artifact() {
        // Issue #1057 (Zeebe parity): a *bare* callActivity (no ioMappings,
        // propagate flags absent ⇒ both `true`) must copy the parent's variables
        // into the child at spawn AND merge the child's variables back into the
        // parent on completion — end-to-end through the shipped wasm surface the
        // console / Bojtos consume, not just the Rust unit layer.
        //
        // The child scriptTask computes `doubled = orderId * 2`. Observing
        // `doubled` on the *parent* proves BOTH directions in one shot: `orderId`
        // reached the child (parent→child), and its result crossed back
        // (child→parent). The parent parks on a downstream job so it stays ACTIVE
        // and its variables remain in the read model.
        const CHILD: &str = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="prop-child">
              <bpmn:startEvent id="s" />
              <bpmn:scriptTask id="dbl">
                <bpmn:extensionElements>
                  <zeebe:script expression="=orderId * 2" resultVariable="doubled" />
                </bpmn:extensionElements>
              </bpmn:scriptTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="dbl" />
              <bpmn:sequenceFlow id="b" sourceRef="dbl" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;
        const PARENT: &str = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="prop-parent">
              <bpmn:startEvent id="s" />
              <bpmn:callActivity id="c1">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="prop-child" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:serviceTask id="park">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="park-work" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="c1" />
              <bpmn:sequenceFlow id="b" sourceRef="c1" targetRef="park" />
              <bpmn:sequenceFlow id="d" sourceRef="park" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let mut eng = TestEngine::new();
        eng.deploy(CHILD).unwrap();
        eng.deploy(PARENT).unwrap();
        let snap = parse(
            &eng.create_instance("prop-parent", r#"{"orderId": 21}"#, None)
                .unwrap(),
        );
        let parent_key = snap["instances"]
            .as_array()
            .and_then(|xs| xs.iter().find(|i| i["processId"] == "prop-parent"))
            .map(|i| i["key"].as_str().unwrap().to_string())
            .expect("parent instance created");

        let vars = parse(&eng.search_variables("").unwrap());
        let items = vars["items"].as_array().expect("items array");
        let doubled = items.iter().find(|v| {
            v["name"] == "doubled" && v["processInstanceKey"] == serde_json::json!(parent_key)
        });
        assert!(
            doubled.is_some(),
            "the child's computed `doubled` must merge back onto the parent \
             (propagateAllChildVariables default true): {vars}"
        );
        assert_eq!(
            doubled.unwrap()["value"],
            "42",
            "orderId=21 reached the child (propagateAllParentVariables default true) \
             and doubled=42 crossed back"
        );
    }

    #[test]
    fn state_filter_honours_the_nested_rest_filter_shape() {
        // The canonical REST body nests the filter under `filter`
        // (`UserTaskSearchQuery`), so a caller pasting the real gateway body must
        // filter, not silently broaden to "no filter".
        assert_eq!(
            parse_state_filter_inner(r#"{"filter":{"state":"CREATED"}}"#, "state").unwrap(),
            Some("CREATED".to_string())
        );
        // Top-level shorthand still works.
        assert_eq!(
            parse_state_filter_inner(r#"{"state":"COMPLETED"}"#, "state").unwrap(),
            Some("COMPLETED".to_string())
        );
        // An empty / null nested filter is "no filter", not an error.
        assert_eq!(
            parse_state_filter_inner(r#"{"filter":{}}"#, "state").unwrap(),
            None
        );
        assert_eq!(
            parse_state_filter_inner(r#"{"filter":null}"#, "state").unwrap(),
            None
        );
        assert_eq!(
            parse_state_filter_inner(r#"{"filter":{"state":null}}"#, "state").unwrap(),
            None
        );
        // A non-string nested value is rejected, as at the top level.
        assert!(parse_state_filter_inner(r#"{"filter":{"state":42}}"#, "state").is_err());
        // A present-but-non-object `filter` is malformed, not "no filter".
        for bad in [
            r#"{"filter":[]}"#,
            r#"{"filter":"CREATED"}"#,
            r#"{"filter":42}"#,
        ] {
            let err = parse_state_filter_inner(bad, "state").unwrap_err();
            assert!(
                err.contains("`filter` field must be a JSON object"),
                "body {bad:?} should be rejected as non-object filter, got {err:?}"
            );
        }
    }

    #[test]
    fn variable_search_truncates_long_values_like_the_gateway() {
        let row = |value: String| VariableRow {
            key: 1,
            instance_key: 2,
            scope_key: 2,
            name: "big".to_string(),
            value,
            process_definition_id: "p".to_string(),
            process_definition_key: "3".to_string(),
        };

        // A short value passes through untouched, `isTruncated: false`.
        let short = variable_search_result(&row("\"hi\"".to_string()));
        assert_eq!(short["value"], "\"hi\"");
        assert_eq!(short["isTruncated"], false);

        // A value at the boundary is not truncated.
        let at_limit = "a".repeat(VARIABLE_VALUE_PREVIEW_LEN);
        let boundary = variable_search_result(&row(at_limit.clone()));
        assert_eq!(
            boundary["value"].as_str().unwrap().len(),
            VARIABLE_VALUE_PREVIEW_LEN
        );
        assert_eq!(boundary["isTruncated"], false);

        // One byte over the limit is truncated to the preview length and flagged.
        let over = "a".repeat(VARIABLE_VALUE_PREVIEW_LEN + 1);
        let truncated = variable_search_result(&row(over));
        assert_eq!(
            truncated["value"].as_str().unwrap().len(),
            VARIABLE_VALUE_PREVIEW_LEN
        );
        assert_eq!(truncated["isTruncated"], true);

        // Truncation lands on a char boundary (never splits a multi-byte char).
        let multibyte = "é".repeat(VARIABLE_VALUE_PREVIEW_LEN); // each 'é' is 2 bytes
        let cut = variable_search_result(&row(multibyte));
        let out = cut["value"].as_str().unwrap();
        assert!(out.len() <= VARIABLE_VALUE_PREVIEW_LEN);
        assert!(out.is_char_boundary(out.len()));
        assert_eq!(cut["isTruncated"], true);
    }

    #[test]
    fn search_user_tasks_rejects_an_unknown_state_spelling() {
        // The gateway rejects unknown `UserTaskStateEnum` spellings during request
        // deserialization; `searchUserTasks` mirrors that instead of silently
        // returning an empty set. The rejection is triggered by
        // `user_task_state_from_rest` returning `None`, and the error message lists
        // the valid spellings — tested at the pure layer because the wrapper's
        // `JsValue` error cannot be inspected on the host target.
        assert_eq!(user_task_state_from_rest("FOO"), None);
        assert_eq!(
            user_task_state_from_rest("created"),
            None,
            "spelling is case-sensitive"
        );
        assert_eq!(user_task_state_from_rest(""), None);
        let spellings = user_task_state_spellings();
        assert!(
            spellings.contains("CREATED")
                && spellings.contains("COMPLETED")
                && spellings.contains("CANCELED"),
            "the rejection message must list every valid spelling, got {spellings:?}"
        );
    }

    #[test]
    fn all_user_task_states_is_exhaustive() {
        // `user_task_state_rest`'s match is compiler-forced exhaustive; this guards
        // that `ALL_USER_TASK_STATES` keeps enumerating every variant so the derived
        // set of valid REST spellings stays complete.
        assert_eq!(ALL_USER_TASK_STATES.len(), 3);
        for st in ALL_USER_TASK_STATES {
            // Every listed variant round-trips through its REST spelling.
            assert_eq!(
                user_task_state_from_rest(user_task_state_rest(st)),
                Some(st)
            );
        }
        assert_eq!(user_task_state_from_rest("nope"), None);
    }

    #[test]
    fn user_task_dates_are_validated_as_rfc3339() {
        // Valid RFC-3339 date-times pass through unchanged.
        for ok in [
            "1970-01-01T00:00:00Z",
            "2026-08-16T11:36:36.344Z",
            "2026-08-16t11:36:36z",
            "2026-01-01T00:00:00+13:00",
            // Leap second: chrono accepts `:60`, so parity requires we accept it too.
            "2026-12-31T23:59:60-05:30",
            "2024-02-29T00:00:00Z",
            "2000-02-29T00:00:00Z",
        ] {
            assert!(is_rfc3339_date_time(ok), "{ok:?} should be valid");
            assert_eq!(
                rfc3339_or_null(&Some(ok.to_string())),
                serde_json::Value::String(ok.to_string())
            );
        }
        // Invalid values (bare date, missing offset, out-of-range, garbage) become
        // null — matching the gateway's `parse_date` coercion.
        for bad in [
            "2026-01-01",
            "2026-08-16T11:36:36",
            "2026-13-01T00:00:00Z",
            "2026-01-01T25:00:00Z",
            "2026-01-01T00:60:00Z",
            "2026-01-01T00:00:00.Z",
            "2026-01-01T00:00:00+24:00",
            "2026-02-31T00:00:00Z",
            "2026-02-29T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2100-02-29T00:00:00Z",
            "2026-01-00T00:00:00Z",
            "not-a-date",
            "",
        ] {
            assert!(!is_rfc3339_date_time(bad), "{bad:?} should be invalid");
            assert_eq!(
                rfc3339_or_null(&Some(bad.to_string())),
                serde_json::Value::Null
            );
        }
        // Absent dates are null.
        assert_eq!(rfc3339_or_null(&None), serde_json::Value::Null);
    }

    #[test]
    fn agent_definition_projection_rejects_missing_required_values() {
        let mut eng = TestEngine::new();
        let minted = mint_agent_instance(&mut eng);
        let key = minted["agentInstanceKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let mut row = eng.read_model.agent_instance(key).unwrap();
        assert!(agent_instance_result(&row).is_ok());
        let model = row.model.take();
        assert!(agent_instance_result(&row).is_err());
        row.model = model;
        row.provider = None;
        assert!(agent_instance_result(&row).is_err());
    }

    // The read-model history output must mirror the gateway's
    // `AgentInstanceHistoryItemResult` wire shape, not dump the engine's internal
    // snake_case layout: content blocks use camelCase keys + REST `contentType`
    // spellings, an `object` payload round-trips as structured JSON (never a
    // JSON-in-a-string), and `producedAt` is an RFC-3339 string round-tripped from
    // the string the driver parsed to millis. Guards the S6 parity fix for the
    // regenerated engine-wasm AgentInstance surface.
    #[test]
    fn agent_history_output_is_rest_shaped_and_round_trips_json_object() {
        let mut eng = TestEngine::new();
        let minted = mint_agent_instance(&mut eng);
        let req = serde_json::json!({
            "elementInstanceKey": minted["elementInstanceKey"],
            "jobKey": minted["jobKey"],
            "jobLease": minted["jobLease"],
            "status": "THINKING",
            "history": [{
                "historyItemId": "assistant-json",
                "loopIteration": 1,
                "producedAt": "2026-01-02T03:04:05.250Z",
                "role": "ASSISTANT",
                "content": [
                    { "contentType": "OBJECT", "object": { "a": 1, "b": [2, 3] } },
                    { "contentType": "TEXT", "text": "hello" },
                ],
            }],
        });
        eng.update_agent_instance(
            minted["agentInstanceKey"].as_str().unwrap(),
            &req.to_string(),
        )
        .unwrap();

        let history = parse(
            &eng.search_agent_instance_history(
                minted["agentInstanceKey"].as_str().unwrap(),
                r#"{"commitStatus":"PENDING"}"#,
            )
            .unwrap(),
        );
        let turn = history["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["role"] == "ASSISTANT")
            .expect("the pushed ASSISTANT turn");

        // producedAt is the RFC-3339 string the gateway emits.
        assert_eq!(turn["producedAt"], "2026-01-02T03:04:05.250Z");

        let content = turn["content"].as_array().unwrap();
        let obj_block = &content[0];
        // camelCase keys + REST enum spelling, never the engine's snake_case layout.
        assert_eq!(obj_block["contentType"], "OBJECT");
        assert!(
            obj_block.get("content_type").is_none(),
            "no snake_case content_type leaks: {obj_block}"
        );
        assert!(
            obj_block.get("documentReference").is_none()
                && obj_block.get("document_reference").is_none(),
            "OBJECT contains no unrelated union fields: {obj_block}"
        );
        // The `object` payload round-trips as structured JSON, not a JSON string.
        assert_eq!(
            obj_block["object"],
            serde_json::json!({ "a": 1, "b": [2, 3] })
        );

        // toolCalls / tools are present as arrays in the REST shape.
        assert!(turn["toolCalls"].is_array() && turn["tools"].is_array());
    }

    /// Registers an agent instance through the worker job and returns its
    /// identity JSON together with the activated job's attribution.
    fn mint_agent_instance(eng: &mut TestEngine) -> J {
        let xml = include_str!("../../engine-core/tests/fixtures/external-agent-job-type.bpmn")
            .replace("agentType=\"external\"", "agentType=\"aiAgentTask\"");
        eng.deploy(&xml).unwrap();
        eng.create_instance(
            "external-agent-routing",
            r#"{"route":"senior:rebase"}"#,
            None,
        )
        .unwrap();
        assert_eq!(
            parse(&eng.search_agent_instances("{}").unwrap())["items"],
            serde_json::json!([])
        );
        let jobs = parse(
            &eng.activate_jobs("senior:rebase", 1, 60_000.0, "W", Some(true))
                .unwrap(),
        );
        let job = &jobs[0];
        eng.create_agent_instance(
            &serde_json::json!({
                "elementInstanceKey": job["elementInstanceKey"],
                "jobKey": job["key"],
                "jobLease": job["jobLeaseToken"],
                "history": [{
                    "historyItemId": "initial-config", "loopIteration": 1,
                    "producedAt": "2026-01-02T03:04:05Z", "role": "CONFIGURATION",
                    "content": [], "model": "gpt", "provider": "openai",
                    "systemPrompt": [{"contentType":"TEXT", "text":"Be helpful"}],
                }],
            })
            .to_string(),
        )
        .unwrap();
        let mut agent = parse(&eng.search_agent_instances("{}").unwrap())["items"][0].clone();
        agent["elementInstanceKey"] = job["elementInstanceKey"].clone();
        agent["jobKey"] = job["key"].clone();
        agent["jobLease"] = job["jobLeaseToken"].clone();
        agent["jobLeaseToken"] = job["jobLeaseToken"].clone();
        agent
    }

    /// The instance's current `tools` array via the read channel.
    fn instance_tools(eng: &TestEngine, key: &str) -> Vec<J> {
        let inst = parse(
            &eng.search_agent_instances(&format!(r#"{{"agentInstanceKey":"{key}"}}"#))
                .unwrap(),
        )["items"][0]
            .clone();
        inst["tools"].as_array().cloned().unwrap_or_default()
    }

    #[test]
    fn update_agent_instance_stages_tools_in_configuration_history() {
        let mut eng = TestEngine::new();
        let minted = mint_agent_instance(&mut eng);
        let key = minted["agentInstanceKey"].as_str().unwrap().to_string();
        let request = serde_json::json!({
            "elementInstanceKey": minted["elementInstanceKey"],
            "jobKey": minted["jobKey"], "jobLease": minted["jobLease"],
            "history": [{
                "historyItemId": "new-tools", "loopIteration": 1, "role":"CONFIGURATION",
                "producedAt": "2026-01-02T03:04:06Z", "content": [],
                "tools": [{"name":"search", "description":null, "elementId":null}],
            }],
        });
        eng.update_agent_instance(&key, &request.to_string())
            .unwrap();
        assert!(
            instance_tools(&eng, &key).is_empty(),
            "uncommitted tools are pending"
        );
        eng.complete_job(
            minted["jobKey"].as_str().unwrap(),
            "{}",
            Some(minted["jobLease"].as_str().unwrap().to_owned()),
        )
        .unwrap();
        assert_eq!(instance_tools(&eng, &key)[0]["name"], "search");
    }

    // The request-level `jobKey`/`jobLease` must be attributed to every appended
    // history turn, mirroring the gateway (`agent_history_turn_from`). The read
    // channel projects `jobKey` onto each history item, so a turn pushed with a
    // `jobKey` must carry it — otherwise wasm callers cannot exercise the job
    // attribution/dedupe semantics the gateway supports.
    #[test]
    fn update_agent_instance_stamps_job_attribution_onto_each_turn() {
        let mut eng = TestEngine::new();
        let minted = mint_agent_instance(&mut eng);
        let key = minted["agentInstanceKey"].as_str().unwrap().to_string();
        let req = serde_json::json!({
            "elementInstanceKey": minted["elementInstanceKey"],
            "status": "THINKING",
            "jobKey": minted["jobKey"],
            "jobLease": minted["jobLease"],
            "history": [
                {
                    "historyItemId": "assistant-a",
                    "loopIteration": 1,
                    "producedAt": "2026-01-02T03:04:05.250Z",
                    "role": "ASSISTANT",
                    "content": [{ "contentType": "TEXT", "text": "a" }],
                },
                {
                    "historyItemId": "assistant-b",
                    "loopIteration": 1,
                    "producedAt": "2026-01-02T03:04:06.250Z",
                    "role": "ASSISTANT",
                    "content": [{ "contentType": "TEXT", "text": "b" }],
                },
            ],
        });
        eng.update_agent_instance(&key, &req.to_string()).unwrap();

        let history = parse(
            &eng.search_agent_instance_history(&key, r#"{"commitStatus":"PENDING"}"#)
                .unwrap(),
        );
        let assistant: Vec<&J> = history["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["role"] == "ASSISTANT")
            .collect();
        assert_eq!(assistant.len(), 2, "both pushed turns are present");
        for turn in assistant {
            assert_eq!(
                turn["jobKey"], minted["jobKey"],
                "each turn carries the request jobKey: {turn}"
            );
            assert_eq!(turn["jobLeaseToken"], minted["jobLeaseToken"]);
        }
    }

    #[test]
    fn update_agent_instance_requires_keys_but_treats_lease_as_opaque() {
        let request = serde_json::json!({
            "elementInstanceKey":"1", "jobKey":"2", "jobLease":"opaque:token/0009",
        });
        let parsed: UpdateAgentInstanceReq = serde_json::from_value(request.clone()).unwrap();
        // The legacy `jobLease` deserializes into its own field; presence and
        // reconciliation are handler-enforced (see `reconcile_agent_lease_token`),
        // not folded onto the canonical field by a serde alias.
        assert_eq!(parsed.job_lease.as_deref(), Some("opaque:token/0009"));
        assert_eq!(parsed.job_lease_token, None);
        // The structural keys remain required at deserialize time.
        for key in ["elementInstanceKey", "jobKey"] {
            let mut missing = request.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<UpdateAgentInstanceReq>(missing).is_err());
            let mut null = request.clone();
            null[key] = J::Null;
            assert!(serde_json::from_value::<UpdateAgentInstanceReq>(null).is_err());
        }
        for field in ["definition", "limits", "metrics", "tools"] {
            let mut old_shape = request.clone();
            old_shape[field] = J::Null;
            assert!(serde_json::from_value::<UpdateAgentInstanceReq>(old_shape).is_err());
        }
    }

    #[test]
    fn agent_lease_reconcile_matches_rest_deprecation_window() {
        // Canonical-only and legacy-only both resolve to their supplied token
        // (the deprecation window keeps legacy `jobLease` working).
        assert_eq!(
            reconcile_agent_lease_token(Some("canon".into()), None).unwrap(),
            "canon"
        );
        assert_eq!(
            reconcile_agent_lease_token(None, Some("legacy".into())).unwrap(),
            "legacy"
        );
        // An EQUAL dual-send is accepted (mirrors REST) — a serde alias would
        // have rejected it as a duplicate field.
        assert_eq!(
            reconcile_agent_lease_token(Some("same".into()), Some("same".into())).unwrap(),
            "same"
        );
        // A DIFFERING dual-send fails loudly rather than fencing on a stale token.
        assert_eq!(
            reconcile_agent_lease_token(Some("a".into()), Some("b".into())),
            Err(LeaseReconcileError::Conflict),
        );
        // Neither name supplied is rejected — the lease is required.
        assert_eq!(
            reconcile_agent_lease_token(None, None),
            Err(LeaseReconcileError::Missing),
        );
    }

    #[test]
    fn create_agent_instance_accepts_equal_dual_send() {
        let mut eng = TestEngine::new();
        let xml = include_str!("../../engine-core/tests/fixtures/external-agent-job-type.bpmn")
            .replace("agentType=\"external\"", "agentType=\"aiAgentTask\"");
        eng.deploy(&xml).unwrap();
        eng.create_instance(
            "external-agent-routing",
            r#"{"route":"senior:rebase"}"#,
            None,
        )
        .unwrap();
        let jobs = parse(
            &eng.activate_jobs("senior:rebase", 1, 60_000.0, "W", Some(true))
                .unwrap(),
        );
        let job = &jobs[0];
        let token = job["jobLeaseToken"].as_str().unwrap();
        let history = serde_json::json!([{
            "historyItemId": "initial-config", "loopIteration": 1,
            "producedAt": "2026-01-02T03:04:05Z", "role": "CONFIGURATION",
            "content": [], "model": "gpt", "provider": "openai",
            "systemPrompt": [{"contentType":"TEXT", "text":"Be helpful"}],
        }]);
        // An EQUAL dual-send (both jobLeaseToken and jobLease, same value) is
        // accepted — the alias-based shape previously rejected it as a duplicate
        // field, making the WASM window inconsistent with REST (#1283). The
        // conflicting/missing branches are covered by the pure-reconcile unit
        // test above and the server REST regression suite (they return a
        // `JsValue` error the native test target cannot construct).
        eng.create_agent_instance(
            &serde_json::json!({
                "elementInstanceKey": job["elementInstanceKey"], "jobKey": job["key"],
                "jobLeaseToken": token, "jobLease": token, "history": history,
            })
            .to_string(),
        )
        .expect("equal dual-sent lease fields are accepted");
    }

    #[test]
    fn agent_instance_search_honours_the_nested_rest_filter_shape() {
        let mut eng = TestEngine::new();
        let minted = mint_agent_instance(&mut eng);
        let key = minted["agentInstanceKey"].as_str().unwrap().to_string();

        let count = |body: &str| {
            parse(&eng.search_agent_instances(body).unwrap())["items"]
                .as_array()
                .unwrap()
                .len()
        };

        // The canonical REST envelope `{ "filter": { … } }` is honoured, not
        // silently ignored: a nested key that matches returns the instance, and a
        // nested key that does not match filters it out. Without the unwrap the
        // nested body would be treated as unfiltered and always return 1.
        assert_eq!(
            count(&format!(r#"{{"filter":{{"agentInstanceKey":"{key}"}}}}"#)),
            1
        );
        assert_eq!(count(r#"{"filter":{"agentInstanceKey":"999999999"}}"#), 0);
        // The top-level shorthand keeps working.
        assert_eq!(count(&format!(r#"{{"agentInstanceKey":"{key}"}}"#)), 1);
        assert_eq!(count(r#"{"agentInstanceKey":"999999999"}"#), 0);
    }

    #[test]
    fn agent_history_search_honours_the_nested_rest_filter_shape() {
        let mut eng = TestEngine::new();
        let minted = mint_agent_instance(&mut eng);
        let key = minted["agentInstanceKey"].as_str().unwrap().to_string();
        let req = serde_json::json!({
            "elementInstanceKey": minted["elementInstanceKey"],
            "jobKey": minted["jobKey"],
            "jobLease": minted["jobLease"],
            "status": "THINKING",
            "history": [{
                "historyItemId":"filter-test",
                "loopIteration": 1,
                "producedAt": "2026-01-02T03:04:05.250Z",
                "role": "ASSISTANT",
                "content": [{ "contentType": "TEXT", "text": "hello" }],
            }],
        });
        eng.update_agent_instance(&key, &req.to_string()).unwrap();

        let has_assistant = |body: &str| {
            parse(&eng.search_agent_instance_history(&key, body).unwrap())["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t["role"] == "ASSISTANT")
        };

        assert!(!has_assistant("{}"), "default excludes pending history");
        assert!(has_assistant(r#"{"filter":{"commitStatus":"PENDING"}}"#));
        assert!(has_assistant(r#"{"commitStatus":"PENDING"}"#));
    }

    #[test]
    fn rest_filter_target_unwraps_the_envelope_and_rejects_a_non_object_filter() {
        let obj = |s: &str| match parse(s) {
            J::Object(m) => m,
            _ => unreachable!("test inputs are objects"),
        };
        // Top-level shorthand: the object itself carries the fields.
        let top = obj(r#"{"state":"CREATED"}"#);
        assert_eq!(rest_filter_target(&top).unwrap()["state"], "CREATED");
        // Nested REST envelope: fields come from the `filter` object.
        let nested = obj(r#"{"filter":{"state":"CREATED"}}"#);
        assert_eq!(rest_filter_target(&nested).unwrap()["state"], "CREATED");
        // A null `filter` falls back to the outer object (⇒ unfiltered).
        let null = obj(r#"{"filter":null}"#);
        assert!(rest_filter_target(&null).unwrap().get("state").is_none());
        // A present-but-non-object `filter` is malformed and rejected.
        for bad in [r#"{"filter":42}"#, r#"{"filter":[]}"#, r#"{"filter":"x"}"#] {
            assert!(
                rest_filter_target(&obj(bad)).is_err(),
                "{bad} must be rejected"
            );
        }
    }
}
