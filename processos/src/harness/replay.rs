//! Recorded-input **replay** evaluator — ProcessOS §7.7 Level-2 fidelity and the
//! verifier of the §7.9 hypothesis loop.
//!
//! Where [`super::sim`] serves a candidate's jobs from *modelled* mock workers
//! (Level-1, distributional), this replays a **real recorded instance** against a
//! candidate model: the instance's creation inputs seed it, and each job the
//! candidate issues is served from the recorded output of *that job type* (the
//! Tier-2 stimulus log, captured by `c8 nano --capture`). Routing therefore uses
//! the real historical variable values, and the end state is checkable against the
//! recorded boundary — this is the *backtest* that lets a candidate be scored on
//! how it would have handled history.
//!
//! The result is a **gradient**, not pass/fail (§7.9): validity (compiler-class),
//! completion, per-job-type **coverage** (a job type the candidate issues that
//! history never produced an output for ⇒ *requires a new worker*), and
//! **boundary-conservation** divergence (terminal variables vs the recorded
//! terminal, key by key). That is the actionable feedback an LLM iterates against.
//!
//! Pure and deterministic; `engine-core` is consumed read-only and unmodified. The
//! recorded timeline is authoritative, so no virtual-clock model is needed — the
//! replay walks the recorded `at` timestamps.
//!
//! **Scope of this first slice.** Job-driven replay is the dominant
//! input-compatible case and is handled fully. Catch-event *messages* cannot be
//! injected without a correlation key, so a candidate that introduces message
//! waits history did not satisfy will simply not complete (reported honestly as
//! `completed: false`); timers auto-advance as in `sim`. Truncated captures are
//! rejected up front (an incomplete log is not safe to replay).

use std::collections::HashMap;

use nanobpmn_engine_core::{
    Command, Engine, Event, JobState, MessageSubscriptionState, ProcessDefinition,
    ProcessInstanceState, TimerState, UserTaskState, Value,
};
use serde_json::Value as Json;

use crate::contracts::{InstanceTrace, Variables};

/// Operator/LLM-supplied **generative mock workers** for the Alternate Reality
/// engine, keyed by job type. A mock worker is a (possibly non-deterministic)
/// [`MockWorker`]: a weighted distribution over output deltas. When a candidate
/// issues a job type history never recorded (a *new worker*), replay serves an
/// outcome drawn from the mock instead of completing empty — so a structural
/// change that adds a worker can actually be scored (Level-3, assumption-based)
/// rather than hitting an unreplayable dead end.
///
/// Non-determinism is what lets the harness exercise downstream **splits**: a
/// worker declared as `preApproved: true @0.7 / false @0.3` makes ~70% of the
/// replayed population take the approve branch of an exclusive gateway and ~30%
/// the reject branch — reproducibly, because the outcome is chosen by a stable
/// seed (instance key + job type + invocation index), not a live RNG.
pub type MockWorkers = HashMap<String, MockWorker>;

/// One possible output of a mock worker, with a relative `weight` within its
/// worker's distribution. An outcome is normally a *completion* carrying `output`
/// deltas, but when `error_code` is set it is instead a *business-error throw* —
/// the mock worker raises a BPMN error (consuming the job) so the candidate's
/// error-handling path (an error boundary event) can be exercised under replay.
/// This is how an LLM mocks a *failure*, not just a success, of a new worker.
#[derive(Clone, Debug)]
pub struct MockOutcome {
    pub weight: f64,
    pub output: HashMap<String, Json>,
    /// When set, this outcome throws a BPMN business error with this code instead
    /// of completing the job. `output` is ignored for a throw outcome.
    pub error_code: Option<String>,
    /// Optional human-readable message accompanying a thrown error.
    pub error_message: Option<String>,
}

/// A mock worker for one new job type: a weighted distribution over output
/// deltas. A deterministic worker is the single-outcome case; two or more
/// outcomes model a non-deterministic worker (e.g. an approve/reject decision).
#[derive(Clone, Debug, Default)]
pub struct MockWorker {
    pub outcomes: Vec<MockOutcome>,
}

impl MockWorker {
    /// A deterministic mock that always emits `output`.
    pub fn deterministic(output: HashMap<String, Json>) -> Self {
        Self {
            outcomes: vec![MockOutcome {
                weight: 1.0,
                output,
                error_code: None,
                error_message: None,
            }],
        }
    }

    /// A deterministic mock that always throws the business error `error_code`.
    #[allow(dead_code)] // used in tests and a useful constructor for callers
    pub fn always_throws(error_code: impl Into<String>) -> Self {
        Self {
            outcomes: vec![MockOutcome {
                weight: 1.0,
                output: HashMap::new(),
                error_code: Some(error_code.into()),
                error_message: None,
            }],
        }
    }

    /// True when this worker can emit more than one distinct output.
    #[allow(dead_code)] // used in tests and a useful predicate for callers
    pub fn is_random(&self) -> bool {
        self.outcomes.len() > 1
    }

    /// Choose an outcome for a job invocation, given a stable `seed` in
    /// `[0, u64::MAX]`. Weights are relative; a non-positive total collapses to
    /// the first outcome. Returns `None` only when there are no outcomes.
    fn pick(&self, seed: u64) -> Option<&MockOutcome> {
        match self.outcomes.as_slice() {
            [] => None,
            [only] => Some(only),
            many => {
                let total: f64 = many.iter().map(|o| o.weight.max(0.0)).sum();
                if total <= 0.0 {
                    return Some(&many[0]);
                }
                let target = (seed as f64 / u64::MAX as f64) * total;
                let mut acc = 0.0;
                for o in many {
                    acc += o.weight.max(0.0);
                    if target < acc {
                        return Some(o);
                    }
                }
                many.last()
            }
        }
    }
}

/// Stable FNV-1a 64-bit hash of a string, used to seed mock-outcome selection so
/// replays are reproducible across processes and rebuilds (unlike `DefaultHasher`).
fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Parse a `mockWorkers` JSON argument into [`MockWorkers`]. The value maps each
/// new job type to a worker spec that is **either**:
///
/// * a static output object — `{ "fraud-check": { "fraudScore": 0.1 } }` — a
///   deterministic worker, or
/// * a distribution — `{ "credit-check": { "outcomes": [
///     { "weight": 0.7, "output": { "preApproved": true } },
///     { "weight": 0.3, "output": { "preApproved": false } } ] } }` — a
///   non-deterministic worker whose outcomes are spread across the replayed
///   population (so a downstream split is exercised both ways).
///
/// Either form can model a **failure** instead of a completion. An outcome (or a
/// static spec) carrying `"throwError": "CODE"` (alias `"errorCode"`) raises a BPMN
/// business error from the worker rather than completing it — so the candidate's
/// error boundary path is exercised. A distribution lets a worker fail a fraction
/// of the population: `{ "credit-check": { "outcomes": [
///     { "weight": 0.9, "output": { "score": 700 } },
///     { "weight": 0.1, "throwError": "CREDIT_DECLINED" } ] } }`.
///
/// The distribution form is recognised by an `outcomes` array; any other object is
/// taken verbatim as a single deterministic output (or throw). A non-object (or
/// absent) value, and per-type values that are not objects, yield no mock.
pub fn parse_mock_workers(v: &Json) -> MockWorkers {
    let mut mocks = MockWorkers::new();
    if let Some(obj) = v.as_object() {
        for (job_type, spec) in obj {
            if let Some(worker) = parse_mock_worker(spec) {
                mocks.insert(job_type.clone(), worker);
            }
        }
    }
    mocks
}

/// Read a thrown-error code from a spec/outcome object: `throwError` (preferred)
/// or `errorCode` — each a string. (A bare `error` key is *not* a trigger: it is a
/// common data-variable name and would collide with completion output.) Returns
/// `(error_code, error_message)`.
fn parse_throw(obj: &serde_json::Map<String, Json>) -> (Option<String>, Option<String>) {
    let code = obj
        .get("throwError")
        .or_else(|| obj.get("errorCode"))
        .and_then(|c| c.as_str())
        .map(|s| s.to_string());
    let message = obj
        .get("errorMessage")
        .or_else(|| obj.get("message"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    (code, message)
}

fn parse_mock_worker(spec: &Json) -> Option<MockWorker> {
    let obj = spec.as_object()?;
    match obj.get("outcomes") {
        Some(Json::Array(items)) => {
            let mut outcomes = Vec::new();
            for it in items {
                let io = match it.as_object() {
                    Some(o) => o,
                    None => continue,
                };
                let output: HashMap<String, Json> = io
                    .get("output")
                    .and_then(|o| o.as_object())
                    .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default();
                let weight = io.get("weight").and_then(|w| w.as_f64()).unwrap_or(1.0);
                let (error_code, error_message) = parse_throw(io);
                outcomes.push(MockOutcome {
                    weight,
                    output,
                    error_code,
                    error_message,
                });
            }
            if outcomes.is_empty() {
                None
            } else {
                Some(MockWorker { outcomes })
            }
        }
        _ => {
            let (error_code, error_message) = parse_throw(obj);
            if error_code.is_some() {
                return Some(MockWorker {
                    outcomes: vec![MockOutcome {
                        weight: 1.0,
                        output: HashMap::new(),
                        error_code,
                        error_message,
                    }],
                });
            }
            let output: HashMap<String, Json> =
                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            Some(MockWorker::deterministic(output))
        }
    }
}
#[derive(Clone, Debug)]
pub struct RecordedInstance {
    pub instance_key: String,
    pub process_id: String,
    pub started_at: u64,
    pub creation_variables: HashMap<String, Json>,
    pub stimuli: Vec<RecordedStimulus>,
}

/// One recorded input, values resolved (and `None` when the stimulus carried no
/// payload — e.g. a timer fire or an empty-output job).
#[derive(Clone, Debug)]
pub struct RecordedStimulus {
    pub seq: u32,
    pub at: u64,
    pub kind: String,
    pub reference: Option<String>,
    pub variables: Option<HashMap<String, Json>>,
}

/// Why a trace cannot be replayed (as opposed to a candidate scoring poorly).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayUnavailable {
    /// Capture was off on the source node — no stimulus log present.
    NoCapture,
    /// The per-instance stimulus cap dropped later inputs; the log is incomplete.
    Truncated,
    /// A captured snapshot exceeded the byte cap, so its values are absent.
    SnapshotTruncated,
}

impl std::fmt::Display for ReplayUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCapture => write!(
                f,
                "trace has no recorded-input log (run the source node with c8 nano --capture)"
            ),
            Self::Truncated => write!(
                f,
                "stimulus log was truncated (NANOBPMN_TRACE_STIMULI_MAX reached); not replayable"
            ),
            Self::SnapshotTruncated => write!(
                f,
                "a captured variable snapshot exceeded the byte cap; values unavailable"
            ),
        }
    }
}

impl RecordedInstance {
    /// Distil a fetched [`InstanceTrace`] into a replayable instance, or explain
    /// why it cannot be replayed. Snapshots whose values were dropped by the byte
    /// cap make the trace unreplayable — partial inputs would silently corrupt the
    /// backtest.
    pub fn from_trace(trace: &InstanceTrace) -> Result<Self, ReplayUnavailable> {
        if trace.stimuli_truncated {
            return Err(ReplayUnavailable::Truncated);
        }
        let stimuli_src = trace.stimuli.as_ref().ok_or(ReplayUnavailable::NoCapture)?;

        let creation_variables = match &trace.creation_variables {
            Some(v) => resolve(v)?,
            // Stimuli present but no creation snapshot: an instance created with no
            // variables. Treat as empty inputs rather than unavailable.
            None => HashMap::new(),
        };

        let mut stimuli = Vec::with_capacity(stimuli_src.len());
        for s in stimuli_src {
            let variables = match &s.variables {
                Some(v) => Some(resolve(v)?),
                None => None,
            };
            stimuli.push(RecordedStimulus {
                seq: s.seq,
                at: s.at,
                kind: s.kind.clone(),
                reference: s.reference.clone(),
                variables,
            });
        }
        // The log is authoritative in `seq` order; the gateway emits it ordered,
        // but sort defensively so attribution never depends on transport order.
        stimuli.sort_by_key(|s| s.seq);

        Ok(Self {
            instance_key: trace.instance_key.clone(),
            process_id: trace.process_id.clone(),
            started_at: trace.started_at,
            creation_variables,
            stimuli,
        })
    }
}

/// Resolve a captured [`Variables`] snapshot to a JSON object map, rejecting a
/// truncated snapshot (its values are absent, so replay would be wrong).
fn resolve(v: &Variables) -> Result<HashMap<String, Json>, ReplayUnavailable> {
    if v.truncated {
        return Err(ReplayUnavailable::SnapshotTruncated);
    }
    match &v.values {
        Some(Json::Object(map)) => Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        // A non-object or absent value with truncated == false means "no variables".
        _ => Ok(HashMap::new()),
    }
}

/// Per-job-type coverage of a replay: how many jobs of this type the candidate
/// issued versus how many recorded outputs of that type the history held.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobCoverage {
    pub job_type: String,
    pub issued: u32,
    pub recorded: u32,
}

/// One boundary-conservation difference: a key present in the recorded terminal
/// whose replayed value differs (or is missing).
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VarDivergence {
    pub key: String,
    pub expected: Json,
    pub got: Option<Json>,
}

/// The gradient a single replay yields (§7.9): validity, completion, coverage, and
/// boundary divergence — the actionable feedback, never collapsed to a verdict.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayResult {
    pub instance_key: String,
    /// The candidate deployed and the instance was created (compiler-class check).
    pub valid: bool,
    /// Validity-failure detail when `valid` is false (parse/deploy/create error).
    pub error: Option<String>,
    /// The instance reached the `Completed` terminal state under replay.
    pub completed: bool,
    /// Replayed end-to-end latency along the recorded timeline (last consumed
    /// input's timestamp minus instance start).
    pub e2e_latency_ms: u64,
    /// Engine driver steps taken (a loop-safety / complexity signal).
    pub steps: u32,
    /// Per-job-type issued-vs-recorded counts.
    pub coverage: Vec<JobCoverage>,
    /// Job types the candidate issued more often than history recorded — these
    /// have no historical output to replay ⇒ *requires a new worker* to deploy.
    pub uncovered_job_types: Vec<String>,
    /// Job types served from an operator/LLM-supplied **generative mock** (a new
    /// worker the candidate introduced, scored on the mock's assumed output rather
    /// than recorded history) — Level-3 fidelity, surfaced not hidden.
    pub mocked_job_types: Vec<String>,
    /// Keys of the recorded terminal output the replay failed to reproduce.
    pub divergences: Vec<VarDivergence>,
    /// Boundary conservation held: completed, fully covered, no divergence.
    pub conserved: bool,
}

/// One recorded non-job input (message / native user task / signal / timer)
/// awaiting delivery during replay. `reference` is the catch element id when the
/// source recorded it; `at` is the real timestamp; `vars` the payload delta.
struct PendingInput {
    reference: Option<String>,
    at: u64,
    vars: HashMap<String, Value>,
}

/// Pop the first queued input whose `reference` matches `element_id` (exact catch
/// element); falls back to the front when the source recorded no element id.
fn take_input_for(
    q: &mut std::collections::VecDeque<PendingInput>,
    element_id: &str,
) -> Option<PendingInput> {
    if let Some(pos) = q
        .iter()
        .position(|i| i.reference.as_deref() == Some(element_id))
    {
        return q.remove(pos);
    }
    if matches!(q.front(), Some(i) if i.reference.is_none()) {
        return q.pop_front();
    }
    None
}

/// Replay one recorded instance against a candidate model. `defs` is the parsed
/// candidate (via `engine_core::bpmn::parse_bpmn`); `process_id` is the process to
/// start (the recorded instance's process id is the natural choice).
pub fn replay_instance(
    defs: &[ProcessDefinition],
    process_id: &str,
    rec: &RecordedInstance,
) -> ReplayResult {
    replay_instance_with_mocks(defs, process_id, rec, &MockWorkers::new())
}

/// Like [`replay_instance`], but with operator/LLM-supplied [`MockWorkers`]: when
/// the candidate issues a job type history never recorded, its completion is served
/// from the matching mock's output (and reported in `mocked_job_types`) instead of
/// completing empty + flagged uncovered. This is what lets the Alternate Reality
/// engine score a variant that introduces a **new worker**.
pub fn replay_instance_with_mocks(
    defs: &[ProcessDefinition],
    process_id: &str,
    rec: &RecordedInstance,
    mocks: &MockWorkers,
) -> ReplayResult {
    // The recorded terminal output: creation inputs folded with every stimulus
    // delta in order (last-writer-wins) — what the original run left behind, and
    // the boundary the candidate must conserve.
    let recorded_terminal = recorded_terminal(rec);

    // FIFO of recorded job outputs, keyed by job type. Each entry is that
    // completion's output delta (empty when the job recorded no output).
    let mut job_outputs: HashMap<String, std::collections::VecDeque<HashMap<String, Json>>> =
        HashMap::new();
    let mut recorded_counts: HashMap<String, u32> = HashMap::new();
    // Non-job recorded inputs (messages, native user tasks, signals, timer
    // fires), drained in `seq` order. Each carries the catch element id (when
    // recorded), the real timestamp, and the payload delta it merged.
    let mut msg_inputs: std::collections::VecDeque<PendingInput> =
        std::collections::VecDeque::new();
    let mut usertask_inputs: std::collections::VecDeque<PendingInput> =
        std::collections::VecDeque::new();
    let mut signal_inputs: std::collections::VecDeque<PendingInput> =
        std::collections::VecDeque::new();
    let mut timer_inputs: std::collections::VecDeque<PendingInput> =
        std::collections::VecDeque::new();
    for s in &rec.stimuli {
        if s.kind == "jobCompleted" {
            if let Some(job_type) = &s.reference {
                job_outputs
                    .entry(job_type.clone())
                    .or_default()
                    .push_back(s.variables.clone().unwrap_or_default());
                *recorded_counts.entry(job_type.clone()).or_insert(0) += 1;
            }
            continue;
        }
        let vars: HashMap<String, Value> = s
            .variables
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), json_to_value(v)))
                    .collect()
            })
            .unwrap_or_default();
        let input = PendingInput {
            reference: s.reference.clone(),
            at: s.at,
            vars,
        };
        match s.kind.as_str() {
            "message" => msg_inputs.push_back(input),
            "userTaskCompleted" => usertask_inputs.push_back(input),
            "signal" => signal_inputs.push_back(input),
            "timer" => timer_inputs.push_back(input),
            _ => {}
        }
    }

    let mut engine = Engine::new();
    let mut clock: u64 = rec.started_at;

    if let Err(e) = engine.apply_command_at(Command::DeployResources(defs.to_vec()), clock) {
        return invalid(rec, format!("candidate failed to deploy: {e:?}"));
    }
    let vars: HashMap<String, Value> = rec
        .creation_variables
        .iter()
        .map(|(k, v)| (k.clone(), json_to_value(v)))
        .collect();
    if let Err(e) = engine.apply_command_at(
        Command::CreateInstance {
            process_id: process_id.to_string(),
            variables: vars,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        },
        clock,
    ) {
        return invalid(rec, format!("candidate failed to create instance: {e:?}"));
    }

    let mut issued: HashMap<String, u32> = HashMap::new();
    let mut uncovered: Vec<String> = Vec::new();
    let mut mocked: Vec<String> = Vec::new();

    // Reconstruct the instance's terminal variables by folding every
    // `VariablesUpdated` delta the engine emits during replay (last-writer-wins),
    // seeded with the creation inputs. Reading hot state at the end no longer
    // works: the engine drops a terminal instance's variable payload on
    // completion (ADR 0012), so the exporter/analysis path must project from
    // events instead.
    let mut produced: HashMap<String, Json> = rec
        .creation_variables
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Walk the recorded stimulus timeline only for its timestamps: each job we
    // complete advances the clock to the next recorded input's `at`, so the
    // replayed latency tracks the real timeline rather than a model.
    let mut next_at: std::collections::VecDeque<u64> = rec.stimuli.iter().map(|s| s.at).collect();
    let mut last_consumed_at = rec.started_at;

    let max_steps = 100_000usize;
    let mut steps = 0u32;
    loop {
        steps += 1;
        if steps as usize > max_steps {
            break;
        }

        let pending: Vec<(u64, String, bool)> = engine
            .state()
            .jobs
            .values()
            .filter(|j| matches!(j.state, JobState::Created | JobState::Activated))
            .map(|j| (j.key, j.job_type.clone(), j.state == JobState::Created))
            .collect();

        if pending.is_empty() {
            // No runnable jobs. Try to deliver the next recorded external input to
            // a token parked on a message catch or a native user task; otherwise
            // fire an armed timer (honouring the recorded fire time when we have
            // it). The message name / correlation key come from the engine's own
            // open subscription, so we never re-evaluate a correlation expression.

            // 1. Correlate a recorded message into an open subscription.
            if !msg_inputs.is_empty() {
                let target = {
                    let st = engine.state();
                    let front_ref = msg_inputs.front().and_then(|i| i.reference.clone());
                    st.message_subscriptions
                        .values()
                        .filter(|s| s.state == MessageSubscriptionState::Open)
                        .find(|s| front_ref.as_deref().is_none_or(|r| s.element_id == r))
                        .or_else(|| {
                            st.message_subscriptions
                                .values()
                                .find(|s| s.state == MessageSubscriptionState::Open)
                        })
                        .map(|s| (s.message_name.clone(), s.correlation_key.clone()))
                };
                if let Some((message_name, correlation_key)) = target {
                    let input = msg_inputs.pop_front().unwrap();
                    clock = clock.max(input.at);
                    last_consumed_at = input.at;
                    apply_and_fold(
                        &mut engine,
                        &mut produced,
                        Command::CorrelateMessage {
                            message_name,
                            correlation_key,
                            variables: input.vars,
                            business_id: None,
                        },
                        clock,
                    );
                    continue;
                }
            }

            // 2. Complete a native (Zeebe) user task waiting on a human action.
            if !usertask_inputs.is_empty() {
                let target = {
                    let st = engine.state();
                    let front_ref = usertask_inputs.front().and_then(|i| i.reference.clone());
                    st.user_tasks
                        .values()
                        .filter(|u| u.state == UserTaskState::Created)
                        .find(|u| front_ref.as_deref().is_none_or(|r| u.element_id == r))
                        .or_else(|| {
                            st.user_tasks
                                .values()
                                .find(|u| u.state == UserTaskState::Created)
                        })
                        .map(|u| u.key)
                };
                if let Some(user_task_key) = target {
                    let input = usertask_inputs.pop_front().unwrap();
                    clock = clock.max(input.at);
                    last_consumed_at = input.at;
                    apply_and_fold(
                        &mut engine,
                        &mut produced,
                        Command::CompleteUserTask {
                            user_task_key,
                            variables: input.vars,
                        },
                        clock,
                    );
                    continue;
                }
            }

            // 3. Broadcast a recorded signal to its open subscription. Signals
            // correlate by name only; read the name from the engine's own open
            // subscription, preferring one matching the recorded element ref.
            if !signal_inputs.is_empty() {
                let target = {
                    let st = engine.state();
                    let front_ref = signal_inputs.front().and_then(|i| i.reference.clone());
                    st.signal_subscriptions
                        .values()
                        .filter(|s| s.state == MessageSubscriptionState::Open)
                        .find(|s| front_ref.as_deref().is_none_or(|r| s.element_id == r))
                        .or_else(|| {
                            st.signal_subscriptions
                                .values()
                                .find(|s| s.state == MessageSubscriptionState::Open)
                        })
                        .map(|s| s.signal_name.clone())
                };
                if let Some(signal_name) = target {
                    let input = signal_inputs.pop_front().unwrap();
                    clock = clock.max(input.at);
                    last_consumed_at = input.at;
                    apply_and_fold(
                        &mut engine,
                        &mut produced,
                        Command::BroadcastSignal {
                            signal_name,
                            variables: input.vars,
                        },
                        clock,
                    );
                    continue;
                }
            }

            // 4. Fire the earliest armed timer. Prefer the recorded fire time for
            // that element (so latency tracks history); fall back to the model's
            // nominal due time when history recorded no fire.
            let next_timer = engine
                .state()
                .timers
                .values()
                .filter(|t| t.state == TimerState::Created)
                .map(|t| (t.element_id.clone(), t.due_at))
                .min_by_key(|(_, due)| *due);
            if let Some((element_id, due)) = next_timer {
                let recorded = take_input_for(&mut timer_inputs, &element_id).map(|i| i.at);
                let at = recorded.unwrap_or(due);
                clock = clock.max(at);
                if recorded.is_some() {
                    last_consumed_at = at;
                }
                apply_and_fold(
                    &mut engine,
                    &mut produced,
                    Command::TriggerTimers { now: clock },
                    clock,
                );
                apply_and_fold(
                    &mut engine,
                    &mut produced,
                    Command::ExpireJobs { now: clock },
                    clock,
                );
                continue;
            }
            // Settled: completed, terminated, or parked waiting on an input the
            // recorded history never supplied.
            break;
        }

        for (job_key, job_type, needs_activation) in pending {
            *issued.entry(job_type.clone()).or_insert(0) += 1;

            if needs_activation {
                apply_and_fold(
                    &mut engine,
                    &mut produced,
                    Command::activate_jobs(
                        job_type.clone(),
                        "replay".to_string(),
                        100_000,
                        u64::MAX / 4,
                        clock,
                    ),
                    clock,
                );
            }

            // Serve from the next recorded output of this job type. Advance the
            // clock to the next recorded timestamp so timing tracks history.
            if let Some(at) = next_at.pop_front() {
                clock = clock.max(at);
                last_consumed_at = at;
            }
            let output = job_outputs.get_mut(&job_type).and_then(|q| q.pop_front());
            match output {
                Some(out_vars) => {
                    let out: HashMap<String, Value> = out_vars
                        .iter()
                        .map(|(k, v)| (k.clone(), json_to_value(v)))
                        .collect();
                    apply_and_fold(
                        &mut engine,
                        &mut produced,
                        Command::complete_job_with(job_key, out),
                        clock,
                    );
                }
                None => {
                    // No recorded output of this type remains. If the operator/LLM
                    // supplied a generative mock for this new worker, serve an
                    // outcome drawn from its distribution (Level-3 fidelity);
                    // otherwise complete empty and flag the type as requiring a new
                    // worker (the historic dead end).
                    let inv = issued.get(&job_type).copied().unwrap_or(1);
                    let seed = fnv1a64(&format!("{}|{}|{}", rec.instance_key, job_type, inv));
                    match mocks.get(&job_type).and_then(|m| m.pick(seed)) {
                        Some(mock_out) => {
                            if !mocked.contains(&job_type) {
                                mocked.push(job_type.clone());
                            }
                            if let Some(code) = &mock_out.error_code {
                                // The mock models a *failure*: throw a BPMN business
                                // error so the candidate's error boundary (if any)
                                // runs. With no matching boundary the engine raises
                                // an incident — exactly the historic failure mode.
                                apply_and_fold(
                                    &mut engine,
                                    &mut produced,
                                    Command::throw_job_error(
                                        job_key,
                                        code.clone(),
                                        mock_out.error_message.clone().unwrap_or_default(),
                                    ),
                                    clock,
                                );
                            } else {
                                let out: HashMap<String, Value> = mock_out
                                    .output
                                    .iter()
                                    .map(|(k, v)| (k.clone(), json_to_value(v)))
                                    .collect();
                                apply_and_fold(
                                    &mut engine,
                                    &mut produced,
                                    Command::complete_job_with(job_key, out),
                                    clock,
                                );
                            }
                        }
                        None => {
                            if !uncovered.contains(&job_type) {
                                uncovered.push(job_type.clone());
                            }
                            apply_and_fold(
                                &mut engine,
                                &mut produced,
                                Command::complete_job_with(job_key, HashMap::new()),
                                clock,
                            );
                        }
                    }
                }
            }
        }
    }

    let state = engine.state();
    let inst = state.instances.values().next();
    let completed = inst
        .map(|i| i.state == ProcessInstanceState::Completed)
        .unwrap_or(false);
    // `produced` was reconstructed from the emitted `VariablesUpdated` events
    // above; the completed instance's hot-state variables have been dropped.

    // Boundary conservation: every key in the recorded terminal must reappear
    // with an equal value. Extra keys the candidate adds are not divergences.
    let mut divergences: Vec<VarDivergence> = recorded_terminal
        .iter()
        .filter_map(|(k, expected)| match produced.get(k) {
            Some(got) if got == expected => None,
            other => Some(VarDivergence {
                key: k.clone(),
                expected: expected.clone(),
                got: other.cloned(),
            }),
        })
        .collect();
    divergences.sort_by(|a, b| a.key.cmp(&b.key));

    let mut coverage: Vec<JobCoverage> = issued
        .iter()
        .map(|(job_type, &issued_n)| JobCoverage {
            job_type: job_type.clone(),
            issued: issued_n,
            recorded: recorded_counts.get(job_type).copied().unwrap_or(0),
        })
        .collect();
    coverage.sort_by(|a, b| a.job_type.cmp(&b.job_type));
    uncovered.sort();
    mocked.sort();

    let e2e_latency_ms = last_consumed_at.saturating_sub(rec.started_at);
    let conserved = completed && uncovered.is_empty() && divergences.is_empty();

    ReplayResult {
        instance_key: rec.instance_key.clone(),
        valid: true,
        error: None,
        completed,
        e2e_latency_ms,
        steps,
        coverage,
        uncovered_job_types: uncovered,
        mocked_job_types: mocked,
        divergences,
        conserved,
    }
}

/// The recorded terminal variable state: creation inputs merged with every
/// stimulus delta in `seq` order (last-writer-wins) — the original run's output.
fn recorded_terminal(rec: &RecordedInstance) -> HashMap<String, Json> {
    let mut acc = rec.creation_variables.clone();
    for s in &rec.stimuli {
        if let Some(vars) = &s.variables {
            for (k, v) in vars {
                acc.insert(k.clone(), v.clone());
            }
        }
    }
    acc
}

fn invalid(rec: &RecordedInstance, error: String) -> ReplayResult {
    ReplayResult {
        instance_key: rec.instance_key.clone(),
        valid: false,
        error: Some(error),
        completed: false,
        e2e_latency_ms: 0,
        steps: 0,
        coverage: Vec::new(),
        uncovered_job_types: Vec::new(),
        mocked_job_types: Vec::new(),
        divergences: Vec::new(),
        conserved: false,
    }
}

// --- Dataset scoring: the verifier as a Level-2 candidate scorer ------------------

/// How often a given recorded-terminal output key diverged across the dataset —
/// the candidate's most actionable failure signal (where it fails to reproduce
/// history), sorted most-divergent first.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyCount {
    pub key: String,
    pub count: u32,
}

/// A candidate's **Level-2 scorecard**: the aggregate gradient over a recorded
/// dataset (§7.7/§7.9). This is what the hypothesis loop ranks candidates by — a
/// fidelity-tiered, confidence-bearing summary, never a single verdict.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayReport {
    /// Recorded instances replayed against this candidate.
    pub instances_total: u32,
    /// Of those, how many the candidate could deploy + create + run (the rest are
    /// validity failures — a structurally broken candidate fails all of them).
    pub valid: u32,
    /// How many reached the `Completed` terminal state.
    pub completed: u32,
    /// How many preserved the recorded boundary (completed, fully covered, no
    /// divergence) — the headline fidelity number.
    pub conserved: u32,
    /// `conserved / instances_total` in `[0,1]`; `0.0` when nothing was replayable.
    pub conserved_rate: f64,
    /// Union of job types the candidate issued that the replayed dataset holds
    /// **no recorded output for anywhere** — genuinely new workers a deploy of
    /// this candidate would require (and a generative mock to score).
    pub uncovered_job_types: Vec<String>,
    /// Union of job types that *do* have recorded history in the dataset yet still
    /// ran their recorded-output FIFO dry under this candidate — i.e. the candidate
    /// issued an **existing** worker more often (or on a path) than history did.
    /// This is a **structural-divergence** signal (a broken gateway/condition or a
    /// duplicated branch), NOT a request to mock a real worker.
    pub divergent_job_types: Vec<String>,
    /// Union of job types served from an operator/LLM-supplied generative mock
    /// (the new workers the candidate introduced, scored on assumed output).
    pub mocked_job_types: Vec<String>,
    /// Recorded-terminal keys the candidate most often failed to reproduce.
    pub divergent_keys: Vec<KeyCount>,
    /// Mean replayed end-to-end latency over completed instances (ms).
    pub avg_e2e_latency_ms: f64,
    /// 99th-percentile replayed end-to-end latency over completed instances (ms).
    pub p99_e2e_latency_ms: u64,
    /// Set when the candidate is structurally invalid for the whole dataset (every
    /// instance failed validity) — the loop should treat this as a compiler error.
    pub error: Option<String>,
    /// Per-instance detail for drill-down (the loop can ignore it and rank on the
    /// aggregate).
    pub results: Vec<ReplayResult>,
}

/// Score one candidate against a recorded dataset by replaying every instance and
/// folding the per-instance gradients into a [`ReplayReport`]. Pure and
/// deterministic; the dataset is fetched by the caller (I/O stays out of here).
pub fn replay_dataset(
    defs: &[ProcessDefinition],
    process_id: &str,
    dataset: &[RecordedInstance],
) -> ReplayReport {
    replay_dataset_with_mocks(defs, process_id, dataset, &MockWorkers::new())
}

/// Like [`replay_dataset`], but threads operator/LLM-supplied [`MockWorkers`] into
/// every instance replay, so a candidate introducing new workers is scored on the
/// mocks' assumed outputs (Level-3) rather than failing as unreplayable.
pub fn replay_dataset_with_mocks(
    defs: &[ProcessDefinition],
    process_id: &str,
    dataset: &[RecordedInstance],
    mocks: &MockWorkers,
) -> ReplayReport {
    let results: Vec<ReplayResult> = dataset
        .iter()
        .map(|rec| replay_instance_with_mocks(defs, process_id, rec, mocks))
        .collect();

    let instances_total = results.len() as u32;
    let valid = results.iter().filter(|r| r.valid).count() as u32;
    let completed = results.iter().filter(|r| r.completed).count() as u32;
    let conserved = results.iter().filter(|r| r.conserved).count() as u32;

    // Union of uncovered job types, sorted + deduped. We then split them by
    // whether the dataset records ANY output of that type: a type with zero
    // recorded outputs anywhere is a genuinely-new worker (requires a mock); a
    // type that *does* have recorded history but still ran dry here diverged
    // structurally (the candidate over-issues an existing worker).
    let mut uncovered_all: Vec<String> = results
        .iter()
        .flat_map(|r| r.uncovered_job_types.iter().cloned())
        .collect();
    uncovered_all.sort();
    uncovered_all.dedup();

    let recorded_job_types: std::collections::HashSet<String> = dataset
        .iter()
        .flat_map(|rec| rec.stimuli.iter())
        .filter(|s| s.kind == "jobCompleted")
        .filter_map(|s| s.reference.clone())
        .collect();

    let mut uncovered: Vec<String> = Vec::new();
    let mut divergent: Vec<String> = Vec::new();
    for jt in uncovered_all {
        if recorded_job_types.contains(&jt) {
            divergent.push(jt);
        } else {
            uncovered.push(jt);
        }
    }

    // Union of mocked job types (new workers served from a supplied mock).
    let mut mocked: Vec<String> = results
        .iter()
        .flat_map(|r| r.mocked_job_types.iter().cloned())
        .collect();
    mocked.sort();
    mocked.dedup();

    // Which recorded-terminal keys diverge most across the dataset.
    let mut key_counts: HashMap<String, u32> = HashMap::new();
    for r in &results {
        for d in &r.divergences {
            *key_counts.entry(d.key.clone()).or_insert(0) += 1;
        }
    }
    let mut divergent_keys: Vec<KeyCount> = key_counts
        .into_iter()
        .map(|(key, count)| KeyCount { key, count })
        .collect();
    // Most-divergent first; ties broken by key for determinism.
    divergent_keys.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.key.cmp(&b.key)));

    // Latency aggregates over completed instances.
    let mut lat: Vec<u64> = results
        .iter()
        .filter(|r| r.completed)
        .map(|r| r.e2e_latency_ms)
        .collect();
    lat.sort_unstable();
    let avg_e2e_latency_ms = if lat.is_empty() {
        0.0
    } else {
        lat.iter().sum::<u64>() as f64 / lat.len() as f64
    };
    let p99_e2e_latency_ms = percentile(&lat, 99);

    let conserved_rate = if instances_total == 0 {
        0.0
    } else {
        conserved as f64 / instances_total as f64
    };

    // Whole-dataset validity failure ⇒ compiler-class error for the loop.
    let error = if instances_total > 0 && valid == 0 {
        results
            .iter()
            .find_map(|r| r.error.clone())
            .or_else(|| Some("candidate is invalid for every recorded instance".to_string()))
    } else if instances_total == 0 {
        Some(
            "no replayable instances in the dataset (was the cluster run with --capture?)"
                .to_string(),
        )
    } else {
        None
    };

    ReplayReport {
        instances_total,
        valid,
        completed,
        conserved,
        conserved_rate,
        uncovered_job_types: uncovered,
        divergent_job_types: divergent,
        mocked_job_types: mocked,
        divergent_keys,
        avg_e2e_latency_ms,
        p99_e2e_latency_ms,
        error,
        results,
    }
}

/// Nearest-rank percentile over a pre-sorted ascending slice (`p` in `1..=100`).
fn percentile(sorted: &[u64], p: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p as f64 / 100.0) * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

// --- Value <-> JSON converters (mirror sim.rs / engine-wasm) ---------------------

/// Applies a command to the replay engine and folds any `VariablesUpdated`
/// deltas it emits into `produced` (last-writer-wins), so the caller can
/// reconstruct the instance's terminal variables from the event stream even
/// after the engine drops them on completion (ADR 0012). Command errors are
/// swallowed — the replay treats an unaccepted command as a no-op, matching the
/// prior `let _ =` behaviour.
fn apply_and_fold(
    engine: &mut Engine,
    produced: &mut HashMap<String, Json>,
    cmd: Command,
    clock: u64,
) {
    if let Ok(events) = engine.apply_command_at(cmd, clock) {
        for e in &events {
            if let Event::VariablesUpdated { variables, .. } = e {
                for (k, v) in variables {
                    produced.insert(k.clone(), value_to_json(v));
                }
            }
        }
    }
}

fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Str(s) => Json::String(s.clone()),
        Value::List(items) => Json::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let mut m = serde_json::Map::new();
            for (k, val) in entries {
                m.insert(k.clone(), value_to_json(val));
            }
            Json::Object(m)
        }
    }
}

fn json_to_value(v: &Json) -> Value {
    match v {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::Int(u as i64)
            } else {
                Value::Double(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => Value::Str(s.clone()),
        Json::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        Json::Object(map) => Value::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::bpmn::parse_bpmn;
    use serde_json::json;

    use super::*;

    // Classify -> Summarize (two service tasks, jobs "classify" then "summarize").
    const TWO_TASK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Defs" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="Summarize" />
    <bpmn:serviceTask id="Summarize" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize" /></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Summarize" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    // Classify only (one service task) — a candidate that drops Summarize.
    const ONE_TASK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Defs" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    fn map(pairs: &[(&str, Json)]) -> HashMap<String, Json> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn job(seq: u32, at: u64, job_type: &str, out: Option<&[(&str, Json)]>) -> RecordedStimulus {
        RecordedStimulus {
            seq,
            at,
            kind: "jobCompleted".to_string(),
            reference: Some(job_type.to_string()),
            variables: out.map(map),
        }
    }

    fn rec(creation: &[(&str, Json)], stimuli: Vec<RecordedStimulus>) -> RecordedInstance {
        RecordedInstance {
            instance_key: "1".to_string(),
            process_id: "P".to_string(),
            started_at: 1000,
            creation_variables: map(creation),
            stimuli,
        }
    }

    #[test]
    fn boundary_conservation_holds_for_identical_model() {
        let r = rec(
            &[("input", json!("x"))],
            vec![
                job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                job(2, 1300, "summarize", Some(&[("summary", json!("S"))])),
            ],
        );
        let defs = parse_bpmn(TWO_TASK).unwrap();
        let res = replay_instance(&defs, "P", &r);

        assert!(res.valid && res.completed, "{:?}", res.error);
        assert!(res.conserved, "divergences: {:?}", res.divergences);
        assert!(res.divergences.is_empty());
        assert!(res.uncovered_job_types.is_empty());
        assert_eq!(res.e2e_latency_ms, 300); // 1300 - 1000
        assert_eq!(
            res.coverage,
            vec![
                JobCoverage {
                    job_type: "classify".into(),
                    issued: 1,
                    recorded: 1
                },
                JobCoverage {
                    job_type: "summarize".into(),
                    issued: 1,
                    recorded: 1
                },
            ]
        );
    }

    #[test]
    fn dropping_a_task_diverges_on_the_missing_output_key() {
        // History produced label + summary; the candidate model omits Summarize, so
        // the recorded terminal key `summary` is never reproduced.
        let r = rec(
            &[("input", json!("x"))],
            vec![
                job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                job(2, 1300, "summarize", Some(&[("summary", json!("S"))])),
            ],
        );
        let defs = parse_bpmn(ONE_TASK).unwrap();
        let res = replay_instance(&defs, "P", &r);

        assert!(res.valid && res.completed);
        assert!(!res.conserved);
        assert_eq!(res.divergences.len(), 1);
        assert_eq!(res.divergences[0].key, "summary");
        assert_eq!(res.divergences[0].expected, json!("S"));
        assert_eq!(res.divergences[0].got, None);
        // Only classify was issued.
        assert_eq!(res.coverage.len(), 1);
        assert_eq!(res.coverage[0].job_type, "classify");
    }

    #[test]
    fn a_supplied_mock_worker_serves_a_new_job_type_instead_of_flagging_it_uncovered() {
        // History recorded only classify; the candidate (TWO_TASK) also issues
        // summarize — a NEW worker. The recorded terminal still held summary=S (folded
        // into the creation boundary here). With a mock for summarize that outputs
        // summary=S, replay serves it: the type is reported mocked (not uncovered) and
        // the mock's output reproduces the recorded boundary ⇒ conserved.
        let mut creation = map(&[("input", json!("x"))]);
        creation.insert("summary".into(), json!("S"));
        let r = RecordedInstance {
            instance_key: "1".into(),
            process_id: "P".into(),
            started_at: 1000,
            creation_variables: creation,
            stimuli: vec![job(1, 1100, "classify", Some(&[("label", json!("A"))]))],
        };
        let defs = parse_bpmn(TWO_TASK).unwrap();

        let mut mocks = MockWorkers::new();
        mocks.insert(
            "summarize".to_string(),
            MockWorker::deterministic([("summary".to_string(), json!("S"))].into_iter().collect()),
        );

        let res = replay_instance_with_mocks(&defs, "P", &r, &mocks);
        assert!(res.valid && res.completed);
        assert!(
            res.uncovered_job_types.is_empty(),
            "mocked type must not be uncovered"
        );
        assert_eq!(res.mocked_job_types, vec!["summarize".to_string()]);
        assert!(res.conserved, "divergences: {:?}", res.divergences);
    }

    #[test]
    fn a_nondeterministic_mock_spreads_outcomes_across_the_population() {
        // A single 70/30 worker, when replayed over many instances, must split the
        // population across its two outcomes (deterministically per instance key).
        let worker = MockWorker {
            outcomes: vec![
                MockOutcome {
                    weight: 0.7,
                    output: map(&[("preApproved", json!(true))]),
                    error_code: None,
                    error_message: None,
                },
                MockOutcome {
                    weight: 0.3,
                    output: map(&[("preApproved", json!(false))]),
                    error_code: None,
                    error_message: None,
                },
            ],
        };
        assert!(worker.is_random());

        let mut trues = 0u32;
        let total = 400u32;
        for i in 0..total {
            let key = format!("inst-{i}");
            let seed = fnv1a64(&format!("{key}|credit-check|1"));
            let out = worker.pick(seed).unwrap();
            // Selection is stable for a given key.
            assert_eq!(out.output, worker.pick(seed).unwrap().output);
            if out.output.get("preApproved") == Some(&json!(true)) {
                trues += 1;
            }
        }
        // Expect roughly 70% true; allow a generous band so the test isn't flaky.
        let frac = trues as f64 / total as f64;
        assert!(
            (0.6..0.8).contains(&frac),
            "split was {frac} ({trues}/{total})"
        );
    }

    #[test]
    fn parse_mock_workers_accepts_static_and_distribution_forms() {
        let v = json!({
            "fraud-check": { "isFraud": false },
            "credit-check": { "outcomes": [
                { "weight": 0.7, "output": { "preApproved": true } },
                { "weight": 0.3, "output": { "preApproved": false } }
            ] }
        });
        let mocks = parse_mock_workers(&v);
        assert!(
            !mocks["fraud-check"].is_random(),
            "static form is deterministic"
        );
        assert_eq!(mocks["fraud-check"].outcomes.len(), 1);
        assert!(
            mocks["credit-check"].is_random(),
            "outcomes form is non-deterministic"
        );
        assert_eq!(mocks["credit-check"].outcomes.len(), 2);
    }

    #[test]
    fn parse_mock_workers_reads_throw_error_in_both_forms() {
        // Static throw form, and a distribution where one outcome throws.
        let v = json!({
            "always-fail": { "throwError": "BOOM", "message": "kaboom" },
            "sometimes-fail": { "outcomes": [
                { "weight": 0.9, "output": { "ok": true } },
                { "weight": 0.1, "errorCode": "CREDIT_DECLINED" }
            ] }
        });
        let mocks = parse_mock_workers(&v);

        let always = &mocks["always-fail"];
        assert_eq!(always.outcomes.len(), 1);
        assert_eq!(always.outcomes[0].error_code.as_deref(), Some("BOOM"));
        assert_eq!(always.outcomes[0].error_message.as_deref(), Some("kaboom"));

        let sometimes = &mocks["sometimes-fail"];
        assert_eq!(sometimes.outcomes.len(), 2);
        assert!(sometimes.outcomes[0].error_code.is_none());
        assert_eq!(
            sometimes.outcomes[1].error_code.as_deref(),
            Some("CREDIT_DECLINED")
        );
    }

    // start -> Risk(serviceTask job=risk-check) with an error boundary catching
    // CREDIT_DECLINED -> Rejected end; normal exit -> Approved end. Process id "PR".
    const ERROR_BOUNDARY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:error id="Err_CD" name="CreditDeclined" errorCode="CREDIT_DECLINED" />
  <bpmn:process id="PR" name="PR" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Risk" />
    <bpmn:serviceTask id="Risk" name="Risk">
      <bpmn:extensionElements><zeebe:taskDefinition type="risk-check" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="OnDeclined" attachedToRef="Risk">
      <bpmn:errorEventDefinition errorRef="Err_CD" />
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:boundaryEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="Risk" targetRef="Approved" />
    <bpmn:sequenceFlow id="f3" sourceRef="OnDeclined" targetRef="Rejected" />
    <bpmn:endEvent id="Approved"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="Rejected"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn a_mock_that_throws_routes_through_the_error_boundary() {
        // History recorded nothing for risk-check (a new worker). A mock that THROWS
        // CREDIT_DECLINED must make the engine take the error boundary path and reach
        // the Rejected end — the instance completes via the modelled failure handler,
        // and the type is reported mocked, not uncovered.
        let r = RecordedInstance {
            instance_key: "1".into(),
            process_id: "PR".into(),
            started_at: 1000,
            creation_variables: map(&[("input", json!("x"))]),
            stimuli: vec![],
        };
        let defs = parse_bpmn(ERROR_BOUNDARY).unwrap();

        let mut mocks = MockWorkers::new();
        mocks.insert(
            "risk-check".to_string(),
            MockWorker::always_throws("CREDIT_DECLINED"),
        );

        let res = replay_instance_with_mocks(&defs, "PR", &r, &mocks);
        assert!(res.valid, "model is valid");
        assert!(
            res.completed,
            "the error boundary should route to an end so the instance completes"
        );
        assert!(
            res.uncovered_job_types.is_empty(),
            "a thrown mock is still a supplied worker, not uncovered"
        );
        assert_eq!(res.mocked_job_types, vec!["risk-check".to_string()]);
    }

    #[test]
    fn without_a_mock_the_same_new_worker_is_still_uncovered() {
        let r = rec(
            &[("input", json!("x"))],
            vec![job(1, 1100, "classify", Some(&[("label", json!("A"))]))],
        );
        let defs = parse_bpmn(TWO_TASK).unwrap();
        let res = replay_instance_with_mocks(&defs, "P", &r, &MockWorkers::new());
        assert_eq!(res.uncovered_job_types, vec!["summarize".to_string()]);
        assert!(res.mocked_job_types.is_empty());
    }

    #[test]
    fn issuing_an_unrecorded_job_type_is_flagged_uncovered() {
        // History recorded only a classify output; the candidate also issues
        // summarize, which has no recorded output ⇒ requires a new worker.
        let r = rec(
            &[("input", json!("x"))],
            vec![job(1, 1100, "classify", Some(&[("label", json!("A"))]))],
        );
        let defs = parse_bpmn(TWO_TASK).unwrap();
        let res = replay_instance(&defs, "P", &r);

        assert!(res.valid && res.completed);
        assert_eq!(res.uncovered_job_types, vec!["summarize".to_string()]);
        assert!(!res.conserved, "uncovered jobs must break conservation");
        // summarize shows issued 1 / recorded 0.
        let cov: HashMap<_, _> = res
            .coverage
            .iter()
            .map(|c| (c.job_type.clone(), (c.issued, c.recorded)))
            .collect();
        assert_eq!(cov["summarize"], (1, 0));
        assert_eq!(cov["classify"], (1, 1));
    }

    #[test]
    fn dataset_split_existing_starved_worker_is_divergent_not_uncovered() {
        // Two recorded instances replayed against TWO_TASK (classify -> summarize):
        //  A records BOTH classify + summarize (fully covered),
        //  B records only classify, so summarize runs its FIFO dry in B.
        // Because summarize HAS recorded history somewhere in the dataset, it must be
        // reported as a STRUCTURAL DIVERGENCE (existing worker over-issued), never as a
        // genuinely-new worker the operator should mock.
        let a = rec(
            &[("input", json!("x"))],
            vec![
                job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                job(2, 1200, "summarize", Some(&[("text", json!("done"))])),
            ],
        );
        let b = rec(
            &[("input", json!("y"))],
            vec![job(1, 1100, "classify", Some(&[("label", json!("B"))]))],
        );
        let defs = parse_bpmn(TWO_TASK).unwrap();
        let report = replay_dataset(&defs, "P", &[a, b]);
        assert_eq!(
            report.divergent_job_types,
            vec!["summarize".to_string()],
            "an existing worker that ran dry is a structural divergence"
        );
        assert!(
            report.uncovered_job_types.is_empty(),
            "an existing worker must not be reported as requiring a new worker"
        );
    }

    #[test]
    fn dataset_genuinely_new_worker_stays_uncovered() {
        // History records only classify; the candidate (TWO_TASK) also issues summarize,
        // which is recorded NOWHERE in the dataset ⇒ a genuinely-new worker (uncovered).
        let r = rec(
            &[("input", json!("x"))],
            vec![job(1, 1100, "classify", Some(&[("label", json!("A"))]))],
        );
        let defs = parse_bpmn(TWO_TASK).unwrap();
        let report = replay_dataset(&defs, "P", &[r]);
        assert_eq!(report.uncovered_job_types, vec!["summarize".to_string()]);
        assert!(report.divergent_job_types.is_empty());
    }

    #[test]
    fn invalid_candidate_reports_validity_failure() {
        let r = rec(&[("input", json!("x"))], vec![]);
        let defs = parse_bpmn(TWO_TASK).unwrap();
        // Start a process id the model doesn't define.
        let res = replay_instance(&defs, "DoesNotExist", &r);
        assert!(!res.valid);
        assert!(res.error.is_some());
        assert!(!res.completed);
    }

    #[test]
    fn from_trace_rejects_truncated_log() {
        let trace = InstanceTrace {
            instance_key: "1".into(),
            process_id: "P".into(),
            version: None,
            outcome: "COMPLETED".into(),
            started_at: 0,
            duration_ms: None,
            elements: vec![],
            incidents: vec![],
            creation_variables: None,
            stimuli: Some(vec![]),
            stimuli_truncated: true,
            domain_signals: vec![],
            outcome_truth: None,
        };
        assert!(matches!(
            RecordedInstance::from_trace(&trace),
            Err(ReplayUnavailable::Truncated)
        ));
    }

    #[test]
    fn from_trace_rejects_missing_capture() {
        let trace = InstanceTrace {
            instance_key: "1".into(),
            process_id: "P".into(),
            version: None,
            outcome: "COMPLETED".into(),
            started_at: 0,
            duration_ms: None,
            elements: vec![],
            incidents: vec![],
            creation_variables: None,
            stimuli: None,
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        };
        assert!(matches!(
            RecordedInstance::from_trace(&trace),
            Err(ReplayUnavailable::NoCapture)
        ));
    }

    #[test]
    fn from_trace_rejects_truncated_snapshot() {
        use crate::contracts::{Stimulus, Variables};
        let trace = InstanceTrace {
            instance_key: "1".into(),
            process_id: "P".into(),
            version: None,
            outcome: "COMPLETED".into(),
            started_at: 0,
            duration_ms: None,
            elements: vec![],
            incidents: vec![],
            creation_variables: Some(Variables {
                truncated: true,
                bytes: 99999,
                values: None,
            }),
            stimuli: Some(vec![Stimulus {
                seq: 1,
                at: 1,
                kind: "jobCompleted".into(),
                reference: Some("classify".into()),
                variables: None,
            }]),
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        };
        assert!(matches!(
            RecordedInstance::from_trace(&trace),
            Err(ReplayUnavailable::SnapshotTruncated)
        ));
    }

    #[test]
    fn from_trace_distils_stimuli_in_seq_order() {
        use crate::contracts::{Stimulus, Variables};
        let trace = InstanceTrace {
            instance_key: "7".into(),
            process_id: "P".into(),
            version: None,
            outcome: "COMPLETED".into(),
            started_at: 500,
            duration_ms: None,
            elements: vec![],
            incidents: vec![],
            creation_variables: Some(Variables {
                truncated: false,
                bytes: 10,
                values: Some(json!({ "input": "x" })),
            }),
            // Deliberately out of order to prove the sort.
            stimuli: Some(vec![
                Stimulus {
                    seq: 2,
                    at: 700,
                    kind: "jobCompleted".into(),
                    reference: Some("summarize".into()),
                    variables: Some(Variables {
                        truncated: false,
                        bytes: 5,
                        values: Some(json!({ "summary": "S" })),
                    }),
                },
                Stimulus {
                    seq: 1,
                    at: 600,
                    kind: "jobCompleted".into(),
                    reference: Some("classify".into()),
                    variables: Some(Variables {
                        truncated: false,
                        bytes: 5,
                        values: Some(json!({ "label": "A" })),
                    }),
                },
            ]),
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        };
        let r = RecordedInstance::from_trace(&trace).unwrap();
        assert_eq!(r.started_at, 500);
        assert_eq!(r.creation_variables["input"], json!("x"));
        assert_eq!(r.stimuli.len(), 2);
        assert_eq!(r.stimuli[0].seq, 1);
        assert_eq!(r.stimuli[0].reference.as_deref(), Some("classify"));
        assert_eq!(r.stimuli[1].seq, 2);
    }

    #[test]
    fn replay_dataset_aggregates_conservation_and_divergence() {
        // Three recorded instances, each with classify + summarize outputs.
        let mk = |k: &str| {
            rec(
                &[("input", json!("x"))],
                vec![
                    job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                    job(2, 1300, "summarize", Some(&[("summary", json!(k))])),
                ],
            )
        };
        let dataset = vec![mk("S1"), mk("S2"), mk("S3")];

        // Identical model: every instance conserves the boundary.
        let two = parse_bpmn(TWO_TASK).unwrap();
        let good = replay_dataset(&two, "P", &dataset);
        assert_eq!(good.instances_total, 3);
        assert_eq!(good.valid, 3);
        assert_eq!(good.conserved, 3);
        assert!((good.conserved_rate - 1.0).abs() < 1e-9);
        assert!(good.divergent_keys.is_empty());
        assert!(good.uncovered_job_types.is_empty());
        assert_eq!(good.avg_e2e_latency_ms, 300.0);
        assert_eq!(good.p99_e2e_latency_ms, 300);
        assert!(good.error.is_none());

        // Candidate that drops Summarize: every instance diverges on `summary`.
        let one = parse_bpmn(ONE_TASK).unwrap();
        let bad = replay_dataset(&one, "P", &dataset);
        assert_eq!(bad.conserved, 0);
        assert!((bad.conserved_rate - 0.0).abs() < 1e-9);
        assert_eq!(bad.divergent_keys.len(), 1);
        assert_eq!(bad.divergent_keys[0].key, "summary");
        assert_eq!(bad.divergent_keys[0].count, 3);
    }

    #[test]
    fn replay_dataset_unions_uncovered_job_types() {
        // History recorded only classify; candidate (TWO_TASK) also issues summarize.
        let dataset = vec![
            rec(
                &[("input", json!("x"))],
                vec![job(1, 1100, "classify", Some(&[("label", json!("A"))]))],
            ),
            rec(
                &[("input", json!("y"))],
                vec![job(1, 1100, "classify", Some(&[("label", json!("B"))]))],
            ),
        ];
        let two = parse_bpmn(TWO_TASK).unwrap();
        let report = replay_dataset(&two, "P", &dataset);
        assert_eq!(report.uncovered_job_types, vec!["summarize".to_string()]);
        assert_eq!(report.conserved, 0);
    }

    #[test]
    fn replay_dataset_empty_is_flagged() {
        let two = parse_bpmn(TWO_TASK).unwrap();
        let report = replay_dataset(&two, "P", &[]);
        assert_eq!(report.instances_total, 0);
        assert_eq!(report.conserved_rate, 0.0);
        assert!(report.error.is_some());
    }

    #[test]
    fn replay_dataset_flags_wholly_invalid_candidate() {
        let dataset = vec![rec(&[("input", json!("x"))], vec![])];
        let two = parse_bpmn(TWO_TASK).unwrap();
        // Wrong process id ⇒ every instance is a validity failure.
        let report = replay_dataset(&two, "Nope", &dataset);
        assert_eq!(report.valid, 0);
        assert!(report.error.is_some());
    }

    // --- Non-job recorded inputs: message / user task / timer -----------------
    use nanobpmn_engine_core::ProcessBuilder;

    fn input(
        seq: u32,
        at: u64,
        kind: &str,
        reference: Option<&str>,
        vars: Option<&[(&str, Json)]>,
    ) -> RecordedStimulus {
        RecordedStimulus {
            seq,
            at,
            kind: kind.to_string(),
            reference: reference.map(|s| s.to_string()),
            variables: vars.map(map),
        }
    }

    fn rec_for(
        process_id: &str,
        creation: &[(&str, Json)],
        stimuli: Vec<RecordedStimulus>,
    ) -> RecordedInstance {
        RecordedInstance {
            instance_key: "1".to_string(),
            process_id: process_id.to_string(),
            started_at: 1000,
            creation_variables: map(creation),
            stimuli,
        }
    }

    #[test]
    fn replays_a_correlated_message_into_an_open_catch() {
        // start → prep (job) → await (message catch) → end.
        let def = ProcessBuilder::new("M")
            .start_event("start")
            .service_task("prep", "prep-job")
            .message_intermediate_catch_event("await", "payment", "orderId")
            .end_event("end")
            .connect("start", "prep")
            .connect("prep", "await")
            .connect("await", "end")
            .build()
            .unwrap();

        let r = rec_for(
            "M",
            &[("orderId", json!("o1"))],
            vec![
                job(1, 1100, "prep-job", None),
                input(
                    2,
                    1500,
                    "message",
                    Some("await"),
                    Some(&[("paid", json!(true))]),
                ),
            ],
        );
        let res = replay_instance(&[def], "M", &r);
        assert!(res.valid && res.completed, "{:?}", res.error);
        assert!(res.conserved, "divergences: {:?}", res.divergences);
        assert_eq!(res.e2e_latency_ms, 500); // 1500 - 1000
    }

    #[test]
    fn replays_a_broadcast_signal_into_an_open_catch() {
        // start → prep (job) → await (signal catch) → end.
        let def = ProcessBuilder::new("S")
            .start_event("start")
            .service_task("prep", "prep-job")
            .signal_intermediate_catch_event("await", "all-clear")
            .end_event("end")
            .connect("start", "prep")
            .connect("prep", "await")
            .connect("await", "end")
            .build()
            .unwrap();

        let r = rec_for(
            "S",
            &[],
            vec![
                job(1, 1100, "prep-job", None),
                input(
                    2,
                    1500,
                    "signal",
                    Some("await"),
                    Some(&[("ok", json!(true))]),
                ),
            ],
        );
        let res = replay_instance(&[def], "S", &r);
        assert!(res.valid && res.completed, "{:?}", res.error);
        assert!(res.conserved, "divergences: {:?}", res.divergences);
        assert_eq!(res.e2e_latency_ms, 500); // 1500 - 1000
    }

    #[test]
    fn replays_a_native_user_task_completion() {
        // start → review (user task) → end.
        let def = ProcessBuilder::new("U")
            .start_event("start")
            .user_task("review")
            .end_event("end")
            .connect("start", "review")
            .connect("review", "end")
            .build()
            .unwrap();

        let r = rec_for(
            "U",
            &[],
            vec![input(
                1,
                1400,
                "userTaskCompleted",
                Some("review"),
                Some(&[("decision", json!("approve"))]),
            )],
        );
        let res = replay_instance(&[def], "U", &r);
        assert!(res.valid && res.completed, "{:?}", res.error);
        assert_eq!(
            res.divergences,
            vec![],
            "user-task output should be conserved"
        );
        assert_eq!(res.e2e_latency_ms, 400);
    }

    #[test]
    fn honours_recorded_timer_fire_time_over_model_due() {
        // start → wait (10 minute timer) → end. The model's nominal due is
        // 1000 + 600_000; history shows it actually fired much later.
        let def = ProcessBuilder::new("T")
            .start_event("start")
            .timer_intermediate_catch_event("wait", 600_000)
            .end_event("end")
            .connect("start", "wait")
            .connect("wait", "end")
            .build()
            .unwrap();

        let recorded_fire = 1000 + 900_000;
        let r = rec_for(
            "T",
            &[],
            vec![input(1, recorded_fire, "timer", Some("wait"), None)],
        );
        let res = replay_instance(&[def], "T", &r);
        assert!(res.valid && res.completed, "{:?}", res.error);
        // Latency tracks the recorded fire, not the model's 600s nominal due.
        assert_eq!(res.e2e_latency_ms, 900_000);
    }
}
