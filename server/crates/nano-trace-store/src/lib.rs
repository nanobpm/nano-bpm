//! Tier-A execution-trace projection (process-optimization design doc §3).
//!
//! A [`TraceStore`] consumes the engine's event stream — the *same* ordered
//! `Arc<Vec<Event>>` batches the read-model exporter already processes — and
//! folds the flat events into per-instance, per-element traces. This is the
//! "observe" leg of the optimization loop: a faithful record a human, an LLM and
//! a static analyzer can all read.
//!
//! Design notes:
//! - **In-memory and bounded.** T1 is observability / demo / debug, so the store
//!   keeps the most-recent `capacity` instances (a ring) rather than persisting
//!   every trace. Configure with `NANOBPMN_TRACE_CAPACITY` (default 2000).
//! - **Timing is ingestion time.** The engine does not stamp a wall clock onto
//!   most events (its clock is *injected* — the determinism property), so the
//!   projection stamps each exporter batch with the server's observation time.
//!   That is exact enough for job lifetime because the transitions
//!   (`JobCreated` → `JobCompleted`) occur in *separate* commands at genuinely
//!   different instants. Bit-exact engine-clock timing is a Tier-B (recorded-input
//!   replay) concern, not T1.
//! - **Activation is not observed (yet).** Job *activation* locks are ephemeral:
//!   `Journal::activate_jobs` applies the lock in-engine but does **not** journal
//!   or export a `JobActivated` event (a crashed lease simply re-activates on
//!   restart). So `activatedAt` / `worker` / `queueMs` / `serviceMs` stay `null`
//!   on this projection; the reliably-available signal is `waitMs`
//!   (`createdAt` → `completedAt`, the job's total parked + service time). The
//!   queue-vs-service split (design doc §3 / Fig 2) needs a dedicated
//!   activation-side trace hook and is the immediate next increment — left out of
//!   T1 because it touches the high-frequency activation path. The `JobActivated`
//!   fold below is kept so the split lights up for free once activation is
//!   exported.
//! - **No hot-path cost.** Ingestion runs on the exporter thread, already off the
//!   command-commit/ack path. The default (no-`console`) build never compiles this
//!   module.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use nanobpmn_engine_core::{Event, Value};
use serde::Serialize;

mod sink;
use sink::TraceSink;

const DEFAULT_CAPACITY: usize = 2000;

/// Default per-instance/per-incident cap on a captured variable snapshot,
/// measured as the serialized JSON byte length. A snapshot larger than this is
/// dropped (only its size is reported) so the bounded in-memory store can never
/// be ballooned by a single large payload. Override with
/// `NANOBPMN_TRACE_VARIABLES_MAX_BYTES`.
const DEFAULT_VARS_MAX_BYTES: usize = 16 * 1024;

/// Default per-instance cap on the recorded-input stimulus log.
const DEFAULT_STIMULI_MAX: usize = 1024;

/// Parses a truthy environment flag (`1`/`true`/`yes`/`on`, case-insensitive).
fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// A captured variable map (instance creation inputs, or the state at an
/// incident). Kept only when variable capture is enabled. When the serialized
/// map exceeds the configured byte cap, `values` is dropped and `truncated` is
/// set — the consumer still learns the snapshot existed and how big it was,
/// without the store paying to retain it.
#[derive(Clone)]
struct VarSnapshot {
    values: Option<serde_json::Value>,
    bytes: usize,
    truncated: bool,
}

impl VarSnapshot {
    fn dto(&self) -> VariablesDto {
        VariablesDto {
            truncated: self.truncated,
            bytes: self.bytes,
            values: self.values.clone(),
        }
    }
}

/// Serializes a variable map to JSON and either retains it (within the cap) or
/// records only its size (over the cap). Never panics on a non-serializable map
/// — `serde_json` cannot fail for a `HashMap<String, Value>`.
fn snapshot_vars(vars: &HashMap<String, Value>, max_bytes: usize) -> VarSnapshot {
    // Render natural JSON (e.g. `5`, not `{"Int":5}`) to match the console's
    // existing variable wire shape — the engine `Value` enum serializes
    // externally-tagged, which is not what consumers expect.
    let value = serde_json::Value::Object(
        vars.iter()
            .map(|(k, v)| (k.clone(), nanobpmn_read_model::value_to_json(v)))
            .collect(),
    );
    let bytes = serde_json::to_vec(&value).map(|v| v.len()).unwrap_or(0);
    if bytes > max_bytes {
        VarSnapshot {
            values: None,
            bytes,
            truncated: true,
        }
    } else {
        VarSnapshot {
            values: Some(value),
            bytes,
            truncated: false,
        }
    }
}

/// A bounded, in-memory projection of recent process-instance execution traces.
pub struct TraceStore {
    inner: Mutex<Inner>,
}

struct Inner {
    capacity: usize,
    /// When true, fold instance creation variables and an incident-time variable
    /// snapshot onto the trace (see `NANOBPMN_TRACE_VARIABLES`). Off by default:
    /// variable payloads can be large (the engine has a spill path for exactly
    /// this) and are a privacy surface once exported / shipped to an LLM.
    capture_vars: bool,
    /// When true, fold an ordered per-instance *recorded-input* log — the external
    /// stimuli the engine consumed (job / user-task outputs, message variables,
    /// timer fires, set-variables deltas) — so a historical instance can be
    /// replayed against a candidate model (ProcessOS T2). See
    /// `NANOBPMN_TRACE_STIMULI`. Same footprint/privacy caveats as `capture_vars`.
    capture_stimuli: bool,
    /// Byte cap applied to each captured variable snapshot.
    vars_max_bytes: usize,
    /// Per-instance cap on the number of recorded stimuli (a long-running instance
    /// could otherwise grow without bound). Beyond it the log stops appending and
    /// is flagged truncated.
    stimuli_max: usize,
    /// Trace per instance key.
    instances: HashMap<u64, InstanceTrace>,
    /// Instance keys in insertion order (oldest at the front) for ring eviction.
    order: VecDeque<u64>,
    /// Latest deployed version per process id (best-effort; an instance's exact
    /// definition version is not carried on `ProcessInstanceCreated`).
    versions: HashMap<String, i32>,
    /// Activations observed before their `JobCreated` was folded. Job activation
    /// is not journaled/exported (see module docs), so it is fed directly from the
    /// activation chokepoint via [`TraceStore::record_activations`]; because the
    /// exporter is async, a fast worker can activate before the create batch
    /// projects. Buffered here (keyed by job key) and drained on fold.
    pending_acts: HashMap<u64, PendingAct>,
    /// Optional durability sink (`NANOBPMN_TRACE_FILE`). When present, a finished
    /// instance is appended to the file as one NDJSON line and dropped from the
    /// ring (keeping at most `sink.tail()` recently-finished ones), so memory is
    /// bounded by the active set even with recorded-input capture on.
    sink: Option<TraceSink>,
    /// Keys of recently-finished instances kept in the ring for the console after
    /// being written to the sink (bounded by `sink.tail()`). Empty when no sink is
    /// configured or its tail is zero.
    finished_tail: VecDeque<u64>,
}

/// A job activation seen before its `JobCreated` reached the projection.
struct PendingAct {
    worker: String,
    activated_at: u64,
    attempts: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Active,
    Completed,
    Terminated,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Active => "active",
            Outcome::Completed => "completed",
            Outcome::Terminated => "terminated",
        }
    }
}

#[derive(Clone)]
struct InstanceTrace {
    instance_key: u64,
    process_id: String,
    version: Option<i32>,
    business_id: Option<String>,
    tags: Vec<String>,
    started_at: u64,
    ended_at: Option<u64>,
    /// Last time any event touched this instance (used to bound open spans).
    last_at: u64,
    outcome: Outcome,
    elements: Vec<ElementTrace>,
    by_eik: HashMap<u64, usize>,
    by_job: HashMap<u64, usize>,
    incidents: Vec<IncidentRec>,
    path: Vec<String>,
    /// The instance's creation inputs (`ProcessInstanceCreated.variables`), kept
    /// only when variable capture is enabled. This is the signal ProcessOS T2
    /// replay needs (the original inputs, not the current merged state).
    creation_variables: Option<VarSnapshot>,
    /// Running merged instance variables, maintained only while capture is on so
    /// an incident can snapshot the state the failing expression saw. Not
    /// serialized directly — only sampled into `IncidentRec::variables`.
    current_variables: Option<HashMap<String, Value>>,
    /// Ordered recorded-input log (Tier 2), kept only when stimulus capture is on.
    /// Each entry is one external stimulus the instance consumed; replaying the
    /// creation inputs then these deltas in order reproduces the run's inputs.
    stimuli: Option<Vec<Stimulus>>,
    /// Index of the stimulus awaiting its output `VariablesUpdated` (a completion
    /// event is immediately followed by its variable merge, if any). Bounded to
    /// the immediate aftermath: cleared once the token advances past the element.
    pending_stimulus: Option<usize>,
    /// Set once the per-instance stimulus cap is hit.
    stimuli_truncated: bool,
}

/// One external stimulus consumed by an instance, in observed order.
#[derive(Clone)]
struct Stimulus {
    seq: u32,
    at: u64,
    /// `jobCompleted` | `userTaskCompleted` | `message` | `timer` | `variablesSet`.
    kind: &'static str,
    /// Job type (for `jobCompleted`) or element id (messages / timers); `None`
    /// where the projection cannot attribute one (e.g. a bare `variablesSet`).
    reference: Option<String>,
    /// The variable delta this stimulus carried into the instance (the worker
    /// output / message payload). `None` for a payload-less stimulus (e.g. a
    /// timer fire, or a completion with no output).
    variables: Option<VarSnapshot>,
}

#[derive(Clone)]
struct ElementTrace {
    element_id: String,
    element_instance_key: u64,
    scope: u64,
    entered_at: u64,
    exited_at: Option<u64>,
    incidents: u32,
    job: Option<JobTrace>,
}

#[derive(Clone)]
struct JobTrace {
    job_key: u64,
    job_type: String,
    worker: Option<String>,
    created_at: u64,
    activated_at: Option<u64>,
    completed_at: Option<u64>,
    attempts: u32,
    failures: u32,
}

#[derive(Clone)]
struct IncidentRec {
    element_id: String,
    element_instance_key: u64,
    kind: String,
    reason: String,
    raised_at: u64,
    resolved_at: Option<u64>,
    /// The instance variables visible when the incident was raised — what a FEEL
    /// expression that threw was evaluated against. `None` unless capture is on.
    variables: Option<VarSnapshot>,
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self::build(
            capacity,
            false,
            false,
            DEFAULT_VARS_MAX_BYTES,
            DEFAULT_STIMULI_MAX,
        )
    }

    /// Like [`new`], but with variable capture enabled and a snapshot byte cap.
    /// Used where the caller explicitly opts into capturing variables.
    pub fn with_variables(capacity: usize, vars_max_bytes: usize) -> Self {
        Self::build(
            capacity,
            true,
            false,
            vars_max_bytes.max(1),
            DEFAULT_STIMULI_MAX,
        )
    }

    /// Like [`new`], but with both variable capture and the recorded-input
    /// stimulus log (Tier 2) enabled. Used in tests / explicit opt-in.
    pub fn with_capture(capacity: usize, vars_max_bytes: usize, stimuli_max: usize) -> Self {
        Self::build(
            capacity,
            true,
            true,
            vars_max_bytes.max(1),
            stimuli_max.max(1),
        )
    }

    /// Test-only: a store with capture on and an explicit NDJSON durability sink.
    #[cfg(test)]
    fn with_sink(capacity: usize, cfg: sink::SinkConfig) -> Self {
        Self::build_with_sink(
            capacity,
            true,
            true,
            DEFAULT_VARS_MAX_BYTES,
            DEFAULT_STIMULI_MAX,
            Some(TraceSink::spawn(cfg)),
        )
    }

    fn build(
        capacity: usize,
        capture_vars: bool,
        capture_stimuli: bool,
        vars_max_bytes: usize,
        stimuli_max: usize,
    ) -> Self {
        Self::build_with_sink(
            capacity,
            capture_vars,
            capture_stimuli,
            vars_max_bytes,
            stimuli_max,
            None,
        )
    }

    fn build_with_sink(
        capacity: usize,
        capture_vars: bool,
        capture_stimuli: bool,
        vars_max_bytes: usize,
        stimuli_max: usize,
        sink: Option<TraceSink>,
    ) -> Self {
        Self {
            inner: Mutex::new(Inner {
                capacity: capacity.max(1),
                capture_vars,
                capture_stimuli,
                vars_max_bytes,
                stimuli_max: stimuli_max.max(1),
                instances: HashMap::new(),
                order: VecDeque::new(),
                versions: HashMap::new(),
                pending_acts: HashMap::new(),
                sink,
                finished_tail: VecDeque::new(),
            }),
        }
    }

    /// Builds a store sized from `NANOBPMN_TRACE_CAPACITY` (default 2000).
    /// Variable capture is opt-in via `NANOBPMN_TRACE_VARIABLES` (truthy), with
    /// the per-snapshot byte cap from `NANOBPMN_TRACE_VARIABLES_MAX_BYTES`
    /// (default 16384). The Tier-2 recorded-input log is opt-in via
    /// `NANOBPMN_TRACE_STIMULI` (truthy), capped per instance by
    /// `NANOBPMN_TRACE_STIMULI_MAX` (default 1024); enabling it implies variable
    /// capture so the replay has both its creation inputs and its deltas.
    ///
    /// Durability is opt-in via `NANOBPMN_TRACE_FILE=<path>`: when set, each
    /// finished instance is appended to the file as one NDJSON line and dropped
    /// from the ring, so history survives restart and memory stays bounded by the
    /// active set even with capture on (issue #1343). See [`sink`] for the full
    /// `NANOBPMN_TRACE_FILE_*` set.
    pub fn from_env() -> Self {
        let cap = std::env::var("NANOBPMN_TRACE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_CAPACITY);
        let capture_stimuli = env_flag("NANOBPMN_TRACE_STIMULI");
        // Stimulus capture needs creation inputs to be a complete replay record,
        // so it implies variable capture.
        let capture_vars = capture_stimuli || env_flag("NANOBPMN_TRACE_VARIABLES");
        let vars_max_bytes = std::env::var("NANOBPMN_TRACE_VARIABLES_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_VARS_MAX_BYTES);
        let stimuli_max = std::env::var("NANOBPMN_TRACE_STIMULI_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_STIMULI_MAX);
        Self::build_with_sink(
            cap,
            capture_vars,
            capture_stimuli,
            vars_max_bytes,
            stimuli_max,
            TraceSink::from_env(),
        )
    }

    /// Folds one exporter batch into the trace store. `now` is the server's
    /// observation time for this batch (ms since the Unix epoch).
    pub fn ingest(&self, events: &[&Event], now: u64) {
        let mut inner = self.inner.lock().unwrap();
        for ev in events {
            inner.apply(ev, now);
        }
    }

    /// Records a batch of job activations — `(instance_key, job_key)` pairs all
    /// locked to `worker` at observation time `now`. Activation locks are
    /// ephemeral (never journaled/exported), so this is fed straight from the
    /// activation chokepoint; it is what lets `queueMs` / `serviceMs` / `worker`
    /// / `attempts` populate. Tolerant of the exporter race: an activation seen
    /// before its `JobCreated` is buffered and applied on fold.
    pub fn record_activations(&self, acts: &[(u64, u64)], worker: &str, now: u64) {
        if acts.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        for &(instance_key, job_key) in acts {
            inner.record_activation(instance_key, job_key, worker, now);
        }
    }

    /// Recent instance summaries, most-recent first, capped at `limit`.
    pub fn list(&self, limit: usize) -> Vec<TraceSummaryDto> {
        let inner = self.inner.lock().unwrap();
        inner
            .order
            .iter()
            .rev()
            .filter_map(|k| inner.instances.get(k))
            .take(limit)
            .map(InstanceTrace::summary)
            .collect()
    }

    /// The full trace for one instance (design doc §3 shape).
    pub fn get(&self, instance_key: u64) -> Option<InstanceTraceDto> {
        let inner = self.inner.lock().unwrap();
        inner.instances.get(&instance_key).map(InstanceTrace::dto)
    }

    /// Durability-sink counters `(written, dropped, errors)` when
    /// `NANOBPMN_TRACE_FILE` is configured, else `None`. `written` counts traces
    /// whose bytes survived a successful flush to the OS; `dropped` counts
    /// finished traces discarded because the bounded writer budget was full
    /// (back-pressure is never applied to the engine); `errors` counts traces
    /// lost to a write/flush failure (e.g. a full disk) after reaching the
    /// writer, plus one for a flush that failed with an empty buffer. Useful
    /// for a metric/log.
    pub fn sink_stats(&self) -> Option<(u64, u64, u64)> {
        let inner = self.inner.lock().unwrap();
        inner
            .sink
            .as_ref()
            .map(|s| (s.written(), s.dropped(), s.errors()))
    }

    /// The instance trace as an OTLP/JSON trace document (resource → scope →
    /// spans): one root span for the instance, a child span per element instance,
    /// and a grandchild span per job. Ingestible by an OpenTelemetry collector.
    pub fn otel(&self, instance_key: u64) -> Option<serde_json::Value> {
        let inner = self.inner.lock().unwrap();
        inner.instances.get(&instance_key).map(InstanceTrace::otel)
    }

    /// The current capture configuration and in-memory ring state.
    pub fn config(&self) -> TraceConfigDto {
        let inner = self.inner.lock().unwrap();
        inner.config()
    }

    /// Toggles variable / stimulus capture at runtime. Omitted (`None`) fields
    /// are left unchanged. Enforces the same implication as [`from_env`]:
    /// stimulus capture requires variable capture (a complete replay record),
    /// and clearing variable capture also clears stimulus capture. Returns the
    /// resulting configuration. Node-local and not persisted — a restart resets
    /// capture to the `NANOBPMN_TRACE_*` env defaults.
    pub fn set_capture(&self, variables: Option<bool>, stimuli: Option<bool>) -> TraceConfigDto {
        let mut inner = self.inner.lock().unwrap();
        let mut vars = variables.unwrap_or(inner.capture_vars);
        let mut stim = stimuli.unwrap_or(inner.capture_stimuli);
        // Enabling the dependent (stimuli) implies the base (variables)...
        if stim {
            vars = true;
        }
        // ...but an explicit request to disable the base dominates: clearing
        // variable capture also clears stimulus capture (the documented rule).
        // This resolves a contradictory request — `{variables:false,
        // stimuli:true}` — deterministically to both-off rather than a
        // surprising both-on-then-off state.
        if variables == Some(false) {
            vars = false;
        }
        // Final invariant repair for any residual carried-over combination so
        // `stimuli ⇒ variables` always holds.
        if !vars {
            stim = false;
        }
        inner.capture_vars = vars;
        inner.capture_stimuli = stim;
        inner.config()
    }
}

impl Inner {
    fn config(&self) -> TraceConfigDto {
        // Saturating conversions: the OpenAPI model is signed `int32`/`int64`,
        // so clamp to the signed max to avoid both truncation and a panic on
        // the DTO → generated-model round-trip. These counts are tiny in
        // practice (ring capacity, live instance count), so clamping is inert.
        let i32_max = i32::MAX as usize;
        let i64_max = i64::MAX as usize;
        TraceConfigDto {
            capture_variables: self.capture_vars,
            capture_stimuli: self.capture_stimuli,
            capacity: self.capacity.min(i32_max) as u32,
            vars_max_bytes: self.vars_max_bytes.min(i64_max) as u64,
            stimuli_max: self.stimuli_max.min(i32_max) as u32,
            traced_instances: self.instances.len().min(i32_max) as u32,
        }
    }

    fn apply(&mut self, ev: &Event, now: u64) {
        match ev {
            Event::ProcessDeployed {
                version, process, ..
            } => {
                let entry = self.versions.entry(process.id.clone()).or_insert(*version);
                if *version > *entry {
                    *entry = *version;
                }
            }
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                variables,
                created_at,
                tags,
                business_id,
                version: pinned_version,
                ..
            } => {
                // Prefer the version the instance was pinned to at creation
                // (a by-key / by-id+version create may target a non-latest
                // version); fall back to the latest deployed so far for events
                // written before version pinning (`pinned_version == 0`).
                let version = if *pinned_version != 0 {
                    Some(*pinned_version)
                } else {
                    self.versions.get(process_id).copied()
                };
                let started = if *created_at != 0 { *created_at } else { now };
                let (creation_variables, current_variables) = if self.capture_vars {
                    (
                        Some(snapshot_vars(variables, self.vars_max_bytes)),
                        Some(variables.clone()),
                    )
                } else {
                    (None, None)
                };
                let trace = InstanceTrace {
                    instance_key: *instance_key,
                    process_id: process_id.clone(),
                    version,
                    business_id: business_id.clone(),
                    tags: tags.clone(),
                    started_at: started,
                    ended_at: None,
                    last_at: now,
                    outcome: Outcome::Active,
                    elements: Vec::new(),
                    by_eik: HashMap::new(),
                    by_job: HashMap::new(),
                    incidents: Vec::new(),
                    path: Vec::new(),
                    creation_variables,
                    current_variables,
                    stimuli: if self.capture_stimuli {
                        Some(Vec::new())
                    } else {
                        None
                    },
                    pending_stimulus: None,
                    stimuli_truncated: false,
                };
                self.insert(*instance_key, trace);
            }
            Event::VariablesUpdated {
                instance_key,
                variables,
            } => {
                if !self.capture_vars {
                    return;
                }
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let running = t.current_variables.get_or_insert_with(HashMap::new);
                    for (k, v) in variables {
                        running.insert(k.clone(), v.clone());
                    }
                    // Tier 2: attribute this delta to the completion it followed
                    // (a job/user-task output or a message payload). With no
                    // pending completion it is a standalone set-variables delta.
                    if self.capture_stimuli {
                        let snap = snapshot_vars(variables, self.vars_max_bytes);
                        match t.pending_stimulus.take() {
                            Some(idx) => {
                                if let Some(s) = t.stimuli.as_mut().and_then(|v| v.get_mut(idx)) {
                                    s.variables = Some(snap);
                                }
                            }
                            None => {
                                t.record_stimulus(
                                    "variablesSet",
                                    None,
                                    Some(snap),
                                    now,
                                    self.stimuli_max,
                                );
                            }
                        }
                    }
                }
            }
            Event::ElementActivating {
                instance_key,
                element_instance_key,
                element_id,
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let fresh = !t.by_eik.contains_key(element_instance_key);
                    t.element_mut(*element_instance_key, element_id, now);
                    if fresh {
                        t.path.push(element_id.clone());
                    }
                }
            }
            Event::ElementActivated {
                instance_key,
                element_instance_key,
                element_id,
                scope,
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let idx = t.element_mut(*element_instance_key, element_id, now);
                    t.elements[idx].scope = *scope;
                }
            }
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_eik.get(element_instance_key) {
                        t.elements[idx].exited_at = Some(now);
                    }
                    // The token has advanced past the completed element, so any
                    // completion still awaiting an output had none — stop a later
                    // unrelated delta from being mis-attributed to it.
                    t.pending_stimulus = None;
                }
            }
            Event::JobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id,
                job_type,
                ..
            } => {
                // Apply any activation that raced ahead of this fold (see
                // `pending_acts`). Removed before borrowing the instance.
                let pending = self.pending_acts.remove(job_key);
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let idx = t.element_mut(*element_instance_key, element_id, now);
                    let mut job = JobTrace {
                        job_key: *job_key,
                        job_type: job_type.clone(),
                        worker: None,
                        created_at: now,
                        activated_at: None,
                        completed_at: None,
                        attempts: 0,
                        failures: 0,
                    };
                    if let Some(p) = pending {
                        job.worker = Some(p.worker);
                        job.activated_at = Some(p.activated_at);
                        job.attempts = p.attempts;
                    }
                    t.elements[idx].job = Some(job);
                    t.by_job.insert(*job_key, idx);
                }
            }
            Event::JobActivated {
                job_key,
                instance_key,
                worker,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_job.get(job_key)
                        && let Some(job) = t.elements[idx].job.as_mut()
                    {
                        job.attempts += 1;
                        job.worker = Some(worker.clone());
                        if job.activated_at.is_none() {
                            job.activated_at = Some(now);
                        }
                    }
                }
            }
            Event::JobCompleted {
                job_key,
                instance_key,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let mut job_type = None;
                    if let Some(&idx) = t.by_job.get(job_key)
                        && let Some(job) = t.elements[idx].job.as_mut()
                    {
                        job.completed_at = Some(now);
                        job_type = Some(job.job_type.clone());
                    }
                    // Tier 2: a job completion is an external input; its output
                    // variables (if any) arrive on the next `VariablesUpdated`,
                    // which attaches to this pending stimulus.
                    if self.capture_stimuli {
                        let idx = t.record_stimulus(
                            "jobCompleted",
                            job_type,
                            None,
                            now,
                            self.stimuli_max,
                        );
                        t.pending_stimulus = idx;
                    }
                }
            }
            Event::JobFailed {
                job_key,
                instance_key,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    if let Some(&idx) = t.by_job.get(job_key)
                        && let Some(job) = t.elements[idx].job.as_mut()
                    {
                        job.failures += 1;
                    }
                }
            }
            Event::IncidentRaised {
                instance_key,
                element_instance_key,
                element_id,
                kind,
                reason,
                created_at,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let raised = if *created_at != 0 { *created_at } else { now };
                    if let Some(&idx) = t.by_eik.get(element_instance_key) {
                        t.elements[idx].incidents += 1;
                    }
                    let variables = if self.capture_vars {
                        t.current_variables
                            .as_ref()
                            .map(|m| snapshot_vars(m, self.vars_max_bytes))
                    } else {
                        None
                    };
                    t.incidents.push(IncidentRec {
                        element_id: element_id.clone(),
                        element_instance_key: *element_instance_key,
                        kind: format!("{kind:?}"),
                        reason: reason.clone(),
                        raised_at: raised,
                        resolved_at: None,
                        variables,
                    });
                }
            }
            Event::IncidentResolved {
                instance_key,
                resolved_at,
                ..
            } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    let when = if *resolved_at != 0 { *resolved_at } else { now };
                    if let Some(rec) = t
                        .incidents
                        .iter_mut()
                        .rev()
                        .find(|r| r.resolved_at.is_none())
                    {
                        rec.resolved_at = Some(when);
                    }
                }
            }
            Event::ProcessInstanceCompleted { instance_key } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    t.ended_at = Some(now);
                    t.outcome = Outcome::Completed;
                }
                self.finish_instance(*instance_key);
            }
            Event::ProcessInstanceTerminated { instance_key } => {
                if let Some(t) = self.instances.get_mut(instance_key) {
                    t.last_at = now;
                    t.ended_at = Some(now);
                    t.outcome = Outcome::Terminated;
                }
                self.finish_instance(*instance_key);
            }
            // --- Tier 2 recorded-input stimuli (only when enabled) -------------
            Event::UserTaskCompleted { instance_key, .. } => {
                if self.capture_stimuli
                    && let Some(t) = self.instances.get_mut(instance_key)
                {
                    t.last_at = now;
                    let idx =
                        t.record_stimulus("userTaskCompleted", None, None, now, self.stimuli_max);
                    t.pending_stimulus = idx;
                }
            }
            Event::MessageCorrelated {
                instance_key,
                element_id,
                ..
            } => {
                if self.capture_stimuli
                    && let Some(t) = self.instances.get_mut(instance_key)
                {
                    t.last_at = now;
                    let idx = t.record_stimulus(
                        "message",
                        Some(element_id.clone()),
                        None,
                        now,
                        self.stimuli_max,
                    );
                    t.pending_stimulus = idx;
                }
            }
            Event::RemoteMessageCorrelation {
                instance_key,
                element_id,
                variables,
                ..
            } => {
                // Cross-partition: the message payload is carried inline, so the
                // stimulus is complete on its own (no following local merge).
                if self.capture_stimuli {
                    let snap = snapshot_vars(variables, self.vars_max_bytes);
                    if let Some(t) = self.instances.get_mut(instance_key) {
                        t.last_at = now;
                        t.record_stimulus(
                            "message",
                            Some(element_id.clone()),
                            Some(snap),
                            now,
                            self.stimuli_max,
                        );
                    }
                }
            }
            Event::TimerTriggered {
                instance_key,
                element_id,
                ..
            } => {
                // A timer fire carries no payload, but its occurrence and timing
                // are part of the recorded input ordering.
                if self.capture_stimuli
                    && let Some(t) = self.instances.get_mut(instance_key)
                {
                    t.last_at = now;
                    t.record_stimulus(
                        "timer",
                        Some(element_id.clone()),
                        None,
                        now,
                        self.stimuli_max,
                    );
                }
            }
            _ => {}
        }
    }

    /// Inserts a new instance trace, evicting the oldest if over capacity.
    fn insert(&mut self, key: u64, trace: InstanceTrace) {
        if self.instances.insert(key, trace).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.instances.remove(&old);
            }
        }
    }

    /// Flushes a just-finished instance to the durability sink (one NDJSON line)
    /// and drops it from the in-memory ring, keeping at most `sink.tail()`
    /// recently-finished instances for the console. A no-op when no sink is
    /// configured, so the default path is unchanged.
    fn finish_instance(&mut self, key: u64) {
        let tail = match self.sink.as_ref() {
            None => return,
            Some(sink) => {
                if let Some(t) = self.instances.get(&key)
                    && let Ok(line) = serde_json::to_string(&t.dto())
                {
                    sink.append(line);
                }
                sink.tail()
            }
        };
        if tail == 0 {
            self.remove_instance(key);
        } else {
            self.finished_tail.push_back(key);
            while self.finished_tail.len() > tail {
                if let Some(old) = self.finished_tail.pop_front()
                    && old != key
                {
                    self.remove_instance(old);
                }
            }
        }
    }

    /// Removes an instance from the ring (both the map and the insertion order),
    /// used when the durability sink has taken ownership of a finished trace.
    fn remove_instance(&mut self, key: u64) {
        if self.instances.remove(&key).is_some() {
            self.order.retain(|&k| k != key);
        }
    }

    /// Folds one job activation into its trace, or buffers it if the matching
    /// `JobCreated` has not been projected yet (exporter race). `activated_at` is
    /// set on the first activation only (so `queueMs` reflects the initial queue
    /// wait); `attempts` counts every activation (lease re-activations included).
    fn record_activation(&mut self, instance_key: u64, job_key: u64, worker: &str, now: u64) {
        if let Some(t) = self.instances.get_mut(&instance_key)
            && let Some(&idx) = t.by_job.get(&job_key)
            && let Some(job) = t.elements[idx].job.as_mut()
        {
            job.attempts += 1;
            job.worker = Some(worker.to_string());
            if job.activated_at.is_none() {
                job.activated_at = Some(now);
            }
            if now > t.last_at {
                t.last_at = now;
            }
            return;
        }
        // JobCreated not folded yet (or the instance was evicted): buffer it.
        let entry = self.pending_acts.entry(job_key).or_insert(PendingAct {
            worker: worker.to_string(),
            activated_at: now,
            attempts: 0,
        });
        entry.attempts += 1;
        entry.worker = worker.to_string();
        if now < entry.activated_at {
            entry.activated_at = now;
        }
        // Defensive bound: stale entries only lose activation metadata for a
        // since-evicted job, never correctness.
        if self.pending_acts.len() > self.capacity.saturating_mul(4)
            && let Some(&k) = self.pending_acts.keys().find(|&&k| k != job_key)
        {
            self.pending_acts.remove(&k);
        }
    }
}

impl InstanceTrace {
    /// Returns the index of the element instance, creating it on first sight.
    fn element_mut(&mut self, eik: u64, element_id: &str, now: u64) -> usize {
        if let Some(&idx) = self.by_eik.get(&eik) {
            return idx;
        }
        let idx = self.elements.len();
        self.elements.push(ElementTrace {
            element_id: element_id.to_string(),
            element_instance_key: eik,
            scope: 0,
            entered_at: now,
            exited_at: None,
            incidents: 0,
            job: None,
        });
        self.by_eik.insert(eik, idx);
        idx
    }

    /// Appends a recorded-input stimulus, honouring the per-instance cap. Returns
    /// the new index (so a completion's deferred output can be attached later) or
    /// `None` when capture is off or the cap was reached.
    fn record_stimulus(
        &mut self,
        kind: &'static str,
        reference: Option<String>,
        variables: Option<VarSnapshot>,
        at: u64,
        max: usize,
    ) -> Option<usize> {
        let log = self.stimuli.as_mut()?;
        if log.len() >= max {
            self.stimuli_truncated = true;
            return None;
        }
        let idx = log.len();
        log.push(Stimulus {
            seq: idx as u32,
            at,
            kind,
            reference,
            variables,
        });
        Some(idx)
    }

    fn summary(&self) -> TraceSummaryDto {
        TraceSummaryDto {
            instance_key: self.instance_key.to_string(),
            process_id: self.process_id.clone(),
            version: self.version,
            business_id: self.business_id.clone(),
            outcome: self.outcome.as_str(),
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_ms: self.ended_at.map(|e| e.saturating_sub(self.started_at)),
            element_count: self.elements.len(),
            incident_count: self.incidents.len(),
        }
    }

    fn dto(&self) -> InstanceTraceDto {
        InstanceTraceDto {
            instance_key: self.instance_key.to_string(),
            process_id: self.process_id.clone(),
            version: self.version,
            business_id: self.business_id.clone(),
            tags: self.tags.clone(),
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_ms: self.ended_at.map(|e| e.saturating_sub(self.started_at)),
            outcome: self.outcome.as_str(),
            elements: self
                .elements
                .iter()
                .map(|e| {
                    let job = e.job.as_ref().map(|j| JobDto {
                        job_type: j.job_type.clone(),
                        worker: j.worker.clone(),
                        created_at: j.created_at,
                        activated_at: j.activated_at,
                        completed_at: j.completed_at,
                        // Total parked + service time. Reliable because both
                        // endpoints are exported (unlike activation).
                        wait_ms: j.completed_at.map(|c| c.saturating_sub(j.created_at)),
                        queue_ms: j.activated_at.map(|a| a.saturating_sub(j.created_at)),
                        service_ms: match (j.activated_at, j.completed_at) {
                            (Some(a), Some(c)) => Some(c.saturating_sub(a)),
                            _ => None,
                        },
                        attempts: j.attempts,
                        failures: j.failures,
                    });
                    ElementDto {
                        element_id: e.element_id.clone(),
                        element_instance_key: e.element_instance_key.to_string(),
                        scope: e.scope.to_string(),
                        entered_at: e.entered_at,
                        exited_at: e.exited_at,
                        duration_ms: e.exited_at.map(|x| x.saturating_sub(e.entered_at)),
                        incidents: e.incidents,
                        job,
                    }
                })
                .collect(),
            incidents: self
                .incidents
                .iter()
                .map(|i| IncidentDto {
                    element_id: i.element_id.clone(),
                    element_instance_key: i.element_instance_key.to_string(),
                    kind: i.kind.clone(),
                    reason: i.reason.clone(),
                    raised_at: i.raised_at,
                    resolved_at: i.resolved_at,
                    variables: i.variables.as_ref().map(VarSnapshot::dto),
                })
                .collect(),
            path: self.path.clone(),
            creation_variables: self.creation_variables.as_ref().map(VarSnapshot::dto),
            stimuli: self.stimuli.as_ref().map(|log| {
                log.iter()
                    .map(|s| StimulusDto {
                        seq: s.seq,
                        at: s.at,
                        kind: s.kind,
                        reference: s.reference.clone(),
                        variables: s.variables.as_ref().map(VarSnapshot::dto),
                    })
                    .collect()
            }),
            stimuli_truncated: self.stimuli_truncated,
        }
    }

    /// Renders the trace as an OTLP/JSON trace document.
    fn otel(&self) -> serde_json::Value {
        use serde_json::json;

        let trace_id = format!("{:032x}", self.instance_key as u128);
        let root_span_id = span_id(self.instance_key);
        let inst_end = self.ended_at.unwrap_or(self.last_at);

        let mut spans = Vec::new();
        spans.push(json!({
            "traceId": trace_id,
            "spanId": root_span_id,
            "name": format!("process {}", self.process_id),
            "kind": 1,
            "startTimeUnixNano": nanos(self.started_at),
            "endTimeUnixNano": nanos(inst_end),
            "attributes": [
                attr_str("nanobpmn.process_id", &self.process_id),
                attr_str("nanobpmn.instance_key", &self.instance_key.to_string()),
                attr_str("nanobpmn.outcome", self.outcome.as_str()),
                attr_int("nanobpmn.element_count", self.elements.len() as i64),
                attr_int("nanobpmn.incident_count", self.incidents.len() as i64),
            ],
        }));

        for e in &self.elements {
            let el_span = span_id(e.element_instance_key);
            let el_end = e.exited_at.unwrap_or(self.last_at);
            spans.push(json!({
                "traceId": trace_id,
                "spanId": el_span,
                "parentSpanId": root_span_id,
                "name": e.element_id,
                "kind": 1,
                "startTimeUnixNano": nanos(e.entered_at),
                "endTimeUnixNano": nanos(el_end),
                "attributes": [
                    attr_str("nanobpmn.element_id", &e.element_id),
                    attr_int("nanobpmn.incidents", e.incidents as i64),
                ],
            }));

            if let Some(j) = &e.job {
                let j_start = j.activated_at.unwrap_or(j.created_at);
                let j_end = j.completed_at.unwrap_or(self.last_at);
                let mut attrs = vec![
                    attr_str("nanobpmn.job_type", &j.job_type),
                    attr_int("nanobpmn.attempts", j.attempts as i64),
                    attr_int("nanobpmn.failures", j.failures as i64),
                ];
                if let Some(w) = &j.worker {
                    attrs.push(attr_str("nanobpmn.worker", w));
                }
                if let Some(a) = j.activated_at {
                    attrs.push(attr_int(
                        "nanobpmn.queue_ms",
                        a.saturating_sub(j.created_at) as i64,
                    ));
                }
                if let Some(c) = j.completed_at {
                    attrs.push(attr_int(
                        "nanobpmn.wait_ms",
                        c.saturating_sub(j.created_at) as i64,
                    ));
                }
                if let (Some(a), Some(c)) = (j.activated_at, j.completed_at) {
                    attrs.push(attr_int("nanobpmn.service_ms", c.saturating_sub(a) as i64));
                }
                spans.push(json!({
                    "traceId": trace_id,
                    "spanId": span_id(j.job_key ^ 0x6a6f_6200),
                    "parentSpanId": el_span,
                    "name": format!("job {}", j.job_type),
                    "kind": 3,
                    "startTimeUnixNano": nanos(j_start),
                    "endTimeUnixNano": nanos(j_end),
                    "attributes": attrs,
                }));
            }
        }

        json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [ attr_str("service.name", "nanobpmn") ]
                },
                "scopeSpans": [{
                    "scope": { "name": "nanobpmn.engine" },
                    "spans": spans,
                }]
            }]
        })
    }
}

fn span_id(seed: u64) -> String {
    // Mix so distinct keys (instance vs element) don't collide on low bits.
    let mixed = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (seed >> 29);
    format!("{mixed:016x}")
}

/// OTLP encodes 64-bit unix-nano timestamps as decimal strings.
fn nanos(ms: u64) -> String {
    (ms.saturating_mul(1_000_000)).to_string()
}

fn attr_str(key: &str, value: &str) -> serde_json::Value {
    serde_json::json!({ "key": key, "value": { "stringValue": value } })
}

fn attr_int(key: &str, value: i64) -> serde_json::Value {
    serde_json::json!({ "key": key, "value": { "intValue": value } })
}

// ---------------------------------------------------------------------------
// DTOs (camelCase, keys as strings — matching the console's metrics/health DTOs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceConfigDto {
    pub capture_variables: bool,
    pub capture_stimuli: bool,
    pub capacity: u32,
    pub vars_max_bytes: u64,
    pub stimuli_max: u32,
    pub traced_instances: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSummaryDto {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    pub business_id: Option<String>,
    pub outcome: &'static str,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub element_count: usize,
    pub incident_count: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceTraceDto {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    pub business_id: Option<String>,
    pub tags: Vec<String>,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub outcome: &'static str,
    pub elements: Vec<ElementDto>,
    pub incidents: Vec<IncidentDto>,
    pub path: Vec<String>,
    /// The instance creation inputs, when variable capture is enabled. Omitted
    /// from the JSON entirely when capture is off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_variables: Option<VariablesDto>,
    /// The ordered recorded-input log (Tier 2), when stimulus capture is enabled.
    /// Omitted from the JSON entirely when capture is off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stimuli: Option<Vec<StimulusDto>>,
    /// True when the per-instance stimulus cap was reached and later inputs were
    /// dropped from the log.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub stimuli_truncated: bool,
}

/// One recorded external input on the trace's Tier-2 log.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StimulusDto {
    pub seq: u32,
    pub at: u64,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<VariablesDto>,
}

/// A captured variable map exposed on the trace. `values` is the JSON object of
/// variable name → value; it is omitted (with `truncated: true`) when the
/// snapshot exceeded the configured byte cap.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VariablesDto {
    pub truncated: bool,
    pub bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<serde_json::Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ElementDto {
    pub element_id: String,
    pub element_instance_key: String,
    pub scope: String,
    pub entered_at: u64,
    pub exited_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub incidents: u32,
    pub job: Option<JobDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDto {
    #[serde(rename = "type")]
    pub job_type: String,
    pub worker: Option<String>,
    pub created_at: u64,
    pub activated_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub wait_ms: Option<u64>,
    pub queue_ms: Option<u64>,
    pub service_ms: Option<u64>,
    pub attempts: u32,
    pub failures: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncidentDto {
    pub element_id: String,
    pub element_instance_key: String,
    pub kind: String,
    pub reason: String,
    pub raised_at: u64,
    pub resolved_at: Option<u64>,
    /// The instance variables the failing element saw, when capture is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<VariablesDto>,
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::IncidentKind;

    use super::*;

    fn created(key: u64, vars: &[(&str, Value)]) -> Event {
        Event::ProcessInstanceCreated {
            instance_key: key,
            process_id: "p".to_string(),
            variables: vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            created_at: 100,
            tags: vec![],
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    fn incident(key: u64, eik: u64) -> Event {
        Event::IncidentRaised {
            incident_key: 9,
            instance_key: key,
            element_instance_key: eik,
            element_id: "gw".to_string(),
            kind: IncidentKind::ExpressionEvaluation,
            reason: "FEEL: '+'(amount, null)".to_string(),
            job_key: None,
            created_at: 200,
            redrive: None,
        }
    }

    #[test]
    fn capture_off_keeps_no_variables() {
        let store = TraceStore::new(8);
        store.ingest(&[&created(1, &[("amount", Value::Int(5))])], 1000);
        store.ingest(&[&incident(1, 0)], 1100);
        let dto = store.get(1).unwrap();
        assert!(dto.creation_variables.is_none());
        assert!(dto.incidents[0].variables.is_none());
    }

    #[test]
    fn set_capture_toggles_flags_and_enforces_implication() {
        let store = TraceStore::new(8);
        // Defaults: everything off.
        let c = store.config();
        assert!(!c.capture_variables && !c.capture_stimuli);
        assert_eq!(c.capacity, 8);

        // Enabling variables alone leaves stimuli off.
        let c = store.set_capture(Some(true), None);
        assert!(c.capture_variables && !c.capture_stimuli);

        // Enabling stimuli implies variables even if variables is not sent.
        let c = store.set_capture(None, Some(true));
        assert!(c.capture_variables && c.capture_stimuli);

        // Omitted fields are left unchanged.
        let c = store.set_capture(None, None);
        assert!(c.capture_variables && c.capture_stimuli);

        // Clearing variables also clears stimuli.
        let c = store.set_capture(Some(false), None);
        assert!(!c.capture_variables && !c.capture_stimuli);

        // A contradictory request resolves deterministically to both-off: an
        // explicit variables=false dominates (clearing the base clears the
        // dependent), matching the documented rule.
        store.set_capture(Some(true), Some(true));
        let c = store.set_capture(Some(false), Some(true));
        assert!(!c.capture_variables && !c.capture_stimuli);
    }

    #[test]
    fn set_capture_takes_effect_on_subsequent_ingest() {
        let store = TraceStore::new(8);
        store.ingest(&[&created(1, &[("amount", Value::Int(5))])], 1000);
        assert!(store.get(1).unwrap().creation_variables.is_none());

        store.set_capture(Some(true), None);
        store.ingest(&[&created(2, &[("amount", Value::Int(9))])], 2000);
        assert!(store.get(2).unwrap().creation_variables.is_some());
    }

    #[test]
    fn captures_creation_inputs_and_incident_snapshot() {
        let store = TraceStore::with_variables(8, 16 * 1024);
        store.ingest(&[&created(1, &[("amount", Value::Int(5))])], 1000);
        // A later merge should be visible to the incident snapshot.
        store.ingest(
            &[&Event::VariablesUpdated {
                instance_key: 1,
                variables: [("fee".to_string(), Value::Str("late".to_string()))]
                    .into_iter()
                    .collect(),
            }],
            1050,
        );
        store.ingest(&[&incident(1, 0)], 1100);
        let dto = store.get(1).unwrap();

        let creation = dto.creation_variables.expect("creation vars");
        assert!(!creation.truncated);
        let cv = creation.values.unwrap();
        assert_eq!(cv["amount"], serde_json::json!(5));
        assert!(
            cv.get("fee").is_none(),
            "creation snapshot is the original inputs only"
        );

        let snap = dto.incidents[0].variables.as_ref().expect("incident vars");
        let sv = snap.values.as_ref().unwrap();
        assert_eq!(sv["amount"], serde_json::json!(5));
        assert_eq!(
            sv["fee"],
            serde_json::json!("late"),
            "snapshot reflects merges at incident time"
        );
    }

    #[test]
    fn oversized_snapshot_is_truncated_not_retained() {
        let store = TraceStore::with_variables(8, 16);
        let big = "x".repeat(1024);
        store.ingest(&[&created(1, &[("blob", Value::Str(big))])], 1000);
        let dto = store.get(1).unwrap();
        let creation = dto.creation_variables.unwrap();
        assert!(creation.truncated);
        assert!(creation.values.is_none());
        assert!(creation.bytes > 16);
    }

    fn job_created(key: u64, job_key: u64, eik: u64, ty: &str) -> Event {
        Event::JobCreated {
            job_key,
            instance_key: key,
            element_instance_key: eik,
            element_id: format!("task-{ty}"),
            job_type: ty.to_string(),
            created_at: 0,
            priority: 50,
            retries: 3,
        }
    }

    fn vars_updated(key: u64, vars: &[(&str, Value)]) -> Event {
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    #[test]
    fn stimuli_off_keeps_no_log() {
        // Tier 1 only: variables captured, but no recorded-input log.
        let store = TraceStore::with_variables(8, 16 * 1024);
        store.ingest(&[&created(1, &[("a", Value::Int(1))])], 1000);
        store.ingest(&[&job_created(1, 10, 2, "classify")], 1010);
        store.ingest(
            &[
                &Event::JobCompleted {
                    job_key: 10,
                    instance_key: 1,
                    created_at: 0,
                    job_type: String::new(),
                    worker: None,
                },
                &vars_updated(1, &[("label", Value::Str("vip".into()))]),
            ],
            1020,
        );
        assert!(store.get(1).unwrap().stimuli.is_none());
    }

    #[test]
    fn records_job_output_message_and_timer_in_order() {
        let store = TraceStore::with_capture(8, 16 * 1024, 1024);
        store.ingest(&[&created(1, &[("a", Value::Int(1))])], 1000);
        store.ingest(&[&job_created(1, 10, 2, "classify")], 1010);
        // Job completes with output → JobCompleted then its VariablesUpdated.
        store.ingest(
            &[
                &Event::JobCompleted {
                    job_key: 10,
                    instance_key: 1,
                    created_at: 0,
                    job_type: String::new(),
                    worker: None,
                },
                &vars_updated(1, &[("label", Value::Str("vip".into()))]),
                &Event::ElementCompleted {
                    instance_key: 1,
                    element_instance_key: 2,
                    element_id: "task-classify".into(),
                },
            ],
            1020,
        );
        // A message correlation with payload.
        store.ingest(
            &[
                &Event::MessageCorrelated {
                    subscription_key: 5,
                    message_key: 6,
                    instance_key: 1,
                    element_instance_key: 3,
                    element_id: "catch".into(),
                },
                &vars_updated(1, &[("approved", Value::Bool(true))]),
            ],
            1030,
        );
        // A timer fire (no payload).
        store.ingest(
            &[&Event::TimerTriggered {
                timer_key: 7,
                instance_key: 1,
                element_instance_key: 4,
                element_id: "wait".into(),
            }],
            1040,
        );

        let stimuli = store.get(1).unwrap().stimuli.expect("stimuli log");
        assert_eq!(stimuli.len(), 3);

        assert_eq!(stimuli[0].kind, "jobCompleted");
        assert_eq!(stimuli[0].reference.as_deref(), Some("classify"));
        assert_eq!(
            stimuli[0]
                .variables
                .as_ref()
                .unwrap()
                .values
                .as_ref()
                .unwrap()["label"],
            serde_json::json!("vip"),
            "job output delta attributed to its completion"
        );

        assert_eq!(stimuli[1].kind, "message");
        assert_eq!(stimuli[1].reference.as_deref(), Some("catch"));
        assert_eq!(
            stimuli[1]
                .variables
                .as_ref()
                .unwrap()
                .values
                .as_ref()
                .unwrap()["approved"],
            serde_json::json!(true)
        );

        assert_eq!(stimuli[2].kind, "timer");
        assert!(stimuli[2].variables.is_none(), "timer carries no payload");
    }

    #[test]
    fn empty_output_job_does_not_swallow_a_later_set_variables() {
        let store = TraceStore::with_capture(8, 16 * 1024, 1024);
        store.ingest(&[&created(1, &[("a", Value::Int(1))])], 1000);
        store.ingest(&[&job_created(1, 10, 2, "noop")], 1010);
        // Job completes with NO output; the token advances (ElementCompleted).
        store.ingest(
            &[
                &Event::JobCompleted {
                    job_key: 10,
                    instance_key: 1,
                    created_at: 0,
                    job_type: String::new(),
                    worker: None,
                },
                &Event::ElementCompleted {
                    instance_key: 1,
                    element_instance_key: 2,
                    element_id: "task-noop".into(),
                },
            ],
            1020,
        );
        // A later standalone set-variables delta must NOT attach to the job.
        store.ingest(&[&vars_updated(1, &[("flag", Value::Bool(true))])], 1030);

        let stimuli = store.get(1).unwrap().stimuli.unwrap();
        assert_eq!(stimuli.len(), 2);
        assert_eq!(stimuli[0].kind, "jobCompleted");
        assert!(
            stimuli[0].variables.is_none(),
            "empty-output job stays varless"
        );
        assert_eq!(stimuli[1].kind, "variablesSet");
        assert_eq!(
            stimuli[1]
                .variables
                .as_ref()
                .unwrap()
                .values
                .as_ref()
                .unwrap()["flag"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn adhoc_tool_activations_appear_as_read_model_element_instances() {
        // ADR 0023 seam 2: an ad-hoc container's tools are real element
        // instances in the engine event stream (ElementActivated /
        // ElementCompleted), not opaque job internals. The trace read model must
        // therefore surface each tool activation as a distinct element instance
        // scoped to the container — the observability guarantee the agentic loop
        // (and the AI-agent parity E2E) relies on.
        let store = TraceStore::new(16);
        store.ingest(&[&created(1, &[])], 1000);
        // The ad-hoc container element activates (eik 2) and parks on its agent job.
        store.ingest(
            &[&Event::ElementActivated {
                instance_key: 1,
                element_instance_key: 2,
                element_id: "AI_Agent".into(),
                scope: 1,
            }],
            1010,
        );
        // The agent returns two tools; each is activated as a child element
        // scoped to the container (eik 2).
        store.ingest(
            &[
                &Event::ElementActivated {
                    instance_key: 1,
                    element_instance_key: 3,
                    element_id: "search_recipe".into(),
                    scope: 2,
                },
                &Event::ElementActivated {
                    instance_key: 1,
                    element_instance_key: 4,
                    element_id: "book_table".into(),
                    scope: 2,
                },
            ],
            1020,
        );
        // Both tools complete, then the container completes.
        store.ingest(
            &[
                &Event::ElementCompleted {
                    instance_key: 1,
                    element_instance_key: 3,
                    element_id: "search_recipe".into(),
                },
                &Event::ElementCompleted {
                    instance_key: 1,
                    element_instance_key: 4,
                    element_id: "book_table".into(),
                },
            ],
            1030,
        );

        let dto = store.get(1).unwrap();
        let tool_a = dto
            .elements
            .iter()
            .find(|e| e.element_id == "search_recipe")
            .expect("tool A is a read-model element instance");
        let tool_b = dto
            .elements
            .iter()
            .find(|e| e.element_id == "book_table")
            .expect("tool B is a read-model element instance");
        assert_eq!(tool_a.element_instance_key, "3");
        assert_eq!(tool_a.scope, "2", "tool scoped to the ad-hoc container eik");
        assert!(tool_a.exited_at.is_some(), "tool completion recorded");
        assert_eq!(tool_b.element_instance_key, "4");
        assert_eq!(tool_b.scope, "2");
        assert!(tool_b.exited_at.is_some());
        // The container itself is also an element instance in the trace.
        assert!(
            dto.elements.iter().any(|e| e.element_id == "AI_Agent"),
            "the ad-hoc container is a read-model element instance"
        );
    }

    #[test]
    fn stimulus_log_is_capped_per_instance() {
        let store = TraceStore::with_capture(8, 16 * 1024, 2);
        store.ingest(&[&created(1, &[])], 1000);
        for i in 0..5u64 {
            store.ingest(
                &[&vars_updated(1, &[("n", Value::Int(i as i64))])],
                1010 + i,
            );
        }
        let dto = store.get(1).unwrap();
        assert_eq!(dto.stimuli.unwrap().len(), 2);
        assert!(dto.stimuli_truncated);
    }

    fn temp_trace_path(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nano-trace-{tag}-{}-{nanos}.ndjson",
            std::process::id()
        ))
    }

    fn sink_cfg(path: std::path::PathBuf) -> sink::SinkConfig {
        sink::SinkConfig {
            path,
            max_bytes: None,
            keep: 5,
            queue_bytes: 16 * 1024 * 1024,
            flush_interval: std::time::Duration::from_millis(20),
            tail: 0,
        }
    }

    fn completed(key: u64) -> Event {
        Event::ProcessInstanceCompleted { instance_key: key }
    }

    fn terminated(key: u64) -> Event {
        Event::ProcessInstanceTerminated { instance_key: key }
    }

    fn terminating(key: u64) -> Event {
        Event::ProcessInstanceTerminating { instance_key: key }
    }

    #[test]
    fn no_sink_by_default_keeps_finished_in_ring() {
        let store = TraceStore::with_capture(8, 16 * 1024, 16);
        store.ingest(&[&created(1, &[("a", Value::Int(1))])], 1000);
        store.ingest(&[&completed(1)], 1100);
        // Default behaviour: the finished instance stays in the ring and no sink
        // stats exist.
        assert!(store.get(1).is_some());
        assert_eq!(store.list(10).len(), 1);
        assert!(store.sink_stats().is_none());
    }

    #[test]
    fn finished_instance_is_written_once_and_dropped_from_ring() {
        let path = temp_trace_path("one-line");
        let _ = std::fs::remove_file(&path);
        {
            let store = TraceStore::with_sink(8, sink_cfg(path.clone()));
            store.ingest(&[&created(1, &[("amount", Value::Int(5))])], 1000);
            // Still active → still in the ring, not yet written.
            assert!(store.get(1).is_some());
            store.ingest(&[&completed(1)], 1100);
            // Finished → gone from the ring (tail = 0).
            assert!(store.get(1).is_none());
            assert_eq!(store.list(10).len(), 0);
            // Dropping the store flushes and joins the writer.
        }
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "exactly one NDJSON line for the instance");
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["instanceKey"], "1");
        assert_eq!(parsed["outcome"], "completed");
        // Same shape as GET /console/api/traces/{key}: capture fields present.
        assert_eq!(parsed["creationVariables"]["values"]["amount"], 5);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn terminated_instance_is_also_sunk() {
        let path = temp_trace_path("terminated");
        let _ = std::fs::remove_file(&path);
        {
            let store = TraceStore::with_sink(8, sink_cfg(path.clone()));
            store.ingest(&[&created(7, &[])], 1000);
            store.ingest(&[&terminated(7)], 1200);
            assert!(store.get(7).is_none());
        }
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["outcome"], "terminated");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn terminating_then_terminated_sinks_exactly_once() {
        // The deferred-cancellation path (cancel with listeners) emits
        // `ProcessInstanceTerminating` first, then `ProcessInstanceTerminated`
        // once termination completes. The intermediate event must not sink the
        // instance early, and the final one must sink it exactly once.
        let path = temp_trace_path("terminating-terminated");
        let _ = std::fs::remove_file(&path);
        {
            let store = TraceStore::with_sink(8, sink_cfg(path.clone()));
            store.ingest(&[&created(9, &[])], 1000);
            store.ingest(&[&terminating(9)], 1100);
            // Intermediate state: still active, still in the ring, not yet sunk.
            assert!(store.get(9).is_some(), "terminating keeps the instance");
            store.ingest(&[&terminated(9)], 1200);
            assert!(store.get(9).is_none(), "terminated drops the instance");
        }
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(
            lines.len(),
            1,
            "Terminating then Terminated must produce exactly one NDJSON line"
        );
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["outcome"], "terminated");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tail_keeps_recent_finished_in_ring() {
        let path = temp_trace_path("tail");
        let _ = std::fs::remove_file(&path);
        let mut cfg = sink_cfg(path.clone());
        cfg.tail = 1;
        {
            let store = TraceStore::with_sink(8, cfg);
            store.ingest(&[&created(1, &[])], 1000);
            store.ingest(&[&completed(1)], 1100);
            // Tail = 1 keeps the most-recent finished instance visible.
            assert!(store.get(1).is_some());
            store.ingest(&[&created(2, &[])], 1200);
            store.ingest(&[&completed(2)], 1300);
            // The newer finished instance evicts the older from the tail.
            assert!(store.get(2).is_some());
            assert!(store.get(1).is_none());
        }
        // Both were still written to the file exactly once.
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn active_instances_stay_in_memory_with_sink_on() {
        let path = temp_trace_path("active");
        let _ = std::fs::remove_file(&path);
        {
            let store = TraceStore::with_sink(8, sink_cfg(path.clone()));
            // Two active instances, one finished: only the finished one is sunk.
            store.ingest(&[&created(1, &[])], 1000);
            store.ingest(&[&created(2, &[])], 1010);
            store.ingest(&[&completed(1)], 1100);
            assert!(store.get(1).is_none(), "finished instance dropped");
            assert!(store.get(2).is_some(), "active instance retained");
            assert_eq!(store.list(10).len(), 1);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rotation_keeps_bounded_files() {
        let path = temp_trace_path("rotate");
        for i in 0..6 {
            let _ = std::fs::remove_file(format!("{}.{i}", path.display()));
        }
        let _ = std::fs::remove_file(&path);
        {
            let mut cfg = sink_cfg(path.clone());
            cfg.max_bytes = Some(1); // rotate on every subsequent write
            cfg.keep = 2;
            let store = TraceStore::with_sink(8, cfg);
            for k in 1..=4u64 {
                store.ingest(&[&created(k, &[])], 1000 + k);
                store.ingest(&[&completed(k)], 1050 + k);
            }
        }
        // Keep window is honoured: at most `keep` rotated files survive.
        assert!(std::fs::metadata(format!("{}.1", path.display())).is_ok());
        assert!(std::fs::metadata(format!("{}.2", path.display())).is_ok());
        assert!(
            std::fs::metadata(format!("{}.3", path.display())).is_err(),
            "rotated files beyond keep are removed"
        );
        for i in 0..6 {
            let _ = std::fs::remove_file(format!("{}.{i}", path.display()));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn queue_is_bounded_by_bytes_not_trace_count() {
        // Regression for the memory-bound finding: a count-only queue lets a
        // stalled disk retain ~queue × per-trace-payload bytes (≈64 GiB at the
        // documented defaults). The budget is in *bytes*, so a tiny budget drops
        // traces once their serialized size exceeds it — regardless of how few
        // traces that is.
        let path = temp_trace_path("byte-bound");
        let _ = std::fs::remove_file(&path);
        let mut cfg = sink_cfg(path.clone());
        // A budget smaller than a single serialized trace: every append drops.
        cfg.queue_bytes = 1;
        cfg.flush_interval = std::time::Duration::from_secs(3600); // never flush mid-test
        {
            let store = TraceStore::with_sink(8, cfg);
            store.ingest(
                &[&created(1, &[("payload", Value::Str("x".repeat(256)))])],
                1000,
            );
            store.ingest(&[&completed(1)], 1100);
            // Give the writer a beat to (not) consume; the line far exceeds the
            // 1-byte budget so it must be dropped, never written.
            std::thread::sleep(std::time::Duration::from_millis(100));
            let (_written, dropped, _errors) = store.sink_stats().unwrap();
            assert_eq!(dropped, 1, "oversized-vs-budget trace is dropped");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn written_counts_only_after_a_successful_flush() {
        // Regression for the durability-counter finding: `written` must not
        // count a trace merely accepted into the BufWriter. With a long flush
        // interval, a trace sits buffered and `written` stays 0 until the flush
        // (here, the shutdown flush on drop) publishes it.
        let path = temp_trace_path("written-after-flush");
        let _ = std::fs::remove_file(&path);
        let mut cfg = sink_cfg(path.clone());
        cfg.flush_interval = std::time::Duration::from_secs(3600); // no periodic flush
        {
            let store = TraceStore::with_sink(8, cfg);
            store.ingest(&[&created(1, &[])], 1000);
            store.ingest(&[&completed(1)], 1100);
            // Let the writer receive (but not flush) the line.
            std::thread::sleep(std::time::Duration::from_millis(150));
            let (written, _dropped, _errors) = store.sink_stats().unwrap();
            assert_eq!(
                written, 0,
                "buffered-but-unflushed trace is not yet counted as written"
            );
            // Dropping the store flushes + joins: now it is written.
        }
        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body.lines().filter(|l| !l.is_empty()).count(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn flush_fires_on_absolute_deadline_despite_continuous_arrivals() {
        // Regression for the flush-starvation finding: a relative recv_timeout
        // restarts on every arrival, so a queue receiving at least one trace per
        // interval never flushes. The absolute deadline must flush even when
        // arrivals are more frequent than the interval.
        let path = temp_trace_path("absolute-deadline");
        let _ = std::fs::remove_file(&path);
        let mut cfg = sink_cfg(path.clone());
        cfg.flush_interval = std::time::Duration::from_millis(80);
        {
            let store = TraceStore::with_sink(64, cfg);
            // Send traces faster than the flush interval for several intervals.
            for k in 1..=40u64 {
                store.ingest(&[&created(k, &[])], 1000 + k);
                store.ingest(&[&completed(k)], 1050 + k);
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            // Continuous arrivals at 10 ms ≪ 80 ms interval. With a relative
            // timeout nothing would have flushed yet; with the absolute deadline
            // at least one flush has fired, so `written` is already non-zero
            // before shutdown.
            let (written, _dropped, _errors) = store.sink_stats().unwrap();
            assert!(
                written > 0,
                "absolute deadline flushes even under continuous arrivals (written={written})"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rotation_is_prospective_not_just_at_cap() {
        // Regression for the rotation off-by-one: a file just below the cap must
        // rotate *before* accepting a trace that would push it over, so a
        // segment never exceeds max_bytes by a whole trace. Use a cap larger
        // than one line but smaller than two.
        let path = temp_trace_path("prospective");
        for i in 0..4 {
            let _ = std::fs::remove_file(format!("{}.{i}", path.display()));
        }
        let _ = std::fs::remove_file(&path);
        // First, measure one line's size with no rotation.
        let line_len = {
            let p = temp_trace_path("prospective-measure");
            let _ = std::fs::remove_file(&p);
            {
                let store = TraceStore::with_sink(8, sink_cfg(p.clone()));
                store.ingest(&[&created(1, &[])], 1000);
                store.ingest(&[&completed(1)], 1100);
            }
            let n = std::fs::metadata(&p).unwrap().len();
            let _ = std::fs::remove_file(&p);
            n
        };
        {
            let mut cfg = sink_cfg(path.clone());
            // Cap fits ~1.5 lines: the second line must trigger rotation *before*
            // being appended, so segment 1 holds exactly one line.
            cfg.max_bytes = Some(line_len + line_len / 2);
            cfg.keep = 3;
            let store = TraceStore::with_sink(8, cfg);
            for k in 1..=3u64 {
                store.ingest(&[&created(k, &[])], 1000 + k);
                store.ingest(&[&completed(k)], 1050 + k);
            }
        }
        // Rotation happened (a .1 segment exists) ...
        assert!(
            std::fs::metadata(format!("{}.1", path.display())).is_ok(),
            "prospective rotation produced a rotated segment"
        );
        // ... and no single segment exceeds the cap by a whole extra line: the
        // rotated segment holds one line, not two.
        let seg1 = std::fs::metadata(format!("{}.1", path.display()))
            .unwrap()
            .len();
        assert!(
            seg1 <= line_len + 1,
            "rotated segment holds one line ({seg1} <= {}), not an over-cap append",
            line_len + 1
        );
        for i in 0..4 {
            let _ = std::fs::remove_file(format!("{}.{i}", path.display()));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn oversized_single_line_goes_to_fresh_segment() {
        // The prospective check must allow a single line larger than the cap
        // into an empty segment (size == 0), otherwise it would spin on
        // rotation forever and never write.
        let path = temp_trace_path("oversized-line");
        for i in 0..4 {
            let _ = std::fs::remove_file(format!("{}.{i}", path.display()));
        }
        let _ = std::fs::remove_file(&path);
        {
            let mut cfg = sink_cfg(path.clone());
            cfg.max_bytes = Some(8); // far smaller than any real trace line
            cfg.keep = 2;
            let store = TraceStore::with_sink(8, cfg);
            store.ingest(&[&created(1, &[])], 1000);
            store.ingest(&[&completed(1)], 1100);
        }
        // The oversized line was still written exactly once (to a fresh segment).
        let mut total = 0;
        for cand in [
            path.clone(),
            std::path::PathBuf::from(format!("{}.1", path.display())),
            std::path::PathBuf::from(format!("{}.2", path.display())),
        ] {
            if let Ok(body) = std::fs::read_to_string(&cand) {
                total += body.lines().filter(|l| !l.is_empty()).count();
            }
        }
        assert_eq!(
            total, 1,
            "the single oversized line is written exactly once"
        );
        for i in 0..4 {
            let _ = std::fs::remove_file(format!("{}.{i}", path.display()));
        }
        let _ = std::fs::remove_file(&path);
    }
}
