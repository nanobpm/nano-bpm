//! **Camunda 8 export → Nano trace transformer.**
//!
//! Camunda/Zeebe brokers stream every state change as an ordered log of
//! `Record<?>` documents (position, key, timestamp, valueType, intent, value).
//! Those records are what the Elasticsearch / Opensearch / debug-log exporters
//! persist as JSON. This module folds that record stream into the per-instance
//! [`crate::contracts::InstanceTrace`] shape ProcessOS already understands, so a
//! customer's *existing* Camunda 8 history can be loaded as a [`DatasetSource`]
//! with no Nano engine running.
//!
//! It is the offline counterpart to the in-engine fold in the Nano gateway
//! (`server/src/console/trace.rs`): the same projection, run over Camunda's
//! records instead of Nano's `Event`s. The intent vocabularies map ~1:1:
//!
//! | Camunda record (valueType / intent)        | Nano event / trace field          |
//! |--------------------------------------------|-----------------------------------|
//! | `PROCESS_INSTANCE` ELEMENT_ACTIVATING/…    | instance start / element timing   |
//! | `PROCESS_INSTANCE` ELEMENT_COMPLETED(proc) | `outcome = completed`             |
//! | `PROCESS_INSTANCE` ELEMENT_TERMINATED(proc)| `outcome = terminated`            |
//! | `JOB` CREATED / COMPLETED / FAILED         | `element.job` queue/service/fail   |
//! | `JOB_BATCH` ACTIVATED                       | job activation (queue/service split)|
//! | `INCIDENT` CREATED                          | `incidents[]`                     |
//! | `PROCESS_INSTANCE_CREATION` CREATED         | `creationVariables` (Tier-1)      |
//! | `JOB` COMPLETED (+variables)                | `stimuli[] jobCompleted` (Tier-2) |
//! | `VARIABLE` CREATED/UPDATED (`scopeKey`)     | ignored (see below)               |
//!
//! Standalone `VARIABLE` records — Camunda's authoritative per-scope variable
//! state, carrying a `scopeKey` distinct from `processInstanceKey` under the
//! hierarchical scope model — are **intentionally ignored**. A recorded-input
//! replay re-derives every scope from the model topology and re-applies the
//! creation (Tier-1) and job-completion (Tier-2) payloads, so folding scoped
//! `VARIABLE` snapshots in as well would double-count them and leak the source
//! engine's scope keys into a scope-agnostic trace.
//!
//! ## Input formats accepted
//! `load_records` is tolerant of the common Camunda dump layouts:
//! - **NDJSON** — one record JSON object per line (debug-log / file exports).
//! - **JSON array** — `[ {record}, … ]`.
//! - **Elasticsearch search response** — `{ "hits": { "hits": [ { "_source": {record} } ] } }`
//!   (and bare `{ "_source": {record} }` lines), so a raw scroll/`_search` dump works.
//! - a **directory** of any of the above (every `*.json` / `*.ndjson` file folded together).
//!
//! ## Fidelity tiers
//! - **Tier-0** (always): instance + per-element durations + jobs + incidents.
//! - **Tier-1** (when present): `creationVariables` from `PROCESS_INSTANCE_CREATION`.
//! - **Tier-2** (when `tier2`): an ordered `jobCompleted` stimulus log, keyed by job
//!   *type* with the completion variables, so the trace is recorded-input replayable.
//!   Other external inputs (messages, timers, user tasks) are **not** captured, so a
//!   model that consumes them is only partially replayable — see `stimuli_truncated`.

use std::collections::HashMap;
use std::path::Path;

use serde::Serialize;
use serde_json::Value as Json;

// ---------------------------------------------------------------------------
// Raw Camunda record (the subset of `Record#toJson()` we read). Field names are
// camelCase, matching the exporter JSON. Unknown fields are ignored; `value`
// stays a generic object and typed fields are pulled lazily per valueType.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawRecord {
    #[serde(default)]
    position: i64,
    #[serde(default)]
    key: i64,
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    value_type: String,
    #[serde(default)]
    intent: String,
    #[serde(default)]
    record_type: String,
    #[serde(default)]
    value: Json,
}

// ---------------------------------------------------------------------------
// Emission shapes — byte-compatible with contracts::InstanceTrace (camelCase).
// Defined locally because contracts:: types are Deserialize-only (same pattern
// as corpus.rs).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceOut {
    instance_key: String,
    process_id: String,
    version: i32,
    outcome: String,
    started_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    elements: Vec<ElementOut>,
    incidents: Vec<IncidentOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    creation_variables: Option<VariablesOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stimuli: Option<Vec<StimulusOut>>,
    stimuli_truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct VariablesOut {
    truncated: bool,
    bytes: usize,
    values: Json,
}

impl VariablesOut {
    fn of(values: Json) -> Self {
        let bytes = serde_json::to_vec(&values).map(|v| v.len()).unwrap_or(0);
        Self {
            truncated: false,
            bytes,
            values,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StimulusOut {
    seq: u32,
    at: u64,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<VariablesOut>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElementOut {
    element_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    incidents: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<JobOut>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobOut {
    #[serde(rename = "type")]
    job_type: String,
    /// `created → activated`. `None` when no `JOB_BATCH ACTIVATED` record pinned
    /// the activation instant (then `service_ms` carries the whole wait).
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_ms: Option<u64>,
    /// `activated → completed`, or `created → completed` when activation is unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    service_ms: Option<u64>,
    failures: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct IncidentOut {
    element_id: String,
    kind: String,
    reason: String,
}

/// What an import produced — printed as JSON by the CLI.
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ImportSummary {
    pub records_read: usize,
    pub traces: usize,
    pub completed: usize,
    pub terminated: usize,
    pub active: usize,
    pub with_incidents: usize,
    pub with_creation_variables: usize,
    pub with_stimuli: usize,
    /// Instances whose recorded-input log is known to be incomplete (an external
    /// input was consumed that Tier-2 could not capture/attribute) — replayable
    /// only partially (the `stimuliTruncated` flag is set on the trace).
    pub partially_replayable: usize,
    pub processes: Vec<String>,
    pub out_dir: String,
}

// ---------------------------------------------------------------------------
// Per-instance fold accumulator.
// ---------------------------------------------------------------------------

struct ElemAcc {
    element_id: String,
    bpmn_type: String,
    first_seen: u64,
    activated_at: Option<u64>,
    completed_at: Option<u64>,
    incidents: u32,
    job_key: Option<i64>,
}

struct JobAcc {
    job_type: String,
    element_instance_key: i64,
    created_at: Option<u64>,
    activated_at: Option<u64>,
    completed_at: Option<u64>,
    failures: u32,
}

#[derive(Default)]
struct InstAcc {
    process_id: String,
    version: i32,
    started_at: Option<u64>,
    ended_at: Option<u64>,
    outcome: Option<String>,
    /// element-instance-key → element accumulator (insertion order preserved).
    elem_order: Vec<i64>,
    elems: HashMap<i64, ElemAcc>,
    jobs: HashMap<i64, JobAcc>,
    incidents: Vec<IncidentOut>,
    creation_vars: Option<Json>,
    stimuli: Vec<StimulusOut>,
    stim_seq: u32,
    /// Set when an external input was consumed that we could not turn into a
    /// captured stimulus — the log is then incomplete (partial replay).
    stimuli_truncated: bool,
}

impl InstAcc {
    fn elem_mut(&mut self, key: i64, element_id: &str, bpmn_type: &str, ts: u64) -> &mut ElemAcc {
        use std::collections::hash_map::Entry;
        match self.elems.entry(key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                self.elem_order.push(key);
                e.insert(ElemAcc {
                    element_id: element_id.to_string(),
                    bpmn_type: bpmn_type.to_string(),
                    first_seen: ts,
                    activated_at: None,
                    completed_at: None,
                    incidents: 0,
                    job_key: None,
                })
            }
        }
    }

    /// Append one recorded external input to the Tier-2 log, assigning the next
    /// sequence number. `reference` is the job type (jobs) or the catch element
    /// id (messages/timers/signals); `variables` is the input's payload delta.
    fn push_stimulus(
        &mut self,
        at: u64,
        kind: &str,
        reference: Option<String>,
        variables: Option<Json>,
    ) {
        // A message/timer/signal input with no catch element cannot be routed to
        // a token during replay, so the recorded log is incomplete (partial).
        // (`userTaskCompleted` legitimately has no reference — replay completes
        // the single waiting user task — and `jobCompleted` is keyed by type.)
        if reference.is_none() && matches!(kind, "message" | "timer" | "signal") {
            self.stimuli_truncated = true;
        }
        self.stim_seq += 1;
        let seq = self.stim_seq;
        self.stimuli.push(StimulusOut {
            seq,
            at,
            kind: kind.to_string(),
            reference,
            variables: variables.map(VariablesOut::of),
        });
    }
}

// ---------------------------------------------------------------------------
// Value-field helpers (camelCase keys on the record's `value` object).
// ---------------------------------------------------------------------------

fn vi64(v: &Json, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}
fn vstr<'a>(v: &'a Json, key: &str) -> Option<&'a str> {
    v.get(key).and_then(|x| x.as_str())
}

/// A non-empty `value.variables` object, if present.
fn variables_of(v: &Json) -> Option<Json> {
    match v.get("variables") {
        Some(Json::Object(m)) if !m.is_empty() => Some(Json::Object(m.clone())),
        _ => None,
    }
}

fn is_process_element(bpmn_type: &str) -> bool {
    bpmn_type.eq_ignore_ascii_case("PROCESS") || bpmn_type == "process"
}

/// Element types that carry no useful trace timing of their own (routing only).
fn is_routing_element(bpmn_type: &str) -> bool {
    matches!(
        bpmn_type.to_ascii_uppercase().as_str(),
        "SEQUENCE_FLOW" | "UNSPECIFIED" | ""
    )
}

// ---------------------------------------------------------------------------
// The fold.
// ---------------------------------------------------------------------------

/// Fold a record stream into Nano traces. `tier2` enables the `jobCompleted`
/// stimulus log. Records are processed in `(timestamp, position)` order so a
/// JOB CREATED is always folded before its JOB_BATCH ACTIVATED / JOB COMPLETED,
/// regardless of input ordering.
pub fn transform(records: &[RawRecord], tier2: bool) -> Vec<TraceOut> {
    /// Get or create the accumulator for `pik`, recording first-seen order.
    /// Returns the live `&mut InstAcc`, so callers never need a follow-up
    /// `get_mut().unwrap()` — the key is present by construction.
    fn touch<'a>(
        insts: &'a mut HashMap<i64, InstAcc>,
        inst_order: &mut Vec<i64>,
        pik: i64,
    ) -> &'a mut InstAcc {
        insts.entry(pik).or_insert_with(|| {
            inst_order.push(pik);
            InstAcc::default()
        })
    }

    let mut order: Vec<usize> = (0..records.len()).collect();
    order.sort_by_key(|&i| (records[i].timestamp, records[i].position));

    let mut insts: HashMap<i64, InstAcc> = HashMap::new();
    let mut inst_order: Vec<i64> = Vec::new();
    // jobKey → processInstanceKey, so a JOB_BATCH ACTIVATED (which spans
    // instances and carries no processInstanceKey) can route activation back.
    let mut job_to_inst: HashMap<i64, i64> = HashMap::new();
    // elementInstanceKey → processInstanceKey, so a SIGNAL_SUBSCRIPTION
    // correlation (which is keyed by its catch-event instance, not the process
    // instance) can route back to the instance that consumed the signal.
    let mut eik_to_inst: HashMap<i64, i64> = HashMap::new();

    for &i in &order {
        let r = &records[i];
        // Only folded settled facts (EVENTs); commands/rejections are intent, not outcome.
        if !r.record_type.is_empty() && r.record_type != "EVENT" {
            continue;
        }
        let ts = r.timestamp.max(0) as u64;
        let vt = r.value_type.as_str();
        let intent = r.intent.as_str();
        let v = &r.value;

        match vt {
            "PROCESS_INSTANCE" => {
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                // Map this element instance to its process instance (the record
                // key is the element-instance key) for later signal routing.
                if r.key != 0 {
                    eik_to_inst.insert(r.key, pik);
                }
                let inst = touch(&mut insts, &mut inst_order, pik);
                let element_id = vstr(v, "elementId").unwrap_or("").to_string();
                let bpmn_type = vstr(v, "bpmnElementType").unwrap_or("").to_string();
                if let Some(pid) = vstr(v, "bpmnProcessId") {
                    if inst.process_id.is_empty() {
                        inst.process_id = pid.to_string();
                    }
                }
                if let Some(ver) = vi64(v, "version") {
                    if inst.version == 0 {
                        inst.version = ver as i32;
                    }
                }

                let is_proc = is_process_element(&bpmn_type) || pik == r.key;
                if is_proc {
                    // Process-scope lifecycle drives instance start/outcome.
                    match intent {
                        "ELEMENT_ACTIVATING" | "ELEMENT_ACTIVATED" => {
                            inst.started_at.get_or_insert(ts);
                        }
                        "ELEMENT_COMPLETED" => {
                            inst.ended_at = Some(ts);
                            inst.outcome = Some("completed".into());
                        }
                        "ELEMENT_TERMINATED" => {
                            inst.ended_at = Some(ts);
                            inst.outcome = Some("terminated".into());
                        }
                        _ => {}
                    }
                    continue;
                }

                if is_routing_element(&bpmn_type) {
                    continue;
                }
                // A normal flow element (task, gateway, event): time it by its
                // element-instance key (the record key).
                let elem = inst.elem_mut(r.key, &element_id, &bpmn_type, ts);
                match intent {
                    "ELEMENT_ACTIVATING" | "ELEMENT_ACTIVATED" => {
                        elem.activated_at.get_or_insert(ts);
                    }
                    "ELEMENT_COMPLETED" | "ELEMENT_TERMINATED" => {
                        elem.completed_at = Some(ts);
                    }
                    _ => {}
                }
            }

            "PROCESS_INSTANCE_CREATION" => {
                if intent != "CREATED" {
                    continue;
                }
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                let inst = touch(&mut insts, &mut inst_order, pik);
                if let Some(pid) = vstr(v, "bpmnProcessId") {
                    if inst.process_id.is_empty() {
                        inst.process_id = pid.to_string();
                    }
                }
                if inst.creation_vars.is_none() {
                    if let Some(vars) = variables_of(v) {
                        inst.creation_vars = Some(vars);
                    }
                }
            }

            "JOB" => {
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                let job_key = r.key;
                let job_type = vstr(v, "type").unwrap_or("").to_string();
                let eik = vi64(v, "elementInstanceKey").unwrap_or(0);
                job_to_inst.insert(job_key, pik);
                let inst = touch(&mut insts, &mut inst_order, pik);
                let job = inst.jobs.entry(job_key).or_insert_with(|| JobAcc {
                    job_type: job_type.clone(),
                    element_instance_key: eik,
                    created_at: None,
                    activated_at: None,
                    completed_at: None,
                    failures: 0,
                });
                if job.job_type.is_empty() {
                    job.job_type = job_type.clone();
                }
                if job.element_instance_key == 0 && eik != 0 {
                    job.element_instance_key = eik;
                }
                // Link the owning element to its job.
                if eik != 0 {
                    if let Some(el) = inst.elems.get_mut(&eik) {
                        el.job_key = Some(job_key);
                    }
                }
                // `job_key` was just inserted above, so the lookups below hit by
                // construction; `if let` keeps the access total regardless.
                match intent {
                    "CREATED" => {
                        if let Some(job) = inst.jobs.get_mut(&job_key) {
                            job.created_at.get_or_insert(ts);
                        }
                    }
                    "COMPLETED" => {
                        if let Some(job) = inst.jobs.get_mut(&job_key) {
                            job.completed_at = Some(ts);
                        }
                        if tier2 {
                            let vars = variables_of(v);
                            inst.push_stimulus(ts, "jobCompleted", Some(job_type.clone()), vars);
                        }
                    }
                    "FAILED" | "ERROR_THROWN" => {
                        if let Some(job) = inst.jobs.get_mut(&job_key) {
                            job.failures += 1;
                        }
                    }
                    _ => {}
                }
            }

            "JOB_BATCH" => {
                if intent != "ACTIVATED" {
                    continue;
                }
                // Pin the activation instant for every job in the batch so the
                // owning instance can split queue (created→activated) from
                // service (activated→completed).
                if let Some(Json::Array(keys)) = v.get("jobKeys") {
                    for k in keys {
                        let Some(jk) = k.as_i64() else { continue };
                        if let Some(&pik) = job_to_inst.get(&jk) {
                            if let Some(inst) = insts.get_mut(&pik) {
                                if let Some(job) = inst.jobs.get_mut(&jk) {
                                    job.activated_at.get_or_insert(ts);
                                }
                            }
                        }
                    }
                }
            }

            "INCIDENT" => {
                if intent != "CREATED" {
                    continue;
                }
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                let inst = touch(&mut insts, &mut inst_order, pik);
                let element_id = vstr(v, "elementId").unwrap_or("").to_string();
                inst.incidents.push(IncidentOut {
                    element_id: element_id.clone(),
                    kind: vstr(v, "errorType").unwrap_or("UNKNOWN").to_string(),
                    reason: vstr(v, "errorMessage").unwrap_or("").to_string(),
                });
                let eik = vi64(v, "elementInstanceKey").unwrap_or(0);
                if let Some(el) = inst.elems.get_mut(&eik) {
                    el.incidents += 1;
                }
            }

            // --- Tier-2 external inputs (message / user-task / timer / signal) ---
            // A native (Zeebe) user task completing is an external input carrying
            // its output variables — the analog of a job completion.
            "USER_TASK" if tier2 && intent == "COMPLETED" => {
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                let element_id = vstr(v, "elementId").map(|s| s.to_string());
                let vars = variables_of(v);
                touch(&mut insts, &mut inst_order, pik).push_stimulus(
                    ts,
                    "userTaskCompleted",
                    element_id,
                    vars,
                );
            }

            // A message correlated to one of the instance's open subscriptions
            // (intermediate catch / receive task / boundary). The per-instance
            // `PROCESS_MESSAGE_SUBSCRIPTION` carries the element and payload.
            "PROCESS_MESSAGE_SUBSCRIPTION" if tier2 && intent == "CORRELATED" => {
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                let element_id = vstr(v, "elementId").map(|s| s.to_string());
                let vars = variables_of(v);
                touch(&mut insts, &mut inst_order, pik)
                    .push_stimulus(ts, "message", element_id, vars);
            }

            // A timer firing carries no payload, but its occurrence and *real*
            // fire time are part of the recorded input ordering — so replay can
            // honour history's latency rather than the model's nominal due time.
            "TIMER" if tier2 && intent == "TRIGGERED" => {
                let Some(pik) = vi64(v, "processInstanceKey") else {
                    continue;
                };
                let element_id = vstr(v, "targetElementId")
                    .or_else(|| vstr(v, "elementId"))
                    .map(|s| s.to_string());
                touch(&mut insts, &mut inst_order, pik)
                    .push_stimulus(ts, "timer", element_id, None);
            }

            // A broadcast signal correlated to one of the instance's signal catch
            // events. The subscription record is keyed by its catch-event
            // instance, so route back to the owning process instance.
            "SIGNAL_SUBSCRIPTION" if tier2 && intent == "CORRELATED" => {
                let pik = vi64(v, "processInstanceKey").or_else(|| {
                    vi64(v, "catchEventInstanceKey")
                        .or_else(|| vi64(v, "elementInstanceKey"))
                        .and_then(|eik| eik_to_inst.get(&eik).copied())
                });
                let Some(pik) = pik else {
                    continue;
                };
                let element_id = vstr(v, "catchEventId")
                    .or_else(|| vstr(v, "elementId"))
                    .map(|s| s.to_string());
                let vars = variables_of(v);
                touch(&mut insts, &mut inst_order, pik)
                    .push_stimulus(ts, "signal", element_id, vars);
            }

            _ => {}
        }
    }

    // Emit in first-seen instance order for stable output.
    let mut out: Vec<TraceOut> = Vec::with_capacity(inst_order.len());
    for pik in inst_order {
        let Some(inst) = insts.remove(&pik) else {
            continue;
        };
        out.push(finish(pik, inst, tier2));
    }
    out
}

fn finish(pik: i64, mut inst: InstAcc, tier2: bool) -> TraceOut {
    let started_at = inst.started_at.unwrap_or(0);
    let duration_ms = match (inst.started_at, inst.ended_at) {
        (Some(s), Some(e)) if e >= s => Some(e - s),
        _ => None,
    };
    let outcome = inst.outcome.clone().unwrap_or_else(|| "active".into());

    // Build elements in execution order (by activation, then first-seen).
    let mut keys = inst.elem_order.clone();
    keys.sort_by_key(|k| {
        let el = &inst.elems[k];
        (el.activated_at.unwrap_or(el.first_seen), el.first_seen)
    });

    let mut elements: Vec<ElementOut> = Vec::new();
    for k in keys {
        let Some(el) = inst.elems.remove(&k) else {
            continue;
        };
        if is_process_element(&el.bpmn_type) || is_routing_element(&el.bpmn_type) {
            continue;
        }
        let el_duration = match (el.activated_at, el.completed_at) {
            (Some(a), Some(c)) if c >= a => Some(c - a),
            _ => None,
        };
        let job = el.job_key.and_then(|jk| inst.jobs.get(&jk)).map(|j| {
            let (queue_ms, service_ms) = job_timing(j);
            JobOut {
                job_type: j.job_type.clone(),
                queue_ms,
                service_ms,
                failures: j.failures,
            }
        });
        // Skip pure-routing noise that carried no timing, job, or incident.
        if el_duration.is_none() && job.is_none() && el.incidents == 0 {
            continue;
        }
        elements.push(ElementOut {
            element_id: el.element_id,
            duration_ms: el_duration,
            incidents: el.incidents,
            job,
        });
    }

    let creation_variables = inst.creation_vars.take().map(VariablesOut::of);
    let stimuli = if tier2 && !inst.stimuli.is_empty() {
        Some(std::mem::take(&mut inst.stimuli))
    } else {
        None
    };

    TraceOut {
        instance_key: pik.to_string(),
        process_id: inst.process_id,
        version: inst.version,
        outcome,
        started_at,
        duration_ms,
        elements,
        incidents: inst.incidents,
        creation_variables,
        // Honest partial-replay flag: set when an external input was consumed
        // that we could not turn into a stimulus (so the log is incomplete).
        stimuli_truncated: tier2 && inst.stimuli_truncated,
        stimuli,
    }
}

/// Split a job's lifetime into queue (created→activated) and service
/// (activated→completed). When no `JOB_BATCH ACTIVATED` pinned the activation,
/// the whole `created→completed` wait is reported as service.
fn job_timing(j: &JobAcc) -> (Option<u64>, Option<u64>) {
    match (j.created_at, j.activated_at, j.completed_at) {
        (Some(c), Some(a), Some(done)) if a >= c && done >= a => (Some(a - c), Some(done - a)),
        (Some(c), _, Some(done)) if done >= c => (None, Some(done - c)),
        _ => (None, None),
    }
}

// ---------------------------------------------------------------------------
// Input loading.
// ---------------------------------------------------------------------------

/// Load Camunda records from a file or directory. See the module docs for the
/// accepted layouts (NDJSON, JSON array, ES search response, `_source` wrappers).
pub fn load_records(input: &Path) -> Result<Vec<RawRecord>, String> {
    let mut out: Vec<RawRecord> = Vec::new();
    if input.is_dir() {
        let mut files: Vec<_> = std::fs::read_dir(input)
            .map_err(|e| format!("read dir {}: {e}", input.display()))?
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.is_file()
                    && p.extension()
                        .and_then(|x| x.to_str())
                        .map(|x| matches!(x, "json" | "ndjson" | "jsonl" | "log"))
                        .unwrap_or(false)
            })
            .collect();
        files.sort();
        if files.is_empty() {
            return Err(format!(
                "no .json/.ndjson/.jsonl/.log files under {}",
                input.display()
            ));
        }
        for f in files {
            let bytes = std::fs::read(&f).map_err(|e| format!("read {}: {e}", f.display()))?;
            parse_blob(&bytes, &mut out);
        }
    } else {
        let bytes = std::fs::read(input).map_err(|e| format!("read {}: {e}", input.display()))?;
        parse_blob(&bytes, &mut out);
    }
    if out.is_empty() {
        return Err(format!(
            "no parseable Camunda records in {}",
            input.display()
        ));
    }
    Ok(out)
}

/// Parse one file's bytes, appending every record found. Tolerant: tries a whole
/// JSON document first (array / ES response / single object), then falls back to
/// line-delimited JSON.
fn parse_blob(bytes: &[u8], out: &mut Vec<RawRecord>) {
    if let Ok(doc) = serde_json::from_slice::<Json>(bytes) {
        collect_from_value(doc, out);
        return;
    }
    // NDJSON: one JSON value per non-blank line.
    for line in bytes.split(|&b| b == b'\n') {
        let s = line;
        if s.iter().all(|&b| b.is_ascii_whitespace()) {
            continue;
        }
        if let Ok(v) = serde_json::from_slice::<Json>(s) {
            collect_from_value(v, out);
        }
    }
}

/// Pull record object(s) out of an arbitrary JSON value, unwrapping the common
/// Elasticsearch envelopes (`hits.hits[]._source`, bare `_source`).
fn collect_from_value(v: Json, out: &mut Vec<RawRecord>) {
    match v {
        Json::Array(arr) => {
            for item in arr {
                collect_from_value(item, out);
            }
        }
        Json::Object(ref m) => {
            // ES search response.
            if let Some(Json::Array(arr)) = m.get("hits").and_then(|h| h.get("hits")) {
                for h in arr.clone() {
                    collect_from_value(h, out);
                }
                return;
            }
            // ES document wrapper.
            if let Some(src) = m.get("_source") {
                collect_from_value(src.clone(), out);
                return;
            }
            // A bare record: it must look like one (has valueType/intent).
            if m.contains_key("valueType") || m.contains_key("intent") {
                if let Ok(r) = serde_json::from_value::<RawRecord>(v) {
                    out.push(r);
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Top-level entry: load → transform → write a DatasetSource folder.
// ---------------------------------------------------------------------------

/// Transform a Camunda export at `input` into a `traces.json` dataset under
/// `out_dir` (creating it if needed). The folder is directly loadable by
/// [`crate::dataset::DatasetSource`].
pub fn import(input: &Path, out_dir: &Path, tier2: bool) -> Result<ImportSummary, String> {
    let records = load_records(input)?;
    let traces = transform(&records, tier2);
    if traces.is_empty() {
        return Err("no process instances reconstructed from the records".into());
    }

    std::fs::create_dir_all(out_dir).map_err(|e| format!("create {}: {e}", out_dir.display()))?;
    let traces_file = out_dir.join("traces.json");
    let bytes = serde_json::to_vec_pretty(&traces).map_err(|e| format!("serialize traces: {e}"))?;
    std::fs::write(&traces_file, &bytes)
        .map_err(|e| format!("write {}: {e}", traces_file.display()))?;

    let mut processes: Vec<String> = traces.iter().map(|t| t.process_id.clone()).collect();
    processes.sort();
    processes.dedup();

    Ok(ImportSummary {
        records_read: records.len(),
        traces: traces.len(),
        completed: traces.iter().filter(|t| t.outcome == "completed").count(),
        terminated: traces.iter().filter(|t| t.outcome == "terminated").count(),
        active: traces.iter().filter(|t| t.outcome == "active").count(),
        with_incidents: traces.iter().filter(|t| !t.incidents.is_empty()).count(),
        with_creation_variables: traces
            .iter()
            .filter(|t| t.creation_variables.is_some())
            .count(),
        with_stimuli: traces.iter().filter(|t| t.stimuli.is_some()).count(),
        partially_replayable: traces.iter().filter(|t| t.stimuli_truncated).count(),
        processes,
        out_dir: out_dir.display().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn rec(value_type: &str, intent: &str, key: i64, ts: i64, pos: i64, value: Json) -> RawRecord {
        RawRecord {
            position: pos,
            key,
            timestamp: ts,
            value_type: value_type.into(),
            intent: intent.into(),
            record_type: "EVENT".into(),
            value,
        }
    }

    // A complete loan-approval-ish instance: create (with vars) → process
    // activate → service task activate → job create/activate/complete → task
    // complete → process complete.
    fn happy_instance() -> Vec<RawRecord> {
        let pik = 1001;
        let eik = 2001; // service-task element instance
        let jk = 3001; // job key
        vec![
            rec(
                "PROCESS_INSTANCE_CREATION",
                "CREATED",
                pik,
                100,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "loan", "version": 3,
                       "variables": {"amount": 500}}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                100,
                2,
                json!({"processInstanceKey": pik, "bpmnProcessId": "loan", "version": 3,
                       "elementId": "loan", "bpmnElementType": "PROCESS"}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                eik,
                110,
                3,
                json!({"processInstanceKey": pik, "bpmnProcessId": "loan",
                       "elementId": "review", "bpmnElementType": "SERVICE_TASK"}),
            ),
            rec(
                "JOB",
                "CREATED",
                jk,
                110,
                4,
                json!({"processInstanceKey": pik, "type": "review-job",
                       "elementInstanceKey": eik, "elementId": "review"}),
            ),
            rec(
                "JOB_BATCH",
                "ACTIVATED",
                9001,
                160,
                5,
                json!({"type": "review-job", "jobKeys": [jk]}),
            ),
            rec(
                "JOB",
                "COMPLETED",
                jk,
                230,
                6,
                json!({"processInstanceKey": pik, "type": "review-job",
                       "elementInstanceKey": eik, "variables": {"approved": true}}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_COMPLETED",
                eik,
                235,
                7,
                json!({"processInstanceKey": pik, "elementId": "review",
                       "bpmnElementType": "SERVICE_TASK"}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_COMPLETED",
                pik,
                250,
                8,
                json!({"processInstanceKey": pik, "bpmnProcessId": "loan",
                       "elementId": "loan", "bpmnElementType": "PROCESS"}),
            ),
        ]
    }

    #[test]
    fn folds_a_completed_instance_with_job_split_and_tier2() {
        let traces = transform(&happy_instance(), true);
        assert_eq!(traces.len(), 1);
        let t = &traces[0];
        assert_eq!(t.instance_key, "1001");
        assert_eq!(t.process_id, "loan");
        assert_eq!(t.version, 3);
        assert_eq!(t.outcome, "completed");
        assert_eq!(t.started_at, 100);
        assert_eq!(t.duration_ms, Some(150)); // 250 - 100

        // One real element (the service task); routing/process scope dropped.
        assert_eq!(t.elements.len(), 1, "elements: {:?}", t.elements);
        let el = &t.elements[0];
        assert_eq!(el.element_id, "review");
        assert_eq!(el.duration_ms, Some(125)); // 235 - 110
        let job = el.job.as_ref().expect("job attached");
        assert_eq!(job.job_type, "review-job");
        assert_eq!(job.queue_ms, Some(50)); // 160 - 110 (created→activated)
        assert_eq!(job.service_ms, Some(70)); // 230 - 160 (activated→completed)
        assert_eq!(job.failures, 0);

        // Tier-1 creation variables.
        let cv = t.creation_variables.as_ref().expect("creation vars");
        assert_eq!(cv.values, json!({"amount": 500}));

        // Tier-2 stimulus log.
        let st = t.stimuli.as_ref().expect("stimuli");
        assert_eq!(st.len(), 1);
        assert_eq!(st[0].kind, "jobCompleted");
        assert_eq!(st[0].reference.as_deref(), Some("review-job"));
        assert_eq!(
            st[0].variables.as_ref().unwrap().values,
            json!({"approved": true})
        );
    }

    #[test]
    fn variable_records_are_ignored_scope_agnostic() {
        // A real Camunda export interleaves standalone VARIABLE records — the
        // authoritative per-scope state, carrying a `scopeKey` distinct from the
        // `processInstanceKey`. The importer must ignore them: trace variables
        // come only from creation (Tier-1) and job completions (Tier-2), so a
        // recorded-input replay stays scope-agnostic and byte-identical to a
        // dump that never carried VARIABLE records.
        let mut records = happy_instance();
        let pik = 1001;
        let eik = 2001; // the service-task element instance = a nested scope
        records.push(rec(
            "VARIABLE",
            "CREATED",
            5001,
            105,
            9,
            // root-scope snapshot (scopeKey == processInstanceKey)
            json!({"processInstanceKey": pik, "scopeKey": pik,
                   "name": "amount", "value": "500"}),
        ));
        records.push(rec(
            "VARIABLE",
            "CREATED",
            5002,
            232,
            10,
            // nested-scope snapshot (scopeKey == elementInstanceKey != pik)
            json!({"processInstanceKey": pik, "scopeKey": eik,
                   "name": "approved", "value": "true"}),
        ));

        let with_vars = transform(&records, true);
        let baseline = transform(&happy_instance(), true);
        assert_eq!(
            serde_json::to_value(&with_vars).unwrap(),
            serde_json::to_value(&baseline).unwrap(),
            "standalone VARIABLE records must not alter the derived trace"
        );

        // And the trace still carries exactly the creation + job-completion vars.
        let t = &with_vars[0];
        assert_eq!(
            t.creation_variables.as_ref().unwrap().values,
            json!({"amount": 500})
        );
        let st = t.stimuli.as_ref().unwrap();
        assert_eq!(st.len(), 1);
        assert_eq!(
            st[0].variables.as_ref().unwrap().values,
            json!({"approved": true})
        );
    }

    #[test]
    fn tier2_disabled_omits_stimuli() {
        let traces = transform(&happy_instance(), false);
        assert!(traces[0].stimuli.is_none());
        // Tier-0/Tier-1 still present.
        assert!(traces[0].creation_variables.is_some());
        assert_eq!(traces[0].elements.len(), 1);
    }

    #[test]
    fn job_without_batch_reports_total_as_service() {
        let pik = 7;
        let eik = 8;
        let jk = 9;
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p", "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                eik,
                10,
                2,
                json!({"processInstanceKey": pik, "elementId": "t", "bpmnElementType": "SERVICE_TASK"}),
            ),
            rec(
                "JOB",
                "CREATED",
                jk,
                10,
                3,
                json!({"processInstanceKey": pik, "type": "t-job", "elementInstanceKey": eik}),
            ),
            rec(
                "JOB",
                "FAILED",
                jk,
                40,
                4,
                json!({"processInstanceKey": pik, "type": "t-job", "elementInstanceKey": eik}),
            ),
            rec(
                "JOB",
                "COMPLETED",
                jk,
                100,
                5,
                json!({"processInstanceKey": pik, "type": "t-job", "elementInstanceKey": eik}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_COMPLETED",
                eik,
                110,
                6,
                json!({"processInstanceKey": pik, "elementId": "t", "bpmnElementType": "SERVICE_TASK"}),
            ),
        ];
        let t = &transform(&recs, true)[0];
        assert_eq!(t.outcome, "active"); // process never completed
        let job = t.elements[0].job.as_ref().unwrap();
        assert_eq!(job.queue_ms, None);
        assert_eq!(job.service_ms, Some(90)); // 100 - 10 (created→completed)
        assert_eq!(job.failures, 1);
    }

    #[test]
    fn incident_is_captured_and_counted() {
        let pik = 5;
        let eik = 6;
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p", "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                eik,
                10,
                2,
                json!({"processInstanceKey": pik, "elementId": "task", "bpmnElementType": "SERVICE_TASK"}),
            ),
            rec(
                "INCIDENT",
                "CREATED",
                99,
                20,
                3,
                json!({"processInstanceKey": pik, "elementId": "task", "elementInstanceKey": eik,
                       "errorType": "IO_MAPPING_ERROR", "errorMessage": "no var 'x'"}),
            ),
        ];
        let t = &transform(&recs, true)[0];
        assert_eq!(t.incidents.len(), 1);
        assert_eq!(t.incidents[0].kind, "IO_MAPPING_ERROR");
        assert_eq!(t.incidents[0].reason, "no var 'x'");
        assert_eq!(t.incidents[0].element_id, "task");
        assert_eq!(t.elements[0].incidents, 1);
    }

    #[test]
    fn terminated_process_sets_outcome() {
        let pik = 42;
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p", "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_TERMINATED",
                pik,
                50,
                2,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p", "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
        ];
        let t = &transform(&recs, true)[0];
        assert_eq!(t.outcome, "terminated");
        assert_eq!(t.duration_ms, Some(50));
    }

    #[test]
    fn commands_are_ignored_only_events_fold() {
        let pik = 1;
        let mut create = rec(
            "PROCESS_INSTANCE",
            "ELEMENT_COMPLETED",
            pik,
            100,
            2,
            json!({"processInstanceKey": pik, "bpmnProcessId": "p", "elementId": "p", "bpmnElementType": "PROCESS"}),
        );
        create.record_type = "COMMAND".into();
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p", "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            create,
        ];
        let t = &transform(&recs, true)[0];
        assert_eq!(t.outcome, "active"); // the COMMAND completion was ignored
    }

    #[test]
    fn es_search_response_envelope_is_unwrapped() {
        let body = json!({
            "hits": { "hits": [
                { "_source": {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED",
                              "recordType": "EVENT", "key": 1, "timestamp": 0, "position": 1,
                              "value": {"processInstanceKey": 1, "bpmnProcessId": "p",
                                        "elementId": "p", "bpmnElementType": "PROCESS"}} }
            ]}
        });
        let mut out = Vec::new();
        collect_from_value(body, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value_type, "PROCESS_INSTANCE");
        assert_eq!(out[0].intent, "ELEMENT_ACTIVATED");
    }

    #[test]
    fn ndjson_and_array_blobs_both_parse() {
        let ndjson = b"{\"valueType\":\"JOB\",\"intent\":\"CREATED\",\"recordType\":\"EVENT\",\"key\":1,\"value\":{\"processInstanceKey\":1,\"type\":\"a\"}}\n\n{\"valueType\":\"JOB\",\"intent\":\"COMPLETED\",\"recordType\":\"EVENT\",\"key\":1,\"value\":{\"processInstanceKey\":1,\"type\":\"a\"}}\n";
        let mut out = Vec::new();
        parse_blob(ndjson, &mut out);
        assert_eq!(out.len(), 2);

        let arr = b"[{\"valueType\":\"INCIDENT\",\"intent\":\"CREATED\",\"recordType\":\"EVENT\",\"key\":1,\"value\":{\"processInstanceKey\":1,\"errorType\":\"X\"}}]";
        let mut out2 = Vec::new();
        parse_blob(arr, &mut out2);
        assert_eq!(out2.len(), 1);
    }

    #[test]
    fn folds_message_user_task_and_timer_inputs_as_stimuli() {
        let pik = 42;
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p",
                       "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            // A native user task completes with output.
            rec(
                "USER_TASK",
                "COMPLETED",
                500,
                10,
                2,
                json!({"processInstanceKey": pik, "elementId": "review",
                       "variables": {"decision": "ok"}}),
            ),
            // A message correlates into a catch event with a payload.
            rec(
                "PROCESS_MESSAGE_SUBSCRIPTION",
                "CORRELATED",
                600,
                20,
                3,
                json!({"processInstanceKey": pik, "elementId": "await-payment",
                       "messageName": "payment", "variables": {"paid": true}}),
            ),
            // A timer fires (no payload).
            rec(
                "TIMER",
                "TRIGGERED",
                700,
                30,
                4,
                json!({"processInstanceKey": pik, "targetElementId": "escalate"}),
            ),
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_COMPLETED",
                pik,
                40,
                5,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p",
                       "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
        ];
        let t = &transform(&recs, true)[0];
        let st = t.stimuli.as_ref().expect("stimuli");
        assert_eq!(st.len(), 3, "{st:?}");

        assert_eq!(st[0].kind, "userTaskCompleted");
        assert_eq!(st[0].reference.as_deref(), Some("review"));
        assert_eq!(
            st[0].variables.as_ref().unwrap().values,
            json!({"decision": "ok"})
        );

        assert_eq!(st[1].kind, "message");
        assert_eq!(st[1].reference.as_deref(), Some("await-payment"));
        assert_eq!(
            st[1].variables.as_ref().unwrap().values,
            json!({"paid": true})
        );

        assert_eq!(st[2].kind, "timer");
        assert_eq!(st[2].reference.as_deref(), Some("escalate"));
        assert!(st[2].variables.is_none());

        // Every input had a usable element/type — the log is complete.
        assert!(!t.stimuli_truncated);
    }

    #[test]
    fn signal_subscription_routes_via_element_instance_key() {
        let pik = 77;
        let catch_eik = 78;
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p",
                       "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            // The signal catch element instance (so eik → instance is known).
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                catch_eik,
                5,
                2,
                json!({"processInstanceKey": pik, "elementId": "await-signal",
                       "bpmnElementType": "INTERMEDIATE_CATCH_EVENT"}),
            ),
            // A signal subscription correlates, keyed only by its catch instance.
            rec(
                "SIGNAL_SUBSCRIPTION",
                "CORRELATED",
                900,
                15,
                3,
                json!({"catchEventInstanceKey": catch_eik, "catchEventId": "await-signal",
                       "signalName": "go"}),
            ),
        ];
        let t = &transform(&recs, true)[0];
        let st = t.stimuli.as_ref().expect("stimuli");
        assert_eq!(st.len(), 1);
        assert_eq!(st[0].kind, "signal");
        assert_eq!(st[0].reference.as_deref(), Some("await-signal"));
    }

    #[test]
    fn referenceless_catch_input_flags_partial_replay() {
        let pik = 5;
        let recs = vec![
            rec(
                "PROCESS_INSTANCE",
                "ELEMENT_ACTIVATED",
                pik,
                0,
                1,
                json!({"processInstanceKey": pik, "bpmnProcessId": "p",
                       "elementId": "p", "bpmnElementType": "PROCESS"}),
            ),
            // A message correlation with no element id — cannot be routed in replay.
            rec(
                "PROCESS_MESSAGE_SUBSCRIPTION",
                "CORRELATED",
                600,
                10,
                2,
                json!({"processInstanceKey": pik, "messageName": "m"}),
            ),
        ];
        let t = &transform(&recs, true)[0];
        assert!(
            t.stimuli_truncated,
            "referenceless message should flag partial"
        );
    }
}
