//! Zeebe differential-trace runner (epic #1391, step 1 — the core guarantee).
//!
//! Every scenario under `formal/traces/scenarios/<name>/` (a DI-bearing
//! `process.bpmn` + a `script.json` command script) is executed on
//! engine-core, its events are rendered as RAW Zeebe-shaped records, and those
//! are normalised by THE shared normaliser spec `formal/traces/normaliser.json`
//! — the same spec the JVM capture harness (`formal/traces/capture/`) applies
//! to the pinned Zeebe engine's records. The result must equal the committed
//! Zeebe golden `formal/traces/goldens/<name>.trace.json`, step by step.
//!
//! A difference fails CI unless the scenario is listed in
//! `formal/traces/allowlist.json` with an issue reference; a listed scenario
//! that no longer diverges (a *stale* entry) fails too, so the allow-list can
//! only shrink honestly. Goldens are refreshed (offline, never in CI) with
//! `formal/traces/capture.sh`; see `formal/traces/README.md`.
#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::xml::{local_name, tokenize, Token};
use nanobpmn_engine_core::{
    Command, Engine, EngineError, Event, IncidentKind, JobActivationOptions, Key, Value,
};
use serde_json::{json, Map, Value as J};

fn traces_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../formal/traces")
}

fn read_json(path: &Path) -> J {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

// ───────────────────────────── normaliser ──────────────────────────────

/// Apply the shared normaliser spec to one step's RAW records. Mirrors
/// `Normaliser.java` in the capture harness; both implement the algorithm
/// written in the spec's `doc` field.
fn normalise(spec: &J, raw: &[J]) -> Vec<J> {
    assert_eq!(spec["version"], 1, "unsupported normaliser spec version");
    let lanes = spec["lanes"].as_array().expect("spec.lanes");
    let mut out: Vec<Vec<J>> = vec![Vec::new(); lanes.len() + 1];
    for r in raw {
        let record_type = r["recordType"].as_str().unwrap_or_default();
        if record_type == "COMMAND_REJECTION" {
            let mut n = Map::new();
            n.insert("recordType".into(), json!(record_type));
            for f in spec["rejection"]["fields"]
                .as_array()
                .expect("rejection.fields")
            {
                let f = f.as_str().expect("field name");
                n.insert(f.into(), r.get(f).cloned().unwrap_or(J::Null));
            }
            out[lanes.len()].push(J::Object(n));
            continue;
        }
        if record_type != "EVENT" {
            continue;
        }
        for (i, lane) in lanes.iter().enumerate() {
            let listed = lane["intents"]
                .as_array()
                .expect("lane.intents")
                .iter()
                .any(|x| *x == r["intent"]);
            if lane["valueType"] != r["valueType"] || !listed {
                continue;
            }
            let mut n = Map::new();
            n.insert("valueType".into(), r["valueType"].clone());
            n.insert("intent".into(), r["intent"].clone());
            for f in lane["fields"].as_array().expect("lane.fields") {
                let f = f.as_str().expect("field name");
                n.insert(f.into(), r["value"].get(f).cloned().unwrap_or(J::Null));
            }
            out[i].push(J::Object(n));
        }
    }
    out.into_iter().flatten().collect()
}

// ───────────────────────────── scenario loading ──────────────────────────────

/// A loaded scenario: the model plus the facts the adapter needs from its XML.
struct Scenario {
    name: String,
    xml: String,
    script: J,
    /// element id -> Zeebe `BpmnElementType` (derived from the BPMN tag name).
    element_types: HashMap<String, String>,
    /// (source, target) -> sequence-flow id.
    flows: HashMap<(String, String), String>,
}

/// One XML start/empty tag: local name + attributes.
struct Tag {
    name: String,
    attrs: HashMap<String, String>,
}

/// Every start/empty tag in the document, scanned by the engine's XML
/// tokenizer (single- or double-quoted attributes, whitespace around `=`,
/// comments and CDATA) — never an ad-hoc scanner that would accept a narrower
/// dialect than the loader it feeds.
fn tags(xml: &str) -> Result<Vec<Tag>, String> {
    let tokens = tokenize(xml).map_err(|e| e.0)?;
    let mut out = Vec::new();
    for token in tokens {
        let Token::Start { name, attrs, .. } = token else {
            continue;
        };
        // Extension elements (`zeebe:userTask`, …) are not BPMN elements.
        if name.starts_with("zeebe:") {
            continue;
        }
        out.push(Tag {
            name: local_name(&name).to_string(),
            attrs: attrs.into_iter().collect(),
        });
    }
    Ok(out)
}

/// `serviceTask` -> `SERVICE_TASK` (Zeebe's `BpmnElementType` naming).
fn zeebe_element_type(tag: &str) -> String {
    let mut out = String::new();
    for c in tag.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

const FLOW_NODE_TAGS: &[&str] = &[
    "startEvent",
    "endEvent",
    "intermediateCatchEvent",
    "intermediateThrowEvent",
    "boundaryEvent",
    "task",
    "serviceTask",
    "userTask",
    "sendTask",
    "receiveTask",
    "scriptTask",
    "businessRuleTask",
    "manualTask",
    "callActivity",
    "subProcess",
    "adHocSubProcess",
    "exclusiveGateway",
    "parallelGateway",
    "inclusiveGateway",
    "eventBasedGateway",
];

/// Load and validate a scenario directory. Refuses a model without DI
/// (AGENTS.md: all BPMN models need DI): a `BPMNPlane` must exist, every flow
/// node needs a `BPMNShape` and every sequence flow a `BPMNEdge`.
fn load_scenario(dir: &Path) -> Result<Scenario, String> {
    let name = dir.file_name().unwrap().to_string_lossy().to_string();
    let xml = fs::read_to_string(dir.join("process.bpmn"))
        .map_err(|e| format!("{name}: process.bpmn: {e}"))?;
    let script: J = serde_json::from_str(
        &fs::read_to_string(dir.join("script.json"))
            .map_err(|e| format!("{name}: script.json: {e}"))?,
    )
    .map_err(|e| format!("{name}: script.json: {e}"))?;
    if !script["steps"].is_array() {
        return Err(format!("{name}: script.json has no `steps` array"));
    }
    let all = tags(&xml).map_err(|e| format!("{name}: process.bpmn: {e}"))?;
    let mut element_types = HashMap::new();
    let mut flows = HashMap::new();
    let mut needs_shape = BTreeSet::new();
    let mut needs_edge = BTreeSet::new();
    let mut shapes = BTreeSet::new();
    let mut edges = BTreeSet::new();
    let mut plane = false;
    for t in &all {
        let id = t.attrs.get("id").cloned().unwrap_or_default();
        match t.name.as_str() {
            "process" => {
                element_types.insert(id, "PROCESS".to_string());
            }
            "sequenceFlow" => {
                let (Some(source), Some(target)) =
                    (t.attrs.get("sourceRef"), t.attrs.get("targetRef"))
                else {
                    return Err(format!(
                        "{name}: sequenceFlow `{id}` lacks sourceRef/targetRef"
                    ));
                };
                let key = (source.clone(), target.clone());
                if flows.insert(key, id.clone()).is_some() {
                    return Err(format!(
                        "{name}: two sequence flows share source and target"
                    ));
                }
                needs_edge.insert(id);
            }
            "BPMNPlane" => plane = true,
            "BPMNShape" => {
                shapes.insert(t.attrs.get("bpmnElement").cloned().unwrap_or_default());
            }
            "BPMNEdge" => {
                edges.insert(t.attrs.get("bpmnElement").cloned().unwrap_or_default());
            }
            tag if FLOW_NODE_TAGS.contains(&tag) => {
                element_types.insert(id.clone(), zeebe_element_type(tag));
                needs_shape.insert(id);
            }
            _ => {}
        }
    }
    let missing: Vec<String> = needs_shape
        .difference(&shapes)
        .chain(needs_edge.difference(&edges))
        .cloned()
        .collect();
    if !plane || !missing.is_empty() {
        return Err(format!(
            "{name}: process.bpmn lacks BPMN DI (AGENTS.md: all BPMN models need DI); \
             plane={plane}, elements without a shape/edge: {missing:?}"
        ));
    }
    Ok(Scenario {
        name,
        xml,
        script,
        element_types,
        flows,
    })
}

fn scenario_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = fs::read_dir(traces_dir().join("scenarios"))
        .expect("formal/traces/scenarios")
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs
}

// ───────────────────────────── nano adapter ──────────────────────────────

fn to_value(j: &J) -> Value {
    match j {
        J::Null => Value::Null,
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .unwrap_or_else(|| Value::Double(n.as_f64().unwrap_or_default())),
        J::String(s) => Value::Str(s.clone()),
        J::Array(a) => Value::List(a.iter().map(to_value).collect()),
        J::Object(o) => Value::Map(o.iter().map(|(k, v)| (k.clone(), to_value(v))).collect()),
    }
}

fn to_json(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Double(d) => json!(d),
        Value::Str(s) => json!(s),
        Value::List(l) => J::Array(l.iter().map(to_json).collect()),
        Value::Map(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), to_json(v))).collect()),
    }
}

fn variables(step: &J) -> HashMap<String, Value> {
    step["variables"]
        .as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), to_value(v))).collect())
        .unwrap_or_default()
}

/// nano incident kind -> Zeebe `ErrorType` (mirrors the server's REST
/// `incident_error_type_enum`; exhaustive so a new kind must be mapped here).
fn zeebe_error_type(kind: IncidentKind) -> &'static str {
    match kind {
        IncidentKind::JobNoRetries => "JOB_NO_RETRIES",
        IncidentKind::NoMatchingSequenceFlow => "CONDITION_ERROR",
        IncidentKind::ExpressionEvaluation => "EXTRACT_VALUE_ERROR",
        IncidentKind::UnhandledError => "UNHANDLED_ERROR_EVENT",
        IncidentKind::DecisionEvaluation => "DECISION_EVALUATION_ERROR",
        IncidentKind::CalledElementError => "CALLED_ELEMENT_ERROR",
        IncidentKind::IoMapping => "IO_MAPPING_ERROR",
    }
}

/// A rejected command's Zeebe `RejectionType`.
fn zeebe_rejection_type(err: &EngineError) -> &'static str {
    match err {
        EngineError::ProcessNotFound { .. }
        | EngineError::JobNotFound { .. }
        | EngineError::IncidentNotFound { .. }
        | EngineError::ScopeNotFound { .. }
        | EngineError::InstanceNotFound { .. }
        | EngineError::ElementNotFound { .. }
        | EngineError::ElementInstanceNotFound { .. }
        | EngineError::UserTaskNotFound { .. } => "NOT_FOUND",
        EngineError::JobNotActive { .. }
        | EngineError::JobNotActivated { .. }
        | EngineError::UserTaskNotActive { .. }
        | EngineError::IncidentNotResolvable { .. }
        | EngineError::InstanceTransitionInvalid { .. } => "INVALID_STATE",
        _ => "INVALID_ARGUMENT",
    }
}

/// The Zeebe (valueType, intent) of the command a script op writes.
fn command_of(op: &str) -> (&'static str, &'static str) {
    match op {
        "createInstance" => ("PROCESS_INSTANCE_CREATION", "CREATE"),
        "completeJob" => ("JOB", "COMPLETE"),
        "failJob" => ("JOB", "FAIL"),
        "completeUserTask" => ("USER_TASK", "COMPLETE"),
        "publishMessage" => ("MESSAGE", "PUBLISH"),
        "resolveIncident" => ("INCIDENT", "RESOLVE"),
        "updateVariables" => ("VARIABLE_DOCUMENT", "UPDATE"),
        other => panic!("unknown op `{other}`"),
    }
}

/// Runs one scenario on engine-core, rendering nano events as RAW records in
/// Zeebe's record shape (`{recordType, valueType, intent, value}`).
struct NanoRun<'a> {
    sc: &'a Scenario,
    engine: Engine,
    now: u64,
    /// The script's target instance: the instance the most recent explicit
    /// `createInstance` step created. `updateVariables` aims at it, mirroring
    /// the Java runner, which keeps the key its create command returned. A
    /// call-activity child (`parent_process_instance_key.is_some()`) is NOT
    /// adopted — it is spawned by the engine, not by a script step.
    instance: Key,
    /// The process id of the script's target instance. Kept in sync with
    /// `instance`; completion/variable labels resolve through `element_ids`
    /// instead, so a spawned child never relabels the parent's records.
    process_id: String,
    element_ids: HashMap<Key, String>,
    jobs_by_type: HashMap<String, Key>,
    jobs: HashMap<Key, (String, String, i64)>,
    user_tasks: HashMap<String, Key>,
    user_task_elements: HashMap<Key, String>,
    incidents: HashMap<String, Key>,
    incident_info: HashMap<Key, (String, &'static str)>,
    timers: HashMap<Key, String>,
    subscriptions: HashMap<Key, (String, String)>,
    /// variables known per runtime scope key (CREATED vs UPDATED). Keyed by the
    /// scope's element-INSTANCE key, not its BPMN element id: two instances of
    /// one process (or a re-entered scope) each get their own CREATED records.
    known: BTreeMap<Key, BTreeSet<String>>,
}

fn event(value_type: &str, intent: &str, value: J) -> J {
    json!({"recordType": "EVENT", "valueType": value_type, "intent": intent, "value": value})
}

impl<'a> NanoRun<'a> {
    fn new(sc: &'a Scenario) -> Self {
        let mut engine = Engine::new();
        let defs = parse_bpmn(&sc.xml).unwrap_or_else(|e| panic!("{}: parse: {e:?}", sc.name));
        engine
            .apply_command_at(Command::DeployResources(defs), 0)
            .unwrap_or_else(|e| panic!("{}: deploy: {e:?}", sc.name));
        NanoRun {
            sc,
            engine,
            now: 0,
            instance: 0,
            process_id: String::new(),
            element_ids: HashMap::new(),
            jobs_by_type: HashMap::new(),
            jobs: HashMap::new(),
            user_tasks: HashMap::new(),
            user_task_elements: HashMap::new(),
            incidents: HashMap::new(),
            incident_info: HashMap::new(),
            timers: HashMap::new(),
            subscriptions: HashMap::new(),
            known: BTreeMap::new(),
        }
    }

    fn key(map: &HashMap<String, Key>, id: &str, what: &str) -> Key {
        *map.get(id)
            .unwrap_or_else(|| panic!("no {what} '{id}' has been created yet"))
    }

    fn apply(&mut self, cmd: Command) -> Result<Vec<Event>, EngineError> {
        self.engine.apply_command_at(cmd, self.now)
    }

    /// Activate exactly the job the script step selected. `jobs_by_type` tracks
    /// the NEWEST job of a type, but a type-based `ActivateJobs` hands out the
    /// OLDEST activatable one — with two live jobs of one type the completion
    /// then targets a job that was never activated (`JobNotActivated`).
    fn activate_selected(&mut self, key: Key) -> Vec<Event> {
        let act = Command::activate_jobs_by_key(
            vec![key],
            "trace-runner",
            300_000,
            self.now,
            JobActivationOptions::default(),
        );
        self.apply(act).expect("activate jobs")
    }

    /// Execute one script step; returns its RAW records.
    fn step(&mut self, step: &J) -> Vec<J> {
        let op = step["op"].as_str().expect("op");
        let reject = step["expectRejection"].as_bool().unwrap_or(false);
        let vars = variables(step);
        let result: Result<Vec<Event>, EngineError> = match op {
            "createInstance" => {
                let pid = step["bpmnProcessId"].as_str().expect("bpmnProcessId");
                self.apply(Command::create_instance_with(pid, vars))
            }
            "completeJob" => {
                let ty = step["jobType"].as_str().expect("jobType").to_string();
                let key = Self::key(&self.jobs_by_type, &ty, "job of type");
                let mut events = Vec::new();
                if !reject {
                    events.extend(self.activate_selected(key));
                }
                self.apply(Command::complete_job_with(key, vars)).map(|e| {
                    events.extend(e);
                    events
                })
            }
            "failJob" => {
                let ty = step["jobType"].as_str().expect("jobType").to_string();
                let key = Self::key(&self.jobs_by_type, &ty, "job of type");
                let mut events = self.activate_selected(key);
                let retries = step["retries"].as_i64().expect("retries") as i32;
                let msg = step["errorMessage"].as_str().unwrap_or_default();
                self.apply(Command::fail_job(key, retries, msg)).map(|e| {
                    events.extend(e);
                    events
                })
            }
            "completeUserTask" => {
                let id = step["elementId"].as_str().expect("elementId");
                let key = Self::key(&self.user_tasks, id, "user task");
                self.apply(Command::complete_user_task_with(key, vars))
            }
            "publishMessage" => {
                let name = step["name"].as_str().expect("name");
                let ck = step["correlationKey"].as_str().expect("correlationKey");
                self.apply(Command::correlate_message_with(name, ck, vars))
            }
            "advanceClock" => {
                self.now += step["millis"].as_u64().expect("millis");
                let now = self.now;
                self.apply(Command::TriggerTimers { now })
            }
            "resolveIncident" => {
                let id = step["elementId"].as_str().expect("elementId");
                let key = Self::key(&self.incidents, id, "incident");
                self.apply(Command::resolve_incident(key))
            }
            "updateVariables" => self.apply(Command::set_variables(self.instance, vars)),
            other => panic!("{}: unknown op `{other}`", self.sc.name),
        };
        match result {
            Ok(events) => events.iter().flat_map(|e| self.render(e)).collect(),
            Err(err) => {
                let (value_type, intent) = command_of(op);
                vec![json!({
                    "recordType": "COMMAND_REJECTION",
                    "valueType": value_type,
                    "intent": intent,
                    "rejectionType": zeebe_rejection_type(&err),
                    "value": {},
                })]
            }
        }
    }

    fn element(&self, id: &str) -> J {
        let ty = self
            .sc
            .element_types
            .get(id)
            .cloned()
            .unwrap_or_else(|| format!("?{id}"));
        json!({"elementId": id, "bpmnElementType": ty})
    }

    fn lifecycle(&self, intent: &str, id: &str) -> J {
        event("PROCESS_INSTANCE", intent, self.element(id))
    }

    /// VARIABLE records for one scope. `scope_key` decides CREATED vs UPDATED
    /// (per runtime scope); `scope_label` is the BPMN element id the golden
    /// compares, mirroring how Zeebe's capture labels `scopeElementId`.
    fn variable_records(
        &mut self,
        scope_key: Key,
        scope_label: String,
        vars: &HashMap<String, Value>,
    ) -> Vec<J> {
        let sorted: BTreeMap<&String, &Value> = vars.iter().collect();
        let known = self.known.entry(scope_key).or_default();
        sorted
            .into_iter()
            .map(|(name, v)| {
                let intent = if known.insert(name.clone()) {
                    "CREATED"
                } else {
                    "UPDATED"
                };
                event(
                    "VARIABLE",
                    intent,
                    json!({"name": name, "value": to_json(v), "scopeElementId": scope_label}),
                )
            })
            .collect()
    }

    fn job_value(&self, key: Key, retries: Option<i64>) -> J {
        let (el, ty, r) = self.jobs.get(&key).cloned().unwrap_or_default();
        json!({"elementId": el, "type": ty, "retries": retries.unwrap_or(r)})
    }

    /// Render one nano event as zero or more RAW Zeebe-shaped records.
    fn render(&mut self, e: &Event) -> Vec<J> {
        match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                variables,
                parent_process_instance_key,
                ..
            } => {
                // Only an explicit `createInstance` step moves the script's
                // target. A call-activity child (has a parent) is spawned by
                // the engine; adopting it would redirect a later
                // `updateVariables` to the child while the Java runner keeps
                // targeting the key its create command returned.
                if parent_process_instance_key.is_none() {
                    self.instance = *instance_key;
                    self.process_id = process_id.clone();
                }
                self.element_ids.insert(*instance_key, process_id.clone());
                let mut out = vec![
                    self.lifecycle("ELEMENT_ACTIVATING", process_id),
                    self.lifecycle("ELEMENT_ACTIVATED", process_id),
                ];
                out.extend(self.variable_records(*instance_key, process_id.clone(), variables));
                out
            }
            Event::ProcessInstanceCompleted { instance_key } => {
                // Label with the COMPLETED instance's process, not the
                // last-created one: a call-activity child created after the
                // parent must not relabel the parent's completion.
                let pid = self
                    .element_ids
                    .get(instance_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{instance_key}"));
                vec![
                    self.lifecycle("ELEMENT_COMPLETING", &pid),
                    self.lifecycle("ELEMENT_COMPLETED", &pid),
                ]
            }
            Event::ElementActivating {
                element_instance_key,
                element_id,
                ..
            } => {
                self.element_ids
                    .insert(*element_instance_key, element_id.to_string());
                vec![self.lifecycle("ELEMENT_ACTIVATING", element_id)]
            }
            Event::ElementActivated { element_id, .. } => {
                vec![self.lifecycle("ELEMENT_ACTIVATED", element_id)]
            }
            Event::ElementCompleting { element_id, .. } => {
                vec![self.lifecycle("ELEMENT_COMPLETING", element_id)]
            }
            Event::ElementCompleted { element_id, .. } => {
                vec![self.lifecycle("ELEMENT_COMPLETED", element_id)]
            }
            Event::SequenceFlowTaken { from, to, .. } => {
                let id = self
                    .sc
                    .flows
                    .get(&(from.to_string(), to.to_string()))
                    .cloned()
                    .unwrap_or_else(|| format!("?{from}->{to}"));
                vec![event(
                    "PROCESS_INSTANCE",
                    "SEQUENCE_FLOW_TAKEN",
                    json!({"elementId": id, "bpmnElementType": "SEQUENCE_FLOW"}),
                )]
            }
            Event::VariablesUpdated {
                instance_key,
                variables,
            } => {
                // Use the event's OWN instance key for both the known-variable
                // set and the scope label: the script target may be a
                // different instance (e.g. message correlation updates A
                // while B was created last), and A's variables are CREATED in
                // A's scope regardless of what B already holds.
                let pid = self
                    .element_ids
                    .get(instance_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{instance_key}"));
                self.variable_records(*instance_key, pid, variables)
            }
            Event::ScopedVariablesUpdated {
                scope_key,
                variables,
                ..
            } => {
                let scope = self
                    .element_ids
                    .get(scope_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{scope_key}"));
                self.variable_records(*scope_key, scope, variables)
            }
            Event::JobCreated {
                job_key,
                element_id,
                job_type,
                retries,
                ..
            } => {
                self.jobs_by_type.insert(job_type.clone(), *job_key);
                self.jobs.insert(
                    *job_key,
                    (
                        element_id.to_string(),
                        job_type.clone(),
                        i64::from(*retries),
                    ),
                );
                vec![event("JOB", "CREATED", self.job_value(*job_key, None))]
            }
            Event::JobCompleted { job_key, .. } => {
                vec![event("JOB", "COMPLETED", self.job_value(*job_key, None))]
            }
            Event::JobFailed {
                job_key, retries, ..
            } => {
                if let Some(j) = self.jobs.get_mut(job_key) {
                    j.2 = i64::from(*retries);
                }
                vec![event("JOB", "FAILED", self.job_value(*job_key, None))]
            }
            Event::JobRetriesUpdated {
                job_key, retries, ..
            } => {
                if let Some(j) = self.jobs.get_mut(job_key) {
                    j.2 = i64::from(*retries);
                }
                vec![event(
                    "JOB",
                    "RETRIES_UPDATED",
                    self.job_value(*job_key, None),
                )]
            }
            Event::JobCanceled { job_key, .. } => {
                vec![event("JOB", "CANCELED", self.job_value(*job_key, None))]
            }
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                ..
            } => {
                self.user_tasks
                    .insert(element_id.to_string(), *user_task_key);
                self.user_task_elements
                    .insert(*user_task_key, element_id.to_string());
                vec![event(
                    "USER_TASK",
                    "CREATED",
                    json!({"elementId": element_id}),
                )]
            }
            Event::UserTaskCompleted { user_task_key, .. } => {
                let el = self.user_task_elements.get(user_task_key).cloned();
                vec![event("USER_TASK", "COMPLETED", json!({"elementId": el}))]
            }
            Event::UserTaskCanceled { user_task_key, .. } => {
                let el = self.user_task_elements.get(user_task_key).cloned();
                vec![event("USER_TASK", "CANCELED", json!({"elementId": el}))]
            }
            Event::MessageSubscriptionCreated {
                subscription_key,
                element_id,
                message_name,
                ..
            } => {
                let v = (element_id.to_string(), message_name.clone());
                self.subscriptions.insert(*subscription_key, v.clone());
                vec![event(
                    "PROCESS_MESSAGE_SUBSCRIPTION",
                    "CREATED",
                    json!({"elementId": v.0, "messageName": v.1}),
                )]
            }
            Event::MessageCorrelated {
                subscription_key, ..
            } => {
                let (el, name) = self
                    .subscriptions
                    .get(subscription_key)
                    .cloned()
                    .unwrap_or_default();
                vec![event(
                    "PROCESS_MESSAGE_SUBSCRIPTION",
                    "CORRELATED",
                    json!({"elementId": el, "messageName": name}),
                )]
            }
            Event::MessageSubscriptionCanceled {
                subscription_key, ..
            } => {
                let (el, name) = self
                    .subscriptions
                    .get(subscription_key)
                    .cloned()
                    .unwrap_or_default();
                vec![event(
                    "PROCESS_MESSAGE_SUBSCRIPTION",
                    "DELETED",
                    json!({"elementId": el, "messageName": name}),
                )]
            }
            Event::TimerCreated {
                timer_key,
                element_id,
                ..
            } => {
                self.timers.insert(*timer_key, element_id.to_string());
                vec![event(
                    "TIMER",
                    "CREATED",
                    json!({"targetElementId": element_id}),
                )]
            }
            Event::TimerTriggered { element_id, .. } => {
                vec![event(
                    "TIMER",
                    "TRIGGERED",
                    json!({"targetElementId": element_id}),
                )]
            }
            Event::TimerCanceled { element_id, .. } => {
                vec![event(
                    "TIMER",
                    "CANCELED",
                    json!({"targetElementId": element_id}),
                )]
            }
            Event::IncidentRaised {
                incident_key,
                element_id,
                kind,
                ..
            } => {
                let info = (element_id.to_string(), zeebe_error_type(*kind));
                self.incidents.insert(info.0.clone(), *incident_key);
                self.incident_info.insert(*incident_key, info.clone());
                vec![event(
                    "INCIDENT",
                    "CREATED",
                    json!({"elementId": info.0, "errorType": info.1}),
                )]
            }
            Event::IncidentResolved { incident_key, .. } => {
                let (el, ty) = self
                    .incident_info
                    .get(incident_key)
                    .cloned()
                    .unwrap_or_default();
                vec![event(
                    "INCIDENT",
                    "RESOLVED",
                    json!({"elementId": el, "errorType": ty}),
                )]
            }
            // Everything else has no counterpart in the normaliser's lanes.
            _ => Vec::new(),
        }
    }
}

/// Run a scenario on engine-core and normalise it: the comparable trace.
fn nano_trace(spec: &J, sc: &Scenario) -> Vec<J> {
    let mut run = NanoRun::new(sc);
    sc.script["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .enumerate()
        .map(|(i, step)| {
            let raw = run.step(step);
            json!({"step": i, "op": step["op"], "records": normalise(spec, &raw)})
        })
        .collect()
}

// ───────────────────────────── differential verdict ──────────────────────────────

/// First difference between the Zeebe golden steps and nano's, if any.
fn diff(golden: &[J], nano: &[J]) -> Option<String> {
    for i in 0..golden.len().max(nano.len()) {
        let (g, n) = (golden.get(i), nano.get(i));
        if g != n {
            return Some(format!(
                "step {i}:\n  zeebe: {}\n  nano:  {}",
                g.map(J::to_string).unwrap_or_else(|| "<none>".into()),
                n.map(J::to_string).unwrap_or_else(|| "<none>".into())
            ));
        }
    }
    None
}

/// Judge per-scenario outcomes (`name -> Some(diff)` when diverging) against
/// the allow-list. Fails on: an unlisted divergence, a listed entry without an
/// issue reference, and a STALE entry (listed but not diverging / unknown).
fn verdict(outcomes: &BTreeMap<String, Option<String>>, allowlist: &J) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let mut listed = BTreeSet::new();
    for entry in allowlist["entries"].as_array().expect("allowlist.entries") {
        let scenario = entry["scenario"].as_str().unwrap_or_default().to_string();
        let issue = entry["issue"].as_str().unwrap_or_default();
        let ok_ref = issue
            .strip_prefix('#')
            .or_else(|| issue.rsplit_once("/issues/").map(|(_, n)| n))
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()));
        if !ok_ref {
            errors.push(format!(
                "allow-list entry `{scenario}` lacks an issue reference (`#N`)"
            ));
        }
        match outcomes.get(&scenario) {
            Some(Some(_)) => {}
            Some(None) => errors.push(format!(
                "STALE allow-list entry `{scenario}` ({issue}): the scenario no longer diverges \
                 from Zeebe — remove the entry"
            )),
            None => errors.push(format!(
                "STALE allow-list entry `{scenario}` ({issue}): no such scenario"
            )),
        }
        listed.insert(scenario);
    }
    for (name, d) in outcomes {
        if let Some(d) = d {
            if !listed.contains(name) {
                errors.push(format!("scenario `{name}` diverges from Zeebe:\n{d}"));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn load_spec() -> J {
    read_json(&traces_dir().join("normaliser.json"))
}

/// Run every scenario: `name -> Some(diff)` when nano diverges from the golden.
fn run_corpus(spec: &J) -> BTreeMap<String, Option<String>> {
    let goldens = traces_dir().join("goldens");
    scenario_dirs()
        .iter()
        .map(|dir| {
            let sc = load_scenario(dir).unwrap_or_else(|e| panic!("{e}"));
            let golden = read_json(&goldens.join(format!("{}.trace.json", sc.name)));
            let steps = golden["steps"].as_array().expect("golden steps").clone();
            let d = diff(&steps, &nano_trace(spec, &sc));
            (sc.name, d)
        })
        .collect()
}

// ───────────────────────────── the gate ──────────────────────────────

/// THE differential gate: every scenario's nano trace equals its Zeebe golden,
/// modulo the issue-referenced allow-list (which must not be stale).
#[test]
fn corpus_matches_zeebe_goldens() {
    let spec = load_spec();
    let outcomes = run_corpus(&spec);
    assert!(!outcomes.is_empty(), "the trace corpus is empty");
    let allowlist = read_json(&traces_dir().join("allowlist.json"));
    if let Err(errors) = verdict(&outcomes, &allowlist) {
        panic!(
            "Zeebe differential traces failed:\n\n{}\n\nSee formal/traces/README.md.",
            errors.join("\n\n")
        );
    }
}

/// Goldens are captured per pin: the stamped pin must be the one in
/// `formal/parity/zeebe-pin.json` (the single source of truth for the version),
/// and every scenario has exactly one golden trace + touched-cells file.
#[test]
fn goldens_match_the_zeebe_pin_and_the_scenarios() {
    let pin = read_json(&traces_dir().join("../parity/zeebe-pin.json"));
    let sha = pin["sha"].as_str().expect("pin sha");
    let goldens = traces_dir().join("goldens");
    let stamped = fs::read_to_string(goldens.join("PIN")).expect("goldens/PIN");
    assert_eq!(
        stamped.trim(),
        sha,
        "goldens were captured at another Zeebe pin — run formal/traces/capture.sh and commit the diff"
    );
    let names: BTreeSet<String> = scenario_dirs()
        .iter()
        .map(|d| d.file_name().unwrap().to_string_lossy().to_string())
        .collect();
    let mut golden_names = BTreeSet::new();
    for entry in fs::read_dir(&goldens).unwrap() {
        let file = entry.unwrap().file_name().to_string_lossy().to_string();
        if let Some(n) = file.strip_suffix(".trace.json") {
            golden_names.insert(n.to_string());
            let cells = read_json(&goldens.join(format!("{n}.cells.json")));
            assert_eq!(cells["zeebePin"], sha, "{n}.cells.json pin");
            let golden = read_json(&goldens.join(&file));
            assert_eq!(golden["zeebePin"], sha, "{file} pin");
            for cell in cells["cells"].as_array().expect("cells") {
                let c = cell.as_str().expect("cell id");
                assert!(
                    well_formed_cell(c),
                    "{n}.cells.json: malformed cell id `{c}`"
                );
            }
        }
    }
    assert_eq!(
        names, golden_names,
        "scenarios without a golden (or orphan goldens)"
    );
}

/// Grammar of the touched-cell ids (shared with `formal/parity/extract.mjs`):
/// `failure:<Class>.<method>:<ErrorType>`, `rejection:<Class>.<method>:<RejectionType>`
/// and the coarse `element:` / `intent:` / `incident:` families.
fn well_formed_cell(c: &str) -> bool {
    let ident = |s: &str| s.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    let upper = |s: &str| s.chars().all(|ch| ch.is_ascii_uppercase() || ch == '_');
    match c.split_once(':') {
        Some(("failure" | "rejection", rest)) => {
            let Some((site, ty)) = rest.rsplit_once(':') else {
                return false;
            };
            let Some((class, method)) = site.split_once('.') else {
                return false;
            };
            !class.is_empty()
                && ident(class)
                && ident(method)
                && upper(ty)
                && (c.starts_with("failure:") || !ty.is_empty())
        }
        Some(("element", rest)) => !rest.is_empty() && ident(rest),
        Some(("incident", rest)) => !rest.is_empty() && upper(rest),
        Some(("intent", rest)) => rest
            .split_once(':')
            .is_some_and(|(a, b)| ident(a) && !b.is_empty() && upper(b)),
        _ => false,
    }
}

// ───────────────────────────── tests of the harness itself ──────────────────────────────

fn raw_event(value_type: &str, intent: &str, value: J) -> J {
    event(value_type, intent, value)
}

#[test]
fn normaliser_keeps_lane_order_selects_fields_and_drops_the_rest() {
    let spec = load_spec();
    let raw = vec![
        raw_event(
            "JOB",
            "CREATED",
            json!({"elementId": "t", "type": "w", "retries": 3, "worker": "x"}),
        ),
        raw_event(
            "PROCESS_INSTANCE",
            "ELEMENT_ACTIVATED",
            json!({"elementId": "t", "bpmnElementType": "SERVICE_TASK", "flowScopeKey": 9}),
        ),
        json!({"recordType": "COMMAND", "valueType": "JOB", "intent": "COMPLETE", "value": {}}),
        raw_event("JOB", "TIMED_OUT", json!({})),
        raw_event("JOB_BATCH", "ACTIVATED", json!({})),
        json!({"recordType": "COMMAND_REJECTION", "valueType": "JOB", "intent": "COMPLETE", "rejectionType": "NOT_FOUND", "rejectionReason": "x", "value": {}}),
        raw_event(
            "PROCESS_INSTANCE",
            "ELEMENT_COMPLETED",
            json!({"elementId": "t"}),
        ),
    ];
    assert_eq!(
        normalise(&spec, &raw),
        vec![
            json!({"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "t", "bpmnElementType": "SERVICE_TASK"}),
            json!({"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_COMPLETED", "elementId": "t", "bpmnElementType": null}),
            json!({"valueType": "JOB", "intent": "CREATED", "elementId": "t", "type": "w", "retries": 3}),
            json!({"recordType": "COMMAND_REJECTION", "valueType": "JOB", "intent": "COMPLETE", "rejectionType": "NOT_FOUND"}),
        ]
    );
}

#[test]
fn normaliser_does_not_compare_interleaving_across_lanes() {
    let spec = load_spec();
    let pi = raw_event(
        "PROCESS_INSTANCE",
        "ELEMENT_ACTIVATED",
        json!({"elementId": "a"}),
    );
    let job = raw_event("JOB", "CREATED", json!({"elementId": "a"}));
    assert_eq!(
        normalise(&spec, &[pi.clone(), job.clone()]),
        normalise(&spec, &[job, pi])
    );
}

#[test]
fn normaliser_spec_lanes_are_unique_and_well_formed() {
    let spec = load_spec();
    let mut seen = BTreeSet::new();
    for lane in spec["lanes"].as_array().unwrap() {
        let vt = lane["valueType"].as_str().unwrap();
        assert!(seen.insert(vt.to_string()), "duplicate lane {vt}");
        assert!(
            !lane["intents"].as_array().unwrap().is_empty(),
            "{vt}: no intents"
        );
        assert!(lane["fields"].is_array(), "{vt}: no fields");
    }
}

fn first_scenario() -> (J, Scenario, Vec<J>) {
    let spec = load_spec();
    let dir = traces_dir().join("scenarios/service-task-complete");
    let sc = load_scenario(&dir).unwrap();
    let golden = read_json(&traces_dir().join("goldens/service-task-complete.trace.json"));
    let steps = golden["steps"].as_array().unwrap().clone();
    (spec, sc, steps)
}

#[test]
fn seeded_divergence_fails_the_runner_unless_allow_listed() {
    let (spec, sc, mut golden) = first_scenario();
    assert_eq!(
        diff(&golden, &nano_trace(&spec, &sc)),
        None,
        "precondition: passes"
    );
    // Seed a divergence: Zeebe "says" the job was created with 4 retries.
    let records = golden[0]["records"].as_array_mut().unwrap();
    let job = records
        .iter_mut()
        .find(|r| r["valueType"] == "JOB")
        .unwrap();
    job["retries"] = json!(4);
    let d = diff(&golden, &nano_trace(&spec, &sc));
    assert!(d.as_deref().is_some_and(|d| d.contains("step 0")), "{d:?}");
    let outcomes = BTreeMap::from([(sc.name.clone(), d)]);
    let err = verdict(&outcomes, &json!({"entries": []})).unwrap_err();
    assert!(err[0].contains("diverges from Zeebe"), "{err:?}");
    let listed = json!({"entries": [{"scenario": sc.name, "issue": "#1391", "reason": "seeded"}]});
    assert_eq!(verdict(&outcomes, &listed), Ok(()));
}

#[test]
fn stale_allow_list_entry_fails_the_runner() {
    let (spec, sc, golden) = first_scenario();
    let outcomes = BTreeMap::from([(sc.name.clone(), diff(&golden, &nano_trace(&spec, &sc)))]);
    let stale =
        json!({"entries": [{"scenario": sc.name, "issue": "#1391", "reason": "fixed since"}]});
    let err = verdict(&outcomes, &stale).unwrap_err();
    assert_eq!(err.len(), 1, "{err:?}");
    assert!(err[0].starts_with("STALE allow-list entry"), "{err:?}");
    let unknown = json!({"entries": [{"scenario": "no-such-scenario", "issue": "#1391"}]});
    assert!(verdict(&outcomes, &unknown).unwrap_err()[0].contains("no such scenario"));
}

#[test]
fn allow_list_entry_without_issue_reference_fails() {
    let outcomes = BTreeMap::from([("s".to_string(), Some("d".to_string()))]);
    let err = verdict(
        &outcomes,
        &json!({"entries": [{"scenario": "s", "issue": "soon"}]}),
    )
    .unwrap_err();
    assert!(err[0].contains("lacks an issue reference"), "{err:?}");
    let url = "https://github.com/nanobpm/nano-bpm/issues/1391";
    assert_eq!(
        verdict(
            &outcomes,
            &json!({"entries": [{"scenario": "s", "issue": url}]})
        ),
        Ok(())
    );
}

#[test]
fn scenario_without_di_is_rejected() {
    let src = traces_dir().join("scenarios/service-task-complete");
    let tmp = std::env::temp_dir().join(format!("nano-trace-no-di-{}", std::process::id()));
    fs::create_dir_all(&tmp).unwrap();
    let xml = fs::read_to_string(src.join("process.bpmn")).unwrap();
    let cut = xml.find("<bpmndi:BPMNDiagram").unwrap();
    fs::write(
        tmp.join("process.bpmn"),
        format!("{}</bpmn:definitions>\n", &xml[..cut]),
    )
    .unwrap();
    fs::copy(src.join("script.json"), tmp.join("script.json")).unwrap();
    let err = load_scenario(&tmp)
        .err()
        .expect("a DI-less scenario must be rejected");
    assert!(err.contains("lacks BPMN DI"), "{err}");
    // A diagram that misses one element's shape is rejected too.
    let partial = xml.replace("bpmnElement=\"task\"", "bpmnElement=\"elsewhere\"");
    fs::write(tmp.join("process.bpmn"), partial).unwrap();
    let err = load_scenario(&tmp)
        .err()
        .expect("partial DI must be rejected");
    assert!(err.contains("\"task\""), "{err}");
    fs::remove_dir_all(&tmp).unwrap();
}

/// The metadata pass over `process.bpmn` uses the engine's XML tokenizer, so
/// single-quoted attributes and whitespace around `=` are recognised exactly
/// as the loader recognises them (an ad-hoc `="..."` scanner used to drop
/// those, losing `sourceRef`/`targetRef` and panicking the loader).
#[test]
fn scenario_attributes_accept_both_quote_styles_and_spacing() {
    let src = traces_dir().join("scenarios/service-task-complete");
    let tmp = std::env::temp_dir().join(format!("nano-trace-attrs-{}", std::process::id()));
    fs::create_dir_all(&tmp).unwrap();
    let xml = fs::read_to_string(src.join("process.bpmn")).unwrap();
    let styled = xml
        .replace("sourceRef=\"start\"", "sourceRef = 'start'")
        .replace("targetRef=\"task\"", "targetRef=  'task'")
        .replace("bpmnElement=\"f1\"", "bpmnElement =\n                'f1'");
    assert_ne!(styled, xml, "precondition: the rewrite changed the model");
    fs::write(tmp.join("process.bpmn"), styled).unwrap();
    fs::copy(src.join("script.json"), tmp.join("script.json")).unwrap();
    let sc = load_scenario(&tmp).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        sc.flows.get(&("start".to_string(), "task".to_string())),
        Some(&"f1".to_string())
    );
    fs::remove_dir_all(&tmp).unwrap();
}

/// Variable existence is tracked per runtime scope key, not per BPMN element
/// id: a second instance of the same process renders its initial variables as
/// CREATED, not UPDATED.
#[test]
fn second_instance_variables_are_created_not_updated() {
    let src = traces_dir().join("scenarios/service-task-complete");
    let tmp = std::env::temp_dir().join(format!("nano-trace-vars-{}", std::process::id()));
    fs::create_dir_all(&tmp).unwrap();
    fs::copy(src.join("process.bpmn"), tmp.join("process.bpmn")).unwrap();
    fs::write(
        tmp.join("script.json"),
        r#"{"description": "two instances", "steps": [
            {"op": "createInstance", "bpmnProcessId": "service-task", "variables": {"x": 1}},
            {"op": "createInstance", "bpmnProcessId": "service-task", "variables": {"x": 2}}
        ]}"#,
    )
    .unwrap();
    let sc = load_scenario(&tmp).unwrap_or_else(|e| panic!("{e}"));
    let trace = nano_trace(&load_spec(), &sc);
    for (i, step) in trace.iter().enumerate() {
        let records = step["records"].as_array().unwrap();
        let var = records
            .iter()
            .find(|r| r["valueType"] == "VARIABLE")
            .unwrap_or_else(|| panic!("step {i}: no VARIABLE record"));
        assert_eq!(
            var["intent"],
            "CREATED",
            "step {i}: the variable of instance {} must be CREATED",
            i + 1
        );
    }
    fs::remove_dir_all(&tmp).unwrap();
}

#[test]
fn cell_id_grammar() {
    for ok in [
        "failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR",
        "failure:Foo.:",
        "rejection:JobCommandPreconditionValidator.checkJobState:NOT_FOUND",
        "element:ServiceTask",
        "intent:ProcessInstance:ELEMENT_ACTIVATED",
        "incident:CONDITION_ERROR",
    ] {
        assert!(well_formed_cell(ok), "{ok}");
    }
    for bad in [
        "failure:BpmnJobBehavior:EXTRACT_VALUE_ERROR",
        "failure:a.b.C.m:X",
        "rejection:Foo.bar:",
        "incident:",
        "thing:x",
    ] {
        assert!(!well_formed_cell(bad), "{bad}");
    }
}

// ───────────────────────────── adapter regressions ──────────────────────────────

/// Write a scratch scenario (model + script) into a unique temp dir and run it
/// through the adapter. The inline models carry DI because `load_scenario`
/// enforces it (AGENTS.md); these tests exercise the runner's command/event
/// correlation, not the loader.
fn run_scratch(tag: &str, xml: &str, script: &str) -> Vec<J> {
    let tmp = std::env::temp_dir().join(format!(
        "nano-trace-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    fs::create_dir_all(&tmp).unwrap();
    fs::write(tmp.join("process.bpmn"), xml).unwrap();
    fs::write(tmp.join("script.json"), script).unwrap();
    let sc = load_scenario(&tmp).unwrap_or_else(|e| panic!("{e}"));
    let trace = nano_trace(&load_spec(), &sc);
    fs::remove_dir_all(&tmp).unwrap();
    trace
}

/// Records of one step carrying `valueType`/`intent`.
fn records_of<'a>(step: &'a J, value_type: &str, intent: &str) -> Vec<&'a J> {
    step["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["valueType"] == value_type && r["intent"] == intent)
        .collect()
}

/// A two-service-task model whose tasks share the job type `work`.
const TWO_TASKS_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_two_tasks" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="two-tasks" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="taskA">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" retries="3" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="taskB">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" retries="3" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="taskA" />
    <bpmn:sequenceFlow id="f2" sourceRef="taskA" targetRef="taskB" />
    <bpmn:sequenceFlow id="f3" sourceRef="taskB" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="two-tasks">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="taskA_di" bpmnElement="taskA">
        <dc:Bounds x="250" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="taskB_di" bpmnElement="taskB">
        <dc:Bounds x="410" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="570" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="250" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="350" y="118" />
        <di:waypoint x="410" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f3_di" bpmnElement="f3">
        <di:waypoint x="510" y="118" />
        <di:waypoint x="570" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// A call-activity parent (`caller`) invoking a trivial child (`child`). The
/// default `propagateAllParentVariables` copies the parent's variables into
/// the child at spawn, so the child creation carries `x`.
const CALL_ACTIVITY_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_call" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="caller" isExecutable="true">
    <bpmn:startEvent id="pstart" />
    <bpmn:callActivity id="call">
      <bpmn:extensionElements>
        <zeebe:calledElement processId="child" />
      </bpmn:extensionElements>
    </bpmn:callActivity>
    <bpmn:endEvent id="pend" />
    <bpmn:sequenceFlow id="pf1" sourceRef="pstart" targetRef="call" />
    <bpmn:sequenceFlow id="pf2" sourceRef="call" targetRef="pend" />
  </bpmn:process>
  <bpmn:process id="child" isExecutable="true">
    <bpmn:startEvent id="cstart" />
    <bpmn:endEvent id="cend" />
    <bpmn:sequenceFlow id="cf1" sourceRef="cstart" targetRef="cend" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="caller">
      <bpmndi:BPMNShape id="pstart_di" bpmnElement="pstart">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="call_di" bpmnElement="call">
        <dc:Bounds x="250" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="pend_di" bpmnElement="pend">
        <dc:Bounds x="412" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="pf1_di" bpmnElement="pf1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="250" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="pf2_di" bpmnElement="pf2">
        <di:waypoint x="350" y="118" />
        <di:waypoint x="412" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
    <bpmndi:BPMNPlane id="BPMNPlane_2" bpmnElement="child">
      <bpmndi:BPMNShape id="cstart_di" bpmnElement="cstart">
        <dc:Bounds x="160" y="300" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="cend_di" bpmnElement="cend">
        <dc:Bounds x="412" y="300" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="cf1_di" bpmnElement="cf1">
        <di:waypoint x="196" y="318" />
        <di:waypoint x="412" y="318" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// A message-catch model: start → catch `go` (correlation `k`) → end.
const MESSAGE_CATCH_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_msg" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="msg-catch" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:intermediateCatchEvent id="catch">
      <bpmn:messageEventDefinition messageRef="m" />
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="catch" />
    <bpmn:sequenceFlow id="f2" sourceRef="catch" targetRef="end" />
    <bpmn:message id="m" name="go">
      <bpmn:extensionElements>
        <zeebe:subscription correlationKey="k" />
      </bpmn:extensionElements>
    </bpmn:message>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="msg-catch">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="catch_di" bpmnElement="catch">
        <dc:Bounds x="250" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="412" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="250" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="286" y="118" />
        <di:waypoint x="412" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// With two live jobs of one type, completing the NEWEST must not fail with
/// `JobNotActivated`: the adapter activates exactly the selected key (a
/// type-based activation hands out the OLDEST job, so completion of the
/// newest then rejected).
#[test]
fn complete_job_activates_the_selected_key() {
    let trace = run_scratch(
        "jobsel",
        TWO_TASKS_XML,
        r#"{"description": "two live jobs, one type", "steps": [
            {"op": "createInstance", "bpmnProcessId": "two-tasks"},
            {"op": "createInstance", "bpmnProcessId": "two-tasks"},
            {"op": "completeJob", "jobType": "work"}
        ]}"#,
    );
    let step = &trace[2];
    assert!(
        records_of(step, "JOB", "COMPLETED").len() == 1,
        "the selected job must complete: {}",
        step
    );
    assert!(
        step["records"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["recordType"] != "COMMAND_REJECTION"),
        "no rejection expected: {step}"
    );
}

/// A call-activity child is spawned by the engine, not by a script step: it
/// must not become the script's target instance. A later `updateVariables`
/// still targets the PARENT (as the Java runner does with the key its create
/// command returned), so the parent's `x` renders UPDATED in the caller's
/// scope — not CREATED against the child.
#[test]
fn spawned_child_does_not_steal_the_script_target() {
    let trace = run_scratch(
        "childtarget",
        CALL_ACTIVITY_XML,
        r#"{"description": "call activity target", "steps": [
            {"op": "createInstance", "bpmnProcessId": "caller", "variables": {"x": 1}},
            {"op": "updateVariables", "variables": {"x": 2}}
        ]}"#,
    );
    let step = &trace[1];
    let vars = records_of(step, "VARIABLE", "UPDATED");
    assert_eq!(vars.len(), 1, "parent x must be UPDATED: {step}");
    assert_eq!(
        vars[0]["scopeElementId"], "caller",
        "the update lands on the parent instance: {step}"
    );
}

/// Completion is labelled with the COMPLETED instance's process: after a call
/// activity spawns `child`, the parent's later completion still renders as
/// `caller`, not as the most recently created process.
#[test]
fn completion_is_labelled_with_the_completed_instance() {
    let trace = run_scratch(
        "completelabel",
        CALL_ACTIVITY_XML,
        r#"{"description": "call activity completion label", "steps": [
            {"op": "createInstance", "bpmnProcessId": "caller"}
        ]}"#,
    );
    let step = &trace[0];
    let completing: Vec<&J> = step["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["valueType"] == "PROCESS_INSTANCE" && r["intent"] == "ELEMENT_COMPLETING")
        .collect();
    // Normalised records carry the value's fields at the top level.
    let labels: BTreeSet<String> = completing
        .iter()
        .map(|r| r["elementId"].as_str().unwrap().to_string())
        .collect();
    assert!(
        labels.contains("caller"),
        "the parent's completion must be labelled `caller`, got {labels:?}: {step}"
    );
    assert!(
        labels.contains("child"),
        "the child's completion must be labelled `child`, got {labels:?}: {step}"
    );
}

/// `VariablesUpdated` is keyed by the EVENT's instance, not the script's
/// last-created one: message-correlating `paid` into instance A renders
/// CREATED in A's scope even though B (created later, holding `paid`) is the
/// current script target.
#[test]
fn variables_updated_uses_the_events_instance_key() {
    let trace = run_scratch(
        "msgvars",
        MESSAGE_CATCH_XML,
        r#"{"description": "message vars land on the correlated instance", "steps": [
            {"op": "createInstance", "bpmnProcessId": "msg-catch", "variables": {"k": "a"}},
            {"op": "createInstance", "bpmnProcessId": "msg-catch", "variables": {"k": "b", "paid": false}},
            {"op": "publishMessage", "name": "go", "correlationKey": "a", "variables": {"paid": true}}
        ]}"#,
    );
    let step = &trace[2];
    let vars: Vec<&J> = step["records"]
        .as_array()
        .unwrap()
        .iter()
        // Normalised VARIABLE records keep `intent` and the value's `name` at
        // the top level (the normaliser flattens the selected fields).
        .filter(|r| r["valueType"] == "VARIABLE" && r["name"] == "paid")
        .collect();
    assert_eq!(vars.len(), 1, "one paid record expected: {step}");
    assert_eq!(
        vars[0]["intent"], "CREATED",
        "paid is new in A's scope even though B already holds it: {step}"
    );
    assert_eq!(
        vars[0]["scopeElementId"], "msg-catch",
        "labelled with A's process: {step}"
    );
}
