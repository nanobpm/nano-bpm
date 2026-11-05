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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::xml::{local_name, tokenize, Token};
use nanobpmn_engine_core::{
    Command, Engine, EngineError, Event, IncidentKind, JobActivationOptions, Key,
    MessageSubscriptionKind, Value,
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
/// written in the spec's `doc` field. `ids` carries the scenario's
/// canonical-key map ACROSS steps (spec `doc` step 5).
fn normalise(spec: &J, raw: &[J], ids: &mut HashMap<u64, u64>) -> Vec<J> {
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
    let kept: Vec<J> = out.into_iter().flatten().collect();
    renumber_keys(kept, ids)
}

/// Rewrite every `*Key` field of the kept records to a canonical id: the
/// first distinct raw key the scenario's records touch (in step order, then
/// lane order) becomes 1, the next 2, and so on, with `ids` kept for the
/// WHOLE scenario — a per-step reset would renumber either of two
/// same-process instances to the same step-local ids, hiding a cross-step
/// target swap (spec `doc` step 5). serde_json objects iterate in sorted
/// field-name order, which the Java mirror matches explicitly, so both sides
/// assign the same ids. Raw key VALUES are engine-specific, but the identity
/// pattern is behavioural: completing a task in instance A must not read as
/// completing it in instance B.
fn renumber_keys(records: Vec<J>, ids: &mut HashMap<u64, u64>) -> Vec<J> {
    let mut renumber = |v: &mut J| {
        let J::Object(o) = v else { return };
        for (name, field) in o.iter_mut() {
            // Only `*Key` fields carry runtime identity — a plain number
            // (`retries`, a numeric variable `value`) is data, not a key.
            if !name.ends_with("Key") {
                continue;
            }
            let J::Number(n) = field else { continue };
            let Some(raw) = n.as_u64() else { continue };
            let next = ids.len() as u64 + 1;
            *field = json!(*ids.entry(raw).or_insert(next));
        }
    };
    records
        .into_iter()
        .map(|mut r| {
            renumber(&mut r);
            r
        })
        .collect()
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
    /// Ids of activities carrying `multiInstanceLoopCharacteristics`: their
    /// first activated element instance is the multi-instance BODY, rendered
    /// `MULTI_INSTANCE_BODY` (Zeebe's type), not the activity's type.
    multi_instance_elements: BTreeSet<String>,
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
    // The id of the most recently opened flow node: a
    // `multiInstanceLoopCharacteristics` child belongs to it (the tokenizer
    // yields start tags in document order, and the MI element is always a
    // direct child of its activity).
    let mut last_flow_node: Option<String> = None;
    let mut multi_instance_elements = BTreeSet::new();
    for t in &all {
        let id = t.attrs.get("id").cloned().unwrap_or_default();
        match t.name.as_str() {
            "process" => {
                element_types.insert(id, "PROCESS".to_string());
                last_flow_node = None;
            }
            "multiInstanceLoopCharacteristics" => {
                if let Some(activity) = &last_flow_node {
                    multi_instance_elements.insert(activity.clone());
                }
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
                needs_shape.insert(id.clone());
                last_flow_node = Some(id);
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
        multi_instance_elements,
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
        IncidentKind::ExpressionEvaluation | IncidentKind::ExtractValueError => {
            "EXTRACT_VALUE_ERROR"
        }
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
        "cancelInstance" => ("PROCESS_INSTANCE", "CANCEL"),
        other => panic!("unknown op `{other}`"),
    }
}

/// Runs one scenario on engine-core, rendering nano events as RAW records in
/// Zeebe's record shape (`{recordType, valueType, intent, value}`).
struct NanoRun<'a> {
    sc: &'a Scenario,
    engine: Engine,
    now: u64,
    /// The script's target instance: the instance the most recent SUCCESSFUL
    /// explicit `createInstance` step created (mirroring the Java runner's
    /// `instanceKey = withVars.create()`, which only moves on success).
    /// `updateVariables` aims at it. An engine-triggered root instance (a
    /// message/timer start has no parent either) is NOT adopted: the Java
    /// runner never sees its key, and adopting it would redirect a later
    /// `updateVariables` to the triggered instance while Capture.java keeps
    /// targeting the script-created one.
    instance: Key,
    /// Every instance an explicit `createInstance` step created, in creation
    /// order — Java's initial-target fallback (`if (instanceKey < 0)`) picks
    /// the FIRST root instance the engine reports, which a message/timer
    /// start can produce before any script step runs.
    created: Vec<Key>,
    /// The process id of the script's target instance. Kept in sync with
    /// `instance`; completion/variable labels resolve through `element_ids`
    /// instead, so a spawned child never relabels the parent's records.
    process_id: String,
    element_ids: HashMap<Key, String>,
    /// Element-instance keys of multi-instance BODIES (rendered
    /// `MULTI_INSTANCE_BODY`): a body and its task children share the
    /// activity's BPMN element id, so the type lookup alone cannot tell
    /// them apart.
    mi_bodies: HashMap<Key, Key>,
    /// Each element instance's FLOW SCOPE (the enclosing sub-process element
    /// instance, from `ElementActivating.scope`; absent = the process root).
    /// Zeebe's `flowScopeKey` for an element inside a sub-process is the
    /// sub-process instance, not the process instance (#1335).
    flow_scopes: HashMap<Key, Key>,
    /// The flow scope of the most recent activation of each element id: a
    /// `SequenceFlowTaken` names only its source/target ids, and a flow lives
    /// in its source element's scope.
    scope_by_element_id: HashMap<(Key, String), Key>,
    /// Element instances activated but not yet completed, in activation
    /// order: `(element-instance key, element id, process-instance key)`.
    /// nano's `ProcessInstanceTerminated` carries the whole termination
    /// cascade in one event; Zeebe writes TERMINATING/TERMINATED for every
    /// active element (innermost/latest first) between the process root's
    /// TERMINATING and TERMINATED. These are those elements.
    active_elements: Vec<(Key, String, Key)>,
    /// Instances whose root TERMINATING was already rendered (a deferred
    /// cancel with `canceling` listeners emits `ProcessInstanceTerminating`).
    terminating: HashSet<Key>,
    jobs_by_type: HashMap<String, Key>,
    jobs: HashMap<Key, (String, String, i64)>,
    /// job key -> (process-instance key, element-instance key).
    job_scopes: HashMap<Key, (Key, Key)>,
    user_tasks: HashMap<String, Key>,
    user_task_elements: HashMap<Key, String>,
    /// user-task key -> (process-instance key, element-instance key).
    user_task_scopes: HashMap<Key, (Key, Key)>,
    incidents: HashMap<String, Key>,
    incident_info: HashMap<Key, (String, &'static str)>,
    /// incident key -> (process-instance key, element-instance key).
    incident_scopes: HashMap<Key, (Key, Key)>,
    timers: HashMap<Key, String>,
    /// timer key -> (process-instance key, target element-instance key).
    timer_scopes: HashMap<Key, (Key, Key)>,
    subscriptions: HashMap<Key, (String, String)>,
    /// subscription key -> (process-instance key, element-instance key).
    subscription_scopes: HashMap<Key, (Key, Key)>,
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
            created: Vec::new(),
            process_id: String::new(),
            element_ids: HashMap::new(),
            mi_bodies: HashMap::new(),
            flow_scopes: HashMap::new(),
            scope_by_element_id: HashMap::new(),
            active_elements: Vec::new(),
            terminating: HashSet::new(),
            jobs_by_type: HashMap::new(),
            jobs: HashMap::new(),
            job_scopes: HashMap::new(),
            user_tasks: HashMap::new(),
            user_task_elements: HashMap::new(),
            user_task_scopes: HashMap::new(),
            incidents: HashMap::new(),
            incident_info: HashMap::new(),
            incident_scopes: HashMap::new(),
            timers: HashMap::new(),
            timer_scopes: HashMap::new(),
            subscriptions: HashMap::new(),
            subscription_scopes: HashMap::new(),
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
            "updateVariables" => {
                // `scopeElementId` targets the newest instance of that element
                // (Capture.java `latestElementInstance`); default: the instance.
                let scope = match step["scopeElementId"].as_str() {
                    Some(id) => self
                        .element_ids
                        .iter()
                        .filter(|(_, e)| e.as_str() == id)
                        .map(|(k, _)| *k)
                        .max()
                        .unwrap_or_else(|| panic!("no element instance of '{id}' yet")),
                    None => self.instance,
                };
                self.apply(Command::set_variables(scope, vars))
            }
            "cancelInstance" => self.apply(Command::cancel_instance(self.instance)),
            other => panic!("{}: unknown op `{other}`", self.sc.name),
        };
        match result {
            Ok(events) => {
                if op == "createInstance" {
                    // The script's target moves ONLY on a successful explicit
                    // creation (Java: `instanceKey = withVars.create()` — a
                    // rejected create leaves it, and an engine-triggered
                    // message/timer-start instance never reaches it).
                    if let Some(key) = events.iter().find_map(|e| match e {
                        Event::ProcessInstanceCreated {
                            instance_key,
                            parent_process_instance_key: None,
                            ..
                        } => Some(*instance_key),
                        _ => None,
                    }) {
                        self.instance = key;
                        self.created.push(key);
                        if let Some(pid) = self.element_ids.get(&key).cloned() {
                            self.process_id = pid;
                        }
                    }
                }
                let mut records: Vec<J> = events.iter().flat_map(|e| self.render(e)).collect();
                records.extend(join_activation_rejections(&events));
                records
            }
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

    /// The Zeebe `BpmnElementType` of the element running as
    /// `element_instance_key`. A multi-instance BODY shares its activity's
    /// BPMN element id with the task children, so the id alone cannot label
    /// it: keys recorded in `mi_bodies` render as `MULTI_INSTANCE_BODY`.
    fn element_type_of(&self, element_instance_key: Key, id: &str) -> String {
        if self.mi_bodies.contains_key(&element_instance_key) {
            return "MULTI_INSTANCE_BODY".to_string();
        }
        self.sc
            .element_types
            .get(id)
            .cloned()
            .unwrap_or_else(|| format!("?{id}"))
    }

    fn lifecycle(&self, intent: &str, id: &str, element_instance_key: Key, instance: Key) -> J {
        event(
            "PROCESS_INSTANCE",
            intent,
            json!({
                "elementId": id,
                "bpmnElementType": self.element_type_of(element_instance_key, id),
                "processInstanceKey": instance,
                // Zeebe's process-root record has no flow scope: -1.
                "flowScopeKey": if element_instance_key == instance {
                    -1
                } else {
                    self.flow_scopes
                        .get(&element_instance_key)
                        .copied()
                        .unwrap_or(instance) as i64
                },
            }),
        )
    }

    /// VARIABLE records for one scope. `scope_key` decides CREATED vs UPDATED
    /// (per runtime scope); `scope_label` is the BPMN element id the golden
    /// compares, mirroring how Zeebe's capture labels `scopeElementId`.
    fn variable_records(
        &mut self,
        scope_key: Key,
        scope_label: String,
        vars: &HashMap<String, Value>,
        instance: Key,
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
                    json!({
                        "name": name,
                        "value": to_json(v),
                        "scopeElementId": scope_label,
                        "processInstanceKey": instance,
                        "scopeKey": scope_key,
                    }),
                )
            })
            .collect()
    }

    fn job_value(&self, key: Key, retries: Option<i64>) -> J {
        let (el, ty, r) = self.jobs.get(&key).cloned().unwrap_or_default();
        let (pik, eik) = self.job_scopes.get(&key).copied().unwrap_or_default();
        json!({
            "elementId": el,
            "type": ty,
            "retries": retries.unwrap_or(r),
            "processInstanceKey": pik,
            "elementInstanceKey": eik,
        })
    }

    /// Render one nano event as zero or more RAW Zeebe-shaped records.
    fn render(&mut self, e: &Event) -> Vec<J> {
        match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                variables,
                ..
            } => {
                self.element_ids.insert(*instance_key, process_id.clone());
                let mut out = vec![
                    self.lifecycle(
                        "ELEMENT_ACTIVATING",
                        process_id,
                        *instance_key,
                        *instance_key,
                    ),
                    self.lifecycle(
                        "ELEMENT_ACTIVATED",
                        process_id,
                        *instance_key,
                        *instance_key,
                    ),
                ];
                out.extend(self.variable_records(
                    *instance_key,
                    process_id.clone(),
                    variables,
                    *instance_key,
                ));
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
                // A top-level terminate end (#985/#1085) completes the process
                // while sibling tokens are still live: nano's journal drops
                // them with the instance (no per-element record), whereas
                // Zeebe writes TERMINATING/TERMINATED for each before the
                // process completes. Derive those records from the tokens
                // still active — exactly as the `ProcessInstanceTerminated`
                // arm does. A normal completion has none left, so it renders
                // unchanged.
                let (doomed, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.active_elements)
                    .into_iter()
                    .partition(|(_, _, pi)| pi == instance_key);
                self.active_elements = kept;
                let mut out = Vec::new();
                for (eik, id, pi) in doomed.into_iter().rev() {
                    out.push(self.lifecycle("ELEMENT_TERMINATING", &id, eik, pi));
                    out.push(self.lifecycle("ELEMENT_TERMINATED", &id, eik, pi));
                }
                out.push(self.lifecycle("ELEMENT_COMPLETING", &pid, *instance_key, *instance_key));
                out.push(self.lifecycle("ELEMENT_COMPLETED", &pid, *instance_key, *instance_key));
                out
            }
            Event::ProcessInstanceTerminating { instance_key } => {
                let pid = self
                    .element_ids
                    .get(instance_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{instance_key}"));
                self.terminating.insert(*instance_key);
                vec![self.lifecycle("ELEMENT_TERMINATING", &pid, *instance_key, *instance_key)]
            }
            Event::ProcessInstanceTerminated { instance_key } => {
                let pid = self
                    .element_ids
                    .get(instance_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{instance_key}"));
                let mut out = Vec::new();
                if self.terminating.insert(*instance_key) {
                    out.push(self.lifecycle(
                        "ELEMENT_TERMINATING",
                        &pid,
                        *instance_key,
                        *instance_key,
                    ));
                }
                let (doomed, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.active_elements)
                    .into_iter()
                    .partition(|(_, _, pi)| pi == instance_key);
                self.active_elements = kept;
                for (eik, id, pi) in doomed.into_iter().rev() {
                    out.push(self.lifecycle("ELEMENT_TERMINATING", &id, eik, pi));
                    out.push(self.lifecycle("ELEMENT_TERMINATED", &id, eik, pi));
                }
                out.push(self.lifecycle("ELEMENT_TERMINATED", &pid, *instance_key, *instance_key));
                out
            }
            Event::ElementActivating {
                instance_key,
                element_instance_key,
                element_id,
                scope,
            } => {
                let flow_scope = if *scope == 0 { *instance_key } else { *scope };
                self.flow_scopes.insert(*element_instance_key, flow_scope);
                self.scope_by_element_id
                    .insert((*instance_key, element_id.to_string()), flow_scope);
                // The FIRST activation of an MI-enabled element id is its body:
                // `activate_multi_instance_body` (engine/mod.rs) mints the body
                // key and emits the body's lifecycle before the fan-out's
                // `MultiInstanceActivated` marker, and the body shares the
                // activity's BPMN element id with its children. Record the body
                // key here so its lifecycle renders `MULTI_INSTANCE_BODY`, not
                // the activity's type.
                if self.sc.multi_instance_elements.contains(element_id)
                    && !self.element_ids.values().any(|known| known == element_id)
                {
                    self.mi_bodies.insert(*element_instance_key, *instance_key);
                }
                self.element_ids
                    .insert(*element_instance_key, element_id.to_string());
                self.active_elements.push((
                    *element_instance_key,
                    element_id.to_string(),
                    *instance_key,
                ));
                vec![self.lifecycle(
                    "ELEMENT_ACTIVATING",
                    element_id,
                    *element_instance_key,
                    *instance_key,
                )]
            }
            Event::ElementActivated {
                instance_key,
                element_instance_key,
                element_id,
                ..
            } => {
                vec![self.lifecycle(
                    "ELEMENT_ACTIVATED",
                    element_id,
                    *element_instance_key,
                    *instance_key,
                )]
            }
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id,
            } => {
                vec![self.lifecycle(
                    "ELEMENT_COMPLETING",
                    element_id,
                    *element_instance_key,
                    *instance_key,
                )]
            }
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id,
            } => {
                self.active_elements
                    .retain(|(eik, _, _)| eik != element_instance_key);
                // The outgoing flow is taken from the instance that just
                // COMPLETED. An MI body and its children share an element id, so
                // a later child activation overwrote the body's entry; refresh it
                // from this completing instance so the body's outgoing flow is
                // labelled with the body's enclosing scope, not the body key.
                if let Some(&scope) = self.flow_scopes.get(element_instance_key) {
                    self.scope_by_element_id
                        .insert((*instance_key, element_id.to_string()), scope);
                }
                vec![self.lifecycle(
                    "ELEMENT_COMPLETED",
                    element_id,
                    *element_instance_key,
                    *instance_key,
                )]
            }
            Event::MultiInstanceActivated {
                instance_key,
                body_key,
                ..
            } => {
                // The authoritative body key (its lifecycle events carry the
                // ACTIVITY's element id, shared with the task children, so the
                // type lookup alone cannot label it). Normally already recorded
                // by the `ElementActivating` arm above; kept as the belt-and-
                // braces record for a listener-deferred fan-out.
                self.mi_bodies.insert(*body_key, *instance_key);
                Vec::new()
            }
            Event::SequenceFlowTaken {
                instance_key,
                from,
                to,
            } => {
                let id = self
                    .sc
                    .flows
                    .get(&(from.to_string(), to.to_string()))
                    .cloned()
                    .unwrap_or_else(|| format!("?{from}->{to}"));
                vec![event(
                    "PROCESS_INSTANCE",
                    "SEQUENCE_FLOW_TAKEN",
                    json!({
                        "elementId": id,
                        "bpmnElementType": "SEQUENCE_FLOW",
                        "processInstanceKey": instance_key,
                        "flowScopeKey": self
                            .scope_by_element_id
                            .get(&(*instance_key, from.to_string()))
                            .copied()
                            .unwrap_or(*instance_key),
                    }),
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
                self.variable_records(*instance_key, pid, variables, *instance_key)
            }
            // A multi-instance child's scope bindings (`inputElement`,
            // `loopCounter`) are Zeebe VARIABLE CREATED records on the child.
            Event::MultiInstanceChildActivated {
                instance_key,
                child_key,
                local_variables,
                ..
            } => {
                let scope = self
                    .element_ids
                    .get(child_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{child_key}"));
                self.variable_records(*child_key, scope, local_variables, *instance_key)
            }
            Event::ScopedVariablesUpdated {
                instance_key,
                scope_key,
                variables,
                ..
            } => {
                let scope = self
                    .element_ids
                    .get(scope_key)
                    .cloned()
                    .unwrap_or_else(|| format!("?{scope_key}"));
                self.variable_records(*scope_key, scope, variables, *instance_key)
            }
            // An execution-listener job (ADR 0037) is an ordinary Zeebe JOB
            // record (`jobKind = EXECUTION_LISTENER`, not projected), so it
            // renders exactly like a task job and is completed by type (#985).
            Event::JobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id,
                job_type,
                retries,
                ..
            }
            | Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key,
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
                self.job_scopes
                    .insert(*job_key, (*instance_key, *element_instance_key));
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
                instance_key,
                element_instance_key,
                element_id,
                ..
            } => {
                self.user_tasks
                    .insert(element_id.to_string(), *user_task_key);
                self.user_task_elements
                    .insert(*user_task_key, element_id.to_string());
                self.user_task_scopes
                    .insert(*user_task_key, (*instance_key, *element_instance_key));
                let (pik, eik) = (*instance_key, *element_instance_key);
                vec![event(
                    "USER_TASK",
                    "CREATED",
                    json!({
                        "elementId": element_id,
                        "processInstanceKey": pik,
                        "elementInstanceKey": eik,
                    }),
                )]
            }
            Event::UserTaskCompleted { user_task_key, .. } => {
                let el = self.user_task_elements.get(user_task_key).cloned();
                let (pik, eik) = self
                    .user_task_scopes
                    .get(user_task_key)
                    .copied()
                    .unwrap_or_default();
                vec![event(
                    "USER_TASK",
                    "COMPLETED",
                    json!({
                        "elementId": el,
                        "processInstanceKey": pik,
                        "elementInstanceKey": eik,
                    }),
                )]
            }
            Event::UserTaskCanceled { user_task_key, .. } => {
                let el = self.user_task_elements.get(user_task_key).cloned();
                let (pik, eik) = self
                    .user_task_scopes
                    .get(user_task_key)
                    .copied()
                    .unwrap_or_default();
                vec![event(
                    "USER_TASK",
                    "CANCELED",
                    json!({
                        "elementId": el,
                        "processInstanceKey": pik,
                        "elementInstanceKey": eik,
                    }),
                )]
            }
            Event::MessageSubscriptionCreated {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                kind,
                ..
            } => {
                // Zeebe's PROCESS_MESSAGE_SUBSCRIPTION names the CATCHING
                // element: for a boundary that is the boundary event, while
                // nano keys the subscription by the attached activity.
                let catching = match kind {
                    MessageSubscriptionKind::InterruptingBoundary {
                        boundary_element_id,
                    }
                    | MessageSubscriptionKind::NonInterruptingBoundary {
                        boundary_element_id,
                    } => boundary_element_id.to_string(),
                    _ => element_id.to_string(),
                };
                let v = (catching, message_name.clone());
                self.subscriptions.insert(*subscription_key, v.clone());
                self.subscription_scopes
                    .insert(*subscription_key, (*instance_key, *element_instance_key));
                vec![event(
                    "PROCESS_MESSAGE_SUBSCRIPTION",
                    "CREATED",
                    json!({
                        "elementId": v.0,
                        "messageName": v.1,
                        "processInstanceKey": instance_key,
                        "elementInstanceKey": element_instance_key,
                    }),
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
                let (pik, eik) = self
                    .subscription_scopes
                    .get(subscription_key)
                    .copied()
                    .unwrap_or_default();
                vec![event(
                    "PROCESS_MESSAGE_SUBSCRIPTION",
                    "CORRELATED",
                    json!({
                        "elementId": el,
                        "messageName": name,
                        "processInstanceKey": pik,
                        "elementInstanceKey": eik,
                    }),
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
                let (pik, eik) = self
                    .subscription_scopes
                    .get(subscription_key)
                    .copied()
                    .unwrap_or_default();
                vec![event(
                    "PROCESS_MESSAGE_SUBSCRIPTION",
                    "DELETED",
                    json!({
                        "elementId": el,
                        "messageName": name,
                        "processInstanceKey": pik,
                        "elementInstanceKey": eik,
                    }),
                )]
            }
            Event::TimerCreated {
                timer_key,
                instance_key,
                element_instance_key,
                element_id,
                ..
            } => {
                self.timers.insert(*timer_key, element_id.to_string());
                self.timer_scopes
                    .insert(*timer_key, (*instance_key, *element_instance_key));
                vec![event(
                    "TIMER",
                    "CREATED",
                    json!({
                        "targetElementId": element_id,
                        "processInstanceKey": instance_key,
                        "elementInstanceKey": element_instance_key,
                    }),
                )]
            }
            Event::TimerTriggered {
                timer_key,
                instance_key,
                element_instance_key,
                element_id,
                ..
            } => {
                self.timer_scopes
                    .insert(*timer_key, (*instance_key, *element_instance_key));
                vec![event(
                    "TIMER",
                    "TRIGGERED",
                    json!({
                        "targetElementId": element_id,
                        "processInstanceKey": instance_key,
                        "elementInstanceKey": element_instance_key,
                    }),
                )]
            }
            Event::TimerCanceled {
                timer_key,
                instance_key,
                element_instance_key,
                element_id,
                ..
            } => {
                self.timer_scopes
                    .insert(*timer_key, (*instance_key, *element_instance_key));
                vec![event(
                    "TIMER",
                    "CANCELED",
                    json!({
                        "targetElementId": element_id,
                        "processInstanceKey": instance_key,
                        "elementInstanceKey": element_instance_key,
                    }),
                )]
            }
            Event::IncidentRaised {
                incident_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                ..
            } => {
                let info = (element_id.to_string(), zeebe_error_type(*kind));
                self.incidents.insert(info.0.clone(), *incident_key);
                self.incident_info.insert(*incident_key, info.clone());
                self.incident_scopes
                    .insert(*incident_key, (*instance_key, *element_instance_key));
                vec![event(
                    "INCIDENT",
                    "CREATED",
                    json!({
                        "elementId": info.0,
                        "errorType": info.1,
                        "processInstanceKey": instance_key,
                        "elementInstanceKey": element_instance_key,
                    }),
                )]
            }
            Event::IncidentResolved { incident_key, .. } => {
                let (el, ty) = self
                    .incident_info
                    .get(incident_key)
                    .cloned()
                    .unwrap_or_default();
                let (pik, eik) = self
                    .incident_scopes
                    .get(incident_key)
                    .copied()
                    .unwrap_or_default();
                vec![event(
                    "INCIDENT",
                    "RESOLVED",
                    json!({
                        "elementId": el,
                        "errorType": ty,
                        "processInstanceKey": pik,
                        "elementInstanceKey": eik,
                    }),
                )]
            }
            // Everything else has no counterpart in the normaliser's lanes.
            _ => Vec::new(),
        }
    }
}

/// Run a scenario on engine-core and normalise it: the comparable trace. The
/// canonical-key map is kept across the scenario's steps (spec `doc` step 5).
fn nano_trace(spec: &J, sc: &Scenario) -> Vec<J> {
    let mut run = NanoRun::new(sc);
    let mut ids = HashMap::new();
    sc.script["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .enumerate()
        .map(|(i, step)| {
            let raw = run.step(step);
            json!({"step": i, "op": step["op"], "records": normalise(spec, &raw, &mut ids)})
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

/// The `ACTIVATE_ELEMENT` rejections Zeebe writes at joins during one command.
/// Zeebe takes a flow into a join by writing an `ACTIVATE_ELEMENT` command, and
/// `ProcessInstanceStateTransitionGuard` (`canActivateParallelGateway` /
/// `canActivateInclusiveGateway`) rejects that command with `INVALID_STATE`
/// while the join is not ready. The arrival that makes the join ready is
/// accepted and activates it. Nano records every arrival
/// (`ParallelJoinTokenArrived`) and every firing (`ParallelJoinFired`), so the
/// rejections are the arrivals that did not fire a join (#1244).
fn join_activation_rejections(events: &[Event]) -> Vec<J> {
    let arrivals = events
        .iter()
        .filter(|e| matches!(e, Event::ParallelJoinTokenArrived { .. }))
        .count();
    let fires = events
        .iter()
        .filter(|e| matches!(e, Event::ParallelJoinFired { .. }))
        .count();
    (0..arrivals.saturating_sub(fires))
        .map(|_| {
            json!({
                "recordType": "COMMAND_REJECTION",
                "valueType": "PROCESS_INSTANCE",
                "intent": "ACTIVATE_ELEMENT",
                "rejectionType": "INVALID_STATE",
                "value": {},
            })
        })
        .collect()
}

/// A step whose `expectRejection` flag does not match the rejection outcome
/// the golden itself recorded. Without this gate, editing a script's
/// `expectRejection` without recapturing leaves command and trace unchanged,
/// so the differential would pass a successful creation the script now
/// requires to be rejected (the `expectRejection` divergence stays invisible).
fn rejection_expectation_mismatch(sc: &Scenario, golden: &[J]) -> Option<String> {
    for (i, step) in sc.script["steps"].as_array()?.iter().enumerate() {
        let Some(expected) = step["expectRejection"].as_bool() else {
            continue;
        };
        let Some(g) = golden.get(i) else { break };
        let actual = g["records"]
            .as_array()
            .is_some_and(|rs| rs.iter().any(|r| r["recordType"] == "COMMAND_REJECTION"));
        if expected != actual {
            return Some(format!(
                "step {i} ({}): script.json expects expectRejection={expected} but the golden \
                 recorded {} — recapture the golden (formal/traces/capture.sh) after changing \
                 the expectation",
                step["op"].as_str().unwrap_or("?"),
                if actual {
                    "a rejection"
                } else {
                    "no rejection"
                },
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

/// Does `record` match `matcher` (every matcher field present and equal)? A
/// field ABSENT from the record never matches — indexing a missing field
/// yields `Null`, so without the presence check a matcher such as
/// `{"elementID": null}` would match every record and silently widen into a
/// whole-scenario exemption.
fn record_matches(record: &J, matcher: &J) -> bool {
    matcher.as_object().is_some_and(|m| {
        !m.is_empty()
            && m.iter()
                .all(|(k, v)| record.as_object().is_some_and(|r| r.get(k) == Some(v)))
    })
}

/// `steps` with every record matching one of `ignore` removed.
fn mask_records(steps: &[J], ignore: &[J]) -> Vec<J> {
    steps
        .iter()
        .map(|step| {
            let mut step = step.clone();
            if let Some(rs) = step["records"].as_array_mut() {
                rs.retain(|r| !ignore.iter().any(|m| record_matches(r, m)));
            }
            step
        })
        .collect()
}

/// Narrow allow-listing: an entry may carry `ignoreRecords` (record-field
/// matchers). Its scenario is then allowed to diverge ONLY in records those
/// matchers select — with them masked out of both sides, every step must
/// still equal Zeebe exactly. Returns one error per residual divergence.
fn masked_residuals(
    allowlist: &J,
    traces: &BTreeMap<String, (Vec<J>, Option<Vec<J>>)>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for entry in allowlist["entries"].as_array().expect("allowlist.entries") {
        let scenario = entry["scenario"].as_str().unwrap_or_default();
        let ignore = match entry.get("ignoreRecords") {
            None => continue,
            Some(J::Array(ignore)) => ignore,
            Some(_) => {
                errors.push(format!(
                    "allow-list entry `{scenario}`: ignoreRecords must be an array of matchers"
                ));
                continue;
            }
        };
        if ignore
            .iter()
            .any(|m| m.as_object().is_none_or(|o| o.is_empty()))
        {
            errors.push(format!(
                "allow-list entry `{scenario}`: every ignoreRecords matcher must be a non-empty object"
            ));
            continue;
        }
        match traces.get(scenario) {
            Some((golden, Some(nano))) => {
                if let Some(d) = diff(&mask_records(golden, ignore), &mask_records(nano, ignore)) {
                    errors.push(format!(
                        "scenario `{scenario}` diverges from Zeebe beyond its allow-listed \
                         ignoreRecords:\n{d}"
                    ));
                }
            }
            // Record masking only narrows a divergence in a successfully
            // replayed trace; it never exempts a scenario nano could not run.
            _ => errors.push(format!(
                "scenario `{scenario}` has ignoreRecords but no nano trace to compare \
                 (replay failed, rejection mismatch, or unknown scenario)"
            )),
        }
    }
    errors
}

fn load_spec() -> J {
    read_json(&traces_dir().join("normaliser.json"))
}

/// Run every scenario: `name -> Some(diff)` when nano diverges from the golden.
fn run_corpus(spec: &J) -> BTreeMap<String, Option<String>> {
    run_corpus_traces(spec)
        .into_iter()
        .map(|(n, (d, _))| (n, d))
        .collect()
}

/// Every scenario's verdict plus its (golden, nano) traces; the nano trace is
/// `None` when nano could not replay the script.
#[allow(clippy::type_complexity)]
fn run_corpus_traces(spec: &J) -> BTreeMap<String, (Option<String>, (Vec<J>, Option<Vec<J>>))> {
    let goldens = traces_dir().join("goldens");
    scenario_dirs()
        .iter()
        .map(|dir| {
            let sc = load_scenario(dir).unwrap_or_else(|e| panic!("{e}"));
            let golden = read_json(&goldens.join(format!("{}.trace.json", sc.name)));
            let steps = golden["steps"].as_array().expect("golden steps").clone();
            let mut nano = None;
            let d = rejection_expectation_mismatch(&sc, &steps).or_else(|| {
                // A script nano cannot even replay (e.g. it resolves an
                // incident nano never raised) is a divergence, not a crash of
                // the whole gate: report it per scenario.
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    nano_trace(spec, &sc)
                })) {
                    Ok(trace) => {
                        let d = diff(&steps, &trace);
                        nano = Some(trace);
                        d
                    }
                    Err(p) => Some(format!(
                        "nano could not replay the script: {}",
                        p.downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| p.downcast_ref::<&str>().copied())
                            .unwrap_or("panic")
                    )),
                }
            });
            (sc.name, (d, (steps, nano)))
        })
        .collect()
}

// ───────────────────────────── the gate ──────────────────────────────

/// THE differential gate: every scenario's nano trace equals its Zeebe golden,
/// modulo the issue-referenced allow-list (which must not be stale).
///
/// `NANO_TRACE_REGEN=1` rewrites the goldens from the nano runner instead —
/// a LOCAL scaffold for spec changes that alter the normalised shape: the
/// capture harness (`formal/traces/capture.sh`, offline against the pinned
/// Zeebe) applies the same spec, so the regenerated golden equals what the
/// next recapture produces. Never set this in CI.
#[test]
fn corpus_matches_zeebe_goldens() {
    let spec = load_spec();
    if std::env::var_os("NANO_TRACE_REGEN").is_some() {
        regen_goldens(&spec);
        return;
    }
    let runs = run_corpus_traces(&spec);
    assert!(!runs.is_empty(), "the trace corpus is empty");
    let allowlist = read_json(&traces_dir().join("allowlist.json"));
    let outcomes = runs
        .iter()
        .map(|(n, (d, _))| (n.clone(), d.clone()))
        .collect();
    let traces = runs.into_iter().map(|(n, (_, t))| (n, t)).collect();
    let mut errors = verdict(&outcomes, &allowlist).err().unwrap_or_default();
    errors.extend(masked_residuals(&allowlist, &traces));
    if !errors.is_empty() {
        panic!(
            "Zeebe differential traces failed:\n\n{}\n\nSee formal/traces/README.md.",
            errors.join("\n\n")
        );
    }
}

/// Harvested-corpus counts (#1391 step 2), printed in CI by the engine-core
/// job's "Zeebe trace corpus report" step (`-- --nocapture`).
///
/// * harvested — Zeebe engine tests turned into `h-*` scenarios
///   (`formal/traces/harvest`), each with DI and a captured golden;
/// * runnable — harvested scenarios nano replays identically to Zeebe;
/// * diverging — harvested scenarios on the allow-list (issue-referenced);
/// * out-of-scope — harvested tests that could not become a scenario, with
///   the reason in `harvest/out-of-scope.json`.
#[derive(Debug, PartialEq)]
struct HarvestCounts {
    harvested: usize,
    runnable: usize,
    diverging: usize,
    out_of_scope: usize,
}

fn harvest_counts(outcomes: &BTreeMap<String, Option<String>>, summary: &J) -> HarvestCounts {
    let harvested: Vec<_> = outcomes
        .iter()
        .filter(|(n, _)| n.starts_with(HARVEST_PREFIX))
        .collect();
    let diverging = harvested.iter().filter(|(_, d)| d.is_some()).count();
    HarvestCounts {
        harvested: harvested.len(),
        runnable: harvested.len() - diverging,
        diverging,
        out_of_scope: summary["outOfScope"].as_u64().expect("summary.outOfScope") as usize,
    }
}

/// Prefix of harvested scenario directories (`Harvest.PREFIX` in the capture
/// harness).
const HARVEST_PREFIX: &str = "h-";

#[test]
fn corpus_report() {
    let spec = load_spec();
    let outcomes = run_corpus(&spec);
    let summary = read_json(&traces_dir().join("harvest/summary.json"));
    let c = harvest_counts(&outcomes, &summary);
    let oos = read_json(&traces_dir().join("harvest/out-of-scope.json"));
    assert_eq!(
        oos.as_object().expect("out-of-scope.json").len(),
        c.out_of_scope,
        "harvest/summary.json and out-of-scope.json disagree — rerun capture.sh harvest"
    );
    assert_eq!(
        summary["harvested"].as_u64(),
        Some(c.harvested as u64),
        "harvest/summary.json does not match the committed h-* scenarios — rerun capture.sh harvest"
    );
    println!(
        "Zeebe harvested corpus: harvested={} runnable={} diverging={} out-of-scope={} \
         (hand-written scenarios: {})",
        c.harvested,
        c.runnable,
        c.diverging,
        c.out_of_scope,
        outcomes.len() - c.harvested
    );
}

#[test]
fn harvest_counts_split_runnable_and_diverging() {
    let outcomes: BTreeMap<String, Option<String>> = [
        ("h-A-ok".to_string(), None),
        ("h-A-bad".to_string(), Some("diff".to_string())),
        ("hand".to_string(), Some("diff".to_string())),
    ]
    .into_iter()
    .collect();
    let c = harvest_counts(&outcomes, &json!({"outOfScope": 4}));
    assert_eq!(
        c,
        HarvestCounts {
            harvested: 2,
            runnable: 1,
            diverging: 1,
            out_of_scope: 4
        }
    );
}

/// Rewrite every golden from the nano runner (see `corpus_matches_zeebe_goldens`).
fn regen_goldens(spec: &J) {
    let pin = fs::read_to_string(traces_dir().join("goldens/PIN"))
        .expect("goldens/PIN")
        .trim()
        .to_string();
    for dir in scenario_dirs() {
        let sc = load_scenario(&dir).unwrap_or_else(|e| panic!("{e}"));
        let golden = json!({
            "scenario": sc.name,
            "zeebePin": pin,
            "steps": nano_trace(spec, &sc),
        });
        let path = traces_dir()
            .join("goldens")
            .join(format!("{}.trace.json", sc.name));
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&golden).unwrap()),
        )
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        eprintln!("regenerated {}", path.display());
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
            json!({"elementId": "t", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 9}),
        ),
        json!({"recordType": "COMMAND", "valueType": "JOB", "intent": "COMPLETE", "value": {}}),
        raw_event("JOB", "TIMED_OUT", json!({})),
        raw_event("JOB_BATCH", "ACTIVATED", json!({})),
        json!({"recordType": "COMMAND_REJECTION", "valueType": "JOB", "intent": "COMPLETE", "rejectionType": "NOT_FOUND", "rejectionReason": "x", "value": {}}),
        raw_event(
            "PROCESS_INSTANCE",
            "ELEMENT_COMPLETED",
            json!({"elementId": "t", "processInstanceKey": 9}),
        ),
    ];
    let mut ids = HashMap::new();
    assert_eq!(
        normalise(&spec, &raw, &mut ids),
        vec![
            json!({"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "t", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 1, "flowScopeKey": null}),
            json!({"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_COMPLETED", "elementId": "t", "bpmnElementType": null, "processInstanceKey": 1, "flowScopeKey": null}),
            json!({"valueType": "JOB", "intent": "CREATED", "elementId": "t", "type": "w", "retries": 3, "processInstanceKey": null, "elementInstanceKey": null}),
            json!({"recordType": "COMMAND_REJECTION", "valueType": "JOB", "intent": "COMPLETE", "rejectionType": "NOT_FOUND"}),
        ]
    );
}

/// Runtime identity survives normalisation, canonically renumbered: raw key
/// VALUES are engine-specific, but the identity pattern is behavioural — the
/// first distinct key the scenario touches becomes 1, the next 2 (spec `doc`
/// step 5). Two instances of one process must read as two distinct identities,
/// and a record landing on instance A must not read as landing on instance B.
#[test]
fn normaliser_renumbers_runtime_keys_canonically() {
    let spec = load_spec();
    let raw = vec![
        raw_event(
            "PROCESS_INSTANCE",
            "ELEMENT_ACTIVATED",
            json!({"elementId": "p", "bpmnElementType": "PROCESS", "processInstanceKey": 2251799813685249_u64, "flowScopeKey": 2251799813685249_u64}),
        ),
        raw_event(
            "PROCESS_INSTANCE",
            "ELEMENT_ACTIVATED",
            json!({"elementId": "p", "bpmnElementType": "PROCESS", "processInstanceKey": 2251799813685254_u64, "flowScopeKey": 2251799813685254_u64}),
        ),
        raw_event(
            "JOB",
            "COMPLETED",
            json!({"elementId": "t", "type": "w", "retries": 3, "processInstanceKey": 2251799813685254_u64, "elementInstanceKey": 2251799813685260_u64}),
        ),
    ];
    let mut ids = HashMap::new();
    assert_eq!(
        normalise(&spec, &raw, &mut ids),
        vec![
            json!({"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "p", "bpmnElementType": "PROCESS", "processInstanceKey": 1, "flowScopeKey": 1}),
            json!({"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "p", "bpmnElementType": "PROCESS", "processInstanceKey": 2, "flowScopeKey": 2}),
            json!({"valueType": "JOB", "intent": "COMPLETED", "elementId": "t", "type": "w", "retries": 3, "processInstanceKey": 2, "elementInstanceKey": 3}),
        ]
    );
    // Swapping the target instance swaps the canonical id: completing in the
    // FIRST instance is observably different from completing in the SECOND.
    let mut swapped = raw.clone();
    swapped[2]["value"]["processInstanceKey"] = json!(2251799813685249_u64);
    let out = normalise(&spec, &swapped, &mut HashMap::new());
    assert_eq!(out[2]["processInstanceKey"], json!(1), "{out:?}");
    assert_ne!(
        out,
        normalise(&spec, &raw, &mut HashMap::new()),
        "the swap must be observable"
    );
    // A plain number is data, not a key: `retries` and a numeric variable
    // `value` pass through unrenumbered.
    let valued = vec![raw_event(
        "VARIABLE",
        "CREATED",
        json!({"name": "n", "value": 2, "scopeElementId": "p", "processInstanceKey": 7, "scopeKey": 7}),
    )];
    let out = normalise(&spec, &valued, &mut HashMap::new());
    assert_eq!(out[0]["value"], json!(2), "{out:?}");
    assert_eq!(out[0]["scopeKey"], json!(1), "{out:?}");
    // Canonical ids are assigned in SORTED field-name order (spec `doc` step
    // 5), so both normalisers agree regardless of their JSON object iteration
    // order. Here the instance and its flow scope are the SAME raw key, and
    // the element instance another. The JOB lane lists processInstanceKey
    // BEFORE elementInstanceKey, so an insertion-order visitor would assign
    // processInstanceKey=1, elementInstanceKey=2 — but sorted order visits
    // elementInstanceKey first, assigning elementInstanceKey=1,
    // processInstanceKey=2. A recapture with the wrong order diverges without
    // any engine behaviour change.
    let shared = vec![raw_event(
        "JOB",
        "FAILED",
        json!({"elementId": "t", "type": "w", "retries": 0, "processInstanceKey": 100_u64, "elementInstanceKey": 200_u64, "flowScopeKey": 100_u64}),
    )];
    let out = normalise(&spec, &shared, &mut HashMap::new());
    assert_eq!(out[0]["flowScopeKey"], J::Null, "not a JOB lane field");
    assert_eq!(out[0]["elementInstanceKey"], json!(1), "{out:?}");
    assert_eq!(out[0]["processInstanceKey"], json!(2), "{out:?}");
    let pi = vec![raw_event(
        "PROCESS_INSTANCE",
        "ELEMENT_ACTIVATING",
        json!({"elementId": "t", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 100_u64, "flowScopeKey": 100_u64}),
    )];
    let out = normalise(&spec, &pi, &mut HashMap::new());
    assert_eq!(out[0]["flowScopeKey"], json!(1), "{out:?}");
    assert_eq!(out[0]["processInstanceKey"], json!(1), "{out:?}");
}

/// The canonical-key map is kept for the WHOLE scenario (spec `doc` step 5):
/// a step re-touching an earlier step's instance renders its established
/// canonical id. With a per-step reset, updating either of two same-process
/// instances would renumber to the same step-local `{processInstanceKey: 1,
/// scopeKey: 1}` and the target swap would be invisible.
#[test]
fn normaliser_keeps_canonical_keys_across_steps() {
    let spec = load_spec();
    let created = |key: u64| {
        vec![raw_event(
            "PROCESS_INSTANCE",
            "ELEMENT_ACTIVATED",
            json!({"elementId": "p", "bpmnElementType": "PROCESS", "processInstanceKey": key, "flowScopeKey": key}),
        )]
    };
    let updated = |key: u64| {
        vec![raw_event(
            "VARIABLE",
            "UPDATED",
            json!({"name": "x", "value": 1, "scopeElementId": "p", "processInstanceKey": key, "scopeKey": key}),
        )]
    };
    let (ka, kb) = (2251799813685249_u64, 2251799813685254_u64);
    let mut ids = HashMap::new();
    normalise(&spec, &created(ka), &mut ids);
    normalise(&spec, &created(kb), &mut ids);
    let update_b = normalise(&spec, &updated(kb), &mut ids);
    assert_eq!(update_b[0]["processInstanceKey"], json!(2), "{update_b:?}");
    assert_eq!(update_b[0]["scopeKey"], json!(2), "{update_b:?}");
    // Updating A instead must read differently from updating B — the swap is
    // observable only because the map survives the step boundary.
    let mut ids = HashMap::new();
    normalise(&spec, &created(ka), &mut ids);
    normalise(&spec, &created(kb), &mut ids);
    let update_a = normalise(&spec, &updated(ka), &mut ids);
    assert_eq!(update_a[0]["processInstanceKey"], json!(1), "{update_a:?}");
    assert_ne!(update_a, update_b, "the cross-step swap must be observable");
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
        normalise(&spec, &[pi.clone(), job.clone()], &mut HashMap::new()),
        normalise(&spec, &[job, pi], &mut HashMap::new())
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

/// An `ignoreRecords` allow-list entry tolerates ONLY the matched records: a
/// divergence elsewhere in the scenario still fails the gate.
#[test]
fn ignore_records_masks_only_the_matched_records() {
    let (spec, sc, mut golden) = first_scenario();
    let nano = nano_trace(&spec, &sc);
    let job = golden[0]["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|r| r["valueType"] == "JOB")
        .unwrap();
    job["retries"] = json!(4);
    let traces = BTreeMap::from([(sc.name.clone(), (golden.clone(), Some(nano.clone())))]);
    let entry =
        |m: J| json!({"entries": [{"scenario": sc.name, "issue": "#1424", "ignoreRecords": m}]});
    assert!(masked_residuals(&entry(json!([{"valueType": "JOB"}])), &traces).is_empty());
    let err = masked_residuals(&entry(json!([{"valueType": "VARIABLE"}])), &traces);
    assert!(err[0].contains("beyond its allow-listed"), "{err:?}");
    let err = masked_residuals(&entry(json!([{}])), &traces);
    assert!(err[0].contains("non-empty object"), "{err:?}");
    // A present-but-malformed mask must not widen into a whole-scenario exemption.
    let err = masked_residuals(&entry(json!({"valueType": "JOB"})), &traces);
    assert!(err[0].contains("must be an array"), "{err:?}");
    // Masking never exempts a scenario nano failed to replay.
    let failed = BTreeMap::from([(sc.name.clone(), (golden, None))]);
    let err = masked_residuals(&entry(json!([{"valueType": "JOB"}])), &failed);
    assert!(err[0].contains("no nano trace"), "{err:?}");
    // A matcher field ABSENT from a record never matches it: indexing a missing
    // field yields Null, so a `{"noSuchField": null}` matcher must not widen
    // into a whole-scenario exemption that masks every record.
    let err = masked_residuals(&entry(json!([{"noSuchField": null}])), &traces);
    assert!(err[0].contains("beyond its allow-listed"), "{err:?}");
}

/// A null-valued matcher field matches only a record that CARRIES the field
/// with value null — never a record where the field is absent (indexing a
/// missing field yields `Null`, which used to match and silently widen the
/// exemption to every record).
#[test]
fn ignore_records_null_matcher_requires_the_field_to_exist() {
    let step = |v: J| {
        json!({"op": "cancelJob", "records": [
            {"valueType": "JOB", "intent": "CANCELED", "elementId": "task", "marker": "m", "retries": v}
        ], "step": 0})
    };
    let traces = BTreeMap::from([(
        "s".to_string(),
        (
            vec![step(json!(null)), step(json!(null))],
            Some(vec![step(json!(42)), step(json!(null))]),
        ),
    )]);
    let entry =
        |m: J| json!({"entries": [{"scenario": "s", "issue": "#1424", "ignoreRecords": m}]});
    // Masking both records by their shared fields leaves no residual...
    let err = masked_residuals(
        &entry(json!([{"valueType": "JOB", "marker": "m"}])),
        &traces,
    );
    assert!(err.is_empty(), "{err:?}");
    // ... and the null matcher masks ONLY the golden record, whose `retries`
    // field is present and null — nano's `retries: 42` survives and the
    // divergence is reported.
    let err = masked_residuals(
        &entry(json!([{"valueType": "JOB", "marker": "m", "retries": null}])),
        &traces,
    );
    assert_eq!(err.len(), 1, "{err:?}");
    assert!(err[0].contains("beyond its allow-listed"), "{err:?}");
    // A null matcher on a field NEITHER record carries masks nothing at all:
    // indexing a missing field yields `Null`, which must NOT satisfy the
    // matcher (that would silently widen the exemption to every record).
    let err = masked_residuals(
        &entry(json!([{"valueType": "JOB", "marker": "m", "noSuchField": null}])),
        &traces,
    );
    assert_eq!(err.len(), 1, "{err:?}");
    assert!(err[0].contains("beyond its allow-listed"), "{err:?}");
}

/// Editing a script's `expectRejection` without recapturing must fail the
/// gate: the golden itself records whether the step's command was rejected,
/// so a stale golden cannot silently satisfy a changed expectation.
#[test]
fn expect_rejection_is_checked_against_the_golden() {
    let (_spec, sc, golden) = first_scenario();
    assert_eq!(
        rejection_expectation_mismatch(&sc, &golden),
        None,
        "precondition: the committed script matches its golden"
    );
    // Flip the rejection step's expectation to `false`: the golden recorded a
    // COMMAND_REJECTION, so the mismatch must be reported even though neither
    // the nano command nor the comparable trace changed.
    let mut flipped = sc.script.clone();
    flipped["steps"][2]["expectRejection"] = json!(false);
    let flipped_sc = Scenario {
        script: flipped,
        ..load_scenario(&traces_dir().join("scenarios/service-task-complete")).unwrap()
    };
    let m = rejection_expectation_mismatch(&flipped_sc, &golden)
        .expect("a flipped expectation must be caught");
    assert!(m.contains("step 2"), "{m}");
    assert!(m.contains("expectRejection=false"), "{m}");
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
    // ... and identity distinguishes the two instances: A's `paid` CREATED
    // record carries A's canonical process-instance key, not B's.
    let a_key = trace[0]["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["valueType"] == "PROCESS_INSTANCE" && r["intent"] == "ELEMENT_ACTIVATED")
        .map(|r| r["processInstanceKey"].clone())
        .unwrap();
    assert_eq!(
        vars[0]["processInstanceKey"], a_key,
        "the correlated update lands on instance A: {step}"
    );
}

/// A multi-instance BODY shares its activity's BPMN element id with the task
/// children, so the type lookup alone cannot label it: the body's lifecycle
/// must render `MULTI_INSTANCE_BODY` (Zeebe's `BpmnElementType`), while the
/// children render the activity's own type.
#[test]
fn multi_instance_body_renders_its_own_lifecycle_type() {
    let trace = run_scratch(
        "mibody",
        MI_SERVICE_TASK_XML,
        r#"{"description": "multi-instance body", "steps": [
            {"op": "createInstance", "bpmnProcessId": "mi", "variables": {"items": [1, 2, 3]}},
            {"op": "completeJob", "jobType": "handle"},
            {"op": "completeJob", "jobType": "handle"},
            {"op": "completeJob", "jobType": "handle"}
        ]}"#,
    );
    let step = &trace[0];
    let activating = records_of(step, "PROCESS_INSTANCE", "ELEMENT_ACTIVATING");
    let bodies: Vec<&J> = activating
        .iter()
        .copied()
        .filter(|r| r["bpmnElementType"] == "MULTI_INSTANCE_BODY")
        .collect();
    assert_eq!(
        bodies.len(),
        1,
        "exactly one MI body activation, rendered MULTI_INSTANCE_BODY: {step}"
    );
    assert_eq!(
        bodies[0]["elementId"], "each",
        "the body carries the activity's id: {step}"
    );
    let children: Vec<&J> = activating
        .iter()
        .copied()
        .filter(|r| r["elementId"] == "each" && r["bpmnElementType"] == "SERVICE_TASK")
        .collect();
    assert_eq!(
        children.len(),
        3,
        "the three children render the activity's own type: {step}"
    );
    // Identity distinguishes the body from its children: each child's job is
    // created on its own element instance.
    let jobs = records_of(step, "JOB", "CREATED");
    assert_eq!(jobs.len(), 3, "one job per child: {step}");
    let child_keys: BTreeSet<String> = jobs
        .iter()
        .map(|r| r["elementInstanceKey"].to_string())
        .collect();
    assert_eq!(
        child_keys.len(),
        3,
        "each child is its own instance: {step}"
    );
}

/// An engine-triggered root instance (a message start has no parent either)
/// must NOT become the script's target: after explicitly creating A and then
/// triggering B, `updateVariables` still targets A — Capture.java only ever
/// learns the key its create command returned.
#[test]
fn message_start_instance_does_not_steal_the_script_target() {
    let trace = run_scratch(
        "msgstart",
        MESSAGE_START_XML,
        r#"{"description": "message start vs script target", "steps": [
            {"op": "createInstance", "bpmnProcessId": "plain", "variables": {"x": 1}},
            {"op": "publishMessage", "name": "boot", "correlationKey": "c"},
            {"op": "updateVariables", "variables": {"x": 2}}
        ]}"#,
    );
    // The message start fired: a `started-by-message` instance activated.
    let started = records_of(&trace[1], "PROCESS_INSTANCE", "ELEMENT_ACTIVATED");
    assert!(
        started
            .iter()
            .any(|r| r["elementId"] == "started-by-message"),
        "the message start must have triggered an instance: {}",
        trace[1]
    );
    // The update still lands on A (the explicitly created instance).
    let step = &trace[2];
    let vars = records_of(step, "VARIABLE", "UPDATED");
    assert_eq!(vars.len(), 1, "A's x must be UPDATED: {step}");
    assert_eq!(
        vars[0]["scopeElementId"], "plain",
        "the update targets the script-created instance A: {step}"
    );
    let a_key = trace[0]["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["valueType"] == "PROCESS_INSTANCE" && r["intent"] == "ELEMENT_ACTIVATED")
        .map(|r| r["processInstanceKey"].clone())
        .unwrap();
    assert_eq!(vars[0]["processInstanceKey"], a_key, "{step}");
}

/// The fallback used to drop `ProcessInstanceTerminating`/`Terminated`: an
/// interrupting boundary message on a call activity terminates its child
/// (engine/boundary.rs), and the child's termination records must appear in
/// the nano trace — labelled with the CHILD's process, the terminating phase
/// exactly once.
#[test]
fn terminated_child_renders_its_termination_records() {
    let trace = run_scratch(
        "terminate",
        CALL_ACTIVITY_BOUNDARY_XML,
        r#"{"description": "interrupting boundary on a call activity", "steps": [
            {"op": "createInstance", "bpmnProcessId": "caller-b"},
            {"op": "publishMessage", "name": "stop", "correlationKey": "c"}
        ]}"#,
    );
    let step = &trace[1];
    let terminated = records_of(step, "PROCESS_INSTANCE", "ELEMENT_TERMINATED");
    let terminating = records_of(step, "PROCESS_INSTANCE", "ELEMENT_TERMINATING");
    let child: Vec<&&J> = terminated
        .iter()
        .filter(|r| r["elementId"] == "child-b")
        .collect();
    assert_eq!(
        child.len(),
        1,
        "the child's ELEMENT_TERMINATED is rendered exactly once, labelled `child-b`: {step}"
    );
    // Zeebe writes the child root's TERMINATING before its TERMINATED even
    // without canceling listeners; the runner renders it exactly once.
    assert_eq!(
        terminating
            .iter()
            .filter(|r| r["elementId"] == "child-b")
            .count(),
        1,
        "the child's ELEMENT_TERMINATING is rendered exactly once: {step}"
    );
    // The child's in-play job is CANCELED inside the CHILD's instance
    // (identity ties the cancellation to the child, not the parent).
    let child_pi = child[0]["processInstanceKey"].clone();
    let canceled = records_of(step, "JOB", "CANCELED");
    assert_eq!(canceled.len(), 1, "the child's job is canceled: {step}");
    assert_eq!(
        canceled[0]["processInstanceKey"], child_pi,
        "the cancellation lands in the CHILD instance: {step}"
    );
    // The parent then leaves via the boundary flow and COMPLETES.
    let completed = records_of(step, "PROCESS_INSTANCE", "ELEMENT_COMPLETED");
    assert!(
        completed.iter().any(|r| r["elementId"] == "caller-b"),
        "the parent completes after the interruption: {step}"
    );
}

/// A service task with parallel multi-instance characteristics over `items`.
const MI_SERVICE_TASK_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_mi" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="mi" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="each">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="handle" retries="3" />
      </bpmn:extensionElements>
      <bpmn:multiInstanceLoopCharacteristics>
        <bpmn:extensionElements>
          <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
        </bpmn:extensionElements>
      </bpmn:multiInstanceLoopCharacteristics>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="each" />
    <bpmn:sequenceFlow id="f2" sourceRef="each" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="mi">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="each_di" bpmnElement="each">
        <dc:Bounds x="250" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="412" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="250" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="350" y="118" />
        <di:waypoint x="412" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// A trivial none-start process (`plain`) deployed alongside a process whose
/// start event is message-triggered (`started-by-message`, message `boot`).
const MESSAGE_START_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_msgstart" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="plain" isExecutable="true">
    <bpmn:startEvent id="pstart" />
    <bpmn:endEvent id="pend" />
    <bpmn:sequenceFlow id="pf1" sourceRef="pstart" targetRef="pend" />
  </bpmn:process>
  <bpmn:process id="started-by-message" isExecutable="true">
    <bpmn:startEvent id="mstart">
      <bpmn:messageEventDefinition messageRef="mb" />
    </bpmn:startEvent>
    <bpmn:endEvent id="mend" />
    <bpmn:sequenceFlow id="mf1" sourceRef="mstart" targetRef="mend" />
  </bpmn:process>
  <bpmn:message id="mb" name="boot" />
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="plain">
      <bpmndi:BPMNShape id="pstart_di" bpmnElement="pstart">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="pend_di" bpmnElement="pend">
        <dc:Bounds x="412" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="pf1_di" bpmnElement="pf1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="412" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
    <bpmndi:BPMNPlane id="BPMNPlane_2" bpmnElement="started-by-message">
      <bpmndi:BPMNShape id="mstart_di" bpmnElement="mstart">
        <dc:Bounds x="160" y="300" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="mend_di" bpmnElement="mend">
        <dc:Bounds x="412" y="300" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="mf1_di" bpmnElement="mf1">
        <di:waypoint x="196" y="318" />
        <di:waypoint x="412" y="318" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// A call activity (`caller-b` → `child-b`) with an interrupting message
/// boundary event (`stop`, correlation `c`): correlating the message abandons
/// the call activity and terminates the spawned child instance.
const CALL_ACTIVITY_BOUNDARY_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_callb" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="caller-b" isExecutable="true">
    <bpmn:startEvent id="pstart" />
    <bpmn:callActivity id="call">
      <bpmn:extensionElements>
        <zeebe:calledElement processId="child-b" />
      </bpmn:extensionElements>
    </bpmn:callActivity>
    <bpmn:boundaryEvent id="stop" attachedToRef="call" cancelActivity="true">
      <bpmn:messageEventDefinition messageRef="ms" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="pend" />
    <bpmn:endEvent id="aborted" />
    <bpmn:sequenceFlow id="pf1" sourceRef="pstart" targetRef="call" />
    <bpmn:sequenceFlow id="pf2" sourceRef="call" targetRef="pend" />
    <bpmn:sequenceFlow id="pf3" sourceRef="stop" targetRef="aborted" />
  </bpmn:process>
  <bpmn:process id="child-b" isExecutable="true">
    <bpmn:startEvent id="cstart" />
    <bpmn:serviceTask id="ctask">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="inner" retries="3" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="cend" />
    <bpmn:sequenceFlow id="cf1" sourceRef="cstart" targetRef="ctask" />
    <bpmn:sequenceFlow id="cf2" sourceRef="ctask" targetRef="cend" />
  </bpmn:process>
  <bpmn:message id="ms" name="stop">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=&quot;c&quot;" />
    </bpmn:extensionElements>
  </bpmn:message>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="caller-b">
      <bpmndi:BPMNShape id="pstart_di" bpmnElement="pstart">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="call_di" bpmnElement="call">
        <dc:Bounds x="250" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="stop_di" bpmnElement="stop">
        <dc:Bounds x="318" y="140" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="pend_di" bpmnElement="pend">
        <dc:Bounds x="412" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="aborted_di" bpmnElement="aborted">
        <dc:Bounds x="412" y="220" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="pf1_di" bpmnElement="pf1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="250" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="pf2_di" bpmnElement="pf2">
        <di:waypoint x="350" y="118" />
        <di:waypoint x="412" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="pf3_di" bpmnElement="pf3">
        <di:waypoint x="336" y="176" />
        <di:waypoint x="412" y="238" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
    <bpmndi:BPMNPlane id="BPMNPlane_2" bpmnElement="child-b">
      <bpmndi:BPMNShape id="cstart_di" bpmnElement="cstart">
        <dc:Bounds x="160" y="300" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="ctask_di" bpmnElement="ctask">
        <dc:Bounds x="250" y="278" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="cend_di" bpmnElement="cend">
        <dc:Bounds x="412" y="300" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="cf1_di" bpmnElement="cf1">
        <di:waypoint x="196" y="318" />
        <di:waypoint x="250" y="318" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="cf2_di" bpmnElement="cf2">
        <di:waypoint x="350" y="318" />
        <di:waypoint x="412" y="318" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// An MI body and its children share an element id, so a child activation
/// overwrites the body's `scope_by_element_id` entry. The body's OUTGOING flow
/// must still carry the body's enclosing scope (refreshed from the completing
/// instance), not the body key.
#[test]
fn multi_instance_body_outgoing_flow_uses_the_enclosing_scope() {
    let sc =
        load_scenario(&traces_dir().join("scenarios/exclusive-gateway-subprocess-scope")).unwrap();
    let mut run = NanoRun::new(&sc);
    let (inst, sub, body, child) = (1_000_u64, 1_001_u64, 1_002_u64, 1_003_u64);
    let activating = |eik, scope| Event::ElementActivating {
        instance_key: inst,
        element_instance_key: eik,
        element_id: "mi".into(),
        scope,
    };
    run.render(&activating(body, sub));
    run.render(&activating(child, body));
    run.render(&Event::ElementCompleted {
        instance_key: inst,
        element_instance_key: child,
        element_id: "mi".into(),
    });
    run.render(&Event::ElementCompleted {
        instance_key: inst,
        element_instance_key: body,
        element_id: "mi".into(),
    });
    let out = run.render(&Event::SequenceFlowTaken {
        instance_key: inst,
        from: "mi".into(),
        to: "next".into(),
    });
    assert_eq!(out[0]["value"]["flowScopeKey"], json!(sub), "{out:?}");
}
