//! Server/wasm conformance cross-validation (#1936).
//!
//! The in-browser `TestEngine` (`engine-wasm`) and this gateway are two
//! surfaces over the same engine. Where a surface is *derived* from a shared
//! layer (the `nanobpmn-search` pipeline, #1883) it cannot drift; everything the
//! wasm facade still maps by hand can. This suite is the drift-lock for those
//! hand-written seams: every scenario in `server/conformance/scenarios.json` is
//! run through
//!
//! * (a) the server's real generated REST router (`nanobpm_gateway_rest`), fed
//!   the spec-shaped request exactly as a client sends it, and
//! * (b) the `TestEngine` method that surfaces the same operation,
//!
//! and the two answers are compared step by step: success vs rejection, the
//! rejection's HTTP status and ProblemDetail `title`, and — for `"compare":
//! "body"` steps — the response body field by field after normalising volatile
//! values (keys, timestamps, lease tokens).
//!
//! Derived, not hand-kept:
//!
//! * the HTTP method and path of each operation are read from the vendored spec
//!   (`spec/*.yaml`) by `operationId`;
//! * the set of operations that MUST be covered is read from
//!   `engine-wasm/src/surface_parity.rs` — every `Surfaced` JS method there needs
//!   a scenario step ([`every_surfaced_wasm_method_has_a_conformance_scenario`]),
//!   so a newly surfaced command cannot land untested.
//!
//! Known drift is recorded in `conformance/known-gaps.json` / [`UNCOVERED_SURFACED_METHODS`],
//! each entry linked to the issue that removes it. Both lists may only shrink:
//! a listed gap that starts conforming fails the suite until it is deleted.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

use axum::body::Body;
use axum::http::{Method, Request};
use nanobpmn_engine_wasm::{HOST_REJECTION_PREFIX, TestEngine};
use serde_json::{Value as J, json};
use tower::ServiceExt;

use super::ServerImpl;

const CONFORMANCE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/conformance");
const SPEC_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../spec");
const SURFACE_PARITY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../engine-wasm/src/surface_parity.rs"
);

/// Drift between the two surfaces that is known and tracked, read from
/// `conformance/known-gaps.json`: keyed `<scenario>/<step>`, each entry carries
/// the issue whose fix removes it and the step's EXACT expected differences.
/// The step must reproduce that baseline: a new difference (a regression
/// hiding behind the gap) and a vanished one (a fix that must shrink the
/// baseline) both fail the suite. The set of gaps may only shrink; re-record
/// the baselines of the listed gaps with `UPDATE_CONFORMANCE_GAPS=1`.
const KNOWN_GAPS_FILE: &str = "known-gaps.json";

#[derive(serde::Deserialize, serde::Serialize)]
struct KnownGap {
    issue: String,
    diffs: Vec<String>,
}

fn known_gaps() -> BTreeMap<String, KnownGap> {
    let src = std::fs::read_to_string(format!("{CONFORMANCE_DIR}/{KNOWN_GAPS_FILE}"))
        .expect("read conformance/known-gaps.json");
    serde_json::from_str(&src).expect("parse conformance/known-gaps.json")
}

/// `Surfaced` `TestEngine` methods with no scenario yet, each linked to the
/// issue that adds one. May only shrink.
const UNCOVERED_SURFACED_METHODS: &[(&str, &str)] = &[
    (
        "completeAgentInstance",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "createAgentInstance",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "deployDecision",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "deployForm",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "deployResource",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "evaluateDecision",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "getAgentInstance",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "getFormByKey",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "getResourceByKey",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    ("migrate", "https://github.com/nanobpm/nano-bpm/issues/1958"),
    ("modify", "https://github.com/nanobpm/nano-bpm/issues/1958"),
    (
        "resolveIncident",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "searchAgentInstanceHistory",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "searchAgentInstances",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "updateAgentInstance",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
    (
        "updateTimeout",
        "https://github.com/nanobpm/nano-bpm/issues/1958",
    ),
];

/// `Surfaced` `TestEngine` methods with no REST counterpart at all: they drive
/// the facade's virtual clock, which the server (wall clock) does not expose.
/// They cannot be cross-validated and are exempt from the coverage guard.
const WASM_ONLY_METHODS: &[&str] = &["advanceTime", "tickNow"];

/// The `TestEngine` method that surfaces each REST operation the scenarios use.
/// The guard below requires every `Surfaced` method to appear here (through a
/// scenario that uses the operation) or in [`UNCOVERED_SURFACED_METHODS`].
const OP_TO_JS_METHOD: &[(&str, &str)] = &[
    ("createDeployment", "deploy"),
    ("createProcessInstance", "createInstance"),
    ("activateJobs", "activateJobs"),
    ("completeJob", "completeJob"),
    ("failJob", "failJob"),
    ("throwJobError", "throwError"),
    ("updateJob", "updateRetries"),
    ("correlateMessage", "correlateMessage"),
    ("broadcastSignal", "broadcastSignal"),
    ("createElementInstanceVariables", "setVariables"),
    ("cancelProcessInstance", "cancelInstance"),
    ("assignUserTask", "assignUserTask"),
    ("unassignUserTask", "unassignUserTask"),
    ("updateUserTask", "updateUserTask"),
    ("completeUserTask", "completeUserTask"),
    ("searchProcessInstances", "searchProcessInstances"),
    ("searchUserTasks", "searchUserTasks"),
    ("searchVariables", "searchVariables"),
    ("searchIncidents", "searchIncidents"),
];

// ---------------------------------------------------------------- fixtures --

#[derive(Debug, Clone)]
struct Step {
    id: String,
    op: String,
    params: BTreeMap<String, String>,
    body: Option<J>,
    resources: Vec<String>,
    capture: BTreeMap<String, Vec<String>>,
    compare_body: bool,
}

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    steps: Vec<Step>,
}

fn scenarios() -> Vec<Scenario> {
    let src = std::fs::read_to_string(format!("{CONFORMANCE_DIR}/scenarios.json"))
        .expect("read conformance/scenarios.json");
    let doc: J = serde_json::from_str(&src).expect("scenarios.json parses");
    let str_of = |v: &J, what: &str| -> String {
        v.as_str()
            .unwrap_or_else(|| panic!("{what} must be a string"))
            .to_string()
    };
    doc["scenarios"]
        .as_array()
        .expect("`scenarios` array")
        .iter()
        .map(|s| Scenario {
            name: str_of(&s["name"], "scenario name"),
            steps: s["steps"]
                .as_array()
                .expect("`steps` array")
                .iter()
                .map(|st| {
                    let compare = str_of(&st["compare"], "compare");
                    assert!(
                        compare == "body" || compare == "status",
                        "compare must be body|status, got {compare}"
                    );
                    Step {
                        id: str_of(&st["id"], "step id"),
                        op: str_of(&st["op"], "op"),
                        params: st["params"]
                            .as_object()
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| (k.clone(), str_of(v, "param")))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        body: st.get("body").cloned(),
                        resources: st["resources"]
                            .as_array()
                            .map(|a| a.iter().map(|r| str_of(r, "resource")).collect())
                            .unwrap_or_default(),
                        capture: st["capture"]
                            .as_object()
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| {
                                        let ptrs = v
                                            .as_array()
                                            .expect("capture pointers array")
                                            .iter()
                                            .map(|p| str_of(p, "pointer"))
                                            .collect();
                                        (k.clone(), ptrs)
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                        compare_body: compare == "body",
                    }
                })
                .collect(),
        })
        .collect()
}

fn resource(name: &str) -> String {
    std::fs::read_to_string(format!("{CONFORMANCE_DIR}/{name}"))
        .unwrap_or_else(|e| panic!("read conformance/{name}: {e}"))
}

/// `operationId` → (HTTP method, `/v2` path template), from the vendored spec.
fn spec_operations() -> BTreeMap<String, (Method, String)> {
    let mut ops = BTreeMap::new();
    for entry in std::fs::read_dir(SPEC_DIR).expect("spec dir") {
        let path = entry.expect("spec entry").path();
        if path.extension().is_none_or(|x| x != "yaml") {
            continue;
        }
        let doc: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(&path).expect("spec file"))
                .expect("spec parses");
        let Some(paths) = doc["paths"].as_mapping() else {
            continue;
        };
        for (p, item) in paths {
            let (Some(p), Some(item)) = (p.as_str(), item.as_mapping()) else {
                continue;
            };
            for (m, op) in item {
                let (Some(m), Some(id)) = (m.as_str(), op["operationId"].as_str()) else {
                    continue;
                };
                let method = Method::from_bytes(m.to_ascii_uppercase().as_bytes())
                    .expect("spec HTTP method");
                ops.insert(id.to_string(), (method, format!("/v2{p}")));
            }
        }
    }
    ops
}

// ---------------------------------------------------------------- outcomes --

/// One surface's answer to a step.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Accepted(J),
    Rejected {
        status: Option<u16>,
        title: Option<String>,
    },
}

/// Replace a whole-string `${name}` with the captured value (recursively).
fn substitute(v: &J, vars: &BTreeMap<String, String>) -> J {
    match v {
        J::String(s) => match s.strip_prefix("${").and_then(|r| r.strip_suffix('}')) {
            Some(name) => J::String(vars.get(name).cloned().unwrap_or_else(|| s.clone())),
            None => v.clone(),
        },
        J::Array(a) => J::Array(a.iter().map(|x| substitute(x, vars)).collect()),
        J::Object(o) => J::Object(
            o.iter()
                .map(|(k, x)| (k.clone(), substitute(x, vars)))
                .collect(),
        ),
        _ => v.clone(),
    }
}

fn capture(out: &Outcome, step: &Step, vars: &mut BTreeMap<String, String>, side: &str) {
    let Outcome::Accepted(body) = out else {
        return;
    };
    for (name, pointers) in &step.capture {
        let found = pointers
            .iter()
            .filter_map(|p| body.pointer(p))
            .find_map(|v| match v {
                J::String(s) => Some(s.clone()),
                J::Number(n) => Some(n.to_string()),
                _ => None,
            });
        match found {
            Some(v) => {
                vars.insert(name.clone(), v);
            }
            None => panic!(
                "{side}: step `{}` succeeded but capture `{name}` resolved none of {pointers:?} \
                 in {body}",
                step.id
            ),
        }
    }
}

// ------------------------------------------------------------ server side --

struct ServerSide {
    server: ServerImpl,
    ops: BTreeMap<String, (Method, String)>,
    fences: u32,
}

impl ServerSide {
    async fn send(&self, method: Method, path: &str, content_type: &str, body: Body) -> Outcome {
        let app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(
            self.server.clone(),
        );
        let res = app
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", "localhost")
                    .header("content-type", content_type)
                    .body(body)
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("response body");
        let json: J = if bytes.is_empty() {
            J::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| J::String(String::from_utf8_lossy(&bytes).into()))
        };
        if status.is_success() {
            Outcome::Accepted(json)
        } else {
            Outcome::Rejected {
                status: Some(status.as_u16()),
                title: json["title"].as_str().map(str::to_string),
            }
        }
    }

    async fn deploy(&self, files: &[(String, String)]) -> Outcome {
        const BOUNDARY: &str = "conformance-boundary";
        let mut body = String::new();
        for (name, content) in files {
            body.push_str(&format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"resources\"; \
                 filename=\"{name}\"\r\nContent-Type: application/octet-stream\r\n\r\n\
                 {content}\r\n"
            ));
        }
        body.push_str(&format!("--{BOUNDARY}--\r\n"));
        let (method, path) = self.ops["createDeployment"].clone();
        self.send(
            method,
            &path,
            &format!("multipart/form-data; boundary={BOUNDARY}"),
            Body::from(body),
        )
        .await
    }

    /// Read-your-writes barrier. The server projects its read model on an
    /// exporter thread that drains one FIFO channel per shard, so once a fence
    /// write issued *after* every earlier write is visible in the read model,
    /// every earlier write is too. The fence is a uniquely named form: it shows
    /// up in no read the scenarios compare.
    async fn fence(&mut self) {
        self.fences += 1;
        let id = format!("conformance-fence-{}", self.fences);
        let form = json!({ "id": id, "type": "default", "components": [] }).to_string();
        let out = self.deploy(&[(format!("{id}.form"), form)]).await;
        assert!(
            matches!(out, Outcome::Accepted(_)),
            "fence deploy failed: {out:?}"
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while self.server.store.form_by_id(&id).is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "read model never projected fence {id}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    async fn run(&mut self, step: &Step, vars: &BTreeMap<String, String>) -> Outcome {
        if step.op == "createDeployment" {
            let files: Vec<(String, String)> = step
                .resources
                .iter()
                .map(|n| (n.clone(), resource(n)))
                .collect();
            return self.deploy(&files).await;
        }
        if step.op.starts_with("search") {
            self.fence().await;
        }
        let (method, template) = self
            .ops
            .get(&step.op)
            .unwrap_or_else(|| panic!("spec has no operationId `{}`", step.op))
            .clone();
        let mut path = template;
        for (name, value) in &step.params {
            let value = substitute(&J::String(value.clone()), vars);
            path = path.replace(
                &format!("{{{name}}}"),
                value.as_str().expect("param is a string"),
            );
        }
        assert!(
            !path.contains('{'),
            "step `{}`: unfilled path parameter in {path}",
            step.id
        );
        let body = match &step.body {
            Some(b) => Body::from(substitute(b, vars).to_string()),
            None => Body::empty(),
        };
        self.send(method, &path, "application/json", body).await
    }
}

// -------------------------------------------------------------- wasm side --

fn s(v: &J) -> String {
    v.as_str().map(str::to_string).unwrap_or_default()
}

fn vars_json(v: &J) -> String {
    if v.is_null() {
        "{}".into()
    } else {
        v.to_string()
    }
}

/// Run one step through the `TestEngine` method that surfaces the operation.
/// A wasm-bindgen error (`JsValue`) cannot be materialised off-wasm32, so a
/// rejection on the host surfaces as an `Err` or a panic; both are a
/// status-less rejection — the bare-string error the facade throws carries no
/// HTTP status or ProblemDetail.
fn wasm_run(eng: &mut TestEngine, step: &Step, vars: &BTreeMap<String, String>) -> Outcome {
    let body = step
        .body
        .as_ref()
        .map(|b| substitute(b, vars))
        .unwrap_or(J::Null);
    let param = |name: &str| -> String {
        let raw = step
            .params
            .get(name)
            .unwrap_or_else(|| panic!("step `{}` needs path param {name}", step.id));
        s(&substitute(&J::String(raw.clone()), vars))
    };
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<String, ()> {
        let r = match step.op.as_str() {
            "createDeployment" => {
                let mut last = Ok(String::new());
                for name in &step.resources {
                    last = eng.deploy(&resource(name));
                    if last.is_err() {
                        break;
                    }
                }
                last
            }
            "createProcessInstance" => eng.create_instance(
                &s(&body["processDefinitionId"]),
                &vars_json(&body["variables"]),
                body["processDefinitionVersion"]
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok()),
            ),
            "activateJobs" => eng.activate_jobs(
                &s(&body["type"]),
                body["maxJobsToActivate"]
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .unwrap_or(1),
                body["timeout"].as_f64().unwrap_or(0.0),
                body["worker"].as_str().unwrap_or("default"),
                None,
            ),
            "completeJob" => {
                eng.complete_job(&param("jobKey"), &vars_json(&body["variables"]), None)
            }
            "failJob" => eng.fail_job(
                &param("jobKey"),
                body["retries"]
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(0),
                &s(&body["errorMessage"]),
                None,
            ),
            "throwJobError" => eng.throw_error(
                &param("jobKey"),
                &s(&body["errorCode"]),
                &s(&body["errorMessage"]),
                None,
            ),
            "updateJob" => eng.update_retries(
                &param("jobKey"),
                body["changeset"]["retries"]
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(0),
                None,
            ),
            "correlateMessage" => eng.correlate_message(
                &s(&body["name"]),
                &s(&body["correlationKey"]),
                &vars_json(&body["variables"]),
            ),
            "broadcastSignal" => {
                eng.broadcast_signal(&s(&body["signalName"]), &vars_json(&body["variables"]))
            }
            "createElementInstanceVariables" => eng.set_variables(
                &param("elementInstanceKey"),
                &vars_json(&body["variables"]),
                body["local"].as_bool().unwrap_or(false),
            ),
            "cancelProcessInstance" => eng.cancel_instance(&param("processInstanceKey")),
            "assignUserTask" => eng.assign_user_task(
                &param("userTaskKey"),
                &s(&body["assignee"]),
                body["allowOverride"].as_bool().unwrap_or(true),
            ),
            "unassignUserTask" => eng.unassign_user_task(&param("userTaskKey")),
            "updateUserTask" => {
                eng.update_user_task(&param("userTaskKey"), &body["changeset"].to_string())
            }
            "completeUserTask" => {
                eng.complete_user_task(&param("userTaskKey"), &vars_json(&body["variables"]))
            }
            "searchProcessInstances" => eng.search_process_instances(&body.to_string()),
            "searchUserTasks" => eng.search_user_tasks(&body.to_string()),
            "searchVariables" => eng.search_variables(&body.to_string(), None),
            "searchIncidents" => eng.search_incidents(&body.to_string()),
            other => panic!("no TestEngine adapter for operation `{other}`"),
        };
        r.map_err(|_| ())
    }));
    match result {
        Ok(Ok(text)) => Outcome::Accepted(serde_json::from_str(&text).unwrap_or(J::String(text))),
        Err(payload) if !is_host_rejection(payload.as_ref()) => resume_unwind(payload),
        Ok(Err(())) | Err(_) => Outcome::Rejected {
            status: None,
            title: None,
        },
    }
}

/// Only a panic raised by the facade's host-side `js_err` is a rejection.
/// Any other panic — a missing path param, an unadapted operation, or a
/// genuine engine crash — must fail the test, never masquerade as drift.
fn is_host_rejection(payload: &(dyn std::any::Any + Send)) -> bool {
    let msg = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied());
    msg.is_some_and(|m| m.starts_with(HOST_REJECTION_PREFIX))
}

// -------------------------------------------------------------- comparing --

/// Fields whose value is volatile across the two surfaces (allocated keys,
/// wall/virtual-clock timestamps, opaque lease tokens, and page cursors, which
/// encode allocated keys). The value is replaced by
/// a marker that still records its JSON type, so a key emitted as a number on
/// one side and a string on the other remains a difference.
fn volatile(field: &str) -> bool {
    field.ends_with("Key")
        || (field.ends_with("Date") && !CLIENT_SUPPLIED_DATES.contains(&field))
        // Incident `creationTime` is a clock timestamp too: wall clock on the
        // server, the virtual clock (epoch at start) in wasm.
        || field.ends_with("Time")
        || field == "deadline"
        || field == "jobLeaseToken"
        || field.ends_with("Cursor")
}

/// `*Date` fields whose value the client supplies (user-task attributes), so
/// both surfaces must echo it verbatim — never volatile.
const CLIENT_SUPPLIED_DATES: &[&str] = &["dueDate", "followUpDate"];

/// Application-owned payload maps: compared verbatim, never normalised, so a
/// `variables.customerKey` that differs between the surfaces is a difference.
const APPLICATION_PAYLOADS: &[&str] = &["variables", "customHeaders"];

fn normalise(v: &J) -> J {
    match v {
        J::Object(o) => J::Object(
            o.iter()
                .map(|(k, x)| {
                    let x = match x {
                        J::String(_) if volatile(k) => J::String("<volatile:string>".into()),
                        J::Number(_) if volatile(k) => J::String("<volatile:number>".into()),
                        _ if APPLICATION_PAYLOADS.contains(&k.as_str()) => x.clone(),
                        _ => normalise(x),
                    };
                    (k.clone(), x)
                })
                .collect(),
        ),
        J::Array(a) => J::Array(a.iter().map(normalise).collect()),
        _ => v.clone(),
    }
}

fn diff(path: &str, server: &J, wasm: &J, out: &mut Vec<String>) {
    match (server, wasm) {
        (J::Object(a), J::Object(b)) => {
            let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
            for k in keys {
                let p = format!("{path}/{k}");
                match (a.get(k), b.get(k)) {
                    (Some(x), Some(y)) => diff(&p, x, y, out),
                    (Some(x), None) => out.push(format!("{p}: server {x}, wasm <absent>")),
                    (None, Some(y)) => out.push(format!("{p}: server <absent>, wasm {y}")),
                    (None, None) => {}
                }
            }
        }
        (J::Array(a), J::Array(b)) if a.len() == b.len() => {
            for (i, (x, y)) in a.iter().zip(b).enumerate() {
                diff(&format!("{path}/{i}"), x, y, out);
            }
        }
        _ if server == wasm => {}
        _ => out.push(format!(
            "{}: server {server}, wasm {wasm}",
            if path.is_empty() { "/" } else { path }
        )),
    }
}

/// The differences between the two surfaces for one step (empty = conforms).
fn compare(step: &Step, server: &Outcome, wasm: &Outcome) -> Vec<String> {
    let mut out = Vec::new();
    match (server, wasm) {
        (Outcome::Accepted(a), Outcome::Accepted(b)) => {
            if step.compare_body {
                diff("", &normalise(a), &normalise(b), &mut out);
            }
        }
        (Outcome::Rejected { .. }, Outcome::Rejected { .. }) if server == wasm => {}
        _ => out.push(format!("outcome: server {server:?}, wasm {wasm:?}")),
    }
    out
}

/// Every step's differences, keyed `<scenario>/<step>`.
async fn run_all() -> BTreeMap<String, Vec<String>> {
    let ops = spec_operations();
    let mut results = BTreeMap::new();
    for scenario in scenarios() {
        let mut server = ServerSide {
            server: ServerImpl::default(),
            ops: ops.clone(),
            fences: 0,
        };
        let mut eng = TestEngine::new();
        let mut server_vars = BTreeMap::new();
        let mut wasm_vars = BTreeMap::new();
        for step in &scenario.steps {
            let s_out = server.run(step, &server_vars).await;
            let w_out = wasm_run(&mut eng, step, &wasm_vars);
            capture(&s_out, step, &mut server_vars, "server");
            capture(&w_out, step, &mut wasm_vars, "wasm");
            results.insert(
                format!("{}/{}", scenario.name, step.id),
                compare(step, &s_out, &w_out),
            );
        }
    }
    results
}

#[tokio::test]
async fn server_and_wasm_conform_on_every_scenario() {
    let results = run_all().await;
    let mut gaps = known_gaps();
    if std::env::var_os("UPDATE_CONFORMANCE_GAPS").is_some() {
        for (id, gap) in &mut gaps {
            if let Some(diffs) = results.get(id) {
                gap.diffs.clone_from(diffs);
            }
        }
        let text = serde_json::to_string_pretty(&gaps).expect("serialise known gaps");
        std::fs::write(
            format!("{CONFORMANCE_DIR}/{KNOWN_GAPS_FILE}"),
            format!("{text}\n"),
        )
        .expect("write conformance/known-gaps.json");
    }
    let mut failures = Vec::new();
    for gap in gaps.keys() {
        if !results.contains_key(gap) {
            failures.push(format!(
                "known-gaps.json names `{gap}`, which is not a scenario step"
            ));
        }
    }
    for (id, diffs) in &results {
        match (diffs.is_empty(), gaps.get(id).map(|g| (&g.issue, &g.diffs))) {
            (true, Some((issue, _))) => failures.push(format!(
                "`{id}` now conforms — delete it from known-gaps.json ({issue})"
            )),
            (false, Some((issue, expected))) if diffs != expected => failures.push(format!(
                "`{id}` no longer matches its known-gaps.json baseline ({issue}):\n    \
                     expected: {expected:?}\n    actual:   {diffs:?}"
            )),
            (false, None) => failures.push(format!(
                "`{id}` diverges between server and wasm:\n    {}",
                diffs.join("\n    ")
            )),
            _ => {}
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ------------------------------------------------------------------ guards --

/// The `Surfaced` JS methods `engine-wasm/src/surface_parity.rs` declares
/// (`Surfaced { js_method: "a/b" }` and `read_model_read("c")`).
fn surfaced_js_methods() -> BTreeSet<String> {
    let src = std::fs::read_to_string(SURFACE_PARITY).expect("read surface_parity.rs");
    let mut out = BTreeSet::new();
    for marker in ["js_method: \"", "read_model_read(\""] {
        for chunk in src.split(marker).skip(1) {
            let Some(end) = chunk.find('"') else { continue };
            for m in chunk[..end].split('/') {
                if !m.is_empty() {
                    out.insert(m.to_string());
                }
            }
        }
    }
    out
}

#[test]
fn every_surfaced_wasm_method_has_a_conformance_scenario() {
    let surfaced = surfaced_js_methods();
    assert!(
        surfaced.contains("createInstance") && surfaced.contains("searchVariables"),
        "surface_parity.rs parse found no Surfaced methods: {surfaced:?}"
    );
    let op_js: BTreeMap<&str, &str> = OP_TO_JS_METHOD.iter().copied().collect();
    let covered: BTreeSet<String> = scenarios()
        .iter()
        .flat_map(|s| s.steps.iter())
        .map(|st| {
            op_js
                .get(st.op.as_str())
                .unwrap_or_else(|| panic!("OP_TO_JS_METHOD has no entry for `{}`", st.op))
                .to_string()
        })
        .collect();
    let uncovered: BTreeSet<String> = surfaced
        .difference(&covered)
        .filter(|m| !WASM_ONLY_METHODS.contains(&m.as_str()))
        .cloned()
        .collect();
    let listed: BTreeSet<String> = UNCOVERED_SURFACED_METHODS
        .iter()
        .map(|(m, _)| m.to_string())
        .collect();
    let missing: Vec<_> = uncovered.difference(&listed).collect();
    let stale: Vec<_> = listed.difference(&uncovered).collect();
    assert!(
        missing.is_empty(),
        "Surfaced TestEngine methods with no conformance scenario: {missing:?} — add a step to \
         server/conformance/scenarios.json"
    );
    assert!(
        stale.is_empty(),
        "UNCOVERED_SURFACED_METHODS lists covered or no-longer-surfaced methods: {stale:?} — \
         delete them"
    );
}

#[test]
fn every_scenario_operation_is_a_spec_operation() {
    let ops = spec_operations();
    for scenario in scenarios() {
        for step in &scenario.steps {
            assert!(
                ops.contains_key(&step.op),
                "{}/{}: `{}` is not an operationId in spec/*.yaml",
                scenario.name,
                step.id,
                step.op
            );
        }
    }
}

/// The comparator itself must see the drift classes it exists to catch.
fn bare_step(op: &str) -> Step {
    Step {
        id: "t".into(),
        op: op.into(),
        params: BTreeMap::new(),
        body: None,
        resources: Vec::new(),
        capture: BTreeMap::new(),
        compare_body: true,
    }
}

#[test]
#[should_panic(expected = "no TestEngine adapter for operation `noSuchOperation`")]
fn unadapted_operation_fails_loudly_not_as_rejection() {
    wasm_run(
        &mut TestEngine::new(),
        &bare_step("noSuchOperation"),
        &BTreeMap::new(),
    );
}

#[test]
#[should_panic(expected = "needs path param jobKey")]
fn missing_path_param_fails_loudly_not_as_rejection() {
    wasm_run(
        &mut TestEngine::new(),
        &bare_step("completeJob"),
        &BTreeMap::new(),
    );
}

#[test]
fn facade_rejection_is_a_status_less_rejection() {
    let mut step = bare_step("completeJob");
    step.params.insert("jobKey".into(), "not-a-key".into());
    assert!(matches!(
        wasm_run(&mut TestEngine::new(), &step, &BTreeMap::new()),
        Outcome::Rejected {
            status: None,
            title: None
        }
    ));
    assert!(is_host_rejection(&format!("{HOST_REJECTION_PREFIX}x")));
    assert!(!is_host_rejection(&"index out of bounds"));
}

#[test]
fn comparator_flags_key_type_and_shape_drift() {
    let step = Step {
        id: "t".into(),
        op: "searchProcessInstances".into(),
        params: BTreeMap::new(),
        body: None,
        resources: Vec::new(),
        capture: BTreeMap::new(),
        compare_body: true,
    };
    let ok = |v: J| Outcome::Accepted(v);
    // Different key VALUES are volatile and conform.
    assert!(
        compare(
            &step,
            &ok(json!({"jobKey": "1"})),
            &ok(json!({"jobKey": "2"}))
        )
        .is_empty()
    );
    // A key emitted as a number vs a string does not.
    assert!(
        !compare(
            &step,
            &ok(json!({"jobKey": "1"})),
            &ok(json!({"jobKey": 1}))
        )
        .is_empty()
    );
    // A renamed / dropped / null-vs-absent field does not.
    assert!(!compare(&step, &ok(json!({"a": 1})), &ok(json!({"b": 1}))).is_empty());
    assert!(!compare(&step, &ok(json!({"a": null})), &ok(json!({}))).is_empty());
    // A rejection with a status vs a bare one does not.
    let bare = Outcome::Rejected {
        status: None,
        title: None,
    };
    let problem = Outcome::Rejected {
        status: Some(404),
        title: Some("NOT_FOUND".into()),
    };
    assert!(!compare(&step, &problem, &bare).is_empty());
    assert!(!compare(&step, &ok(J::Null), &bare).is_empty());
}

#[test]
fn comparator_keeps_client_supplied_dates_and_payloads_verbatim() {
    let step = bare_step("searchUserTasks");
    let ok = |v: J| Outcome::Accepted(v);
    for field in ["dueDate", "followUpDate"] {
        assert!(
            !compare(
                &step,
                &ok(json!({ field: "2030-01-01T00:00:00Z" })),
                &ok(json!({ field: "2031-01-01T00:00:00Z" }))
            )
            .is_empty(),
            "{field} must not be normalised"
        );
    }
    // Server-generated dates stay volatile.
    assert!(
        compare(
            &step,
            &ok(json!({"creationDate": "a"})),
            &ok(json!({"creationDate": "b"}))
        )
        .is_empty()
    );
    for payload in ["variables", "customHeaders"] {
        for inner in ["customerKey", "deliveryDate", "plain"] {
            assert!(
                !compare(
                    &step,
                    &ok(json!({ "items": [{ payload: { inner: "1" } }] })),
                    &ok(json!({ "items": [{ payload: { inner: "2" } }] }))
                )
                .is_empty(),
                "{payload}.{inner} must be compared verbatim"
            );
        }
    }
}

/// Review #1997 (operator): multipart deployment parity. Every fixture in
/// `conformance/multipart-deploy.json` is POSTed raw to the native router AND
/// to the wasm `handle()` driver; both must answer with the fixture's status
/// and, on success, deploy exactly its resource names. The engine-wasm suite
/// runs the same file against `handle()` alone, so either side drifting fails.
#[tokio::test]
async fn multipart_deploy_fixtures_agree_between_server_and_wasm() {
    let bpmn = resource("conformance-service.bpmn");
    let raw: J = serde_json::from_str(&resource("multipart-deploy.json")).expect("fixtures");
    let ops = spec_operations();
    let (method, path) = ops["createDeployment"].clone();
    let names = |v: &J| -> Vec<String> {
        v["deployments"]
            .as_array()
            .expect("deployments")
            .iter()
            .flat_map(|d| d.as_object().expect("metadata").values())
            .filter_map(|m| m.get("resourceName").and_then(J::as_str))
            .map(str::to_string)
            .collect()
    };
    for f in raw["fixtures"].as_array().expect("fixtures array") {
        let name = s(&f["name"]);
        let ct = s(&f["contentType"]);
        let body = s(&f["body"]).replace("{{BPMN}}", &bpmn);
        let status = f["status"].as_u64().expect("status");
        let expected: Vec<String> = f["resources"]
            .as_array()
            .expect("resources")
            .iter()
            .map(s)
            .collect();

        let server = ServerSide {
            server: ServerImpl::default(),
            ops: ops.clone(),
            fences: 0,
        };
        match server
            .send(method.clone(), &path, &ct, Body::from(body.clone()))
            .await
        {
            Outcome::Accepted(v) => {
                assert_eq!(
                    status, 200,
                    "{name}: server accepted, fixture expects {status}"
                );
                assert_eq!(names(&v), expected, "{name}: server resources");
            }
            Outcome::Rejected { status: got, .. } => {
                assert_eq!(
                    got,
                    Some(u16::try_from(status).expect("u16")),
                    "{name}: server status"
                );
            }
        }

        let mut eng = TestEngine::new();
        let headers = json!({ "content-type": ct }).to_string();
        let res: J =
            serde_json::from_str(&eng.handle("POST", &path, &headers, &body)).expect("res");
        assert_eq!(
            res["status"].as_u64(),
            Some(status),
            "{name}: wasm status {res}"
        );
        if status == 200 {
            let v: J = serde_json::from_str(res["body"].as_str().expect("body")).expect("json");
            assert_eq!(names(&v), expected, "{name}: wasm resources");
        }
    }
}
