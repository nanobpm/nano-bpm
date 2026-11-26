//! The positional `TestEngine` facade over `handle()` (#2046).
//!
//! In the `read-model` build there is ONE wasm implementation of each served
//! REST operation: the [`handle()`](TestEngine::handle) route table
//! ([`ROUTES`](crate::gateway::ROUTES)). Every positional method that has a
//! route is a thin wrapper here: it builds the route's request, calls
//! `handle()`, and shapes the answer for the positional contract —
//!
//! * **reads** return the REST response body unchanged; a 404 is JSON `null`;
//! * **writes** keep returning the post-run `Snapshot` (plus the identities the
//!   method has always returned), so the console and Bojtos see no change.
//!
//! A rejection is thrown as the ProblemDetail-shaped error every other method
//! throws ([`problem_err`]), its fields taken from the route's ProblemDetail.
//!
//! `FACADE` (test-only) classifies EVERY exported `TestEngine` method: a [`Facade::Route`]
//! wrapper, or a `Facade::Kept` method with the reason it is not one. The
//! guard test (`facade_guard_tests` below) derives the exported set from the
//! wasm-bindgen sources and the routes from [`ROUTES`](crate::gateway::ROUTES),
//! so a new hand-written method that duplicates a route cannot appear.

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{form_id_of, ProblemKind};
use serde_json::{json, Value as J};
use wasm_bindgen::prelude::*;

use super::{js_err, problem_err, resource_looks_like_dmn, to_json};
use super::{parse_key, TestEngine};

/// How an exported `TestEngine` method relates to the `handle()` route table.
#[cfg(test)]
#[derive(Debug)]
pub(crate) enum Facade {
    /// A thin `handle()` wrapper over the route `(method, path template)`.
    Route(&'static str, &'static str),
    /// Not a wrapper: either no route serves it, or (the `reason` says which)
    /// its positional semantics are not expressible through the route.
    Kept(&'static str),
    /// Kept as a direct-command implementation although `handle()` serves
    /// the route `(method, path template)`, because the route cannot express
    /// the positional semantics (`reason`). It must be `@deprecated` naming
    /// `handle("<method>", "<path>", …)` (#2046's deprecation fallback).
    Deprecated(&'static str, &'static str, &'static str),
}

#[cfg(test)]
const NO_ROUTE: &str = "no handle() route serves this operation yet";
#[cfg(test)]
const TEST_ONLY: &str = "test-only control with no REST counterpart";

/// Every exported `TestEngine` method (by its JS name) and how it is served.
/// The guard test checks this table against the wasm-bindgen exports and
/// [`ROUTES`](crate::gateway::ROUTES).
#[cfg(test)]
pub(crate) const FACADE: &[(&str, Facade)] = &[
    // --- handle() wrappers -------------------------------------------------
    ("deploy", Facade::Route("POST", "/deployments")),
    ("deployDecision", Facade::Route("POST", "/deployments")),
    ("deployForm", Facade::Route("POST", "/deployments")),
    ("deployResource", Facade::Route("POST", "/deployments")),
    (
        "createInstance",
        Facade::Route("POST", "/process-instances"),
    ),
    (
        "cancelInstance",
        Facade::Route(
            "POST",
            "/process-instances/{processInstanceKey}/cancellation",
        ),
    ),
    ("activateJobs", Facade::Route("POST", "/jobs/activation")),
    (
        "completeJob",
        Facade::Route("POST", "/jobs/{jobKey}/completion"),
    ),
    ("failJob", Facade::Route("POST", "/jobs/{jobKey}/failure")),
    ("throwError", Facade::Route("POST", "/jobs/{jobKey}/error")),
    (
        "setVariables",
        Facade::Route("PUT", "/element-instances/{elementInstanceKey}/variables"),
    ),
    (
        "broadcastSignal",
        Facade::Route("POST", "/signals/broadcast"),
    ),
    (
        "completeUserTask",
        Facade::Route("POST", "/user-tasks/{userTaskKey}/completion"),
    ),
    (
        "assignUserTask",
        Facade::Route("POST", "/user-tasks/{userTaskKey}/assignment"),
    ),
    (
        "unassignUserTask",
        Facade::Route("DELETE", "/user-tasks/{userTaskKey}/assignee"),
    ),
    ("getFormByKey", Facade::Route("GET", "/forms/{formKey}")),
    (
        "getResourceByKey",
        Facade::Route("GET", "/resources/{resourceKey}"),
    ),
    (
        "getAgentInstance",
        Facade::Route("GET", "/agent-instances/{agentInstanceKey}"),
    ),
    (
        "searchUserTasks",
        Facade::Route("POST", "/user-tasks/search"),
    ),
    (
        "searchProcessInstances",
        Facade::Route("POST", "/process-instances/search"),
    ),
    (
        "searchIncidents",
        Facade::Route("POST", "/incidents/search"),
    ),
    (
        "searchVariables",
        Facade::Route("POST", "/variables/search"),
    ),
    (
        "searchAgentInstances",
        Facade::Route("POST", "/agent-instances/search"),
    ),
    (
        "searchAgentInstanceHistory",
        Facade::Route("POST", "/agent-instances/{agentInstanceKey}/history/search"),
    ),
    // --- kept: the route cannot express the positional semantics ------------
    (
        "updateUserTask",
        Facade::Deprecated(
            "PATCH",
            "/user-tasks/{userTaskKey}",
            "PATCH /user-tasks/{userTaskKey} cannot clear a date (REST null preserves it), \
             which the positional changeset (`null`/\"\" clears) supports",
        ),
    ),
    (
        "completeAgentJob",
        Facade::Deprecated(
            "POST",
            "/jobs/{jobKey}/completion",
            "POST /jobs/{jobKey}/completion rejects a job `result` (activateElements) \
             in the in-browser engine",
        ),
    ),
    (
        "tickNow",
        Facade::Kept(
            "monotonic host-clock tick; PUT /clock pins the EXACT instant (it may move \
             the clock backwards)",
        ),
    ),
    // --- kept: no handle() route yet ----------------------------------------
    ("evaluateDecision", Facade::Kept(NO_ROUTE)),
    ("correlateMessage", Facade::Kept(NO_ROUTE)),
    ("publishMessage", Facade::Kept(NO_ROUTE)),
    ("updateRetries", Facade::Kept(NO_ROUTE)),
    ("updateTimeout", Facade::Kept(NO_ROUTE)),
    ("resolveIncident", Facade::Kept(NO_ROUTE)),
    ("modify", Facade::Kept(NO_ROUTE)),
    ("migrate", Facade::Kept(NO_ROUTE)),
    ("createAgentInstance", Facade::Kept(NO_ROUTE)),
    ("updateAgentInstance", Facade::Kept(NO_ROUTE)),
    (
        "completeAgentInstance",
        Facade::Kept("legacy embedded-only extension with no REST counterpart"),
    ),
    // --- kept: test-only controls (virtual clock, debugger, log, snapshot) --
    ("new", Facade::Kept(TEST_ONLY)),
    ("now", Facade::Kept(TEST_ONLY)),
    ("reset", Facade::Kept(TEST_ONLY)),
    ("advanceTime", Facade::Kept(TEST_ONLY)),
    ("debugCreateInstance", Facade::Kept(TEST_ONLY)),
    ("debugResume", Facade::Kept(TEST_ONLY)),
    ("debugStep", Facade::Kept(TEST_ONLY)),
    ("debugIsPaused", Facade::Kept(TEST_ONLY)),
    ("debugClear", Facade::Kept(TEST_ONLY)),
    ("snapshot", Facade::Kept(TEST_ONLY)),
    ("events", Facade::Kept(TEST_ONLY)),
    ("replayEvents", Facade::Kept(TEST_ONLY)),
    ("handle", Facade::Kept("the REST driver itself")),
];

/// A non-2xx `handle()` answer: its ProblemDetail fields.
#[derive(Debug)]
pub(crate) struct Rejection {
    pub status: u16,
    pub title: String,
    pub detail: String,
}

impl Rejection {
    /// The ProblemDetail-shaped error the positional method throws.
    fn into_js(self, context: &str) -> JsValue {
        let retryable = ProblemKind::ALL
            .iter()
            .any(|k| k.title() == self.title && k.retryable());
        problem_err(context, self.status, &self.title, &self.detail, retryable)
    }
}

/// Why a read helper failed: a malformed positional argument (no request was
/// made), or a non-404 `handle()` rejection, kept structured so the JS error
/// carries `status`/`title`/`detail`/`nonRetryable` like a write's.
#[derive(Debug)]
pub(crate) enum ReadError {
    Arg(String),
    Rejected(String, Rejection),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Arg(m) => f.write_str(m),
            ReadError::Rejected(op, r) => write!(f, "{op}: {}", r.detail),
        }
    }
}

impl From<String> for ReadError {
    fn from(m: String) -> Self {
        ReadError::Arg(m)
    }
}

impl ReadError {
    fn into_js(self) -> JsValue {
        match self {
            ReadError::Arg(m) => js_err(&m),
            ReadError::Rejected(op, r) => r.into_js(&op),
        }
    }
}

/// Whether `deploy` sends `xml` as a DMN resource: BPMN wins whenever it
/// parses as BPMN (so a BPMN document that merely mentions the DMN namespace
/// stays BPMN), DMN when it parses as DMN; for a document neither parser
/// accepts, the namespace picks which parser's error is reported — the same
/// order the lean `deploy` uses.
pub(crate) fn deploy_as_dmn(xml: &str) -> bool {
    if parse_bpmn(xml).is_ok() {
        return false;
    }
    nanobpmn_engine_core::dmn::parse_dmn(xml).is_ok() || resource_looks_like_dmn(xml)
}

/// The `multipart/form-data` boundary the deploy wrappers frame their one
/// resource with.
const BOUNDARY: &str = "nanobpm-testengine-facade-2046";

/// A one-resource `multipart/form-data` deployment: `(content type, body)`.
///
/// Fails closed on a filename that would break out of its quoted header
/// (`"`, `\\`, or any control character such as CR/LF), and picks a boundary
/// that does not occur in the content so the frame cannot be split early.
fn multipart_one(filename: &str, content: &str) -> Result<(String, String), String> {
    if filename
        .chars()
        .any(|c| c == '"' || c == '\\' || c.is_control())
    {
        return Err(format!(
            "invalid resource name {filename:?}: it must not contain quotes, backslashes or control characters"
        ));
    }
    let mut boundary = BOUNDARY.to_string();
    let mut n: u32 = 0;
    while content.contains(boundary.as_str()) {
        n = n.saturating_add(1);
        boundary = format!("{BOUNDARY}-{n}");
    }
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"resources\"; \
         filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n\
         {content}\r\n--{boundary}--\r\n"
    );
    Ok((format!("multipart/form-data; boundary={boundary}"), body))
}

/// A caller key as one route path segment: rejected unless it is non-empty
/// and free of anything that could change the route (`/`, `?`, `#`, `%`,
/// `\\` or control characters). Other characters reach the server's own
/// key parsing, which answers a malformed key like the REST surface does.
fn path_key(op: &str, key: &str) -> Result<String, String> {
    if key.is_empty() {
        return Err(format!("{op}: the key must not be empty"));
    }
    if key
        .chars()
        .any(|c| matches!(c, '/' | '?' | '#' | '%' | '\\') || c.is_control())
    {
        return Err(format!("{op}: invalid key {key:?}"));
    }
    Ok(key.to_string())
}

/// A positional JSON-object argument (`""` ⇒ `{}`), with the messages the
/// positional methods have always used.
fn object_arg(raw: &str, what: &str) -> Result<J, JsValue> {
    let t = raw.trim();
    if t.is_empty() {
        return Ok(json!({}));
    }
    match serde_json::from_str::<J>(t) {
        Ok(v @ J::Object(_)) => Ok(v),
        Ok(_) => Err(js_err(&format!("{what} must be a JSON object"))),
        Err(e) => Err(js_err(&format!("invalid {what} JSON: {e}"))),
    }
}

fn vars(raw: &str) -> Result<J, JsValue> {
    object_arg(raw, "variables")
}

/// A response body as JSON (a 2xx answer is always well-formed JSON or empty).
fn body_json(body: &str) -> J {
    serde_json::from_str(body).unwrap_or(J::Null)
}

/// The `deployments[]` entries of one kind (`processDefinition`, …).
fn deployed<'a>(result: &'a J, kind: &'a str) -> impl Iterator<Item = &'a J> + 'a {
    result["deployments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(move |d| d.get(kind).filter(|v| !v.is_null()))
}

/// The JS-free routing core (host-testable).
impl TestEngine {
    /// Serve one request through `handle()`: the 2xx body, or the rejection.
    pub(crate) fn call_route(
        &mut self,
        method: &str,
        path: &str,
        content_type: Option<&str>,
        body: &str,
    ) -> Result<String, Rejection> {
        let headers = content_type
            .map(|ct| json!({ "content-type": ct }).to_string())
            .unwrap_or_default();
        let res = self.handle_request(method, path, &headers, body);
        if (200..300).contains(&res.status) {
            return Ok(res.body);
        }
        let p = body_json(&res.body);
        Err(Rejection {
            status: res.status,
            title: p["title"].as_str().unwrap_or_default().to_string(),
            detail: p["detail"].as_str().unwrap_or_default().to_string(),
        })
    }

    /// A JSON write through `handle()`.
    fn write_route(
        &mut self,
        context: &str,
        method: &str,
        path: &str,
        body: Option<&J>,
    ) -> Result<J, JsValue> {
        let (ct, raw) = match body {
            Some(b) => (Some("application/json"), b.to_string()),
            None => (None, String::new()),
        };
        self.call_route(method, path, ct, &raw)
            .map(|b| body_json(&b))
            .map_err(|r| r.into_js(context))
    }

    /// A one-resource deployment through `POST /deployments`.
    fn deploy_route(&mut self, filename: &str, content: &str) -> Result<J, JsValue> {
        let (ct, body) =
            multipart_one(filename, content).map_err(|e| js_err(&format!("deploy error: {e}")))?;
        self.call_route("POST", "/deployments", Some(&ct), &body)
            .map(|b| body_json(&b))
            .map_err(|r| r.into_js("deploy error"))
    }

    /// A read through `handle()` (host-testable): the REST body, JSON `null`
    /// for a 404, or the structured [`ReadError::Rejected`] for any other
    /// rejection (thrown to JS as the same ProblemDetail-shaped error writes
    /// throw).
    pub(crate) fn read_route(
        &mut self,
        op: &str,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<String, ReadError> {
        let body = if body.trim().is_empty() { "" } else { body };
        let ct = (!body.is_empty()).then_some("application/json");
        match self.call_route(method, path, ct, body) {
            Ok(b) => Ok(b),
            Err(r) if r.status == 404 => Ok("null".to_string()),
            Err(r) => Err(ReadError::Rejected(op.to_string(), r)),
        }
    }

    /// The positional job methods' convenience: without a lease token a
    /// `Created` job is activated first (refused while a debug run is paused).
    fn activate_unless_leased(&mut self, job_key: u64, unleased: bool) -> Result<(), JsValue> {
        self.guard_paused()?;
        if unleased {
            self.ensure_activated(job_key)?;
        }
        Ok(())
    }

    /// The post-run snapshot every positional write returns.
    fn snapshot_json(&self, created: Option<u64>) -> Result<String, JsValue> {
        to_json(&self.snapshot_value(created))
    }

    /// The decision-deployment shape `deploy`/`deployDecision` return.
    fn decision_result(&self, result: &J) -> Result<String, JsValue> {
        let drg = deployed(result, "decisionRequirements")
            .next()
            .ok_or_else(|| js_err("deploy error: no decision requirements were deployed"))?;
        let decisions: Vec<J> = deployed(result, "decisionDefinition")
            .map(|d| {
                json!({
                    "decisionId": d["decisionDefinitionId"],
                    "decisionName": d["name"],
                    "decisionKey": d["decisionDefinitionKey"],
                    "version": d["version"],
                })
            })
            .collect();
        to_json(&json!({
            "decisionRequirementsId": drg["decisionRequirementsId"],
            "decisionRequirementsKey": drg["decisionRequirementsKey"],
            "version": drg["version"],
            "decisions": decisions,
            "snapshot": self.snapshot_value(None),
        }))
    }
}

// The read helpers the host tests drive (a `JsValue` cannot be built off
// wasm32); the exported read methods below are these plus the JS error.
impl TestEngine {
    pub(crate) fn get_form_by_key_json(&mut self, form_key: &str) -> Result<String, ReadError> {
        let k = path_key("getFormByKey", form_key)?;
        self.read_route("getFormByKey", "GET", &format!("/forms/{k}"), "")
    }

    pub(crate) fn get_resource_by_key_json(&mut self, key: &str) -> Result<String, ReadError> {
        let k = path_key("getResourceByKey", key)?;
        self.read_route("getResourceByKey", "GET", &format!("/resources/{k}"), "")
    }

    pub(crate) fn get_agent_instance_json(&mut self, key: &str) -> Result<String, ReadError> {
        let k = path_key("getAgentInstance", key)?;
        self.read_route(
            "getAgentInstance",
            "GET",
            &format!("/agent-instances/{k}"),
            "",
        )
    }

    pub(crate) fn search_user_tasks_json(&mut self, query: &str) -> Result<String, ReadError> {
        self.read_route("searchUserTasks", "POST", "/user-tasks/search", query)
    }

    pub(crate) fn search_process_instances_json(
        &mut self,
        query: &str,
    ) -> Result<String, ReadError> {
        self.read_route(
            "searchProcessInstances",
            "POST",
            "/process-instances/search",
            query,
        )
    }

    pub(crate) fn search_incidents_json(&mut self, query: &str) -> Result<String, ReadError> {
        self.read_route("searchIncidents", "POST", "/incidents/search", query)
    }

    pub(crate) fn search_variables_json(
        &mut self,
        query: &str,
        truncate_values: Option<bool>,
    ) -> Result<String, ReadError> {
        let path = format!(
            "/variables/search?truncateValues={}",
            truncate_values.unwrap_or(true)
        );
        self.read_route("searchVariables", "POST", &path, query)
    }

    pub(crate) fn search_agent_instances_json(&mut self, query: &str) -> Result<String, ReadError> {
        self.read_route(
            "searchAgentInstances",
            "POST",
            "/agent-instances/search",
            query,
        )
    }

    pub(crate) fn search_agent_instance_history_json(
        &mut self,
        key: &str,
        query: &str,
    ) -> Result<String, ReadError> {
        let k = path_key("searchAgentInstanceHistory", key)?;
        self.read_route(
            "searchAgentInstanceHistory",
            "POST",
            &format!("/agent-instances/{k}/history/search"),
            query,
        )
    }
}

/// The `read-model` build's positional methods: thin `handle()` wrappers.
#[wasm_bindgen]
impl TestEngine {
    /// Parse and deploy a BPMN **or** DMN resource through
    /// `POST /deployments`. A BPMN resource returns
    /// `{ "processIds": [...], "snapshot": {...} }`; a DMN resource (one that
    /// declares the DMN namespace) returns the `deployDecision` shape.
    pub fn deploy(&mut self, xml: &str) -> Result<String, JsValue> {
        let dmn = deploy_as_dmn(xml);
        let file = if dmn { "resource.dmn" } else { "resource.bpmn" };
        let result = self.deploy_route(file, xml)?;
        if dmn {
            return self.decision_result(&result);
        }
        let ids: Vec<J> = deployed(&result, "processDefinition")
            .map(|p| p["processDefinitionId"].clone())
            .collect();
        to_json(&json!({ "processIds": ids, "snapshot": self.snapshot_value(None) }))
    }

    /// Deploy a DMN decision-requirements resource through `POST /deployments`.
    /// Returns `{ "decisionRequirementsId", "decisionRequirementsKey",
    /// "version", "decisions": [{ "decisionId", "decisionName", "decisionKey",
    /// "version" }], "snapshot" }`.
    #[wasm_bindgen(js_name = deployDecision)]
    pub fn deploy_decision(&mut self, xml: &str) -> Result<String, JsValue> {
        let result = self.deploy_route("resource.dmn", xml)?;
        self.decision_result(&result)
    }

    /// Deploy a `form-js` `.form` resource (named `<id>.form`) through
    /// `POST /deployments`. Returns `{ "formKey", "formId", "version",
    /// "resourceName", "snapshot" }`.
    #[wasm_bindgen(js_name = deployForm)]
    pub fn deploy_form(&mut self, schema: &str) -> Result<String, JsValue> {
        let id = form_id_of(schema).ok_or_else(|| {
            js_err(
                "invalid form: not a valid form-js document (expected a JSON object \
                 with a non-empty string \"id\")",
            )
        })?;
        let result = self.deploy_route(&format!("{id}.form"), schema)?;
        let form = deployed(&result, "form")
            .next()
            .cloned()
            .ok_or_else(|| js_err("deploy error: no form was deployed"))?;
        to_json(&json!({
            "formKey": form["formKey"],
            "formId": form["formId"],
            "version": form["version"],
            "resourceName": form["resourceName"],
            "snapshot": self.snapshot_value(None),
        }))
    }

    /// Deploy a resource under `resource_name` through `POST /deployments`
    /// (classified by its extension, as the gateway does). Returns
    /// `{ "resourceKey", "resourceId", "version", "resourceName", "snapshot" }`.
    #[wasm_bindgen(js_name = deployResource)]
    pub fn deploy_resource(
        &mut self,
        resource_name: &str,
        content: &str,
    ) -> Result<String, JsValue> {
        if !crate::gateway::is_generic_resource(resource_name, content) {
            return Err(js_err(&format!(
                "deploy error: {resource_name:?} would deploy as a process, decision or \
                 form, not a generic resource; use deploy/deployDecision/deployForm"
            )));
        }
        let result = self.deploy_route(resource_name, content)?;
        let r = deployed(&result, "resource")
            .next()
            .cloned()
            .ok_or_else(|| js_err("deploy error: no resource was deployed"))?;
        to_json(&json!({
            "resourceKey": r["resourceKey"],
            "resourceId": r["resourceId"],
            "version": r["version"],
            "resourceName": r["resourceName"],
            "snapshot": self.snapshot_value(None),
        }))
    }

    /// Start an instance through `POST /process-instances`. `version` selects
    /// a process version (non-positive / absent ⇒ latest). Returns the
    /// snapshot with `created` holding the new instance key.
    #[wasm_bindgen(js_name = createInstance)]
    pub fn create_instance(
        &mut self,
        process_id: &str,
        variables_json: &str,
        version: Option<i32>,
    ) -> Result<String, JsValue> {
        let mut req =
            json!({ "processDefinitionId": process_id, "variables": vars(variables_json)? });
        if let Some(v) = version.filter(|v| *v > 0) {
            req["processDefinitionVersion"] = json!(v);
        }
        let res = self.write_route("create error", "POST", "/process-instances", Some(&req))?;
        let created = res["processInstanceKey"]
            .as_str()
            .and_then(|k| k.parse::<u64>().ok());
        self.snapshot_json(created)
    }

    /// Cancel a process instance through
    /// `POST /process-instances/{processInstanceKey}/cancellation`. Returns the
    /// snapshot.
    #[wasm_bindgen(js_name = cancelInstance)]
    pub fn cancel_instance(&mut self, instance_key: &str) -> Result<String, JsValue> {
        let key = parse_key(instance_key)?;
        let path = format!("/process-instances/{key}/cancellation");
        self.write_route("cancel instance error", "POST", &path, None)?;
        self.snapshot_json(None)
    }

    /// Activate up to `max_jobs` jobs of `job_type` through
    /// `POST /jobs/activation` (`timeout_ms` ≤ 0 ⇒ 30 s). Returns the activated
    /// jobs in the positional shape (`key`, `type`, `instanceKey`, …).
    #[wasm_bindgen(js_name = activateJobs)]
    pub fn activate_jobs(
        &mut self,
        job_type: &str,
        max_jobs: u32,
        timeout_ms: f64,
        worker: &str,
        with_lease: Option<bool>,
    ) -> Result<String, JsValue> {
        let timeout = if timeout_ms.is_finite() && timeout_ms > 0.0 {
            timeout_ms as u64
        } else {
            30_000
        };
        let req = json!({
            "type": job_type,
            "maxJobsToActivate": max_jobs.max(1),
            "timeout": timeout,
            "worker": worker,
            "withLease": with_lease.unwrap_or(false),
        });
        let res = self.write_route("activate error", "POST", "/jobs/activation", Some(&req))?;
        let jobs: Vec<J> = res["jobs"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|j| {
                json!({
                    "key": j["jobKey"],
                    "type": j["type"],
                    "instanceKey": j["processInstanceKey"],
                    "elementInstanceKey": j["elementInstanceKey"],
                    "elementId": j["elementId"],
                    "bpmnProcessId": j["processDefinitionId"],
                    "processDefinitionKey": j["processDefinitionKey"],
                    "processDefinitionVersion": j["processDefinitionVersion"],
                    "worker": j["worker"],
                    "retries": j["retries"],
                    "deadline": j["deadline"],
                    "priority": j["priority"],
                    "customHeaders": j["customHeaders"],
                    "tags": j["tags"],
                    "businessId": j["businessId"],
                    "variables": j["variables"],
                    "jobLeaseToken": j["jobLeaseToken"],
                })
            })
            .collect();
        to_json(&J::Array(jobs))
    }

    /// Complete a job through `POST /jobs/{jobKey}/completion`, merging
    /// `variables_json`. Without a lease token a `Created` job is activated
    /// first, so the UI can complete a freshly-created job. Returns the snapshot.
    #[wasm_bindgen(js_name = completeJob)]
    pub fn complete_job(
        &mut self,
        job_key: &str,
        variables_json: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        let variables = vars(variables_json)?;
        self.activate_unless_leased(key, lease_token.is_none())?;
        let req = json!({ "variables": variables, "jobLeaseToken": lease_token });
        let path = format!("/jobs/{key}/completion");
        self.write_route("complete error", "POST", &path, Some(&req))?;
        self.snapshot_json(None)
    }

    /// Fail a job through `POST /jobs/{jobKey}/failure` with the remaining
    /// `retries` and `message` (activating a `Created` job first when no lease
    /// token is given). Returns the snapshot.
    #[wasm_bindgen(js_name = failJob)]
    pub fn fail_job(
        &mut self,
        job_key: &str,
        retries: i32,
        message: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        self.activate_unless_leased(key, lease_token.is_none())?;
        let req =
            json!({ "retries": retries, "errorMessage": message, "jobLeaseToken": lease_token });
        let path = format!("/jobs/{key}/failure");
        self.write_route("fail error", "POST", &path, Some(&req))?;
        self.snapshot_json(None)
    }

    /// Throw a BPMN error from a job through `POST /jobs/{jobKey}/error`
    /// (activating a `Created` job first when no lease token is given).
    /// Returns the snapshot.
    #[wasm_bindgen(js_name = throwError)]
    pub fn throw_error(
        &mut self,
        job_key: &str,
        error_code: &str,
        error_message: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        let key = parse_key(job_key)?;
        self.activate_unless_leased(key, lease_token.is_none())?;
        let req = json!({
            "errorCode": error_code,
            "errorMessage": error_message,
            "jobLeaseToken": lease_token,
        });
        let path = format!("/jobs/{key}/error");
        self.write_route("throw error", "POST", &path, Some(&req))?;
        self.snapshot_json(None)
    }

    /// Merge variables into a scope through
    /// `PUT /element-instances/{elementInstanceKey}/variables`. Returns the
    /// snapshot.
    #[wasm_bindgen(js_name = setVariables)]
    pub fn set_variables(
        &mut self,
        scope_key: &str,
        variables_json: &str,
        local: bool,
    ) -> Result<String, JsValue> {
        let key = parse_key(scope_key)?;
        let req = json!({ "variables": vars(variables_json)?, "local": local });
        let path = format!("/element-instances/{key}/variables");
        self.write_route("set variables error", "PUT", &path, Some(&req))?;
        self.snapshot_json(None)
    }

    /// Broadcast a signal through `POST /signals/broadcast`. Returns the
    /// snapshot.
    #[wasm_bindgen(js_name = broadcastSignal)]
    pub fn broadcast_signal(
        &mut self,
        signal_name: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        let req = json!({ "signalName": signal_name, "variables": vars(variables_json)? });
        self.write_route(
            "broadcast signal error",
            "POST",
            "/signals/broadcast",
            Some(&req),
        )?;
        self.snapshot_json(None)
    }

    /// Complete a user task through `POST /user-tasks/{userTaskKey}/completion`.
    /// Returns the snapshot.
    #[wasm_bindgen(js_name = completeUserTask)]
    pub fn complete_user_task(
        &mut self,
        user_task_key: &str,
        variables_json: &str,
    ) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        let req = json!({ "variables": vars(variables_json)? });
        let path = format!("/user-tasks/{key}/completion");
        self.write_route("complete user task error", "POST", &path, Some(&req))?;
        self.snapshot_json(None)
    }

    /// Assign a user task through `POST /user-tasks/{userTaskKey}/assignment`.
    /// Returns the snapshot.
    #[wasm_bindgen(js_name = assignUserTask)]
    pub fn assign_user_task(
        &mut self,
        user_task_key: &str,
        assignee: &str,
        allow_override: bool,
    ) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        let req = json!({ "assignee": assignee, "allowOverride": allow_override });
        let path = format!("/user-tasks/{key}/assignment");
        self.write_route("assign user task error", "POST", &path, Some(&req))?;
        self.snapshot_json(None)
    }

    /// Clear a user task's assignee through
    /// `DELETE /user-tasks/{userTaskKey}/assignee`. Returns the snapshot.
    #[wasm_bindgen(js_name = unassignUserTask)]
    pub fn unassign_user_task(&mut self, user_task_key: &str) -> Result<String, JsValue> {
        let key = parse_key(user_task_key)?;
        let path = format!("/user-tasks/{key}/assignee");
        self.write_route("unassign user task error", "DELETE", &path, None)?;
        self.snapshot_json(None)
    }

    /// `GET /forms/{formKey}`: the `FormResult` JSON, or `null` on 404.
    #[wasm_bindgen(js_name = getFormByKey)]
    pub fn get_form_by_key(&mut self, form_key: &str) -> Result<String, JsValue> {
        self.get_form_by_key_json(form_key)
            .map_err(ReadError::into_js)
    }

    /// `GET /resources/{resourceKey}`: the `ResourceResult` JSON, or `null` on
    /// 404.
    #[wasm_bindgen(js_name = getResourceByKey)]
    pub fn get_resource_by_key(&mut self, resource_key: &str) -> Result<String, JsValue> {
        self.get_resource_by_key_json(resource_key)
            .map_err(ReadError::into_js)
    }

    /// `GET /agent-instances/{agentInstanceKey}`: the `AgentInstanceResult`
    /// JSON, or `null` on 404.
    #[wasm_bindgen(js_name = getAgentInstance)]
    pub fn get_agent_instance(&mut self, agent_instance_key: &str) -> Result<String, JsValue> {
        self.get_agent_instance_json(agent_instance_key)
            .map_err(ReadError::into_js)
    }

    /// `POST /user-tasks/search`: `query_json` is the spec's
    /// `UserTaskSearchQuery` (empty ⇒ none); returns the
    /// `UserTaskSearchQueryResult`.
    #[wasm_bindgen(js_name = searchUserTasks)]
    pub fn search_user_tasks(&mut self, query_json: &str) -> Result<String, JsValue> {
        self.search_user_tasks_json(query_json)
            .map_err(ReadError::into_js)
    }

    /// `POST /process-instances/search`: returns the
    /// `ProcessInstanceSearchQueryResult`.
    #[wasm_bindgen(js_name = searchProcessInstances)]
    pub fn search_process_instances(&mut self, query_json: &str) -> Result<String, JsValue> {
        self.search_process_instances_json(query_json)
            .map_err(ReadError::into_js)
    }

    /// `POST /incidents/search`: returns the `IncidentSearchQueryResult`.
    #[wasm_bindgen(js_name = searchIncidents)]
    pub fn search_incidents(&mut self, query_json: &str) -> Result<String, JsValue> {
        self.search_incidents_json(query_json)
            .map_err(ReadError::into_js)
    }

    /// `POST /variables/search?truncateValues=…` (default `true`): returns the
    /// `VariableSearchQueryResult`.
    #[wasm_bindgen(js_name = searchVariables)]
    pub fn search_variables(
        &mut self,
        query_json: &str,
        truncate_values: Option<bool>,
    ) -> Result<String, JsValue> {
        self.search_variables_json(query_json, truncate_values)
            .map_err(ReadError::into_js)
    }

    /// `POST /agent-instances/search`: returns the
    /// `AgentInstanceSearchQueryResult`.
    #[wasm_bindgen(js_name = searchAgentInstances)]
    pub fn search_agent_instances(&mut self, query_json: &str) -> Result<String, JsValue> {
        self.search_agent_instances_json(query_json)
            .map_err(ReadError::into_js)
    }

    /// `POST /agent-instances/{agentInstanceKey}/history/search`: returns the
    /// `AgentInstanceHistorySearchQueryResult`.
    #[wasm_bindgen(js_name = searchAgentInstanceHistory)]
    pub fn search_agent_instance_history(
        &mut self,
        agent_instance_key: &str,
        query_json: &str,
    ) -> Result<String, JsValue> {
        self.search_agent_instance_history_json(agent_instance_key, query_json)
            .map_err(ReadError::into_js)
    }

    /// `handle` — the transport-free REST driver (#992): serve one Camunda v2
    /// REST request as the gateway would. `method` is the HTTP method, `path`
    /// the request target (with or without the `/v2` prefix; a query string is
    /// honoured), `headers_json` a JSON object of header name → value (empty ⇒
    /// none; `content-type` carries a multipart deployment's boundary) and
    /// `body` the request body. Returns `{ "status", "headers", "body" }` as
    /// JSON; never throws — a rejection is an RFC 9457 ProblemDetail response
    /// whose status/title/detail come from the shared engine-error mapping.
    pub fn handle(&mut self, method: &str, path: &str, headers_json: &str, body: &str) -> String {
        let res = self.handle_request(method, path, headers_json, body);
        serde_json::to_string(&res).unwrap_or_default()
    }
}

/// The guard for the defect class #2046 retires: a hand-written positional
/// method duplicating a `handle()` route. The exported set is derived from the
/// `#[wasm_bindgen] impl TestEngine` blocks in the sources, the routes from
/// [`ROUTES`](crate::gateway::ROUTES).
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test-only module (feature-gated, so the clippy.toml test exemptions do not apply)"
)]
mod facade_guard_tests {
    use super::{Facade, FACADE};
    use crate::gateway::ROUTES;

    /// Which build an exported `impl` block is compiled into.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Build {
        Both,
        Lean,
        ReadModel,
    }

    /// One exported method: its JS name, doc comment and body.
    #[derive(Debug)]
    struct Export {
        js: String,
        build: Build,
        docs: String,
        body: String,
    }

    const SOURCES: &[(&str, &str)] = &[
        ("lib.rs", include_str!("lib.rs")),
        ("facade.rs", include_str!("facade.rs")),
    ];

    /// Every `pub fn` of every `#[wasm_bindgen] impl TestEngine` block.
    fn exports() -> Vec<Export> {
        let mut out = Vec::new();
        for (file, src) in SOURCES {
            out.extend(exports_in(file, src));
        }
        out
    }

    /// The `pub fn`s of every `#[wasm_bindgen] impl TestEngine` block in `src`.
    /// The block's outer attributes are collected as a set, in any order, so an
    /// attribute between `#[wasm_bindgen]` and the `impl` cannot hide a block.
    fn exports_in(file: &str, src: &str) -> Vec<Export> {
        let mut out = Vec::new();
        {
            let lines: Vec<&str> = src.lines().collect();
            let mut i = 0;
            while i < lines.len() {
                if lines[i] != "impl TestEngine {" {
                    i += 1;
                    continue;
                }
                let mut outer = Vec::new();
                let mut k = i;
                // Walk up over the outer attributes, skipping doc comments,
                // plain comments and blank lines interleaved with them; stop
                // only at real code.
                while k > 0 {
                    let l = lines[k - 1].trim();
                    if l.starts_with("#[") {
                        outer.push(l);
                    } else if !(l.is_empty()
                        || l.starts_with("//")
                        || l.starts_with("/*")
                        || l.starts_with('*'))
                    {
                        break;
                    }
                    k -= 1;
                }
                if !outer.iter().any(|a| a.starts_with("#[wasm_bindgen")) {
                    i += 1;
                    continue;
                }
                let has = |a: &str| outer.contains(&a);
                let build = if has("#[cfg(not(feature = \"read-model\"))]") {
                    Build::Lean
                } else if has("#[cfg(feature = \"read-model\")]") || file == "facade.rs" {
                    Build::ReadModel
                } else {
                    Build::Both
                };
                let mut docs = String::new();
                let mut attrs = String::new();
                i += 1;
                while i < lines.len() && lines[i] != "}" {
                    let l = lines[i];
                    let t = l.trim_start();
                    if l.starts_with("    ///") {
                        docs.push_str(t);
                        docs.push('\n');
                    } else if l.starts_with("    #[") {
                        attrs.push_str(t);
                    } else if let Some(rest) = l.strip_prefix("    pub fn ") {
                        let name = rest.split('(').next().unwrap_or_default().to_string();
                        let js = if attrs.contains("constructor") {
                            "new".to_string()
                        } else if let Some(j) = attrs.split("js_name = ").nth(1) {
                            j.split([')', ','])
                                .next()
                                .unwrap_or_default()
                                .trim()
                                .to_string()
                        } else {
                            name
                        };
                        let mut body = String::new();
                        while i < lines.len() && lines[i] != "    }" {
                            body.push_str(lines[i]);
                            body.push('\n');
                            i += 1;
                        }
                        out.push(Export {
                            js,
                            build,
                            docs: std::mem::take(&mut docs),
                            body,
                        });
                        attrs.clear();
                    } else if t.is_empty() || l == "    }" {
                        docs.clear();
                        attrs.clear();
                    }
                    i += 1;
                }
            }
        }
        out
    }

    #[test]
    fn export_scan_finds_impl_blocks_whatever_the_attribute_order() {
        let fixtures = [
            "#[wasm_bindgen]\n#[cfg(feature = \"read-model\")]\nimpl TestEngine {\n    pub fn sneaky(&self) {\n    }\n}\n",
            "#[cfg(feature = \"read-model\")]\n#[wasm_bindgen]\nimpl TestEngine {\n    pub fn sneaky(&self) {\n    }\n}\n",
            "#[wasm_bindgen]\n#[cfg(not(feature = \"read-model\"))]\n#[allow(dead_code)]\nimpl TestEngine {\n    pub fn sneaky(&self) {\n    }\n}\n",
            "#[wasm_bindgen]\n/// Lean-only methods\nimpl TestEngine {\n    pub fn sneaky(&self) {\n    }\n}\n",
            "#[wasm_bindgen]\n\n// note\n#[cfg(not(feature = \"read-model\"))]\n/** doc */\nimpl TestEngine {\n    pub fn sneaky(&self) {\n    }\n}\n",
        ];
        let builds = [
            Build::ReadModel,
            Build::ReadModel,
            Build::Lean,
            Build::Both,
            Build::Lean,
        ];
        for (src, build) in fixtures.iter().zip(builds) {
            let e = exports_in("lib.rs", src);
            assert_eq!(e.len(), 1, "missed export in:\n{src}");
            assert_eq!(e[0].js, "sneaky");
            assert_eq!(e[0].build, build);
        }
        // A non-exported impl block is not an export.
        let plain = "#[cfg(test)]\nimpl TestEngine {\n    pub fn hidden(&self) {\n    }\n}\n";
        assert!(exports_in("lib.rs", plain).is_empty());
    }

    fn facade(js: &str) -> Option<&'static Facade> {
        FACADE.iter().find(|(n, _)| *n == js).map(|(_, f)| f)
    }

    /// The body of a non-exported helper `fn <name>(` in the sources.
    fn helper_body(name: &str) -> String {
        let needle = format!("fn {name}(");
        for (_, src) in SOURCES {
            if let Some(at) = src.find(&needle) {
                let rest = &src[at..];
                let end = rest.find("\n    }\n").unwrap_or(rest.len());
                return rest[..end].to_string();
            }
        }
        String::new()
    }

    /// Whether `body` is a thin `handle()` wrapper: it routes through the
    /// facade's `*_route` helpers (directly or via a `*_json` read helper) and
    /// applies no engine command itself.
    fn is_wrapper(body: &str) -> bool {
        is_wrapper_with(body, helper_body)
    }

    fn is_wrapper_with(body: &str, helper_body: impl Fn(&str) -> String) -> bool {
        let routes = |b: &str| {
            b.contains("self.write_route(")
                || b.contains("self.deploy_route(")
                || b.contains("self.read_route(")
        };
        let delegated = body
            .split("self.")
            .skip(1)
            .filter_map(|s| s.split('(').next())
            .filter(|n| n.ends_with("_json"))
            .map(helper_body)
            .collect::<Vec<_>>();
        let applies = |b: &str| b.contains("Command::") || b.contains("self.apply(");
        (routes(body) || delegated.iter().any(|h| routes(h)))
            && !applies(body)
            && !delegated.iter().any(|h| applies(h))
    }

    #[test]
    fn wrapper_guard_rejects_a_command_applied_inside_a_delegated_helper() {
        // `get_form_by_key_json` routes; a body delegating to it is a wrapper…
        assert!(is_wrapper("self.get_form_by_key_json(k)"));
        // …but a helper that both routes and applies a command is not.
        assert!(!is_wrapper_with("self.evil_json(k)", |_| {
            "self.read_route(x); self.apply(Command::Reset)".to_string()
        }));
        assert!(!is_wrapper_with("self.evil_json(k)", |_| {
            "self.read_route(x); Command::Reset".to_string()
        }));
    }

    #[test]
    fn the_export_scan_finds_the_known_surface() {
        // Guard against a vacuous pass: the scan must see every block kind.
        let e = exports();
        for build in [Build::Both, Build::Lean, Build::ReadModel] {
            assert!(
                e.iter().any(|x| x.build == build),
                "no {build:?} exports found"
            );
        }
        assert!(e.iter().any(|x| x.js == "new"), "constructor not found");
        assert!(
            e.iter().any(|x| x.js == "debugIsPaused"),
            "getter not found"
        );
    }

    #[test]
    fn every_export_is_a_handle_wrapper_or_on_the_kept_allowlist() {
        let exports = exports();
        let mut problems = Vec::new();
        for e in exports.iter().filter(|e| e.build != Build::Lean) {
            match facade(&e.js) {
                None => problems.push(format!(
                    "`{}` is exported but not classified in FACADE: make it a handle() \
                     wrapper (facade.rs) or add it to the Kept allowlist with a reason",
                    e.js
                )),
                Some(Facade::Route(..)) if e.build == Build::Both => problems.push(format!(
                    "`{}` has a route but is hand-written for both builds; the read-model \
                     build must serve it through handle()",
                    e.js
                )),
                Some(Facade::Route(..)) if !is_wrapper(&e.body) => problems.push(format!(
                    "`{}` is classified as a handle() wrapper but its body does not \
                     delegate to handle() (or applies a Command itself)",
                    e.js
                )),
                Some(Facade::Kept(reason) | Facade::Deprecated(_, _, reason))
                    if reason.trim().is_empty() =>
                {
                    problems.push(format!("`{}` is kept without a reason", e.js))
                }
                Some(Facade::Deprecated(method, path, _))
                    if !(e.docs.contains("@deprecated")
                        && e.docs.contains(&format!("handle(\"{method}\", \"{path}\""))) =>
                {
                    problems.push(format!(
                        "`{}` duplicates {method} {path} but is not `@deprecated` naming \
                         handle(\"{method}\", \"{path}\", …)",
                        e.js
                    ))
                }
                _ => {}
            }
        }
        for (js, _) in FACADE {
            let n = exports
                .iter()
                .filter(|e| e.build != Build::Lean && e.js == *js)
                .count();
            if n != 1 {
                problems.push(format!(
                    "FACADE entry `{js}` matches {n} read-model exports"
                ));
            }
        }
        assert!(problems.is_empty(), "{problems:#?}");
    }

    #[test]
    fn every_wrapper_names_a_served_route_and_every_matching_route_is_decided() {
        for (js, f) in FACADE {
            if let Facade::Route(method, path) | Facade::Deprecated(method, path, _) = f {
                assert!(
                    ROUTES
                        .iter()
                        .any(|r| r.method == *method && r.path == *path),
                    "`{js}` names {method} {path}, which no handle() route serves"
                );
            }
        }
        // A route whose operationId is also a positional method's name must be
        // served by it, or kept with a reason that names the route.
        for r in ROUTES {
            if let Some(Facade::Kept(reason)) = facade(r.operation_id) {
                assert!(
                    reason.contains(r.path),
                    "`{}` duplicates route {} {} but is kept without saying why the \
                     route cannot serve it",
                    r.operation_id,
                    r.method,
                    r.path
                );
            }
        }
    }

    #[test]
    fn every_lean_method_with_a_route_is_deprecated_naming_it() {
        let exports = exports();
        let lean: Vec<&Export> = exports.iter().filter(|e| e.build == Build::Lean).collect();
        assert!(!lean.is_empty());
        for e in lean {
            match facade(&e.js) {
                Some(Facade::Route(method, path)) => assert!(
                    e.docs.contains("@deprecated")
                        && e.docs.contains(&format!("handle(\"{method}\", \"{path}\"")),
                    "lean `{}` must carry `@deprecated` naming handle(\"{method}\", \"{path}\", …)",
                    e.js
                ),
                other => panic!(
                    "lean-only `{}` is {other:?}: only routed methods belong in the lean block",
                    e.js
                ),
            }
        }
        // And every routed method has its lean counterpart (no silent removal).
        for (js, f) in FACADE {
            if matches!(f, Facade::Route(..)) && !js.starts_with("get") && !js.starts_with("search")
            {
                assert!(
                    exports
                        .iter()
                        .any(|e| e.build == Build::Lean && e.js == *js),
                    "routed write `{js}` has no lean implementation"
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test-only module (feature-gated, so the clippy.toml test exemptions do not apply)"
)]
mod request_framing_tests {
    use super::{body_json, deploy_as_dmn, deployed, multipart_one, path_key, ReadError, BOUNDARY};
    use crate::{resource_looks_like_dmn, TestEngine};

    #[test]
    fn multipart_rejects_header_breaking_filenames() {
        for bad in [
            "a\".bpmn",
            "a\r\nX: y.bpmn",
            "a\n.bpmn",
            "a\\b.bpmn",
            "a\0.bpmn",
        ] {
            assert!(multipart_one(bad, "<x/>").is_err(), "{bad:?}");
        }
        assert!(multipart_one("my process (v2).bpmn", "<x/>").is_ok());
    }

    #[test]
    fn multipart_boundary_never_occurs_in_content() {
        let content = format!("a --{BOUNDARY} b --{BOUNDARY}-1 c");
        let (ct, body) = multipart_one("r.bpmn", &content).unwrap();
        let boundary = ct.split("boundary=").nth(1).unwrap();
        assert!(!content.contains(boundary));
        assert!(body.contains(&content));
        assert!(body.ends_with(&format!("\r\n--{boundary}--\r\n")));
    }

    const MINI_BPMN: &str = r#"<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:dmn="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d">
  <process id="p" isExecutable="true">
    <documentation>see https://www.omg.org/spec/DMN/</documentation>
    <startEvent id="s"/><endEvent id="e"/>
    <sequenceFlow id="f" sourceRef="s" targetRef="e"/>
  </process>
</definitions>"#;

    #[test]
    fn deploy_classifies_by_parse_not_by_dmn_substring() {
        // A BPMN document that merely mentions the DMN namespace stays BPMN.
        assert!(resource_looks_like_dmn(MINI_BPMN));
        assert!(!deploy_as_dmn(MINI_BPMN));
        let mut eng = TestEngine::new();
        let ids = deployed_ids(&mut eng, MINI_BPMN);
        assert_eq!(ids, vec!["p".to_string()]);
        // A real DMN document still routes to DMN; an unparseable one reports
        // the error of the format its namespace names.
        assert!(deploy_as_dmn(
            r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="x" />"#
        ));
        assert!(!deploy_as_dmn("<definitions/>"));
    }

    fn deployed_ids(eng: &mut TestEngine, xml: &str) -> Vec<String> {
        let file = if deploy_as_dmn(xml) {
            "r.dmn"
        } else {
            "r.bpmn"
        };
        let (ct, body) = multipart_one(file, xml).unwrap();
        let res = body_json(
            &eng.call_route("POST", "/deployments", Some(&ct), &body)
                .unwrap(),
        );
        deployed(&res, "processDefinition")
            .map(|p| p["processDefinitionId"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn deploy_resource_precheck_matches_the_route_dispatch() {
        for (name, content) in [
            ("a.bpmn", ""),
            ("A.BPMN", ""),
            ("a.dmn", ""),
            ("f.form", r#"{"id":"f"}"#),
            ("p.xml", MINI_BPMN),
        ] {
            assert!(
                !crate::gateway::is_generic_resource(name, content),
                "{name}"
            );
        }
        for (name, content) in [("a.txt", "x"), ("c.xml", "<config/>"), ("s.json", "{}")] {
            assert!(crate::gateway::is_generic_resource(name, content), "{name}");
        }
    }

    #[test]
    fn read_rejections_stay_structured() {
        let mut eng = TestEngine::new();
        let err = eng
            .search_user_tasks_json(r#"{"filter":{"nope":1}}"#)
            .unwrap_err();
        match &err {
            ReadError::Rejected(op, r) => {
                assert_eq!(op, "searchUserTasks");
                assert_eq!(r.status, 400, "{r:?}");
                assert!(!r.title.is_empty());
            }
            other => panic!("expected a structured rejection, got {other:?}"),
        }
        assert!(err.to_string().starts_with("searchUserTasks: "));
        assert!(matches!(
            eng.get_form_by_key_json("").unwrap_err(),
            ReadError::Arg(_)
        ));
    }

    #[test]
    fn path_key_rejects_route_changing_keys() {
        for bad in ["", "1?x=y", "1/history/search", "1#", "1%2F", "a\\b", "1\n"] {
            assert!(path_key("op", bad).is_err(), "{bad:?}");
        }
        assert_eq!(
            path_key("op", "2251799813685249").unwrap(),
            "2251799813685249"
        );
        assert_eq!(path_key("op", "my-form_id.v1").unwrap(), "my-form_id.v1");
    }
}
