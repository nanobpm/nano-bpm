//! The transport-free REST driver ABI (#992):
//! `handle(method, path, headers, body) -> { status, headers, body }`.
//!
//! One entry point lets a consumer (e.g. the Camunda JS SDK's in-process
//! transport) drive the in-browser [`TestEngine`] exactly as it would drive the
//! gateway over HTTP, with no per-method glue:
//!
//! * **reads** are answered by the shared `nanobpmn-search` layer (the gateway
//!   core the server serves its REST reads from);
//! * **writes / clock** apply the engine-core [`Command`]s the `TestEngine`
//!   methods already apply, and shape the spec's response model.
//!
//! The route table [`ROUTES`] is keyed by the spec's `operationId`; the
//! `routes_match_the_spec` drift guard (tests below) checks every entry's method,
//! path template and success status against `spec/*.yaml`, so a route cannot
//! drift from the contract it claims to serve.
//!
//! Every rejection is an RFC 9457 ProblemDetail. An engine rejection takes its
//! status, title and detail from the ONE shared mapping,
//! [`EngineError::problem`](nanobpmn_engine_core::EngineError::problem) — there is
//! no per-endpoint catch-all to 500.

use std::collections::BTreeMap;

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, form_id_of, Command, DeploymentBundle, EngineError, Event, FormResource,
    GenericResource, JobActivationOptions, ProblemKind, UserTaskChangeset, Value,
};
use nanobpmn_search::{models, types};
use serde::Serialize;

use super::{json_to_value, TestEngine};

/// One served operation. The variant is the dispatch key; the spec identity
/// (method, path, operationId, success status) lives in [`ROUTES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    CreateDeployment,
    CreateProcessInstance,
    CancelProcessInstance,
    ActivateJobs,
    CompleteJob,
    FailJob,
    ThrowJobError,
    CreateElementInstanceVariables,
    BroadcastSignal,
    CompleteUserTask,
    AssignUserTask,
    UnassignUserTask,
    UpdateUserTask,
    PinClock,
    GetFormByKey,
    GetResource,
    GetAgentInstance,
    SearchUserTasks,
    SearchProcessInstances,
    SearchVariables,
    SearchIncidents,
    SearchAgentInstances,
    SearchAgentInstanceHistory,
}

/// A route: the spec operation an [`Op`] serves.
#[derive(Debug)]
pub(crate) struct Route {
    /// Upper-case HTTP method.
    pub method: &'static str,
    /// The spec path template (without the `/v2` server prefix).
    pub path: &'static str,
    /// The spec `operationId` (the drift guard's join key into the spec).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by the spec drift guard (tests)")
    )]
    pub operation_id: &'static str,
    /// The spec's success status for the operation.
    pub status: u16,
    pub op: Op,
}

const fn route(
    method: &'static str,
    path: &'static str,
    operation_id: &'static str,
    status: u16,
    op: Op,
) -> Route {
    Route {
        method,
        path,
        operation_id,
        status,
        op,
    }
}

/// The served operations. Checked against `spec/*.yaml` by the drift guard.
pub(crate) const ROUTES: &[Route] = &[
    route(
        "POST",
        "/deployments",
        "createDeployment",
        200,
        Op::CreateDeployment,
    ),
    route(
        "POST",
        "/process-instances",
        "createProcessInstance",
        200,
        Op::CreateProcessInstance,
    ),
    route(
        "POST",
        "/process-instances/{processInstanceKey}/cancellation",
        "cancelProcessInstance",
        204,
        Op::CancelProcessInstance,
    ),
    route(
        "POST",
        "/jobs/activation",
        "activateJobs",
        200,
        Op::ActivateJobs,
    ),
    route(
        "POST",
        "/jobs/{jobKey}/completion",
        "completeJob",
        204,
        Op::CompleteJob,
    ),
    route(
        "POST",
        "/jobs/{jobKey}/failure",
        "failJob",
        204,
        Op::FailJob,
    ),
    route(
        "POST",
        "/jobs/{jobKey}/error",
        "throwJobError",
        204,
        Op::ThrowJobError,
    ),
    route(
        "PUT",
        "/element-instances/{elementInstanceKey}/variables",
        "createElementInstanceVariables",
        204,
        Op::CreateElementInstanceVariables,
    ),
    route(
        "POST",
        "/signals/broadcast",
        "broadcastSignal",
        200,
        Op::BroadcastSignal,
    ),
    route(
        "POST",
        "/user-tasks/{userTaskKey}/completion",
        "completeUserTask",
        204,
        Op::CompleteUserTask,
    ),
    route(
        "POST",
        "/user-tasks/{userTaskKey}/assignment",
        "assignUserTask",
        204,
        Op::AssignUserTask,
    ),
    route(
        "DELETE",
        "/user-tasks/{userTaskKey}/assignee",
        "unassignUserTask",
        204,
        Op::UnassignUserTask,
    ),
    route(
        "PATCH",
        "/user-tasks/{userTaskKey}",
        "updateUserTask",
        204,
        Op::UpdateUserTask,
    ),
    route("PUT", "/clock", "pinClock", 204, Op::PinClock),
    route(
        "GET",
        "/forms/{formKey}",
        "getFormByKey",
        200,
        Op::GetFormByKey,
    ),
    route(
        "GET",
        "/resources/{resourceKey}",
        "getResource",
        200,
        Op::GetResource,
    ),
    route(
        "GET",
        "/agent-instances/{agentInstanceKey}",
        "getAgentInstance",
        200,
        Op::GetAgentInstance,
    ),
    route(
        "POST",
        "/user-tasks/search",
        "searchUserTasks",
        200,
        Op::SearchUserTasks,
    ),
    route(
        "POST",
        "/process-instances/search",
        "searchProcessInstances",
        200,
        Op::SearchProcessInstances,
    ),
    route(
        "POST",
        "/variables/search",
        "searchVariables",
        200,
        Op::SearchVariables,
    ),
    route(
        "POST",
        "/incidents/search",
        "searchIncidents",
        200,
        Op::SearchIncidents,
    ),
    route(
        "POST",
        "/agent-instances/search",
        "searchAgentInstances",
        200,
        Op::SearchAgentInstances,
    ),
    route(
        "POST",
        "/agent-instances/{agentInstanceKey}/history/search",
        "searchAgentInstanceHistory",
        200,
        Op::SearchAgentInstanceHistory,
    ),
];

/// A transport-free HTTP response.
#[derive(Debug, Serialize)]
pub(crate) struct HttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: String,
}

/// A rejection, rendered as an RFC 9457 ProblemDetail.
#[derive(Debug)]
pub(crate) struct Problem {
    status: u16,
    title: String,
    detail: String,
}

impl Problem {
    /// A rejection of the given Zeebe class (status + title derived from it).
    fn of(kind: ProblemKind, detail: impl Into<String>) -> Self {
        Problem {
            status: kind.status(),
            title: kind.title().to_string(),
            detail: detail.into(),
        }
    }

    /// A defect in the driver itself (500), never an engine rejection.
    fn internal(detail: impl Into<String>) -> Self {
        Problem {
            status: 500,
            title: "INTERNAL".to_string(),
            detail: detail.into(),
        }
    }

    /// Malformed request data (400 `INVALID_ARGUMENT`).
    fn invalid(detail: impl Into<String>) -> Self {
        Self::of(ProblemKind::InvalidArgument, detail)
    }
}

impl From<EngineError> for Problem {
    /// The ONE engine-error mapping, shared with the server (#1937).
    fn from(e: EngineError) -> Self {
        let p = e.problem();
        Problem {
            status: p.status,
            title: p.title.to_string(),
            detail: p.detail,
        }
    }
}

type Handled = Result<Option<String>, Problem>;

// Test-only fault injection: the next engine command the driver applies is
// rejected with this error instead. Lets the problem-mapping test drive a
// rejection class the in-memory engine cannot produce on its own (503
// `UNAVAILABLE` — off-heap state, #1388) through the real `handle()` path.
#[cfg(test)]
thread_local! {
    pub(crate) static INJECTED_FAULT: std::cell::RefCell<Option<EngineError>> =
        const { std::cell::RefCell::new(None) };
}

/// A parsed request target: the path (without `/v2` and query) and the query
/// parameters.
struct Target<'a> {
    path: &'a str,
    /// Form-decoded query parameters, or the 400 for a malformed escape.
    query: Result<BTreeMap<String, String>, Problem>,
}

/// Decode one `application/x-www-form-urlencoded` component: `+` is a space
/// and `%XX` a byte; a malformed escape or non-UTF-8 result is a 400.
fn form_decode(raw: &str) -> Result<String, Problem> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let byte = bytes
                    .get(i + 1..i + 3)
                    .filter(|h| h.iter().all(u8::is_ascii_hexdigit))
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| {
                        Problem::invalid(format!("invalid percent-escape in query: {raw:?}"))
                    })?;
                out.push(byte);
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out)
        .map_err(|_| Problem::invalid(format!("query component is not UTF-8: {raw:?}")))
}

fn split_target(raw: &str) -> Target<'_> {
    let (path, query) = raw.split_once('?').unwrap_or((raw, ""));
    let path = path.strip_prefix("/v2").unwrap_or(path);
    let path = if path.len() > 1 {
        path.trim_end_matches('/')
    } else {
        path
    };
    let query = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            Ok((form_decode(k)?, form_decode(v)?))
        })
        .collect::<Result<_, Problem>>();
    Target { path, query }
}

/// Match `path` against a route template, returning the `{param}` captures in
/// order, or `None` when it does not match.
fn match_template(template: &str, path: &str) -> Option<Vec<String>> {
    let t: Vec<&str> = template.trim_start_matches('/').split('/').collect();
    let p: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if t.len() != p.len() {
        return None;
    }
    let mut params = Vec::new();
    for (ts, ps) in t.iter().zip(p.iter()) {
        if ts.starts_with('{') && ts.ends_with('}') {
            if ps.is_empty() {
                return None;
            }
            params.push((*ps).to_string());
        } else if ts != ps {
            return None;
        }
    }
    Some(params)
}

/// Resolve `(method, path)` to a route. A path served under a different method
/// is a 405; an unknown path a 404.
fn resolve(method: &str, path: &str) -> Result<(&'static Route, Vec<String>), Problem> {
    // Prefer literal segments over `{param}` captures (`/user-tasks/search`
    // must not resolve as `/user-tasks/{userTaskKey}`): the most literal
    // segments win.
    let literal = |r: &Route| r.path.split('/').filter(|s| !s.starts_with('{')).count();
    let mut candidates: Vec<(&'static Route, Vec<String>)> = ROUTES
        .iter()
        .filter_map(|r| match_template(r.path, path).map(|p| (r, p)))
        .collect();
    candidates.sort_by_key(|(r, _)| std::cmp::Reverse(literal(r)));
    let best = candidates.first().map(|(r, _)| literal(r));
    let mut path_known = false;
    for (r, params) in candidates {
        if Some(literal(r)) != best {
            break;
        }
        path_known = true;
        if r.method.eq_ignore_ascii_case(method) {
            return Ok((r, params));
        }
    }
    if path_known {
        return Err(Problem {
            status: 405,
            title: "METHOD_NOT_ALLOWED".to_string(),
            detail: format!("{method} is not supported on {path}"),
        });
    }
    Err(Problem::of(
        ProblemKind::NotFound,
        format!("no operation is served at {method} {path} by the in-browser engine"),
    ))
}

/// Case-insensitive header lookup over the request's header map.
fn header<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Parse a JSON request body into the generated model with the gateway's
/// strict decoder (an undefined property is rejected, never ignored) and the
/// generated validation. `Ok(None)` for an absent (empty / `null`) body.
fn body<T>(raw: &str) -> Result<Option<T>, Problem>
where
    T: serde::de::DeserializeOwned + Serialize + nanobpmn_search::validator::Validate,
{
    let parsed = nanobpmn_search::strict::decode_strict::<T>(raw.as_bytes())
        .map_err(|e| Problem::invalid(format!("invalid request body: {e}")))?;
    // The in-browser engine has no audit-record execution path, so a caller's
    // `operationReference` would be silently lost. Reject it on every route,
    // before any mutation, rather than accept-and-drop.
    if let Ok(serde_json::Value::Object(m)) = serde_json::from_str::<serde_json::Value>(raw) {
        if m.contains_key("operationReference") {
            return Err(Problem::invalid(
                "operationReference is not supported by the in-browser engine",
            ));
        }
    }
    if let Some(v) = &parsed {
        v.validate()
            .map_err(|e| Problem::invalid(format!("invalid request body: {e}")))?;
    }
    Ok(parsed)
}

/// A user-task `action` cannot be carried by the engine command, so a
/// non-null one is rejected before mutation rather than silently dropped.
fn reject_action(action: Option<String>) -> Result<(), Problem> {
    match action {
        Some(_) => Err(Problem::invalid(
            "`action` is not supported by the in-browser engine",
        )),
        None => Ok(()),
    }
}

/// A required request body.
fn required_body<T>(raw: &str) -> Result<T, Problem>
where
    T: serde::de::DeserializeOwned + Serialize + nanobpmn_search::validator::Validate,
{
    body(raw)?.ok_or_else(|| Problem::invalid("request body is required"))
}

/// A path key (`{jobKey}`, `{userTaskKey}`, …): a positive decimal key.
fn path_key(name: &str, raw: &str) -> Result<u64, Problem> {
    raw.parse::<u64>()
        .ok()
        .filter(|k| *k > 0)
        .ok_or_else(|| Problem::invalid(format!("{name} {raw:?} is not a valid key")))
}

/// Engine variables from a REST `Object` map.
fn variables(
    map: Option<&std::collections::HashMap<String, types::Object>>,
) -> BTreeMap<String, Value> {
    map.map(|m| {
        m.iter()
            .map(|(k, v)| (k.clone(), json_to_value(&v.0)))
            .collect()
    })
    .unwrap_or_default()
}

/// Resolve the canonical `jobLeaseToken` / deprecated `leaseToken` pair the
/// way the gateway's `resolve_lease_token` does: prefer the canonical name,
/// fall back to the legacy one, and reject both-present-but-different with a
/// 400 BEFORE any command runs.
fn lease_token(
    canonical: Option<String>,
    legacy: Option<String>,
) -> Result<Option<String>, Problem> {
    match (canonical, legacy) {
        (Some(c), Some(l)) if c != l => Err(Problem::invalid(
            "jobLeaseToken and leaseToken were both supplied with different values",
        )),
        (Some(c), _) => Ok(Some(c)),
        (None, l) => Ok(l),
    }
}

/// A `Nullable` optional: absent and explicit `null` both read as `None`.
fn present<T: Clone>(v: Option<&types::Nullable<T>>) -> Option<T> {
    match v {
        Some(types::Nullable::Present(t)) => Some(t.clone()),
        _ => None,
    }
}

/// Serialise a success body.
fn ok_json<T: Serialize>(value: &T) -> Handled {
    serde_json::to_string(value)
        .map(Some)
        .map_err(|e| Problem::internal(format!("serialize error: {e}")))
}

/// A `nanobpmn_search` lookup as a response: 200 / 404 / 400 with the shared
/// layer's own problem title and detail.
fn lookup<T: Serialize>(l: nanobpmn_search::get::Lookup<T>) -> Handled {
    use nanobpmn_search::get::Lookup;
    match l {
        Lookup::Found(v) => ok_json(&v),
        Lookup::NotFound { title, detail } => Err(Problem {
            status: 404,
            title: title.to_string(),
            detail,
        }),
        Lookup::Invalid { title, detail } => Err(Problem {
            status: 400,
            title: title.to_string(),
            detail,
        }),
    }
}

/// Parse a `*SearchQuery` body for the shared search pipeline.
fn search<T>(raw: &str) -> Result<Option<T>, Problem>
where
    T: serde::de::DeserializeOwned + Serialize + nanobpmn_search::validator::Validate,
{
    nanobpmn_search::parse_search_query::<T>(raw).map_err(Problem::invalid)
}

/// A shared-layer source failure (the in-memory read model failed to answer).
fn source_failure(e: impl std::fmt::Display) -> Problem {
    Problem::of(ProblemKind::Unavailable, e.to_string())
}

impl TestEngine {
    /// The JS-free body of `handle()` (host-testable). `headers_json` is a JSON
    /// object of header name → value (empty ⇒ none). Never fails: every
    /// rejection is a ProblemDetail response.
    pub(crate) fn handle_request(
        &mut self,
        method: &str,
        target: &str,
        headers_json: &str,
        body: &str,
    ) -> HttpResponse {
        let t = split_target(target);
        let instance = format!("/v2{}", t.path);
        let outcome = t.query.and_then(|query| {
            let headers = parse_headers(headers_json)?;
            let (route, params) = resolve(method, t.path)?;
            self.dispatch(route.op, &params, &query, &headers, body)
                .map(|b| (route.status, b))
        });
        let mut headers = BTreeMap::new();
        match outcome {
            Ok((status, Some(b))) => {
                headers.insert("content-type".to_string(), "application/json".to_string());
                HttpResponse {
                    status,
                    headers,
                    body: b,
                }
            }
            Ok((status, None)) => HttpResponse {
                status,
                headers,
                body: String::new(),
            },
            Err(p) => {
                headers.insert(
                    "content-type".to_string(),
                    "application/problem+json".to_string(),
                );
                let detail = models::ProblemDetail::new(p.title, p.status, p.detail, instance);
                HttpResponse {
                    status: p.status,
                    headers,
                    body: serde_json::to_string(&detail).unwrap_or_default(),
                }
            }
        }
    }

    /// Apply a command for the driver: a debug run paused mid-command rejects
    /// it (409, see `guard_paused`), an engine rejection maps through
    /// [`EngineError::problem`].
    fn exec(&mut self, command: Command) -> Result<Vec<Event>, Problem> {
        self.check_not_paused()
            .map_err(|m| Problem::of(ProblemKind::InvalidState, m))?;
        #[cfg(test)]
        if let Some(fault) = INJECTED_FAULT.with(|f| f.borrow_mut().take()) {
            return Err(fault.into());
        }
        Ok(self.apply(command)?)
    }

    fn dispatch(
        &mut self,
        op: Op,
        params: &[String],
        query: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        raw: &str,
    ) -> Handled {
        let param = |i: usize| params.get(i).map(String::as_str).unwrap_or_default();
        match op {
            Op::CreateDeployment => self.create_deployment(headers, raw),
            Op::CreateProcessInstance => self.create_process_instance(raw),
            Op::CancelProcessInstance => {
                let key = path_key("processInstanceKey", param(0))?;
                body::<models::CancelProcessInstanceRequest>(raw)?;
                self.exec(Command::cancel_instance_including_suspended(key))?;
                Ok(None)
            }
            Op::ActivateJobs => self.activate_jobs_rest(raw),
            Op::CompleteJob => {
                let job_key = path_key("jobKey", param(0))?;
                let req = body::<models::JobCompletionRequest>(raw)?;
                let (vars, lease, business_id) = match &req {
                    Some(r) => {
                        if r.result.is_some() {
                            return Err(Problem::invalid(
                                "completeJob: a job `result` is not supported by the in-browser engine",
                            ));
                        }
                        (
                            present(r.variables.as_ref()),
                            lease_token(
                                present(r.job_lease_token.as_ref()),
                                present(r.lease_token.as_ref()),
                            )?,
                            present(r.business_id.as_ref()),
                        )
                    }
                    None => (None, None, None),
                };
                self.exec(Command::CompleteJob {
                    job_key,
                    attribution: None,
                    lease_token: lease,
                    variables: variables(vars.as_ref()),
                    adhoc_result: None,
                    task_listener_result: None,
                    business_id,
                })?;
                Ok(None)
            }
            Op::FailJob => {
                let job_key = path_key("jobKey", param(0))?;
                let req = body::<models::JobFailRequest>(raw)?;
                let req = req.as_ref();
                if req.is_some_and(|r| !variables(r.variables.as_ref()).is_empty()) {
                    return Err(Problem::invalid(
                        "failJob: failure `variables` are not supported by the in-browser engine",
                    ));
                }
                let lease = match req {
                    Some(r) => lease_token(
                        present(r.job_lease_token.as_ref()),
                        present(r.lease_token.as_ref()),
                    )?,
                    None => None,
                };
                self.exec(Command::FailJob {
                    job_key,
                    attribution: None,
                    lease_token: lease,
                    retries: req.and_then(|r| r.retries).unwrap_or(0),
                    error_message: req
                        .and_then(|r| r.error_message.clone())
                        .unwrap_or_default(),
                    retry_back_off: req
                        .and_then(|r| r.retry_back_off)
                        .and_then(|b| u64::try_from(b).ok())
                        .unwrap_or(0),
                })?;
                Ok(None)
            }
            Op::ThrowJobError => {
                let job_key = path_key("jobKey", param(0))?;
                let req = required_body::<models::JobErrorRequest>(raw)?;
                self.exec(Command::ThrowJobError {
                    job_key,
                    attribution: None,
                    error_code: req.error_code.clone(),
                    error_message: present(req.error_message.as_ref()).unwrap_or_default(),
                    variables: variables(present(req.variables.as_ref()).as_ref()),
                    lease_token: lease_token(
                        present(req.job_lease_token.as_ref()),
                        present(req.lease_token.as_ref()),
                    )?,
                })?;
                Ok(None)
            }
            Op::CreateElementInstanceVariables => {
                let scope_key = path_key("elementInstanceKey", param(0))?;
                let req = required_body::<models::SetVariableRequest>(raw)?;
                self.exec(Command::SetVariables {
                    scope_key,
                    variables: variables(Some(&req.variables)),
                    local: req.local.unwrap_or(false),
                })?;
                Ok(None)
            }
            Op::BroadcastSignal => {
                let req = required_body::<models::SignalBroadcastRequest>(raw)?;
                let events = self.exec(Command::BroadcastSignal {
                    signal_name: req.signal_name.clone(),
                    variables: variables(req.variables.as_ref()),
                })?;
                let signal_key = events
                    .iter()
                    .find_map(|e| match e {
                        Event::SignalBroadcast { signal_key, .. } => Some(*signal_key),
                        _ => None,
                    })
                    .ok_or_else(|| Problem::internal("broadcastSignal: no signal was recorded"))?;
                ok_json(&models::SignalBroadcastResult {
                    tenant_id: nanobpmn_engine_core::DEFAULT_TENANT.to_string(),
                    signal_key: models::SignalKey(signal_key.to_string()),
                })
            }
            Op::CompleteUserTask => {
                let user_task_key = path_key("userTaskKey", param(0))?;
                let req = body::<models::UserTaskCompletionRequest>(raw)?;
                reject_action(req.as_ref().and_then(|r| present(r.action.as_ref())))?;
                let vars = req.as_ref().and_then(|r| present(r.variables.as_ref()));
                self.exec(Command::CompleteUserTask {
                    user_task_key,
                    variables: variables(vars.as_ref()),
                })?;
                Ok(None)
            }
            Op::AssignUserTask => {
                let user_task_key = path_key("userTaskKey", param(0))?;
                let req = required_body::<models::UserTaskAssignmentRequest>(raw)?;
                reject_action(present(req.action.as_ref()))?;
                let assignee = req
                    .assignee
                    .clone()
                    .filter(|a| !a.is_empty())
                    .ok_or_else(|| {
                        Problem::invalid("assignUserTask: `assignee` must be a non-empty string")
                    })?;
                self.exec(Command::AssignUserTask {
                    user_task_key,
                    assignee,
                    allow_override: present(req.allow_override.as_ref()).unwrap_or(true),
                })?;
                Ok(None)
            }
            Op::UnassignUserTask => {
                let user_task_key = path_key("userTaskKey", param(0))?;
                self.exec(Command::UnassignUserTask { user_task_key })?;
                Ok(None)
            }
            Op::UpdateUserTask => {
                let user_task_key = path_key("userTaskKey", param(0))?;
                let req = body::<models::UserTaskUpdateRequest>(raw)?;
                reject_action(req.as_ref().and_then(|r| present(r.action.as_ref())))?;
                let changeset = req
                    .as_ref()
                    .and_then(|r| present(r.changeset.as_ref()))
                    .map(|c| changeset(&c))
                    .unwrap_or_default();
                self.exec(Command::UpdateUserTask {
                    user_task_key,
                    changeset,
                })?;
                Ok(None)
            }
            Op::PinClock => {
                let req = required_body::<models::ClockPinRequest>(raw)?;
                let pinned = u64::try_from(req.timestamp).map_err(|_| {
                    Problem::invalid("pinClock: `timestamp` must be a non-negative epoch millis")
                })?;
                // `pinClock` sets the EXACT instant (spec/clock.yaml), unlike
                // the monotonic `tickNow`: pinning to an earlier time is allowed.
                self.check_not_paused()
                    .map_err(|m| Problem::of(ProblemKind::InvalidState, m))?;
                self.now = pinned;
                let now = self.now;
                self.exec(Command::TriggerTimers { now })?;
                self.exec(Command::ExpireJobs { now })?;
                Ok(None)
            }
            Op::GetFormByKey => lookup(nanobpmn_search::get::get_form_by_key(
                &self.read_model,
                param(0),
            )),
            Op::GetResource => lookup(nanobpmn_search::get::get_resource(
                &self.read_model,
                param(0),
            )),
            Op::GetAgentInstance => lookup(
                nanobpmn_search::get::get_agent_instance(param(0), |k| {
                    self.read_model.try_agent_instance(k)
                })
                .map_err(source_failure)?,
            ),
            Op::SearchUserTasks => {
                let q = search::<models::UserTaskSearchQuery>(raw)?;
                ok_json(&nanobpmn_search::user_task::search_user_tasks(
                    &self.read_model,
                    q.as_ref(),
                ))
            }
            Op::SearchProcessInstances => {
                let q = search::<models::ProcessInstanceSearchQuery>(raw)?;
                ok_json(
                    &nanobpmn_search::process_instance::search_process_instances(
                        &self.read_model,
                        q.as_ref(),
                    ),
                )
            }
            Op::SearchVariables => {
                let truncate = match query.get("truncateValues").map(String::as_str) {
                    None | Some("true") => true,
                    Some("false") => false,
                    Some(other) => {
                        return Err(Problem::invalid(format!(
                            "truncateValues must be true or false, got {other:?}"
                        )))
                    }
                };
                let q = search::<models::VariableSearchQuery>(raw)?;
                ok_json(&nanobpmn_search::variable::search_variables(
                    &self.read_model,
                    truncate,
                    q.as_ref(),
                ))
            }
            Op::SearchIncidents => {
                let q = search::<models::IncidentSearchQuery>(raw)?;
                ok_json(&nanobpmn_search::incident::search_incidents(
                    &self.read_model,
                    q.as_ref(),
                ))
            }
            Op::SearchAgentInstances => {
                let q = search::<models::AgentInstanceSearchQuery>(raw)?;
                let result = nanobpmn_search::agent::search_agent_instances(q.as_ref(), || {
                    self.read_model.try_agent_instances(
                        &nanobpmn_read_model::AgentInstanceFilter::default(),
                        None,
                    )
                })
                .map_err(source_failure)?;
                ok_json(&result)
            }
            Op::SearchAgentInstanceHistory => {
                let key = path_key("agentInstanceKey", param(0))?;
                let q = search::<models::AgentInstanceHistorySearchQuery>(raw)?;
                let result = nanobpmn_search::agent::search_agent_instance_history(
                    key,
                    q.as_ref(),
                    |filter| self.read_model.try_agent_history(filter, None),
                )
                .map_err(source_failure)?;
                ok_json(&result)
            }
        }
    }
}

/// The request headers (a JSON object of name → string value; empty ⇒ none).
fn parse_headers(raw: &str) -> Result<BTreeMap<String, String>, Problem> {
    if raw.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str(raw)
        .map_err(|e| Problem::invalid(format!("headers must be a JSON object of strings: {e}")))
}

/// The engine changeset for a REST user-task `Changeset`. A REST `null` (or an
/// absent field) preserves the attribute; dates are RFC 3339 strings.
fn changeset(c: &models::Changeset) -> UserTaskChangeset {
    UserTaskChangeset {
        candidate_groups: present(c.candidate_groups.as_ref()),
        candidate_users: present(c.candidate_users.as_ref()),
        due_date: present(c.due_date.as_ref()).map(|d| Some(d.to_rfc3339())),
        follow_up_date: present(c.follow_up_date.as_ref()).map(|d| Some(d.to_rfc3339())),
        priority: present(c.priority.as_ref()).map(i32::from),
    }
}

impl TestEngine {
    /// `POST /process-instances` → `CreateProcessInstanceResult`.
    fn create_process_instance(&mut self, raw: &str) -> Handled {
        use models::ProcessInstanceCreationInstruction as I;
        let req = required_body::<I>(raw)?;
        let (process_id, key, version, vars, tags, business_id, unsupported) = match &req {
            I::ProcessInstanceCreationInstructionById(r) => (
                r.process_definition_id.clone(),
                None,
                r.process_definition_version.filter(|v| *v > 0),
                r.variables.as_ref(),
                r.tags.clone(),
                r.business_id.clone(),
                r.start_instructions.is_some()
                    || r.runtime_instructions.is_some()
                    || r.await_completion == Some(true),
            ),
            I::ProcessInstanceCreationInstructionByKey(r) => {
                let key = path_key("processDefinitionKey", &r.process_definition_key.0)?;
                let process_id = self
                    .engine
                    .state()
                    .process_by_key(key)
                    .map(|p| p.definition.id.clone())
                    .ok_or_else(|| {
                        Problem::from(EngineError::ProcessNotFound {
                            process_id: key.to_string(),
                        })
                    })?;
                (
                    process_id,
                    Some(key),
                    r.process_definition_version.filter(|v| *v > 0),
                    r.variables.as_ref(),
                    r.tags.clone(),
                    r.business_id.clone(),
                    r.start_instructions.is_some()
                        || r.runtime_instructions.is_some()
                        || r.await_completion == Some(true),
                )
            }
        };
        // A request feature the in-browser engine does not implement is
        // rejected, never silently dropped.
        if unsupported {
            return Err(Problem::invalid(
                "createProcessInstance: startInstructions, runtimeInstructions and \
                 awaitCompletion are not supported by the in-browser engine",
            ));
        }
        let tags: Vec<String> = tags.unwrap_or_default().into_iter().map(|t| t.0).collect();
        let events = self.exec(Command::create_instance_versioned(
            process_id,
            variables(vars),
            tags,
            business_id,
            key,
            version,
        ))?;
        let created = events
            .iter()
            .find_map(|e| match e {
                Event::ProcessInstanceCreated {
                    instance_key,
                    process_id,
                    tags,
                    business_id,
                    process_definition_key,
                    version,
                    ..
                } => Some(models::CreateProcessInstanceResult {
                    process_definition_id: process_id.clone(),
                    process_definition_version: *version,
                    tenant_id: nanobpmn_engine_core::DEFAULT_TENANT.to_string(),
                    // Like the native gateway when `awaitCompletion` is not set
                    // (the only mode supported here): no variables echoed.
                    variables: Default::default(),
                    process_definition_key: models::ProcessDefinitionKey(
                        process_definition_key.to_string(),
                    ),
                    process_instance_key: models::ProcessInstanceKey(instance_key.to_string()),
                    tags: tags.iter().cloned().map(models::Tag).collect(),
                    business_id: business_id
                        .clone()
                        .map_or(types::Nullable::Null, types::Nullable::Present),
                    process_completed: false,
                }),
                _ => None,
            })
            .ok_or_else(|| Problem::internal("createProcessInstance: no instance was created"))?;
        let completed = events.iter().any(|e| {
            matches!(e, Event::ProcessInstanceCompleted { instance_key, .. }
                if instance_key.to_string() == created.process_instance_key.0)
        });
        ok_json(&models::CreateProcessInstanceResult {
            process_completed: completed,
            ..created
        })
    }

    /// `POST /jobs/activation` → `JobActivationResult`, each job shaped by the
    /// mapper the gateway uses (`nanobpmn_search::job`).
    ///
    /// Long-poll semantics (#996): the engine is synchronous, so there is
    /// nothing to wait *for* in-process. The `requestTimeout` long-poll
    /// parameter is accepted (and validated by the generated model) but never
    /// blocks: the call returns **immediately** with the jobs activatable at
    /// the current virtual instant — an empty list (200) when there are none.
    /// Consumers re-poll after advancing the virtual clock (`PUT /clock`), so
    /// a re-poll costs no real time. There is no streaming/SSE.
    fn activate_jobs_rest(&mut self, raw: &str) -> Handled {
        let req = required_body::<models::JobActivationRequest>(raw)?;
        // Native gateway parity (`activate_jobs_impl`): a non-positive
        // `maxJobsToActivate` drains nothing (200, empty list, no command) and
        // `timeout` is clamped to `>= 0` rather than rejected.
        let max_jobs = usize::try_from(req.max_jobs_to_activate.max(0)).unwrap_or(0);
        if max_jobs == 0 {
            return ok_json(&models::JobActivationResult { jobs: Vec::new() });
        }
        let timeout = u64::try_from(req.timeout.max(0)).unwrap_or(0);
        // An empty `fetchVariable` list means "all visible variables"
        // (`spec/jobs.yaml`), as the native gateway normalises it.
        let fetch = req.fetch_variable.clone().filter(|f| !f.is_empty());
        let now = self.now;
        let events = self.exec(Command::activate_jobs_with_options(
            req.r_type.clone(),
            // Native parity (`activate_jobs_impl`): no worker ⇒ `"default"`.
            req.worker.clone().unwrap_or_else(|| "default".to_string()),
            max_jobs,
            timeout,
            now,
            JobActivationOptions {
                fetch_variables: fetch.clone().unwrap_or_default(),
                with_lease: present(req.with_lease.as_ref()).unwrap_or(false),
            },
        ))?;
        let jobs = events
            .iter()
            .filter_map(|ev| match ev {
                Event::JobActivated { job_key, .. } => self.engine.activated_job(*job_key),
                _ => None,
            })
            .map(|j| {
                let (id, version, pdk) = (
                    j.bpmn_process_id.clone(),
                    j.process_definition_version,
                    j.process_definition_key.to_string(),
                );
                nanobpmn_search::job::activated_job_result(j, id, version, pdk, fetch.as_deref())
            })
            .collect();
        ok_json(&models::JobActivationResult { jobs })
    }
}

/// One resource part of a `multipart/form-data` deployment.
struct Part {
    filename: String,
    content: String,
}

/// Parse a `multipart/form-data` deployment body exactly as the native
/// gateway's `create_deployment_impl` does: the boundary via
/// `multer::parse_boundary` (axum's `Multipart` extractor), the parts via
/// `multer` itself, `tenantId` skipped, every other part a resource named by
/// its filename or `resource-<n>.bpmn` when it has none. Sharing the parser
/// (rather than re-implementing it) is what keeps the two transports from
/// drifting; `server/conformance/multipart-deploy.json` pins the parity.
fn multipart(headers: &BTreeMap<String, String>, raw: &str) -> Result<Vec<Part>, Problem> {
    let boundary = header(headers, "content-type")
        .and_then(|ct| multer::parse_boundary(ct).ok())
        .ok_or_else(|| {
            Problem::invalid(
                "createDeployment: expected a multipart/form-data body with a boundary",
            )
        })?;
    let body = bytes::Bytes::from(raw.to_owned());
    let stream =
        futures_util::stream::once(std::future::ready(Ok::<_, std::convert::Infallible>(body)));
    let mut form = multer::Multipart::new(stream, boundary);
    let malformed =
        |e: multer::Error| Problem::invalid(format!("Malformed multipart request: {e}."));
    let mut parts = Vec::new();
    while let Some(field) = poll_ready(form.next_field())?.map_err(malformed)? {
        let name = field.name().unwrap_or_default().to_string();
        let file_name = field.file_name().map(str::to_string);
        let data = poll_ready(field.bytes())?.map_err(malformed)?;
        if name == "tenantId" {
            continue;
        }
        let content = String::from_utf8(data.to_vec())
            .map_err(|_| Problem::invalid("A deployment resource was not valid UTF-8 BPMN XML."))?;
        let filename = file_name.unwrap_or_else(|| format!("resource-{}.bpmn", parts.len()));
        parts.push(Part { filename, content });
    }
    if parts.is_empty() {
        return Err(Problem::invalid(
            "At least one deployment resource is required.",
        ));
    }
    Ok(parts)
}

/// Drive a `multer` future to completion. The whole body is one ready
/// in-memory chunk, so the parser never waits on I/O; a `Pending` would be a
/// parser that needs more input than exists, i.e. a malformed body.
fn poll_ready<F: std::future::Future>(fut: F) -> Result<F::Output, Problem> {
    let mut fut = std::pin::pin!(fut);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(out) => Ok(out),
        std::task::Poll::Pending => Err(Problem::invalid(
            "Malformed multipart request: incomplete body.",
        )),
    }
}

/// A `DeploymentMetadataResult` with exactly one member set.
fn deployment_meta(kind: &str, value: serde_json::Value) -> serde_json::Value {
    let mut m = serde_json::json!({
        "processDefinition": null,
        "decisionDefinition": null,
        "decisionRequirements": null,
        "form": null,
        "resource": null,
    });
    m[kind] = value;
    m
}

impl TestEngine {
    /// `POST /deployments` (multipart) → `DeploymentResult`. Every resource is
    /// registered by ONE `DeployBundle` command, so co-deployed resources share
    /// one deployment key (#1193), as on the gateway.
    fn create_deployment(&mut self, headers: &BTreeMap<String, String>, raw: &str) -> Handled {
        let parts = multipart(headers, raw)?;
        let mut bundle = DeploymentBundle::default();
        // (resource name, deployed id) for the response.
        let mut processes: Vec<(String, String)> = Vec::new();
        let mut drgs: Vec<(String, String)> = Vec::new();
        for Part { filename, content } in parts {
            let lower = filename.to_ascii_lowercase();
            // Dispatch mirrors the gateway's `parse_deploy_resources`: `.bpmn`
            // is always BPMN; any `.xml` is BPMN iff it parses as BPMN, else
            // a generic resource.
            let xml_bpmn = if lower.ends_with(".xml") {
                parse_bpmn(&content).ok()
            } else {
                None
            };
            if lower.ends_with(".bpmn") || xml_bpmn.is_some() {
                let defs = match xml_bpmn {
                    Some(defs) => defs,
                    None => parse_bpmn(&content)
                        .map_err(|e| Problem::invalid(format!("{filename}: {e}")))?,
                };
                processes.extend(defs.iter().map(|d| (filename.clone(), d.id.clone())));
                bundle.processes.extend(defs);
            } else if lower.ends_with(".dmn") {
                let drg = nanobpmn_engine_core::dmn::parse_dmn(&content)
                    .map_err(|e| Problem::invalid(format!("{filename}: {e}")))?;
                drgs.push((filename.clone(), drg.id.clone()));
                bundle.decisions.push(drg);
            } else if lower.ends_with(".form") {
                let id = form_id_of(&content).ok_or_else(|| {
                    Problem::invalid(format!(
                        "{filename}: not a valid form-js document (expected a JSON object \
                         with a non-empty string \"id\")"
                    ))
                })?;
                bundle.forms.push(FormResource {
                    id,
                    resource_name: filename.clone(),
                    version_tag: nanobpmn_engine_core::form_version_tag_of(&content),
                    schema: content,
                });
            } else {
                bundle.resources.push(GenericResource {
                    resource_id: filename.clone(),
                    resource_name: filename,
                    content,
                    version_tag: None,
                });
            }
        }
        let forms: Vec<String> = bundle.forms.iter().map(|f| f.id.clone()).collect();
        let resources: Vec<String> = bundle
            .resources
            .iter()
            .map(|r| r.resource_id.clone())
            .collect();
        let events = self.exec(Command::DeployBundle(bundle))?;
        let deployment_key = events
            .iter()
            .find_map(|e| match e {
                Event::DeploymentCreated { deployment_key } => Some(*deployment_key),
                _ => None,
            })
            .ok_or_else(|| Problem::internal("createDeployment: no deployment was recorded"))?;
        // Resolve identities from post-apply state, not events: an idempotent
        // redeploy emits no definition event but still reports the existing
        // identity (native parity).
        let state = self.engine.state();
        let tenant = nanobpmn_engine_core::DEFAULT_TENANT;
        let missing = |what: &str| {
            Problem::internal(format!("createDeployment: {what} not found after deploy"))
        };
        let mut deployments = Vec::new();
        for (resource_name, id) in &processes {
            let p = state.processes.get(id).ok_or_else(|| missing(id))?;
            deployments.push(deployment_meta(
                "processDefinition",
                serde_json::json!({
                    "processDefinitionId": id,
                    "processDefinitionVersion": p.version,
                    "resourceName": resource_name,
                    "tenantId": tenant,
                    "processDefinitionKey": p.key.to_string(),
                }),
            ));
        }
        for (resource_name, id) in &drgs {
            let d = state
                .decision_requirements
                .get(id)
                .ok_or_else(|| missing(id))?;
            deployments.push(deployment_meta(
                "decisionRequirements",
                serde_json::json!({
                    "decisionRequirementsId": id,
                    "decisionRequirementsName": d.drg.name,
                    "version": d.version,
                    "resourceName": resource_name,
                    "tenantId": tenant,
                    "decisionRequirementsKey": d.key.to_string(),
                }),
            ));
            for decision in &d.drg.decisions {
                let dd = state
                    .decisions
                    .get(&decision.id)
                    .ok_or_else(|| missing(&decision.id))?;
                deployments.push(deployment_meta(
                    "decisionDefinition",
                    serde_json::json!({
                        "decisionDefinitionId": decision.id,
                        "version": dd.version,
                        "name": dd.decision_name,
                        "tenantId": tenant,
                        "decisionRequirementsId": id,
                        "decisionDefinitionKey": dd.key.to_string(),
                        "decisionRequirementsKey": d.key.to_string(),
                    }),
                ));
            }
        }
        for id in &forms {
            let f = state.forms.get(id).ok_or_else(|| missing(id))?;
            deployments.push(deployment_meta(
                "form",
                serde_json::json!({
                    "formId": id,
                    "version": f.version,
                    "resourceName": f.resource_name,
                    "tenantId": tenant,
                    "formKey": f.key.to_string(),
                }),
            ));
        }
        for id in &resources {
            let r = state.resources.get(id).ok_or_else(|| missing(id))?;
            deployments.push(deployment_meta(
                "resource",
                serde_json::json!({
                    "resourceId": id,
                    "resourceName": r.resource_name,
                    "version": r.version,
                    "tenantId": tenant,
                    "resourceKey": r.key.to_string(),
                }),
            ));
        }
        ok_json(&serde_json::json!({
            "deploymentKey": deployment_key.to_string(),
            "tenantId": tenant,
            "deployments": deployments,
        }))
    }
}
