//! Tests for the `handle(req) -> res` REST driver (#992): the route-table drift
//! guard against `spec/*.yaml`, the ProblemDetail mapping of every engine
//! rejection class, and an end-to-end flow driven only through `handle()`.

use nanobpmn_engine_core::EngineError;
use nanobpmn_search::models;
use serde_json::{json, Value as J};

use super::gateway::{
    BodyKind, HttpResponse, INJECTED_FAULT, ROUTES, UNSUPPORTED_MEDIA_TYPE_DETAIL,
};
use super::TestEngine;

/// Every vendored spec document, read from disk so a newly vendored file is
/// covered without touching this guard.
fn spec_documents() -> Vec<(String, serde_yaml::Value)> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../spec");
    let mut docs: Vec<(String, serde_yaml::Value)> = std::fs::read_dir(dir)
        .expect("spec dir")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .map(|p| {
            let src = std::fs::read_to_string(&p).expect("read spec");
            (
                p.display().to_string(),
                serde_yaml::from_str(&src).expect("spec parses"),
            )
        })
        .collect();
    docs.sort_by(|a, b| a.0.cmp(&b.0));
    docs
}

/// `(METHOD, path, operationId, success status, request body)` for every
/// spec operation.
fn spec_operations() -> Vec<(String, String, String, u16, BodyKind)> {
    let mut ops = Vec::new();
    for (_, doc) in spec_documents() {
        let Some(paths) = doc.get("paths").and_then(|p| p.as_mapping()) else {
            continue;
        };
        for (path, item) in paths {
            let path = path.as_str().expect("path key").to_string();
            let Some(item) = item.as_mapping() else {
                continue;
            };
            for (method, op) in item {
                let method = method.as_str().expect("method").to_ascii_uppercase();
                let Some(op_id) = op.get("operationId").and_then(|o| o.as_str()) else {
                    continue;
                };
                let status = op
                    .get("responses")
                    .and_then(|r| r.as_mapping())
                    .into_iter()
                    .flatten()
                    .filter_map(|(code, _)| match code {
                        serde_yaml::Value::String(s) => s.parse::<u16>().ok(),
                        serde_yaml::Value::Number(n) => n.as_u64().map(|n| n as u16),
                        _ => None,
                    })
                    .find(|c| (200..300).contains(c))
                    .expect("operation has a 2xx response");
                ops.push((
                    method,
                    path.clone(),
                    op_id.to_string(),
                    status,
                    body_kind(op),
                ));
            }
        }
    }
    ops
}

/// The request body an operation declares: absent, multipart, or JSON that is
/// `required` or not — the distinction that picks the native extractor.
fn body_kind(op: &serde_yaml::Value) -> BodyKind {
    let Some(rb) = op.get("requestBody") else {
        return BodyKind::None;
    };
    let multipart = rb
        .get("content")
        .and_then(|c| c.as_mapping())
        .is_some_and(|c| c.keys().any(|k| k.as_str() == Some("multipart/form-data")));
    if multipart {
        BodyKind::Multipart
    } else if rb.get("required").and_then(serde_yaml::Value::as_bool) == Some(true) {
        BodyKind::RequiredJson
    } else {
        BodyKind::OptionalJson
    }
}

/// Drift guard: every served route IS a spec operation — same operationId,
/// method, path template and success status. A route renamed, re-pathed or
/// re-statused in the spec fails here instead of silently serving the old shape.
#[test]
fn routes_match_the_spec() {
    let spec = spec_operations();
    assert!(!spec.is_empty(), "no spec operations found");
    for r in ROUTES {
        let found = spec
            .iter()
            .find(|(_, _, id, _, _)| id == r.operation_id)
            .unwrap_or_else(|| panic!("route {} is not a spec operation", r.operation_id));
        assert_eq!(
            (found.0.as_str(), found.1.as_str(), found.3, found.4),
            (r.method, r.path, r.status, r.body),
            "route {} drifted from the spec (method, path, success status, request body)",
            r.operation_id
        );
    }
    let mut ids: Vec<&str> = ROUTES.iter().map(|r| r.operation_id).collect();
    ids.sort_unstable();
    let n = ids.len();
    ids.dedup();
    assert_eq!(n, ids.len(), "an operationId is routed twice");
}

const BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d" targetNamespace="t">
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge">
      <bpmn:extensionElements><zeebe:taskDefinition type="charge" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="review">
      <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="review" />
    <bpmn:sequenceFlow id="f3" sourceRef="review" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="dia">
    <bpmndi:BPMNPlane id="plane" bpmnElement="order">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start"><dc:Bounds x="100" y="100" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="charge_di" bpmnElement="charge"><dc:Bounds x="190" y="78" width="100" height="80" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="review_di" bpmnElement="review"><dc:Bounds x="340" y="78" width="100" height="80" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end"><dc:Bounds x="490" y="100" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1"><di:waypoint x="136" y="118" /><di:waypoint x="190" y="118" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2"><di:waypoint x="290" y="118" /><di:waypoint x="340" y="118" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f3_di" bpmnElement="f3"><di:waypoint x="440" y="118" /><di:waypoint x="490" y="118" /></bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>"#;

fn call(eng: &mut TestEngine, method: &str, path: &str, body: &J) -> HttpResponse {
    let headers = json!({ "Content-Type": "application/json" }).to_string();
    eng.handle_request(method, path, &headers, &body.to_string())
}

fn json_body(res: &HttpResponse) -> J {
    serde_json::from_str(&res.body).expect("JSON body")
}

/// Decode a success body with the gateway's strict decoder into the spec
/// model: the response must be exactly the contract's shape.
fn decode<T: serde::de::DeserializeOwned + serde::Serialize>(res: &HttpResponse) -> T {
    nanobpmn_search::strict::decode_strict::<T>(res.body.as_bytes())
        .expect("response matches the spec model")
        .expect("non-empty body")
}

fn deploy(eng: &mut TestEngine) -> models::DeploymentResult {
    let boundary = "nano-boundary";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"resources\"; filename=\"order.bpmn\"\r\nContent-Type: application/xml\r\n\r\n{BPMN}\r\n--{boundary}--\r\n"
    );
    let headers = json!({ "Content-Type": format!("multipart/form-data; boundary={boundary}") });
    let res = eng.handle_request("POST", "/v2/deployments", &headers.to_string(), &body);
    assert_eq!(res.status, 200, "{}", res.body);
    decode(&res)
}

fn create(eng: &mut TestEngine) -> models::CreateProcessInstanceResult {
    let res = call(
        eng,
        "POST",
        "/v2/process-instances",
        &json!({ "processDefinitionId": "order", "variables": { "amount": 42 } }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    decode(&res)
}

fn activate(eng: &mut TestEngine) -> models::JobActivationResult {
    let res = call(
        eng,
        "POST",
        "/v2/jobs/activation",
        &json!({ "type": "charge", "maxJobsToActivate": 5, "timeout": 60000, "worker": "w" }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    decode(&res)
}

fn user_task_key(eng: &mut TestEngine) -> String {
    let res = call(eng, "POST", "/v2/user-tasks/search", &json!({}));
    assert_eq!(res.status, 200, "{}", res.body);
    let page = json_body(&res);
    page["items"][0]["userTaskKey"]
        .as_str()
        .expect("one user task")
        .to_string()
}

/// End to end, through `handle()` only: deploy → create → activate → complete
/// job → search/assign/complete user task → the instance is COMPLETED.
#[test]
fn drives_a_process_end_to_end() {
    let mut eng = TestEngine::new();
    let deployment = deploy(&mut eng);
    assert_eq!(deployment.deployments.len(), 1);

    let created = create(&mut eng);
    assert_eq!(created.process_definition_id, "order");
    // Gateway parity: no variables echoed without `awaitCompletion`.
    assert!(created.variables.is_empty(), "{:?}", created.variables);

    let jobs = activate(&mut eng).jobs;
    assert_eq!(jobs.len(), 1);
    let job_key = &jobs[0].job_key.0;
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{job_key}/completion"),
        &json!({ "variables": { "charged": true } }),
    );
    assert_eq!((res.status, res.body.as_str()), (204, ""));

    let task = user_task_key(&mut eng);
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/user-tasks/{task}/assignment"),
        &json!({ "assignee": "ada" }),
    );
    assert_eq!(res.status, 204, "{}", res.body);
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/user-tasks/{task}/completion"),
        &json!({}),
    );
    assert_eq!(res.status, 204, "{}", res.body);

    let res = call(
        &mut eng,
        "POST",
        "/v2/process-instances/search?ignored=1",
        &json!({ "filter": { "processInstanceKey": created.process_instance_key.0 } }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    assert_eq!(json_body(&res)["items"][0]["state"], json!("COMPLETED"));

    let res = call(
        &mut eng,
        "POST",
        "/v2/variables/search?truncateValues=false",
        &json!({ "filter": { "name": "charged" } }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    assert_eq!(json_body(&res)["items"][0]["value"], json!("true"));
}

/// #994: a no-retries `failJob` raises an incident that `searchIncidents`
/// returns, filtered by the server's algebra, and the instance reports
/// `hasIncident` on its process-instance result.
#[test]
fn no_retries_fail_job_incident_is_searchable() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    let failed = create(&mut eng);
    let healthy = create(&mut eng);
    let failed_key = &failed.process_instance_key.0;
    let jobs = activate(&mut eng).jobs;
    let job = jobs
        .iter()
        .find(|j| &j.process_instance_key.0 == failed_key)
        .expect("the failed instance's job");
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{}/failure", job.job_key.0),
        &json!({ "retries": 0, "errorMessage": "card declined" }),
    );
    assert_eq!(res.status, 204, "{}", res.body);

    let res = call(&mut eng, "POST", "/v2/incidents/search", &json!({}));
    assert_eq!(res.status, 200, "{}", res.body);
    let page: models::IncidentSearchQueryResult = decode(&res);
    assert_eq!(page.items.len(), 1, "{}", res.body);
    let body = json_body(&res);
    let inc = &body["items"][0];
    assert_eq!(inc["processInstanceKey"], json!(failed_key));
    assert_eq!(inc["elementId"], json!("charge"));
    assert_eq!(inc["errorType"], json!("JOB_NO_RETRIES"));
    assert_eq!(inc["errorMessage"], json!("card declined"));
    assert_eq!(inc["state"], json!("ACTIVE"));
    assert_eq!(inc["jobKey"], json!(job.job_key.0));

    // The server's filter algebra: an operator filter on the instance key.
    let res = call(
        &mut eng,
        "POST",
        "/v2/incidents/search",
        &json!({ "filter": { "processInstanceKey": { "$eq": healthy.process_instance_key.0 } } }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    assert_eq!(json_body(&res)["items"], json!([]));
    let res = call(
        &mut eng,
        "POST",
        "/v2/incidents/search",
        &json!({ "filter": { "unknownField": 1 } }),
    );
    assert_eq!(res.status, 400, "{}", res.body);

    // Incident fields on the process-instance result.
    let res = call(
        &mut eng,
        "POST",
        "/v2/process-instances/search",
        &json!({ "filter": { "hasIncident": true } }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    let body = json_body(&res);
    assert_eq!(body["items"].as_array().map(Vec::len), Some(1), "{body}");
    assert_eq!(body["items"][0]["processInstanceKey"], json!(failed_key));
    assert_eq!(body["items"][0]["hasIncident"], json!(true));

    // The positional method answers the same as the route.
    let direct: serde_json::Value =
        serde_json::from_str(&eng.search_incidents_json("{}").unwrap()).unwrap();
    let routed = json_body(&call(&mut eng, "POST", "/v2/incidents/search", &json!({})));
    assert_eq!(direct, routed);
}

/// Assert `res` is the ProblemDetail of the given status / title / detail.
fn assert_problem(res: &HttpResponse, status: u16, title: &str, detail: &str) {
    assert_eq!(res.status, status, "{}", res.body);
    assert_eq!(
        res.headers.get("content-type").map(String::as_str),
        Some("application/problem+json")
    );
    let p: models::ProblemDetail = serde_json::from_str(&res.body).expect("ProblemDetail");
    assert_eq!(p.r_type, "about:blank");
    assert_eq!(p.status, status);
    assert_eq!(p.title, title);
    assert_eq!(p.detail, detail);
}

/// Each rejection class maps through the SHARED `EngineError::problem()` —
/// status, title and detail are exactly what the server would send.
#[test]
fn engine_rejections_map_through_problem() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    let job_key = activate(&mut eng).jobs[0].job_key.0.clone();

    // 400 — an unknown process definition (INVALID_ARGUMENT class).
    let res = call(
        &mut eng,
        "POST",
        "/v2/process-instances",
        &json!({ "processDefinitionId": "nope" }),
    );
    let p = EngineError::ProcessNotFound {
        process_id: "nope".into(),
    }
    .problem();
    assert_eq!(p.status, 400);
    assert_problem(&res, p.status, p.title, &p.detail);

    // 404 — completing a job that does not exist.
    let res = call(&mut eng, "POST", "/v2/jobs/999999/completion", &json!({}));
    let p = EngineError::JobNotFound { job_key: 999_999 }.problem();
    assert_eq!(p.status, 404);
    assert_problem(&res, p.status, p.title, &p.detail);

    // 409 — assigning an already-assigned task without allowOverride.
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{job_key}/completion"),
        &json!({}),
    );
    assert_eq!(res.status, 204, "{}", res.body);
    let task = user_task_key(&mut eng);
    let assign = |eng: &mut TestEngine, who: &str| {
        call(
            eng,
            "POST",
            &format!("/v2/user-tasks/{task}/assignment"),
            &json!({ "assignee": who, "allowOverride": false }),
        )
    };
    assert_eq!(assign(&mut eng, "ada").status, 204);
    let res = assign(&mut eng, "bob");
    let p = EngineError::UserTaskAlreadyAssigned {
        user_task_key: task.parse().expect("key"),
    }
    .problem();
    assert_eq!(p.status, 409);
    assert_problem(&res, p.status, p.title, &p.detail);

    // 503 — the instance's state is unavailable (off-heap, #1388).
    INJECTED_FAULT
        .with(|f| *f.borrow_mut() = Some(EngineError::InstanceUnavailable { instance_key: 7 }));
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/user-tasks/{task}/completion"),
        &json!({}),
    );
    let p = EngineError::InstanceUnavailable { instance_key: 7 }.problem();
    assert_eq!(p.status, 503);
    assert_problem(&res, p.status, p.title, &p.detail);
}

/// Request-level rejections are ProblemDetails too: an unknown route (404), a
/// wrong method (405), an undefined body property and a malformed key (400).
#[test]
fn request_rejections_are_problem_details() {
    let mut eng = TestEngine::new();
    let res = call(&mut eng, "GET", "/v2/nothing-here", &json!(null));
    assert_eq!(res.status, 404);
    let res = call(&mut eng, "GET", "/v2/jobs/activation", &json!(null));
    assert_eq!(res.status, 405);
    let res = call(
        &mut eng,
        "POST",
        "/v2/process-instances",
        &json!({ "processDefinitionId": "order", "bogus": 1 }),
    );
    assert_eq!(res.status, 400, "{}", res.body);
    let res = call(&mut eng, "POST", "/v2/jobs/abc/completion", &json!({}));
    assert_eq!(res.status, 400, "{}", res.body);
    let p: models::ProblemDetail = serde_json::from_str(&res.body).expect("ProblemDetail");
    assert_eq!(p.instance, "/v2/jobs/abc/completion");
}

fn deploy_raw(eng: &mut TestEngine, filename: &str, content: &str) -> HttpResponse {
    let boundary = "nano-boundary";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"resources\"; filename=\"{filename}\"\r\n\r\n{content}\r\n--{boundary}--\r\n"
    );
    let headers = json!({ "Content-Type": format!("multipart/form-data; boundary={boundary}") });
    eng.handle_request("POST", "/v2/deployments", &headers.to_string(), &body)
}

/// Review #1997: BPMN under any `.xml` name registers the process (gateway parity).
#[test]
fn bpmn_under_plain_xml_name_is_deployed_as_a_process() {
    let mut eng = TestEngine::new();
    let res = deploy_raw(&mut eng, "order.xml", BPMN);
    assert_eq!(res.status, 200, "{}", res.body);
    assert!(
        json_body(&res)["deployments"][0]["processDefinition"].is_object(),
        "{}",
        res.body
    );
    create(&mut eng);
    // Non-BPMN XML stays a generic resource.
    let res = deploy_raw(&mut eng, "data.xml", "<a/>");
    assert_eq!(res.status, 200, "{}", res.body);
    assert!(
        json_body(&res)["deployments"][0]["resource"].is_object(),
        "{}",
        res.body
    );
}

/// Review #1997: boundary text inside content is NOT framing; content is kept
/// verbatim, and a body without its closing delimiter is rejected.
#[test]
fn multipart_only_frames_on_complete_delimiter_lines() {
    let mut eng = TestEngine::new();
    let content = "prefix--nano-boundary--suffix\n--nano-boundaryX";
    let res = deploy_raw(&mut eng, "notes.txt", content);
    assert_eq!(res.status, 200, "{}", res.body);

    let headers = json!({ "Content-Type": "multipart/form-data; boundary=nano-boundary" });
    let body = "--nano-boundary\r\nContent-Disposition: form-data; name=\"resources\"; filename=\"a.txt\"\r\n\r\nx\r\n";
    let res = eng.handle_request("POST", "/v2/deployments", &headers.to_string(), body);
    assert_eq!(res.status, 400, "{}", res.body);
}

/// Review #1997: conflicting lease tokens are rejected before any mutation, on
/// every job command; failure variables are rejected rather than dropped.
#[test]
fn job_commands_reject_conflicting_lease_tokens_and_fail_variables() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    let job = activate(&mut eng).jobs[0].job_key.0.clone();
    let conflict = json!({ "jobLeaseToken": "a", "leaseToken": "b" });
    for (suffix, extra) in [
        ("completion", json!({})),
        ("failure", json!({})),
        ("error", json!({ "errorCode": "E" })),
    ] {
        let mut body = conflict.clone();
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let res = call(&mut eng, "POST", &format!("/v2/jobs/{job}/{suffix}"), &body);
        assert_eq!(res.status, 400, "{suffix}: {}", res.body);
    }
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{job}/failure"),
        &json!({ "retries": 1, "variables": { "x": 1 } }),
    );
    assert_eq!(res.status, 400, "{}", res.body);
    // The job was never mutated: it still completes.
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{job}/completion"),
        &json!({}),
    );
    assert_eq!(res.status, 204, "{}", res.body);
}

/// Review #1997: an explicit empty `fetchVariable` returns all variables.
#[test]
fn empty_fetch_variable_returns_all_variables() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    let res = call(
        &mut eng,
        "POST",
        "/v2/jobs/activation",
        &json!({ "type": "charge", "maxJobsToActivate": 5, "timeout": 60000, "fetchVariable": [] }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    assert_eq!(json_body(&res)["jobs"][0]["variables"]["amount"], json!(42));
}

/// Review #1997: a `;` inside a quoted filename does not split the parameter.
#[test]
fn quoted_multipart_filename_keeps_semicolons() {
    let mut eng = TestEngine::new();
    let res = deploy_raw(&mut eng, "order;v2.bpmn", BPMN);
    assert_eq!(res.status, 200, "{}", res.body);
    assert!(
        json_body(&res)["deployments"][0]["processDefinition"].is_object(),
        "{}",
        res.body
    );
}

/// Review #1997: `operationReference` is rejected before mutation on every
/// mutating route, never accepted and silently dropped.
#[test]
fn operation_reference_is_rejected_not_dropped() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    let res = call(
        &mut eng,
        "POST",
        "/v2/process-instances",
        &json!({ "processDefinitionId": "order", "operationReference": 7 }),
    );
    assert_eq!(res.status, 400, "{}", res.body);
    let key = create(&mut eng).process_instance_key.0;
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/process-instances/{key}/cancellation"),
        &json!({ "operationReference": 7 }),
    );
    assert_eq!(res.status, 400, "{}", res.body);
    let res = call(
        &mut eng,
        "PUT",
        &format!("/v2/element-instances/{key}/variables"),
        &json!({ "variables": { "a": 1 }, "operationReference": 7 }),
    );
    assert_eq!(res.status, 400, "{}", res.body);
}

/// Review #1997: user-task `action` is rejected (never silently dropped) on
/// completion, assignment and update — before any mutation.
#[test]
fn user_task_action_is_rejected_before_mutation() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    let job_key = activate(&mut eng).jobs[0].job_key.0.clone();
    call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{job_key}/completion"),
        &json!({}),
    );
    let task = user_task_key(&mut eng);
    for (suffix, method, body) in [
        ("completion", "POST", json!({ "action": "approve" })),
        (
            "assignment",
            "POST",
            json!({ "assignee": "ada", "action": "approve" }),
        ),
        (
            "",
            "PATCH",
            json!({ "action": "approve", "changeset": { "priority": 10 } }),
        ),
    ] {
        let path = if suffix.is_empty() {
            format!("/v2/user-tasks/{task}")
        } else {
            format!("/v2/user-tasks/{task}/{suffix}")
        };
        let res = call(&mut eng, method, &path, &body);
        assert_eq!(res.status, 400, "{path}: {}", res.body);
    }
    let res = call(&mut eng, "POST", "/v2/user-tasks/search", &json!({}));
    let item = &json_body(&res)["items"][0];
    assert_eq!(item["state"], json!("CREATED"), "{item}");
    assert!(item.get("assignee").is_none_or(J::is_null), "{item}");
}

/// Review #1997: an empty assignee is a 400, not an assignment to "".
#[test]
fn empty_assignee_is_rejected() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    let job_key = activate(&mut eng).jobs[0].job_key.0.clone();
    call(
        &mut eng,
        "POST",
        &format!("/v2/jobs/{job_key}/completion"),
        &json!({}),
    );
    let task = user_task_key(&mut eng);
    let res = call(
        &mut eng,
        "POST",
        &format!("/v2/user-tasks/{task}/assignment"),
        &json!({ "assignee": "" }),
    );
    assert_eq!(res.status, 400, "{}", res.body);
}

/// Review #1997: `pinClock` sets the exact instant, including an earlier one.
#[test]
fn pin_clock_can_move_backwards() {
    let mut eng = TestEngine::new();
    for t in [2000, 1000] {
        let res = call(&mut eng, "PUT", "/v2/clock", &json!({ "timestamp": t }));
        assert_eq!(res.status, 204, "{}", res.body);
        assert_eq!(eng.now(), t as f64);
    }
}

/// Review #1997: a resource part is accepted whatever its form name.
#[test]
fn deployment_accepts_resource_parts_with_any_form_name() {
    let mut eng = TestEngine::new();
    let boundary = "nano-boundary";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"tenantId\"\r\n\r\n<default>\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"order.bpmn\"; filename=\"order.bpmn\"\r\n\r\n{BPMN}\r\n--{boundary}--\r\n"
    );
    let headers = json!({ "Content-Type": format!("multipart/form-data; boundary={boundary}") });
    let res = eng.handle_request("POST", "/v2/deployments", &headers.to_string(), &body);
    assert_eq!(res.status, 200, "{}", res.body);
}

/// Native parity (`activate_jobs_impl`): `timeout` is clamped to `>= 0` and a
/// non-positive `maxJobsToActivate` drains nothing — 200 `{jobs: []}`, never 400.
#[test]
fn activation_clamps_timeout_and_non_positive_max_like_native() {
    for max in [0, -1] {
        let mut eng = TestEngine::new();
        deploy(&mut eng);
        create(&mut eng);
        let res = call(
            &mut eng,
            "POST",
            "/v2/jobs/activation",
            &json!({ "type": "charge", "maxJobsToActivate": max, "timeout": 60000 }),
        );
        assert_eq!(res.status, 200, "{}", res.body);
        assert_eq!(json_body(&res), json!({ "jobs": [] }));
        // No command ran: the job is still activatable.
        assert_eq!(activate(&mut eng).jobs.len(), 1);
    }
    for timeout in [0, -5] {
        let mut eng = TestEngine::new();
        deploy(&mut eng);
        create(&mut eng);
        let res = call(
            &mut eng,
            "POST",
            "/v2/jobs/activation",
            &json!({ "type": "charge", "maxJobsToActivate": 5, "timeout": timeout }),
        );
        assert_eq!(res.status, 200, "{}", res.body);
        assert_eq!(json_body(&res)["jobs"].as_array().map(Vec::len), Some(1));
    }
}

/// Query keys and values are `application/x-www-form-urlencoded`-decoded.
#[test]
fn query_parameters_are_form_decoded() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    for q in ["truncateValues=%66alse", "%74runcateValues=false"] {
        let res = call(
            &mut eng,
            "POST",
            &format!("/v2/variables/search?{q}"),
            &json!({}),
        );
        assert_eq!(res.status, 200, "{q}: {}", res.body);
    }
    // `+` decodes to a space, so the value is not a boolean.
    let res = call(
        &mut eng,
        "POST",
        "/v2/variables/search?truncateValues=+false",
        &json!({}),
    );
    assert_eq!(res.status, 400, "{}", res.body);
    for bad in [
        "truncateValues=%6",
        "truncateValues=%zz",
        "%G1=x",
        "a=%ff",
        "truncateValues=%+fals",
        "a=%-1",
    ] {
        let res = call(
            &mut eng,
            "POST",
            &format!("/v2/variables/search?{bad}"),
            &json!({}),
        );
        assert_eq!(res.status, 400, "{bad}: {}", res.body);
    }
}

/// Review #1997 (operator): activation without a `worker` attributes the jobs
/// to `"default"`, as the native `activate_jobs_impl` does — never `""`.
#[test]
fn activation_without_worker_attributes_jobs_to_default() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    create(&mut eng);
    let res = call(
        &mut eng,
        "POST",
        "/v2/jobs/activation",
        &json!({ "type": "charge", "maxJobsToActivate": 5, "timeout": 60000 }),
    );
    assert_eq!(res.status, 200, "{}", res.body);
    assert_eq!(json_body(&res)["jobs"][0]["worker"], json!("default"));
}

/// Review #1997 (operator): the shared multipart fixtures
/// (`server/conformance/multipart-deploy.json`, also run against the native
/// gateway by `server/src/wasm_conformance_tests.rs`) produce the expected
/// status and deployed resource names through `handle()`.
#[test]
fn multipart_deploy_fixtures_match_the_native_gateway_expectations() {
    for f in multipart_fixtures() {
        let mut eng = TestEngine::new();
        let headers = json!({ "Content-Type": f.content_type });
        let res = eng.handle_request("POST", "/v2/deployments", &headers.to_string(), &f.body);
        assert_eq!(res.status, f.status, "{}: {}", f.name, res.body);
        if res.status == 200 {
            assert_eq!(resource_names(&json_body(&res)), f.resources, "{}", f.name);
        }
    }
}

struct MultipartFixture {
    name: String,
    content_type: String,
    body: String,
    status: u16,
    resources: Vec<String>,
}

fn multipart_fixtures() -> Vec<MultipartFixture> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../server/conformance");
    let bpmn = std::fs::read_to_string(format!("{dir}/conformance-service.bpmn")).unwrap();
    let raw: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{dir}/multipart-deploy.json")).unwrap(),
    )
    .unwrap();
    raw["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| MultipartFixture {
            name: f["name"].as_str().unwrap().to_string(),
            content_type: f["contentType"].as_str().unwrap().to_string(),
            body: f["body"].as_str().unwrap().replace("{{BPMN}}", &bpmn),
            status: u16::try_from(f["status"].as_u64().unwrap()).unwrap(),
            resources: f["resources"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r.as_str().unwrap().to_string())
                .collect(),
        })
        .collect()
}

/// Every `resourceName` in a `DeploymentResult`, in response order.
fn resource_names(v: &serde_json::Value) -> Vec<String> {
    v["deployments"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|d| d.as_object().unwrap().values())
        .filter_map(|m| m.get("resourceName").and_then(|n| n.as_str()))
        .map(str::to_string)
        .collect()
}

/// #996: `activateJobs` long-poll over the ABI returns immediately — the
/// `requestTimeout` is accepted but never blocks, an empty activation is 200
/// with an empty list, and after the virtual clock advances past a lock the
/// next poll returns the now-activatable job.
#[test]
fn activation_long_poll_returns_immediately_and_repolls_on_virtual_clock() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    let poll = |eng: &mut TestEngine| {
        let started = std::time::Instant::now();
        let res = call(
            eng,
            "POST",
            "/v2/jobs/activation",
            &json!({ "type": "charge", "maxJobsToActivate": 5, "timeout": 1000,
                     "worker": "w", "requestTimeout": 600_000 }),
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(res.status, 200, "{}", res.body);
        json_body(&res)["jobs"].as_array().map(Vec::len)
    };
    // Nothing activatable yet: 200 + empty list, promptly.
    assert_eq!(poll(&mut eng), Some(0));
    create(&mut eng);
    assert_eq!(poll(&mut eng), Some(1));
    // Locked to the first worker: the re-poll is empty, not blocking.
    assert_eq!(poll(&mut eng), Some(0));
    // Advance the virtual clock past the 1s lock; the next poll returns it.
    let res = call(
        &mut eng,
        "PUT",
        "/v2/clock",
        &json!({ "timestamp": 10_000_000_000_000i64 }),
    );
    assert!(res.status < 300, "{}", res.body);
    assert_eq!(poll(&mut eng), Some(1));
}

/// #2020: a JSON-bodied operation with an unsupported `Content-Type` is a 415
/// BEFORE any mutation — the instance is not created — exactly as the native
/// gateway's `Json` / `OptionalJson` extractors answer.
#[test]
fn unsupported_media_type_is_rejected_before_any_mutation() {
    let mut eng = TestEngine::new();
    deploy(&mut eng);
    let body = json!({ "processDefinitionId": "order" }).to_string();
    for ct in [
        None,
        Some("text/plain"),
        Some("application/xml"),
        Some("json"),
    ] {
        let headers = match ct {
            Some(ct) => json!({ "Content-Type": ct }).to_string(),
            None => String::new(),
        };
        let res = eng.handle_request("POST", "/v2/process-instances", &headers, &body);
        assert_eq!(res.status, 415, "{ct:?}: {}", res.body);
        assert_eq!(
            res.headers.get("content-type").map(String::as_str),
            Some("application/problem+json")
        );
        let p = json_body(&res);
        assert_eq!(p["status"], json!(415));
        assert_eq!(p["detail"], json!(UNSUPPORTED_MEDIA_TYPE_DETAIL));
    }
    let search = call(&mut eng, "POST", "/v2/process-instances/search", &json!({}));
    assert_eq!(
        json_body(&search)["page"]["totalItems"],
        json!(0),
        "nothing ran"
    );

    // `application/*+json` and parameters are accepted, as by axum.
    for ct in [
        "application/vnd.nano+json",
        "application/json; charset=utf-8",
    ] {
        let headers = json!({ "content-type": ct }).to_string();
        let res = eng.handle_request("POST", "/v2/process-instances", &headers, &body);
        assert_eq!(res.status, 200, "{ct}: {}", res.body);
    }
}

/// #2020: an OPTIONAL body that is empty needs no `Content-Type`; a non-empty
/// one does. A body-less operation never inspects it.
#[test]
fn optional_and_absent_bodies_follow_the_native_media_type_rule() {
    let mut eng = TestEngine::new();
    let res = eng.handle_request("POST", "/v2/jobs/1/completion", "", "");
    assert_ne!(res.status, 415, "empty optional body: {}", res.body);
    let res = eng.handle_request("POST", "/v2/jobs/1/completion", "", "{}");
    assert_eq!(res.status, 415, "non-empty optional body: {}", res.body);
    let res = eng.handle_request("POST", "/v2/user-tasks/search", "", "{}");
    assert_eq!(res.status, 415, "search body: {}", res.body);
    let res = eng.handle_request("DELETE", "/v2/user-tasks/1/assignee", "", "");
    assert_ne!(res.status, 415, "no body: {}", res.body);
    // A required body is checked even when empty.
    let res = eng.handle_request("PUT", "/v2/clock", "", "");
    assert_eq!(res.status, 415, "required body: {}", res.body);
}

/// #2020: the multipart `tenantId` part is echoed in the deployment result,
/// as the native gateway does (a blank one falls back to `<default>`).
#[test]
fn deployment_echoes_the_multipart_tenant_id() {
    for (part, want) in [("acme", "acme"), ("  ", "<default>")] {
        let mut eng = TestEngine::new();
        let body = format!(
            "--b\r\nContent-Disposition: form-data; name=\"tenantId\"\r\n\r\n{part}\r\n--b\r\nContent-Disposition: form-data; name=\"resources\"; filename=\"order.bpmn\"\r\n\r\n{BPMN}\r\n--b--\r\n"
        );
        let headers = json!({ "Content-Type": "multipart/form-data; boundary=b" });
        let res = eng.handle_request("POST", "/v2/deployments", &headers.to_string(), &body);
        assert_eq!(res.status, 200, "{}", res.body);
        let v = json_body(&res);
        assert_eq!(v["tenantId"], json!(want));
        assert_eq!(
            v["deployments"][0]["processDefinition"]["tenantId"],
            json!(want)
        );
    }
}

/// #2020: the shared REST parity fixtures (`server/conformance/rest-parity.json`,
/// also run against the native gateway by
/// `server/src/wasm_conformance_tests.rs`, which compares the two answers)
/// produce the expected status and process-instance count through `handle()`.
#[test]
fn rest_parity_fixtures_match_the_native_gateway_expectations() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../server/conformance");
    let bpmn = std::fs::read_to_string(format!("{dir}/conformance-service.bpmn")).unwrap();
    let raw: J =
        serde_json::from_str(&std::fs::read_to_string(format!("{dir}/rest-parity.json")).unwrap())
            .unwrap();
    let deploy_headers = json!({ "content-type": "multipart/form-data; boundary=b" }).to_string();
    let deploy_body = format!(
        "--b\r\nContent-Disposition: form-data; name=\"resources\"; filename=\"svc.bpmn\"\r\n\r\n{bpmn}\r\n--b--\r\n"
    );
    for f in raw["fixtures"].as_array().unwrap() {
        let name = f["name"].as_str().unwrap();
        let mut eng = TestEngine::new();
        let d = eng.handle_request("POST", "/v2/deployments", &deploy_headers, &deploy_body);
        assert_eq!(d.status, 200, "{name}: setup {}", d.body);
        for (i, st) in f["setup"].as_array().into_iter().flatten().enumerate() {
            let h = st["contentType"].as_str().map_or(String::new(), |ct| {
                json!({ "content-type": ct }).to_string()
            });
            let r = eng.handle_request(
                st["method"].as_str().unwrap(),
                st["path"].as_str().unwrap(),
                &h,
                st["body"].as_str().unwrap(),
            );
            assert_eq!(r.status, 200, "{name}: setup step {i}: {}", r.body);
        }
        let headers = f["contentType"].as_str().map_or(String::new(), |ct| {
            json!({ "content-type": ct }).to_string()
        });
        let body = f["body"].as_str().unwrap().replace("{{BPMN}}", &bpmn);
        let res = eng.handle_request(
            f["method"].as_str().unwrap(),
            f["path"].as_str().unwrap(),
            &headers,
            &body,
        );
        assert_eq!(
            Some(u64::from(res.status)),
            f["status"].as_u64(),
            "{name}: {}",
            res.body
        );
        let count = call(&mut eng, "POST", "/v2/process-instances/search", &json!({}));
        assert_eq!(
            json_body(&count)["page"]["totalItems"],
            f["instancesAfter"],
            "{name}: instances after"
        );
    }
}
