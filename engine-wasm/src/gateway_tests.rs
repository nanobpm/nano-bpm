//! Tests for the `handle(req) -> res` REST driver (#992): the route-table drift
//! guard against `spec/*.yaml`, the ProblemDetail mapping of every engine
//! rejection class, and an end-to-end flow driven only through `handle()`.

use nanobpmn_engine_core::EngineError;
use nanobpmn_search::models;
use serde_json::{json, Value as J};

use super::gateway::{HttpResponse, INJECTED_FAULT, ROUTES};
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

/// `(METHOD, path, operationId, success status)` for every spec operation.
fn spec_operations() -> Vec<(String, String, String, u16)> {
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
                ops.push((method, path.clone(), op_id.to_string(), status));
            }
        }
    }
    ops
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
            .find(|(_, _, id, _)| id == r.operation_id)
            .unwrap_or_else(|| panic!("route {} is not a spec operation", r.operation_id));
        assert_eq!(
            (found.0.as_str(), found.1.as_str(), found.3),
            (r.method, r.path, r.status),
            "route {} drifted from the spec (method, path, success status)",
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
    eng.handle_request(method, path, "", &body.to_string())
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
    // The framing keeps the content byte-for-byte.
    let body = format!("--nano-boundary\r\nH: v\r\n\r\n{content}\r\n--nano-boundary--\r\n");
    let parts = super::gateway::split_multipart(&body, "nano-boundary").expect("framed");
    assert_eq!(parts, vec![format!("H: v\r\n\r\n{content}")]);

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

/// Review #1997: adjacent delimiter lines are malformed (400), never a panic.
#[test]
fn adjacent_multipart_delimiters_are_rejected_without_panicking() {
    assert!(super::gateway::split_multipart("--b\r\n--b--\r\n", "b").is_err());
    assert!(super::gateway::split_multipart("--b\n--b--", "b").is_err());
}

/// Review #1997: a `;` inside a quoted filename does not split the parameter.
#[test]
fn quoted_multipart_filename_keeps_semicolons() {
    assert_eq!(
        super::gateway::split_params(r#"form-data; name="resources"; filename="order;v2.bpmn""#),
        vec![
            "form-data",
            r#" name="resources""#,
            r#" filename="order;v2.bpmn""#
        ]
    );
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
