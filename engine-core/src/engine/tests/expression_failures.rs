//! Expression-evaluation failures raise Zeebe-matching incidents instead of
//! silently falling back (#1390, #1391 step 4). Ported from Zeebe's
//! `JobWorkerElementIncidentTest`.
use super::*;

fn job_type_process(job_type: &str) -> ProcessDefinition {
    ProcessBuilder::new("jt")
        .start_event("start")
        .service_task("work", job_type)
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap()
}

/// Deploys `def`, creates an instance of `proc_id` with `vars_in`, and returns
/// `(engine, instance_key)`.
fn start(def: ProcessDefinition, proc_id: &str, vars_in: HashMap<String, Value>) -> (Engine, Key) {
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(proc_id, vars_in))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    (engine, inst)
}

/// The single active incident; asserts it is an `EXTRACT_VALUE_ERROR`
/// (`ExpressionEvaluation`) on `element_id`.
fn extract_value_incident(engine: &Engine, element_id: &str) -> state::Incident {
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "exactly one incident: {incidents:?}");
    let incident = incidents[0].clone();
    assert_eq!(incident.kind, state::IncidentKind::ExpressionEvaluation);
    assert_eq!(incident.element_id, element_id);
    incident
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_evaluation_failed() {
    // #1390: the old behaviour created a job whose type was the expression text.
    let (engine, _) = start(job_type_process("=jobType"), "jt", HashMap::new());
    assert!(engine.pending_jobs().is_empty(), "no job is created");
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("'=jobType'") && incident.reason.contains("'NULL'"),
        "{}",
        incident.reason
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_of_invalid_type() {
    let (engine, _) = start(
        job_type_process("=jobType"),
        "jt",
        vars(&[("jobType", Value::Int(42))]),
    );
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("to be 'STRING', but was 'NUMBER'"),
        "{}",
        incident.reason
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_is_empty_string() {
    let (engine, _) = start(job_type_process("=\"\""), "jt", HashMap::new());
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("not-empty string"),
        "{}",
        incident.reason
    );
}

#[test]
fn should_create_incident_if_job_type_exceeds_max_length() {
    let long = "x".repeat(crate::engine::expression::MAX_JOB_TYPE_LENGTH + 1);
    let (engine, _) = start(
        job_type_process("=jobType"),
        "jt",
        vars(&[("jobType", Value::Str(long))]),
    );
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("maximum length"),
        "{}",
        incident.reason
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_resolve_incident_after_job_type_expression_evaluation_failed() {
    let (mut engine, inst) = start(job_type_process("=jobType"), "jt", HashMap::new());
    let incident = extract_value_incident(&engine, "work");
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("jobType", Value::Str("payment".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert!(engine.active_incidents().is_empty());
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1, "resolving retries the job creation");
    assert_eq!(jobs[0].job_type, "payment");
    assert_eq!(jobs[0].element_instance_key, incident.element_instance_key);
    // ...and the instance runs to completion from there.
    complete_one(&mut engine, "payment");
    assert!(engine.is_completed(inst));
}

#[test]
fn resolving_with_still_failing_job_type_raises_a_fresh_incident() {
    let (mut engine, _) = start(job_type_process("=jobType"), "jt", HashMap::new());
    let incident = extract_value_incident(&engine, "work");
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert!(engine.pending_jobs().is_empty());
    let again = extract_value_incident(&engine, "work");
    assert_ne!(again.key, incident.key);
}

#[test]
fn retries_expression_of_invalid_type_raises_incident_and_no_job() {
    // Zeebe `BpmnJobBehavior.evalRetriesExp` -> `evaluateLongExpression`.
    let engine = deploy_and_create_retryable(
        Some("=maxRetries"),
        vars(&[("maxRetries", Value::Str("many".into()))]),
    );
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("to be 'NUMBER', but was 'STRING'"),
        "{}",
        incident.reason
    );
}

#[test]
fn multi_instance_child_job_type_failure_raises_incident_on_the_child() {
    let def = ProcessBuilder::new("mi-jt")
        .start_event("start")
        .service_task("work", "=\"w-\" + item")
        .with_multi_instance(
            "work",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: true,
            },
        )
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    // `item` 7 makes `"w-" + 7` fail to evaluate (string + number).
    let (engine, _) = start(
        def,
        "mi-jt",
        vars(&[("items", Value::List(vec![Value::Int(7)]))]),
    );
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("Expected to evaluate expression"),
        "{}",
        incident.reason
    );
}

#[test]
fn message_catch_name_expression_failure_raises_incident_and_reopens_on_resolve() {
    // Zeebe evaluates the message name with `evaluateStringExpression` when
    // opening the subscription; a failure is an EXTRACT_VALUE_ERROR incident,
    // not a subscription named after the expression text.
    let def = ProcessBuilder::new("msg-name")
        .start_event("start")
        .message_intermediate_catch_event("await", "=msgName", "k")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = start(def, "msg-name", vars(&[("k", Value::Str("c-1".into()))]));
    assert!(engine.message_subscriptions().is_empty());
    let incident = extract_value_incident(&engine, "await");
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("msgName", Value::Str("answered".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].message_name, "answered");
}

#[test]
fn decision_id_expression_failure_raises_extract_value_incident() {
    // Zeebe `BpmnDecisionBehavior.evaluateDecision` evaluates the decision id
    // with `evaluateStringExpression`: a failure is EXTRACT_VALUE_ERROR, not a
    // lookup of a decision named after the expression text.
    let def = ProcessBuilder::new("brt")
        .start_event("start")
        .business_rule_task("decide", "=decisionId", Some("result".into()))
        .end_event("end")
        .connect("start", "decide")
        .connect("decide", "end")
        .build()
        .unwrap();
    let (engine, _) = start(def, "brt", HashMap::new());
    let incident = extract_value_incident(&engine, "decide");
    assert!(
        incident.reason.contains("'=decisionId'"),
        "{}",
        incident.reason
    );
}

#[test]
fn multi_instance_child_job_property_resolve_does_not_reapply_inputs() {
    // #1399 review: a job-property failure on an MI child happens AFTER its
    // per-child input mappings succeeded. Resolving it must re-run only the
    // child's behaviour — re-applying a non-idempotent input mapping
    // (`=item + 1 -> item`) would increment `item` once per resolution.
    let def = ProcessBuilder::new("mi-jt-io")
        .start_event("start")
        .service_task("work", "=jobType")
        .with_multi_instance(
            "work",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: true,
            },
        )
        .with_io(
            "work",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=item + 1".to_string(),
                    target: "item".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = start(
        def,
        "mi-jt-io",
        vars(&[("items", Value::List(vec![Value::Int(1)]))]),
    );
    // Resolve twice with the job type still missing, then once fixed.
    for _ in 0..2 {
        let incident = extract_value_incident(&engine, "work");
        engine
            .apply_command(Command::resolve_incident(incident.key))
            .unwrap();
    }
    let incident = extract_value_incident(&engine, "work");
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("jobType", Value::Str("payment".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1, "resolving creates the child's job");
    let scope = engine.variables_for_element(inst, jobs[0].element_instance_key);
    assert_eq!(
        scope.get("item"),
        Some(&Value::Int(2)),
        "input mapping applied exactly once"
    );
}

/// The lifecycle labels (`Activating`/`Activated`/…) emitted for `element_id`.
fn lifecycle_of(events: &[Event], element_id: &str) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::ElementActivating { element_id: id, .. } if id == element_id => {
                Some("Activating")
            }
            Event::ElementActivated { element_id: id, .. } if id == element_id => Some("Activated"),
            Event::ElementCompleting { element_id: id, .. } if id == element_id => {
                Some("Completing")
            }
            Event::ElementCompleted { element_id: id, .. } if id == element_id => Some("Completed"),
            _ => None,
        })
        .collect()
}

/// Zeebe `JobWorkerTaskProcessor.onActivate` evaluates the job expressions
/// BEFORE `transitionToActivated`: a failure leaves the element in
/// `ELEMENT_ACTIVATING` with the incident, and only the successful retry on
/// resolution writes `ELEMENT_ACTIVATED` (then the job). Found by the
/// `job-type-expression-failure` Zeebe trace (formal/traces, #1391).
#[test]
fn job_type_expression_failure_parks_the_element_in_activating() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(job_type_process("=jobType")))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with("jt", HashMap::new()))
        .unwrap();
    assert_eq!(lifecycle_of(&created, "work"), vec!["Activating"]);
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    let incident = extract_value_incident(&engine, "work");
    // The parked token is still a live token of the instance.
    assert!(!engine.is_completed(inst));

    // A still-failing retry stays in ACTIVATING.
    let again = engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert!(lifecycle_of(&again, "work").is_empty(), "{again:?}");
    let incident = extract_value_incident(&engine, "work");

    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("jobType", Value::Str("payment".into()))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert_eq!(lifecycle_of(&resolved, "work"), vec!["Activated"]);
    let activated = resolved
        .iter()
        .position(|e| matches!(e, Event::ElementActivated { .. }))
        .unwrap();
    let job = resolved
        .iter()
        .position(|e| matches!(e, Event::JobCreated { .. }))
        .unwrap();
    assert!(activated < job, "ACTIVATED precedes the job: {resolved:?}");
    complete_one(&mut engine, "payment");
    assert!(engine.is_completed(inst));
}

/// Same lifecycle for an input-mapping failure (Zeebe applies input mappings in
/// `onActivate`, before `transitionToActivated`).
#[test]
fn input_mapping_failure_parks_the_element_in_activating() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="io" isExecutable="true">
    <bpmn:startEvent id="start"/>
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="w"/>
        <zeebe:ioMapping><zeebe:input source="=missing.x + 1" target="y"/></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end"/>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with("io", HashMap::new()))
        .unwrap();
    assert_eq!(lifecycle_of(&created, "work"), vec!["Activating"]);
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    let incident = engine.active_incidents()[0].clone();
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[(
                "missing",
                Value::Map(std::collections::BTreeMap::from([(
                    "x".into(),
                    Value::Int(1),
                )])),
            )]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert_eq!(lifecycle_of(&resolved, "work"), vec!["Activated"]);
    complete_one(&mut engine, "w");
    assert!(engine.is_completed(inst));
}
