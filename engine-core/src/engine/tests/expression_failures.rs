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
    assert_eq!(incident.kind, state::IncidentKind::ExtractValueError);
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
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    // `item` 7 makes `"w-" + 7` a type error, which feel-scala suppresses to
    // `null` (#1392) — not a string, so the job type is rejected.
    let (engine, _) = start(
        def,
        "mi-jt",
        vars(&[("items", Value::List(vec![Value::Int(7)]))]),
    );
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work");
    assert!(
        incident.reason.contains("to be 'STRING', but was 'NULL'"),
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
                cancel_remaining_instances: true,
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
        <zeebe:ioMapping><zeebe:input source="=assert(missing.x, missing.x != null) + 1" target="y"/></zeebe:ioMapping>
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

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
/// `JobWorkerElementIncidentTest#shouldResolveIncidentIfTaskIsTerminated`:
/// cancelling the instance terminates the incident's element, and Zeebe
/// resolves the incident with it — it must not linger as an open incident on
/// a terminated instance.
#[test]
fn should_resolve_incident_if_task_is_terminated() {
    let (mut engine, inst) = start(job_type_process("=jobType"), "jt", HashMap::new());
    let incident = extract_value_incident(&engine, "work");
    let events = engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::IncidentResolved { incident_key, .. } if *incident_key == incident.key
        )),
        "cancel resolves the element's incident: {events:?}"
    );
    assert!(engine.active_incidents().is_empty());
}

// ---------------------------------------------------------------------------
// #1402: a split gateway (exclusive, inclusive fork) selects its outgoing flows
// in Zeebe's `onActivate` (`ExclusiveGatewayProcessor.findSequenceFlowToTake`,
// `InclusiveGatewayProcessor.findSequenceFlowsToTake`) — BEFORE
// `transitionToActivated`. A routing failure (no matching flow, or a condition
// that fails to evaluate) therefore raises its CONDITION_ERROR incident while
// the gateway is still ACTIVATING; resolving re-runs the selection and only
// then emits ACTIVATED → COMPLETING → COMPLETED. Guarded for the whole class:
// both split-gateway kinds × both failure modes.
// ---------------------------------------------------------------------------

/// Which split gateway the routing-failure fixture uses.
#[derive(Clone, Copy, Debug)]
enum SplitGateway {
    Exclusive,
    Inclusive,
}

/// `start → g (split) → end`, single conditional flow `condition`, no default.
fn split_gateway_process(gateway: SplitGateway, condition: &str) -> ProcessDefinition {
    let builder = ProcessBuilder::new("split").start_event("s");
    let builder = match gateway {
        SplitGateway::Exclusive => builder.exclusive_gateway("g"),
        SplitGateway::Inclusive => builder.inclusive_gateway("g"),
    };
    builder
        .end_event("e")
        .connect("s", "g")
        .connect_when("g", "e", condition)
        .build()
        .unwrap()
}

/// Drives one routing failure end to end and asserts the Zeebe lifecycle:
/// the incident is raised with `g` only ACTIVATING, and resolution (after the
/// variables are fixed) activates, completes and routes it.
fn assert_routing_failure_parks_activating(
    gateway: SplitGateway,
    bad: Value,
    expected_kind: state::IncidentKind,
) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(split_gateway_process(
            gateway,
            "amount > 100",
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split",
            HashMap::from([("amount".to_string(), bad)]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating"],
        "{gateway:?}: the routing incident is raised while ACTIVATING: {created:?}"
    );
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "{gateway:?}: {incidents:?}");
    assert_eq!(incidents[0].kind, expected_kind, "{gateway:?}");
    assert_eq!(incidents[0].element_id, "g");
    let incident_key = incidents[0].key;
    let raised_at = created
        .iter()
        .position(|e| matches!(e, Event::IncidentRaised { .. }))
        .unwrap();
    assert!(
        !created[..raised_at]
            .iter()
            .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "g")),
        "{gateway:?}: no ELEMENT_ACTIVATED before the incident"
    );

    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&resolved, "g"),
        vec!["Activated", "Completing", "Completed"],
        "{gateway:?}: resolution re-runs the full gated activation ON the \
         parked element instance (Zeebe's resolve re-drives `onActivate` on \
         the same element instance — no second `ELEMENT_ACTIVATING`): \
         {resolved:?}"
    );
    assert!(engine.active_incidents().is_empty());
    assert!(
        engine.is_completed(inst),
        "{gateway:?}: token routed to the end"
    );
}

#[test]
fn exclusive_gateway_no_matching_flow_raises_incident_while_activating() {
    assert_routing_failure_parks_activating(
        SplitGateway::Exclusive,
        Value::Int(5),
        state::IncidentKind::NoMatchingSequenceFlow,
    );
}

#[test]
fn exclusive_gateway_condition_failure_raises_incident_while_activating() {
    assert_routing_failure_parks_activating(
        SplitGateway::Exclusive,
        Value::Str("five".into()),
        state::IncidentKind::ExpressionEvaluation,
    );
}

#[test]
fn inclusive_split_no_matching_flow_raises_incident_while_activating() {
    assert_routing_failure_parks_activating(
        SplitGateway::Inclusive,
        Value::Int(5),
        state::IncidentKind::NoMatchingSequenceFlow,
    );
}

#[test]
fn inclusive_split_condition_failure_raises_incident_while_activating() {
    assert_routing_failure_parks_activating(
        SplitGateway::Inclusive,
        Value::Str("five".into()),
        state::IncidentKind::ExpressionEvaluation,
    );
}

// ---------------------------------------------------------------------------
// #1402 review hardening — three defect classes in the activation-time gate:
//
//  1. The activation-time selection must evaluate against the same post-input
//     view the rest of the activation body uses (`activation_vars`): input
//     mappings and sub-process locals are not yet applied to state when the
//     body runs, so a selector that re-reads state sees only the root scope.
//  2. The selection made while ACTIVATING must be carried through completion:
//     FEEL conditions are not required to be deterministic, so re-evaluating
//     at `Step::Complete` can pick a different route (or raise an incident
//     after `ElementActivated`).
//  3. A `start` execution-listener chain defers the activation body; the
//     routing gate must still apply when the deferred body runs, so no
//     routing incident is raised after `ElementActivated`.
// ---------------------------------------------------------------------------

/// `start → g (split, input mapping `amount = 500`) → end`, one conditional
/// flow `amount > 100`. The mapped value only exists in the activation body's
/// post-input view; a selector that reads state (root `amount = 5`) instead
/// raises a no-match incident.
fn input_mapped_split_gateway_process(gateway: SplitGateway) -> ProcessDefinition {
    let builder = ProcessBuilder::new("split-mapped").start_event("s");
    let builder = match gateway {
        SplitGateway::Exclusive => builder.exclusive_gateway("g"),
        SplitGateway::Inclusive => builder.inclusive_gateway("g"),
    };
    builder
        .with_io(
            "g",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=500".to_string(),
                    target: "amount".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "g")
        .connect_when("g", "e", "amount > 100")
        .build()
        .unwrap()
}

#[test]
fn exclusive_gateway_activation_selection_sees_input_mapped_variables() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(input_mapped_split_gateway_process(
            SplitGateway::Exclusive,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split-mapped",
            HashMap::from([("amount".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "the input-mapped `amount = 500` satisfies `amount > 100`: {:?}",
        engine.active_incidents()
    );
    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating", "Activated", "Completing", "Completed"],
        "the gateway routes on the post-input view: {created:?}"
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn inclusive_split_activation_selection_sees_input_mapped_variables() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(input_mapped_split_gateway_process(
            SplitGateway::Inclusive,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split-mapped",
            HashMap::from([("amount".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "the input-mapped `amount = 500` satisfies `amount > 100`: {:?}",
        engine.active_incidents()
    );
    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating", "Activated", "Completing", "Completed"],
        "the gateway routes on the post-input view: {created:?}"
    );
    assert!(engine.is_completed(inst));
}

/// `start → g (split, `start` execution listener) → a / d(default)`: the
/// listener completes with a variable write that flips the condition, so a
/// completion that re-evaluates would take the default — the activation-time
/// selection must be the one carried through.
fn listener_flip_split_gateway_process(gateway: SplitGateway) -> ProcessDefinition {
    let builder = ProcessBuilder::new("split-flip").start_event("s");
    let builder = match gateway {
        SplitGateway::Exclusive => builder.exclusive_gateway("g"),
        SplitGateway::Inclusive => builder.inclusive_gateway("g"),
    };
    builder
        .with_listeners(
            "g",
            vec![el(ListenerEventType::Start, "gate-listener")],
            Vec::new(),
        )
        .service_task("a", "work_a")
        .service_task("d", "work_d")
        .end_event("e")
        .end_event("e_d")
        .connect("s", "g")
        .connect_when("g", "a", "amount > 100")
        .connect_default("g", "d")
        .connect("a", "e")
        .connect("d", "e_d")
        .build()
        .unwrap()
}

fn assert_selection_is_carried_through_completion(gateway: SplitGateway) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(listener_flip_split_gateway_process(
            gateway,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split-flip",
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating", "Activated"],
        "{gateway:?}: the gateway parks on its start listener: {created:?}"
    );

    // The listener completes with a write that flips the condition variable,
    // so a completion-time re-evaluation would take the default flow instead.
    let listener = engine.activate_jobs("gate-listener", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "{gateway:?}: one start-listener job");
    let resumed = engine
        .apply_command(Command::complete_job_with(
            listener[0].key,
            HashMap::from([("amount".to_string(), Value::Int(5))]),
        ))
        .unwrap();

    assert!(
        resumed.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "g" && to == "a"
        )),
        "{gateway:?}: the activation-time selection (`a`) is carried through \
         completion even though `amount > 100` no longer holds: {resumed:?}"
    );
    assert!(
        !resumed.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "g" && to == "d"
        )),
        "{gateway:?}: completion must not re-evaluate into the default flow"
    );
    assert!(engine.active_incidents().is_empty(), "{gateway:?}");
    complete_one(&mut engine, "work_a");
    assert!(engine.is_completed(inst), "{gateway:?}");
}

#[test]
fn exclusive_gateway_carries_its_activation_selection_through_completion() {
    assert_selection_is_carried_through_completion(SplitGateway::Exclusive);
}

#[test]
fn inclusive_split_carries_its_activation_selection_through_completion() {
    assert_selection_is_carried_through_completion(SplitGateway::Inclusive);
}

/// `start → g (split, `start` execution listener) → end`, one conditional
/// flow `amount > 100`. The listener defers the gateway's activation body —
/// but the routing gate must still apply to the deferred body: no
/// CONDITION_ERROR incident may be raised after `ElementActivated`.
fn start_listener_split_gateway_process(gateway: SplitGateway) -> ProcessDefinition {
    let builder = ProcessBuilder::new("split-listener").start_event("s");
    let builder = match gateway {
        SplitGateway::Exclusive => builder.exclusive_gateway("g"),
        SplitGateway::Inclusive => builder.inclusive_gateway("g"),
    };
    builder
        .with_listeners(
            "g",
            vec![el(ListenerEventType::Start, "gate-listener")],
            Vec::new(),
        )
        .end_event("e")
        .connect("s", "g")
        .connect_when("g", "e", "amount > 100")
        .build()
        .unwrap()
}

fn assert_start_listener_defers_the_routing_gate(gateway: SplitGateway) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            start_listener_split_gateway_process(gateway),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split-listener",
            HashMap::from([("amount".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The routing gate runs while the gateway is ACTIVATING — BEFORE the start
    // listener chain opens: the failure parks the token on the incident and no
    // listener job is minted (a listener would otherwise run for a gateway
    // that can never route).
    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating"],
        "{gateway:?}: the routing incident precedes the listener chain: {created:?}"
    );
    assert!(
        !created
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "{gateway:?}: no listener job is minted for a gateway that cannot \
         route: {created:?}"
    );
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "{gateway:?}: {incidents:?}");
    assert_eq!(
        incidents[0].kind,
        state::IncidentKind::NoMatchingSequenceFlow,
        "{gateway:?}"
    );
    assert_eq!(incidents[0].element_id, "g");
    assert_eq!(
        incidents[0].redrive,
        Some(state::IoMappingRedrive::ActivationPending),
        "{gateway:?}: the gateway's routing failure is an ACTIVATION-phase \
         incident (resolution re-runs the gated activation), not a \
         completion-phase one: {:?}",
        incidents[0]
    );
    let incident_key = incidents[0].key;

    // Resolution fixes the variable and re-runs the gate: the gateway
    // activates, runs its listener, then completes through the carried
    // selection.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&resolved, "g"),
        vec!["Activated"],
        "{gateway:?}: resolution re-runs the full gated activation on the \
         parked instance: the gate passes, the gateway activates, and the \
         `start` listener chain the failed attempt never opened runs before \
         the behaviour: {resolved:?}"
    );
    assert!(
        resolved.iter().any(|e| matches!(
            e,
            Event::ExecutionListenerJobCreated {
                event_type: ListenerEventType::Start,
                ..
            }
        )),
        "{gateway:?}: the retry opens the start-listener chain: {resolved:?}"
    );
    let listener = engine.activate_jobs("gate-listener", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "{gateway:?}: one start-listener job");
    let drained = engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&drained, "g"),
        vec!["Completing", "Completed"],
        "{gateway:?}: the drained listener completes the gateway through the \
         retry's journaled selection: {drained:?}"
    );
    assert!(engine.active_incidents().is_empty());
    assert!(engine.is_completed(inst), "{gateway:?}");
}

#[test]
fn exclusive_gateway_start_listener_defers_the_routing_gate() {
    assert_start_listener_defers_the_routing_gate(SplitGateway::Exclusive);
}

#[test]
fn inclusive_split_start_listener_defers_the_routing_gate() {
    assert_start_listener_defers_the_routing_gate(SplitGateway::Inclusive);
}

/// `start → g (split, `start` execution listener) → end`, one conditional
/// flow `amount > 100` — the retry-companion of
/// [`start_listener_split_gateway_process`]: the routing incident's resolution
/// must re-run the SAME gated activation (routing gate, then the unopened
/// start-listener chain), not a bare behaviour redrive.
fn assert_retry_redrives_the_gated_activation(gateway: SplitGateway) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            start_listener_split_gateway_process(gateway),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split-listener",
            HashMap::from([("amount".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating"],
        "{gateway:?}: parked ACTIVATING on the routing incident: {created:?}"
    );
    let incident_key = engine.active_incidents()[0].key;

    // Resolve with variables that STILL match nothing: the retry must re-run
    // the routing gate and park again while ACTIVATING — never emitting
    // `ElementActivated`, and never minting the start-listener job for a
    // gateway that still cannot route.
    let retried = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&retried, "g"),
        Vec::<&'static str>::new(),
        "{gateway:?}: a still-failing retry re-runs the gate on the same \
         parked instance and re-raises the incident — no `ElementActivated`, \
         no second `ElementActivating`: {retried:?}"
    );
    assert!(
        !retried
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "{gateway:?}: no listener job for a retry that still cannot route: \
         {retried:?}"
    );
    let incidents = engine.active_incidents();
    assert_eq!(
        incidents.len(),
        1,
        "{gateway:?}: the retry re-raises the routing incident: {incidents:?}"
    );
    assert_eq!(
        incidents[0].kind,
        state::IncidentKind::NoMatchingSequenceFlow,
        "{gateway:?}"
    );
    assert_eq!(
        incidents[0].redrive,
        Some(state::IoMappingRedrive::ActivationPending),
        "{gateway:?}: the re-raised incident stays activation-phase: {:?}",
        incidents[0]
    );

    // Now fix the variable and resolve: the retry re-runs the gate, which
    // passes — so the gateway activates and its `start` listener chain (never
    // opened on the failed attempts) runs before the behaviour completes
    // through the freshly journaled selection.
    let retry_incident_key = incidents[0].key;
    drop(incidents);
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(retry_incident_key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&resolved, "g"),
        vec!["Activated"],
        "{gateway:?}: the successful retry re-runs the gated activation on the \
         parked instance and parks on its start listener: {resolved:?}"
    );
    assert!(
        resolved.iter().any(|e| matches!(
            e,
            Event::ExecutionListenerJobCreated {
                event_type: ListenerEventType::Start,
                ..
            }
        )),
        "{gateway:?}: the retry opens the start-listener chain the failed \
         attempts never reached: {resolved:?}"
    );

    let listener = engine.activate_jobs("gate-listener", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "{gateway:?}: one start-listener job");
    let drained = engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&drained, "g"),
        vec!["Completing", "Completed"],
        "{gateway:?}: the drained listener completes the gateway through the \
         retry's journaled selection: {drained:?}"
    );
    assert!(
        drained.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "g" && to == "e"
        )),
        "{gateway:?}: the retry's selection routes to `e`: {drained:?}"
    );
    assert!(engine.active_incidents().is_empty(), "{gateway:?}");
    assert!(engine.is_completed(inst), "{gateway:?}");
}

#[test]
fn exclusive_gateway_retry_redrives_the_gated_activation() {
    assert_retry_redrives_the_gated_activation(SplitGateway::Exclusive);
}

#[test]
fn inclusive_split_retry_redrives_the_gated_activation() {
    assert_retry_redrives_the_gated_activation(SplitGateway::Inclusive);
}

// ---------------------------------------------------------------------------
// Round-4 review hardening (#1402): the gateway retry must (a) RESUME a
// partially-drained `start` listener chain instead of re-running the whole
// gated activation, and (b) evaluate against the element-local view the
// non-gateway retry path uses.
// ---------------------------------------------------------------------------

/// `start → g (split, two `start` listeners: `l0` then `=nextType`) → e/f`,
/// two conditional flows `amount > 100` → `e` and `amount > 5` → `f`, no
/// default. With `amount = 10` the gate selects `f` (only `amount > 5`
/// matches); listener `l0` then bumps `amount` past 100, and listener #1's
/// job-type expression fails. Resolving THAT incident must resume the chain —
/// keeping the carried route to `f` — not re-run the gate (which would now
/// select `e`) nor re-emit `ElementActivated`.
fn two_listener_split_gateway_process(gateway: SplitGateway) -> ProcessDefinition {
    let builder = ProcessBuilder::new("split-two-listeners").start_event("s");
    let builder = match gateway {
        SplitGateway::Exclusive => builder.exclusive_gateway("g"),
        SplitGateway::Inclusive => builder.inclusive_gateway("g"),
    };
    builder
        .with_listeners(
            "g",
            vec![
                el(ListenerEventType::Start, "l0"),
                ExecutionListener {
                    event_type: ListenerEventType::Start,
                    job_type: "=nextType".to_string(),
                    retries: None,
                },
            ],
            Vec::new(),
        )
        .end_event("e")
        .end_event("f")
        .connect("s", "g")
        .connect_when("g", "e", "amount > 100")
        .connect_when("g", "f", "amount > 5")
        .build()
        .unwrap()
}

fn assert_listener_failure_retry_resumes_the_chain(gateway: SplitGateway) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_listener_split_gateway_process(
            gateway,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "split-two-listeners",
            HashMap::from([("amount".to_string(), Value::Int(10))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        lifecycle_of(&created, "g"),
        vec!["Activating", "Activated"],
        "{gateway:?}: the gate passes (`amount > 5` → `f`) and the listener \
         chain opens: {created:?}"
    );
    assert!(
        created.iter().any(|e| matches!(
            e,
            Event::GatewayRouteSelected { element_id, .. } if element_id == "g"
        )),
        "{gateway:?}: the activation-time selection is journaled: {created:?}"
    );

    // Listener #0 completes, bumping `amount` past 100; listener #1's job-type
    // expression (`=nextType`, unbound) fails — the element parks on an
    // EXTRACT_VALUE_ERROR incident mid-chain.
    let l0 = engine.activate_jobs("l0", "W", 10, 1_000, 0);
    assert_eq!(l0.len(), 1, "{gateway:?}: one l0 listener job");
    let advanced = engine
        .apply_command(Command::complete_job_with(
            l0[0].key,
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap();
    assert!(
        !advanced
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "{gateway:?}: the failing second listener mints no job: {advanced:?}"
    );
    let incident = extract_value_incident(&engine, "g");
    assert_eq!(
        incident.redrive,
        Some(state::IoMappingRedrive::ExecutionListenerJob {
            event_type: crate::model::ListenerEventType::Start,
            listener_index: 1,
            scope: 0,
        }),
        "{gateway:?}: a mid-chain listener failure re-mints only the failed \
         listener (#1398), not the whole activation phase: {:?}",
        incident
    );

    // Resolve: the retry must RESUME the chain at listener #1 — the carried
    // route (to `f`) is not re-selected even though `amount = 500` now matches
    // the `e` branch, and `ElementActivated` (emitted on the first pass) is
    // not repeated.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("nextType".to_string(), Value::Str("l1".into()))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&resolved, "g"),
        Vec::<&'static str>::new(),
        "{gateway:?}: resolving a mid-chain listener failure resumes the \
         chain — no second `ElementActivated`: {resolved:?}"
    );
    assert!(
        !resolved
            .iter()
            .any(|e| matches!(e, Event::GatewayRouteSelected { .. })),
        "{gateway:?}: the carried route is kept — the gate does not re-run: \
         {resolved:?}"
    );
    let l1 = engine.activate_jobs("l1", "W", 10, 1_000, 0);
    assert_eq!(
        l1.len(),
        1,
        "{gateway:?}: the resumed chain mints listener #1's job"
    );
    let drained = engine
        .apply_command(Command::complete_job(l1[0].key))
        .unwrap();
    assert_eq!(
        lifecycle_of(&drained, "g"),
        vec!["Completing", "Completed"],
        "{gateway:?}: the drained chain completes the gateway: {drained:?}"
    );
    assert!(
        drained.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "g" && to == "f"
        )),
        "{gateway:?}: the completion takes the route journaled at activation \
         (to `f`), not the route the listener's write would now select: \
         {drained:?}"
    );
    assert!(engine.active_incidents().is_empty(), "{gateway:?}");
    assert!(engine.is_completed(inst), "{gateway:?}");
}

#[test]
fn exclusive_gateway_listener_failure_retry_resumes_the_chain() {
    assert_listener_failure_retry_resumes_the_chain(SplitGateway::Exclusive);
}

#[test]
fn inclusive_split_listener_failure_retry_resumes_the_chain() {
    assert_listener_failure_retry_resumes_the_chain(SplitGateway::Inclusive);
}

/// `start → g (split) → e`, one conditional flow `amount > 100`, no default.
/// A modification activates `g` with the element-local `amount = 5` while the
/// root scope carries `amount = 500`: the gate must evaluate against the
/// SEEDED view (no flow matches → the gateway parks ACTIVATING), and resolving
/// the incident unchanged must re-evaluate against that same element-local
/// view — re-raising the incident — never falling back to the root value.
fn assert_gateway_retry_uses_the_element_local_view(gateway: SplitGateway) {
    // The straight-through token would route `g` to `e` (root 500 > 100) and
    // complete the instance — a modification cannot target a completed
    // instance — so the fixture parks the instance on a user task first; the
    // modification then activates `g` as a SECOND token seeded with the local.
    let mut engine = Engine::new();
    let def = {
        let builder = ProcessBuilder::new("split-parked").start_event("s");
        let builder = match gateway {
            SplitGateway::Exclusive => builder.exclusive_gateway("g"),
            SplitGateway::Inclusive => builder.inclusive_gateway("g"),
        };
        builder
            .user_task("park")
            .end_event("e")
            .connect("s", "park")
            .connect_when("g", "e", "amount > 100")
            .build()
            .unwrap()
    };
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "split-parked",
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let modified = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![crate::command::ActivateElementInstruction {
                element_id: "g".into(),
                variable_instructions: vec![crate::command::VariableInstruction {
                    variables: HashMap::from([("amount".to_string(), Value::Int(5))]),
                    scope_id: Some("g".to_string()),
                }],
                ..Default::default()
            }],
            terminate_instructions: vec![],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap();
    let incidents = engine.active_incidents();
    assert_eq!(
        incidents.len(),
        1,
        "{gateway:?}: the seeded activation's gate sees the element-local \
         `amount = 5`, not the root 500 — no flow matches: {modified:?}"
    );
    assert_eq!(
        incidents[0].kind,
        state::IncidentKind::NoMatchingSequenceFlow,
        "{gateway:?}"
    );
    assert_eq!(
        incidents[0].redrive,
        // A modification's seeded activation emits `ElementActivated` up front
        // (the seed precedes the activation body), so the gate failure is
        // `Activation`-phase, not `ActivationPending`.
        Some(state::IoMappingRedrive::Activation),
        "{gateway:?}: {:?}",
        incidents[0]
    );

    // Resolving unchanged must re-evaluate against the SAME element-local
    // view: the local `amount = 5` still matches nothing, so the retry
    // re-raises the incident instead of routing on the root `amount = 500`.
    let resolved = engine
        .apply_command(Command::resolve_incident(incidents[0].key))
        .unwrap();
    let incidents = engine.active_incidents();
    assert_eq!(
        incidents.len(),
        1,
        "{gateway:?}: the retry re-evaluates against the element-local view \
         and re-raises the incident: {resolved:?}"
    );
    assert_eq!(
        incidents[0].kind,
        state::IncidentKind::NoMatchingSequenceFlow,
        "{gateway:?}"
    );
    assert!(
        !resolved.iter().any(|e| matches!(
            e,
            Event::ElementActivated { element_id, .. } if element_id == "g"
        )),
        "{gateway:?}: a still-failing retry never emits `ElementActivated`: \
         {resolved:?}"
    );
}

#[test]
fn exclusive_gateway_retry_uses_the_element_local_view() {
    assert_gateway_retry_uses_the_element_local_view(SplitGateway::Exclusive);
}

#[test]
fn inclusive_split_retry_uses_the_element_local_view() {
    assert_gateway_retry_uses_the_element_local_view(SplitGateway::Inclusive);
}
