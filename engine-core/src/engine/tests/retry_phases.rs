//! #1406: incident resolution must not replay completed work.
//!
//! Recovery regressions for the fine-grained redrives (`state::IoMappingRedrive`)
//! of #1406's findings 2, 3, 5 and 6: resolving a job-property incident must
//! never re-run an already-merged script result or output mapping, re-initialise
//! a multi-instance body, re-arm a live boundary, or lose a body's aggregated
//! results. Each test resolves **twice** (an unfixed resolve re-raises, then the
//! fixed one recovers), the defect-class guard: a re-raise must not replay side
//! effects either. Finding 5's "the script is not re-run" is the behaviour of the
//! pinned Zeebe engine (captured differential trace, see #1406).
//!
//! Finding 7 (decided on #1406: match Zeebe) is pinned by the trace scenarios
//! `execution-listener-retry-resumes-at-failure`,
//! `start-listener-retry-restarts-chain` and
//! `gateway-start-listener-retry-restarts-chain`: resolving a listener incident
//! restarts the chain at listener 0 and re-applies the element's mappings.
use super::*;

const NS: &str = r#"xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0""#;

fn parse(body: &str) -> ProcessDefinition {
    let xml = format!("<bpmn:definitions {NS}>{body}</bpmn:definitions>");
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

fn start(def: ProcessDefinition, vars_in: HashMap<String, Value>) -> (Engine, Key) {
    let mut engine = Engine::new();
    let id = def.id.clone();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(&id, vars_in))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    (engine, inst)
}

fn only_incident(engine: &Engine, element_id: &str) -> state::Incident {
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "exactly one incident: {incidents:?}");
    assert_eq!(incidents[0].element_id, element_id, "{incidents:?}");
    assert_eq!(incidents[0].kind, state::IncidentKind::ExtractValueError);
    incidents[0].clone()
}

fn resolve(engine: &mut Engine, inst: Key, incident: &state::Incident, set: &[(&str, Value)]) {
    if !set.is_empty() {
        engine
            .apply_command(Command::set_variables(inst, vars(set)))
            .unwrap();
    }
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
}

fn job_types(engine: &Engine) -> Vec<String> {
    let mut t: Vec<String> = engine
        .pending_jobs()
        .iter()
        .map(|j| j.job_type.clone())
        .collect();
    t.sort();
    t
}

/// Activates and completes every pending job of `job_type` (with `out`).
fn complete_all(engine: &mut Engine, job_type: &str, out: &[(&str, Value)]) -> usize {
    let jobs = engine.activate_jobs(job_type, "W", 100, 60_000, 0);
    let n = jobs.len();
    for j in jobs {
        engine
            .apply_command(Command::complete_job_with(j.key, vars(out)))
            .unwrap();
    }
    n
}

fn root_var(engine: &Engine, inst: Key, name: &str) -> Option<Value> {
    engine.instance(inst).unwrap().variables.get(name).cloned()
}

// Finding 5: an end-listener failure after a non-idempotent script result was
// merged must not re-run the script on resolution.
#[test]
fn end_listener_failure_does_not_rerun_completed_script() {
    let def = parse(
        r#"<bpmn:process id="scr" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:scriptTask id="inc">
            <bpmn:extensionElements>
              <zeebe:script expression="=counter + 1" resultVariable="counter" />
              <zeebe:ioMapping><zeebe:output source="=counter * 10" target="scaled" /></zeebe:ioMapping>
              <zeebe:executionListeners>
                <zeebe:executionListener eventType="end" type="=lt" />
              </zeebe:executionListeners>
            </bpmn:extensionElements>
          </bpmn:scriptTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="inc" />
          <bpmn:sequenceFlow id="f2" sourceRef="inc" targetRef="e" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(def, vars(&[("counter", Value::Int(0))]));
    let incident = only_incident(&engine, "inc");
    assert_eq!(root_var(&engine, inst, "counter"), Some(Value::Int(1)));
    resolve(&mut engine, inst, &incident, &[]);
    let incident = only_incident(&engine, "inc");
    assert_eq!(root_var(&engine, inst, "counter"), Some(Value::Int(1)));
    resolve(
        &mut engine,
        inst,
        &incident,
        &[("lt", Value::Str("audit".into()))],
    );
    assert_eq!(job_types(&engine), vec!["audit".to_string()]);
    assert_eq!(root_var(&engine, inst, "counter"), Some(Value::Int(1)));
    assert_eq!(root_var(&engine, inst, "scaled"), Some(Value::Int(10)));
    assert_eq!(complete_all(&mut engine, "audit", &[]), 1);
    assert!(engine.is_completed(inst));
}

fn mi_body(listener: &str, extra: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="mib" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="each">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="handle" />
              <zeebe:ioMapping><zeebe:output source="=&quot;done:&quot; + item" target="label" /></zeebe:ioMapping>
              <zeebe:executionListeners>{listener}</zeebe:executionListeners>
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" outputCollection="results" outputElement="=&quot;done:&quot; + item" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          {extra}
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="each" />
          <bpmn:sequenceFlow id="f2" sourceRef="each" targetRef="e" />
        </bpmn:process>"#
    ))
}

fn items() -> HashMap<String, Value> {
    vars(&[(
        "items",
        Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
    )])
}

// Finding 2: a failed MI-body start listener retries only the listener gate —
// the collection is not re-initialised and boundaries are not re-armed.
#[test]
fn mi_body_start_listener_retry_preserves_body_and_boundaries() {
    let def = mi_body(
        r#"<zeebe:executionListener eventType="start" type="=lt" />"#,
        r#"<bpmn:boundaryEvent id="ping" attachedToRef="each" cancelActivity="false">
            <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="eb" />
          <bpmn:sequenceFlow id="f3" sourceRef="ping" targetRef="eb" />"#,
    );
    let (mut engine, inst) = start(def, items());
    let incident = only_incident(&engine, "each");
    let timers = |e: &Engine| -> Vec<(Key, u64, state::TimerState)> {
        e.timers()
            .iter()
            .map(|t| (t.key, t.due_at, t.state))
            .collect()
    };
    let before = timers(&engine);
    assert_eq!(
        before.len(),
        1,
        "exactly one boundary timer armed: {before:?}"
    );
    assert_eq!(before[0].2, state::TimerState::Created, "{before:?}");
    // Change the collection: a re-evaluating retry would yield three children.
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[(
                "items",
                Value::List(vec![
                    Value::Str("x".into()),
                    Value::Str("y".into()),
                    Value::Str("z".into()),
                ]),
            )]),
        ))
        .unwrap();
    resolve(&mut engine, inst, &incident, &[]);
    let incident = only_incident(&engine, "each");
    assert_eq!(
        before,
        timers(&engine),
        "re-raise must not re-arm boundaries"
    );
    resolve(
        &mut engine,
        inst,
        &incident,
        &[("lt", Value::Str("audit".into()))],
    );
    assert_eq!(complete_all(&mut engine, "audit", &[]), 1);
    assert_eq!(
        before,
        timers(&engine),
        "the original boundary timer survives, still Created"
    );
    assert_eq!(
        job_types(&engine),
        vec!["handle".to_string(), "handle".to_string()]
    );
    assert_eq!(complete_all(&mut engine, "handle", &[]), 2);
    assert!(engine.is_completed(inst), "{:?}", job_types(&engine));
}

// Finding 6: a failed MI-body end listener resumes body completion — the
// per-child output mapping (which reads the child-only `item`) never raises a
// false IO_MAPPING_ERROR and the aggregated results survive.
#[test]
fn mi_body_end_listener_retry_uses_body_completion() {
    let def = mi_body(
        r#"<zeebe:executionListener eventType="end" type="=lt" />"#,
        "",
    );
    let (mut engine, inst) = start(def, items());
    assert_eq!(complete_all(&mut engine, "handle", &[]), 2);
    let incident = only_incident(&engine, "each");
    resolve(&mut engine, inst, &incident, &[]);
    let incident = only_incident(&engine, "each");
    resolve(
        &mut engine,
        inst,
        &incident,
        &[("lt", Value::Str("audit".into()))],
    );
    assert!(
        engine.active_incidents().is_empty(),
        "{:?}",
        engine.active_incidents()
    );
    assert_eq!(
        root_var(&engine, inst, "results"),
        Some(Value::List(vec![
            Value::Str("done:a".into()),
            Value::Str("done:b".into())
        ]))
    );
    // The child-only `label` output (reads `item`) must not leak a body-level
    // `null` into the parent scope on the restart re-projection.
    assert_ne!(root_var(&engine, inst, "label"), Some(Value::Null));
    assert_eq!(complete_all(&mut engine, "audit", &[]), 1);
    assert!(engine.is_completed(inst), "{:?}", job_types(&engine));
}

// Finding 3 (nested container): a nested AGENTIC container whose catalog job
// type fails parks the outer container BEFORE any child is created; a re-raise
// keeps it parked, and the fixed resolve mints exactly one nested agent job.
#[test]
fn nested_agentic_container_job_type_retry_is_catalog_aware() {
    let def = parse(
        r#"<bpmn:process id="nest" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
            </bpmn:extensionElements>
            <bpmn:adHocSubProcess id="inner">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=innerType" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="leaf">
                <bpmn:extensionElements><zeebe:taskDefinition type="leaf" /></bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(def, HashMap::new());
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 60_000, 0)
        .into_iter()
        .next()
        .expect("agent job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("inner")],
                ..Default::default()
            },
        ))
        .unwrap();
    let incident = only_incident(&engine, "agent");
    let jobs_before = engine.state().jobs.len();
    resolve(&mut engine, inst, &incident, &[]);
    let incident = only_incident(&engine, "agent");
    assert_eq!(
        engine.state().jobs.len(),
        jobs_before,
        "re-raise mints no job"
    );
    resolve(
        &mut engine,
        inst,
        &incident,
        &[("innerType", Value::Str("inner-agent".into()))],
    );
    assert!(
        engine.active_incidents().is_empty(),
        "{:?}",
        engine.active_incidents()
    );
    assert_eq!(
        engine
            .activate_jobs("inner-agent", "W", 10, 60_000, 0)
            .len(),
        1,
        "exactly one nested agent job after the catalog-aware retry"
    );
}
