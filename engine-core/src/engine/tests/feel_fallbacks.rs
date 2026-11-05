//! #1398: the remaining silent FEEL fallbacks raise the incident Zeebe raises.
//!
//! Each test pins one former `Tracked(#1398)` site of
//! `engine-core/tests/feel_fallback_lint.rs`: an expression failure that nano
//! used to swallow (literal text, default, empty, `null`, "not satisfied") now
//! raises an `EXTRACT_VALUE_ERROR` incident (or, at deploy / migration, the
//! rejection Zeebe returns), and resolving the incident after fixing the data
//! retries the failed work.
use super::*;

const NS: &str = r#"xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0""#;

/// Parses a single-process BPMN document whose `<bpmn:definitions>` body is
/// `body` (namespaces supplied).
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

/// The single active incident: an `EXTRACT_VALUE_ERROR` on `element_id`
/// whose message contains `needle`.
fn extract_value_incident(engine: &Engine, element_id: &str, needle: &str) -> state::Incident {
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "exactly one incident: {incidents:?}");
    let incident = incidents[0].clone();
    assert_eq!(
        incident.kind,
        state::IncidentKind::ExtractValueError,
        "{incident:?}"
    );
    assert_eq!(incident.element_id, element_id, "{incident:?}");
    assert!(incident.reason.contains(needle), "{}", incident.reason);
    incident
}

fn set_and_resolve(
    engine: &mut Engine,
    inst: Key,
    incident: &state::Incident,
    set: &[(&str, Value)],
) {
    engine
        .apply_command(Command::set_variables(inst, vars(set)))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "{:?}",
        engine.active_incidents()
    );
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

// ---------------------------------------------------------------------------
// Listener / container job properties (Zeebe `BpmnJobBehavior`)
// ---------------------------------------------------------------------------

fn task_with_start_listener(listener_attrs: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="el" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="work" />
              <zeebe:executionListeners>
                <zeebe:executionListener eventType="start" {listener_attrs} />
              </zeebe:executionListeners>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn execution_listener_retries_expression_failure_raises_incident() {
    // Zeebe `BpmnJobBehavior.evalRetriesExp` for listener jobs: a non-NUMBER
    // retries is EXTRACT_VALUE_ERROR — nano used to default to 3 retries.
    let (mut engine, inst) = start(
        task_with_start_listener(r#"type="audit" retries="=r""#),
        vars(&[("r", Value::Str("many".into()))]),
    );
    assert!(engine.pending_jobs().is_empty(), "no listener job");
    let incident = extract_value_incident(&engine, "work", "to be 'NUMBER', but was 'STRING'");
    set_and_resolve(&mut engine, inst, &incident, &[("r", Value::Int(5))]);
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_type, "audit");
    assert_eq!(jobs[0].retries, 5);
}

fn user_task_with_creating_listener(listener_type: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="tl" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:taskListeners>
                <zeebe:taskListener eventType="creating" type="{listener_type}" />
              </zeebe:taskListeners>
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
        </bpmn:process>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn task_listener_job_type_expression_failure_raises_incident() {
    // Zeebe evaluates task-listener job properties through `BpmnJobBehavior`
    // too: a failure is EXTRACT_VALUE_ERROR, never a job typed `=listenerType`.
    let (mut engine, inst) = start(
        user_task_with_creating_listener("=listenerType"),
        HashMap::new(),
    );
    assert!(
        engine
            .pending_jobs()
            .iter()
            .all(|j| !j.job_type.starts_with('=')),
        "no job named after the expression text: {:?}",
        job_types(&engine)
    );
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "review", "'=listenerType'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("listenerType", Value::Str("create-audit".into()))],
    );
    assert_eq!(job_types(&engine), vec!["create-audit".to_string()]);
}

// ---------------------------------------------------------------------------
// Boundary / catch / start event subscriptions (Zeebe `CatchEventBehavior`)
// ---------------------------------------------------------------------------

fn task_with_message_boundary(name: &str, key: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="mb" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:boundaryEvent id="b" attachedToRef="work">
            <bpmn:messageEventDefinition messageRef="M" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="e" />
          <bpmn:endEvent id="eb" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
          <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="eb" />
        </bpmn:process>
        <bpmn:message id="M" name="{name}">
          <bpmn:extensionElements><zeebe:subscription correlationKey="{key}" /></bpmn:extensionElements>
        </bpmn:message>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn boundary_message_name_expression_failure_raises_incident_on_host() {
    // Zeebe `BpmnEventSubscriptionBehavior.subscribeToEvents` fails the host's
    // activation: no job, no subscription named after the expression text.
    let (mut engine, inst) = start(
        task_with_message_boundary("=msgName", "=k"),
        vars(&[("k", Value::Str("c-1".into()))]),
    );
    assert!(engine.message_subscriptions().is_empty());
    assert!(engine.pending_jobs().is_empty(), "the host creates no job");
    let incident = extract_value_incident(&engine, "work", "'=msgName'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("msgName", Value::Str("cancel".into()))],
    );
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].message_name, "cancel");
    assert_eq!(job_types(&engine), vec!["work".to_string()]);
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn boundary_correlation_key_failure_raises_incident_on_host() {
    // nano used to open the boundary subscription with an empty (never
    // matchable) correlation key.
    let (mut engine, inst) = start(task_with_message_boundary("cancel", "=k"), HashMap::new());
    assert!(engine.message_subscriptions().is_empty());
    assert!(engine.pending_jobs().is_empty());
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "{incidents:?}");
    let incident = incidents[0].clone();
    assert_eq!(incident.element_id, "work");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("k", Value::Str("c-9".into()))],
    );
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].correlation_key, "c-9");
}

fn signal_process(catch: &str) -> ProcessDefinition {
    let body = match catch {
        "boundary" => {
            r#"<bpmn:serviceTask id="work">
                 <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:boundaryEvent id="b" attachedToRef="work">
                 <bpmn:signalEventDefinition signalRef="S" />
               </bpmn:boundaryEvent>
               <bpmn:endEvent id="eb" />
               <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="eb" />"#
        }
        _ => {
            r#"<bpmn:intermediateCatchEvent id="work">
                 <bpmn:signalEventDefinition signalRef="S" />
               </bpmn:intermediateCatchEvent>"#
        }
    };
    parse(&format!(
        r#"<bpmn:process id="sig" isExecutable="true">
          <bpmn:startEvent id="s" />
          {body}
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>
        <bpmn:signal id="S" name="=sigName" />"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn signal_boundary_and_catch_name_expression_failure_raise_incidents() {
    for catch in ["boundary", "intermediate"] {
        let (mut engine, inst) = start(signal_process(catch), HashMap::new());
        assert!(engine.signal_subscriptions().is_empty(), "{catch}");
        let incident = extract_value_incident(&engine, "work", "'=sigName'");
        set_and_resolve(
            &mut engine,
            inst,
            &incident,
            &[("sigName", Value::Str("go".into()))],
        );
        let subs = engine.signal_subscriptions();
        assert_eq!(subs.len(), 1, "{catch}");
        assert_eq!(subs[0].signal_name, "go", "{catch}");
    }
}

#[test]
fn message_start_name_expression_failure_rejects_the_deployment() {
    // Zeebe `ProcessMessageStartEventMessageNameValidator`: a start-event
    // message name is evaluated at deploy (empty context); a failure rejects
    // the deployment instead of subscribing to the literal `=name`.
    let def = parse(
        r#"<bpmn:process id="ms" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:messageEventDefinition messageRef="M" /></bpmn:startEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="M" name="=missing" />"#,
    );
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::DeployProcess(def))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::StartEventExpressionInvalid { ref element_id, .. } if element_id == "s"),
        "{err:?}"
    );
    assert!(engine.state().processes.is_empty(), "nothing deployed");
}

#[test]
fn invalid_start_expression_rejects_the_whole_batch_atomically() {
    // A later process with an invalid start expression must reject the whole
    // `DeployResources` batch BEFORE anything is applied: the earlier, valid
    // process must not be left installed outside the (discarded) journal.
    let good = parse(
        r#"<bpmn:process id="good" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:messageEventDefinition messageRef="G" /></bpmn:startEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="G" name="go" />"#,
    );
    let bad = parse(
        r#"<bpmn:process id="bad" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:messageEventDefinition messageRef="M" /></bpmn:startEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="M" name="=missing" />"#,
    );
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::DeployResources(vec![good, bad]))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::StartEventExpressionInvalid { ref process_id, .. } if process_id == "bad"),
        "{err:?}"
    );
    assert!(engine.state().processes.is_empty(), "nothing deployed");
    assert!(
        engine.state().message_subscriptions.is_empty(),
        "no start subscription left behind"
    );
}

fn timer_process(catch: &str, duration: &str) -> ProcessDefinition {
    let body = match catch {
        "boundary" => format!(
            r#"<bpmn:serviceTask id="work">
                 <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:boundaryEvent id="b" attachedToRef="work">
                 <bpmn:timerEventDefinition><bpmn:timeDuration>{duration}</bpmn:timeDuration></bpmn:timerEventDefinition>
               </bpmn:boundaryEvent>
               <bpmn:endEvent id="eb" />
               <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="eb" />"#
        ),
        _ => format!(
            r#"<bpmn:intermediateCatchEvent id="work">
                 <bpmn:timerEventDefinition><bpmn:timeDuration>{duration}</bpmn:timeDuration></bpmn:timerEventDefinition>
               </bpmn:intermediateCatchEvent>"#
        ),
    };
    parse(&format!(
        r#"<bpmn:process id="tm" isExecutable="true">
          <bpmn:startEvent id="s" />
          {body}
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn timer_expression_failure_raises_incident_instead_of_static_fallback() {
    // Zeebe `CatchEventBehavior.evaluateTimer`: a failed / unparseable timer
    // expression is EXTRACT_VALUE_ERROR; nano used to arm the static timer.
    for catch in ["boundary", "intermediate"] {
        for (vars_in, needle) in [
            (vars(&[]), "'=d'"),
            (vars(&[("d", Value::Str("soon".into()))]), "soon"),
        ] {
            let (mut engine, inst) = start(timer_process(catch, "=d"), vars_in);
            assert!(engine.timers().is_empty(), "{catch}: no timer armed");
            let incident = extract_value_incident(&engine, "work", needle);
            set_and_resolve(
                &mut engine,
                inst,
                &incident,
                &[("d", Value::Str("PT1H".into()))],
            );
            let timers = engine.timers();
            assert_eq!(timers.len(), 1, "{catch}");
            assert_eq!(timers[0].due_at, engine.now + 3_600_000, "{catch}");
        }
    }
}

#[test]
fn start_timer_expression_failure_rejects_the_deployment() {
    // Zeebe `TimerCatchEventExpressionValidator`: a process start timer is
    // evaluated at deploy; a failure rejects the deployment.
    let def = parse(
        r#"<bpmn:process id="ts" isExecutable="true">
          <bpmn:startEvent id="s">
            <bpmn:timerEventDefinition><bpmn:timeDuration>=missing</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:startEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
        </bpmn:process>"#,
    );
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::DeployProcess(def))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::StartEventExpressionInvalid { ref element_id, .. } if element_id == "s"),
        "{err:?}"
    );
    assert!(engine.timers().is_empty());
}

// ---------------------------------------------------------------------------
// Multi-instance (Zeebe `MultiInstanceBodyProcessor`)
// ---------------------------------------------------------------------------

fn mi_process(loop_attrs: &str, completion: &str) -> ProcessDefinition {
    mi_process_ending_in(loop_attrs, completion, r#"<bpmn:endEvent id="e" />"#)
}

/// [`mi_process`] whose MI activity flows into `end` (element id `e`).
fn mi_process_ending_in(loop_attrs: &str, completion: &str, end: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="mi" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics isSequential="true">
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics {loop_attrs} />
              </bpmn:extensionElements>
              {completion}
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          {end}
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn mi_input_collection_not_a_list_raises_incident_on_the_body() {
    // Zeebe `evaluateArrayExpression`: nano used to run zero iterations and
    // complete the activity at once.
    let (mut engine, inst) = start(
        mi_process(r#"inputCollection="=items" inputElement="item""#, ""),
        vars(&[("items", Value::Str("abc".into()))]),
    );
    assert!(
        !engine.is_completed(inst),
        "the loop must not silently run 0 times"
    );
    let incident = extract_value_incident(&engine, "work", "to be 'ARRAY', but was 'STRING'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("items", Value::List(vec![Value::Int(1)]))],
    );
    complete_one(&mut engine, "work");
    assert!(engine.is_completed(inst));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn mi_completion_condition_failure_raises_incident_on_the_child() {
    // Zeebe `satisfiesCompletionCondition` (beforeExecutionPathCompleted): an
    // evaluation failure is an incident — nano used to treat it as `false`.
    let (mut engine, inst) = start(
        mi_process(
            r#"inputCollection="=items" inputElement="item""#,
            r#"<bpmn:completionCondition>=done</bpmn:completionCondition>"#,
        ),
        vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
    );
    complete_one(&mut engine, "work");
    assert!(
        engine.pending_jobs().is_empty(),
        "the next child must not start"
    );
    let incident = extract_value_incident(&engine, "work", "'=done'");
    set_and_resolve(&mut engine, inst, &incident, &[("done", Value::Bool(true))]);
    assert!(
        engine.is_completed(inst),
        "the satisfied condition completes the body"
    );
}

#[test]
fn mi_completion_condition_sees_enclosing_subprocess_locals() {
    // The completion condition is evaluated in the MI body's visible scope,
    // not process-root variables only: a local of the enclosing sub-process
    // must resolve rather than raise an incident.
    let def = parse(
        r#"<bpmn:process id="mi" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:subProcess id="sp">
            <bpmn:extensionElements>
              <zeebe:ioMapping><zeebe:input source="=true" target="done" /></zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:startEvent id="ss" />
            <bpmn:serviceTask id="work">
              <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
              <bpmn:multiInstanceLoopCharacteristics isSequential="true">
                <bpmn:extensionElements>
                  <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
                </bpmn:extensionElements>
                <bpmn:completionCondition>=done</bpmn:completionCondition>
              </bpmn:multiInstanceLoopCharacteristics>
            </bpmn:serviceTask>
            <bpmn:endEvent id="se" />
            <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="work" />
            <bpmn:sequenceFlow id="sf2" sourceRef="work" targetRef="se" />
          </bpmn:subProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sp" />
          <bpmn:sequenceFlow id="f2" sourceRef="sp" targetRef="e" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(
        def,
        vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
    );
    complete_one(&mut engine, "work");
    assert!(
        engine.is_completed(inst),
        "a sub-process local must be visible to the completion condition (done=true ends the loop)"
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn mi_output_element_failure_raises_incident_on_the_child() {
    // Zeebe `MultiInstanceOutputCollectionBehavior.readOutputElementVariable`:
    // a failure is an incident — nano used to collect `null`.
    let (mut engine, inst) = start(
        mi_process_ending_in(
            r#"inputCollection="=items" inputElement="item" outputCollection="out" outputElement="=item + suffix""#,
            "",
            r#"<bpmn:userTask id="e" />"#,
        ),
        vars(&[("items", Value::List(vec![Value::Str("a".into())]))]),
    );
    complete_one(&mut engine, "work");
    let incident = extract_value_incident(&engine, "work", "'=item + suffix'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("suffix", Value::Str("!".into()))],
    );
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("out"),
        Some(&Value::List(vec![Value::Str("a!".into())]))
    );
}

// ---------------------------------------------------------------------------
// Ad-hoc sub-process (Zeebe `AdHocSubProcessProcessor`)
// ---------------------------------------------------------------------------

fn adhoc_process(adhoc_attrs: &str, completion: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="ah" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="host">
            <bpmn:extensionElements><zeebe:adHoc {adhoc_attrs} /></bpmn:extensionElements>
            {completion}
            <bpmn:serviceTask id="tool">
              <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="host" />
          <bpmn:sequenceFlow id="f2" sourceRef="host" targetRef="e" />
        </bpmn:process>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_active_elements_collection_failure_raises_incident() {
    // Zeebe `readActivateElementsCollection` (evaluateArrayOfStringsExpression):
    // nano used to activate nothing and complete the container at once.
    let (mut engine, inst) = start(
        adhoc_process(r#"activeElementsCollection="=tools""#, ""),
        vars(&[("tools", Value::Str("tool".into()))]),
    );
    assert!(!engine.is_completed(inst));
    let incident = extract_value_incident(&engine, "host", "to be 'ARRAY', but was 'STRING'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("tools", Value::List(vec![Value::Str("tool".into())]))],
    );
    assert_eq!(job_types(&engine), vec!["tool".to_string()]);
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_completion_condition_failure_raises_incident() {
    // Zeebe `AdHocSubProcessProcessor` evaluates the completion condition with
    // `evaluateBooleanExpression`; a failure is an incident, not `false`.
    let (mut engine, inst) = start(
        adhoc_process(
            r#"activeElementsCollection="=[&quot;tool&quot;]""#,
            r#"<bpmn:completionCondition>=done</bpmn:completionCondition>"#,
        ),
        HashMap::new(),
    );
    complete_one(&mut engine, "tool");
    let incident = extract_value_incident(&engine, "tool", "'=done'");
    set_and_resolve(&mut engine, inst, &incident, &[("done", Value::Bool(true))]);
    assert!(engine.is_completed(inst));
}

// ---------------------------------------------------------------------------
// User task properties (Zeebe `BpmnUserTaskBehavior.evaluateUserTaskExpressions`)
// ---------------------------------------------------------------------------

fn user_task(ext: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="ut" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements><zeebe:userTask />{ext}</bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
        </bpmn:process>"#
    ))
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn user_task_property_expression_failures_raise_incidents() {
    let cases: Vec<(&str, HashMap<String, Value>, &str)> = vec![
        // evaluateAssigneeExpression -> evaluateStringExpression: NULL fails.
        (
            r#"<zeebe:assignmentDefinition assignee="=who" />"#,
            vars(&[]),
            "to be 'STRING', but was 'NULL'",
        ),
        (
            r#"<zeebe:assignmentDefinition assignee="=who" />"#,
            vars(&[("who", Value::Int(7))]),
            "to be 'STRING', but was 'NUMBER'",
        ),
        // evaluateArrayOfStringsExpression.
        (
            r#"<zeebe:assignmentDefinition candidateGroups="=groups" />"#,
            vars(&[("groups", Value::Int(1))]),
            "to be 'ARRAY', but was 'NUMBER'",
        ),
        (
            r#"<zeebe:assignmentDefinition candidateUsers="=users" />"#,
            vars(&[("users", Value::List(vec![Value::Int(1)]))]),
            "containing 'STRING' items",
        ),
        // evaluateDateTimeExpression(nullable): an unparseable string fails.
        (
            r#"<zeebe:taskSchedule dueDate="=due" />"#,
            vars(&[("due", Value::Str("tomorrow".into()))]),
            "tomorrow",
        ),
        // evaluatePriorityExpression: NUMBER in [0,100].
        (
            r#"<zeebe:priorityDefinition priority="=prio" />"#,
            vars(&[("prio", Value::Str("high".into()))]),
            "'NUMBER', but was 'STRING'",
        ),
        (
            r#"<zeebe:priorityDefinition priority="=prio" />"#,
            vars(&[("prio", Value::Int(101))]),
            "Expected priority to be in the range [0,100] but was '101'.",
        ),
    ];
    for (ext, vars_in, needle) in cases {
        let (engine, _) = start(user_task(ext), vars_in);
        assert!(engine.state().user_tasks.is_empty(), "{ext}: no user task");
        extract_value_incident(&engine, "review", needle);
    }
}

#[test]
fn user_task_nullable_dates_and_resolution() {
    // A null due date is legal (Zeebe `isNullable = true`).
    let (engine, _) = start(
        user_task(r#"<zeebe:taskSchedule dueDate="=due" />"#),
        HashMap::new(),
    );
    assert!(engine.active_incidents().is_empty());
    assert_eq!(engine.state().user_tasks.len(), 1);

    // Resolving an assignee incident after fixing the variable creates the task.
    let (mut engine, inst) = start(
        user_task(r#"<zeebe:assignmentDefinition assignee="=who" />"#),
        HashMap::new(),
    );
    let incident = extract_value_incident(&engine, "review", "'NULL'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("who", Value::Str("ana".into()))],
    );
    let tasks: Vec<_> = engine.state().user_tasks.values().collect();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].assignee.as_deref(), Some("ana"));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn job_priority_expression_failure_raises_incident() {
    // Zeebe `BpmnJobBehavior.evalPriorityExp`: a non-NUMBER job priority is
    // EXTRACT_VALUE_ERROR — nano used to default it.
    let def = ProcessBuilder::new("jp")
        .start_event("start")
        .service_task_with_priority("work", "work", Some("=prio".to_string()))
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = start(def, vars(&[("prio", Value::Str("high".into()))]));
    assert!(engine.pending_jobs().is_empty());
    let incident = extract_value_incident(&engine, "work", "for the job priority to be 'NUMBER'");
    set_and_resolve(&mut engine, inst, &incident, &[("prio", Value::Int(70))]);
    assert_eq!(engine.pending_jobs()[0].priority, 70);
}

// ---------------------------------------------------------------------------
// Incident-resolution retry fidelity (#1398 review): a resolved incident
// re-drives ONLY the failed work — never the already-applied input mappings,
// the already-run start listeners, the host's live job, or the other boundary
// subscriptions — and preserves the already-projected call-activity outputs.
// ---------------------------------------------------------------------------

/// A service task with a non-idempotent input mapping (`=counter + 1 ->
/// counter`) whose job TYPE expression fails on activation.
fn task_with_compounding_input_and_bad_job_type() -> ProcessDefinition {
    parse(
        r#"<bpmn:process id="nip" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="=jobType" />
              <zeebe:ioMapping><zeebe:input source="=counter + 1" target="counter" /></zeebe:ioMapping>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#,
    )
}

#[test]
fn resolved_activation_body_failure_does_not_reapply_input_mappings() {
    // The input mapping `=counter + 1 -> counter` applies once (counter 0 -> 1).
    // The job-type expression then fails. Resolving the incident must re-run
    // ONLY the behaviour (mint the job) — re-applying the input mapping would
    // increment `counter` a second time (#1398 review).
    let (mut engine, inst) = start(
        task_with_compounding_input_and_bad_job_type(),
        vars(&[("counter", Value::Int(0))]),
    );
    assert!(engine.pending_jobs().is_empty(), "no job yet");
    let incident = extract_value_incident(&engine, "work", "'=jobType'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("jobType", Value::Str("work".into()))],
    );
    assert_eq!(job_types(&engine), vec!["work".to_string()]);
    let scope = engine.variables_for_element(inst, engine.pending_jobs()[0].element_instance_key);
    assert_eq!(
        scope.get("counter"),
        Some(&Value::Int(1)),
        "the input mapping applied exactly once, not again on retry"
    );
}

/// A user task with a `creating` listener whose assignee expression fails AFTER
/// the listener chain drained.
fn user_task_with_listener_and_bad_assignee() -> ProcessDefinition {
    parse(
        r#"<bpmn:process id="utl" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:assignmentDefinition assignee="=assignee" />
              <zeebe:taskListeners>
                <zeebe:taskListener eventType="creating" type="audit" />
              </zeebe:taskListeners>
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
        </bpmn:process>"#,
    )
}

#[test]
fn resolved_assignee_failure_does_not_rerun_completed_start_listener() {
    // The `creating` listener job runs and completes; the assignee expression
    // then fails. Resolving the incident must NOT run the `creating` listener a
    // second time (#1398 review).
    let (mut engine, inst) = start(user_task_with_listener_and_bad_assignee(), HashMap::new());
    // The creating listener job exists; the assignee failure parks the task.
    let listener_jobs: Vec<String> = engine
        .pending_jobs()
        .iter()
        .map(|j| j.job_type.clone())
        .collect();
    // The assignee failure happens at activation, before the listener chain
    // runs — so resolve the assignee FIRST, then drain the listener once.
    let incident = extract_value_incident(&engine, "review", "'=assignee'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("assignee", Value::Str("sam".into()))],
    );
    // Now the creating listener job is minted exactly once.
    let jobs = engine.pending_jobs();
    assert_eq!(
        jobs.iter().filter(|j| j.job_type == "audit").count(),
        1,
        "the creating listener ran exactly once: {listener_jobs:?} -> {jobs:?}"
    );
}

#[test]
fn creating_task_listener_type_reads_staged_input_mapping() {
    // The first `creating` task listener is minted before the task's input
    // mappings are applied to state. A listener `type="=listenerType"` supplied
    // by an input mapping must resolve against the staged post-input view, not
    // raise an incident (#1398 review).
    let def = parse(
        r#"<bpmn:process id="tlm" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:ioMapping><zeebe:input source="=&quot;create-audit&quot;" target="listenerType" /></zeebe:ioMapping>
              <zeebe:taskListeners>
                <zeebe:taskListener eventType="creating" type="=listenerType" />
              </zeebe:taskListeners>
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
        </bpmn:process>"#,
    );
    let (engine, _inst) = start(def, HashMap::new());
    assert!(
        engine.active_incidents().is_empty(),
        "the mapped listener type resolves, no incident: {:?}",
        engine.active_incidents()
    );
    assert_eq!(job_types(&engine), vec!["create-audit".to_string()]);
}

/// A service task with a non-interrupting timer boundary whose duration
/// expression fails.
fn task_with_failing_boundary_timer() -> ProcessDefinition {
    parse(
        r#"<bpmn:process id="nbt" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:boundaryEvent id="ping" attachedToRef="work" cancelActivity="false">
            <bpmn:timerEventDefinition><bpmn:timeDuration>=d</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="e" />
          <bpmn:endEvent id="eb" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
          <bpmn:sequenceFlow id="f3" sourceRef="ping" targetRef="eb" />
        </bpmn:process>"#,
    )
}

#[test]
fn resolved_boundary_timer_failure_does_not_duplicate_host_job() {
    // The host's job is minted; the boundary timer's duration expression fails.
    // Resolving the incident must re-arm ONLY the timer — a full activation
    // re-drive would mint a SECOND host job (#1398 review).
    let (mut engine, inst) = start(task_with_failing_boundary_timer(), HashMap::new());
    assert_eq!(
        job_types(&engine),
        vec!["work".to_string()],
        "host job lives"
    );
    assert!(engine.timers().is_empty(), "no timer armed yet");
    let incident = extract_value_incident(&engine, "work", "'=d'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("d", Value::Str("PT1H".into()))],
    );
    assert_eq!(
        job_types(&engine),
        vec!["work".to_string()],
        "exactly one host job — the retry did not re-run the activation"
    );
    assert_eq!(engine.timers().len(), 1, "the boundary timer is re-armed");
    assert_eq!(engine.timers()[0].due_at, engine.now + 3_600_000);
}

#[test]
fn mi_call_activity_completion_retry_reuses_projected_output_overlay() {
    // A multi-instance CALL ACTIVITY child whose `outputElement` fails: the
    // callee's results live in the projected overlay, not the child scope, so a
    // generic completion retry would collect `null`. The retry must reuse the
    // preserved overlay (#1398 review).
    let callee = ProcessBuilder::new("callee")
        .start_event("cs")
        .service_task("ct", "callee-work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    let def = ProcessBuilder::new("mi-call")
        .start_event("start")
        .call_activity("each", "callee")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=result + suffix".to_string()),
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("done")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(callee))
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-call",
            vars(&[("items", Value::List(vec![Value::Int(7)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // Complete the callee's job, producing `result` — the child then fails on
    // `outputElement = result + suffix` (suffix unset).
    let jobs = engine.activate_jobs("callee-work", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1);
    engine
        .apply_command(Command::complete_job_with_result(
            jobs[0].key,
            HashMap::from([("result".to_string(), Value::Int(7))]),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    let incident = extract_value_incident(&engine, "each", "suffix");
    // Fix `suffix` and resolve: the retry must collect the callee's real
    // projected result (7 + 1 = 8), not `null`.
    set_and_resolve(&mut engine, key, &incident, &[("suffix", Value::Int(1))]);
    // The body drained and wrote the aggregate; the token advanced to the sink.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(8)])),
        "the retry reused the preserved output overlay"
    );
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

#[test]
fn mi_completion_latch_skips_reevaluation_after_variable_change() {
    // With `cancelRemainingInstances=false`, a fulfilled completion condition is
    // latched: a later child completes the drained body WITHOUT re-evaluating
    // the condition — even after the variable it reads becomes unreadable
    // (#1398 review).
    let def = parse(
        r#"<bpmn:process id="mil" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics isSequential="false" cancelRemainingInstances="false">
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
              </bpmn:extensionElements>
              <bpmn:completionCondition>=done</bpmn:completionCondition>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(
        def,
        vars(&[
            ("items", Value::List(vec![Value::Int(1), Value::Int(2)])),
            ("done", Value::Bool(true)),
        ]),
    );
    // First child completes: the condition fires (done=true) and latches; the
    // body defers (cancelRemainingInstances=false) with one child still active.
    // Activate and complete exactly ONE child (a batch-10 `complete_one` would
    // drain both at once).
    let first = engine
        .activate_jobs("work", "test-worker", 1, 60_000, 0)
        .into_iter()
        .next()
        .expect("one child job");
    engine
        .apply_command(Command::complete_job(first.key))
        .unwrap();
    assert!(
        !engine.is_completed(inst),
        "the body defers until the remaining child drains"
    );
    assert_eq!(engine.pending_jobs().len(), 1, "one child still running");
    // Make the condition unreadable, then complete the remaining child: the
    // latched fulfilment must complete the body WITHOUT re-evaluating `=done`.
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("done", Value::Str("not-a-bool".into()))]),
        ))
        .unwrap();
    let second = engine
        .activate_jobs("work", "test-worker", 1, 60_000, 0)
        .into_iter()
        .next()
        .expect("the remaining child job");
    engine
        .apply_command(Command::complete_job(second.key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "the latched condition completes the drained body without re-evaluating"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no incident from re-evaluating the latched condition: {:?}",
        engine.active_incidents()
    );
}

#[test]
fn resolved_boundary_timer_failure_preserves_user_task_host() {
    // The same boundary-scoped retry preserves a USER TASK host: the task record
    // is created, the boundary timer's expression fails, and resolution re-arms
    // ONLY the timer — never a second task (#1398 review, host-preservation
    // class).
    let def = parse(
        r#"<bpmn:process id="utb" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:boundaryEvent id="ping" attachedToRef="review" cancelActivity="false">
            <bpmn:timerEventDefinition><bpmn:timeDuration>=d</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="e" />
          <bpmn:endEvent id="eb" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
          <bpmn:sequenceFlow id="f3" sourceRef="ping" targetRef="eb" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(def, HashMap::new());
    assert_eq!(
        engine.state().user_tasks.len(),
        1,
        "the user task host is live"
    );
    assert!(engine.timers().is_empty(), "no timer armed yet");
    let incident = extract_value_incident(&engine, "review", "'=d'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("d", Value::Str("PT1H".into()))],
    );
    assert_eq!(
        engine.state().user_tasks.len(),
        1,
        "exactly one user task — the retry did not re-run the activation"
    );
    assert_eq!(engine.timers().len(), 1, "the boundary timer is re-armed");
}

#[test]
fn resolved_boundary_timer_failure_rearms_every_sibling_boundary() {
    // A failing timer between two armable siblings: the boundary armed before it
    // stays live, and resolution re-arms the failed timer AND every sibling
    // after it (timer + message) — the host never ends up missing boundaries
    // (#1398 review, mixed static/failing boundaries).
    let def = parse(
        r#"<bpmn:message id="msg" name="poke"><bpmn:extensionElements><zeebe:subscription correlationKey="=&quot;k&quot;" /></bpmn:extensionElements></bpmn:message>
        <bpmn:process id="mixb" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:boundaryEvent id="a_static" attachedToRef="review" cancelActivity="false">
            <bpmn:timerEventDefinition><bpmn:timeDuration>PT2H</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:boundaryEvent>
          <bpmn:boundaryEvent id="b_failing" attachedToRef="review" cancelActivity="false">
            <bpmn:timerEventDefinition><bpmn:timeDuration>=d</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:boundaryEvent>
          <bpmn:boundaryEvent id="c_static" attachedToRef="review" cancelActivity="false">
            <bpmn:timerEventDefinition><bpmn:timeDuration>PT3H</bpmn:timeDuration></bpmn:timerEventDefinition>
          </bpmn:boundaryEvent>
          <bpmn:boundaryEvent id="m_msg" attachedToRef="review" cancelActivity="false">
            <bpmn:messageEventDefinition messageRef="msg" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="e" />
          <bpmn:endEvent id="ea" />
          <bpmn:endEvent id="eb" />
          <bpmn:endEvent id="ec" />
          <bpmn:endEvent id="em" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
          <bpmn:sequenceFlow id="fa" sourceRef="a_static" targetRef="ea" />
          <bpmn:sequenceFlow id="fb" sourceRef="b_failing" targetRef="eb" />
          <bpmn:sequenceFlow id="fc" sourceRef="c_static" targetRef="ec" />
          <bpmn:sequenceFlow id="fm" sourceRef="m_msg" targetRef="em" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(def, HashMap::new());
    assert_eq!(engine.state().user_tasks.len(), 1, "the host is live");
    let incident = extract_value_incident(&engine, "review", "'=d'");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("d", Value::Str("PT1H".into()))],
    );
    assert_eq!(engine.state().user_tasks.len(), 1, "no duplicate host");
    assert_eq!(
        engine.timers().len(),
        3,
        "every timer boundary is armed exactly once: {:?}",
        engine.timers()
    );
    assert_eq!(
        engine.state().message_subscriptions.len(),
        1,
        "the message boundary after the failing timer is armed too"
    );
    assert!(engine.active_incidents().is_empty());
}

#[test]
fn sequential_mi_completion_latch_completes_without_spawning_remaining_items() {
    // Sequential counterpart of the parallel latch: with
    // `cancelRemainingInstances=false`, a condition fulfilled by the first child
    // completes the body once that child drains, instead of wedging with
    // unspawned items and no active child (#1398 review).
    let def = parse(
        r#"<bpmn:process id="mils" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics isSequential="true" cancelRemainingInstances="false">
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
              </bpmn:extensionElements>
              <bpmn:completionCondition>=done</bpmn:completionCondition>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(
        def,
        vars(&[
            (
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
            ("done", Value::Bool(true)),
        ]),
    );
    let first = engine
        .activate_jobs("work", "test-worker", 10, 60_000, 0)
        .into_iter()
        .next()
        .expect("first sequential child job");
    engine
        .apply_command(Command::complete_job(first.key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "the latched sequential body completes; pending jobs: {:?}",
        engine.pending_jobs()
    );
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn resolved_agent_job_retries_failure_preserves_container_results() {
    // The agent job's `retries` expression fails when the job for the NEXT turn
    // is minted (after a tool drained). Resolution must re-mint only the agent
    // job — not re-run the container's activation, which would reset its
    // `outputCollection` and ad-hoc state (#1398 review).
    let def = parse(
        r#"<bpmn:process id="agentr" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" retries="=agentRetries" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>"#,
    );
    let (mut engine, inst) = start(def, vars(&[("agentRetries", Value::Int(3))]));
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 60_000, 0)
        .into_iter()
        .next()
        .expect("turn-1 agent job");
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    // Break the agent's retries expression, then drain the tool: the turn-2
    // agent job cannot be minted.
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("agentRetries", Value::Str("bad".into()))]),
        ))
        .unwrap();
    let tool = engine
        .activate_jobs("tool", "W", 10, 60_000, 0)
        .into_iter()
        .next()
        .expect("tool job");
    engine
        .apply_command(Command::complete_job_with(
            tool.key,
            vars(&[("result", Value::Str("A".into()))]),
        ))
        .unwrap();
    let collected = |engine: &Engine| {
        engine
            .instance(inst)
            .unwrap()
            .scope_variables
            .get(&container)
            .and_then(|m| m.get("results"))
            .cloned()
    };
    assert_eq!(
        collected(&engine),
        Some(Value::List(vec![Value::Str("A".into())]))
    );
    let incident = extract_value_incident(&engine, "agent", "agentRetries");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("agentRetries", Value::Int(3))],
    );
    assert_eq!(
        collected(&engine),
        Some(Value::List(vec![Value::Str("A".into())])),
        "the collected tool result survives the retry"
    );
    let agents: Vec<_> = engine
        .pending_jobs()
        .into_iter()
        .filter(|j| j.job_type == "agent-worker")
        .collect();
    assert_eq!(agents.len(), 1, "exactly one turn-2 agent job is minted");
    assert_eq!(agents[0].element_instance_key, container);
}

// ---------------------------------------------------------------------------
// Execution-listener job-property retry re-mints ONLY the failed listener
// (#1398 review): a lifecycle-phase restart would replay completed listeners,
// non-idempotent input/output mappings or boundary arming.
// ---------------------------------------------------------------------------

/// `work` declares a non-idempotent mapping (`input`: `=n + 1 -> n` local,
/// `output`: `=n -> n` root) and two `event_type` listeners, `first` and
/// `=second` (an expression that fails until `second` is set).
fn task_with_two_listeners(event_type: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<bpmn:process id="el2" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="work" />
              <zeebe:ioMapping>
                <zeebe:input source="=n + 1" target="n" />
                <zeebe:output source="=n" target="n" />
              </zeebe:ioMapping>
              <zeebe:executionListeners>
                <zeebe:executionListener eventType="{event_type}" type="first" />
                <zeebe:executionListener eventType="{event_type}" type="=second" />
              </zeebe:executionListeners>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
          <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
        </bpmn:process>"#
    ))
}

fn complete_only_job(engine: &mut Engine, job_type: &str) {
    let jobs = engine.activate_jobs(job_type, "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1, "one {job_type} job: {:?}", job_types(engine));
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn later_start_listener_failure_retries_only_that_listener() {
    let (mut engine, inst) = start(
        task_with_two_listeners("start"),
        vars(&[("n", Value::Int(0))]),
    );
    complete_only_job(&mut engine, "first");
    let incident = extract_value_incident(&engine, "work", "second");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("second", Value::Str("second".into()))],
    );
    // Only the failed listener is re-minted — `first` does not run again.
    assert_eq!(job_types(&engine), vec!["second".to_string()]);
    complete_only_job(&mut engine, "second");
    // The input mapping ran exactly once (0 + 1), not again on the retry.
    let jobs = engine.activate_jobs("work", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].variables.get("n"), Some(&Value::Int(1)));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn later_end_listener_failure_retries_only_that_listener() {
    let (mut engine, inst) = start(
        task_with_two_listeners("end"),
        vars(&[("n", Value::Int(0))]),
    );
    complete_only_job(&mut engine, "work");
    complete_only_job(&mut engine, "first");
    let incident = extract_value_incident(&engine, "work", "second");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("second", Value::Str("second".into()))],
    );
    assert_eq!(job_types(&engine), vec!["second".to_string()]);
    // The input (0 + 1) and output mappings ran exactly once.
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("n"),
        Some(&Value::Int(1))
    );
    complete_only_job(&mut engine, "second");
    assert!(engine.pending_jobs().is_empty());
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn first_start_listener_failure_does_not_reapply_input_mappings() {
    let (mut engine, inst) = start(task_with_start_listener(r#"type="=audit""#), HashMap::new());
    let incident = extract_value_incident(&engine, "work", "audit");
    set_and_resolve(
        &mut engine,
        inst,
        &incident,
        &[("audit", Value::Str("audit".into()))],
    );
    assert_eq!(job_types(&engine), vec!["audit".to_string()]);
    complete_only_job(&mut engine, "audit");
    assert_eq!(job_types(&engine), vec!["work".to_string()]);
}
