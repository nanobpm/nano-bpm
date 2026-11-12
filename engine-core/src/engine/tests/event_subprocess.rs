//! Event sub-process (`subProcess triggeredByEvent="true"`) engine tests —
//! #1555.
//!
//! Model/deploy contract (wave 0): deploying a model containing an event
//! sub-process succeeds and the event sub-process is never activated by
//! sequence flow. Runtime (esp-runtime slice): the start-event trigger
//! (message, timer, signal, conditional) is armed when the enclosing scope
//! activates and disarmed when it completes or terminates; an interrupting
//! trigger tears the scope's other children down, a non-interrupting one runs
//! the event sub-process concurrently (and repeatedly).
use super::*;

/// A process with a message event sub-process beside the main flow
/// `s -> work -> e`.
fn esp_message_xml() -> String {
    r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
         <bpmn:message id="Msg" name="orderMsg">
           <bpmn:extensionElements>
             <zeebe:subscription correlationKey="=orderId" />
           </bpmn:extensionElements>
         </bpmn:message>
         <bpmn:process id="p" isExecutable="true">
           <bpmn:startEvent id="s" />
           <bpmn:serviceTask id="work">
             <bpmn:extensionElements><zeebe:taskDefinition type="w" /></bpmn:extensionElements>
           </bpmn:serviceTask>
           <bpmn:endEvent id="e" />
           <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
           <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
           <bpmn:subProcess id="esp" triggeredByEvent="true">
             <bpmn:startEvent id="esp_start"><bpmn:messageEventDefinition messageRef="Msg" /></bpmn:startEvent>
             <bpmn:task id="esp_task" />
             <bpmn:endEvent id="esp_end" />
             <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_task" />
             <bpmn:sequenceFlow id="ef2" sourceRef="esp_task" targetRef="esp_end" />
           </bpmn:subProcess>
         </bpmn:process>
       </bpmn:definitions>"#
        .to_string()
}

#[test]
fn deploying_a_model_with_an_event_subprocess_succeeds() {
    // The model parses and deploys; the event sub-process is in the definition.
    let def = crate::bpmn::parse_bpmn(&esp_message_xml())
        .unwrap()
        .remove(0);
    assert!(matches!(
        def.element("esp").unwrap().kind,
        crate::model::ElementKind::EventSubProcess { .. }
    ));
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(def))
        .expect("a model with an event sub-process deploys");
}

#[test]
fn event_subprocess_is_never_triggered_by_token_flow() {
    // Until the runtime slice lands, the event sub-process is inert: running
    // the main flow to completion never activates `esp` or its inner elements,
    // and the instance completes normally (no behaviour change).
    let def = crate::bpmn::parse_bpmn(&esp_message_xml())
        .unwrap()
        .remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Int(7));
    let created = engine
        .apply_command(Command::create_instance_with("p", vars))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Drive the only job to completion; the main flow runs to the end.
    let events = complete_one(&mut engine, "w");

    // The instance completed; the event sub-process never activated.
    assert!(
        engine.is_completed(inst),
        "main flow completes with an inert event sub-process present"
    );
    let activated = |events: &[Event], id: &str| {
        events
            .iter()
            .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == id))
    };
    assert!(
        !activated(&created, "esp") && !activated(&events, "esp"),
        "event sub-process never activated"
    );
    assert!(
        !activated(&created, "esp_start") && !activated(&events, "esp_start"),
        "event sub-process start never activated"
    );
    assert!(
        activated(&created, "work") || activated(&events, "work"),
        "main flow ran"
    );
}

/// Builds `s -> e` plus an event sub-process `esp` with inner `esp_start ->
/// esp_end`, letting each test mutate one structural aspect.
fn esp_builder(
    trigger: crate::model::EventSubProcessTrigger,
    interrupting: bool,
) -> crate::model::ProcessBuilder {
    crate::model::ProcessBuilder::new("p")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .event_sub_process("esp", "esp_start", trigger, interrupting)
        .start_event("esp_start")
        .end_event("esp_end")
        .connect("esp_start", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_end", "esp")
}

fn signal() -> crate::model::EventSubProcessTrigger {
    crate::model::EventSubProcessTrigger::Signal {
        signal_name: "sig".into(),
    }
}

fn assert_esp_rejected(b: crate::model::ProcessBuilder, needle: &str) {
    match b.build() {
        Err(crate::model::BuildError::InvalidEventSubProcess { id, reason }) => {
            assert_eq!(id, "esp");
            assert!(
                reason.contains(needle),
                "reason {reason:?} lacks {needle:?}"
            );
        }
        other => panic!("expected InvalidEventSubProcess, got {other:?}"),
    }
}

#[test]
fn well_formed_event_subprocess_builds() {
    esp_builder(signal(), false).build().unwrap();
}

#[test]
fn flow_into_event_subprocess_is_rejected() {
    assert_esp_rejected(esp_builder(signal(), true).connect("s", "esp"), "incoming");
}

#[test]
fn flow_out_of_event_subprocess_is_rejected() {
    assert_esp_rejected(esp_builder(signal(), true).connect("esp", "e"), "outgoing");
}

#[test]
fn unknown_event_subprocess_start_is_rejected() {
    let b = crate::model::ProcessBuilder::new("p")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .event_sub_process("esp", "ghost", signal(), true);
    assert_esp_rejected(b, "unknown start event ghost");
}

#[test]
fn event_subprocess_start_outside_the_container_is_rejected() {
    // `s` is a start event but the process-level one, not inside `esp`.
    let b = crate::model::ProcessBuilder::new("p")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .event_sub_process("esp", "s", signal(), true);
    assert_esp_rejected(b, "directly contained");
}

#[test]
fn event_subprocess_start_that_is_not_a_start_event_is_rejected() {
    let b = crate::model::ProcessBuilder::new("p")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .event_sub_process("esp", "t", signal(), true)
        .task("t")
        .contained_in("t", "esp");
    assert_esp_rejected(b, "directly contained");
}

#[test]
fn non_interrupting_error_event_subprocess_is_rejected_by_builder() {
    let err = crate::model::EventSubProcessTrigger::Error { error_code: None };
    assert_esp_rejected(esp_builder(err, false), "interrupting");
}

#[test]
fn bpmn_sequence_flow_into_or_out_of_event_subprocess_is_rejected() {
    let base = esp_message_xml();
    for extra in [
        r#"<bpmn:sequenceFlow id="fx" sourceRef="s" targetRef="esp" />"#,
        r#"<bpmn:sequenceFlow id="fx" sourceRef="esp" targetRef="e" />"#,
    ] {
        let xml = base.replace(
            r#"<bpmn:subProcess id="esp""#,
            &format!(r#"{extra}<bpmn:subProcess id="esp""#),
        );
        let err = crate::bpmn::parse_bpmn(&xml).unwrap_err().to_string();
        assert!(err.contains("event sub-process esp"), "{extra}: {err}");
    }
}

#[test]
fn flow_from_outside_into_event_subprocess_child_is_rejected() {
    assert_esp_rejected(
        esp_builder(signal(), true).connect("s", "esp_end"),
        "crosses",
    );
}

#[test]
fn flow_from_event_subprocess_child_to_outside_is_rejected() {
    assert_esp_rejected(
        esp_builder(signal(), true).connect("esp_start", "e"),
        "crosses",
    );
}

#[test]
fn flow_into_nested_descendant_of_event_subprocess_is_rejected() {
    let b = esp_builder(signal(), true)
        .sub_process("inner", "inner_start")
        .start_event("inner_start")
        .end_event("inner_end")
        .connect("inner_start", "inner_end")
        .contained_in("inner", "esp")
        .contained_in("inner_start", "inner")
        .contained_in("inner_end", "inner")
        .connect("s", "inner_end");
    assert_esp_rejected(b, "crosses");
    let b = esp_builder(signal(), true)
        .sub_process("inner", "inner_start")
        .start_event("inner_start")
        .end_event("inner_end")
        .connect("inner_start", "inner_end")
        .contained_in("inner", "esp")
        .contained_in("inner_start", "inner")
        .contained_in("inner_end", "inner")
        .connect("inner_end", "e");
    assert_esp_rejected(b, "crosses");
}

#[test]
fn second_start_event_in_event_subprocess_is_rejected() {
    let b = esp_builder(signal(), true)
        .start_event("esp_start2")
        .contained_in("esp_start2", "esp");
    assert_esp_rejected(b, "exactly one start event");
}

#[test]
fn typed_start_event_in_event_subprocess_is_rejected() {
    let b = crate::model::ProcessBuilder::new("p")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .event_sub_process("esp", "esp_start", signal(), true)
        .message_start_event("esp_start", "other")
        .end_event("esp_end")
        .connect("esp_start", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_end", "esp");
    assert_esp_rejected(b, "plain start event");
}

#[test]
fn bpmn_message_event_subprocess_start_is_a_plain_start_event() {
    let def = crate::bpmn::parse_bpmn(&esp_message_xml()).unwrap();
    assert!(matches!(
        def[0].elements["esp_start"].kind,
        crate::model::ElementKind::StartEvent
    ));
}

/// A built (valid) definition mutated to add `s -> esp`, which the builder
/// would reject — the shape a direct `DeployProcess` caller could hand in.
fn def_with_flow_into_esp() -> crate::model::ProcessDefinition {
    let mut def = esp_builder(signal(), true).build().unwrap();
    def.elements
        .get_mut("s")
        .unwrap()
        .outgoing
        .push(crate::model::SequenceFlow {
            to: "esp".into(),
            condition: None,
            is_default: false,
        });
    def
}

#[test]
fn direct_deploy_of_invalid_event_subprocess_is_rejected() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::DeployProcess(def_with_flow_into_esp()))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::InvalidProcessModel { ref process_id, .. } if process_id == "p"),
        "{err:?}"
    );
}

#[test]
fn batch_deploy_with_invalid_event_subprocess_is_rejected_atomically() {
    let mut engine = Engine::new();
    let good = crate::model::ProcessBuilder::new("good")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .build()
        .unwrap();
    let err = engine
        .apply_command(Command::DeployResources(vec![
            good,
            def_with_flow_into_esp(),
        ]))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::InvalidProcessModel { .. }),
        "{err:?}"
    );
    // Nothing from the batch was installed.
    assert!(
        engine
            .apply_command(Command::create_instance_with("good", HashMap::new()))
            .is_err(),
        "the valid process of a rejected batch must not be deployed"
    );
}

#[test]
fn modification_cannot_activate_an_event_subprocess_or_its_descendants() {
    let def = crate::bpmn::parse_bpmn(&esp_message_xml())
        .unwrap()
        .remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Int(7));
    let created = engine
        .apply_command(Command::create_instance_with("p", vars))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    for target in ["esp", "esp_task", "esp_end"] {
        let err = engine
            .apply_command(Command::ModifyInstance {
                instance_key: inst,
                activate_instructions: vec![crate::command::ActivateElementInstruction {
                    element_id: target.into(),
                    ..Default::default()
                }],
                terminate_instructions: vec![],
                terminate_element_ids: vec![],
                move_instructions: vec![],
            })
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ElementNotActivatable { ref reason, .. } if reason.contains("event sub-process")),
            "{target}: {err:?}"
        );
    }
}

#[test]
fn modification_with_cyclic_containment_does_not_hang() {
    // A malformed model can declare a containment cycle (`a` contained in `b`,
    // `b` contained in `a`) — the builder and the ESP validator both accept it
    // when no event sub-process exists. The ESP ancestor walk in
    // `plan_modification` must be cycle-bounded (it reuses
    // `element_flow_scope_chain`), or the modification hangs forever before
    // ever reaching the bounded scope resolver.
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("a", "w")
        .task("b")
        .end_event("e")
        .connect("s", "a")
        .connect("a", "e")
        .contained_in("a", "b")
        .contained_in("b", "a")
        .build()
        .expect("the builder accepts cyclic containment when no ESP exists");
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with("p", HashMap::new()))
        .unwrap();
    let inst = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .unwrap();

    // Terminates (no hang); the outcome itself is unspecified — the model is
    // malformed — but the modification must return promptly one way or the
    // other.
    let _ = engine.apply_command(Command::ModifyInstance {
        instance_key: inst,
        activate_instructions: vec![crate::command::ActivateElementInstruction {
            element_id: "a".into(),
            ..Default::default()
        }],
        terminate_instructions: vec![],
        terminate_element_ids: vec![],
        move_instructions: vec![],
    });
}

// ---------------------------------------------------------------------------
// Runtime (esp-runtime slice, #1555)
// ---------------------------------------------------------------------------

use crate::model::{EventSubProcessTrigger as Trig, TimerDef, TimerDefKind};

/// `s -> work("w") -> e` (or, `embedded`, `s -> sub[sub_s -> work -> sub_e]
/// -> after("after") -> e`) plus an event sub-process `esp` (in the process,
/// or inside `sub` when embedded) running `esp_start -> esp_work("esp") ->
/// esp_end`.
fn esp_model(trigger: Trig, interrupting: bool, embedded: bool) -> ProcessDefinition {
    let mut b = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "w")
        .end_event("e")
        .event_sub_process("esp", "esp_start", trigger, interrupting)
        .start_event("esp_start")
        .service_task("esp_work", "esp")
        .end_event("esp_end")
        .connect("esp_start", "esp_work")
        .connect("esp_work", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_work", "esp")
        .contained_in("esp_end", "esp");
    if embedded {
        b = b
            .sub_process("sub", "sub_s")
            .start_event("sub_s")
            .end_event("sub_e")
            .service_task("after", "after")
            .connect("s", "sub")
            .connect("sub_s", "work")
            .connect("work", "sub_e")
            .connect("sub", "after")
            .connect("after", "e")
            .contained_in("sub_s", "sub")
            .contained_in("work", "sub")
            .contained_in("sub_e", "sub")
            .contained_in("esp", "sub");
    } else {
        b = b.connect("s", "work").connect("work", "e");
    }
    b.build().unwrap()
}

fn msg() -> Trig {
    Trig::Message {
        message_name: "m".into(),
        correlation_key: "orderId".into(),
    }
}
fn timer(kind: TimerDefKind, expr: &str) -> Trig {
    Trig::Timer(TimerDef {
        kind,
        expr: expr.into(),
    })
}
fn sig() -> Trig {
    Trig::Signal {
        signal_name: "sig".into(),
    }
}
fn cond() -> Trig {
    Trig::Conditional {
        condition: "=flag".into(),
    }
}

#[derive(Clone, Copy, Debug)]
enum T {
    Message,
    Timer,
    Signal,
    Conditional,
}

fn trig_of(t: T) -> Trig {
    match t {
        T::Message => msg(),
        T::Timer => timer(TimerDefKind::Duration, "PT5S"),
        T::Signal => sig(),
        T::Conditional => cond(),
    }
}

fn start(engine: &mut Engine, def: ProcessDefinition) -> Key {
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("orderId", Value::Int(7)), ("flag", Value::Bool(false))]),
        ))
        .unwrap();
    created.iter().find_map(|e| e.instance_key()).unwrap()
}

/// Fires trigger `t` once (the timer at `due`).
fn fire(engine: &mut Engine, inst: Key, t: T, due: u64) -> Vec<Event> {
    match t {
        T::Message => engine
            .apply_command(Command::CorrelateMessage {
                message_name: "m".into(),
                correlation_key: "7".into(),
                variables: vars(&[("payload", Value::Int(42))]),
                business_id: None,
            })
            .unwrap(),
        T::Timer => engine.trigger_timers(due),
        T::Signal => engine
            .apply_command(Command::BroadcastSignal {
                signal_name: "sig".into(),
                variables: vars(&[("payload", Value::Int(42))]),
            })
            .unwrap(),
        T::Conditional => engine
            .apply_command(Command::SetVariables {
                scope_key: inst,
                variables: vars(&[("flag", Value::Bool(true))]),
                local: false,
            })
            .unwrap(),
    }
}

fn job_count(engine: &mut Engine, job_type: &str) -> usize {
    engine
        .state
        .jobs
        .values()
        .filter(|j| {
            j.job_type == job_type
                && matches!(
                    j.state,
                    crate::state::JobState::Created | crate::state::JobState::Activated
                )
        })
        .count()
}

/// Every still-armed trigger (timer / message / signal / conditional) of `inst`.
fn armed(engine: &Engine, inst: Key) -> usize {
    use crate::state::{MessageSubscriptionState as S, TimerState};
    let s = &engine.state;
    s.timers
        .values()
        .filter(|t| t.instance_key == inst && t.state == TimerState::Created)
        .count()
        + s.message_subscriptions
            .values()
            .filter(|x| x.instance_key == inst && matches!(x.state, S::Open | S::Opening))
            .count()
        + s.signal_subscriptions
            .values()
            .filter(|x| x.instance_key == inst && x.state == S::Open)
            .count()
        + s.conditional_subscriptions
            .values()
            .filter(|x| x.instance_key == inst && x.state == S::Open)
            .count()
}

const ALL: [T; 4] = [T::Message, T::Timer, T::Signal, T::Conditional];

fn interrupting_case(t: T, embedded: bool) {
    let mut engine = Engine::new();
    let inst = start(&mut engine, esp_model(trig_of(t), true, embedded));
    assert_eq!(
        armed(&engine, inst),
        1,
        "{t:?}/{embedded}: ESP trigger armed"
    );
    let fired = fire(&mut engine, inst, t, 5_000);
    assert!(
        fired.iter().any(
            |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "esp")
        ),
        "{t:?}/{embedded}: event sub-process activated"
    );
    assert_eq!(
        job_count(&mut engine, "w"),
        0,
        "{t:?}/{embedded}: sibling work torn down"
    );
    assert_eq!(
        job_count(&mut engine, "esp"),
        1,
        "{t:?}/{embedded}: ESP runs"
    );
    assert_eq!(
        armed(&engine, inst),
        0,
        "{t:?}/{embedded}: interrupting trigger consumed"
    );
    complete_one(&mut engine, "esp");
    if embedded {
        // The sub-process completed via its event sub-process and continued.
        assert!(!engine.is_completed(inst));
        complete_one(&mut engine, "after");
    }
    assert!(
        engine.is_completed(inst),
        "{t:?}/{embedded}: scope completes with the ESP"
    );
}

fn non_interrupting_case(t: T, embedded: bool) {
    let mut engine = Engine::new();
    let inst = start(&mut engine, esp_model(trig_of(t), false, embedded));
    fire(&mut engine, inst, t, 5_000);
    assert_eq!(
        job_count(&mut engine, "w"),
        1,
        "{t:?}/{embedded}: main work keeps running"
    );
    assert_eq!(
        job_count(&mut engine, "esp"),
        1,
        "{t:?}/{embedded}: ESP runs concurrently"
    );
    // Finish the main work first: the scope must wait for the running ESP.
    complete_one(&mut engine, "w");
    if embedded {
        assert_eq!(
            job_count(&mut engine, "after"),
            0,
            "{t:?}: sub-process waits for ESP"
        );
    }
    assert!(
        !engine.is_completed(inst),
        "{t:?}/{embedded}: scope waits for ESP"
    );
    complete_one(&mut engine, "esp");
    if embedded {
        complete_one(&mut engine, "after");
    }
    assert!(
        engine.is_completed(inst),
        "{t:?}/{embedded}: completes after ESP"
    );
    assert_eq!(
        armed(&engine, inst),
        0,
        "{t:?}/{embedded}: disarmed on completion"
    );
}

#[test]
fn interrupting_event_subprocess_in_process() {
    for t in ALL {
        interrupting_case(t, false);
    }
}

#[test]
fn interrupting_event_subprocess_in_embedded_subprocess() {
    for t in ALL {
        interrupting_case(t, true);
    }
}

#[test]
fn non_interrupting_event_subprocess_in_process() {
    for t in ALL {
        non_interrupting_case(t, false);
    }
}

#[test]
fn non_interrupting_event_subprocess_in_embedded_subprocess() {
    for t in ALL {
        non_interrupting_case(t, true);
    }
}

#[test]
fn completing_the_scope_disarms_event_subprocess_triggers() {
    for t in ALL {
        for embedded in [false, true] {
            let mut engine = Engine::new();
            let inst = start(&mut engine, esp_model(trig_of(t), true, embedded));
            assert_eq!(armed(&engine, inst), 1);
            complete_one(&mut engine, "w");
            // The embedded sub-process completed (instance parked on `after`):
            // its event sub-process trigger is disarmed with it.
            assert_eq!(armed(&engine, inst), 0, "{t:?}/{embedded}: disarmed");
            if embedded {
                complete_one(&mut engine, "after");
            }
            assert!(engine.is_completed(inst));
            // A late trigger does nothing.
            let late = fire(&mut engine, inst, t, 10_000);
            assert!(!late.iter().any(
                |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "esp")
            ));
        }
    }
}

#[test]
fn terminating_the_scope_disarms_event_subprocess_triggers() {
    for t in ALL {
        for embedded in [false, true] {
            let mut engine = Engine::new();
            let inst = start(&mut engine, esp_model(trig_of(t), false, embedded));
            engine
                .apply_command(Command::CancelInstance { instance_key: inst })
                .unwrap();
            assert_eq!(
                armed(&engine, inst),
                0,
                "{t:?}/{embedded}: disarmed on cancel"
            );
        }
    }
}

#[test]
fn non_interrupting_event_subprocess_fires_repeatedly() {
    for (t, trig) in [
        (T::Message, msg()),
        (T::Signal, sig()),
        (T::Timer, timer(TimerDefKind::Cycle, "R/PT5S")),
    ] {
        for embedded in [false, true] {
            let mut engine = Engine::new();
            let inst = start(&mut engine, esp_model(trig.clone(), false, embedded));
            fire(&mut engine, inst, t, 5_000);
            fire(&mut engine, inst, t, 10_000);
            assert_eq!(
                job_count(&mut engine, "esp"),
                2,
                "{t:?}/{embedded}: fired twice"
            );
            assert_eq!(armed(&engine, inst), 1, "{t:?}/{embedded}: stays armed");
            // Both concurrent ESP instances run to completion.
            let jobs = engine.activate_jobs("esp", "test-worker", 10, 60_000, 0);
            assert_eq!(jobs.len(), 2);
            for job in jobs {
                engine
                    .apply_command(Command::complete_job(job.key))
                    .unwrap();
            }
            complete_one(&mut engine, "w");
            if embedded {
                complete_one(&mut engine, "after");
            }
            assert!(engine.is_completed(inst), "{t:?}/{embedded}");
            assert_eq!(armed(&engine, inst), 0, "{t:?}/{embedded}: disarmed");
        }
    }
}

#[test]
fn event_payload_is_visible_in_the_event_subprocess() {
    for t in [T::Message, T::Signal] {
        let mut engine = Engine::new();
        let inst = start(&mut engine, esp_model(trig_of(t), true, false));
        fire(&mut engine, inst, t, 0);
        assert_eq!(
            engine.variables(inst).get("payload"),
            Some(&Value::Int(42)),
            "{t:?}: payload merged into the instance"
        );
    }
}

#[test]
fn interrupting_event_subprocess_disarms_sibling_event_subprocesses() {
    // Two ESPs in the process: an interrupting signal and a non-interrupting
    // message. Firing the signal disarms the message ESP.
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "w")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .event_sub_process("esp", "esp_start", sig(), true)
        .start_event("esp_start")
        .service_task("esp_work", "esp")
        .end_event("esp_end")
        .connect("esp_start", "esp_work")
        .connect("esp_work", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_work", "esp")
        .contained_in("esp_end", "esp")
        .event_sub_process("esp2", "esp2_start", msg(), false)
        .start_event("esp2_start")
        .end_event("esp2_end")
        .connect("esp2_start", "esp2_end")
        .contained_in("esp2_start", "esp2")
        .contained_in("esp2_end", "esp2")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def);
    assert_eq!(armed(&engine, inst), 2);
    fire(&mut engine, inst, T::Signal, 0);
    assert_eq!(armed(&engine, inst), 0, "sibling ESP disarmed");
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst));
}
