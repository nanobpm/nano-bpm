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

// ---- Copilot review round 2 (#1579): defect-class regressions --------------

/// A non-interrupting ESP inside embedded `sub`, plus an interrupting signal
/// boundary `kill` on `sub` routed to `killed`.
fn esp_model_with_boundary(t: T) -> ProcessDefinition {
    let b = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "w")
        .end_event("e")
        .event_sub_process("esp", "esp_start", trig_of(t), false)
        .start_event("esp_start")
        .service_task("esp_work", "esp")
        .end_event("esp_end")
        .connect("esp_start", "esp_work")
        .connect("esp_work", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_work", "esp")
        .contained_in("esp_end", "esp")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .end_event("sub_e")
        .connect("s", "sub")
        .connect("sub_s", "work")
        .connect("work", "sub_e")
        .connect("sub", "e")
        .contained_in("sub_s", "sub")
        .contained_in("work", "sub")
        .contained_in("sub_e", "sub")
        .contained_in("esp", "sub")
        .signal_boundary_event("kill", "sub", "kill")
        .end_event("killed")
        .connect("kill", "killed");
    b.build().unwrap()
}

#[test]
fn boundary_interruption_of_the_host_disarms_event_subprocess_triggers() {
    // Every trigger family: a boundary event interrupting the embedded host
    // sub-process must disarm the host's ESP trigger too, so it can never
    // fire into a torn-down scope.
    for t in ALL {
        let mut engine = Engine::new();
        let inst = start(&mut engine, esp_model_with_boundary(t));
        assert_eq!(
            armed(&engine, inst),
            2,
            "{t:?}: ESP trigger + boundary armed"
        );
        engine
            .apply_command(Command::BroadcastSignal {
                signal_name: "kill".into(),
                variables: HashMap::new(),
            })
            .unwrap();
        assert_eq!(armed(&engine, inst), 0, "{t:?}: ESP trigger disarmed");
        assert!(
            engine.is_completed(inst),
            "{t:?}: instance completed via boundary"
        );
        let late = fire(&mut engine, inst, t, 5_000);
        assert!(
            !late.iter().any(
                |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "esp")
            ),
            "{t:?}: no ESP activation after the host was interrupted"
        );
    }
}

#[test]
fn interrupting_event_subprocess_purges_queued_sibling_activations() {
    // A non-interrupting and an interrupting timer ESP due in the same batch:
    // the interrupting one must leave nothing of the other running.
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "w")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .event_sub_process(
            "esp_a",
            "a_start",
            timer(TimerDefKind::Duration, "PT5S"),
            false,
        )
        .start_event("a_start")
        .service_task("a_work", "ni")
        .end_event("a_end")
        .connect("a_start", "a_work")
        .connect("a_work", "a_end")
        .contained_in("a_start", "esp_a")
        .contained_in("a_work", "esp_a")
        .contained_in("a_end", "esp_a")
        .event_sub_process(
            "esp_b",
            "b_start",
            timer(TimerDefKind::Duration, "PT5S"),
            true,
        )
        .start_event("b_start")
        .service_task("b_work", "int")
        .end_event("b_end")
        .connect("b_start", "b_work")
        .connect("b_work", "b_end")
        .contained_in("b_start", "esp_b")
        .contained_in("b_work", "esp_b")
        .contained_in("b_end", "esp_b")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def);
    engine.trigger_timers(5_000);
    assert_eq!(job_count(&mut engine, "w"), 0, "main flow torn down");
    assert_eq!(job_count(&mut engine, "int"), 1, "interrupting ESP runs");
    assert_eq!(
        job_count(&mut engine, "ni"),
        0,
        "no non-interrupting work survives the interrupting ESP"
    );
    complete_one(&mut engine, "int");
    assert!(engine.is_completed(inst));
}

#[test]
fn terminating_a_running_event_subprocess_tears_down_its_children() {
    // `owns_token_scope` covers EventSubProcess: modification teardown of a
    // running ESP cancels its inner job instead of leaking it.
    let mut engine = Engine::new();
    let inst = start(&mut engine, esp_model(sig(), false, false));
    let fired = fire(&mut engine, inst, T::Signal, 0);
    let esp_eik = fired
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_id,
                element_instance_key,
                ..
            } if element_id == "esp" => Some(*element_instance_key),
            _ => None,
        })
        .unwrap();
    assert_eq!(job_count(&mut engine, "esp"), 1);
    engine
        .apply_command(Command::modify_instance(inst, vec![], vec![esp_eik]))
        .unwrap();
    assert_eq!(job_count(&mut engine, "esp"), 0, "ESP child job cancelled");
    assert_eq!(job_count(&mut engine, "w"), 1, "main flow untouched");
}

#[test]
fn redelivered_message_continuation_starts_one_non_interrupting_run() {
    // A persistent (non-interrupting) ESP subscription parked off-partition
    // (`Opening`) never settles, so it dedupes routed continuations by message
    // key: a redelivered correlation must not start a second run, while a
    // genuinely new message does.
    const N: u64 = 2;
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| crate::state::subscription_partition(k, N) == 1)
        .unwrap();
    let mut engine = Engine::with_partition(0);
    engine.set_num_partitions(N);
    engine
        .apply_command(Command::DeployProcess(esp_model(msg(), false, false)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("orderId", Value::Str(order))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    let (sub, eik, element_id, kind) = created
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionOpening {
                subscription_key,
                element_instance_key,
                element_id,
                kind,
                ..
            } => Some((
                *subscription_key,
                *element_instance_key,
                element_id.clone(),
                kind.clone(),
            )),
            _ => None,
        })
        .expect("off-partition Opening record");
    let deliver = |engine: &mut Engine, message_key| {
        engine
            .apply_command(Command::CorrelateMessageSubscription {
                subscription_key: sub,
                message_key,
                instance_key: inst,
                element_instance_key: eik,
                element_id: element_id.clone(),
                kind: kind.clone(),
                variables: HashMap::new(),
            })
            .unwrap()
    };
    deliver(&mut engine, 77);
    assert_eq!(
        job_count(&mut engine, "esp"),
        1,
        "first delivery runs the ESP"
    );
    deliver(&mut engine, 77);
    assert_eq!(job_count(&mut engine, "esp"), 1, "redelivery ignored");
    deliver(&mut engine, 78);
    assert_eq!(job_count(&mut engine, "esp"), 2, "a new message runs again");
}

#[test]
fn event_subprocess_triggers_resolve_against_post_input_mapping_variables() {
    // The embedded host's input mapping rebinds `orderId`; the ESP message
    // correlation key must see the mapped value, not the outer one.
    let mut def = esp_model(msg(), false, true);
    def.elements.get_mut("sub").unwrap().io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=99".into(),
            target: "orderId".into(),
        }],
        outputs: vec![],
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, def);
    let keys: Vec<String> = engine
        .state
        .message_subscriptions
        .values()
        .filter(|s| s.instance_key == inst)
        .map(|s| s.correlation_key.clone())
        .collect();
    assert_eq!(keys, vec!["99".to_string()]);
}

#[test]
fn conditional_event_subprocess_true_on_entry_fires_before_scope_drains() {
    // `s -> e` ends immediately; a conditional ESP whose condition already
    // holds must be evaluated before the scope's drain sweep disarms it.
    for (interrupting, embedded) in [(true, false), (false, false), (true, true), (false, true)] {
        let mut b = esp_builder(cond(), interrupting);
        if embedded {
            // Same shape one level down: `s -> sub[sub_s -> sub_e] -> e` with
            // the ESP inside `sub`.
            b = crate::model::ProcessBuilder::new("p")
                .start_event("s")
                .end_event("e")
                .sub_process("sub", "sub_s")
                .start_event("sub_s")
                .end_event("sub_e")
                .connect("s", "sub")
                .connect("sub_s", "sub_e")
                .connect("sub", "e")
                .contained_in("sub_s", "sub")
                .contained_in("sub_e", "sub")
                .event_sub_process("esp", "esp_start", cond(), interrupting)
                .start_event("esp_start")
                .end_event("esp_end")
                .connect("esp_start", "esp_end")
                .contained_in("esp_start", "esp")
                .contained_in("esp_end", "esp")
                .contained_in("esp", "sub");
        }
        let def = b.build().unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance_with(
                "p",
                vars(&[("flag", Value::Bool(true))]),
            ))
            .unwrap();
        assert!(
            created.iter().any(
                |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "esp")
            ),
            "interrupting={interrupting} embedded={embedded}: conditional ESP fired on entry"
        );
    }
}

#[test]
fn migrating_an_instance_with_armed_event_subprocess_triggers_is_rejected() {
    // Scope-owned ESP triggers are not reconciled by migration yet (#1379):
    // reject explicitly rather than keep a stale trigger armed.
    for t in ALL {
        let mut engine = Engine::new();
        let inst = start(&mut engine, esp_model(trig_of(t), false, false));
        // A changed target (interrupting flag flipped) so the redeploy is a new
        // version.
        let target_key = deploy_for_migration(&mut engine, esp_model(trig_of(t), true, false));
        let err = engine
            .apply_command(Command::migrate_instance(
                inst,
                target_key,
                vec![("work".into(), "work".into())],
            ))
            .unwrap_err();
        match err {
            EngineError::UnsupportedMigration {
                element_id, reason, ..
            } => {
                assert_eq!(element_id, "esp", "{t:?}");
                assert!(reason.contains("#1379"), "{t:?}: {reason}");
            }
            other => panic!("{t:?}: expected UnsupportedMigration, got {other:?}"),
        }
        assert_eq!(armed(&engine, inst), 1, "{t:?}: nothing changed");
    }
}
#[test]
fn esp_output_satisfying_a_sibling_condition_fires_before_the_host_drains() {
    // Review r4557686184: a non-interrupting ESP whose completion output
    // satisfies a SIBLING conditional trigger must not let the drain sweep
    // complete the (now childless) host before `reevaluate_conditionals` runs
    // — the recursive re-sweep did exactly that, disarming the sibling before
    // it could fire.
    //
    // `s -> sub[ sub_s -> hold(holdwork) -> sub_e ] -> e` with two ESPs inside
    // `sub`: `esp_msg` (non-interrupting message) whose output mapping sets
    // `flag := true`, and `esp_cond` (conditional `=flag`) whose body parks on
    // the `condwork` job. The host's own flow parks on the `holdwork` job, so
    // `sub` is alive when the message fires. Completing the ESP's job drains
    // `esp_msg` and writes `flag = true`; the conditional must fire before
    // the drained-ESP sweep can complete the host and disarm it. The
    // `condwork` job makes the fire observable: were the trigger disarmed
    // first, no `condwork` job would ever exist.
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .end_event("e")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .service_task("hold", "holdwork")
        .end_event("sub_e")
        .connect("s", "sub")
        .connect("sub_s", "hold")
        .connect("hold", "sub_e")
        .connect("sub", "e")
        .contained_in("sub_s", "sub")
        .contained_in("hold", "sub")
        .contained_in("sub_e", "sub")
        .event_sub_process("esp_msg", "esp_msg_start", msg(), false)
        .start_event("esp_msg_start")
        .service_task("esp_msg_work", "espwork")
        .end_event("esp_msg_end")
        .connect("esp_msg_start", "esp_msg_work")
        .connect("esp_msg_work", "esp_msg_end")
        .with_io(
            "esp_msg",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=true".to_string(),
                    target: "flag".to_string(),
                }],
            },
        )
        .contained_in("esp_msg_start", "esp_msg")
        .contained_in("esp_msg_work", "esp_msg")
        .contained_in("esp_msg_end", "esp_msg")
        .contained_in("esp_msg", "sub")
        .event_sub_process("esp_cond", "esp_cond_start", cond(), false)
        .start_event("esp_cond_start")
        .service_task("esp_cond_work", "condwork")
        .end_event("esp_cond_end")
        .connect("esp_cond_start", "esp_cond_work")
        .connect("esp_cond_work", "esp_cond_end")
        .contained_in("esp_cond_start", "esp_cond")
        .contained_in("esp_cond_work", "esp_cond")
        .contained_in("esp_cond_end", "esp_cond")
        .contained_in("esp_cond", "sub")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def);
    // Fire the message ESP; its body parks on the `espwork` job while the
    // host's own token rests on `holdwork`.
    engine
        .apply_command(Command::CorrelateMessage {
            message_name: "m".into(),
            correlation_key: "7".into(),
            variables: HashMap::new(),
            business_id: None,
        })
        .unwrap();
    // Complete the HOST's own job first: the host then has no flow-flow child
    // left and stays alive only because `esp_msg` is still running. Draining
    // the ESP next leaves the host genuinely childless — the exact shape whose
    // recursive re-sweep completed the host and disarmed the sibling
    // conditional before `reevaluate_conditionals` could fire it.
    complete_one(&mut engine, "holdwork");
    // Completing the ESP's job drains `esp_msg`: its output mapping sets
    // `flag = true` on `sub`, which satisfies the sibling conditional.
    complete_one(&mut engine, "espwork");
    assert_eq!(
        job_count(&mut engine, "condwork"),
        1,
        "the sibling conditional ESP fired on the drained ESP's output, before \
         the host's drain sweep could disarm it"
    );
    // Drain the rest: the conditional ESP's job completes the host.
    complete_one(&mut engine, "condwork");
    assert!(
        engine.is_completed(inst),
        "both ESPs and the host drain; the instance completes"
    );
}

#[test]
fn modify_created_flow_scope_arms_its_event_subprocesses() {
    // Review r4557686184's sibling (the "previously missed" advisory): a flow
    // scope created implicitly by `ModifyInstance` (Zeebe
    // `shouldCreateEventSubscriptionsWhenActivatingElementsInsideNonExistingFlowScopes`)
    // must arm the event sub-processes it contains — it bypasses the
    // activation body where arming otherwise lives.
    //
    // `s -> w(work) -> e`, with `sub[ sub_s -> inner(innerwork) -> sub_e ]`
    // disconnected (reachable only by modification) and a non-interrupting
    // message ESP inside `sub`.
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("w", "work")
        .end_event("e")
        .connect("s", "w")
        .connect("w", "e")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .service_task("inner", "innerwork")
        .end_event("sub_e")
        .connect("sub_s", "inner")
        .connect("inner", "sub_e")
        .contained_in("sub_s", "sub")
        .contained_in("inner", "sub")
        .contained_in("sub_e", "sub")
        .event_sub_process("esp", "esp_start", msg(), false)
        .start_event("esp_start")
        .service_task("esp_work", "esp")
        .end_event("esp_end")
        .connect("esp_start", "esp_work")
        .connect("esp_work", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_work", "esp")
        .contained_in("esp_end", "esp")
        .contained_in("esp", "sub")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def);
    assert_eq!(armed(&engine, inst), 0, "no ESP scope is live yet");

    // Activate `inner`, implicitly creating the `sub` flow scope.
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![crate::ActivateElementInstruction::new("inner")],
            terminate_instructions: vec![],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        1,
        "the modify-created `sub` scope arms its event sub-process"
    );

    // The armed trigger actually fires: correlating the message starts a
    // non-interrupting run of the ESP inside the created scope.
    engine
        .apply_command(Command::CorrelateMessage {
            message_name: "m".into(),
            correlation_key: "7".into(),
            variables: HashMap::new(),
            business_id: None,
        })
        .unwrap();
    assert_eq!(
        job_count(&mut engine, "esp"),
        1,
        "the ESP triggered inside the modify-created scope"
    );

    // Completing the scope's work drains `sub`, disarming the (still-armed,
    // non-interrupting) trigger.
    complete_one(&mut engine, "esp");
    complete_one(&mut engine, "innerwork");
    assert_eq!(
        armed(&engine, inst),
        0,
        "draining the modify-created scope disarms its event sub-process"
    );
    complete_one(&mut engine, "work");
    assert!(engine.is_completed(inst));
}
