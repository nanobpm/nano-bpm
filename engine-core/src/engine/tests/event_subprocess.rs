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
    let mut vars = BTreeMap::new();
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
            .apply_command(Command::create_instance_with("good", BTreeMap::new()))
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
    let mut vars = BTreeMap::new();
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
        .apply_command(Command::create_instance_with("p", BTreeMap::new()))
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
                message_key: None,
                time_to_live: None,
                message_id: None,
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
                .apply_command(Command::cancel_instance(inst))
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
                variables: BTreeMap::new(),
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
                variables: BTreeMap::new(),
                // `0` = no routed-delivery identity (#1497), so the dedup under
                // test is the ESP subscription's own message-key dedup.
                delivery_key: 0,
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

/// `esp_model` without the event sub-process (`s -> work -> e`, or nested in
/// `sub` when `embedded`).
fn no_esp_model(embedded: bool) -> ProcessDefinition {
    let b = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "w")
        .end_event("e");
    let b = if embedded {
        b.sub_process("sub", "sub_s")
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
    } else {
        b.connect("s", "work").connect("work", "e")
    };
    b.build().unwrap()
}

fn migrate(engine: &mut Engine, inst: Key, target_key: Key, embedded: bool) -> Vec<Event> {
    let mut mappings = vec![("work".to_string(), "work".to_string())];
    if embedded {
        mappings.push(("sub".into(), "sub".into()));
    }
    engine
        .apply_command(Command::migrate_instance(inst, target_key, mappings))
        .unwrap()
}

/// The keys of every still-armed trigger of `inst` (sorted).
fn armed_keys(engine: &Engine, inst: Key) -> Vec<Key> {
    use crate::state::{MessageSubscriptionState as S, TimerState};
    let s = &engine.state;
    let mut keys: Vec<Key> = s
        .timers
        .values()
        .filter(|t| t.instance_key == inst && t.state == TimerState::Created)
        .map(|t| t.key)
        .chain(
            s.message_subscriptions
                .values()
                .filter(|x| x.instance_key == inst && matches!(x.state, S::Open | S::Opening))
                .map(|x| x.key),
        )
        .chain(
            s.signal_subscriptions
                .values()
                .filter(|x| x.instance_key == inst && x.state == S::Open)
                .map(|x| x.key),
        )
        .chain(
            s.conditional_subscriptions
                .values()
                .filter(|x| x.instance_key == inst && x.state == S::Open)
                .map(|x| x.key),
        )
        .collect();
    keys.sort_unstable();
    keys
}

fn esp_activated(events: &[Event]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "esp"))
}

#[test]
fn migration_recreates_a_changed_event_subprocess_trigger_on_its_scope() {
    // #1379: scope-owned ESP triggers are reconciled, not rejected. Flipping
    // the interrupting flag closes the source trigger and arms the target's;
    // firing it afterwards runs the TARGET semantics (interrupting: the
    // sibling `work` is torn down).
    for embedded in [false, true] {
        for t in ALL {
            let mut engine = Engine::new();
            let inst = start(&mut engine, esp_model(trig_of(t), false, embedded));
            let before = armed_keys(&engine, inst);
            assert_eq!(before.len(), 1, "{t:?}/{embedded}");
            let target_key =
                deploy_for_migration(&mut engine, esp_model(trig_of(t), true, embedded));
            let events = migrate(&mut engine, inst, target_key, embedded);
            let after = armed_keys(&engine, inst);
            assert_eq!(after.len(), 1, "{t:?}/{embedded}: exactly one trigger");
            assert_ne!(after, before, "{t:?}/{embedded}: re-created, not kept");
            let audited = events.iter().any(|e| match e {
                Event::ProcessInstanceMigrated { subscriptions, .. } => {
                    matches!(t, T::Conditional)
                        || subscriptions.iter().any(|s| {
                            s.element_id == "esp_start"
                                && s.action == crate::event::SubscriptionMigrationAction::Recreated
                        })
                }
                _ => false,
            });
            assert!(audited, "{t:?}/{embedded}: recreation audited");
            let fired = fire(&mut engine, inst, t, 5_000);
            assert!(esp_activated(&fired), "{t:?}/{embedded}: target ESP fires");
            assert_eq!(
                job_count(&mut engine, "w"),
                0,
                "{t:?}/{embedded}: interrupting"
            );
            assert_eq!(job_count(&mut engine, "esp"), 1, "{t:?}/{embedded}");
        }
    }
}

#[test]
fn migration_retains_an_unchanged_event_subprocess_trigger() {
    for embedded in [false, true] {
        for t in ALL {
            let mut engine = Engine::new();
            let inst = start(&mut engine, esp_model(trig_of(t), false, embedded));
            let before = armed_keys(&engine, inst);
            // A distinct version (an extra element) with an identical ESP.
            let mut target = esp_model(trig_of(t), false, embedded);
            target.name = Some("v2".into());
            let target_key = deploy_for_migration(&mut engine, target);
            migrate(&mut engine, inst, target_key, embedded);
            assert_eq!(
                armed_keys(&engine, inst),
                before,
                "{t:?}/{embedded}: retained"
            );
            let fired = fire(&mut engine, inst, t, 5_000);
            assert!(esp_activated(&fired), "{t:?}/{embedded}: still fires");
            assert_eq!(
                job_count(&mut engine, "w"),
                1,
                "{t:?}/{embedded}: non-interrupting"
            );
        }
    }
}

#[test]
fn migration_disarms_the_trigger_of_a_removed_event_subprocess() {
    for embedded in [false, true] {
        for t in ALL {
            let mut engine = Engine::new();
            let inst = start(&mut engine, esp_model(trig_of(t), false, embedded));
            let target_key = deploy_for_migration(&mut engine, no_esp_model(embedded));
            migrate(&mut engine, inst, target_key, embedded);
            assert_eq!(
                armed(&engine, inst),
                0,
                "{t:?}/{embedded}: no stale trigger"
            );
            let fired = fire(&mut engine, inst, t, 5_000);
            assert!(
                !esp_activated(&fired),
                "{t:?}/{embedded}: removed ESP never fires"
            );
        }
    }
}

#[test]
fn migration_arms_a_newly_added_event_subprocess() {
    for embedded in [false, true] {
        for t in ALL {
            let mut engine = Engine::new();
            let inst = start(&mut engine, no_esp_model(embedded));
            assert_eq!(armed(&engine, inst), 0);
            let target_key =
                deploy_for_migration(&mut engine, esp_model(trig_of(t), false, embedded));
            migrate(&mut engine, inst, target_key, embedded);
            assert_eq!(armed(&engine, inst), 1, "{t:?}/{embedded}: new ESP armed");
            let fired = fire(&mut engine, inst, t, 5_000);
            assert!(esp_activated(&fired), "{t:?}/{embedded}: new ESP fires");
        }
    }
}

#[test]
fn migration_does_not_rearm_a_fired_one_shot_event_subprocess_timer() {
    // A non-interrupting one-shot timer ESP that already fired has no armed
    // trigger; migrating onto the same ESP must not arm it again (its handler
    // would run twice).
    let mut engine = Engine::new();
    let inst = start(&mut engine, esp_model(trig_of(T::Timer), false, false));
    let fired = fire(&mut engine, inst, T::Timer, 5_000);
    assert!(esp_activated(&fired));
    assert_eq!(armed(&engine, inst), 0);
    let mut target = esp_model(trig_of(T::Timer), false, false);
    target.name = Some("v2".into());
    let target_key = deploy_for_migration(&mut engine, target);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("work".into(), "work".into()),
                ("esp".into(), "esp".into()),
                ("esp_work".into(), "esp_work".into()),
            ],
        ))
        .unwrap();
    assert_eq!(armed(&engine, inst), 0, "fired one-shot timer stays fired");
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
            variables: BTreeMap::new(),
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
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
            variables: BTreeMap::new(),
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
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

#[test]
fn modify_created_flow_scope_arms_its_event_subprocesses_against_host_locals() {
    // Review r4558198708: a modify-created host's event sub-processes live
    // INSIDE the host, so their trigger expressions must see the
    // instruction-supplied locals of that host — a local `orderId = 99`
    // shadowing the root's `7` subscribes to `99`, and a local-only key is
    // accepted (not rejected during planning).
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
    let mut activate = crate::ActivateElementInstruction::new("inner");
    activate
        .variable_instructions
        .push(crate::event::VariableInstruction {
            variables: vars(&[("orderId", Value::Int(99))]),
            scope_id: Some("sub".into()),
        });
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![activate],
            terminate_instructions: vec![],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(armed(&engine, inst), 1);
    let _ = engine.apply_command(Command::CorrelateMessage {
        message_name: "m".into(),
        correlation_key: "7".into(),
        variables: BTreeMap::new(),
        business_id: None,
        message_key: None,
        time_to_live: None,
        message_id: None,
    });
    assert_eq!(
        job_count(&mut engine, "esp"),
        0,
        "the root's shadowed `orderId = 7` is not the host's correlation key"
    );
    engine
        .apply_command(Command::CorrelateMessage {
            message_name: "m".into(),
            correlation_key: "99".into(),
            variables: BTreeMap::new(),
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
        })
        .unwrap();
    assert_eq!(
        job_count(&mut engine, "esp"),
        1,
        "the ESP subscribed with the host-local `orderId = 99`"
    );
}

#[test]
fn modify_created_flow_scope_accepts_a_host_local_only_esp_correlation_key() {
    // The local-only variant: the key exists ONLY as a host local, so a
    // parent-view check would reject the modification.
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
        .event_sub_process(
            "esp",
            "esp_start",
            Trig::Message {
                message_name: "m".into(),
                correlation_key: "localKey".into(),
            },
            false,
        )
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
    let mut activate = crate::ActivateElementInstruction::new("inner");
    activate
        .variable_instructions
        .push(crate::event::VariableInstruction {
            variables: vars(&[("localKey", Value::Int(5))]),
            scope_id: Some("sub".into()),
        });
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![activate],
            terminate_instructions: vec![],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .expect("a host-local correlation key is visible to the host's ESPs");
    assert_eq!(armed(&engine, inst), 1);
}

#[test]
fn modify_created_flow_scope_evaluates_a_conditional_event_subprocess_on_open() {
    // A conditional ESP armed by a ModifyInstance-created scope must get its
    // initial evaluation: the `ConditionalSubscriptionCreated` event the arming
    // emits is picked up by the same drain's `reevaluate_conditionals`, so a
    // condition that already holds fires in the same command.
    //
    // Same shape as `modify_created_flow_scope_arms_its_event_subprocesses`,
    // but the ESP is conditional (`=flag`, already `true` at the root).
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
        .event_sub_process("esp", "esp_start", cond(), false)
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
    // `flag` is already true (the `start` helper seeds it false; set it).
    engine
        .apply_command(Command::SetVariables {
            scope_key: inst,
            variables: vars(&[("flag", Value::Bool(true))]),
            local: false,
        })
        .unwrap();

    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![crate::ActivateElementInstruction::new("inner")],
            terminate_instructions: vec![],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap();
    assert!(
        events.iter().any(
            |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "esp")
        ),
        "the conditional ESP fires on the modify-created scope in the same \
         command: {events:?}"
    );
}

#[test]
fn start_output_mappings_keep_the_event_payload_local_to_the_start_event() {
    // Zeebe parity (trace `esp-message-output-mapping`): when the event
    // sub-process start event carries output mappings, the message / signal
    // payload is created LOCAL to the start event (not merged into the host
    // scope); only the mapped targets propagate outward.
    for t in [T::Message, T::Signal] {
        let mut def = esp_model(trig_of(t), false, false);
        def.elements.get_mut("esp_start").unwrap().io = crate::model::IoMapping {
            inputs: vec![],
            outputs: vec![crate::model::Mapping {
                source: "=payload".into(),
                target: "mapped".into(),
            }],
        };
        let mut engine = Engine::new();
        let inst = start(&mut engine, def);
        let events = fire(&mut engine, inst, t, 0);
        let vars = engine.variables(inst);
        assert_eq!(
            vars.get("mapped"),
            Some(&Value::Int(42)),
            "{t:?}: the output mapping propagated its target: {events:?}"
        );
        assert_eq!(
            vars.get("payload"),
            None,
            "{t:?}: the raw payload stays local to the start event: {events:?}"
        );
    }
}

/// A message / signal event sub-process whose start event maps `=payload`
/// to `mapped`, with optional extra XML inside the event sub-process
/// container (`esp_ext`) and on its start event (`start_ext`), hosted in
/// `host` (`"process"` or a parallel multi-instance sub-process over `[1,2]`).
fn esp_payload_xml(signal: bool, mi_host: bool, esp_ext: &str, start_ext: &str) -> String {
    let def = if signal {
        r#"<bpmn:signalEventDefinition signalRef="Sig" />"#
    } else {
        r#"<bpmn:messageEventDefinition messageRef="Msg" />"#
    };
    let esp = format!(
        r#"<bpmn:subProcess id="esp" triggeredByEvent="true">
             <bpmn:extensionElements>{esp_ext}</bpmn:extensionElements>
             <bpmn:startEvent id="esp_start" isInterrupting="false">
               <bpmn:extensionElements>
                 <zeebe:ioMapping><zeebe:output source="=payload" target="mapped" /></zeebe:ioMapping>
                 {start_ext}
               </bpmn:extensionElements>
               {def}
             </bpmn:startEvent>
             <bpmn:endEvent id="esp_end" />
             <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_end" />
           </bpmn:subProcess>"#
    );
    let work = r#"<bpmn:serviceTask id="work">
             <bpmn:extensionElements><zeebe:taskDefinition type="w" /></bpmn:extensionElements>
           </bpmn:serviceTask>"#;
    let body = if mi_host {
        format!(
            r#"<bpmn:subProcess id="host">
             <bpmn:multiInstanceLoopCharacteristics>
               <bpmn:extensionElements>
                 <zeebe:loopCharacteristics inputCollection="=[1,2]" inputElement="item" />
               </bpmn:extensionElements>
             </bpmn:multiInstanceLoopCharacteristics>
             <bpmn:startEvent id="hs" />
             {work}
             <bpmn:endEvent id="he" />
             <bpmn:sequenceFlow id="h1" sourceRef="hs" targetRef="work" />
             <bpmn:sequenceFlow id="h2" sourceRef="work" targetRef="he" />
             {esp}
           </bpmn:subProcess>
           <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="host" />
           <bpmn:sequenceFlow id="f2" sourceRef="host" targetRef="e" />"#
        )
    } else {
        format!(
            r#"{work}
           <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="work" />
           <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
           {esp}"#
        )
    };
    format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
         <bpmn:message id="Msg" name="m">
           <bpmn:extensionElements><zeebe:subscription correlationKey="=orderId" /></bpmn:extensionElements>
         </bpmn:message>
         <bpmn:signal id="Sig" name="sig" />
         <bpmn:process id="p" isExecutable="true">
           <bpmn:startEvent id="s" />
           <bpmn:endEvent id="e" />
           {body}
         </bpmn:process>
       </bpmn:definitions>"#
    )
}

fn start_xml(engine: &mut Engine, xml: &str) -> (Key, Vec<Event>) {
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut log = engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("orderId", Value::Int(7))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    log.extend(created);
    (inst, log)
}

/// Every value an event in `events` wrote to variable `name`.
fn writes_of(events: &[Event], name: &str) -> Vec<Value> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::ScopedVariablesUpdated { variables, .. }
            | Event::VariablesUpdated { variables, .. } => variables.get(name).cloned(),
            _ => None,
        })
        .collect()
}

#[test]
fn one_signal_broadcast_seeds_every_concurrent_host_scopes_start_payload() {
    // Review r4562389430: a broadcast reaching the same event sub-process in
    // two concurrent multi-instance host scopes must hand EACH start event
    // its payload — the hold is keyed by the concrete host scope, not just
    // the start event id.
    let mut engine = Engine::new();
    let (_inst, _) = start_xml(&mut engine, &esp_payload_xml(true, true, "", ""));
    let events = engine
        .apply_command(Command::BroadcastSignal {
            signal_name: "sig".into(),
            variables: vars(&[("payload", Value::Int(42))]),
        })
        .unwrap();
    // Each host scope's start event receives the payload locally. (Both then
    // map it to the same root `mapped`; the second, equal-value write is
    // skipped as in Zeebe's `VariableBehavior` (#1537), so `mapped` is written
    // once.)
    assert_eq!(
        writes_of(&events, "payload"),
        vec![Value::Int(42), Value::Int(42)],
        "both host scopes' start events received the payload: {events:?}"
    );
    assert_eq!(
        writes_of(&events, "mapped"),
        vec![Value::Int(42)],
        "the shared root `mapped` is written once, the equal rewrite skipped: {events:?}"
    );
}

#[test]
fn container_start_listener_defers_the_start_without_losing_its_payload() {
    // Review r4562389487: a `start` execution listener on the event
    // sub-process container defers its start event to a later command (the
    // listener job's completion). The held payload must survive that — and a
    // replay of the journal in between.
    for replay in [false, true] {
        let mut engine = Engine::new();
        let listener = r#"<zeebe:executionListeners>
                 <zeebe:executionListener eventType="start" type="esp-gate" />
               </zeebe:executionListeners>"#;
        let (inst, mut log) = start_xml(&mut engine, &esp_payload_xml(false, false, listener, ""));
        log.extend(
            engine
                .apply_command(Command::CorrelateMessage {
                    message_name: "m".into(),
                    correlation_key: "7".into(),
                    variables: vars(&[("payload", Value::Int(42))]),
                    business_id: None,
                    message_key: None,
                    time_to_live: None,
                    message_id: None,
                })
                .unwrap(),
        );
        assert!(
            writes_of(&log, "mapped").is_empty(),
            "the start event waits on the container listener"
        );
        if replay {
            engine = Engine::replay(log);
        }
        let events = complete_one(&mut engine, "esp-gate");
        assert_eq!(
            writes_of(&events, "mapped"),
            vec![Value::Int(42)],
            "replay={replay}: the deferred start mapped the held payload: {events:?}"
        );
        assert_eq!(engine.variables(inst).get("payload"), None);
    }
}

#[test]
fn start_event_activation_expressions_see_the_local_payload() {
    // Review r4562389516: the payload is local to the start event, so its
    // own activation-time expressions (here a `start` listener's job type)
    // resolve against it.
    let mut engine = Engine::new();
    let listener = r#"<zeebe:executionListeners>
                 <zeebe:executionListener eventType="start" type="=listenerType" />
               </zeebe:executionListeners>"#;
    let (inst, _) = start_xml(&mut engine, &esp_payload_xml(false, false, "", listener));
    let events = engine
        .apply_command(Command::CorrelateMessage {
            message_name: "m".into(),
            correlation_key: "7".into(),
            variables: vars(&[
                ("payload", Value::Int(42)),
                ("listenerType", Value::Str("dyn".into())),
            ]),
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
        })
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { .. })),
        "no extraction incident: {events:?}"
    );
    assert_eq!(job_count(&mut engine, "dyn"), 1, "{events:?}");
    let events = complete_one(&mut engine, "dyn");
    assert_eq!(writes_of(&events, "mapped"), vec![Value::Int(42)]);
    assert_eq!(engine.variables(inst).get("listenerType"), None);
}

#[test]
fn held_esp_payloads_are_dropped_when_the_container_finishes_or_the_instance_ends() {
    // Review r4562643140: the held start payload must not outlive its event
    // sub-process container — neither across repeated non-interrupting runs
    // (normal completion) nor on instance termination.
    for completed_via_listener in [false, true] {
        let mut engine = Engine::new();
        let listener = r#"<zeebe:executionListeners>
                 <zeebe:executionListener eventType="start" type="esp-gate" />
               </zeebe:executionListeners>"#;
        let ext = if completed_via_listener { listener } else { "" };
        let (inst, _) = start_xml(&mut engine, &esp_payload_xml(false, false, ext, ""));
        for n in 0..3 {
            engine
                .apply_command(Command::CorrelateMessage {
                    message_name: "m".into(),
                    correlation_key: "7".into(),
                    variables: vars(&[("payload", Value::Int(n))]),
                    business_id: None,
                    message_key: None,
                    time_to_live: None,
                    message_id: None,
                })
                .unwrap();
        }
        let held = engine.state.instances[&inst].esp_held_payloads.len();
        if completed_via_listener {
            // Containers still parked on their listener legitimately hold.
            assert!(held > 0, "listener-deferred starts hold their payload");
            engine
                .apply_command(Command::cancel_instance(inst))
                .unwrap();
            assert!(
                engine.state.instances[&inst].esp_held_payloads.is_empty(),
                "termination clears held payloads"
            );
        } else {
            assert_eq!(held, 0, "completed ESP runs leave no held payload");
        }
    }
}

#[test]
fn migration_arms_a_subscription_trigger_replacing_an_error_or_escalation_one() {
    // #1379 review r4578935957: error / escalation event sub-processes have
    // no armed subscription, so they must not suppress arming a same-id target
    // event sub-process that now carries a subscription trigger.
    let sources = [
        Trig::Error {
            error_code: Some("E".into()),
        },
        Trig::Escalation {
            escalation_code: Some("X".into()),
        },
    ];
    for embedded in [false, true] {
        for src in &sources {
            for t in ALL {
                let mut engine = Engine::new();
                let inst = start(&mut engine, esp_model(src.clone(), true, embedded));
                assert_eq!(armed(&engine, inst), 0);
                let target_key =
                    deploy_for_migration(&mut engine, esp_model(trig_of(t), false, embedded));
                migrate(&mut engine, inst, target_key, embedded);
                assert_eq!(
                    armed(&engine, inst),
                    1,
                    "{src:?}->{t:?}/{embedded}: target trigger armed"
                );
                let fired = fire(&mut engine, inst, t, 5_000);
                assert!(esp_activated(&fired), "{src:?}->{t:?}/{embedded}: fires");
            }
        }
    }
}

/// `s -> work("w") -> e` plus two non-interrupting signal event sub-processes
/// `a` and `b` (or only `a` when `!with_b`).
fn two_esp_model(with_b: bool) -> ProcessDefinition {
    let mut b = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "w")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e");
    let ids: &[&str] = if with_b { &["a", "b"] } else { &["a"] };
    for id in ids {
        let (st, wk, en) = (format!("{id}_s"), format!("{id}_w"), format!("{id}_e"));
        b = b
            .event_sub_process(*id, st.clone(), sig(), false)
            .start_event(st.clone())
            .service_task(wk.clone(), *id)
            .end_event(en.clone())
            .connect(st.clone(), wk.clone())
            .connect(wk.clone(), en.clone())
            .contained_in(st, *id)
            .contained_in(wk, *id)
            .contained_in(en, *id);
    }
    b.build().unwrap()
}

#[test]
fn migration_rejects_merging_event_subprocesses_onto_one_target() {
    // #1379 review r4578935984: mapping `b -> a` while `a` is implicitly
    // retained would leave two live triggers for target `a`.
    let mut engine = Engine::new();
    let inst = start(&mut engine, two_esp_model(true));
    assert_eq!(armed(&engine, inst), 2);
    let target_key = deploy_for_migration(&mut engine, two_esp_model(false));
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("work".into(), "work".into()), ("b".into(), "a".into())],
        ))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::MigrationCatchEventsMerged { target_catch_event_id, source_catch_event_ids, .. }
            if target_catch_event_id == "a" && source_catch_event_ids == &vec!["a".to_string(), "b".to_string()]),
        "{err:?}"
    );
    assert_eq!(
        armed(&engine, inst),
        2,
        "rejected migration changes nothing"
    );
}

#[test]
fn migration_does_not_arm_root_triggers_while_the_start_listener_is_pending() {
    // #1379 review r4578935998: the root arms its event sub-processes when the
    // process start event's body runs. Migrating while that start event waits
    // on a start listener must not arm them early (completing the listener
    // would arm them again).
    use crate::model::{ExecutionListener, ListenerEventType};
    let listener = || {
        vec![ExecutionListener {
            event_type: ListenerEventType::Start,
            job_type: "gate".into(),
            retries: None,
        }]
    };
    let base = |with_esp: bool| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            .with_listeners("s", listener(), vec![]);
        if with_esp {
            b = b
                .event_sub_process("esp", "esp_start", sig(), false)
                .start_event("esp_start")
                .service_task("esp_work", "esp")
                .end_event("esp_end")
                .connect("esp_start", "esp_work")
                .connect("esp_work", "esp_end")
                .contained_in("esp_start", "esp")
                .contained_in("esp_work", "esp")
                .contained_in("esp_end", "esp");
        }
        b.build().unwrap()
    };
    let (src, tgt) = (base(false), base(true));
    let mut engine = Engine::new();
    let inst = start(&mut engine, src);
    assert_eq!(job_count(&mut engine, "gate"), 1);
    let target_key = deploy_for_migration(&mut engine, tgt);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("s".into(), "s".into())],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        0,
        "not armed before the start body runs"
    );
    complete_one(&mut engine, "gate");
    assert_eq!(armed(&engine, inst), 1, "armed exactly once");
}

#[test]
fn migration_arms_a_new_root_event_subprocess_despite_a_completion_incident() {
    // #1379 review r4579023810: a root start event parked in COMPLETING — an
    // output-mapping (`Completion`) incident, or an `end`-listener job that
    // failed to mint — resumes COMPLETION on resolution, never its activation
    // body (which is what arms the root's event sub-processes). The root is
    // therefore already steady, so migration must arm a newly added root event
    // sub-process, not skip it as "activation pending".
    use crate::model::{ExecutionListener, IoMapping, ListenerEventType, Mapping};

    // Case (a): an output-mapping (`Completion`) incident on the start event.
    let out_mapping = || IoMapping {
        inputs: Vec::new(),
        outputs: vec![Mapping {
            source: "=assert(bad, bad instance of number) + 1".into(),
            target: "exported".into(),
        }],
    };
    let base = |with_esp: bool| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .with_io("s", out_mapping())
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e");
        if with_esp {
            b = b
                .event_sub_process("esp", "esp_start", sig(), false)
                .start_event("esp_start")
                .service_task("esp_work", "esp")
                .end_event("esp_end")
                .connect("esp_start", "esp_work")
                .connect("esp_work", "esp_end")
                .contained_in("esp_start", "esp")
                .contained_in("esp_work", "esp")
                .contained_in("esp_end", "esp");
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(base(false)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("bad", Value::Str("oops".into()))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    // The start event's output mapping fails on completion: it parks in
    // COMPLETING with a `Completion` incident (its activation already ran).
    let incident = engine.active_incidents();
    assert_eq!(incident.len(), 1, "output mapping parked one incident");
    assert_eq!(incident[0].element_id, "s");
    assert_eq!(armed(&engine, inst), 0, "source has no event sub-process");
    let target_key = deploy_for_migration(&mut engine, base(true));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("s".into(), "s".into()), ("work".into(), "work".into())],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        1,
        "completion-phase incident does not block arming a new root event sub-process"
    );

    // Case (b): an `end`-listener job-type failure on the start event (an
    // `ExecutionListenerJob { End }` incident — resolution resumes completion).
    let end_listener = || {
        vec![ExecutionListener {
            event_type: ListenerEventType::End,
            // A job-type expression that fails to evaluate -> the listener job
            // cannot be minted, parking the start event in COMPLETING.
            job_type: "=missing_var".into(),
            retries: None,
        }]
    };
    let base_el = |with_esp: bool| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            .with_listeners("s", vec![], end_listener());
        if with_esp {
            b = b
                .event_sub_process("esp", "esp_start", sig(), false)
                .start_event("esp_start")
                .service_task("esp_work", "esp")
                .end_event("esp_end")
                .connect("esp_start", "esp_work")
                .connect("esp_work", "esp_end")
                .contained_in("esp_start", "esp")
                .contained_in("esp_work", "esp")
                .contained_in("esp_end", "esp");
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, base_el(false));
    let incident = engine.active_incidents();
    assert_eq!(incident.len(), 1, "end-listener mint parked one incident");
    assert_eq!(incident[0].element_id, "s");
    assert_eq!(armed(&engine, inst), 0);
    let target_key = deploy_for_migration(&mut engine, base_el(true));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("s".into(), "s".into()), ("work".into(), "work".into())],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        1,
        "end-listener completion incident does not block arming a new root event sub-process"
    );
}

#[test]
fn migration_recreates_a_retained_trigger_whose_start_event_is_remapped() {
    // #1379 review (previously missed): an unchanged event sub-process whose
    // START EVENT is mapped away (`esp_start -> s`, onto the process start
    // event) must NOT be retained — `ProcessInstanceMigrated` would re-point
    // the retained trigger's `element_id` at that wrong element. It is deleted
    // and re-created under the target event sub-process's actual start event.
    for t in ALL {
        let mut engine = Engine::new();
        let inst = start(&mut engine, esp_model(trig_of(t), false, false));
        assert_eq!(armed(&engine, inst), 1, "{t:?}: source trigger armed");
        // Identical event sub-process in the target (same `esp`/`esp_start`),
        // but the mapping redirects `esp_start` onto the process start event.
        let mut target = esp_model(trig_of(t), false, false);
        target.name = Some("v2".into());
        let target_key = deploy_for_migration(&mut engine, target);
        let events = engine
            .apply_command(Command::migrate_instance(
                inst,
                target_key,
                vec![
                    ("work".into(), "work".into()),
                    ("esp".into(), "esp".into()),
                    ("esp_start".into(), "s".into()),
                ],
            ))
            .unwrap();
        // Exactly one trigger survives, and it is re-created (not retained).
        assert_eq!(armed(&engine, inst), 1, "{t:?}: exactly one trigger");
        let recreated = events.iter().any(|e| match e {
            Event::ProcessInstanceMigrated { subscriptions, .. } => {
                matches!(t, T::Conditional)
                    || subscriptions.iter().any(|s| {
                        s.element_id == "esp_start"
                            && s.action == crate::event::SubscriptionMigrationAction::Recreated
                    })
            }
            _ => false,
        });
        assert!(recreated, "{t:?}: re-created under the real start event");
        // The live trigger names the event sub-process's start event, not the
        // process start event the mapping pointed at.
        let names_esp_start = match t {
            T::Timer => engine
                .state
                .timers
                .values()
                .any(|x| x.instance_key == inst && x.element_id == "esp_start"),
            T::Message => engine
                .state
                .message_subscriptions
                .values()
                .any(|x| x.instance_key == inst && x.element_id == "esp_start"),
            T::Signal => engine
                .state
                .signal_subscriptions
                .values()
                .any(|x| x.instance_key == inst && x.element_id == "esp_start"),
            T::Conditional => engine
                .state
                .conditional_subscriptions
                .values()
                .any(|x| x.instance_key == inst && x.element_id == "esp_start"),
        };
        assert!(names_esp_start, "{t:?}: trigger names esp_start, not s");
        let fired = fire(&mut engine, inst, t, 5_000);
        assert!(esp_activated(&fired), "{t:?}: re-created trigger fires");
    }
}

#[test]
fn migration_does_not_rearm_a_consumed_event_subprocess_moved_into_a_live_scope() {
    // #1379 review (previously missed): a non-interrupting one-shot timer event
    // sub-process that already FIRED (and completed) at the root, then moved
    // into an already-live embedded sub-process, must not be armed again — its
    // handler already ran. The consumed source counterpart is preserved across
    // the scope move.
    //
    // Target: the SAME event sub-process now lives inside the embedded
    // sub-process `sub` (which is already live in the source instance).
    let target = esp_model(trig_of(T::Timer), false, true);

    let mut engine = Engine::new();
    // Source: the event sub-process at the ROOT, with a live embedded
    // sub-process `sub` alongside (so `sub` is an active scope at migration).
    let src_with_sub = root_esp_beside_sub(trig_of(T::Timer));
    let inst = start(&mut engine, src_with_sub);
    // Fire the root one-shot timer: the event sub-process runs and is consumed.
    let fired = fire(&mut engine, inst, T::Timer, 5_000);
    assert!(esp_activated(&fired));
    complete_one(&mut engine, "esp");
    assert_eq!(armed(&engine, inst), 0, "consumed: no armed trigger");
    assert!(
        engine.state.instances[&inst]
            .active
            .values()
            .any(|id| id == "sub"),
        "the embedded sub-process is still live"
    );

    // Migrate: `esp` moves from the root into the live `sub` scope.
    let target_key = deploy_for_migration(&mut engine, target);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("sub".into(), "sub".into()),
                ("work".into(), "work".into()),
                ("esp".into(), "esp".into()),
                ("esp_work".into(), "esp_work".into()),
            ],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        0,
        "consumed event sub-process is not re-armed after the scope move"
    );
    let fired = fire(&mut engine, inst, T::Timer, 10_000);
    assert!(
        !esp_activated(&fired),
        "the consumed handler does not run a second time"
    );
}

/// A source model: event sub-process `esp` at the ROOT beside a live embedded
/// sub-process `sub` (hosting `work`).
fn root_esp_beside_sub(trigger: Trig) -> ProcessDefinition {
    ProcessBuilder::new("p")
        .start_event("s")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .service_task("work", "w")
        .end_event("sub_e")
        .end_event("e")
        .connect("s", "sub")
        .connect("sub_s", "work")
        .connect("work", "sub_e")
        .connect("sub", "e")
        .contained_in("sub_s", "sub")
        .contained_in("work", "sub")
        .contained_in("sub_e", "sub")
        // The event sub-process at the ROOT (not inside `sub`).
        .event_sub_process("esp", "esp_start", trigger, false)
        .start_event("esp_start")
        .service_task("esp_work", "esp")
        .end_event("esp_end")
        .connect("esp_start", "esp_work")
        .connect("esp_work", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_work", "esp")
        .contained_in("esp_end", "esp")
        .build()
        .unwrap()
}

#[test]
fn migration_audits_an_armed_trigger_moved_across_scopes_as_recreated() {
    // #1379 review: an ARMED root event sub-process moved into a live embedded
    // scope is re-armed there, and the audit reports it as `Recreated` (an
    // existing trigger replaced), not `Created`.
    for t in [T::Timer, T::Message, T::Signal] {
        let mut engine = Engine::new();
        let inst = start(&mut engine, root_esp_beside_sub(trig_of(t)));
        assert_eq!(armed(&engine, inst), 1, "{t:?}: armed at the root");
        let target_key = deploy_for_migration(&mut engine, esp_model(trig_of(t), false, true));
        let events = engine
            .apply_command(Command::migrate_instance(
                inst,
                target_key,
                vec![("sub".into(), "sub".into()), ("work".into(), "work".into())],
            ))
            .unwrap();
        assert_eq!(armed(&engine, inst), 1, "{t:?}: re-armed in `sub`");
        let created = |action| {
            events.iter().any(|e| match e {
                Event::ProcessInstanceMigrated { subscriptions, .. } => subscriptions
                    .iter()
                    .any(|s| s.element_id == "esp_start" && s.action == action),
                _ => false,
            })
        };
        assert!(
            created(crate::event::SubscriptionMigrationAction::Recreated),
            "{t:?}"
        );
        assert!(
            !created(crate::event::SubscriptionMigrationAction::Created),
            "{t:?}"
        );
    }
}

#[test]
fn migration_arms_an_event_subprocess_moved_out_of_an_activation_pending_scope() {
    // #1379 review: an embedded scope still waiting on its start listener has
    // never armed its event sub-processes, so they are NOT consumed — moving
    // one to the steady root arms it there.
    use crate::model::{ExecutionListener, ListenerEventType};
    let model = |esp_in_sub: bool| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .sub_process("sub", "sub_s")
            .start_event("sub_s")
            .service_task("work", "w")
            .end_event("sub_e")
            .end_event("e")
            .connect("s", "sub")
            .connect("sub_s", "work")
            .connect("work", "sub_e")
            .connect("sub", "e")
            .contained_in("sub_s", "sub")
            .contained_in("work", "sub")
            .contained_in("sub_e", "sub")
            .with_listeners(
                "sub",
                vec![ExecutionListener {
                    event_type: ListenerEventType::Start,
                    job_type: "gate".into(),
                    retries: None,
                }],
                vec![],
            )
            .event_sub_process("esp", "esp_start", sig(), false)
            .start_event("esp_start")
            .service_task("esp_work", "esp")
            .end_event("esp_end")
            .connect("esp_start", "esp_work")
            .connect("esp_work", "esp_end")
            .contained_in("esp_start", "esp")
            .contained_in("esp_work", "esp")
            .contained_in("esp_end", "esp");
        if esp_in_sub {
            b = b.contained_in("esp", "sub");
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model(true));
    assert_eq!(job_count(&mut engine, "gate"), 1);
    assert_eq!(armed(&engine, inst), 0, "sub not yet activated");
    let target_key = deploy_for_migration(&mut engine, model(false));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("sub".into(), "sub".into())],
        ))
        .unwrap();
    assert_eq!(armed(&engine, inst), 1, "armed at the steady root");
}

#[test]
fn migration_does_not_rearm_a_consumed_event_subprocess_moved_out_of_a_completion_pending_scope() {
    // #1379 review: a scope parked in its COMPLETION phase (here: an
    // end-listener job is outstanding) already ran its activation body and
    // armed its event sub-process triggers, so a one-shot trigger that is no
    // longer armed there FIRED and is consumed. Moving that event sub-process
    // to the steady root must not arm it again — its handler already ran.
    //
    // Only ACTIVATION-pending scopes are exempt from consumed detection; a
    // completion-pending scope is not (#1379 review r4579294280).
    use crate::model::{ExecutionListener, ListenerEventType};
    let model = |esp_in_sub: bool| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .sub_process("sub", "sub_s")
            .start_event("sub_s")
            .service_task("work", "w")
            .end_event("sub_e")
            .end_event("e")
            .connect("s", "sub")
            .connect("sub_s", "work")
            .connect("work", "sub_e")
            .connect("sub", "e")
            .contained_in("sub_s", "sub")
            .contained_in("work", "sub")
            .contained_in("sub_e", "sub")
            .with_listeners(
                "sub",
                vec![],
                vec![ExecutionListener {
                    event_type: ListenerEventType::End,
                    job_type: "gate".into(),
                    retries: None,
                }],
            )
            .event_sub_process("esp", "esp_start", trig_of(T::Timer), false)
            .start_event("esp_start")
            .service_task("esp_work", "esp")
            .end_event("esp_end")
            .connect("esp_start", "esp_work")
            .connect("esp_work", "esp_end")
            .contained_in("esp_start", "esp")
            .contained_in("esp_work", "esp")
            .contained_in("esp_end", "esp");
        if esp_in_sub {
            b = b.contained_in("esp", "sub");
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model(true));
    assert_eq!(armed(&engine, inst), 1, "sub armed its timer on activation");
    // Fire the one-shot timer inside `sub`; its handler runs and is consumed.
    let fired = fire(&mut engine, inst, T::Timer, 5_000);
    assert!(esp_activated(&fired));
    complete_one(&mut engine, "esp");
    assert_eq!(armed(&engine, inst), 0, "consumed: no armed trigger");
    // Complete `work`: `sub` enters its completion phase and parks on the
    // outstanding end-listener job — completion-pending, not activation-pending.
    complete_one(&mut engine, "w");
    assert_eq!(
        job_count(&mut engine, "gate"),
        1,
        "end listener outstanding"
    );
    assert!(
        engine.state.instances[&inst]
            .active
            .values()
            .any(|id| id == "sub"),
        "the completion-pending sub-process is still live"
    );

    // Migrate: `esp` moves from the completion-pending `sub` to the root.
    let target_key = deploy_for_migration(&mut engine, model(false));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("sub".into(), "sub".into())],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        0,
        "the fired timer is not re-armed at the root"
    );
    let fired = fire(&mut engine, inst, T::Timer, 10_000);
    assert!(
        !esp_activated(&fired),
        "the consumed handler does not run a second time"
    );
}

#[test]
fn migration_keeps_an_armed_trigger_when_a_sibling_hosts_consumed_counterpart_moves() {
    // #1379 review r4579294264: two concurrent instances of a non-interrupting
    // event sub-process `esp1` each host the embedded sub-process `sub`, which
    // hosts a one-shot timer event sub-process `inner`. The timer already fired
    // in the FIRST host (consumed) and is still armed in the SECOND. Moving
    // `inner` from the first host's `sub` into `sub` itself cancels the second
    // host's armed timer — the consumed counterpart must suppress a replacement
    // only in ITS OWN destination scope, never in the second host's.
    let model = |inner_in_sub: bool| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            // Non-interrupting signal event sub-process at the root; each fire
            // starts another instance hosting `sub`.
            .event_sub_process("esp1", "esp1_start", sig(), false)
            .start_event("esp1_start")
            .sub_process("sub", "sub_s")
            .start_event("sub_s")
            .service_task("sub_work", "sw")
            .end_event("sub_e")
            .end_event("esp1_end")
            .connect("esp1_start", "sub")
            .connect("sub_s", "sub_work")
            .connect("sub_work", "sub_e")
            .connect("sub", "esp1_end")
            .contained_in("esp1_start", "esp1")
            .contained_in("sub", "esp1")
            .contained_in("sub_s", "sub")
            .contained_in("sub_work", "sub")
            .contained_in("sub_e", "sub")
            .contained_in("esp1_end", "esp1")
            // One-shot timer event sub-process (in `sub` in the source, at the
            // root in the target when `inner_in_sub` is false).
            .event_sub_process("inner", "inner_start", trig_of(T::Timer), false)
            .start_event("inner_start")
            .service_task("inner_work", "inner")
            .end_event("inner_end")
            .connect("inner_start", "inner_work")
            .connect("inner_work", "inner_end")
            .contained_in("inner_start", "inner")
            .contained_in("inner_work", "inner")
            .contained_in("inner_end", "inner");
        if inner_in_sub {
            b = b.contained_in("inner", "sub");
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model(true));
    // Two concurrent `esp1` instances, each hosting a live `sub`.
    fire(&mut engine, inst, T::Signal, 0);
    fire(&mut engine, inst, T::Signal, 0);
    let mut subs: Vec<Key> = engine.state.instances[&inst]
        .active
        .iter()
        .filter_map(|(k, id)| (id == "sub").then_some(*k))
        .collect();
    subs.sort_unstable();
    assert_eq!(subs.len(), 2, "two live `sub` hosts");
    assert_eq!(armed(&engine, inst), 3, "esp1's signal + two inner timers");
    // The FIRST host's one-shot timer fired: its `inner` ran and is consumed.
    // (`trigger_timers` would fire BOTH hosts' timers, so mark the fired one
    // directly — a `Triggered` timer is exactly what a real fire leaves behind,
    // and `armed_event_subprocess_triggers` reads only `Created` ones.)
    let fired_key = engine
        .state
        .timers
        .values()
        .find(|t| {
            t.instance_key == inst
                && t.element_instance_key == subs[0]
                && t.state == crate::state::TimerState::Created
        })
        .map(|t| t.key)
        .expect("the first host's inner timer is armed");
    engine
        .state
        .timers
        .get_mut(&fired_key)
        .expect("the timer exists")
        .state = crate::state::TimerState::Triggered;
    assert_eq!(
        armed(&engine, inst),
        2,
        "the second host's timer is still armed"
    );

    // Migrate: `inner` moves from each host's `sub` to the ROOT.
    let target_key = deploy_for_migration(&mut engine, model(false));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("work".into(), "work".into()),
                ("esp1".into(), "esp1".into()),
                ("sub".into(), "sub".into()),
                ("sub_work".into(), "sub_work".into()),
            ],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        2,
        "the armed host's timer is re-armed at the root; the consumed one is not"
    );
    // The surviving timer is armed at the root: firing it activates `inner`
    // (the consumed counterpart stays silent).
    let fired = fire(&mut engine, inst, T::Timer, 10_000);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::ElementActivated { element_id, .. } if element_id == "inner"
        )),
        "the surviving timer fires its handler"
    );
}

/// Two concurrent `esp1` instances, each hosting `sub`, which hosts a one-shot
/// timer event sub-process `inner`. In the target, `inner` is renamed to
/// `inner_id` and contained in `inner_parent` (`None` = the root). Marks the
/// timer of each host whose `consumed` flag is set as fired, migrates, and
/// returns the engine, instance, and the source `sub` / `esp1` hosts (sorted,
/// paired).
fn two_host_inner_move(
    consumed: [bool; 2],
    inner_id: &str,
    inner_parent: Option<&str>,
) -> (Engine, Key, Vec<(Key, Key)>) {
    let model = |inner_id: &str, inner_parent: Option<&str>| {
        let start = format!("{inner_id}_start");
        let work = format!("{inner_id}_work");
        let end = format!("{inner_id}_end");
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            .event_sub_process("esp1", "esp1_start", sig(), false)
            .start_event("esp1_start")
            .sub_process("sub", "sub_s")
            .start_event("sub_s")
            .service_task("sub_work", "sw")
            .end_event("sub_e")
            .end_event("esp1_end")
            .connect("esp1_start", "sub")
            .connect("sub_s", "sub_work")
            .connect("sub_work", "sub_e")
            .connect("sub", "esp1_end")
            .contained_in("esp1_start", "esp1")
            .contained_in("sub", "esp1")
            .contained_in("sub_s", "sub")
            .contained_in("sub_work", "sub")
            .contained_in("sub_e", "sub")
            .contained_in("esp1_end", "esp1")
            .event_sub_process(inner_id, &start, trig_of(T::Timer), false)
            .start_event(&start)
            .service_task(&work, "inner")
            .end_event(&end)
            .connect(&start, &work)
            .connect(&work, &end)
            .contained_in(&start, inner_id)
            .contained_in(&work, inner_id)
            .contained_in(&end, inner_id);
        if let Some(p) = inner_parent {
            b = b.contained_in(inner_id, p);
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model("inner", Some("sub")));
    fire(&mut engine, inst, T::Signal, 0);
    fire(&mut engine, inst, T::Signal, 0);
    let mut subs: Vec<Key> = engine.state.instances[&inst]
        .active
        .iter()
        .filter_map(|(k, id)| (id == "sub").then_some(*k))
        .collect();
    subs.sort_unstable();
    let hosts: Vec<(Key, Key)> = subs
        .iter()
        .map(|s| (*s, engine.scope_of(inst, *s)))
        .collect();
    for (i, (sub, _)) in hosts.iter().enumerate() {
        if !consumed[i] {
            continue;
        }
        let key = engine
            .state
            .timers
            .values()
            .find(|t| {
                t.instance_key == inst
                    && t.element_instance_key == *sub
                    && t.state == crate::state::TimerState::Created
            })
            .map(|t| t.key)
            .expect("host timer armed");
        engine.state.timers.get_mut(&key).expect("timer").state =
            crate::state::TimerState::Triggered;
    }
    let target_key = deploy_for_migration(&mut engine, model(inner_id, inner_parent));
    let mut mappings: Vec<(ElementId, ElementId)> = vec![
        ("work".into(), "work".into()),
        ("esp1".into(), "esp1".into()),
        ("sub".into(), "sub".into()),
        ("sub_work".into(), "sub_work".into()),
    ];
    if inner_id != "inner" {
        mappings.push(("inner".into(), inner_id.into()));
    }
    engine
        .apply_command(Command::migrate_instance(inst, target_key, mappings))
        .unwrap();
    (engine, inst, hosts)
}

/// Element instance keys hosting the instance's armed timers.
fn armed_timer_hosts(engine: &Engine, inst: Key) -> Vec<Key> {
    let mut v: Vec<Key> = engine
        .state
        .timers
        .values()
        .filter(|t| t.instance_key == inst && t.state == crate::state::TimerState::Created)
        .map(|t| t.element_instance_key)
        .collect();
    v.sort_unstable();
    v
}

#[test]
fn migration_moving_consumed_timers_into_distinct_parent_hosts_rearms_none() {
    // #1379 review r4579460688: both hosts' `inner` timers fired; moving
    // `inner` from each `sub` into its enclosing `esp1` must re-arm neither —
    // each consumed counterpart resolves to ITS OWN `esp1` instance.
    let (engine, inst, _) = two_host_inner_move([true, true], "inner", Some("esp1"));
    assert!(
        armed_timer_hosts(&engine, inst).is_empty(),
        "no inner timer re-armed"
    );
}

#[test]
fn migration_mixed_armed_and_consumed_into_distinct_parent_hosts() {
    // #1379 review r4579460688: host 0's timer fired, host 1's is armed. The
    // armed timer is re-armed in host 1's `esp1` only; host 0's `esp1` stays
    // unarmed (its counterpart is consumed).
    for consumed in [[true, false], [false, true]] {
        let (engine, inst, hosts) = two_host_inner_move(consumed, "inner", Some("esp1"));
        let armed_idx = if consumed[0] { 1 } else { 0 };
        assert_eq!(
            armed_timer_hosts(&engine, inst),
            vec![hosts[armed_idx].1],
            "only the armed host's esp1 carries the timer ({consumed:?})"
        );
    }
}

#[test]
fn migration_rearms_a_moved_armed_trigger_across_an_esp_rename() {
    // #1379 review r4579460727: the armed host's timer moves to the root AND
    // `inner` is renamed; the other host's consumed `renamed` counterpart must
    // not suppress the replacement.
    let (engine, inst, _) = two_host_inner_move([true, false], "renamed", None);
    assert_eq!(
        armed_timer_hosts(&engine, inst),
        vec![0],
        "re-armed at the root"
    );
}

#[test]
fn migration_resolves_destination_by_the_unchanged_target_parent_id() {
    // #1379 review r4579460715: live scopes mapped `subA -> subB` and
    // `subB -> subC`; a consumed root timer ESP moving into target `subB` must
    // be suppressed in the `subB` host (the source `subA` instance), not
    // looked up as the host of `subC`.
    let model = |esp_parent: Option<&str>| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .parallel_gateway("fork")
            .sub_process("subA", "a_s")
            .start_event("a_s")
            .service_task("a_w", "aw")
            .end_event("a_e")
            .sub_process("subB", "b_s")
            .start_event("b_s")
            .service_task("b_w", "bw")
            .end_event("b_e")
            .sub_process("subC", "c_s")
            .start_event("c_s")
            .service_task("c_w", "cw")
            .end_event("c_e")
            .end_event("e1")
            .end_event("e2")
            .connect("s", "fork")
            .connect("fork", "subA")
            .connect("fork", "subB")
            .connect("subA", "e1")
            .connect("subB", "e2")
            .connect("a_s", "a_w")
            .connect("a_w", "a_e")
            .connect("b_s", "b_w")
            .connect("b_w", "b_e")
            .connect("c_s", "c_w")
            .connect("c_w", "c_e")
            .contained_in("a_s", "subA")
            .contained_in("a_w", "subA")
            .contained_in("a_e", "subA")
            .contained_in("b_s", "subB")
            .contained_in("b_w", "subB")
            .contained_in("b_e", "subB")
            .contained_in("c_s", "subC")
            .contained_in("c_w", "subC")
            .contained_in("c_e", "subC")
            .event_sub_process("esp", "esp_start", trig_of(T::Timer), false)
            .start_event("esp_start")
            .service_task("esp_w", "ew")
            .end_event("esp_e")
            .connect("esp_start", "esp_w")
            .connect("esp_w", "esp_e")
            .contained_in("esp_start", "esp")
            .contained_in("esp_w", "esp")
            .contained_in("esp_e", "esp");
        if let Some(p) = esp_parent {
            b = b.contained_in("esp", p);
        }
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model(None));
    // The root timer fires once (consumed).
    for t in engine.state.timers.values_mut() {
        if t.instance_key == inst && t.state == crate::state::TimerState::Created {
            t.state = crate::state::TimerState::Triggered;
        }
    }
    assert_eq!(armed(&engine, inst), 0);
    let target_key = deploy_for_migration(&mut engine, model(Some("subB")));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("subA".into(), "subB".into()),
                ("a_w".into(), "b_w".into()),
                ("subB".into(), "subC".into()),
                ("b_w".into(), "c_w".into()),
            ],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        0,
        "the consumed one-shot timer is not re-armed in the subB host"
    );
}

#[test]
fn migration_does_not_rearm_a_consumed_timer_moved_between_live_siblings() {
    // #1379 review r4579525797: a consumed one-shot timer ESP hosted by live
    // `subA` moves to live sibling `subB`; its destination resolves through
    // the nearest shared runtime ancestor (the root), so it stays suppressed.
    let model = |esp_parent: &str| {
        ProcessBuilder::new("p")
            .start_event("s")
            .parallel_gateway("fork")
            .sub_process("subA", "a_s")
            .start_event("a_s")
            .service_task("a_w", "aw")
            .end_event("a_e")
            .sub_process("subB", "b_s")
            .start_event("b_s")
            .service_task("b_w", "bw")
            .end_event("b_e")
            .end_event("e1")
            .end_event("e2")
            .connect("s", "fork")
            .connect("fork", "subA")
            .connect("fork", "subB")
            .connect("subA", "e1")
            .connect("subB", "e2")
            .connect("a_s", "a_w")
            .connect("a_w", "a_e")
            .connect("b_s", "b_w")
            .connect("b_w", "b_e")
            .contained_in("a_s", "subA")
            .contained_in("a_w", "subA")
            .contained_in("a_e", "subA")
            .contained_in("b_s", "subB")
            .contained_in("b_w", "subB")
            .contained_in("b_e", "subB")
            .event_sub_process("esp", "esp_start", trig_of(T::Timer), false)
            .start_event("esp_start")
            .service_task("esp_w", "ew")
            .end_event("esp_e")
            .connect("esp_start", "esp_w")
            .connect("esp_w", "esp_e")
            .contained_in("esp_start", "esp")
            .contained_in("esp_w", "esp")
            .contained_in("esp_e", "esp")
            .contained_in("esp", esp_parent)
            .build()
            .unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model("subA"));
    assert_eq!(armed(&engine, inst), 1, "armed in subA");
    for t in engine.state.timers.values_mut() {
        if t.instance_key == inst && t.state == crate::state::TimerState::Created {
            t.state = crate::state::TimerState::Triggered;
        }
    }
    assert_eq!(armed(&engine, inst), 0);
    let target_key = deploy_for_migration(&mut engine, model("subB"));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("subA".into(), "subA".into()),
                ("a_w".into(), "a_w".into()),
                ("subB".into(), "subB".into()),
                ("b_w".into(), "b_w".into()),
            ],
        ))
        .unwrap();
    assert_eq!(
        armed(&engine, inst),
        0,
        "the consumed one-shot timer is not re-armed in sibling subB"
    );
}

#[test]
fn migration_bounds_destination_search_to_the_mapped_common_ancestor_instance() {
    // #1379 review r4579614315: two concurrent `esp1` hosts. Host 0's `subA`
    // timer fired (consumed); host 1 has a live `subB`. The consumed ESP moves
    // to `subB` in the target. Host 0 has no live `subB`, so its consumed
    // counterpart must resolve to NO destination (its mapped common ancestor
    // is its own `esp1` instance) — the search must NOT climb to the root and
    // pick host 1's `subB`, which would suppress host 1's own new timer.
    let model = |esp_parent: &str| {
        ProcessBuilder::new("p")
            .start_event("s")
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            .event_sub_process("esp1", "esp1_start", sig(), false)
            .start_event("esp1_start")
            .sub_process("subA", "a_s")
            .start_event("a_s")
            .service_task("a_w", "aw")
            .end_event("a_e")
            .sub_process("subB", "b_s")
            .start_event("b_s")
            .service_task("b_w", "bw")
            .end_event("b_e")
            .end_event("esp1_end")
            .connect("esp1_start", "subA")
            .connect("a_s", "a_w")
            .connect("a_w", "a_e")
            .connect("subA", "subB")
            .connect("b_s", "b_w")
            .connect("b_w", "b_e")
            .connect("subB", "esp1_end")
            .contained_in("esp1_start", "esp1")
            .contained_in("subA", "esp1")
            .contained_in("a_s", "subA")
            .contained_in("a_w", "subA")
            .contained_in("a_e", "subA")
            .contained_in("subB", "esp1")
            .contained_in("b_s", "subB")
            .contained_in("b_w", "subB")
            .contained_in("b_e", "subB")
            .contained_in("esp1_end", "esp1")
            .event_sub_process("esp", "esp_start", trig_of(T::Timer), false)
            .start_event("esp_start")
            .service_task("esp_w", "ew")
            .end_event("esp_e")
            .connect("esp_start", "esp_w")
            .connect("esp_w", "esp_e")
            .contained_in("esp_start", "esp")
            .contained_in("esp_w", "esp")
            .contained_in("esp_e", "esp")
            .contained_in("esp", esp_parent)
            .build()
            .unwrap()
    };
    let mut engine = Engine::new();
    let inst = start(&mut engine, model("subA"));
    // Two concurrent `esp1` hosts; each parks in its `subA` (its `subB` is
    // not yet entered).
    fire(&mut engine, inst, T::Signal, 0);
    fire(&mut engine, inst, T::Signal, 0);
    let mut sub_a: Vec<Key> = engine.state.instances[&inst]
        .active
        .iter()
        .filter_map(|(k, id)| (id == "subA").then_some(*k))
        .collect();
    sub_a.sort_unstable();
    assert_eq!(sub_a.len(), 2, "two concurrent hosts each in subA");
    // Consume host 0's timer only.
    let key = engine
        .state
        .timers
        .values()
        .find(|t| {
            t.instance_key == inst
                && t.element_instance_key == sub_a[0]
                && t.state == crate::state::TimerState::Created
        })
        .map(|t| t.key)
        .expect("host 0 timer armed");
    engine.state.timers.get_mut(&key).expect("timer").state = crate::state::TimerState::Triggered;
    // Host 1 completes `subA` and enters its `subB` (host 0 never does).
    let b_w_job = engine
        .activate_jobs("aw", "test-worker", 10, 60_000, 0)
        .into_iter()
        .find(|j| engine.scope_of(inst, j.element_instance_key) == sub_a[1])
        .map(|j| j.key)
        .expect("host 1 a_w job");
    engine
        .apply_command(Command::complete_job(b_w_job))
        .unwrap();
    let sub_b1: Vec<Key> = engine.state.instances[&inst]
        .active
        .iter()
        .filter_map(|(k, id)| (id == "subB").then_some(*k))
        .collect();
    assert_eq!(sub_b1.len(), 1, "only host 1 has a live subB");
    // Migrate: the timer ESP moves from `subA` to `subB`.
    let target_key = deploy_for_migration(&mut engine, model("subB"));
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("work".into(), "work".into()),
                ("esp1".into(), "esp1".into()),
                ("subA".into(), "subA".into()),
                ("a_w".into(), "a_w".into()),
                ("subB".into(), "subB".into()),
                ("b_w".into(), "b_w".into()),
            ],
        ))
        .unwrap();
    // Host 1's live `subB` arms the moved-in timer (its source `subA`
    // counterpart was still armed). Host 0's consumed counterpart resolves to
    // NO destination — bounded by its own `esp1` instance — and must NOT
    // suppress host 1's timer.
    assert_eq!(
        armed_timer_hosts(&engine, inst),
        sub_b1,
        "host 1's subB carries the timer; host 0's consumed timer suppresses nothing"
    );
}

#[test]
fn migration_cyclic_containment_in_unused_target_scope_does_not_hang() {
    // #1379 review r4579688057: `ProcessBuilder::contained_in("sub", "sub")`
    // and deployment preflight do not reject cyclic containment, so an armed
    // event sub-process moved under an unused target sub-process whose parent
    // chain CYCLES passed the active-element checks, then the destination
    // search's model-ancestor walk looped forever. The walk must be the
    // cycle-safe `flow_scope::element_flow_scope_chain`: the migration
    // completes, and with no live `sub` instance the consumed source trigger
    // resolves to no destination (nothing re-arms).
    let model = |esp_parent: Option<&str>| {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("work", "w")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            .sub_process("sub", "sub_s")
            .start_event("sub_s")
            .service_task("sub_w", "sw")
            .end_event("sub_e")
            .connect("sub_s", "sub_w")
            .connect("sub_w", "sub_e")
            .contained_in("sub_s", "sub")
            .contained_in("sub_w", "sub")
            .contained_in("sub_e", "sub")
            .event_sub_process("esp", "esp_start", trig_of(T::Timer), false)
            .start_event("esp_start")
            .service_task("esp_w", "ew")
            .end_event("esp_e")
            .connect("esp_start", "esp_w")
            .connect("esp_w", "esp_e")
            .contained_in("esp_start", "esp")
            .contained_in("esp_w", "esp")
            .contained_in("esp_e", "esp");
        b = match esp_parent {
            Some(p) => b.contained_in("esp", p),
            None => b,
        };
        b.build().unwrap()
    };
    let mut engine = Engine::new();
    // Source: the timer ESP lives in the process-level scope; park in `work`.
    let inst = start(&mut engine, model(None));
    // Consume the armed timer.
    let key = engine
        .state
        .timers
        .values()
        .find(|t| t.instance_key == inst && t.state == crate::state::TimerState::Created)
        .map(|t| t.key)
        .expect("esp timer armed");
    engine.state.timers.get_mut(&key).expect("timer").state = crate::state::TimerState::Triggered;
    // Target: the ESP moves under `sub`, whose containment is CYCLIC
    // (`sub` inside `sub`). `sub` is never entered, so no live scope maps to
    // it. This must terminate (not hang) and re-arm nothing.
    let mut target = model(Some("sub"));
    target.elements.get_mut("sub").expect("sub exists").parent = Some("sub".into());
    let target_key = deploy_for_migration(&mut engine, target);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("work".into(), "work".into())],
        ))
        .unwrap();
    assert!(
        armed_timer_hosts(&engine, inst).is_empty(),
        "cyclic unused target scope: consumed timer re-arms nothing"
    );
}
