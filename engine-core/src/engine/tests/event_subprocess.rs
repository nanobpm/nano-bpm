//! Event sub-process (`subProcess triggeredByEvent="true"`) engine tests —
//! #1555 wave-0 model slice.
//!
//! The runtime does NOT trigger an event sub-process yet (that is the
//! follow-up runtime slice). These tests pin the wave-0 contract: deploying a
//! model containing an event sub-process succeeds, the event sub-process is
//! never activated by token flow, and an existing model's behaviour is
//! unchanged.
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
