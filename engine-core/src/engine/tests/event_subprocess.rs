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
