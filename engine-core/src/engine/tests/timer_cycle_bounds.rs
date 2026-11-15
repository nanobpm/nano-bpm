//! Bounded `Rn/…` timer cycles (#1596): Zeebe fires an `R2/PT…` cycle exactly
//! twice and then stops re-arming. Covers every cycle re-arm path —
//! non-interrupting boundary (static + FEEL), event sub-process, and timer
//! start event — plus replay of the persisted remaining-fire count.
use super::*;

fn parse(xml: &str) -> ProcessDefinition {
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn armed(engine: &Engine) -> Vec<u64> {
    let mut v: Vec<u64> = engine
        .timers()
        .iter()
        .filter(|t| t.state == state::TimerState::Created)
        .map(|t| t.due_at)
        .collect();
    v.sort_unstable();
    v
}

fn boundary_process(cycle: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="ticker" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge">
      <bpmn:extensionElements><zeebe:taskDefinition type="payment" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="tick" attachedToRef="charge" cancelActivity="false">
      <bpmn:timerEventDefinition><bpmn:timeCycle>{cycle}</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="ticked" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="done" />
    <bpmn:sequenceFlow id="f3" sourceRef="tick" targetRef="ticked" />
  </bpmn:process>
</bpmn:definitions>"#
    ))
}

fn assert_boundary_fires_twice(cycle: &str) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(boundary_process(cycle)))
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("ticker"), 1_000)
        .unwrap();
    assert_eq!(armed(&engine), vec![6_000], "{cycle}");
    engine.trigger_timers(6_000);
    assert_eq!(armed(&engine), vec![11_000], "{cycle}: first fire re-arms");
    let fired = engine.trigger_timers(11_000);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
    )));
    assert!(
        !fired
            .iter()
            .any(|e| matches!(e, Event::TimerCreated { .. })),
        "{cycle}: the 2nd fire is the last — no third TimerCreated"
    );
    assert!(armed(&engine).is_empty());
    assert!(engine.trigger_timers(100_000).is_empty());
}

#[test]
fn bounded_static_cycle_boundary_stops_after_n_fires() {
    assert_boundary_fires_twice("R2/PT5S");
}

#[test]
fn bounded_feel_cycle_boundary_stops_after_n_fires() {
    assert_boundary_fires_twice("=\"R2/PT5S\"");
}

#[test]
fn unbounded_cycle_boundary_keeps_re_arming() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(boundary_process("R/PT5S")))
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("ticker"), 1_000)
        .unwrap();
    for (fire, next) in [(6_000, 11_000), (11_000, 16_000), (16_000, 21_000)] {
        engine.trigger_timers(fire);
        assert_eq!(armed(&engine), vec![next]);
    }
}

fn esp_process() -> ProcessDefinition {
    parse(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="esp-cycle" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
    <bpmn:subProcess id="esp" triggeredByEvent="true">
      <bpmn:startEvent id="esp_start" isInterrupting="false">
        <bpmn:timerEventDefinition><bpmn:timeCycle>R2/PT1M</bpmn:timeCycle></bpmn:timerEventDefinition>
      </bpmn:startEvent>
      <bpmn:endEvent id="esp_end" />
      <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_end" />
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>"#,
    )
}

#[test]
fn bounded_cycle_event_subprocess_stops_after_n_fires_and_survives_replay() {
    let mut engine = Engine::new();
    let mut log = engine
        .apply_command(Command::DeployProcess(esp_process()))
        .unwrap();
    log.extend(
        engine
            .apply_command_at(Command::create_instance("esp-cycle"), 0)
            .unwrap(),
    );
    assert_eq!(armed(&engine), vec![60_000]);
    log.extend(engine.trigger_timers(60_000));
    assert_eq!(armed(&engine), vec![120_000]);

    // The remaining-fire count is carried on the journal, so a replayed engine
    // also stops after the second fire.
    let mut recovered = Engine::replay(log);
    for e in [&mut engine, &mut recovered] {
        let fired = e.trigger_timers(120_000);
        assert!(fired.iter().any(
            |ev| matches!(ev, Event::TimerTriggered { element_id, .. } if element_id == "esp_start")
        ));
        assert!(
            !fired
                .iter()
                .any(|ev| matches!(ev, Event::TimerCreated { .. })),
            "R2 fires exactly twice"
        );
        assert!(armed(e).is_empty());
    }
}

#[test]
fn bounded_cycle_start_timer_stops_after_n_fires() {
    let def = parse(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
  <bpmn:process id="recurring" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:timerEventDefinition><bpmn:timeCycle>R2/PT10S</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#,
    );
    let mut engine = Engine::new();
    let mut log = engine
        .apply_command_at(Command::DeployProcess(def), 1_000)
        .unwrap();
    log.extend(engine.trigger_timers(11_000));
    assert_eq!(engine.state().instances.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(21_000));

    let mut recovered = Engine::replay(log);
    for e in [&mut engine, &mut recovered] {
        e.trigger_timers(21_000);
        assert_eq!(e.state().instances.len(), 2);
        let timer = e.state().start_timers.values().next().unwrap();
        assert_eq!(timer.due_at, None, "R2 start cycle fires exactly twice");
        assert!(e.trigger_timers(100_000).is_empty());
        assert_eq!(e.state().instances.len(), 2);
    }
}
