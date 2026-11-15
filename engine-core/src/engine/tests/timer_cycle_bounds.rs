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

// ---------------------------------------------------------------------------
// #1596 review: a FEEL cycle whose re-arm fails raises an incident; resolving
// it must resume the PARTIALLY CONSUMED cycle (remaining count + the fired
// timer's due time), never reset it to the definition's full `Rn` count. Both
// re-arm paths (non-interrupting boundary, non-interrupting ESP) are covered.
// ---------------------------------------------------------------------------

/// Drive an `R3` FEEL cycle (`=cyc`): fire once, break `cyc`, fire again (the
/// re-arm raises an incident), repair `cyc`, resolve — the resumed cycle must
/// fire exactly once more (3 fires total) on the original schedule.
fn assert_rearm_incident_resumes_remaining_count(def: ProcessDefinition, process_id: &str) {
    let cycle = Value::Str("R3/PT5S".into());
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command_at(
            Command::create_instance_with(
                process_id,
                HashMap::from([("cyc".to_string(), cycle.clone())]),
            ),
            1_000,
        )
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(armed(&engine), vec![6_000]);
    engine.trigger_timers(6_000); // fire 1 of 3
    assert_eq!(armed(&engine), vec![11_000]);

    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("cyc".to_string(), Value::Int(42))]),
        ))
        .unwrap();
    engine.trigger_timers(11_000); // fire 2 of 3; the re-arm fails
    assert!(armed(&engine).is_empty());
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "the failed re-arm parks an incident");
    assert!(matches!(
        incidents[0].redrive,
        Some(state::IoMappingRedrive::TimerCycleRearm {
            previous_due_at: 11_000,
            remaining: Some(2),
            ..
        })
    ));
    let incident_key = incidents[0].key;

    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("cyc".to_string(), cycle)]),
        ))
        .unwrap();
    engine
        .apply_command_at(Command::resolve_incident(incident_key), 13_000)
        .unwrap();
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        armed(&engine),
        vec![16_000],
        "the retry re-arms only the cycle, from the fired timer's due time"
    );
    let fired = engine.trigger_timers(16_000); // fire 3 of 3 — the last
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(
        armed(&engine).is_empty(),
        "R3 fires exactly three times — resolution must not reset the count"
    );
    assert!(engine.trigger_timers(1_000_000).is_empty());
}

#[test]
fn boundary_cycle_rearm_incident_resumes_remaining_count() {
    assert_rearm_incident_resumes_remaining_count(boundary_process("=cyc"), "ticker");
}

#[test]
fn event_subprocess_cycle_rearm_incident_resumes_remaining_count() {
    let def = parse(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="esp-feel" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
    <bpmn:subProcess id="esp" triggeredByEvent="true">
      <bpmn:startEvent id="esp_start" isInterrupting="false">
        <bpmn:timerEventDefinition><bpmn:timeCycle>=cyc</bpmn:timeCycle></bpmn:timerEventDefinition>
      </bpmn:startEvent>
      <bpmn:endEvent id="esp_end" />
      <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_end" />
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>"#,
    );
    assert_rearm_incident_resumes_remaining_count(def, "esp-feel");
}

/// #1583: an event sub-process hosted by an EMBEDDED sub-process — the re-arm
/// incident's redrive must re-arm the timer in the sub-process scope (the host
/// element instance), never retry the ESP's activation.
#[test]
fn embedded_event_subprocess_cycle_rearm_incident_resumes_remaining_count() {
    let def = parse(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="esp-embedded" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:subProcess id="host">
      <bpmn:startEvent id="host_start" />
      <bpmn:serviceTask id="work">
        <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="host_end" />
      <bpmn:sequenceFlow id="hf1" sourceRef="host_start" targetRef="work" />
      <bpmn:sequenceFlow id="hf2" sourceRef="work" targetRef="host_end" />
      <bpmn:subProcess id="esp" triggeredByEvent="true">
        <bpmn:startEvent id="esp_start" isInterrupting="false">
          <bpmn:timerEventDefinition><bpmn:timeCycle>=cyc</bpmn:timeCycle></bpmn:timerEventDefinition>
        </bpmn:startEvent>
        <bpmn:endEvent id="esp_end" />
        <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_end" />
      </bpmn:subProcess>
    </bpmn:subProcess>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="host" />
    <bpmn:sequenceFlow id="f2" sourceRef="host" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#,
    );
    assert_rearm_incident_resumes_remaining_count(def, "esp-embedded");
}

// ---------------------------------------------------------------------------
// #1596 review: a re-arm incident can outlive its instance. Resolving it after
// the instance completed must not mint a timer (or raise another incident) on
// the completed instance.
// ---------------------------------------------------------------------------

#[test]
fn event_subprocess_cycle_rearm_incident_resolution_after_completion_is_noop() {
    let def = parse(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="esp-complete" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
    <bpmn:subProcess id="esp" triggeredByEvent="true">
      <bpmn:startEvent id="esp_start" isInterrupting="false">
        <bpmn:timerEventDefinition><bpmn:timeCycle>=cyc</bpmn:timeCycle></bpmn:timerEventDefinition>
      </bpmn:startEvent>
      <bpmn:endEvent id="esp_end" />
      <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_end" />
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>"#,
    );
    let cycle = Value::Str("R3/PT5S".into());
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command_at(
            Command::create_instance_with(
                "esp-complete",
                HashMap::from([("cyc".to_string(), cycle.clone())]),
            ),
            1_000,
        )
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(armed(&engine), vec![6_000]);
    engine.trigger_timers(6_000); // fire 1 of 3
    assert_eq!(armed(&engine), vec![11_000]);

    // Break the cycle variable so the second fire's re-arm raises an incident.
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("cyc".to_string(), Value::Int(42))]),
        ))
        .unwrap();
    engine.trigger_timers(11_000); // fire 2 of 3; the re-arm fails
    assert!(armed(&engine).is_empty());
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1);
    let incident_key = incidents[0].key;

    // Complete the instance while the incident is still open.
    complete_one(&mut engine, "work");
    assert!(engine.is_completed(instance_key));

    // Repair the variable and resolve the incident: the retry must be skipped —
    // no new timer, no new incident, and the incident record is closed.
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("cyc".to_string(), cycle)]),
        ))
        .unwrap();
    let events = engine
        .apply_command_at(Command::resolve_incident(incident_key), 13_000)
        .unwrap();
    assert!(engine.active_incidents().is_empty());
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::TimerCreated { .. })),
        "no timer is minted on a completed instance: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { .. })),
        "no fresh incident is raised on a completed instance: {events:?}"
    );
    assert!(armed(&engine).is_empty());
}

// ---------------------------------------------------------------------------
// #1596 review: migrating to renamed host and boundary elements must re-point
// the boundary id inside a pending `TimerCycleRearm` redrive, or resolving the
// incident after migration looks up the old boundary id and silently drops the
// remaining fires.
// ---------------------------------------------------------------------------

fn boundary_process_named(process_id: &str, task: &str, boundary: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="{process_id}" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="{task}">
      <bpmn:extensionElements><zeebe:taskDefinition type="payment" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="{boundary}" attachedToRef="{task}" cancelActivity="false">
      <bpmn:timerEventDefinition><bpmn:timeCycle>=cyc</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="ticked" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="{task}" />
    <bpmn:sequenceFlow id="f2" sourceRef="{task}" targetRef="done" />
    <bpmn:sequenceFlow id="f3" sourceRef="{boundary}" targetRef="ticked" />
  </bpmn:process>
</bpmn:definitions>"#
    ))
}

#[test]
fn migration_remaps_pending_cycle_rearm_boundary_id() {
    let cycle = Value::Str("R3/PT5S".into());
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        boundary_process_named("source", "charge", "tick"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        boundary_process_named("target", "charge2", "tick2"),
    );

    let created = engine
        .apply_command_at(
            Command::create_instance_with(
                "source",
                HashMap::from([("cyc".to_string(), cycle.clone())]),
            ),
            1_000,
        )
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(armed(&engine), vec![6_000]);
    engine.trigger_timers(6_000); // fire 1 of 3
    assert_eq!(armed(&engine), vec![11_000]);

    // Break the cycle variable so the second fire's re-arm raises an incident.
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("cyc".to_string(), Value::Int(42))]),
        ))
        .unwrap();
    engine.trigger_timers(11_000); // fire 2 of 3; the re-arm fails
    assert!(armed(&engine).is_empty());
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1);
    let incident_key = incidents[0].key;
    assert!(matches!(
        incidents[0].redrive,
        Some(state::IoMappingRedrive::TimerCycleRearm {
            kind: state::TimerKind::NonInterruptingBoundary { ref boundary_element_id },
            ..
        }) if boundary_element_id == "tick"
    ));

    // Migrate to a definition that renames both the host task and the boundary.
    engine
        .apply_command(Command::migrate_instance(
            instance_key,
            target_key,
            vec![
                ("charge".to_string(), "charge2".to_string()),
                ("tick".to_string(), "tick2".to_string()),
            ],
        ))
        .unwrap();
    let incident = engine.incident(incident_key).unwrap();
    assert!(
        matches!(
            incident.redrive,
            Some(state::IoMappingRedrive::TimerCycleRearm {
                kind: state::TimerKind::NonInterruptingBoundary { ref boundary_element_id },
                ..
            }) if boundary_element_id == "tick2"
        ),
        "migration re-points the pending re-arm's boundary id"
    );

    // Repair the variable and resolve: the retry re-arms against the renamed
    // boundary and the bounded cycle fires exactly once more.
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("cyc".to_string(), cycle)]),
        ))
        .unwrap();
    engine
        .apply_command_at(Command::resolve_incident(incident_key), 13_000)
        .unwrap();
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        armed(&engine),
        vec![16_000],
        "the retry re-arms the renamed boundary from the fired timer's due time"
    );
    let fired = engine.trigger_timers(16_000); // fire 3 of 3 — the last
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(
        armed(&engine).is_empty(),
        "R3 fires exactly three times — migration must not lose the remaining count"
    );
}
