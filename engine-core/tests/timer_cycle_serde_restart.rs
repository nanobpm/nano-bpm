//! Bounded `Rn/…` timer cycles across a **serde restart** (#1596): the
//! remaining-fire count is persisted on the journal (`TimerCreated.repetitions`)
//! and in the snapshot (`Timer.repetitions` / `StartTimer.repetitions`), so an
//! engine rebuilt from either must resume a partially consumed cycle and fire
//! only the remaining fires.
//!
//! The in-memory replay halves of these scenarios live in
//! `src/engine/tests/timer_cycle_bounds.rs`; this file guards the serialized
//! persistence those tests skip — both restart surfaces:
//!
//! 1. **Journal** — every emitted event is serialized to a JSON journal line and
//!    decoded back through [`decode_event_json`] (the same decoder the storage
//!    layer replays with), then replayed into a fresh engine.
//! 2. **Snapshot** — the live engine's [`Engine::snapshot`] is round-tripped
//!    through `serde_json` and restored with [`Engine::from_snapshot`].
//!
//! Both halves run for an **instance timer** (a non-interrupting event
//! sub-process cycle) and a **process-start timer**, so neither persistence
//! surface can drift without a red test.
//!
//! # CRITICAL: `--features serde`
//!
//! The `Event`/`EngineSnapshot` serde derives are feature-gated, so this whole
//! file is `#![cfg(feature = "serde")]` and must be built/run with
//! `--features serde` (the `engine-core (clippy + test)` CI job passes it).

#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, decode_event_json, Command, Engine, Event, Key, ProcessDefinition, TimerState,
};

fn parse(xml: &str) -> ProcessDefinition {
    parse_bpmn(xml).unwrap().remove(0)
}

/// Due times of the live (Created) instance timers, ascending.
fn armed(engine: &Engine) -> Vec<u64> {
    let mut v: Vec<u64> = engine
        .timers()
        .iter()
        .filter(|t| t.state == TimerState::Created)
        .map(|t| t.due_at)
        .collect();
    v.sort_unstable();
    v
}

/// Serialize every event to a journal line and decode each back through the
/// canonical decoder — the bytes the storage layer would persist and replay.
fn serde_round_trip_journal(log: &[Event]) -> Vec<Event> {
    log.iter()
        .map(|e| {
            let line = serde_json::to_string(e).expect("event serializes to a journal line");
            decode_event_json(&line).expect("journal line decodes")
        })
        .collect()
}

/// Serialize the engine's snapshot and restore a fresh engine from the decoded
/// bytes — the Raft state-machine restart path.
fn serde_round_trip_snapshot(engine: &Engine) -> Engine {
    let json = serde_json::to_string(&engine.snapshot()).expect("snapshot serializes");
    let snapshot = serde_json::from_str(&json).expect("snapshot deserializes");
    Engine::from_snapshot(snapshot)
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

/// Instance timer (event sub-process `R2/PT1M`): consume the first fire, then
/// restart through the serialized journal and through the serialized snapshot.
/// Both resumed engines must carry exactly one remaining fire (`repetitions ==
/// Some(1)`), fire it, and never re-arm.
#[test]
fn bounded_cycle_instance_timer_survives_serde_restart() {
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
    assert_eq!(armed(&engine), vec![120_000], "first fire re-arms once");
    let armed_timer = engine
        .timers()
        .into_iter()
        .find(|t| t.state == TimerState::Created)
        .expect("one armed timer");
    assert_eq!(armed_timer.repetitions, Some(1));

    // Restart 1: replay the serialized journal. Restart 2: restore the
    // serialized snapshot.
    let mut from_journal = Engine::replay(serde_round_trip_journal(&log));
    let mut from_snapshot = serde_round_trip_snapshot(&engine);

    for e in [&mut engine, &mut from_journal, &mut from_snapshot] {
        assert_eq!(armed(e), vec![120_000]);
        let fired = e.trigger_timers(120_000);
        assert!(fired.iter().any(
            |ev| matches!(ev, Event::TimerTriggered { element_id, .. } if element_id == "esp_start")
        ));
        assert!(
            !fired
                .iter()
                .any(|ev| matches!(ev, Event::TimerCreated { .. })),
            "R2 fires exactly twice — no third TimerCreated after a serde restart"
        );
        assert!(armed(e).is_empty());
        assert!(e.trigger_timers(1_000_000).is_empty());
    }
}

fn start_timer_process() -> ProcessDefinition {
    parse(
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
    )
}

fn the_start_timer(engine: &Engine, key: Key) -> &nanobpmn_engine_core::StartTimer {
    engine
        .state()
        .start_timers
        .get(&key)
        .expect("start timer present")
}

/// Process-start timer (`R2/PT10S`): consume the first fire, restart through
/// the serialized journal and the serialized snapshot, and verify each resumed
/// engine creates exactly one more instance and then disarms.
#[test]
fn bounded_cycle_start_timer_survives_serde_restart() {
    let mut engine = Engine::new();
    let mut log = engine
        .apply_command_at(Command::DeployProcess(start_timer_process()), 1_000)
        .unwrap();
    log.extend(engine.trigger_timers(11_000));
    assert_eq!(engine.state().instances.len(), 1);
    let (&timer_key, timer) = engine.state().start_timers.iter().next().unwrap();
    assert_eq!(timer.due_at, Some(21_000));
    assert_eq!(timer.repetitions, Some(1));

    let mut from_journal = Engine::replay(serde_round_trip_journal(&log));
    let mut from_snapshot = serde_round_trip_snapshot(&engine);

    for e in [&mut engine, &mut from_journal, &mut from_snapshot] {
        assert_eq!(the_start_timer(e, timer_key).due_at, Some(21_000));
        e.trigger_timers(21_000);
        assert_eq!(e.state().instances.len(), 2, "second and final fire");
        let timer = the_start_timer(e, timer_key);
        assert_eq!(timer.due_at, None, "R2 start cycle disarms after 2 fires");
        assert_eq!(timer.repetitions, Some(0));
        assert!(e.trigger_timers(1_000_000).is_empty());
        assert_eq!(e.state().instances.len(), 2);
    }
}
