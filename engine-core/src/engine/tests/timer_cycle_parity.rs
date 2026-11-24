//! Timer-cycle parity with Zeebe (#1878): cron cycles, ISO-8601 repeating
//! intervals with a start date, the FEEL `cycle()` built-in, cluster variables
//! in a timer-start expression, and the reschedule semantics of
//! `TimerTriggerProcessor` — for timer start events, non-interrupting boundary
//! timers and event sub-process timers. Cron is evaluated in UTC (see
//! `engine/timer_cycle.rs`).
use std::collections::BTreeMap;

use super::*;
use crate::cluster_vars::ClusterVariableKind;
use crate::feel::temporal::days_from_civil;

fn parse(xml: &str) -> ProcessDefinition {
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// Epoch millis of a UTC civil date-time.
fn at(y: i32, mo: u32, d: u32, h: u64, mi: u64) -> u64 {
    let day = u64::try_from(days_from_civil(y, mo, d)).unwrap();
    (day * 86_400 + h * 3_600 + mi * 60) * 1_000
}

const HOUR: u64 = 3_600_000;

fn start_process(id: &str, cycle: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
  <bpmn:process id="{id}" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:timerEventDefinition><bpmn:timeCycle>{cycle}</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#
    ))
}

fn start_due(engine: &Engine) -> Option<u64> {
    let timers: Vec<_> = engine.state().start_timers.values().collect();
    assert_eq!(timers.len(), 1, "exactly one start timer: {timers:?}");
    timers[0].due_at
}

fn deploy_at(engine: &mut Engine, def: ProcessDefinition, now: u64) -> Vec<Event> {
    engine
        .apply_command_at(Command::DeployProcess(def), now)
        .unwrap()
}

/// Deploying `cycle` on a timer start event at `now` fails with Zeebe's
/// cycle-extraction message.
fn deploy_rejected(cycle: &str) -> String {
    let mut engine = Engine::new();
    let err = engine
        .apply_command_at(Command::DeployProcess(start_process("p", cycle)), 0)
        .unwrap_err();
    match err {
        EngineError::StartEventExpressionInvalid { reason, .. } => reason,
        other => panic!("{cycle}: unexpected error {other:?}"),
    }
}

// --- timer start events ------------------------------------------------------

#[test]
fn cron_start_timer_fires_on_schedule_and_skips_missed_fires() {
    let mut engine = Engine::new();
    let log = deploy_at(
        &mut engine,
        start_process("sweep", "0 0 */12 * * ?"),
        at(2026, 1, 1, 5, 30),
    );
    assert_eq!(start_due(&engine), Some(at(2026, 1, 1, 12, 0)));
    // Unbounded: a cron cycle carries no repetition count.
    assert!(log.iter().any(|e| matches!(
        e,
        Event::ProcessStartTimerArmed {
            repetitions: None,
            repeating: true,
            ..
        }
    )));

    engine.trigger_timers(at(2026, 1, 1, 12, 0));
    assert_eq!(engine.state().instances.len(), 1);
    assert_eq!(start_due(&engine), Some(at(2026, 1, 2, 0, 0)));

    // The engine is not driven for two days: Zeebe fires the overdue timer
    // once and reschedules from NOW (CronTimer.getDueDate), skipping the
    // missed matches.
    engine.trigger_timers(at(2026, 1, 3, 13, 0));
    assert_eq!(engine.state().instances.len(), 2);
    assert_eq!(start_due(&engine), Some(at(2026, 1, 4, 0, 0)));
}

#[test]
fn cron_start_timer_crosses_month_and_leap_day_boundaries() {
    // "last day of the month at 23:00" — 30/31-day months and a leap February.
    let mut engine = Engine::new();
    deploy_at(
        &mut engine,
        start_process("eom", "0 0 23 L * ?"),
        at(2028, 1, 31, 23, 30),
    );
    let mut seen = Vec::new();
    for _ in 0..3 {
        let due = start_due(&engine).unwrap();
        seen.push(due);
        engine.trigger_timers(due);
    }
    assert_eq!(
        seen,
        vec![
            at(2028, 2, 29, 23, 0),
            at(2028, 3, 31, 23, 0),
            at(2028, 4, 30, 23, 0)
        ]
    );
    assert_eq!(engine.state().instances.len(), 3);
}

#[test]
fn cron_is_evaluated_in_utc_across_a_dst_transition() {
    // Europe/US DST switches happen on these Sundays; UTC has no transition,
    // so a daily 02:30 cron keeps a fixed 24h spacing (Zeebe on a UTC broker).
    let mut engine = Engine::new();
    deploy_at(
        &mut engine,
        start_process("daily", "0 30 2 * * *"),
        at(2026, 3, 28, 12, 0),
    );
    let first = start_due(&engine).unwrap();
    assert_eq!(first, at(2026, 3, 29, 2, 30));
    engine.trigger_timers(first);
    assert_eq!(start_due(&engine), Some(first + 24 * HOUR));
}

#[test]
fn iso_cycle_with_future_start_first_fires_at_start() {
    let mut engine = Engine::new();
    let start = at(2026, 1, 1, 6, 0);
    deploy_at(
        &mut engine,
        start_process("p", "R/2026-01-01T06:00:00Z/PT12H"),
        start - 10 * HOUR,
    );
    assert_eq!(start_due(&engine), Some(start));
    engine.trigger_timers(start);
    assert_eq!(start_due(&engine), Some(start + 12 * HOUR));
    engine.trigger_timers(start + 12 * HOUR);
    assert_eq!(start_due(&engine), Some(start + 24 * HOUR));
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn iso_cycle_with_past_start_fires_immediately() {
    let mut engine = Engine::new();
    let now = at(2026, 6, 1, 9, 0);
    deploy_at(
        &mut engine,
        start_process("p", "R/2026-01-01T06:00:00Z/PT12H"),
        now,
    );
    // Interval.toEpochMilli: max(start, from).
    assert_eq!(start_due(&engine), Some(now));
    engine.trigger_timers(now);
    assert_eq!(start_due(&engine), Some(now + 12 * HOUR));
}

#[test]
fn bounded_iso_cycle_with_start_counts_the_first_fire_at_start() {
    let mut engine = Engine::new();
    let start = at(2026, 1, 1, 6, 0);
    deploy_at(
        &mut engine,
        start_process("p", "R3/2026-01-01T07:00:00+01:00/PT1H"),
        0,
    );
    assert_eq!(start_due(&engine), Some(start));
    for k in 0..3 {
        engine.trigger_timers(start + k * HOUR);
    }
    assert_eq!(engine.state().instances.len(), 3);
    assert_eq!(start_due(&engine), None, "R3 fires exactly three times");
    assert!(engine.trigger_timers(start + 100 * HOUR).is_empty());
}

#[test]
fn feel_cycle_builtin_drives_a_start_timer() {
    let mut engine = Engine::new();
    deploy_at(
        &mut engine,
        start_process("p", r#"=cycle(duration("PT12H"))"#),
        1_000,
    );
    assert_eq!(start_due(&engine), Some(1_000 + 12 * HOUR));
    engine.trigger_timers(1_000 + 12 * HOUR);
    assert_eq!(start_due(&engine), Some(1_000 + 24 * HOUR));

    let mut engine = Engine::new();
    deploy_at(
        &mut engine,
        start_process("p", r#"=cycle(2, duration("PT1M"))"#),
        0,
    );
    engine.trigger_timers(60_000);
    engine.trigger_timers(120_000);
    assert_eq!(engine.state().instances.len(), 2);
    assert_eq!(start_due(&engine), None, "cycle(2, …) is R2");
}

#[test]
fn feel_cron_string_drives_a_start_timer() {
    let mut engine = Engine::new();
    deploy_at(
        &mut engine,
        start_process("p", r#"="0 0 * * * *""#),
        at(2026, 1, 1, 0, 10),
    );
    assert_eq!(start_due(&engine), Some(at(2026, 1, 1, 1, 0)));
}

#[test]
fn cluster_variables_resolve_in_a_timer_start_expression() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::CreateClusterVariable {
            tenant_id: None,
            name: "sweepCycle".to_string(),
            value: Value::Str("R/PT6H".to_string()),
            kind: ClusterVariableKind::Json,
            metadata: BTreeMap::new(),
            distributed: false,
        })
        .unwrap();
    deploy_at(&mut engine, start_process("p", "=sweepCycle"), 0);
    assert_eq!(start_due(&engine), Some(6 * HOUR));
    engine.trigger_timers(6 * HOUR);
    assert_eq!(start_due(&engine), Some(12 * HOUR));
}

#[test]
fn redeploy_replaces_the_start_timer_with_the_new_schedule() {
    let mut engine = Engine::new();
    deploy_at(
        &mut engine,
        start_process("p", "0 0 */12 * * ?"),
        at(2026, 1, 1, 1, 0),
    );
    assert_eq!(start_due(&engine), Some(at(2026, 1, 1, 12, 0)));
    deploy_at(
        &mut engine,
        start_process("p", "R/2026-01-01T03:00:00Z/PT1H"),
        at(2026, 1, 1, 2, 0),
    );
    // Only the new version's timer remains armed.
    assert_eq!(start_due(&engine), Some(at(2026, 1, 1, 3, 0)));
    engine.trigger_timers(at(2026, 1, 1, 3, 0));
    assert_eq!(start_due(&engine), Some(at(2026, 1, 1, 4, 0)));
    assert_eq!(engine.state().instances.len(), 1);
}

#[test]
fn invalid_cycles_are_rejected_at_deploy() {
    for (cycle, shown) in [
        ("0 0 */12 * *", "0 0 */12 * *"),
        ("60 * * * * *", "60 * * * * *"),
        ("R/PT1H/2026-01-01T06:00:00Z", "R/PT1H/2026-01-01T06:00:00Z"),
        ("R/2026-01-01T06:00:00/PT1H", "R/2026-01-01T06:00:00/PT1H"),
    ] {
        let reason = deploy_rejected(cycle);
        assert!(
            reason.contains("to be a valid cycle") && reason.contains(shown),
            "{cycle}: {reason}"
        );
    }
}

#[test]
fn start_timer_schedule_survives_replay() {
    let mut engine = Engine::new();
    let mut log = deploy_at(
        &mut engine,
        start_process("p", "0 0 */12 * * ?"),
        at(2026, 1, 1, 5, 0),
    );
    log.extend(engine.trigger_timers(at(2026, 1, 1, 12, 0)));
    let mut recovered = Engine::replay(log);
    for e in [&mut engine, &mut recovered] {
        assert_eq!(start_due(e), Some(at(2026, 1, 2, 0, 0)));
        e.trigger_timers(at(2026, 1, 2, 0, 0));
        assert_eq!(start_due(e), Some(at(2026, 1, 2, 12, 0)));
    }
}

// --- in-instance cycles ------------------------------------------------------

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
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="tick" attachedToRef="work" cancelActivity="false">
      <bpmn:timerEventDefinition><bpmn:timeCycle>{cycle}</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="ticked" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="done" />
    <bpmn:sequenceFlow id="f3" sourceRef="tick" targetRef="ticked" />
  </bpmn:process>
</bpmn:definitions>"#
    ))
}

fn esp_process(cycle: &str) -> ProcessDefinition {
    parse(&format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="ticker" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
    <bpmn:subProcess id="esp" triggeredByEvent="true">
      <bpmn:startEvent id="esp_start" isInterrupting="false">
        <bpmn:timerEventDefinition><bpmn:timeCycle>{cycle}</bpmn:timeCycle></bpmn:timerEventDefinition>
      </bpmn:startEvent>
      <bpmn:endEvent id="esp_end" />
      <bpmn:sequenceFlow id="ef1" sourceRef="esp_start" targetRef="esp_end" />
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>"#
    ))
}

fn assert_in_instance_schedule(def: ProcessDefinition, created_at: u64, expected: &[u64]) {
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command_at(Command::create_instance("ticker"), created_at)
        .unwrap();
    for (i, due) in expected.iter().enumerate() {
        assert_eq!(armed(&engine), vec![*due], "fire #{i}");
        engine.trigger_timers(*due);
    }
}

#[test]
fn cron_boundary_and_event_subprocess_timers() {
    let t0 = at(2026, 1, 1, 5, 30);
    let expected = [
        at(2026, 1, 1, 12, 0),
        at(2026, 1, 2, 0, 0),
        at(2026, 1, 2, 12, 0),
    ];
    assert_in_instance_schedule(boundary_process("0 0 */12 * * ?"), t0, &expected);
    assert_in_instance_schedule(esp_process("0 0 */12 * * ?"), t0, &expected);
    assert_in_instance_schedule(boundary_process(r#"="0 0 */12 * * ?""#), t0, &expected);
}

#[test]
fn iso_start_boundary_and_event_subprocess_timers() {
    let start = at(2026, 1, 1, 6, 0);
    let expected = [start, start + 12 * HOUR, start + 24 * HOUR];
    let cycle = "R/2026-01-01T06:00:00Z/PT12H";
    assert_in_instance_schedule(boundary_process(cycle), start - HOUR, &expected);
    assert_in_instance_schedule(esp_process(cycle), start - HOUR, &expected);
}

#[test]
fn feel_cycle_builtin_boundary_timer() {
    let expected = [1_000 + 60_000, 1_000 + 120_000];
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(boundary_process(
            r#"=cycle(2, duration("PT1M"))"#,
        )))
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("ticker"), 1_000)
        .unwrap();
    for due in expected {
        assert_eq!(armed(&engine), vec![due]);
        engine.trigger_timers(due);
    }
    assert!(armed(&engine).is_empty(), "cycle(2, …) fires exactly twice");
}

#[test]
fn overdue_interval_cycle_reschedules_from_now() {
    // TimerTriggerProcessor.refreshTimer + Interval.toEpochMilli: the next due
    // date is max(previous due + interval, now), so an engine that was not
    // driven for many periods does not replay every missed fire.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(boundary_process("R/PT5S")))
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("ticker"), 0)
        .unwrap();
    assert_eq!(armed(&engine), vec![5_000]);
    let fired = engine.trigger_timers(1_000_000);
    let fires = fired
        .iter()
        .filter(|e| matches!(e, Event::TimerTriggered { .. }))
        .count();
    assert!(fires <= 2, "missed fires collapse (Zeebe parity): {fires}");
    let next = armed(&engine);
    assert_eq!(next.len(), 1);
    // Re-armed at max(5s + 5s, now) = now: due again on the next tick.
    assert_eq!(next, vec![1_000_000]);
    engine.trigger_timers(1_000_000);
    assert_eq!(armed(&engine), vec![1_005_000]);
}

#[test]
fn builder_cycle_without_timer_def_collapses_missed_fires() {
    // A ProcessBuilder cycle carries no TimerDef; its static-period fallback
    // must clamp to now exactly like a declared interval cycle.
    let def = ProcessBuilder::new("built")
        .timer_start_event_cycle("start", 5_000)
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    deploy_at(&mut engine, def, 0);
    assert_eq!(start_due(&engine), Some(5_000));
    engine.trigger_timers(1_000_000);
    assert_eq!(start_due(&engine), Some(1_000_000));
    engine.trigger_timers(1_000_000);
    assert_eq!(start_due(&engine), Some(1_005_000));
}

#[test]
fn invalid_start_expression_fallback_collapses_missed_fires() {
    // The cluster variable backing the cycle is removed after deploy: the
    // fixed-period fallback keeps the schedule but never re-arms before now.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::CreateClusterVariable {
            tenant_id: None,
            name: "tickCycle".to_string(),
            value: Value::Str("R/PT5S".to_string()),
            kind: ClusterVariableKind::Json,
            metadata: BTreeMap::new(),
            distributed: false,
        })
        .unwrap();
    deploy_at(&mut engine, start_process("p", "=tickCycle"), 0);
    assert_eq!(start_due(&engine), Some(5_000));
    engine
        .apply_command(Command::DeleteClusterVariable {
            tenant_id: None,
            name: "tickCycle".to_string(),
            distributed: false,
        })
        .unwrap();
    engine.trigger_timers(1_000_000);
    assert_eq!(start_due(&engine), Some(1_000_000));
}
