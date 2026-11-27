//! The `event-gateway-runtime-v1` capability latch (#2004, the
//! `message-correlation-ack-v1` pattern of #1949): before the latch an
//! event-based gateway runs the legacy #1676 race path; the latch is recorded
//! in the log once the replicated envelope enables the capability (every
//! member advertises it) and no legacy race is left; after it, the gateway
//! waits on its own subscriptions and its trigger activates only the winner.
use super::*;

fn race_def() -> ProcessDefinition {
    ProcessBuilder::new("evt")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end1")
        .connect("onM2", "end2")
        .build()
        .unwrap()
}

/// An engine with the race deployed, under the given envelope bit, plus the
/// journal so far.
fn engine_with(envelope: bool) -> (Engine, Vec<Event>) {
    let mut engine = Engine::new();
    engine.set_event_gateway_runtime(envelope);
    let journal = engine
        .apply_command(Command::DeployProcess(race_def()))
        .unwrap();
    (engine, journal)
}

fn create(engine: &mut Engine) -> (Key, Vec<Event>) {
    let events = engine
        .apply_command(Command::create_instance_with(
            "evt",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    (inst, events)
}

fn latches(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::EventGatewayRuntimeActivated { .. }))
        .count()
}

/// A mixed-version cluster (envelope `false`: some member lacks the
/// capability) keeps the legacy race: every catch armed as an active element
/// tagged with its gateway activation, and no latch recorded.
#[test]
fn mixed_version_envelope_keeps_the_legacy_race() {
    let (mut engine, _) = engine_with(false);
    let (inst, events) = create(&mut engine);
    assert_eq!(latches(&events), 0);
    assert!(!engine.event_gateway_runtime_latched());
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::EventGatewaySubscriptionsOpened { .. })));
    let instance = &engine.state().instances[&inst];
    let mut active: Vec<&str> = instance.active.values().map(String::as_str).collect();
    active.sort_unstable();
    assert_eq!(active, vec!["onM1", "onM2"], "legacy arms every catch");
    assert_eq!(instance.races.len(), 2, "each catch carries its race");
    // The winner withdraws its sibling (legacy deferred choice).
    let events = engine
        .apply_command(Command::correlate_message("M2", "k"))
        .unwrap();
    assert!(events.iter().any(|e| matches!(e,
        Event::ElementCompleted { element_id, .. } if element_id == "onM1")));
    assert!(events.iter().any(|e| matches!(e,
        Event::ProcessInstanceCompleted { instance_key } if *instance_key == inst)));
    assert!(!engine.event_gateway_runtime_latched());
}

/// Once every member advertises the capability, the first gateway activation
/// with no legacy race left records the latch exactly once, then runs the
/// gateway-owned trigger path; a later disabled envelope (a peer transiently
/// unreachable) is ignored.
#[test]
fn enabled_envelope_latches_once_then_runs_the_new_path() {
    let (mut engine, _) = engine_with(false);
    create(&mut engine);
    engine.set_event_gateway_runtime(true);
    // The pending legacy race blocks the latch until it settles.
    let (_, events) = create(&mut engine);
    assert_eq!(latches(&events), 0, "legacy race state left: no latch");
    engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    assert!(engine
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());
    let (inst, events) = create(&mut engine);
    assert_eq!(latches(&events), 1);
    assert!(engine.event_gateway_runtime_latched());
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::EventGatewaySubscriptionsOpened { .. })));
    let instance = &engine.state().instances[&inst];
    assert_eq!(
        instance.active.values().collect::<Vec<_>>(),
        vec!["gw"],
        "the gateway waits"
    );
    assert!(instance.races.is_empty());

    engine.set_event_gateway_runtime(false);
    let (_, events) = create(&mut engine);
    assert_eq!(latches(&events), 0, "the latch is recorded once");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::EventGatewaySubscriptionsOpened { .. })),
        "a later disabled envelope is ignored once latched"
    );
}

/// The latch is durable: journal replay restores it, and a journal written
/// before it replays under legacy semantics — the replayed state is exactly
/// the live legacy engine's, with no latch.
#[test]
fn latch_survives_replay_and_pre_latch_journals_replay_legacy() {
    let (mut live, mut journal) = engine_with(false);
    journal.extend(create(&mut live).1);
    let replayed = Engine::replay(journal.clone());
    assert!(!replayed.event_gateway_runtime_latched());
    assert_eq!(replayed.state(), live.state(), "pre-latch replay is legacy");
    assert_eq!(
        replayed.state().legacy_event_gateway_race_instances().len(),
        1
    );
    assert!(replayed
        .state()
        .refused_legacy_event_gateway_race_instances()
        .is_empty());

    // Settle the race, then latch on the next gateway activation.
    journal.extend(
        live.apply_command(Command::correlate_message("M1", "k"))
            .unwrap(),
    );
    live.set_event_gateway_runtime(true);
    journal.extend(create(&mut live).1);
    assert!(live.event_gateway_runtime_latched());
    let replayed = Engine::replay(journal);
    assert!(replayed.event_gateway_runtime_latched());
    assert_eq!(replayed.state(), live.state());
}

/// A latched state that still holds legacy race state (only reachable by a
/// corrupt or hand-edited log, since the latch waits for the last legacy race
/// to settle) is refused by the loaders' shared check.
#[test]
fn legacy_race_state_after_the_latch_is_refused() {
    let (mut live, mut journal) = engine_with(false);
    let (inst, created) = create(&mut live);
    journal.extend(created);
    journal.push(Event::EventGatewayRuntimeActivated { partition: 0 });
    let replayed = Engine::replay(journal);
    assert_eq!(
        replayed
            .state()
            .refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
}

/// #2047 review: a pre-latch (legacy) gateway paused on its `end` listener has
/// no `races` entry yet, but completing the listener arms its catches as a
/// legacy race. Recording the latch meanwhile would leave a latched partition
/// holding legacy race state, which the next restart refuses. The latch must
/// wait until no legacy gateway is still in flight.
#[test]
fn latch_waits_for_a_legacy_gateway_paused_on_its_end_listener() {
    let listened = ProcessBuilder::new("evtl")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end1")
        .connect("onM2", "end2")
        .with_listeners("gw", Vec::new(), vec![el(ListenerEventType::End, "gw-end")])
        .build()
        .unwrap();
    let (mut engine, _) = engine_with(false);
    engine
        .apply_command(Command::DeployProcess(listened))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "evtl",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    assert!(
        engine.pending_jobs().iter().any(|j| j.job_type == "gw-end"),
        "the legacy gateway parks on its end listener"
    );

    // Every member now advertises the capability, but the parked legacy
    // gateway can still arm a race: no cutover yet.
    engine.set_event_gateway_runtime(true);
    let (_, events) = create(&mut engine);
    assert_eq!(
        latches(&events),
        0,
        "no latch while a legacy gateway is in flight"
    );
    assert!(!engine.event_gateway_runtime_latched());

    // Draining the listener arms the legacy race on an unlatched partition.
    complete_one(&mut engine, "gw-end");
    assert!(!engine.event_gateway_runtime_latched());
    assert!(
        engine
            .state
            .refused_legacy_event_gateway_race_instances()
            .is_empty(),
        "an unlatched partition never refuses its legacy races"
    );
}

/// Cold spill must not hide a legacy cutover blocker (#2004, #2047 review):
/// the cutover decision scans RESIDENT instances only, so a cold-spilled
/// legacy race (or a legacy gateway still in flight) would let a cold-spilling
/// replica latch where a resident one keeps the legacy path — divergence, and
/// a latched snapshot that refuses the race once folded back. Legacy blockers
/// therefore stay resident until they settle: neither offered as cold-spill
/// candidates nor accepted by `snapshot_instance`.
#[test]
fn legacy_cutover_blockers_are_never_cold_spilled() {
    // A legacy race (armed catches tagged with their gateway).
    let (mut engine, _) = engine_with(false);
    let (race, _) = create(&mut engine);
    assert!(!engine
        .state
        .legacy_event_gateway_race_instances()
        .is_empty());
    assert!(!engine.cold_spillable_instances(usize::MAX).contains(&race));
    assert_eq!(engine.cold_spillable_count(), 0);
    assert!(engine.snapshot_instance(race).is_none());

    // So a resident replica and one that tried to spill decide alike.
    engine.set_event_gateway_runtime(true);
    let (_, events) = create(&mut engine);
    assert_eq!(
        latches(&events),
        0,
        "the resident legacy race blocks the latch"
    );

    // A legacy gateway paused on its end listener (in flight, no race yet).
    let listened = ProcessBuilder::new("evtl")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end1")
        .connect("onM2", "end2")
        .with_listeners("gw", Vec::new(), vec![el(ListenerEventType::End, "gw-end")])
        .build()
        .unwrap();
    let (mut engine, _) = engine_with(false);
    engine
        .apply_command(Command::DeployProcess(listened))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "evtl",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let paused = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.state.legacy_event_gateway_in_flight(0));
    assert!(!engine
        .cold_spillable_instances(usize::MAX)
        .contains(&paused));
    assert!(engine.snapshot_instance(paused).is_none());
}

/// A pre-#1676 snapshot arms gateway catches WITHOUT race tags (#2047
/// review). Such a race-less legacy catch still blocks the cutover and is
/// pinned against cold spill, so a new-runtime winner never coexists with a
/// legacy catch whose scope-only withdrawal could cancel it.
#[test]
fn raceless_legacy_catch_blocks_the_latch_and_cold_spill() {
    let (mut engine, _) = engine_with(false);
    let (legacy, _) = create(&mut engine);
    // Strip the race tags, as a pre-#1676 snapshot carries none.
    engine
        .state
        .instances
        .get_mut(&legacy)
        .unwrap()
        .races
        .clear();
    assert!(engine
        .state
        .legacy_event_gateway_race_instances()
        .is_empty());
    let instance = engine.state.instances[&legacy].clone();
    assert!(engine.state.is_legacy_event_gateway_blocker(&instance));
    assert!(engine.state.legacy_event_gateway_in_flight(0));
    assert!(!engine
        .cold_spillable_instances(usize::MAX)
        .contains(&legacy));

    engine.set_event_gateway_runtime(true);
    let (_, events) = create(&mut engine);
    assert_eq!(
        latches(&events),
        0,
        "no latch while a race-less catch is armed"
    );
    assert!(!engine.event_gateway_runtime_latched());
}

/// Strips the #1676 race tags from a journal: exactly what a pre-#1676
/// journal carries (race-less gateway catches).
fn strip_race_tags(journal: &mut [Event]) {
    for event in journal.iter_mut() {
        if let Event::ElementActivating { race, .. } = event {
            *race = None;
        }
    }
}

/// Snapshot round-trip through serde, as the loaders see it.
#[cfg(feature = "serde")]
fn reload(engine: &Engine) -> Engine {
    let json = serde_json::to_string(&engine.snapshot()).unwrap();
    Engine::from_snapshot(serde_json::from_str(&json).unwrap())
}

/// #2047 operator review: the load-time refusal uses the SAME predicate as
/// the latch decision, so a latched log holding a race-less pre-#1676
/// gateway catch (no `races` entry) is refused, after journal replay and at
/// snapshot load — never silently run under the new runtime.
#[test]
fn latched_raceless_legacy_catch_is_refused_on_replay_and_snapshot_load() {
    let (mut live, mut journal) = engine_with(false);
    let (inst, created) = create(&mut live);
    journal.extend(created);
    strip_race_tags(&mut journal);
    journal.push(Event::EventGatewayRuntimeActivated { partition: 0 });
    let replayed = Engine::replay(journal);
    assert!(replayed.event_gateway_runtime_latched());
    assert!(replayed
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());
    assert_eq!(
        replayed
            .state()
            .refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
    #[cfg(feature = "serde")]
    assert_eq!(
        reload(&replayed)
            .state()
            .refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
}

/// #2047 operator review: a latched log holding a legacy gateway still in
/// flight (paused on its end listener, no race armed yet) is refused, after
/// journal replay and at snapshot load.
#[test]
fn latched_in_flight_legacy_gateway_is_refused_on_replay_and_snapshot_load() {
    let listened = ProcessBuilder::new("evtl")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end1")
        .connect("onM2", "end2")
        .with_listeners("gw", Vec::new(), vec![el(ListenerEventType::End, "gw-end")])
        .build()
        .unwrap();
    let (mut live, mut journal) = engine_with(false);
    journal.extend(
        live.apply_command(Command::DeployProcess(listened))
            .unwrap(),
    );
    let events = live
        .apply_command(Command::create_instance_with(
            "evtl",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    journal.extend(events);
    journal.push(Event::EventGatewayRuntimeActivated { partition: 0 });
    let replayed = Engine::replay(journal);
    assert!(replayed.event_gateway_runtime_latched());
    assert!(replayed
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());
    assert_eq!(
        replayed
            .state()
            .refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
    #[cfg(feature = "serde")]
    assert_eq!(
        reload(&replayed)
            .state()
            .refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
}

/// The widened refusal must not refuse VALID latched state: a new-runtime
/// gateway waiting on its subscriptions, or paused on its start/end
/// listeners, is not legacy.
#[test]
fn latched_new_runtime_gateways_are_not_refused() {
    let listened = ProcessBuilder::new("evtl")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end1")
        .connect("onM2", "end2")
        .with_listeners(
            "gw",
            vec![el(ListenerEventType::Start, "gw-start")],
            vec![el(ListenerEventType::End, "gw-end")],
        )
        .build()
        .unwrap();
    let (mut engine, _) = engine_with(true);
    create(&mut engine);
    assert!(engine.event_gateway_runtime_latched());
    engine
        .apply_command(Command::DeployProcess(listened))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "evtl",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let refused = |e: &Engine| e.state().refused_legacy_event_gateway_race_instances();
    assert!(refused(&engine).is_empty(), "paused on its start listener");
    complete_one(&mut engine, "gw-start");
    assert!(refused(&engine).is_empty(), "waiting on its subscriptions");
    engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    assert!(
        engine.pending_jobs().iter().any(|j| j.job_type == "gw-end"),
        "the triggered gateway parks on its end listener"
    );
    assert!(refused(&engine).is_empty(), "paused on its end listener");
    complete_one(&mut engine, "gw-end");
    assert!(refused(&engine).is_empty(), "the winner catch is active");
}

/// Applies `cmd` to `engine`, recording its events in `journal`.
fn journaled(engine: &mut Engine, journal: &mut Vec<Event>, cmd: Command) -> Vec<Event> {
    let events = engine.apply_command(cmd).unwrap();
    journal.extend(events.iter().cloned());
    events
}

/// A latched engine plus its full journal (deploy + the latching instance).
fn latched_with_journal() -> (Engine, Vec<Event>) {
    let (mut engine, mut journal) = engine_with(true);
    journaled(
        &mut engine,
        &mut journal,
        Command::create_instance_with("evt", vars(&[("key", Value::Str("k0".into()))])),
    );
    assert!(engine.event_gateway_runtime_latched());
    (engine, journal)
}

/// Every recovery path a latched partition takes: the live engine, journal-
/// only replay and (with serde) a snapshot round-trip. Each must load
/// without refusal.
fn recovered(live: &Engine, journal: &[Event]) -> Vec<Engine> {
    let replayed = Engine::replay(journal.to_vec());
    assert!(replayed.event_gateway_runtime_latched());
    #[allow(unused_mut)]
    let mut out = vec![replayed];
    #[cfg(feature = "serde")]
    out.push(reload(live));
    let _ = live;
    for engine in &out {
        assert!(
            engine
                .state()
                .refused_legacy_event_gateway_race_instances()
                .is_empty(),
            "latched new-runtime state must load"
        );
    }
    out
}

fn assert_completed(engine: &Engine, inst: Key) {
    assert_eq!(
        engine.instance(inst).unwrap().state,
        crate::state::ProcessInstanceState::Completed
    );
}

/// #2047 review: on a LATCHED partition, `ModifyInstance` activating an
/// event-based gateway with element-local variables emits `ElementActivated`
/// before its start listener runs (clearing `activating`). The gateway paused
/// on that listener is new-runtime state: it must survive snapshot load and
/// journal-only replay, and the instance must then complete.
#[test]
fn latched_modified_gateway_paused_on_start_listener_recovers() {
    let listened = ProcessBuilder::new("evtl")
        .start_event("start")
        .user_task("hold")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "hold")
        .connect("hold", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end1")
        .connect("onM2", "end2")
        .with_listeners(
            "gw",
            vec![el(ListenerEventType::Start, "gw-start")],
            Vec::new(),
        )
        .build()
        .unwrap();
    let (mut live, mut journal) = latched_with_journal();
    journaled(&mut live, &mut journal, Command::DeployProcess(listened));
    let events = journaled(
        &mut live,
        &mut journal,
        Command::create_instance_with("evtl", vars(&[("key", Value::Str("k".into()))])),
    );
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    let hold = live.state().instances[&inst]
        .active
        .iter()
        .find_map(|(k, id)| (id == "hold").then_some(*k))
        .unwrap();
    journaled(
        &mut live,
        &mut journal,
        Command::modify_instance(
            inst,
            vec![crate::command::ActivateElementInstruction {
                element_id: "gw".into(),
                variable_instructions: vec![crate::event::VariableInstruction {
                    variables: vars(&[("local", Value::Str("v".into()))]),
                    scope_id: Some("gw".into()),
                }],
                ..Default::default()
            }],
            vec![hold],
        ),
    );
    assert!(live.pending_jobs().iter().any(|j| j.job_type == "gw-start"));
    assert!(live
        .state()
        .refused_legacy_event_gateway_race_instances()
        .is_empty());
    for mut engine in recovered(&live, &journal) {
        complete_one(&mut engine, "gw-start");
        engine
            .apply_command(Command::correlate_message("M2", "k"))
            .unwrap();
        assert_completed(&engine, inst);
    }
}

/// #2047 review: on a LATCHED partition, a catch activated directly onto a
/// gateway-owned catch by `ModifyInstance` carries neither a race tag nor a
/// gateway trigger; it is still new-runtime state and must recover.
#[test]
fn latched_modified_gateway_owned_catch_recovers() {
    let (mut live, mut journal) = latched_with_journal();
    let events = journaled(
        &mut live,
        &mut journal,
        Command::create_instance_with("evt", vars(&[("key", Value::Str("k".into()))])),
    );
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    let gw = live.state().instances[&inst]
        .active
        .iter()
        .find_map(|(k, id)| (id == "gw").then_some(*k))
        .unwrap();
    journaled(
        &mut live,
        &mut journal,
        Command::modify_instance(
            inst,
            vec![crate::command::ActivateElementInstruction {
                element_id: "onM1".into(),
                ..Default::default()
            }],
            vec![gw],
        ),
    );
    assert!(live.state().instances[&inst]
        .active
        .values()
        .any(|id| id == "onM1"));
    for mut engine in recovered(&live, &journal) {
        engine
            .apply_command(Command::correlate_message("M1", "k"))
            .unwrap();
        assert_completed(&engine, inst);
    }
}

/// #2047 review: on a LATCHED partition, migrating an ordinary waiting catch
/// onto a gateway-owned catch leaves a race-less, trigger-less catch; it is
/// new-runtime state and must recover.
#[test]
fn latched_catch_migrated_onto_gateway_owned_catch_recovers() {
    let source = ProcessBuilder::new("plain")
        .start_event("start")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .end_event("end1")
        .connect("start", "onM1")
        .connect("onM1", "end1")
        .build()
        .unwrap();
    let (mut live, mut journal) = latched_with_journal();
    journaled(&mut live, &mut journal, Command::DeployProcess(source));
    // A distinct target (re-deploying `race_def` unchanged is a no-op).
    let mut target = race_def();
    target.id = "evt2".into();
    let target_key = journaled(&mut live, &mut journal, Command::DeployProcess(target))
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap();
    let events = journaled(
        &mut live,
        &mut journal,
        Command::create_instance_with("plain", vars(&[("key", Value::Str("k".into()))])),
    );
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    journaled(
        &mut live,
        &mut journal,
        Command::migrate_instance(inst, target_key, vec![("onM1".into(), "onM1".into())]),
    );
    assert_eq!(live.state().instances[&inst].process_id, "evt2");
    for mut engine in recovered(&live, &journal) {
        engine
            .apply_command(Command::correlate_message("M1", "k"))
            .unwrap();
        assert_completed(&engine, inst);
    }
}
