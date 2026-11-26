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
