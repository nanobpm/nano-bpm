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
        replayed.state().refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
}
