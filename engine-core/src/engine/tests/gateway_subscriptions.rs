//! Gateway-owned event-based gateway subscription state (#1963 slice i,
//! #2003): the additive `ProcessInstance::gateway_subscriptions` projection
//! and the legacy-race detector the v4 snapshot loader uses to refuse
//! in-flight #1676 races.
use super::*;

fn gateway_race() -> ProcessDefinition {
    ProcessBuilder::new("evt")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end")
        .connect("onM2", "end")
        .build()
        .unwrap()
}

#[test]
fn gateway_subscriptions_open_then_clear_on_trigger_and_on_gateway_end() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(gateway_race()))
        .unwrap();
    let inst =
        create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
    let mut state = engine.state().clone();
    let opened = |gw: Key| Event::EventGatewaySubscriptionsOpened {
        instance_key: inst,
        gateway_element_instance_key: gw,
        gateway_element_id: "gw".to_string(),
        catch_element_ids: vec!["onM1".to_string(), "onM2".to_string()],
    };

    crate::state::apply(&mut state, &opened(9001));
    assert_eq!(
        state.instances[&inst].gateway_subscriptions.get(&9001),
        Some(&vec!["onM1".to_string(), "onM2".to_string()]),
        "subscriptions are owned by the gateway EIK, tagged with catch ids"
    );

    // First trigger wins: every subscription the gateway owns is deleted.
    crate::state::apply(
        &mut state,
        &Event::EventGatewayTriggered {
            instance_key: inst,
            gateway_element_instance_key: 9001,
            catch_element_id: "onM1".to_string(),
            catch_element_instance_key: 9010,
        },
    );
    assert!(!state.instances[&inst]
        .gateway_subscriptions
        .contains_key(&9001));

    // Terminating the gateway unsubscribes everything it owns.
    crate::state::apply(&mut state, &opened(9002));
    crate::state::apply(
        &mut state,
        &Event::ElementTerminated {
            instance_key: inst,
            element_instance_key: 9002,
            element_id: "gw".to_string(),
        },
    );
    assert!(!state.instances[&inst]
        .gateway_subscriptions
        .contains_key(&9002));
}

#[test]
fn gateway_owned_runtime_arms_no_legacy_race() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(gateway_race()))
        .unwrap();
    let inst =
        create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
    let instance = &engine.state().instances[&inst];
    assert!(
        engine.event_gateway_runtime_latched(),
        "the runtime latch is recorded"
    );
    assert!(instance.races.is_empty());
    assert!(engine
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());
    engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    assert!(engine
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());
}

/// An instance resting on a legacy race: the gateway is gone and its catches
/// are active element instances. Built by hand, since this build never arms
/// one.
fn legacy_race_state(races: bool, latched: bool) -> (State, Key) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(gateway_race()))
        .unwrap();
    let inst =
        create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
    let mut state = engine.state().clone();
    let instance = state.instances.get_mut(&inst).unwrap();
    instance.active.clear();
    instance.gateway_subscriptions.clear();
    instance.active.insert(9101, "onM1".to_string());
    instance.active.insert(9102, "onM2".to_string());
    if races {
        instance.races.insert(9101, 9100);
        instance.races.insert(9102, 9100);
    }
    state.event_gateway_runtime_latched = latched;
    (state, inst)
}

#[test]
fn legacy_race_detector_names_races_and_refuses_them_only_once_latched() {
    // A #1676 race carries `races` membership.
    let (state, inst) = legacy_race_state(true, false);
    assert_eq!(state.legacy_event_gateway_race_instances(), vec![inst]);
    // Before the latch the legacy runtime still owns it: loadable.
    assert!(state
        .refused_legacy_event_gateway_race_instances()
        .is_empty());
    // Once latched, legacy race state is refused.
    let (state, inst) = legacy_race_state(true, true);
    assert_eq!(
        state.refused_legacy_event_gateway_race_instances(),
        vec![inst]
    );
    // No `races` membership: nothing to refuse either way.
    let (state, _) = legacy_race_state(false, true);
    assert!(state
        .refused_legacy_event_gateway_race_instances()
        .is_empty());
}

#[test]
fn latched_refusal_survives_serde() {
    #[cfg(feature = "serde")]
    {
        let (state, inst) = legacy_race_state(true, true);
        let json = serde_json::to_string(&state).unwrap();
        let back: State = serde_json::from_str(&json).unwrap();
        assert!(back.event_gateway_runtime_latched);
        assert_eq!(
            back.refused_legacy_event_gateway_race_instances(),
            vec![inst]
        );
        // A pre-latch snapshot serializes without the field and loads unlatched.
        let (state, _) = legacy_race_state(true, false);
        let json = serde_json::to_string(&state).unwrap();
        assert!(!json.contains("event_gateway_runtime_latched"));
    }
}

/// Whole-instance teardown (a top-level completion — e.g. a terminate end — or
/// a cancellation) bypasses per-gateway terminal events, so it must clear the
/// gateway-owned subscriptions itself or they strand on the terminal shell.
#[test]
fn gateway_subscriptions_clear_on_whole_instance_teardown() {
    for terminal in [
        (|k| Event::ProcessInstanceCompleted { instance_key: k }) as fn(Key) -> Event,
        |k| Event::ProcessInstanceTerminated { instance_key: k },
    ] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(gateway_race()))
            .unwrap();
        let inst =
            create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
        let mut state = engine.state().clone();
        crate::state::apply(
            &mut state,
            &Event::EventGatewaySubscriptionsOpened {
                instance_key: inst,
                gateway_element_instance_key: 9001,
                gateway_element_id: "gw".to_string(),
                catch_element_ids: vec!["onM1".to_string()],
            },
        );
        assert!(!state.instances[&inst].gateway_subscriptions.is_empty());
        crate::state::apply(&mut state, &terminal(inst));
        assert!(
            state.instances[&inst].gateway_subscriptions.is_empty(),
            "teardown {:?} must clear gateway subscriptions",
            terminal(inst)
        );
    }
}

/// #2004 deferred item 3: a legacy journal tail — a catch armed by a completed
/// gateway (`ElementActivating.race`, no gateway-owned subscriptions) —
/// replays into state the post-replay refusal names.
#[test]
fn replayed_legacy_journal_tail_is_detected() {
    let mut engine = Engine::new();
    let mut journal = engine
        .apply_command(Command::DeployProcess(gateway_race()))
        .unwrap();
    // A stand-in for an instance created by a legacy build that never
    // reached the gateway yet (the gateway-opening events are dropped).
    let created = engine
        .apply_command(Command::create_instance_with(
            "evt",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    journal.extend(
        created
            .into_iter()
            .filter(|e| !matches!(e, Event::EventGatewaySubscriptionsOpened { .. })),
    );
    assert!(Engine::replay(journal.clone())
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());

    for (race, legacy) in [(Some(9100), true), (None, false)] {
        let mut tail = journal.clone();
        tail.push(Event::ElementActivating {
            instance_key: inst,
            element_instance_key: 9101,
            element_id: "onM1".to_string(),
            scope: 0,
            race,
        });
        assert_eq!(
            Engine::replay(tail)
                .state()
                .legacy_event_gateway_race_instances(),
            if legacy { vec![inst] } else { Vec::new() },
            "race {race:?}"
        );
    }
}

/// A gateway-triggered catch whose path leads to an inclusive join is an
/// in-transit join source (#2004): when one signal triggers both the
/// gateway's catch and a sibling branch's catch, the join must wait for the
/// gateway branch instead of firing early and then again.
#[test]
fn inclusive_join_waits_for_a_gateway_triggered_catch() {
    for gateway_first in [true, false] {
        let mut b = ProcessBuilder::new("p")
            .start_event("start")
            .parallel_gateway("fork")
            .event_based_gateway("gw")
            .signal_intermediate_catch_event("onGw", "go")
            .timer_intermediate_catch_event("onTimer", 60_000)
            .signal_intermediate_catch_event("onSib", "go")
            .inclusive_gateway("join")
            .end_event("end")
            .end_event("timerEnd")
            .connect("start", "fork");
        b = if gateway_first {
            b.connect("fork", "gw").connect("fork", "onSib")
        } else {
            b.connect("fork", "onSib").connect("fork", "gw")
        };
        let def = b
            .connect("gw", "onGw")
            .connect("gw", "onTimer")
            .connect("onTimer", "timerEnd")
            .connect("onGw", "join")
            .connect("onSib", "join")
            .connect("join", "end")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        deploy_for_migration(&mut engine, def);
        let inst = create_instance_with_vars(&mut engine, "p", BTreeMap::new());
        let events = engine.broadcast_signal("go", BTreeMap::new(), 0);
        let join_completions = events
            .iter()
            .filter(
                |e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == "join"),
            )
            .count();
        assert_eq!(
            join_completions, 1,
            "gateway_first={gateway_first}: {events:?}"
        );
        assert_eq!(
            engine.instance(inst).unwrap().state,
            ProcessInstanceState::Completed
        );
    }
}

/// #2047 review: `EventGatewayTriggered` MINTS the catch's element instance
/// key, and when the catch's activation never follows (its scope is
/// terminated first) that record is the only carrier of the key. Replay
/// restores the key allocator from `Event::max_key`, so every gateway-owned
/// key field must count, or a replayed engine re-issues a live key.
#[test]
fn event_gateway_records_count_every_owned_key_in_max_key() {
    let triggered = Event::EventGatewayTriggered {
        instance_key: 10,
        gateway_element_instance_key: 20,
        catch_element_id: "onM1".into(),
        catch_element_instance_key: 30,
    };
    assert_eq!(triggered.max_key(), 30);
    let triggered_gw_high = Event::EventGatewayTriggered {
        instance_key: 10,
        gateway_element_instance_key: 40,
        catch_element_id: "onM1".into(),
        catch_element_instance_key: 0,
    };
    assert_eq!(triggered_gw_high.max_key(), 40);
    let opened = Event::EventGatewaySubscriptionsOpened {
        instance_key: 10,
        gateway_element_instance_key: 50,
        gateway_element_id: "gw".into(),
        catch_element_ids: vec!["onM1".into()],
    };
    assert_eq!(opened.max_key(), 50);
}
