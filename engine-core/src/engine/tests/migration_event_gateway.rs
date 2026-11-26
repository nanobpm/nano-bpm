//! Migration of an instance resting at an event-based gateway together with the
//! subscriptions it owns (#1554, #2004, Zeebe
//! `ProcessInstanceMigrationMigrateProcessor`).
use super::*;

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// A gateway racing a timer, a message and a signal catch. `sfx` suffixes
/// every element id so source and target ids differ.
fn race(id: &str, sfx: &str, msg: &str) -> ProcessDefinition {
    race_builder(id, sfx, msg).build().unwrap()
}

fn race_builder(id: &str, sfx: &str, msg: &str) -> ProcessBuilder {
    let e = |n: &str| format!("{n}{sfx}");
    ProcessBuilder::new(id)
        .start_event("start")
        .event_based_gateway(e("gw"))
        .timer_intermediate_catch_event(e("onTimer"), 5_000)
        .message_intermediate_catch_event(e("onMsg"), msg, "=orderId")
        .signal_intermediate_catch_event(e("onSig"), "go")
        .end_event(e("endTimer"))
        .end_event(e("endMsg"))
        .end_event(e("endSig"))
        .connect("start", e("gw"))
        .connect(e("gw"), e("onTimer"))
        .connect(e("gw"), e("onMsg"))
        .connect(e("gw"), e("onSig"))
        .connect(e("onTimer"), e("endTimer"))
        .connect(e("onMsg"), e("endMsg"))
        .connect(e("onSig"), e("endSig"))
}

fn full_mapping() -> Vec<(String, String)> {
    pairs(&[
        ("gw", "gw2"),
        ("onTimer", "onTimer2"),
        ("onMsg", "onMsg2"),
        ("onSig", "onSig2"),
    ])
}

fn migrated_instance(target_msg: &str, mapping: Vec<(String, String)>) -> (Engine, Key) {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, race("source", "", "reply"));
    let target_key = deploy_for_migration(&mut engine, race("target", "2", target_msg));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("orderId", Value::Str("o1".into()))]),
    );
    engine
        .apply_command(Command::migrate_instance(inst, target_key, mapping))
        .unwrap();
    (engine, inst)
}

fn active_ids(engine: &Engine, inst: Key) -> Vec<String> {
    let mut ids: Vec<String> = engine
        .instance(inst)
        .unwrap()
        .active
        .values()
        .cloned()
        .collect();
    ids.sort();
    ids
}

fn assert_completed_via(engine: &Engine, inst: Key, events: &[Event], end: &str) {
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == end)),
        "completed via {end}"
    );
}

/// The gateway rests as an active element owning one subscription per catch
/// (#2004): no catch is an active element until the gateway is triggered.
#[test]
fn migrated_event_gateway_waits_with_its_target_subscriptions() {
    let (engine, inst) = migrated_instance("reply", full_mapping());
    assert_eq!(active_ids(&engine, inst), ["gw2"]);
    let instance = engine.instance(inst).unwrap();
    let subs: Vec<&Vec<String>> = instance.gateway_subscriptions.values().collect();
    assert_eq!(
        subs,
        [&vec![
            "onTimer2".to_string(),
            "onMsg2".to_string(),
            "onSig2".to_string()
        ]]
    );
}

#[test]
fn migrated_event_gateway_is_triggered_by_target_timer() {
    let (mut engine, inst) = migrated_instance("reply", full_mapping());
    let events = engine.trigger_timers(6_000);
    assert_completed_via(&engine, inst, &events, "endTimer2");
}

#[test]
fn migrated_event_gateway_is_triggered_by_target_message() {
    // The target renames the message: the subscription is re-created.
    let (mut engine, inst) = migrated_instance("settled", full_mapping());
    let events = engine
        .apply_command(Command::correlate_message("settled", "o1"))
        .unwrap();
    assert_completed_via(&engine, inst, &events, "endMsg2");
    // The other subscriptions were deleted by the trigger and never fire.
    assert!(!engine
        .trigger_timers(60_000)
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { .. })));
}

#[test]
fn migrated_event_gateway_is_triggered_by_target_signal() {
    let (mut engine, inst) = migrated_instance("reply", full_mapping());
    let events = engine.broadcast_signal("go", BTreeMap::new(), 0);
    assert_completed_via(&engine, inst, &events, "endSig2");
}

/// Zeebe deletes the subscription of an unmapped catch event of the gateway
/// and opens one for a target catch event with no mapped source.
#[test]
fn migration_replaces_an_unmapped_catch_subscription() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, race("source", "", "reply"));
    let target_key = deploy_for_migration(&mut engine, race("target", "2", "reply"));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("orderId", Value::Str("o1".into()))]),
    );
    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("gw", "gw2"), ("onTimer", "onTimer2"), ("onSig", "onSig2")]),
        ))
        .unwrap();
    let subs = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceMigrated { subscriptions, .. } => Some(subscriptions),
            _ => None,
        })
        .unwrap();
    assert!(
        subs.iter().any(|s| s.element_id == "onMsg"
            && s.action == crate::event::SubscriptionMigrationAction::Deleted),
        "{subs:?}"
    );
    assert!(
        subs.iter().any(|s| s.element_id == "onMsg2"
            && s.action == crate::event::SubscriptionMigrationAction::Created),
        "{subs:?}"
    );
    let events = engine
        .apply_command(Command::correlate_message("reply", "o1"))
        .unwrap();
    assert_completed_via(&engine, inst, &events, "endMsg2");
}

#[test]
fn migration_rejects_an_unmapped_waiting_gateway() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, race("source", "", "reply"));
    let target_key = deploy_for_migration(&mut engine, race("target", "2", "reply"));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("orderId", Value::Str("o1".into()))]),
    );
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("onTimer", "onTimer2")]),
        ))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::UnmappedActiveElement { element_id, .. } if element_id == "gw"),
        "{err:?}"
    );
}

fn two_timer_race(id: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .event_based_gateway("gw")
        .timer_intermediate_catch_event("t1", 5_000)
        .timer_intermediate_catch_event("t2", 9_000)
        .end_event("e1")
        .end_event("e2")
        .connect("start", "gw")
        .connect("gw", "t1")
        .connect("gw", "t2")
        .connect("t1", "e1")
        .connect("t2", "e2")
        .build()
        .unwrap()
}

#[test]
fn migration_rejects_gateway_catches_merged_onto_one_target() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, two_timer_race("source"));
    let target_key = deploy_for_migration(&mut engine, two_timer_race("target"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("gw", "gw"), ("t1", "t1"), ("t2", "t1")]),
        ))
        .unwrap_err();
    match err {
        EngineError::MigrationCatchEventsMerged {
            element_id,
            target_catch_event_id,
            source_catch_event_ids,
            ..
        } => {
            assert_eq!(element_id, "gw");
            assert_eq!(target_catch_event_id, "t1");
            assert_eq!(source_catch_event_ids, ["t1", "t2"]);
        }
        other => panic!("expected merge rejection, got {other:?}"),
    }
    assert_eq!(engine.instance(inst).unwrap().process_id, "source");
}

#[test]
fn migration_rejects_gateway_catch_detached_from_the_target_gateway() {
    // The target timer is a plain catch, not a catch event of the gateway.
    let target = ProcessBuilder::new("target")
        .start_event("start")
        .event_based_gateway("gw")
        .timer_intermediate_catch_event("t1", 5_000)
        .timer_intermediate_catch_event("t", 5_000)
        .end_event("e")
        .connect("start", "gw")
        .connect("gw", "t1")
        .connect("t1", "e")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, two_timer_race("source"));
    let target_key = deploy_for_migration(&mut engine, target);
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("gw", "gw"), ("t2", "t")]),
        ))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::MigrationCatchEventDetached { element_id, catch_event_id, .. }
            if element_id == "gw" && catch_event_id == "t2"),
        "{err:?}"
    );
}

/// A triggered gateway whose catch is parked on a start-listener job is past
/// the gateway: the catch is an ordinary active element and migrates as one.
#[test]
fn migration_of_a_triggered_catch_parked_on_its_start_listener() {
    let mut engine = Engine::new();
    let source = race_builder("source", "", "reply")
        .with_listeners(
            "onMsg",
            vec![el(ListenerEventType::Start, "audit")],
            Vec::new(),
        )
        .build()
        .unwrap();
    deploy_for_migration(&mut engine, source);
    let target_key = deploy_for_migration(&mut engine, race("target", "2", "reply"));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("orderId", Value::Str("o1".into()))]),
    );
    engine
        .apply_command(Command::correlate_message("reply", "o1"))
        .unwrap();
    assert_eq!(active_ids(&engine, inst), ["onMsg"]);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("onMsg", "onMsg2")]),
        ))
        .unwrap();
    assert_eq!(active_ids(&engine, inst), ["onMsg2"]);
    let job = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    assert_eq!(job.len(), 1);
    let events = engine
        .apply_command(Command::complete_job(job[0].key))
        .unwrap();
    assert_completed_via(&engine, inst, &events, "endMsg2");
}

/// Copilot #1564: the indexed race lookup must agree with the naive per-catch
/// graph scan on every element of a definition carrying the ambiguous shapes
/// (a catch fed by two gateways, a gateway with two incoming flows) that make
/// a catch not an unambiguous race member.
#[test]
fn race_index_matches_naive_scan() {
    let def = race_builder("p", "", "reply")
        .event_based_gateway("gw2")
        .timer_intermediate_catch_event("shared", 3_000)
        .end_event("endShared")
        .connect("gw", "shared")
        .connect("gw2", "shared")
        .connect("shared", "endShared")
        .connect("start", "gw2")
        .build()
        .unwrap();
    let index = event_gateway_race_index(&def);
    let naive = |catch_id: &str| {
        if !is_intermediate_catch(&def, catch_id) || def.incoming_count(catch_id) != 1 {
            return None;
        }
        let owners: Vec<&crate::model::Element> = def
            .elements
            .values()
            .filter(|element| matches!(element.kind, ElementKind::EventBasedGateway))
            .filter(|element| element.outgoing.iter().any(|f| f.to == catch_id))
            .collect();
        let [owner] = owners.as_slice() else {
            return None;
        };
        (def.incoming_count(&owner.id) == 1).then_some(*owner)
    };
    for id in def.elements.keys() {
        assert_eq!(
            index.owner_of(id).map(|o| o.id.as_str()),
            naive(id).map(|o| o.id.as_str()),
            "owner of {id}"
        );
    }
    // The ambiguous catch is no race member; the gateway with two incoming
    // flows owns no race.
    assert!(index.owner_of("shared").is_none());
    assert!(index.owner_of("onTimer").is_some_and(|o| o.id == "gw"));
    assert!(index.owner_of("onMsg").is_some_and(|o| o.id == "gw"));
    assert!(index.owner_of("onSig").is_some_and(|o| o.id == "gw"));
}

/// Copilot #1564: a catch routed into by two event-based gateways where one
/// is itself race-ineligible (two incoming flows) is still no race member —
/// the single-traversal index must not let the eligible claimant win by
/// default.
#[test]
fn race_index_marks_catch_ambiguous_when_one_claimant_is_ineligible() {
    let def = race_builder("p", "", "reply")
        .event_based_gateway("gw2")
        .timer_intermediate_catch_event("contested", 3_000)
        .end_event("endContested")
        .start_event("start2")
        // gw2 has two incoming flows, so it owns no race; but it still routes
        // into `contested`, making the catch's owner ambiguous.
        .connect("start2", "gw2")
        .connect("gw", "contested")
        .connect("gw2", "contested")
        .connect("contested", "endContested")
        .build()
        .unwrap();
    let index = event_gateway_race_index(&def);
    assert!(index.owner_of("contested").is_none());
    // The eligible gateway's own unambiguous members are unaffected.
    assert!(index.owner_of("onTimer").is_some_and(|o| o.id == "gw"));
    assert!(index.owner_of("onMsg").is_some_and(|o| o.id == "gw"));
}

/// Copilot #1564: a gateway with two incoming flows (concurrent activations
/// possible) owns no race even when its catch is private (one incoming flow),
/// so completing that catch never withdraws catches of an unrelated
/// activation. Exercises the gateway in-degree guard in isolation.
#[test]
fn race_index_excludes_private_catch_of_gateway_with_two_incoming_flows() {
    let def = race_builder("p", "", "reply")
        .start_event("start2")
        .event_based_gateway("gw2")
        .timer_intermediate_catch_event("private", 3_000)
        .end_event("endPrivate")
        .connect("start", "gw2")
        .connect("start2", "gw2")
        .connect("gw2", "private")
        .connect("private", "endPrivate")
        .build()
        .unwrap();
    assert_eq!(def.incoming_count("gw2"), 2);
    assert_eq!(def.incoming_count("private"), 1);
    let index = event_gateway_race_index(&def);
    assert!(index.owner_of("private").is_none());
    assert!(index.owner_of("onTimer").is_some_and(|o| o.id == "gw"));
}

/// start -> fork(and) => merge(xor) -> gw(event-based) -> { onM1 | onM2 } -> end
///
/// Two activations of ONE gateway wait independently in the root scope; `sfx`
/// renames every element so a migration maps both onto new ids. Each gateway
/// owns its own subscriptions, so one message triggers exactly one of them.
fn twin_gateway_race(id: &str, sfx: &str) -> ProcessDefinition {
    let e = |n: &str| format!("{n}{sfx}");
    ProcessBuilder::new(id)
        .start_event(e("start"))
        .parallel_gateway(e("fork"))
        .exclusive_gateway(e("merge"))
        .event_based_gateway(e("gw"))
        .message_intermediate_catch_event(e("onM1"), "M1", "=key")
        .message_intermediate_catch_event(e("onM2"), "M2", "=key")
        .end_event(e("end"))
        .connect(e("start"), e("fork"))
        .connect(e("fork"), e("merge"))
        .connect(e("fork"), e("merge"))
        .connect(e("merge"), e("gw"))
        .connect(e("gw"), e("onM1"))
        .connect(e("gw"), e("onM2"))
        .connect(e("onM1"), e("end"))
        .connect(e("onM2"), e("end"))
        .build()
        .unwrap()
}

#[test]
fn migration_keeps_each_gateway_activation_independent() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, twin_gateway_race("source", ""));
    let target_key = deploy_for_migration(&mut engine, twin_gateway_race("target", "2"));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("key", Value::Str("k".into()))]),
    );
    assert_eq!(
        engine.instance(inst).unwrap().gateway_subscriptions.len(),
        2
    );

    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("gw", "gw2"), ("onM1", "onM12"), ("onM2", "onM22")]),
        ))
        .unwrap();
    assert_eq!(active_ids(&engine, inst), ["gw2", "gw2"]);

    let first = engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    assert_eq!(completions(&first, "onM12"), 1, "M1 triggers one gateway");
    assert_eq!(completions(&first, "onM22"), 0, "no catch is withdrawn");
    let second = engine
        .apply_command(Command::correlate_message("M2", "k"))
        .unwrap();
    assert_eq!(
        completions(&second, "onM22"),
        1,
        "the other gateway still waits after migration"
    );
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

/// A new-build instance resting on a plain message catch, migrated onto a
/// target where that catch is gateway-owned, must carry the format marker:
/// otherwise the legacy-race loader refuses it on the next restart (#2004).
#[test]
fn migration_onto_gateway_owned_catch_is_not_a_legacy_race() {
    let mut engine = Engine::new();
    let source = ProcessBuilder::new("plain")
        .start_event("start")
        .message_intermediate_catch_event("onMsg", "reply", "=orderId")
        .end_event("end")
        .connect("start", "onMsg")
        .connect("onMsg", "end")
        .build()
        .unwrap();
    deploy_for_migration(&mut engine, source);
    let target_key = deploy_for_migration(&mut engine, race("target", "2", "reply"));
    let inst = create_instance_with_vars(
        &mut engine,
        "plain",
        vars(&[("orderId", Value::Str("o1".into()))]),
    );
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("onMsg", "onMsg2")]),
        ))
        .unwrap();
    assert_eq!(active_ids(&engine, inst), vec!["onMsg2".to_string()]);
    assert!(
        engine
            .state()
            .legacy_event_gateway_race_instances()
            .is_empty(),
        "a migrated new-build instance must not be refused as a legacy race"
    );
}
