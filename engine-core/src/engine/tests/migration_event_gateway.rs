//! Migration of an instance resting at an event-based gateway together with its
//! armed catch events (#1554, Zeebe `ProcessInstanceMigrationMigrateProcessor`).
use super::*;

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// A gateway racing a timer, a message and a signal catch. `sfx` suffixes
/// every element id so source and target ids differ.
fn race(id: &str, sfx: &str, msg: &str) -> ProcessDefinition {
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
        .build()
        .unwrap()
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

#[test]
fn migrated_event_gateway_race_is_won_by_target_timer() {
    let (mut engine, inst) = migrated_instance("reply", full_mapping());
    assert_eq!(active_ids(&engine, inst), ["onMsg2", "onSig2", "onTimer2"]);
    let events = engine.trigger_timers(6_000);
    assert_completed_via(&engine, inst, &events, "endTimer2");
}

#[test]
fn migrated_event_gateway_race_is_won_by_target_message() {
    // The target renames the message: the subscription is re-created.
    let (mut engine, inst) = migrated_instance("settled", full_mapping());
    let events = engine
        .apply_command(Command::correlate_message("settled", "o1"))
        .unwrap();
    assert_completed_via(&engine, inst, &events, "endMsg2");
    // The losing timer was withdrawn and never fires.
    assert!(!engine
        .trigger_timers(60_000)
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { .. })));
}

#[test]
fn migrated_event_gateway_race_is_won_by_target_signal() {
    let (mut engine, inst) = migrated_instance("reply", full_mapping());
    let events = engine.broadcast_signal("go", HashMap::new(), 0);
    assert_completed_via(&engine, inst, &events, "endSig2");
}

#[test]
fn migration_withdraws_an_unmapped_race_member() {
    // Zeebe deletes the subscription of an unmapped catch event of the gateway.
    let (mut engine, inst) = migrated_instance(
        "reply",
        pairs(&[("onTimer", "onTimer2"), ("onSig", "onSig2")]),
    );
    assert_eq!(active_ids(&engine, inst), ["onSig2", "onTimer2"]);
    assert!(
        !engine
            .state()
            .message_subscriptions
            .values()
            .any(|s| s.instance_key == inst && s.state == state::MessageSubscriptionState::Open),
        "the unmapped message catch's subscription is deleted"
    );
    let events = engine.broadcast_signal("go", HashMap::new(), 0);
    assert_completed_via(&engine, inst, &events, "endSig2");
}

#[test]
fn migration_rejects_race_with_no_mapped_member() {
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
            pairs(&[("gw", "gw2")]),
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::UnmappedActiveElement { .. }),
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
fn migration_rejects_race_members_merged_onto_one_target() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, two_timer_race("source"));
    let target_key = deploy_for_migration(&mut engine, two_timer_race("target"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("t1", "t1"), ("t2", "t1")]),
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
fn migration_rejects_race_member_detached_from_the_target_gateway() {
    // The target timer is a plain catch, not a member of any gateway race.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, two_timer_race("source"));
    let target_key = deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("target")
            .start_event("start")
            .timer_intermediate_catch_event("t", 5_000)
            .end_event("e")
            .connect("start", "t")
            .connect("t", "e")
            .build()
            .unwrap(),
    );
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("t1", "t")]),
        ))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::MigrationCatchEventDetached { element_id, catch_event_id, .. }
            if element_id == "gw" && catch_event_id == "t1"),
        "{err:?}"
    );
}

#[test]
fn migration_rejects_race_members_split_across_target_gateways() {
    let target = ProcessBuilder::new("target")
        .start_event("start")
        .parallel_gateway("fork")
        .event_based_gateway("gwA")
        .event_based_gateway("gwB")
        .timer_intermediate_catch_event("a", 5_000)
        .timer_intermediate_catch_event("b", 9_000)
        .end_event("ea")
        .end_event("eb")
        .connect("start", "fork")
        .connect("fork", "gwA")
        .connect("fork", "gwB")
        .connect("gwA", "a")
        .connect("gwB", "b")
        .connect("a", "ea")
        .connect("b", "eb")
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
            pairs(&[("t1", "a"), ("t2", "b")]),
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::MigrationCatchEventDetached { .. }),
        "{err:?}"
    );
}
