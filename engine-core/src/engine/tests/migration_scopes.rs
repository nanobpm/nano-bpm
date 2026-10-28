//! #1374 — migration of embedded sub-processes, boundary events and
//! intermediate catch-event subscriptions (Zeebe `MigrateProcessInstanceTest`,
//! `MigrateProcessInstanceRejectionTest`, `MigrateSubprocessTest`,
//! `MigrateBoundaryEventTest`, `MigrateCatchEventTest` parity).
use super::*;
use crate::event::{MigratedSubscriptionTrigger, SubscriptionMigrationAction};

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .collect()
}

/// start -> `sub`[ s_start -> `inner`(work) -> s_end ] -> end
fn sub_process(id: &str, sub: &str, inner: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .sub_process(sub, "s_start")
        .start_event("s_start")
        .contained_in("s_start", sub)
        .service_task(inner, "work")
        .contained_in(inner, sub)
        .end_event("s_end")
        .contained_in("s_end", sub)
        .end_event("end")
        .connect("start", sub)
        .connect("s_start", inner)
        .connect(inner, "s_end")
        .connect(sub, "end")
        .build()
        .unwrap()
}

fn migrated_event(events: &[Event]) -> &Event {
    events
        .iter()
        .find(|e| matches!(e, Event::ProcessInstanceMigrated { .. }))
        .expect("a ProcessInstanceMigrated fact is emitted")
}

fn subscriptions_of(events: &[Event]) -> Vec<crate::event::MigratedSubscription> {
    match migrated_event(events) {
        Event::ProcessInstanceMigrated { subscriptions, .. } => subscriptions.clone(),
        _ => unreachable!(),
    }
}

fn live_timers(engine: &Engine, inst: Key) -> Vec<state::Timer> {
    let mut t: Vec<state::Timer> = engine
        .state()
        .timers
        .values()
        .filter(|t| t.instance_key == inst && t.state == state::TimerState::Created)
        .cloned()
        .collect();
    t.sort_by_key(|t| t.key);
    t
}

fn live_message_subs(engine: &Engine, inst: Key) -> Vec<state::MessageSubscription> {
    let mut s: Vec<state::MessageSubscription> = engine
        .state()
        .message_subscriptions
        .values()
        .filter(|s| s.instance_key == inst && s.state == state::MessageSubscriptionState::Open)
        .cloned()
        .collect();
    s.sort_by_key(|s| s.key);
    s
}

fn last_deployed_key(log: &[Event]) -> Key {
    log.iter()
        .rev()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap()
}

fn pending_job_element(engine: &Engine, inst: Key) -> String {
    engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst && j.state == state::JobState::Created)
        .map(|j| j.element_id.clone())
        .expect("a pending job")
}

// ---------------------------------------------------------------------------
// Embedded sub-processes (Zeebe MigrateSubprocessTest)
// ---------------------------------------------------------------------------

#[test]
fn migration_remaps_active_subprocess_and_its_inner_task() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, sub_process("source", "sub", "a"));
    let target_key = deploy_for_migration(&mut engine, sub_process("target", "sub2", "b"));
    let inst = create_instance_key(&mut engine, "source");
    let sub_eik = active_eik_of(&engine, inst, "sub");

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("sub", "sub2"), ("a", "b")]),
        ))
        .unwrap();

    match migrated_event(&events) {
        Event::ProcessInstanceMigrated {
            migrated_flow_scopes,
            ..
        } => {
            assert_eq!(migrated_flow_scopes.len(), 1);
            assert_eq!(migrated_flow_scopes[0].element_instance_key, sub_eik);
            assert_eq!(migrated_flow_scopes[0].source_element_id, "sub");
            assert_eq!(migrated_flow_scopes[0].target_element_id, "sub2");
        }
        _ => unreachable!(),
    }
    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.active.get(&sub_eik).map(String::as_str),
        Some("sub2")
    );
    assert_eq!(pending_job_element(&engine, inst), "b");

    // The migrated scope drains and completes under the target model.
    complete_one(&mut engine, "work");
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn migration_rejects_mapping_that_changes_the_flow_scope() {
    // Zeebe `shouldRejectCommandWhenFlowScopeIsChanged`: the inner task is mapped
    // to a task that lives at process level in the target.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, sub_process("source", "sub", "a"));
    let target = ProcessBuilder::new("target")
        .start_event("start")
        .sub_process("sub2", "s_start")
        .start_event("s_start")
        .contained_in("s_start", "sub2")
        .end_event("s_end")
        .contained_in("s_end", "sub2")
        .service_task("b", "work")
        .end_event("end")
        .connect("start", "sub2")
        .connect("s_start", "s_end")
        .connect("sub2", "b")
        .connect("b", "end")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);
    let inst = create_instance_key(&mut engine, "source");

    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("sub", "sub2"), ("a", "b")]),
        ))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::MigrationFlowScopeChanged {
            instance_key: inst,
            element_id: "a".to_string(),
            source_flow_scope_id: Some("sub2".to_string()),
            target_flow_scope_id: Some("target".to_string()),
        }
    );
    assert_eq!(
        err.to_string(),
        format!(
            "Expected to migrate process instance '{inst}' but the flow scope of active element \
             with id 'a' is changed. The flow scope of the active element is expected to be \
             'sub2' but was 'target'. The flow scope of an element cannot be changed during \
             migration yet."
        )
    );
    assert_eq!(engine.instance(inst).unwrap().process_id, "source");
}

#[test]
fn migration_rejects_active_subprocess_without_mapping() {
    // The enclosing sub-process is itself an active element and needs a
    // mapping (Zeebe `ERROR_UNMAPPED_ACTIVE_ELEMENT`).
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, sub_process("source", "sub", "a"));
    let target_key = deploy_for_migration(&mut engine, sub_process("target", "sub2", "b"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b")]),
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::UnmappedActiveElement { element_id, .. } if element_id == "sub"
    ));
}

#[test]
fn migration_of_subprocess_survives_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(sub_process("source", "sub", "a")))
            .unwrap(),
    );
    let deployed = engine
        .apply_command(Command::DeployProcess(sub_process("target", "sub2", "b")))
        .unwrap();
    log.extend(deployed);
    let target_key = last_deployed_key(&log);
    let created = engine
        .apply_command(Command::create_instance("source"))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    log.extend(created);
    log.extend(
        engine
            .apply_command(Command::migrate_instance(
                inst,
                target_key,
                pairs(&[("sub", "sub2"), ("a", "b")]),
            ))
            .unwrap(),
    );
    let recovered = Engine::replay(log);
    assert_eq!(recovered.state(), engine.state());
}

// ---------------------------------------------------------------------------
// Boundary events (Zeebe MigrateBoundaryEventTest / MigrateTimerBoundaryEventTest
// / MigrateMessageBoundaryEventTest)
// ---------------------------------------------------------------------------

fn task_with_timer_boundary(
    id: &str,
    task: &str,
    boundary: &str,
    millis: u64,
) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .service_task(task, "work")
        .timer_boundary_event(boundary, task, millis)
        .end_event("end")
        .end_event("timed_out")
        .connect("start", task)
        .connect(task, "end")
        .connect(boundary, "timed_out")
        .build()
        .unwrap()
}

fn task_with_message_boundary(
    id: &str,
    task: &str,
    boundary: &str,
    msg: &str,
) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .service_task(task, "work")
        .message_boundary_event(boundary, task, msg, "=key")
        .end_event("end")
        .end_event("cancelled")
        .connect("start", task)
        .connect(task, "end")
        .connect(boundary, "cancelled")
        .build()
        .unwrap()
}

#[test]
fn migration_retains_unchanged_timer_boundary() {
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("source", "a", "t1", 60_000),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("target", "b", "t2", 60_000),
    );
    let inst = create_instance_key(&mut engine, "source");
    let before = live_timers(&engine, inst);
    assert_eq!(before.len(), 1);

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("t1", "t2")]),
        ))
        .unwrap();

    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::TimerCanceled { .. } | Event::TimerCreated { .. })),
        "an unchanged timer is retained, not re-created: {events:?}"
    );
    let after = live_timers(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].key, before[0].key);
    assert_eq!(after[0].due_at, before[0].due_at);
    assert_eq!(after[0].element_id, "b");
    assert_eq!(
        after[0].kind,
        state::TimerKind::InterruptingBoundary {
            boundary_element_id: "t2".to_string()
        }
    );
    let subs = subscriptions_of(&events);
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].action, SubscriptionMigrationAction::Retained);
    assert_eq!(subs[0].element_id, "t2");
    assert!(matches!(
        subs[0].trigger,
        MigratedSubscriptionTrigger::Timer { timer_key, .. } if timer_key == before[0].key
    ));

    // The retained timer fires the TARGET boundary's flow.
    let fired = engine.trigger_timers(before[0].due_at);
    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.state, ProcessInstanceState::Completed);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "t2"
    )));
}

#[test]
fn migration_recreates_timer_boundary_whose_timer_changed() {
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("source", "a", "t1", 60_000),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("target", "b", "t2", 5_000),
    );
    let inst = create_instance_key(&mut engine, "source");
    let old = live_timers(&engine, inst)[0].clone();

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("t1", "t2")]),
        ))
        .unwrap();
    let after = live_timers(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_ne!(after[0].key, old.key, "the timer is re-created");
    assert_eq!(after[0].element_id, "b");
    assert_eq!(after[0].due_at, old.due_at - 60_000 + 5_000);
    let subs = subscriptions_of(&events);
    assert_eq!(subs.len(), 2);
    assert!(subs.iter().any(|s| s.action == SubscriptionMigrationAction::Deleted
        && matches!(s.trigger, MigratedSubscriptionTrigger::Timer { timer_key, .. } if timer_key == old.key)));
    assert!(subs.iter().any(|s| s.action == SubscriptionMigrationAction::Recreated
        && s.element_id == "t2"
        && matches!(s.trigger, MigratedSubscriptionTrigger::Timer { timer_key, .. } if timer_key == after[0].key)));
}

#[test]
fn migration_deletes_unmapped_boundary_and_creates_new_target_boundary() {
    // Source boundary has no counterpart (unmapped) → its timer is cancelled;
    // the target's message boundary has no source → its subscription is opened.
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("source", "a", "t1", 60_000),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_message_boundary("target", "b", "m2", "cancel"),
    );
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("key", Value::Str("k1".into()))]),
    );
    let old = live_timers(&engine, inst)[0].clone();

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b")]),
        ))
        .unwrap();
    assert!(live_timers(&engine, inst).is_empty());
    let msgs = live_message_subs(&engine, inst);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message_name, "cancel");
    assert_eq!(msgs[0].correlation_key, "k1");
    assert_eq!(msgs[0].element_id, "b");
    let subs = subscriptions_of(&events);
    assert!(subs.iter().any(|s| s.action == SubscriptionMigrationAction::Deleted
        && s.element_id == "t1"
        && matches!(s.trigger, MigratedSubscriptionTrigger::Timer { timer_key, .. } if timer_key == old.key)));
    assert!(subs.iter().any(|s| s.action == SubscriptionMigrationAction::Created
        && s.element_id == "m2"
        && matches!(&s.trigger, MigratedSubscriptionTrigger::Message { message_name, correlation_key, .. }
            if message_name == "cancel" && correlation_key == "k1")));

    // The created boundary subscription is live: correlating interrupts `b`.
    engine
        .apply_command(Command::correlate_message("cancel", "k1"))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn migration_retains_message_boundary_and_recreates_on_name_change() {
    // Unchanged name + correlation key → retained.
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_message_boundary("source", "a", "m1", "cancel"),
    );
    let same_key = deploy_for_migration(
        &mut engine,
        task_with_message_boundary("same", "b", "m2", "cancel"),
    );
    let renamed_key = deploy_for_migration(
        &mut engine,
        task_with_message_boundary("renamed", "c", "m3", "abort"),
    );
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("key", Value::Str("k".into()))]),
    );
    let original = live_message_subs(&engine, inst)[0].clone();

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            same_key,
            pairs(&[("a", "b"), ("m1", "m2")]),
        ))
        .unwrap();
    let retained = live_message_subs(&engine, inst);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].key, original.key);
    assert_eq!(retained[0].element_id, "b");
    assert_eq!(
        retained[0].kind,
        state::MessageSubscriptionKind::InterruptingBoundary {
            boundary_element_id: "m2".to_string()
        }
    );
    assert_eq!(
        subscriptions_of(&events)
            .iter()
            .map(|s| s.action)
            .collect::<Vec<_>>(),
        vec![SubscriptionMigrationAction::Retained]
    );

    // Changed message name → re-subscribed.
    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            renamed_key,
            pairs(&[("b", "c"), ("m2", "m3")]),
        ))
        .unwrap();
    let recreated = live_message_subs(&engine, inst);
    assert_eq!(recreated.len(), 1);
    assert_ne!(recreated[0].key, original.key);
    assert_eq!(recreated[0].message_name, "abort");
    let mut actions: Vec<_> = subscriptions_of(&events).iter().map(|s| s.action).collect();
    actions.sort_by_key(|a| *a as u8);
    assert_eq!(
        actions,
        vec![
            SubscriptionMigrationAction::Recreated,
            SubscriptionMigrationAction::Deleted
        ]
    );
}

#[test]
fn migration_rejects_boundary_detached_from_its_element() {
    // Zeebe `ERROR_CATCH_EVENT_DETACHED_FROM_ELEMENT`.
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("source", "a", "t1", 60_000),
    );
    let target = ProcessBuilder::new("target")
        .start_event("start")
        .service_task("b", "work")
        .service_task("c", "other")
        .timer_boundary_event("t2", "c", 60_000)
        .end_event("end")
        .end_event("timed_out")
        .connect("start", "b")
        .connect("b", "c")
        .connect("c", "end")
        .connect("t2", "timed_out")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("t1", "t2")]),
        ))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "Expected to migrate process instance '{inst}' but active element with id 'a' is \
             mapped to an element with id 'b' and has a catch event with id 't1' that is mapped \
             to a catch event with id 't2'. These mappings detach the catch event from the \
             element in the target process. Catch events must stay attached to the same \
             element instance."
        )
    );
    assert!(matches!(
        err,
        EngineError::MigrationCatchEventDetached { .. }
    ));
}

#[test]
fn migration_rejects_two_boundaries_mapped_to_one_target_catch_event() {
    // Zeebe `requireNoDuplicateTargetsInCatchEventMappings`.
    let mut engine = Engine::new();
    let source = ProcessBuilder::new("source")
        .start_event("start")
        .service_task("a", "work")
        .timer_boundary_event("t1", "a", 60_000)
        .timer_boundary_event("t1b", "a", 70_000)
        .end_event("end")
        .end_event("x")
        .end_event("y")
        .connect("start", "a")
        .connect("a", "end")
        .connect("t1", "x")
        .connect("t1b", "y")
        .build()
        .unwrap();
    deploy_for_migration(&mut engine, source);
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("target", "b", "t2", 60_000),
    );
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("t1", "t2"), ("t1b", "t2")]),
        ))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "Expected to migrate process instance '{inst}' but active element with id 'a' has a \
             catch event attached that is mapped to a catch event with id 't2'. There are \
             multiple mapping instructions that target this catch event: 't1', 't1b'. Catch \
             events cannot be merged by process instance migration. Please ensure the mapping \
             instructions target a catch event only once."
        )
    );
    assert!(matches!(
        err,
        EngineError::MigrationCatchEventsMerged { .. }
    ));
}

#[test]
fn migration_retains_boundary_on_migrated_subprocess() {
    // A boundary armed on an active embedded sub-process is migrated with it.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_timer_boundary(),
        ))
        .unwrap();
    let mut target = process_with_subprocess_timer_boundary();
    target.id = "sub-timer-v2".to_string();
    let target_key = deploy_for_migration(&mut engine, target);
    let inst = create_instance_key(&mut engine, "sub-timer");
    let timer = live_timers(&engine, inst)[0].clone();

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("sub", "sub"), ("inner", "inner"), ("timeout", "timeout")]),
        ))
        .unwrap();
    assert_eq!(live_timers(&engine, inst)[0].key, timer.key);
    assert_eq!(
        subscriptions_of(&events)[0].action,
        SubscriptionMigrationAction::Retained
    );
    engine.trigger_timers(timer.due_at);
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

// ---------------------------------------------------------------------------
// Intermediate catch events (Zeebe MigrateCatchEventTest)
// ---------------------------------------------------------------------------

fn message_catch(id: &str, catch: &str, msg: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .message_intermediate_catch_event(catch, msg, "=key")
        .end_event("end")
        .connect("start", catch)
        .connect(catch, "end")
        .build()
        .unwrap()
}

#[test]
fn migration_retains_unchanged_message_catch_subscription() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, message_catch("source", "c1", "paid"));
    let target_key = deploy_for_migration(&mut engine, message_catch("target", "c2", "paid"));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("key", Value::Str("o1".into()))]),
    );
    let sub = live_message_subs(&engine, inst)[0].clone();

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("c1", "c2")]),
        ))
        .unwrap();
    let after = live_message_subs(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].key, sub.key);
    assert_eq!(after[0].element_id, "c2");
    assert_eq!(
        subscriptions_of(&events)
            .iter()
            .map(|s| s.action)
            .collect::<Vec<_>>(),
        vec![SubscriptionMigrationAction::Retained]
    );
    engine
        .apply_command(Command::correlate_message("paid", "o1"))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn migration_resubscribes_message_catch_whose_message_name_changed() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, message_catch("source", "c1", "paid"));
    let target_key = deploy_for_migration(&mut engine, message_catch("target", "c2", "settled"));
    let inst = create_instance_with_vars(
        &mut engine,
        "source",
        vars(&[("key", Value::Str("o1".into()))]),
    );
    let sub = live_message_subs(&engine, inst)[0].clone();

    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("c1", "c2")]),
        ))
        .unwrap();
    let after = live_message_subs(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_ne!(after[0].key, sub.key);
    assert_eq!(after[0].message_name, "settled");
    assert_eq!(after[0].element_id, "c2");
    // The old name no longer correlates; the new one does.
    engine
        .apply_command(Command::correlate_message("settled", "o1"))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn migration_with_boundaries_survives_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(task_with_timer_boundary(
                "source", "a", "t1", 60_000,
            )))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command(Command::DeployProcess(task_with_message_boundary(
                "target", "b", "m2", "cancel",
            )))
            .unwrap(),
    );
    let target_key = last_deployed_key(&log);
    let created = engine
        .apply_command(Command::create_instance_with(
            "source",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    log.extend(created);
    log.extend(
        engine
            .apply_command(Command::migrate_instance(
                inst,
                target_key,
                pairs(&[("a", "b")]),
            ))
            .unwrap(),
    );
    let recovered = Engine::replay(log);
    assert_eq!(recovered.state(), engine.state());
}

fn task_with_signal_boundary(
    id: &str,
    task: &str,
    boundary: &str,
    signal: &str,
) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .service_task(task, "work")
        .signal_boundary_event(boundary, task, signal)
        .end_event("end")
        .end_event("cancelled")
        .connect("start", task)
        .connect(task, "end")
        .connect(boundary, "cancelled")
        .build()
        .unwrap()
}

fn live_signal_subs(engine: &Engine, inst: Key) -> Vec<state::SignalSubscription> {
    let mut s: Vec<state::SignalSubscription> = engine
        .state()
        .signal_subscriptions
        .values()
        .filter(|s| s.instance_key == inst && s.state == state::MessageSubscriptionState::Open)
        .cloned()
        .collect();
    s.sort_by_key(|s| s.key);
    s
}

#[test]
fn migration_retains_signal_boundary_with_unchanged_signal_name() {
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_signal_boundary("source", "a", "s1", "go"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_signal_boundary("target", "b", "s2", "go"),
    );
    let inst = create_instance_key(&mut engine, "source");
    let before = live_signal_subs(&engine, inst);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("s1", "s2")]),
        ))
        .unwrap();
    let after = live_signal_subs(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].key, before[0].key, "subscription is retained");
    assert_eq!(after[0].element_id, "b");
    assert_eq!(
        after[0].kind,
        state::MessageSubscriptionKind::InterruptingBoundary {
            boundary_element_id: "s2".to_string()
        }
    );
}

#[test]
fn migration_recreates_signal_boundary_when_signal_name_changes() {
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_signal_boundary("source", "a", "s1", "go"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_signal_boundary("target", "b", "s2", "stop"),
    );
    let inst = create_instance_key(&mut engine, "source");
    let before = live_signal_subs(&engine, inst);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("s1", "s2")]),
        ))
        .unwrap();
    let after = live_signal_subs(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_ne!(after[0].key, before[0].key, "subscription is re-created");
    assert_eq!(after[0].signal_name, "stop");
}

#[test]
fn migration_retains_timer_of_intermediate_catch_event() {
    let def = |id: &str, catch: &str| {
        ProcessBuilder::new(id)
            .start_event("start")
            .timer_intermediate_catch_event(catch, 60_000)
            .end_event("end")
            .connect("start", catch)
            .connect(catch, "end")
            .build()
            .unwrap()
    };
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, def("source", "wait"));
    let target_key = deploy_for_migration(&mut engine, def("target", "wait2"));
    let inst = create_instance_key(&mut engine, "source");
    let before = live_timers(&engine, inst);
    assert_eq!(before.len(), 1);
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("wait", "wait2")]),
        ))
        .unwrap();
    let after = live_timers(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].key, before[0].key);
    assert_eq!(after[0].element_id, "wait2");
    assert_eq!(after[0].due_at, before[0].due_at);
}

/// Copilot #1380: the planner must read the instance's PINNED source version,
/// not the latest deployment of the same process id. v1 (60s) -> v2 (120s)
/// changes the timer, so it is re-created rather than retained.
#[test]
fn same_process_id_migration_plans_against_the_pinned_source_version() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, task_with_timer_boundary("p", "a", "t", 60_000));
    let inst = create_instance_key(&mut engine, "p");
    let v2 = deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("p", "a", "t", 120_000),
    );
    let before = live_timers(&engine, inst);
    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            v2,
            pairs(&[("a", "a"), ("t", "t")]),
        ))
        .unwrap();
    assert_eq!(
        subscriptions_of(&events)
            .iter()
            .map(|s| s.action)
            .collect::<Vec<_>>(),
        vec![
            SubscriptionMigrationAction::Deleted,
            SubscriptionMigrationAction::Recreated
        ]
    );
    let after = live_timers(&engine, inst);
    assert_eq!(after.len(), 1);
    assert_ne!(after[0].key, before[0].key);
}

/// Copilot #1380: a removed boundary in a same-process-id v2 must have its
/// v1 timer cancelled (the latest definition does not know the boundary).
#[test]
fn same_process_id_migration_cancels_timer_of_removed_boundary() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, task_with_timer_boundary("p", "a", "t", 60_000));
    let inst = create_instance_key(&mut engine, "p");
    let v2 = deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("p")
            .start_event("start")
            .service_task("a", "work")
            .end_event("end")
            .connect("start", "a")
            .connect("a", "end")
            .build()
            .unwrap(),
    );
    engine
        .apply_command(Command::migrate_instance(inst, v2, pairs(&[("a", "a")])))
        .unwrap();
    assert!(live_timers(&engine, inst).is_empty());
}

fn task_with_non_interrupting_timer(id: &str, task: &str, boundary: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .service_task(task, "work")
        .non_interrupting_timer_boundary_event(boundary, task, 1_000)
        .end_event("end")
        .end_event("side")
        .connect("start", task)
        .connect(task, "end")
        .connect(boundary, "side")
        .build()
        .unwrap()
}

/// Copilot #1380: a one-shot non-interrupting timer that already fired is not
/// armed any more; mapping it onto an identical target boundary must not
/// re-arm it (the side path would run twice).
#[test]
fn migration_does_not_rearm_a_mapped_fired_non_interrupting_timer() {
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        task_with_non_interrupting_timer("source", "a", "t1"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_non_interrupting_timer("target", "b", "t2"),
    );
    let inst = create_instance_key(&mut engine, "source");
    let due = live_timers(&engine, inst)[0].due_at;
    engine.trigger_timers(due);
    assert!(live_timers(&engine, inst).is_empty());

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("t1", "t2")]),
        ))
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::TimerCreated { .. })),
        "a fired mapped timer must not be re-armed: {events:?}"
    );
    assert!(live_timers(&engine, inst).is_empty());
}

/// Copilot #1380: an element parked in its start-listener chain has not armed
/// its boundaries yet; migration must leave arming to the activation body, or
/// the boundary is armed twice once the listener completes.
#[test]
fn migration_defers_boundary_arming_for_pending_start_listener() {
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("source")
            .start_event("start")
            .service_task("a", "work")
            .end_event("end")
            .connect("start", "a")
            .connect("a", "end")
            .with_listeners("a", vec![el(ListenerEventType::Start, "audit")], Vec::new())
            .build()
            .unwrap(),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_timer_boundary("target", "b", "t2", 60_000),
    );
    let inst = create_instance_key(&mut engine, "source");

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b")]),
        ))
        .unwrap();
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::TimerCreated { .. })));

    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1);
    engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert_eq!(
        live_timers(&engine, inst).len(),
        1,
        "boundary armed exactly once"
    );
}

/// Copilot #1380: an intermediate catch whose correlation key reads an
/// input-mapped local must be fingerprinted against its element-instance view;
/// otherwise the unchanged subscription is cancelled and re-arming raises an
/// expression incident.
#[test]
fn migration_retains_input_mapped_message_catch() {
    let catch = |id: &str, c: &str| {
        ProcessBuilder::new(id)
            .start_event("start")
            .message_intermediate_catch_event(c, "paid", "=key")
            .with_io(
                c,
                crate::model::IoMapping {
                    inputs: vec![crate::model::Mapping {
                        source: "=\"o1\"".to_string(),
                        target: "key".to_string(),
                    }],
                    outputs: Vec::new(),
                },
            )
            .end_event("end")
            .connect("start", c)
            .connect(c, "end")
            .build()
            .unwrap()
    };
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, catch("source", "c1"));
    let target_key = deploy_for_migration(&mut engine, catch("target", "c2"));
    let inst = create_instance_key(&mut engine, "source");
    let sub = live_message_subs(&engine, inst)[0].clone();
    assert_eq!(sub.correlation_key, "o1");

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("c1", "c2")]),
        ))
        .unwrap();
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::IncidentRaised { .. })));
    assert_eq!(
        subscriptions_of(&events)
            .iter()
            .map(|s| s.action)
            .collect::<Vec<_>>(),
        vec![SubscriptionMigrationAction::Retained]
    );
    assert_eq!(live_message_subs(&engine, inst)[0].key, sub.key);
}

/// Copilot #1380: a retained OFF-partition message boundary is re-pointed only
/// on the instance partition; the canonical record on the message partition
/// still forwards the source host/boundary ids. The continuation must resolve
/// them from the local record so the TARGET boundary's flow runs.
#[test]
fn off_partition_boundary_correlates_onto_migrated_target_boundary() {
    const N: u64 = 2;
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| state::subscription_partition(k, N) == 1)
        .unwrap();
    let mut engine = Engine::with_partition(0);
    engine.set_num_partitions(N);
    deploy_for_migration(
        &mut engine,
        task_with_message_boundary("source", "a", "m1", "cancel"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        task_with_message_boundary("target", "b", "m2", "cancel"),
    );
    let created = engine
        .apply_command(Command::create_instance_with(
            "source",
            vars(&[("key", Value::Str(order))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    let (sub_key, eik, stale_eid, stale_kind) = created
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionOpening {
                subscription_key,
                element_instance_key,
                element_id,
                kind,
                ..
            } => Some((
                *subscription_key,
                *element_instance_key,
                element_id.clone(),
                kind.clone(),
            )),
            _ => None,
        })
        .expect("off-partition Opening record");

    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b"), ("m1", "m2")]),
        ))
        .unwrap();

    // The remote partition forwards the stale (source) ids.
    let advanced = engine
        .apply_command(Command::CorrelateMessageSubscription {
            subscription_key: sub_key,
            message_key: 999_999,
            instance_key: inst,
            element_instance_key: eik,
            element_id: stale_eid,
            kind: stale_kind,
            variables: HashMap::new(),
        })
        .unwrap();
    assert!(
        advanced.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "m2"
        )),
        "the target boundary's flow runs: {advanced:?}"
    );
    assert!(engine.is_completed(inst));
}
