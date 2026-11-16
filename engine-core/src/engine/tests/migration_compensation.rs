//! #1631 — migration of compensation subscriptions (Zeebe
//! `MigrateCompensationSubscriptionTest` /
//! `MigrateCompensationSubscriptionRejectionTest` parity): a completed
//! compensable activity whose compensation boundary is mapped follows it to
//! the target, and its (target) handler runs on a later throw; an in-flight
//! compensation keeps waiting on its migrated handler. Zeebe's two
//! rejections are kept: a compensation boundary mapped onto a boundary of
//! another event type, and a changed flow-scope depth.
use super::*;

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .collect()
}

/// `s -> {task}(compensation boundary {boundary} -> {handler}) -> {next} ->
/// {throw} -> e`. Job types: `{task}`, `next` (shared), `{handler}`.
fn root_level(
    process: &str,
    task: &str,
    boundary: &str,
    handler: &str,
    next: &str,
    throw: &str,
) -> ProcessDefinition {
    ProcessBuilder::new(process)
        .start_event("s")
        .service_task(task, task)
        .compensation_boundary_event(boundary, task, handler)
        .service_task(handler, handler)
        .service_task(next, "next")
        .compensation_throw_event(throw)
        .end_event("e")
        .connect("s", task)
        .connect(task, next)
        .connect(next, throw)
        .connect(throw, "e")
        .build()
        .unwrap()
}

/// `s -> {sub}[ss -> {task}(comp boundary {boundary} -> {handler}) -> se] ->
/// {next} -> throw -> e`. With `nested`, `{task}` sits one sub-process deeper
/// (`{sub}[inner[...]]`).
fn in_subprocess(
    process: &str,
    sub: &str,
    task: &str,
    boundary: &str,
    handler: &str,
    next: &str,
    nested: bool,
) -> ProcessDefinition {
    let scope = if nested { "inner" } else { sub };
    let mut b = ProcessBuilder::new(process)
        .start_event("s")
        .sub_process(sub, "ss")
        .start_event("ss")
        .contained_in("ss", sub)
        .end_event("se")
        .contained_in("se", sub);
    if nested {
        b = b
            .sub_process("inner", "is")
            .contained_in("inner", sub)
            .start_event("is")
            .contained_in("is", "inner")
            .end_event("ie")
            .contained_in("ie", "inner")
            .connect("ss", "inner")
            .connect("inner", "se")
            .connect("is", task)
            .connect(task, "ie");
    } else {
        b = b.connect("ss", task).connect(task, "se");
    }
    b.service_task(task, task)
        .contained_in(task, scope)
        .compensation_boundary_event(boundary, task, handler)
        .contained_in(boundary, scope)
        .service_task(handler, handler)
        .contained_in(handler, scope)
        .service_task(next, "next")
        .compensation_throw_event("throw")
        .end_event("e")
        .connect("s", sub)
        .connect(sub, next)
        .connect(next, "throw")
        .connect("throw", "e")
        .build()
        .unwrap()
}

fn start(engine: &mut Engine, log: &mut Vec<Event>, process: &str) -> Key {
    let events = engine
        .apply_command(Command::create_instance(process))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    log.extend(events);
    inst
}

fn complete(engine: &mut Engine, log: &mut Vec<Event>, job_type: &str) {
    let job = engine.activate_jobs(job_type, "w", 1, 60_000, 0)[0].key;
    log.extend(engine.apply_command(Command::complete_job(job)).unwrap());
}

fn deploy(engine: &mut Engine, log: &mut Vec<Event>, def: ProcessDefinition) -> Key {
    let events = engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap();
    log.extend(events);
    key
}

fn compensable(engine: &Engine, inst: Key) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = engine
        .instance(inst)
        .unwrap()
        .compensable
        .iter()
        .map(|c| (c.element_id.clone(), c.handler.clone()))
        .collect();
    out.sort();
    out
}

fn assert_replays(log: &[Event], engine: &Engine, inst: Key) {
    let recovered = Engine::replay(log.to_vec());
    assert_eq!(recovered.instance(inst), engine.instance(inst));
}

/// Zeebe `shouldWriteCompensationMigratedEvent`: the subscription now names
/// the target activity and handler, and a later throw runs the TARGET handler.
#[test]
fn compensation_subscription_migrates_and_its_target_handler_runs() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        root_level("p", "A", "boundary1", "undoA", "B", "throw"),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        root_level("p2", "C", "boundary2", "undoC", "D", "throw2"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    assert_eq!(compensable(&engine, inst), pairs(&[("A", "undoA")]));

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("B", "D"), ("boundary1", "boundary2")]),
        ))
        .unwrap();
    let migrated = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceMigrated {
                compensation_subscriptions,
                compensation_migrated,
                ..
            } => {
                assert!(*compensation_migrated);
                Some(compensation_subscriptions.clone())
            }
            _ => None,
        })
        .expect("a ProcessInstanceMigrated fact is emitted");
    assert_eq!(
        migrated,
        vec![crate::event::MigratedCompensationSubscription {
            source_element_id: "A".into(),
            target_element_id: "C".into(),
            target_handler: "undoC".into(),
        }]
    );
    log.extend(events);
    assert_eq!(compensable(&engine, inst), pairs(&[("C", "undoC")]));
    assert_replays(&log, &engine, inst);

    complete(&mut engine, &mut log, "next");
    assert!(log.iter().any(|e| matches!(
        e,
        Event::CompensationTriggered { throw_element_id, handlers, .. }
            if throw_element_id == "throw2" && handlers == &vec!["undoC".to_string()]
    )));
    assert!(engine.activate_jobs("undoA", "w", 1, 60_000, 0).is_empty());
    complete(&mut engine, &mut log, "undoC");
    assert!(engine.is_completed(inst));
    assert_replays(&log, &engine, inst);
}

/// Zeebe `shouldWriteCompensationMigratedEventForCompensationInsideSubprocess`:
/// the completed sub-process's scope subscription follows its flow scope.
#[test]
fn compensation_inside_a_subprocess_migrates_with_its_scope_subscription() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        in_subprocess("p", "subProcess1", "A", "boundary1", "undoA", "B", false),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        in_subprocess("p2", "subProcess2", "C", "boundary2", "undoC", "D", false),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    assert_eq!(
        compensable(&engine, inst),
        pairs(&[("A", "undoA"), ("subProcess1", "")])
    );

    log.extend(
        engine
            .apply_command(Command::migrate_instance(
                inst,
                target,
                pairs(&[("B", "D"), ("boundary1", "boundary2")]),
            ))
            .unwrap(),
    );
    assert_eq!(
        compensable(&engine, inst),
        pairs(&[("C", "undoC"), ("subProcess2", "")])
    );
    assert_replays(&log, &engine, inst);

    complete(&mut engine, &mut log, "next");
    complete(&mut engine, &mut log, "undoC");
    assert!(engine.is_completed(inst));
    assert_replays(&log, &engine, inst);
}

/// An unmapped compensation boundary deletes its subscription (Zeebe
/// `unsubscribeFromCompensationEvents`): the later throw has nothing to do.
#[test]
fn unmapped_compensation_boundary_deletes_the_subscription() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        root_level("p", "A", "boundary1", "undoA", "B", "throw"),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        root_level("p2", "C", "boundary2", "undoC", "D", "throw2"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    log.extend(
        engine
            .apply_command(Command::migrate_instance(
                inst,
                target,
                pairs(&[("B", "D")]),
            ))
            .unwrap(),
    );
    assert!(compensable(&engine, inst).is_empty());
    assert_replays(&log, &engine, inst);
    complete(&mut engine, &mut log, "next");
    assert!(engine.is_completed(inst));
}

/// An in-flight compensation (the throw waiting on its handler) migrates: the
/// wait is re-pointed at the mapped throw / handler, so the migrated handler's
/// completion still releases the throw.
#[test]
fn in_flight_compensation_migrates_and_its_handler_releases_the_throw() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        root_level("p", "A", "boundary1", "undoA", "B", "throw"),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        root_level("p2", "C", "boundary2", "undoA", "D", "throw2"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    complete(&mut engine, &mut log, "next");
    assert!(!engine.instance(inst).unwrap().compensation_waits.is_empty());

    log.extend(
        engine
            .apply_command(Command::migrate_instance(
                inst,
                target,
                pairs(&[
                    ("throw", "throw2"),
                    ("undoA", "undoA"),
                    ("boundary1", "boundary2"),
                ]),
            ))
            .unwrap(),
    );
    let wait = engine
        .instance(inst)
        .unwrap()
        .compensation_waits
        .values()
        .next()
        .cloned()
        .unwrap();
    assert_eq!(wait.throw_element_id, "throw2");
    assert_eq!(wait.pending_handlers, vec!["undoA".to_string()]);
    assert_replays(&log, &engine, inst);

    complete(&mut engine, &mut log, "undoA");
    assert!(log.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw2" && to == "e"
    )));
    assert!(engine.is_completed(inst));
    assert_replays(&log, &engine, inst);
}

/// Zeebe `shouldRejectCompensationMigrationWhenBoundaryIsMappedToDifferentTypeBoundary`
/// (INVALID_ARGUMENT -> HTTP 400).
#[test]
fn rejects_a_compensation_boundary_mapped_to_a_different_type_boundary() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        root_level("p", "A", "boundary1", "undoA", "B", "throw"),
    );
    let target_def = ProcessBuilder::new("p2")
        .start_event("s")
        .service_task("C", "C")
        .timer_boundary_event("boundary2", "C", 600_000)
        .service_task("D", "next")
        .end_event("e")
        .end_event("te")
        .connect("s", "C")
        .connect("C", "D")
        .connect("D", "e")
        .connect("boundary2", "te")
        .build()
        .unwrap();
    let target = deploy(&mut engine, &mut log, target_def);
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("B", "D"), ("boundary1", "boundary2")]),
        ))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::MigrationCompensationBoundaryTypeChanged {
            instance_key: inst,
            process_id: "p".into(),
            source_boundary_id: "boundary1".into(),
            target_boundary_id: "boundary2".into(),
            target_event_type: "TIMER",
        }
    );
    assert_eq!(
        err.to_string(),
        "Expected to migrate process instance with id 'p' but compensation boundary event \
         'boundary1' is mapped to a target boundary event 'boundary2' that has event type \
         'TIMER' different than compensation boundary event type."
    );
}

/// Zeebe `shouldRejectCompensationMigrationWhenBoundaryEventFlowScopeIsDeeper`
/// (INVALID_STATE -> HTTP 409).
#[test]
fn rejects_a_compensation_boundary_whose_flow_scope_gets_deeper() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        in_subprocess("p", "subProcess1", "A", "boundary1", "undoA", "B", false),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        in_subprocess("p2", "subProcess2", "C", "boundary2", "undoC", "D", true),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("B", "D"), ("boundary1", "boundary2")]),
        ))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Expected to migrate process instance with id 'p' but the flow scope of compensation \
         boundary event is changed. Flow scope 'p' is not in the same level as 'subProcess2'. \
         The flow scope of a compensation boundary event cannot be changed during migration yet."
    );
    assert!(matches!(
        err,
        EngineError::MigrationCompensationFlowScopeChanged { .. }
    ));
    // Rejected: nothing was migrated.
    assert_eq!(
        compensable(&engine, inst),
        pairs(&[("A", "undoA"), ("subProcess1", "")])
    );
}

/// The shallower mirror (Zeebe
/// `shouldRejectCompensationMigrationWhenBoundaryEventFlowScopeIsShallower`).
#[test]
fn rejects_a_compensation_boundary_whose_flow_scope_gets_shallower() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        in_subprocess("p", "subProcess1", "A", "boundary1", "undoA", "B", true),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        in_subprocess("p2", "subProcess2", "C", "boundary2", "undoC", "D", false),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("B", "D"), ("boundary1", "boundary2")]),
        ))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::MigrationCompensationFlowScopeChanged {
            instance_key: inst,
            process_id: "p".into(),
            source_flow_scope_id: "subProcess1".into(),
            target_flow_scope_id: "p2".into(),
        }
    );
}
