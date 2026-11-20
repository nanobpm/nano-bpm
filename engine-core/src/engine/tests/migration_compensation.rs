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
    let job = engine
        .activate_jobs(job_type, "w", 1, 60_000, 0)
        .first()
        .unwrap_or_else(|| panic!("expected an activatable job of type {job_type}"))
        .key;
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
    assert_eq!(migrated.len(), 1);
    assert_eq!(migrated[0].source_element_id, "A");
    assert_eq!(migrated[0].source_handler, "undoA");
    assert_eq!(migrated[0].target_element_id, "C");
    assert_eq!(migrated[0].target_handler, "undoC");
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

/// `s -> A(comp boundaries b1 -> {h1}, b2 -> {h2}) -> B -> throw -> e`.
fn two_boundaries(process: &str, h1: &str, h2: &str, b: &str) -> ProcessDefinition {
    let mut builder = ProcessBuilder::new(process)
        .start_event("s")
        .service_task("A", "A")
        .compensation_boundary_event("b1", "A", h1)
        .compensation_boundary_event("b2", "A", h2)
        .service_task(h1, h1);
    if h2 != h1 {
        builder = builder.service_task(h2, h2);
    }
    builder
        .service_task(b, "next")
        .compensation_throw_event("throw")
        .end_event("e")
        .connect("s", "A")
        .connect("A", b)
        .connect(b, "throw")
        .connect("throw", "e")
        .build()
        .unwrap()
}

fn migrate(engine: &mut Engine, log: &mut Vec<Event>, inst: Key, target: Key, m: &[(&str, &str)]) {
    log.extend(
        engine
            .apply_command(Command::migrate_instance(inst, target, pairs(m)))
            .unwrap(),
    );
}

/// #1635 review: with two compensation boundaries on one task, mapping only
/// one keeps only ITS subscription (no double compensation).
#[test]
fn mapping_one_of_two_boundaries_migrates_only_its_subscription() {
    for mapped in ["b1", "b2"] {
        let mut engine = Engine::new();
        let mut log = Vec::new();
        deploy(&mut engine, &mut log, two_boundaries("p", "u1", "u2", "B"));
        let target = deploy(&mut engine, &mut log, two_boundaries("p2", "v1", "v2", "D"));
        let inst = start(&mut engine, &mut log, "p");
        complete(&mut engine, &mut log, "A");
        assert_eq!(
            compensable(&engine, inst),
            pairs(&[("A", "u1"), ("A", "u2")])
        );
        migrate(
            &mut engine,
            &mut log,
            inst,
            target,
            &[("A", "A"), ("B", "D"), (mapped, mapped)],
        );
        let expected = if mapped == "b1" { "v1" } else { "v2" };
        assert_eq!(compensable(&engine, inst), pairs(&[("A", expected)]));
        assert_replays(&log, &engine, inst);
    }
}

/// #1635 review: crossed mappings take the MAPPED boundary's handler.
#[test]
fn crossed_boundary_mappings_take_each_mapped_boundarys_handler() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, two_boundaries("p", "u1", "u2", "B"));
    let target = deploy(&mut engine, &mut log, two_boundaries("p2", "v1", "v2", "D"));
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    migrate(
        &mut engine,
        &mut log,
        inst,
        target,
        &[("A", "A"), ("B", "D"), ("b1", "b2"), ("b2", "b1")],
    );
    let subs: Vec<String> = engine
        .instance(inst)
        .unwrap()
        .compensable
        .iter()
        .map(|c| c.handler.clone())
        .collect();
    assert_eq!(subs, vec!["v2".to_string(), "v1".to_string()]);
    assert_replays(&log, &engine, inst);
}

/// #1635 review: two boundaries sharing one handler; mapping one keeps one.
#[test]
fn shared_handler_boundaries_migrate_one_subscription_per_mapped_boundary() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, two_boundaries("p", "u", "u", "B"));
    let target = deploy(&mut engine, &mut log, two_boundaries("p2", "v", "v", "D"));
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    assert_eq!(compensable(&engine, inst), pairs(&[("A", "u"), ("A", "u")]));
    migrate(
        &mut engine,
        &mut log,
        inst,
        target,
        &[("A", "A"), ("B", "D"), ("b2", "b2")],
    );
    assert_eq!(compensable(&engine, inst), pairs(&[("A", "v")]));
    assert_replays(&log, &engine, inst);
}

/// `s -> {sub}(comp boundary {sb} -> {sh})[ss -> A(comp boundary ab -> {ah}) -> se]
/// -> {next} -> throw -> e`.
fn compensable_subprocess(
    process: &str,
    sub: &str,
    sb: &str,
    sh: &str,
    ah: &str,
    next: &str,
) -> ProcessDefinition {
    ProcessBuilder::new(process)
        .start_event("s")
        .sub_process(sub, "ss")
        .start_event("ss")
        .contained_in("ss", sub)
        .service_task("A", "A")
        .contained_in("A", sub)
        .compensation_boundary_event("ab", "A", ah)
        .contained_in("ab", sub)
        .service_task(ah, ah)
        .contained_in(ah, sub)
        .end_event("se")
        .contained_in("se", sub)
        .compensation_boundary_event(sb, sub, sh)
        .service_task(sh, sh)
        .service_task(next, "next")
        .compensation_throw_event("throw")
        .end_event("e")
        .connect("ss", "A")
        .connect("A", "se")
        .connect("s", sub)
        .connect(sub, next)
        .connect(next, "throw")
        .connect("throw", "e")
        .build()
        .unwrap()
}

fn explicit_sub_handlers(engine: &Engine, inst: Key) -> Vec<(String, String)> {
    compensable(engine, inst)
        .into_iter()
        .filter(|(_, h)| h == "subUndo" || h == "subUndo2")
        .collect()
}

/// #1635 review: an explicit sub-process compensation subscription follows
/// its OWN boundary mapping, independent of nested boundaries.
#[test]
fn explicit_subprocess_subscription_follows_its_own_boundary() {
    // Own boundary mapped, nested one not: the explicit subscription survives.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        compensable_subprocess("p", "sub1", "sb1", "subUndo", "undoA", "B"),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        compensable_subprocess("p2", "sub2", "sb2", "subUndo2", "undoA2", "D"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    assert_eq!(
        explicit_sub_handlers(&engine, inst),
        pairs(&[("sub1", "subUndo")])
    );
    migrate(
        &mut engine,
        &mut log,
        inst,
        target,
        &[("sub1", "sub2"), ("B", "D"), ("sb1", "sb2")],
    );
    assert_eq!(
        explicit_sub_handlers(&engine, inst),
        pairs(&[("sub2", "subUndo2")])
    );
    assert_replays(&log, &engine, inst);

    // Nested boundary mapped, own boundary not: the explicit one is deleted.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        compensable_subprocess("p", "sub1", "sb1", "subUndo", "undoA", "B"),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        compensable_subprocess("p2", "sub2", "sb2", "subUndo2", "undoA2", "D"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    migrate(
        &mut engine,
        &mut log,
        inst,
        target,
        &[("sub1", "sub2"), ("B", "D"), ("ab", "ab")],
    );
    assert!(explicit_sub_handlers(&engine, inst).is_empty());
    assert!(compensable(&engine, inst).contains(&("A".to_string(), "undoA2".to_string())));
    assert_replays(&log, &engine, inst);
}

/// #1635 review: the boundary identity is persisted, so a SUCCESSIVE
/// migration of the shared-handler survivor (`b2`) still resolves it as `b2`
/// (not as occurrence 0 = `b1`), and a throw journals `b2`'s lifecycle.
#[test]
fn successive_migrations_keep_the_subscriptions_boundary_identity() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, two_boundaries("p", "u", "u", "B"));
    let second = deploy(&mut engine, &mut log, two_boundaries("p2", "v", "v", "D"));
    let third = deploy(&mut engine, &mut log, two_boundaries("p3", "w", "w", "F"));
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let boundaries = |engine: &Engine| -> Vec<String> {
        engine
            .instance(inst)
            .unwrap()
            .compensable
            .iter()
            .map(|c| c.boundary_id.clone())
            .collect()
    };
    assert_eq!(
        boundaries(&engine),
        vec!["b1".to_string(), "b2".to_string()]
    );
    migrate(
        &mut engine,
        &mut log,
        inst,
        second,
        &[("A", "A"), ("B", "D"), ("b2", "b2")],
    );
    assert_eq!(boundaries(&engine), vec!["b2".to_string()]);
    migrate(
        &mut engine,
        &mut log,
        inst,
        third,
        &[("A", "A"), ("D", "F"), ("b2", "b2")],
    );
    assert_eq!(compensable(&engine, inst), pairs(&[("A", "w")]));
    assert_eq!(boundaries(&engine), vec!["b2".to_string()]);
    assert_replays(&log, &engine, inst);

    // The throw journals the lifecycle of the subscription's OWN boundary.
    let job = engine
        .activate_jobs("next", "w", 1, 60_000, 0)
        .first()
        .expect("expected an activatable job of type next")
        .key;
    let events = engine.apply_command(Command::complete_job(job)).unwrap();
    let activated: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            Event::ElementActivated { element_id, .. }
                if element_id == "b1" || element_id == "b2" =>
            {
                Some(element_id.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(activated, vec!["b2"]);
}

/// `(task, compensation boundary, handler)`.
type CompTask<'a> = (&'a str, &'a str, &'a str);

/// `s -> {sub}[...tasks each with its comp boundary...] -> {next} -> throw -> e`
/// followed by an extra (empty) sub-process `{other}` before `{next}` when
/// given. Tasks run in sequence inside `{sub}`.
fn subs_with_tasks(process: &str, subs: &[(&str, &[CompTask])], next: &str) -> ProcessDefinition {
    let mut b = ProcessBuilder::new(process).start_event("s");
    let mut prev = "s".to_string();
    for &(sub, tasks) in subs {
        let ss = format!("{sub}_s");
        let se = format!("{sub}_e");
        b = b
            .sub_process(sub, &ss)
            .start_event(&ss)
            .contained_in(&ss, sub)
            .end_event(&se)
            .contained_in(&se, sub)
            .connect(&prev, sub);
        let mut inner = ss.clone();
        for &(task, boundary, handler) in tasks {
            b = b
                .service_task(task, task)
                .contained_in(task, sub)
                .compensation_boundary_event(boundary, task, handler)
                .contained_in(boundary, sub)
                .service_task(handler, handler)
                .contained_in(handler, sub)
                .connect(&inner, task);
            inner = task.to_string();
        }
        b = b.connect(&inner, &se);
        prev = sub.to_string();
    }
    b.service_task(next, "next")
        .compensation_throw_event("throw")
        .end_event("e")
        .connect(&prev, next)
        .connect(next, "throw")
        .connect("throw", "e")
        .build()
        .unwrap()
}

/// #1635 review: a completed sub-process explicitly mapped to `T` cannot be
/// re-derived onto `U` by a nested subscription's boundary mapping.
#[test]
fn rejects_a_derived_scope_that_conflicts_with_an_explicit_mapping() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        subs_with_tasks("p", &[("S", &[("A", "ab", "undoA")])], "B"),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        subs_with_tasks("p2", &[("T", &[]), ("U", &[("C", "cb", "undoC")])], "D"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let before = engine.instance(inst).unwrap().clone();
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("S", "T"), ("B", "D"), ("ab", "cb")]),
        ))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::MigrationCompensationFlowScopeChanged {
            instance_key: inst,
            process_id: "p".into(),
            source_flow_scope_id: "S".into(),
            target_flow_scope_id: "U".into(),
        }
    );
    assert_eq!(engine.instance(inst).unwrap(), &before);
}

/// #1635 review: two nested subscriptions cannot derive two different
/// targets for the same completed sub-process scope.
#[test]
fn rejects_two_subscriptions_deriving_different_targets_for_one_scope() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        subs_with_tasks(
            "p",
            &[("S", &[("A1", "a1b", "undoA1"), ("A2", "a2b", "undoA2")])],
            "B",
        ),
    );
    let target = deploy(
        &mut engine,
        &mut log,
        subs_with_tasks(
            "p2",
            &[
                ("T1", &[("C1", "c1b", "undoC1")]),
                ("T2", &[("C2", "c2b", "undoC2")]),
            ],
            "D",
        ),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A1");
    complete(&mut engine, &mut log, "A2");
    let before = engine.instance(inst).unwrap().clone();
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("B", "D"), ("a1b", "c1b"), ("a2b", "c2b")]),
        ))
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::MigrationCompensationFlowScopeChanged {
                ref source_flow_scope_id,
                ..
            } if source_flow_scope_id == "S"
        ),
        "{err:?}"
    );
    assert_eq!(engine.instance(inst).unwrap(), &before);
}

/// #1635 review: a completed activity's compensation boundaries get the same
/// no-merge validation as an active element's catch events — two source
/// boundaries mapped onto one target boundary would double-run its handler.
#[test]
fn rejects_two_completed_boundaries_merged_onto_one_target_boundary() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, two_boundaries("p", "u1", "u2", "B"));
    let target = deploy(&mut engine, &mut log, two_boundaries("p2", "v1", "v2", "D"));
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            pairs(&[("A", "A"), ("B", "D"), ("b1", "b1"), ("b2", "b1")]),
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::MigrationCatchEventsMerged { .. }),
        "{err:?}"
    );
    assert_eq!(
        compensable(&engine, inst),
        pairs(&[("A", "u1"), ("A", "u2")])
    );
}

/// #1635 review: an explicit activity mapping must agree with the activity its
/// mapped compensation boundary is attached to (stay attached).
#[test]
fn rejects_a_completed_activity_mapping_its_boundary_detaches_from() {
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
        root_level("p2", "C", "boundary2", "undoC", "D", "throw"),
    );
    let inst = start(&mut engine, &mut log, "p");
    complete(&mut engine, &mut log, "A");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target,
            // A -> D, but boundary2 is attached to C.
            pairs(&[("A", "D"), ("B", "D"), ("boundary1", "boundary2")]),
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::MigrationCatchEventDetached { .. }),
        "{err:?}"
    );
    assert_eq!(compensable(&engine, inst), pairs(&[("A", "undoA")]));
}
