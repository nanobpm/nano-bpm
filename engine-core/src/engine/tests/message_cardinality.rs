//! Message cardinality (#1676, Zeebe `MessageCorrelator`): one published
//! message correlates to at most ONE subscription per process instance. Two
//! tokens resting on separate armed copies of the same catch (here: two
//! activations of one event-based gateway, each subscribing to `M1` and `M2`
//! under the same correlation key) must be resumed one per message, not both
//! by the first message.
use super::*;

/// start -> fork(and) => merge(xor) -> gw(event-based) -> { onM1 | onM2 } -> end
///
/// Both fork branches pass the merge, so `gw` is activated twice in the root
/// scope: two independent races, each arming its own `onM1` and `onM2`
/// (the shape of the fuzz reproducer `Gen_8208a6f661`).
fn twin_gateways() -> ProcessDefinition {
    ProcessBuilder::new("twins")
        .start_event("start")
        .parallel_gateway("fork")
        .exclusive_gateway("merge")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end")
        .connect("start", "fork")
        .connect("fork", "merge")
        .connect("fork", "merge")
        .connect("merge", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end")
        .connect("onM2", "end")
        .build()
        .unwrap()
}

#[test]
fn one_message_correlates_once_per_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(twin_gateways()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "twins",
        vars(&[("key", Value::Str("k".into()))]),
    );

    let first = engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    // M1 triggers exactly one gateway; the other gateway's subscriptions
    // stay open (no catch is ever withdrawn: none was activated).
    assert_eq!(
        completions(&first, "onM1"),
        1,
        "M1 must resume exactly one waiting gateway"
    );
    assert_eq!(completions(&first, "onM2"), 0, "no onM2 was activated");

    let second = engine
        .apply_command(Command::correlate_message("M2", "k"))
        .unwrap();
    assert_eq!(
        completions(&second, "onM2"),
        1,
        "M2 must resume the other armed gateway"
    );
    assert_eq!(completions(&second, "onM1"), 0, "M1 does not run twice");
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn one_message_still_correlates_to_every_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(twin_gateways()))
        .unwrap();
    let a = create_instance_with_vars(
        &mut engine,
        "twins",
        vars(&[("key", Value::Str("k".into()))]),
    );
    let b = create_instance_with_vars(
        &mut engine,
        "twins",
        vars(&[("key", Value::Str("k".into()))]),
    );
    let events = engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    for inst in [a, b] {
        let n = events
            .iter()
            .filter(|e| matches!(e, Event::ElementCompleted { instance_key, element_id, .. } if *instance_key == inst && element_id.as_str() == "onM1"))
            .count();
        assert_eq!(n, 1, "instance {inst} must correlate M1 once");
    }
}

/// start -> gw(event-based) -> { onM1 | onM2 } -> end
///
/// A single event-based gateway activation arming both message catches: the
/// smallest shape that leaves a non-empty `races` map while the instance waits.
fn single_gateway() -> ProcessDefinition {
    ProcessBuilder::new("single")
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

/// start -> fork(and) => { gw(event-based) -> { onM1 | onM2 } -> end
///                         \-> hold(service task) -> stop(terminate end) }
///
/// The fork's second branch waits on a `hold` job, so the first branch's
/// gateway catches are provably armed before the branch then reaches a
/// TOP-LEVEL terminate end event: the terminate completes the
/// whole instance without emitting per-catch terminal events, so only the
/// whole-instance reducer can clear the armed races (#1676 review).
fn gateway_then_top_level_terminate() -> ProcessDefinition {
    ProcessBuilder::new("gw-stop")
        .start_event("start")
        .parallel_gateway("fork")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end")
        .service_task("hold", "hold")
        .terminate_end_event("stop")
        .connect("start", "fork")
        .connect("fork", "gw")
        .connect("fork", "hold")
        .connect("hold", "stop")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end")
        .connect("onM2", "end")
        .build()
        .unwrap()
}

/// Cancellation bypasses the per-catch terminal events, so the armed race
/// membership of a waiting gateway's catches must be cleared by the
/// whole-instance `ProcessInstanceTerminated` reducer — never stranded on the
/// terminal shell until eviction.
#[test]
fn cancel_clears_gateway_subscriptions() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(single_gateway()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "single",
        vars(&[("key", Value::Str("k".into()))]),
    );
    assert_eq!(
        engine.instance(inst).unwrap().gateway_subscriptions.len(),
        1,
        "the waiting gateway owns its subscriptions"
    );

    engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.state,
        ProcessInstanceState::Terminated,
        "the cancel terminates the instance"
    );
    assert!(
        instance.gateway_subscriptions.is_empty(),
        "terminal teardown must not retain gateway subscriptions: {:?}",
        instance.gateway_subscriptions
    );
}

/// A top-level terminate end event completes the instance while the sibling
/// branch's gateway catches are still armed — again bypassing their per-catch
/// terminal events — so the `ProcessInstanceCompleted` reducer must clear the
/// armed races the same way.
#[test]
fn top_level_terminate_end_clears_gateway_subscriptions() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(gateway_then_top_level_terminate()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "gw-stop",
        vars(&[("key", Value::Str("k".into()))]),
    );
    assert_eq!(
        engine.instance(inst).unwrap().gateway_subscriptions.len(),
        1,
        "the gateway waits while the terminate branch waits on its job"
    );

    let job_key = engine.activate_jobs("hold", "w", 1, 60_000, 0)[0].key;
    engine
        .apply_command(Command::complete_job_with(job_key, BTreeMap::new()))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.state,
        ProcessInstanceState::Completed,
        "the top-level terminate end completes the whole instance"
    );
    assert!(
        instance.gateway_subscriptions.is_empty(),
        "terminal teardown must not retain gateway subscriptions: {:?}",
        instance.gateway_subscriptions
    );
}

/// The persisted race identity must survive a serialized snapshot restore AND
/// a journal replay (#1676): two races armed before the restore still race
/// independently after it — one message resumes one race, and only that
/// race's losing sibling is withdrawn. Guards the `races` snapshot field and
/// the `ElementActivating.race` journal field against a serde/restoration
/// regression that a live-engine-only test cannot see.
#[cfg(feature = "serde")]
#[test]
fn twin_gateways_survive_snapshot_restore_and_journal_replay() {
    // Build the two-race armed state once, collecting the journal, then
    // exercise both restore paths from the same point.
    let mut source = Engine::new();
    let mut journal: Vec<Event> = source
        .apply_command(Command::DeployProcess(twin_gateways()))
        .unwrap();
    let created = source
        .apply_command(Command::create_instance_with(
            "twins",
            vars(&[("key", Value::Str("k".into()))]),
        ))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    journal.extend(created);
    assert_eq!(
        source.instance(inst).unwrap().gateway_subscriptions.len(),
        2,
        "two gateways wait before the restore"
    );

    // Path 1: restore from a SERIALIZED snapshot (round-tripped through JSON,
    // so a lost/renamed `races` field is caught, not just a clone).
    let snapshot_json = serde_json::to_vec(&source.snapshot()).expect("snapshot serializes");
    let from_snapshot =
        Engine::from_snapshot(serde_json::from_slice(&snapshot_json).expect("snapshot decodes"));

    // Path 2: rebuild by REPLAYING the journal (the migration-by-replay path,
    // #1071) — a lost `race` field on `ElementActivating` shows up here.
    let from_journal = Engine::replay(journal);

    for (path, mut engine) in [("snapshot", from_snapshot), ("journal", from_journal)] {
        assert_eq!(
            engine.instance(inst).unwrap().gateway_subscriptions.len(),
            2,
            "{path}: the gateway-owned subscriptions survive the restore"
        );
        let first = engine
            .apply_command(Command::correlate_message("M1", "k"))
            .unwrap();
        assert_eq!(
            completions(&first, "onM1"),
            1,
            "{path}: M1 resumes exactly one armed gateway after the restore"
        );
        assert_eq!(
            completions(&first, "onM2"),
            0,
            "{path}: the other gateway still waits after the restore"
        );
        let second = engine
            .apply_command(Command::correlate_message("M2", "k"))
            .unwrap();
        assert_eq!(
            completions(&second, "onM2"),
            1,
            "{path}: the other race is still winnable after the restore"
        );
        assert_eq!(
            engine.instance(inst).unwrap().state,
            ProcessInstanceState::Completed,
            "{path}: both races settle after the restore"
        );
    }
}
