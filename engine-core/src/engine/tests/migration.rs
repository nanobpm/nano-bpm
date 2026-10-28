//! `migration` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn modify_terminates_one_element_and_activates_another() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Token parked on service task `a`.
    let a_eik = active_eik_of(&engine, inst, "a");
    let job_a = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst)
        .unwrap()
        .key;

    let events = engine
        .apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variables: HashMap::new(),
                ..Default::default()
            }],
            vec![a_eik],
        ))
        .unwrap();

    // `a`'s job is cancelled and its element instance completed.
    assert!(events.contains(&Event::JobCanceled {
        job_key: job_a,
        instance_key: inst,
    }));
    assert_eq!(engine.job(job_a).unwrap().state, state::JobState::Canceled);
    // The instance stays active, now with a token (and fresh job) on `b`.
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
    assert!(engine
        .instance(inst)
        .unwrap()
        .active
        .values()
        .any(|id| id.as_str() == "b"));
    assert!(engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == inst && j.state == state::JobState::Created));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
}

#[test]
fn modify_terminating_the_last_token_terminates_the_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let charge_eik = active_eik_of(&engine, inst, "charge");
    let job = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst)
        .unwrap()
        .key;

    let events = engine
        .apply_command(Command::modify_instance(inst, vec![], vec![charge_eik]))
        .unwrap();

    // The token's job is cancelled and the instance is terminated (not
    // auto-completed) since nothing was activated to replace the last token.
    assert!(events.contains(&Event::JobCanceled {
        job_key: job,
        instance_key: inst,
    }));
    assert!(events.contains(&Event::ProcessInstanceTerminated { instance_key: inst }));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(engine.instance(inst).unwrap().active.is_empty());
}

#[test]
fn modify_activating_an_element_merges_global_variables() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let a_eik = active_eik_of(&engine, inst, "a");

    let mut vars = HashMap::new();
    vars.insert("approved".to_string(), Value::Bool(true));
    let events = engine
        .apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variables: vars,
                ..Default::default()
            }],
            vec![a_eik],
        ))
        .unwrap();

    assert!(events.iter().any(|e| matches!(
        e,
        Event::VariablesUpdated { instance_key, variables }
            if *instance_key == inst && variables.get("approved") == Some(&Value::Bool(true))
    )));
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("approved"),
        Some(&Value::Bool(true))
    );
}

#[test]
fn modify_rejects_unknown_instance_and_element_ids() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();

    // Unknown instance.
    assert_eq!(
        engine.apply_command(Command::modify_instance(999, vec![], vec![])),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    );

    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let a_eik = active_eik_of(&engine, inst, "a");

    // Unknown activate element id — rejected, leaving the instance untouched.
    assert_eq!(
        engine.apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction {
                element_id: "nope".into(),
                variables: HashMap::new(),
                ..Default::default()
            }],
            vec![],
        )),
        Err(EngineError::ElementNotFound {
            instance_key: inst,
            element_id: "nope".into(),
        })
    );

    // Unknown terminate element-instance key.
    assert_eq!(
        engine.apply_command(Command::modify_instance(inst, vec![], vec![424242])),
        Err(EngineError::ElementInstanceNotFound {
            instance_key: inst,
            element_instance_key: 424242,
        })
    );

    // The rejected commands changed nothing: the token is still on `a`.
    assert_eq!(active_eik_of(&engine, inst, "a"), a_eik);
}

#[test]
fn modify_survives_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(two_service_tasks()))
            .unwrap(),
    );
    let inst = {
        let events = engine
            .apply_command(Command::create_instance("two"))
            .unwrap();
        let k = events.iter().find_map(|e| e.instance_key()).unwrap();
        log.extend(events);
        k
    };
    let a_eik = active_eik_of(&engine, inst, "a");
    log.extend(
        engine
            .apply_command(Command::modify_instance(
                inst,
                vec![ActivateElementInstruction {
                    element_id: "b".into(),
                    variables: HashMap::new(),
                    ..Default::default()
                }],
                vec![a_eik],
            ))
            .unwrap(),
    );

    let recovered = Engine::replay(log);
    assert_eq!(
        recovered.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
    assert!(recovered
        .instance(inst)
        .unwrap()
        .active
        .values()
        .any(|id| id.as_str() == "b"));
}

#[test]
fn migration_remaps_active_service_task_and_its_job() {
    // A token parked on service task `a` of `source` is migrated to `b` of
    // `target`: the instance now belongs to `target`, its active element and its
    // pending job are re-pointed at `b`, and the job keeps its type.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));

    let inst = create_instance_key(&mut engine, "source");
    assert_eq!(pending_job_element(&engine, inst), "a");

    let events = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceMigrated { .. })),
        "a ProcessInstanceMigrated fact is emitted"
    );

    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.process_id, "target",
        "instance re-homed to target id"
    );
    assert_eq!(
        instance.process_definition_key, target_key,
        "instance re-pinned to the target definition version, so `definition_for` \
         resolves execution against the migrated-to model, not the source"
    );
    assert!(
        instance.active.values().any(|e| e == "b"),
        "active token re-pointed at target element b"
    );
    assert!(
        !instance.active.values().any(|e| e == "a"),
        "no active token still points at source element a"
    );
    assert_eq!(
        pending_job_element(&engine, inst),
        "b",
        "the pending job is re-pointed at b"
    );

    // The in-flight count moved from source to target.
    assert_eq!(
        engine.state().inflight_by_process.get("source").copied(),
        None,
        "source has no live instances"
    );
    assert_eq!(
        engine.state().inflight_by_process.get("target").copied(),
        Some(1),
        "target has one live instance"
    );
}

#[test]
fn migration_completes_via_remapped_job() {
    // After migrating, completing the (re-pointed) job drives the token to the
    // TARGET's end event, so the instance completes cleanly under its new
    // definition — proving the remap is executable, not cosmetic.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();

    let job = engine
        .activate_jobs("work", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.instance_key == inst)
        .unwrap();
    assert_eq!(job.element_id, "b");
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed,
        "instance completed through the target's end event"
    );
}

/// A `par` process `s -> split =< a, b >= join -> e` whose branch `a` has
/// already reached the join (so the join is half-open) while branch `b` is
/// still parked, migrated to an identically-shaped `target` with renamed
/// elements. The join's arrival maps (`join_flow_arrivals`, `join_counts`) and
/// `join_instances` are keyed by the join gateway's element id, so the applier
/// must remap them *together*; a regression that remapped only the counts left
/// `join_instances` pointing at the source id, desyncing `join_eik` from the
/// counts after migration. The counted flow `a -> join` names its source, so
/// `a` is mapped too (Zeebe requires a mapping for every taken flow, #1233).
#[test]
fn migration_remaps_both_parallel_join_maps_together() {
    fn par_join_process(id: &str, split: &str, a: &str, b: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway(split)
            .service_task(a, "ja")
            .service_task(b, "jb")
            .parallel_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, a)
            .connect(split, b)
            .connect(a, join)
            .connect(b, join)
            .connect(join, "e")
            .build()
            .unwrap()
    }

    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        par_join_process("source", "split", "a", "b", "join"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        par_join_process("target", "split2", "a2", "b2", "join2"),
    );

    let inst = create_instance_key(&mut engine, "source");
    // Drive branch `a` into the join so it opens and waits for branch `b`.
    complete_one(&mut engine, "ja");

    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_instances.contains_key("join"),
        "the join is half-open on the source id before migration"
    );
    assert_eq!(
        instance.join_flows_taken("join"),
        1,
        "one branch has arrived at the join before migration"
    );

    // Active elements at this point: the parked task `b` and the open `join`.
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("a".to_string(), "a2".to_string()),
                ("b".to_string(), "b2".to_string()),
                ("join".to_string(), "join2".to_string()),
            ],
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_flow_arrivals.contains_key("join2")
            && !instance.join_flow_arrivals.contains_key("join"),
        "join_flow_arrivals re-keyed onto the target join id"
    );
    assert!(
        instance.join_instances.contains_key("join2")
            && !instance.join_instances.contains_key("join"),
        "join_instances re-keyed onto the target join id (kept in sync with the counts)"
    );
    let arrivals = &instance.join_flow_arrivals["join2"];
    assert_eq!(
        arrivals.count(&IncomingFlow {
            from: "a2".to_string(),
            ordinal: 0
        }),
        1,
        "the counted flow is renamed to the target's `a2 -> join2`"
    );

    // Executable proof: completing the remaining branch fires the join once and
    // the instance completes cleanly under the target definition.
    complete_one(&mut engine, "jb");
    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed,
        "the remapped join fires and the instance completes under the target"
    );
}

/// An already-*open* parallel join (branch `a` has arrived, so a partial count
/// of 1 is durably recorded) cannot be migrated onto a target join with a
/// *different* number of incoming flows: the partial count is compared against
/// the target definition's incoming-flow count at fire time, so a 2-flow join
/// half-open at count 1, remapped onto a 3-flow join, would deadlock (its third
/// flow never arrives) — and a wider-to-narrower remap would early-fire.
/// Reject with `MigratedParallelJoinArityChanged`. Red/Green guard for the
/// failure-mode class "durable count state whose threshold lives in the
/// definition being migrated away".
#[test]
fn migration_rejects_open_join_with_different_incoming_arity() {
    fn par_join_2(id: &str, split: &str, a: &str, b: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway(split)
            .service_task(a, "ja")
            .service_task(b, "jb")
            .parallel_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, a)
            .connect(split, b)
            .connect(a, join)
            .connect(b, join)
            .connect(join, "e")
            .build()
            .unwrap()
    }
    // Target join `join2` has THREE incoming flows, versus the source's two.
    fn par_join_3(id: &str, split: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway(split)
            .service_task("a2", "ja")
            .service_task("b2", "jb")
            .service_task("c2", "jc")
            .parallel_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, "a2")
            .connect(split, "b2")
            .connect(split, "c2")
            .connect("a2", join)
            .connect("b2", join)
            .connect("c2", join)
            .connect(join, "e")
            .build()
            .unwrap()
    }

    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, par_join_2("source", "split", "a", "b", "join"));
    let target_key = deploy_for_migration(&mut engine, par_join_3("target", "split2", "join2"));

    let inst = create_instance_key(&mut engine, "source");
    // Open the join: branch `a` arrives, leaving the join half-open at count 1.
    complete_one(&mut engine, "ja");
    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_instances.contains_key("join"),
        "precondition: the join is open before migration"
    );

    // Active elements now: parked task `b` and the open `join`. Map both, but
    // point the open join at the 3-flow target join.
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("b".to_string(), "b2".to_string()),
                ("join".to_string(), "join2".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(
        matches!(
            &err,
            EngineError::MigratedParallelJoinArityChanged {
                source_element_id,
                target_element_id,
                source_incoming_count: 2,
                target_incoming_count: 3,
                ..
            } if source_element_id == "join" && target_element_id == "join2"
        ),
        "an open 2-flow join mapped onto a 3-flow join is rejected, got {err:?}"
    );

    // The rejection is all-or-nothing: the instance is untouched (still on the
    // source definition, join still open on the source id).
    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.process_id, "source", "instance not migrated");
    assert!(
        instance.join_instances.contains_key("join"),
        "the open join is left intact on the source id"
    );
}

/// An open join has counted a token on `a -> join`, but the target's join has
/// no incoming flow from `a` (nor from anything `a` is mapped to). The migrated
/// join would count a flow that does not exist, so the migration is rejected,
/// as Zeebe rejects an unmapped taken sequence flow into a parallel or
/// inclusive gateway (`ERROR_TAKEN_SEQUENCE_FLOW_NOT_MAPPED`, #1233, #1237).
#[test]
fn migration_rejects_open_join_whose_counted_flow_is_missing_in_target() {
    assert_counted_join_flow_must_be_mapped(ProcessBuilder::parallel_gateway);
}

#[test]
fn migration_rejects_open_inclusive_join_whose_counted_flow_is_missing_in_target() {
    assert_counted_join_flow_must_be_mapped(ProcessBuilder::inclusive_gateway);
}

fn assert_counted_join_flow_must_be_mapped(
    gateway: fn(ProcessBuilder, &'static str) -> ProcessBuilder,
) {
    let par_join = |id: &str, a: &str| {
        let b = ProcessBuilder::new(id).start_event("s");
        let b = gateway(b, "split")
            .service_task(a, "ja")
            .service_task("b", "jb");
        gateway(b, "join")
            .end_event("e")
            .connect("s", "split")
            .connect("split", a)
            .connect("split", "b")
            .connect(a, "join")
            .connect("b", "join")
            .connect("join", "e")
            .build()
            .unwrap()
    };
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, par_join("source", "a"));
    let target_key = deploy_for_migration(&mut engine, par_join("target", "a2"));
    let inst = create_instance_key(&mut engine, "source");
    complete_one(&mut engine, "ja");

    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("b".to_string(), "b".to_string()),
                ("join".to_string(), "join".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(
        matches!(
            &err,
            EngineError::MigratedJoinFlowMissing {
                source_element_id,
                flow_source_element_id,
                ..
            } if source_element_id == "join" && flow_source_element_id == "a"
        ),
        "a counted flow with no target counterpart is rejected, got {err:?}"
    );
    assert_eq!(engine.instance(inst).unwrap().process_id, "source");

    // Mapping the flow's source onto the target's `a2` resolves it.
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("a".to_string(), "a2".to_string()),
                ("b".to_string(), "b".to_string()),
                ("join".to_string(), "join".to_string()),
            ],
        ))
        .unwrap();
    complete_one(&mut engine, "jb");
    assert_eq!(
        engine.instance(inst).unwrap().state,
        crate::state::ProcessInstanceState::Completed
    );
}

/// An **inclusive**-gateway join reuses the same `join_instances` bookkeeping as
/// a parallel join. It fires once its identified taken flows cover the
/// incoming flows, or when no active path can reach it (#1241). The per-flow
/// migration check guarantees every identified arrival names a target incoming
/// flow, so the covering test cannot early-fire under a different arity: an
/// open inclusive join with only identified arrivals, remapped onto a target
/// join with a *different* incoming arity, is compatible and must migrate. Red/Green guard that the new element (#1168)
/// did not silently inherit `MigratedParallelJoinArityChanged` by sharing the
/// join maps.
#[test]
fn migration_allows_open_inclusive_join_with_different_incoming_arity() {
    fn inc_join_2(id: &str, split: &str, a: &str, b: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .inclusive_gateway(split)
            .service_task(a, "ja")
            .service_task(b, "jb")
            .inclusive_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, a)
            .connect(split, b)
            .connect(a, join)
            .connect(b, join)
            .connect(join, "e")
            .build()
            .unwrap()
    }
    // Target inclusive join `join2` has THREE incoming flows, versus the two of
    // the source — an arity change that would reject a *parallel* join.
    fn inc_join_3(id: &str, split: &str, join: &str) -> ProcessDefinition {
        ProcessBuilder::new(id)
            .start_event("s")
            .inclusive_gateway(split)
            .service_task("a2", "ja")
            .service_task("b2", "jb")
            .service_task("c2", "jc")
            .inclusive_gateway(join)
            .end_event("e")
            .connect("s", split)
            .connect(split, "a2")
            .connect(split, "b2")
            .connect(split, "c2")
            .connect("a2", join)
            .connect("b2", join)
            .connect("c2", join)
            .connect(join, "e")
            .build()
            .unwrap()
    }

    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, inc_join_2("source", "split", "a", "b", "join"));
    let target_key = deploy_for_migration(&mut engine, inc_join_3("target", "split2", "join2"));

    let inst = create_instance_key(&mut engine, "source");
    // Open the join: branch `a` arrives, leaving it half-open (branch `b` still
    // parked and able to reach the join, so it has not fired).
    complete_one(&mut engine, "ja");
    let instance = engine.instance(inst).unwrap();
    assert!(
        instance.join_instances.contains_key("join"),
        "precondition: the inclusive join is open before migration"
    );

    // Map the parked branch `b` and the open join onto the 3-flow target join,
    // plus the counted flow's source `a` (a taken flow must be mapped, #1237).
    // Unlike a parallel join, the arity change must be accepted.
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("a".to_string(), "a2".to_string()),
                ("b".to_string(), "b2".to_string()),
                ("join".to_string(), "join2".to_string()),
            ],
        ))
        .expect("an open inclusive join tolerates an incoming-arity change on migration");

    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.process_id, "target", "instance migrated");
    assert!(
        instance.join_instances.contains_key("join2"),
        "the open inclusive join is re-pointed at the target id"
    );

    // The migrated instance still drives to completion: the parked branch `b`
    // (now `b2`) finishes and the inclusive join synchronises and completes.
    complete_one(&mut engine, "jb");
    assert!(
        engine.is_completed(inst),
        "the migrated inclusive join synchronises its branches and completes"
    );
}

/// An inclusive join accepts once its taken flows cover its incoming flows
/// (#1241). An *unidentified* arrival (`join_counts`: a modification that
/// activated the join directly) names no flow, so it is only a valid "one taken
/// flow" against the arity it was counted under — exactly the parallel-join
/// hazard. Migrating it onto a target join with fewer incoming flows would
/// early-fire, so it must be rejected like a parallel join.
#[test]
fn migration_rejects_inclusive_join_with_unidentified_arrival_and_different_arity() {
    fn inc_join(id: &str, branches: &[&str]) -> ProcessDefinition {
        let mut b = ProcessBuilder::new(id)
            .start_event("s")
            .inclusive_gateway("split");
        for &t in branches {
            b = b.service_task(t, format!("j{t}"));
        }
        let mut b = b
            .inclusive_gateway("join")
            .end_event("e")
            .connect("s", "split");
        for &t in branches {
            b = b.connect("split", t).connect(t, "join");
        }
        b.connect("join", "e").build().unwrap()
    }

    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, inc_join("source", &["a", "b", "c"]));
    let target_key = deploy_for_migration(&mut engine, inc_join("target", &["a", "b"]));
    let inst = create_instance_key(&mut engine, "source");
    // Park an unidentified arrival on the join and retire branch `c`, which
    // has no counterpart in the target.
    let c_eik = active_eik_of(&engine, inst, "c");
    engine
        .apply_command(Command::modify_instance(
            inst,
            vec![crate::command::ActivateElementInstruction {
                element_id: "join".to_string(),
                variables: Default::default(),
                ..Default::default()
            }],
            vec![c_eik],
        ))
        .unwrap();
    let instance = engine.instance(inst).unwrap();
    assert_eq!(
        instance.join_counts.get("join"),
        Some(&1),
        "precondition: the join holds one unidentified arrival"
    );

    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("a".to_string(), "a".to_string()),
                ("b".to_string(), "b".to_string()),
                ("join".to_string(), "join".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::MigratedParallelJoinArityChanged {
                source_incoming_count: 3,
                target_incoming_count: 2,
                ..
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn migration_rejects_unknown_instance() {
    let mut engine = Engine::new();
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let err = engine
        .apply_command(Command::migrate_instance(
            999,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::InstanceNotFound { instance_key: 999 }
    ));
}

#[test]
fn migration_rejects_unknown_target_definition() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            424_242,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::TargetProcessDefinitionNotFound {
            process_definition_key: 424_242
        }
    ));
}

#[test]
fn migration_rejects_duplicate_source_mapping() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("a".to_string(), "b".to_string()),
                ("a".to_string(), "b".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::DuplicateMappingSourceElement { element_id, .. } if element_id == "a"
    ));
}

#[test]
fn migration_rejects_unknown_source_element() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("nope".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::MappingSourceElementNotFound { element_id, .. } if element_id == "nope"
    ));
}

#[test]
fn migration_rejects_unknown_target_element() {
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "nope".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::MappingTargetElementNotFound { element_id, .. } if element_id == "nope"
    ));
}

#[test]
fn migration_rejects_unmapped_active_element() {
    // No mapping for the active service task `a` → the active token would be
    // orphaned, so migration is rejected (Zeebe 409 parity).
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(inst, target_key, vec![]))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::UnmappedActiveElement { element_id, .. } if element_id == "a"
    ));
}

#[test]
fn migration_rejects_type_change() {
    // Mapping the active service task to a user task changes the element type →
    // rejected (Zeebe 409 parity).
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, migratable_task_process("source", "a", "work"));
    let target_key = deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("target")
            .start_event("start")
            .user_task("b")
            .end_event("end")
            .connect("start", "b")
            .connect("b", "end")
            .build()
            .unwrap(),
    );
    let inst = create_instance_key(&mut engine, "source");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::MappedElementTypeChanged {
            source_element_id,
            target_element_id,
            ..
        } if source_element_id == "a" && target_element_id == "b"
    ));
}

#[test]
fn migration_deletes_the_armed_timer_of_an_unmapped_boundary_event() {
    // Zeebe parity (#1374): an armed boundary event whose catch event is not
    // mapped is not carried over — its timer is canceled before the migration
    // fact, and the target (which has no boundary) arms nothing.
    let mut engine = Engine::new();
    deploy_for_migration(
        &mut engine,
        ProcessBuilder::new("source")
            .start_event("start")
            .service_task("a", "work")
            .timer_boundary_event("boundary", "a", 60_000)
            .end_event("end")
            .end_event("caught")
            .connect("start", "a")
            .connect("a", "end")
            .connect("boundary", "caught")
            .build()
            .unwrap(),
    );
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));
    let inst = create_instance_key(&mut engine, "source");
    let log = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();
    assert!(
        matches!(log.first(), Some(Event::TimerCanceled { element_id, .. }) if element_id == "a")
    );
    assert!(matches!(
        log.get(1),
        Some(Event::ProcessInstanceMigrated { .. })
    ));
    assert_eq!(log.len(), 2);
    assert!(engine
        .state()
        .timers
        .values()
        .all(|t| t.state != state::TimerState::Created));
}

#[test]
fn unsupported_migration_reason_admits_every_boundary_kind_and_sub_process() {
    // #1374: boundary events and embedded sub-processes are migratable (their
    // runtime is planned in `engine/migration.rs`), so no boundary kind and no
    // sub-process may be rejected by the element-class guard any more. Call
    // activities and event-based gateways stay rejected until their slice lands.
    use crate::model::ElementKind;
    let supported = [
        ElementKind::ErrorBoundaryEvent {
            attached_to: "host".to_string(),
            error_code: "E".to_string(),
        },
        ElementKind::TimerBoundaryEvent {
            attached_to: "host".to_string(),
            duration_millis: 1_000,
            interrupting: true,
            repeating: false,
        },
        ElementKind::MessageBoundaryEvent {
            attached_to: "host".to_string(),
            message_name: "M".to_string(),
            correlation_key: "k".to_string(),
            interrupting: true,
        },
        ElementKind::SignalBoundaryEvent {
            attached_to: "host".to_string(),
            signal_name: "S".to_string(),
            interrupting: true,
        },
        ElementKind::ConditionalBoundaryEvent {
            attached_to: "host".to_string(),
            condition: "=true".to_string(),
            interrupting: true,
        },
        ElementKind::EscalationBoundaryEvent {
            attached_to: "host".to_string(),
            escalation_code: "OVERLOAD".to_string(),
            interrupting: true,
        },
        ElementKind::SubProcess {
            start_event: "s".to_string(),
        },
    ];
    for kind in supported {
        assert_eq!(
            unsupported_migration_reason(&kind),
            None,
            "{kind:?} is migratable"
        );
    }
    assert!(unsupported_migration_reason(&ElementKind::EventBasedGateway).is_some());
}

#[test]
fn migration_rejects_active_multi_instance_body() {
    // An active parallel multi-instance body keeps per-element bookkeeping in
    // `ProcessInstance.multi_instances` that this phase does not remap, so
    // migration is rejected as "not supported yet" (Zeebe parity), matching the
    // boundary-event rejection above.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let target = ProcessBuilder::new("mi-target")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item * 2".to_string()),
                completion_condition: None,
                sequential: false,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);
    let inst = create_instance_with_vars(
        &mut engine,
        "mi",
        vars(&[(
            "items",
            Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
        )]),
    );
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("each".to_string(), "each".to_string())],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::UnsupportedMigration { element_id, reason, .. }
            if element_id == "each" && reason.contains("multi-instance")
    ));
}

#[test]
fn migration_rejects_active_token_inside_a_nested_flow_scope() {
    // Defect class (Zeebe's "flow scope unchanged" precondition): an active
    // token resting inside an embedded sub-process lives in a non-root flow
    // scope. The applier re-points element ids but does NOT remap the scope
    // tree (`scopes` / `scope_parents` / `scope_variables`), so migrating such
    // an instance would leave its scope tree pointing at the source structure.
    // Since #1374 the scope tree is preserved and an inner token may migrate
    // only together with its (mapped) enclosing sub-process.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    // The instance parks a token on the `work` job INSIDE sub-process `sub`, so
    // `instance.scopes` is non-empty.
    let inst =
        create_instance_with_vars(&mut engine, "sub-scope", vars(&[("seed", Value::Int(4))]));
    assert!(
        !engine.instance(inst).unwrap().scopes.is_empty(),
        "precondition: the token must sit inside a non-root flow scope"
    );

    // A flat target carrying the inner task at process level.
    let target = ProcessBuilder::new("flat-target")
        .start_event("s")
        .service_task("inner", "work")
        .end_event("e")
        .connect("s", "inner")
        .connect("inner", "e")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);

    // The active sub-process itself has no mapping → rejected (Zeebe).
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![("inner".to_string(), "inner".to_string())],
        ))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::UnmappedActiveElement {
            instance_key: inst,
            element_id: "sub".to_string(),
        }
    );
    // Mapping the sub-process onto the flat target's task moves `inner` out of
    // its flow scope → rejected by `requireUnchangedFlowScope`.
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            vec![
                ("inner".to_string(), "inner".to_string()),
                ("sub".to_string(), "inner".to_string()),
            ],
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::MappedElementTypeChanged { .. }),
        "got {err:?}"
    );
    // The instance stays on its source definition — nothing was migrated.
    assert_eq!(engine.instance(inst).unwrap().process_id, "sub-scope");
}

/// #1374 scaffold: the Zeebe-parity `ModifyInstance` fields the engine does not
/// act on yet are rejected explicitly (never silently ignored), atomically.
#[test]
fn modify_rejects_not_yet_supported_instruction_fields() {
    use crate::command::{
        AncestorScopeInstruction, MoveInstruction, MoveSource, VariableInstruction,
    };
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let a_eik = active_eik_of(&engine, inst, "a");

    let modify = |activate: Vec<ActivateElementInstruction>,
                  by_id: Vec<String>,
                  moves: Vec<MoveInstruction>| Command::ModifyInstance {
        instance_key: inst,
        activate_instructions: activate,
        terminate_instructions: vec![a_eik],
        terminate_element_ids: by_id,
        move_instructions: moves,
    };
    let cases = [
        (
            modify(
                vec![ActivateElementInstruction {
                    element_id: "b".into(),
                    ancestor_element_instance_key: Some(inst),
                    ..Default::default()
                }],
                vec![],
                vec![],
            ),
            "activate_instructions[].ancestor_element_instance_key",
        ),
        (
            modify(
                vec![ActivateElementInstruction {
                    element_id: "b".into(),
                    variable_instructions: vec![VariableInstruction {
                        variables: HashMap::new(),
                        scope_id: Some("b".into()),
                    }],
                    ..Default::default()
                }],
                vec![],
                vec![],
            ),
            "activate_instructions[].variable_instructions",
        ),
        (
            modify(vec![], vec!["a".into()], vec![]),
            "terminate_element_ids",
        ),
        (
            modify(
                vec![],
                vec![],
                vec![MoveInstruction {
                    source: MoveSource::ById("a".into()),
                    target_element_id: "b".into(),
                    ancestor_scope: AncestorScopeInstruction::SourceParent,
                    variable_instructions: vec![],
                }],
            ),
            "move_instructions",
        ),
    ];
    for (command, instruction) in cases {
        let err = engine.apply_command(command).unwrap_err();
        assert_eq!(
            err,
            EngineError::ModificationInstructionNotYetSupported {
                instance_key: inst,
                instruction,
            }
        );
        assert!(err.to_string().contains("not yet supported"), "{err}");
        // Atomic: the bundled terminate of `a` did not happen.
        assert_eq!(active_eik_of(&engine, inst, "a"), a_eik);
    }
}

/// #1374 scaffold: a suspended instance exists but is in the wrong state for
/// modification — `InstanceNotActive` (409), not `InstanceNotFound` (404).
#[test]
fn modify_of_suspended_instance_is_rejected_as_not_active() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(two_service_tasks()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("two"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    engine
        .apply_command(Command::suspend_instance(inst))
        .unwrap();
    let err = engine
        .apply_command(Command::modify_instance(inst, vec![], vec![]))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::InstanceNotActive { instance_key, process_id, .. }
            if *instance_key == inst && process_id == "two"),
        "{err:?}"
    );
    assert_eq!(
        engine.apply_command(Command::modify_instance(999_999, vec![], vec![])),
        Err(EngineError::InstanceNotFound {
            instance_key: 999_999
        })
    );
}
