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
        canceled_at: 0,
        deadline: None,
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
        canceled_at: 0,
        deadline: None,
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
    // sub-process may be rejected by the element-class guard any more; #1553
    // added call activities (the child instance keeps its parent link).
    // Event-based gateways stay rejected until their slice lands.
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
        ElementKind::CallActivity {
            called_process_id: "callee".to_string(),
            propagate_all_parent_variables: true,
            propagate_all_child_variables: true,
        },
    ];
    for kind in supported {
        assert_eq!(
            unsupported_migration_reason(&kind),
            None,
            "{kind:?} is migratable"
        );
    }
    assert_eq!(
        unsupported_migration_reason(&ElementKind::EventBasedGateway),
        None
    );
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
                cancel_remaining_instances: true,
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

/// #1374 scaffold: `move_instructions` (not acted on yet) are rejected explicitly (never silently ignored), atomically.
#[test]
fn modify_rejects_not_yet_supported_instruction_fields() {
    use crate::command::{AncestorScopeInstruction, MoveInstruction, MoveSource};
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
    let cases = [(
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
    )];
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

#[test]
fn migration_rejects_instance_with_compensation_runtime() {
    // Copilot on #1418: `compensation_waits` (throw id + pending handler ids)
    // and saved `compensable` entries are keyed by source element ids the
    // applier does not remap, so a migrated in-flight compensation would never
    // release its throw. Reject until supported (fail closed).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_compensation()))
        .unwrap();
    let mut target = process_with_compensation();
    target.id = "trip-v2".to_string();
    let target_key = deploy_for_migration(&mut engine, target);
    let created = engine
        .apply_command(Command::create_instance("trip"))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    let book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    engine.apply_command(Command::complete_job(book)).unwrap();
    let mut mapping: Vec<(String, String)> = engine
        .instance(inst)
        .unwrap()
        .active
        .values()
        .map(|e| (e.clone(), e.clone()))
        .collect();
    mapping.sort();
    mapping.dedup();
    let err = engine
        .apply_command(Command::migrate_instance(inst, target_key, mapping))
        .unwrap_err();
    assert!(
        matches!(
            &err,
            EngineError::UnsupportedMigration { reason, .. } if reason.contains("compensation")
        ),
        "got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// #1553 — migration of an active call activity (Zeebe `MigrateCallActivityTest`
// parity): the parent's call-activity element instance is remapped; the child
// process instance is untouched and keeps its parent link, so its completion
// still releases the parent's (now remapped) call-activity token, which
// continues on the TARGET model.
// ---------------------------------------------------------------------------

/// A call-activity parent `id`: start -> `call` (calling `callee`) -> `after`
/// (a service task on job type `after-work`) -> end. Naming the trailing task
/// per-model lets a test prove the parent continued on the TARGET's flow.
fn call_activity_parent(id: &str, call: &str, callee: &str, after: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .call_activity(call, callee)
        .service_task(after, "after-work")
        .end_event("end")
        .connect("start", call)
        .connect(call, after)
        .connect(after, "end")
        .build()
        .unwrap()
}

/// Creates an instance of `parent_id` parked in its call activity and returns
/// `(parent_instance_key, call_eik, child_instance_key)`.
fn create_calling_instance(
    engine: &mut Engine,
    parent_id: &str,
    call: &str,
    callee_id: &str,
) -> (Key, Key, Key) {
    let created = engine
        .apply_command(Command::create_instance(parent_id))
        .unwrap();
    let parent = parent_key_of(&created, parent_id);
    let call_eik = active_eik_of(engine, parent, call);
    let child = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                parent_process_instance_key,
                parent_element_instance_key,
                ..
            } if process_id == callee_id => Some((
                *instance_key,
                *parent_process_instance_key,
                *parent_element_instance_key,
            )),
            _ => None,
        })
        .map(|(key, ppik, peik)| {
            assert_eq!(ppik, Some(parent), "child links back to the parent");
            assert_eq!(
                peik,
                Some(call_eik),
                "child links back to the call activity"
            );
            key
        })
        .expect("a child instance of the callee was created");
    (parent, call_eik, child)
}

#[test]
fn migration_remaps_active_call_activity_and_keeps_child_attached() {
    // Zeebe `MigrateCallActivityTest.shouldMigrateCallActivity` /
    // `shouldAllowToContinueFlowAfterMigratingParentProcessInstance`: an
    // instance waiting in call activity `a` migrates onto a target whose call
    // activity is `b` (calling the same child). The child instance is NOT
    // migrated — it stays on its own definition and keeps its parent link;
    // when it completes afterwards, the parent's remapped token completes and
    // takes the TARGET's outgoing flow.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, phase_process("child", "child-work"));
    deploy_for_migration(
        &mut engine,
        call_activity_parent("source", "a", "child", "done_a"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        call_activity_parent("target", "b", "child", "done_b"),
    );

    let (parent, call_eik, child) = create_calling_instance(&mut engine, "source", "a", "child");
    assert_eq!(
        engine.instance(child).unwrap().process_id,
        "child",
        "precondition: the child runs the callee definition"
    );

    let events = engine
        .apply_command(Command::migrate_instance(
            parent,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();

    // The migration fact names the kept-attached child (Zeebe's
    // `childInstanceKeys` audit on `ProcessInstanceMigrated`).
    let migrated = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceMigrated {
                child_instances, ..
            } => Some(child_instances.clone()),
            _ => None,
        })
        .expect("a ProcessInstanceMigrated fact is emitted");
    assert_eq!(
        migrated,
        vec![crate::event::MigratedChildInstance {
            call_activity_element_instance_key: call_eik,
            child_instance_key: child,
            target_element_id: "b".to_string(),
        }],
        "the child instance is reported as kept attached to the migrated call activity"
    );

    // The parent's token moved to the target call activity; the child is
    // untouched — same instance, same definition, same parent linkage.
    let instance = engine.instance(parent).unwrap();
    assert_eq!(instance.process_id, "target");
    assert_eq!(
        instance.process_definition_key, target_key,
        "the parent is re-pinned to the target definition"
    );
    assert_eq!(
        instance.active.get(&call_eik).map(String::as_str),
        Some("b")
    );
    let child_instance = engine.instance(child).unwrap();
    assert_eq!(child_instance.process_id, "child");
    assert_eq!(child_instance.parent_process_instance_key, Some(parent));
    assert_eq!(
        child_instance.parent_element_instance_key,
        Some(call_eik),
        "the child keeps its parent link across the migration"
    );

    // The child completes afterwards; the parent's remapped call-activity token
    // is released and continues on the TARGET model (`b -> done_b`), running the
    // parent to completion under its new definition.
    let events = complete_one(&mut engine, "child-work");
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ProcessInstanceCompleted { instance_key } if *instance_key == child
        )),
        "the child instance completed"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "b" && to == "done_b"
        )),
        "the parent continues on the target model's flow out of the remapped call activity"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementActivated { element_id, .. } if element_id == "done_b"
        )),
        "the target's follow-up task is activated"
    );
    complete_one(&mut engine, "after-work");
    assert!(
        engine.is_completed(parent),
        "the parent completes under the target definition"
    );
}

#[test]
fn migration_of_call_activity_survives_replay() {
    // The migration is journaled: replaying the log must rebuild the remapped
    // parent AND the untouched child link, so a child completing after a
    // snapshot/restart still releases the parent's parked token.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    for def in [
        phase_process("child", "child-work"),
        call_activity_parent("source", "a", "child", "done_a"),
        call_activity_parent("target", "b", "child", "done_b"),
    ] {
        log.extend(engine.apply_command(Command::DeployProcess(def)).unwrap());
    }
    let target_key = {
        let mut k = None;
        for e in &log {
            if let Event::ProcessDeployed {
                process_definition_key,
                ..
            } = e
            {
                k = Some(*process_definition_key);
            }
        }
        k.unwrap()
    };

    let created = engine
        .apply_command(Command::create_instance("source"))
        .unwrap();
    let parent = parent_key_of(&created, "source");
    let child = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                ..
            } if process_id == "child" => Some(*instance_key),
            _ => None,
        })
        .expect("child created");
    log.extend(created);
    log.extend(
        engine
            .apply_command(Command::migrate_instance(
                parent,
                target_key,
                vec![("a".to_string(), "b".to_string())],
            ))
            .unwrap(),
    );

    // Replay from the journal: the rebuilt engine must hold the migrated state.
    let mut recovered = Engine::replay(log);
    let instance = recovered.instance(parent).unwrap();
    assert_eq!(instance.process_id, "target");
    assert!(instance.active.values().any(|e| e == "b"));
    let child_instance = recovered.instance(child).unwrap();
    assert_eq!(child_instance.parent_process_instance_key, Some(parent));

    // And the rebuilt engine drives the completion end to end.
    let events = complete_one(&mut recovered, "child-work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "b" && to == "done_b"
    )));
    complete_one(&mut recovered, "after-work");
    assert!(recovered.is_completed(parent));
}

#[test]
fn migration_rejects_call_activity_mapped_to_a_non_call_activity() {
    // Zeebe `MigrateProcessInstanceRejectionTest
    // .shouldRejectCommandWhenAnyElementsMappedToADifferentBpmnElementType`:
    // the generic same-type precondition covers call activities too — mapping
    // the active call activity onto a service task is rejected and nothing is
    // migrated.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, phase_process("child", "child-work"));
    deploy_for_migration(
        &mut engine,
        call_activity_parent("source", "a", "child", "done_a"),
    );
    let target_key =
        deploy_for_migration(&mut engine, migratable_task_process("target", "b", "work"));

    let (parent, _call_eik, _child) = create_calling_instance(&mut engine, "source", "a", "child");

    let err = engine
        .apply_command(Command::migrate_instance(
            parent,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(
        matches!(
            &err,
            EngineError::MappedElementTypeChanged {
                source_element_id,
                target_element_id,
                ..
            } if source_element_id == "a" && target_element_id == "b"
        ),
        "got {err:?}"
    );
    assert_eq!(
        engine.instance(parent).unwrap().process_id,
        "source",
        "the rejection is all-or-nothing: the instance is untouched"
    );
}

#[test]
fn migration_rejects_unmapped_active_call_activity() {
    // Zeebe `shouldRejectCommandWhenActiveElementIsNotMapped`: every active
    // element needs a mapping — an active call activity is no exception.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, phase_process("child", "child-work"));
    deploy_for_migration(
        &mut engine,
        call_activity_parent("source", "a", "child", "done_a"),
    );
    let target_key = deploy_for_migration(
        &mut engine,
        call_activity_parent("target", "b", "child", "done_b"),
    );

    let (parent, _call_eik, _child) = create_calling_instance(&mut engine, "source", "a", "child");

    let err = engine
        .apply_command(Command::migrate_instance(parent, target_key, vec![]))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::UnmappedActiveElement {
            instance_key: parent,
            element_id: "a".to_string(),
        }
    );
}

#[test]
fn migration_rejects_call_activity_flow_scope_change() {
    // Zeebe `requireUnchangedFlowScope`: the mapped target call activity must
    // sit in the mapped target of the source call activity's flow scope — a
    // target whose call activity moved inside a sub-process is rejected.
    let mut engine = Engine::new();
    deploy_for_migration(&mut engine, phase_process("child", "child-work"));
    deploy_for_migration(
        &mut engine,
        call_activity_parent("source", "a", "child", "done_a"),
    );
    // Target: the call activity `b` lives INSIDE sub-process `sub`.
    let target = ProcessBuilder::new("target")
        .start_event("start")
        .sub_process("sub", "s_start")
        .start_event("s_start")
        .contained_in("s_start", "sub")
        .call_activity("b", "child")
        .contained_in("b", "sub")
        .end_event("s_end")
        .contained_in("s_end", "sub")
        .end_event("end")
        .connect("start", "sub")
        .connect("s_start", "b")
        .connect("b", "s_end")
        .connect("sub", "end")
        .build()
        .unwrap();
    let target_key = deploy_for_migration(&mut engine, target);

    let (parent, _call_eik, _child) = create_calling_instance(&mut engine, "source", "a", "child");

    let err = engine
        .apply_command(Command::migrate_instance(
            parent,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap_err();
    assert!(
        matches!(
            &err,
            EngineError::MigrationFlowScopeChanged { element_id, .. } if element_id == "a"
        ),
        "got {err:?}"
    );
    assert_eq!(engine.instance(parent).unwrap().process_id, "source");
}

#[test]
fn migration_of_call_activity_with_spawn_incident_redrives_against_the_target() {
    // Zeebe `MigrateCallActivityTest.shouldMigrateCallActivityWithIncident`: a
    // call activity parked on a recoverable spawn incident (unknown callee)
    // migrates with its incident; the incident is re-pointed at the target
    // element, and resolving it re-drives the spawn against the TARGET
    // definition's `calledElement` (#1381's element-scoped retry resolves the
    // callee id through `element_kind`, which reads the migrated definition).
    let mut engine = Engine::new();
    // The source calls a callee that does not exist; the target calls one that
    // does — resolving the parked incident after migration must spawn it.
    deploy_for_migration(
        &mut engine,
        call_activity_parent("source", "a", "missing", "done_a"),
    );
    deploy_for_migration(&mut engine, phase_process("child", "child-work"));
    let target_key = deploy_for_migration(
        &mut engine,
        call_activity_parent("target", "b", "child", "done_b"),
    );

    let created = engine
        .apply_command(Command::create_instance("source"))
        .unwrap();
    let parent = parent_key_of(&created, "source");
    let call_eik = active_eik_of(&engine, parent, "a");
    // No child was created; the call activity is parked on a CalledElementError.
    assert!(engine
        .instance(parent)
        .unwrap()
        .active
        .values()
        .any(|e| e == "a"));
    let incident_key = engine
        .state()
        .incidents
        .values()
        .find(|i| {
            i.instance_key == parent
                && i.element_instance_key == call_eik
                && i.state == state::IncidentState::Active
        })
        .map(|i| i.key)
        .expect("a spawn incident is parked on the call activity");

    engine
        .apply_command(Command::migrate_instance(
            parent,
            target_key,
            vec![("a".to_string(), "b".to_string())],
        ))
        .unwrap();

    // The incident moved with the element (Zeebe's IncidentIntent.MIGRATED).
    let incident = &engine.state().incidents[&incident_key];
    assert_eq!(incident.element_id, "b");
    assert_eq!(incident.state, state::IncidentState::Active);
    // No child yet — the incident only re-points, it does not spawn.
    assert!(engine
        .state()
        .instances
        .values()
        .all(|i| i.parent_process_instance_key != Some(parent)));

    // Resolving it re-drives the spawn against the TARGET's callee (`child`):
    // the child is created, linked to the remapped call activity, and its
    // completion releases the parent onto the target's flow.
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let child = engine
        .state()
        .instances
        .values()
        .find(|i| i.parent_process_instance_key == Some(parent))
        .map(|i| i.key)
        .expect("resolving the incident spawned the target's callee");
    assert_eq!(engine.instance(child).unwrap().process_id, "child");
    assert_eq!(
        engine.instance(child).unwrap().parent_element_instance_key,
        Some(call_eik)
    );

    let events = complete_one(&mut engine, "child-work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "b" && to == "done_b"
    )));
    complete_one(&mut engine, "after-work");
    assert!(engine.is_completed(parent));
}
