//! `multi_instance` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn subprocess_terminate_end_clears_a_multi_instance_body_in_its_scope() {
    // A terminate end must drop the runtime record of a multi-instance body torn
    // down with its scope. `ElementCompleted` removes the body token but leaves
    // `multi_instances` populated (cleared only by `MultiInstanceCompleted`), so
    // without explicit teardown the terminated scope keeps a stale active-child
    // set — and the dead-scope guard would read it as a live loop.
    //
    //  sub: sub_start -> split =< each(MI svc "handle") -> each_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .contained_in("each", "sub")
        .end_event("each_end")
        .contained_in("each_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "each")
        .connect("each", "each_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // The multi-instance body registered a runtime record on activation.
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    // The sibling terminate fires when `trigger` completes.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::MultiInstanceCompleted { .. })),
        "scope teardown must emit MultiInstanceCompleted for the torn-down body"
    );
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record must be gone after terminate"
    );
}

/// Issue #1170 regression — an EARLY multi-instance completion (satisfied
/// `completionCondition`) that cancels a still-running embedded SUB-PROCESS child
/// must tear down that child's whole INNER scope (its inner job, timers, nested
/// tokens), not just the child's own sub-process token. The leaf-only
/// `cancel_mi_child_events` reaches only resources owned directly by the child
/// element instance, so a sub-process child's inner job would otherwise be
/// orphaned — leaving the process `Active` forever even though the loop
/// "completed" early. The early-cancel path must sweep descendants (mirroring the
/// interrupting-boundary teardown) before completing the child.
#[test]
fn early_multi_instance_completion_tears_down_a_running_subprocess_child_scope() {
    // start -> each(MI sub[ sub_start -> inner("work") -> sub_end ]) -> sink -> done
    // parallel MI, completionCondition = true (fires as soon as ONE child drains),
    // leaving the sibling sub-process child's inner job running to be cancelled.
    let def = ProcessBuilder::new("mi-sub-early")
        .start_event("start")
        .sub_process("each", "sub_start")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: Some("=true".to_string()),
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "each")
        .service_task("inner", "work")
        .contained_in("inner", "each")
        .end_event("sub_end")
        .contained_in("sub_end", "each")
        .service_task("sink", "sink-work")
        .end_event("done")
        .connect("start", "each")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("each", "sink")
        .connect("sink", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub-early",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("work", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 2, "each MI sub-process child runs an inner job");
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    // Completing ONE inner job drains that child's sub-process and satisfies the
    // completion condition, ending the body early. The sibling child's inner job
    // must be cancelled with its scope — not orphaned.
    let sibling = jobs[1].key;
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();

    assert_eq!(
        engine.state().jobs[&sibling].state,
        state::JobState::Canceled,
        "the still-running sub-process child's inner job is cancelled on early completion (#1170)"
    );
    assert!(
        !engine.pending_jobs().iter().any(|j| j.job_type == "work"),
        "no inner job leaks past the early completion"
    );
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the loop's runtime record is cleared"
    );
    // The body completed early and advanced to `sink` — the instance is legitimately
    // parked on the sink job, NOT wedged Active by an orphaned inner job.
    assert!(
        engine
            .pending_jobs()
            .iter()
            .any(|j| j.job_type == "sink-work"),
        "the early completion advances the body to its outgoing flow (`sink`)"
    );
}

/// Defect-class guard (dead-scope guard, multi-instance retry branches): a
/// resolved MI input-mapping incident re-drives the *already-activated* child /
/// body via `RetryMiChildActivation` / `RetryMiBodyActivation`, reusing existing
/// keys. If a scoped terminate tore the loop's body (and children) down first,
/// those queued retries must be dropped — dispatching them would re-drive work in
/// a dead scope. A bare instance-terminal check is insufficient because the
/// instance itself stays active.
#[test]
fn dead_scope_guard_rejects_mi_retries_after_the_body_is_torn_down() {
    //  sub: sub_start -> split =< each(MI svc "handle"), trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .contained_in("each", "sub")
        .end_event("each_end")
        .contained_in("each_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "each")
        .connect("each", "each_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The MI body is active with its children parked on jobs. Capture the body
    // and one child element-instance key.
    let body_key = *engine
        .instance(key)
        .unwrap()
        .multi_instances
        .keys()
        .next()
        .expect("MI body registered");
    let child_key = *engine
        .instance(key)
        .unwrap()
        .scopes
        .iter()
        .find(|(_, parent)| **parent == body_key)
        .map(|(child, _)| child)
        .expect("MI child active under the body");

    let child_retry = Step::RetryMiChildActivation {
        instance_key: key,
        element_id: "each".to_string(),
        body_key,
        child_key,
        index: 0,
        reapply_inputs: true,
    };
    let body_retry = Step::RetryMiBodyActivation {
        instance_key: key,
        element_id: "each".to_string(),
        body_key,
        scope: body_key,
    };

    // While the loop is live the guard admits both retries.
    assert!(!engine.step_targets_dead_scope(&child_retry));
    assert!(!engine.step_targets_dead_scope(&body_retry));

    // The sibling terminate tears the whole sub-process (and its MI body) down.
    complete_one(&mut engine, "trigger-job");
    assert!(engine.instance(key).unwrap().multi_instances.is_empty());
    assert_eq!(
        engine.instance(key).unwrap().state,
        crate::state::ProcessInstanceState::Active,
        "the parent instance continues past the scoped terminate"
    );

    // Now the queued retries target a dead body: the guard must drop both, even
    // though the instance is still active.
    assert!(
        engine.step_targets_dead_scope(&child_retry),
        "a child retry whose body was torn down must be dropped"
    );
    assert!(
        engine.step_targets_dead_scope(&body_retry),
        "a body retry whose body token was torn down must be dropped"
    );
}

#[test]
fn top_level_terminate_end_clears_a_multi_instance_body() {
    // A top-level terminate ends the whole instance as COMPLETED (#1085); its
    // `ProcessInstanceCompleted` reducer must also drop the multi-instance runtime
    // record, or a large in-flight loop keeps its item/output payload on the
    // terminal instance until eviction.
    //
    //  start -> split =< each(MI svc "handle") -> each_end,
    //                    trigger(svc) -> stop(terminate) >
    let proc = ProcessBuilder::new("mi-term")
        .start_event("start")
        .parallel_gateway("split")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .end_event("each_end")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "each")
        .connect("each", "each_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-term",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "terminal teardown must clear the multi-instance runtime record"
    );
}

#[test]
fn multi_instance_child_input_mapping_failure_raises_io_mapping_and_reactivates() {
    // #946 item 1 (MI child input): a per-child input `zeebe:ioMapping` that fails
    // must raise `IO_MAPPING_ERROR` and park the child (NOT skip to `Complete`).
    // Resolution re-drives the child's *activation* (`MiChildActivation`),
    // re-applying the now-fixed input and minting the child's job.
    let def = ProcessBuilder::new("mi-child-io")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                // References the per-child binding `item` (tolerated at the body
                // level) and the root `factor` (a string ⇒ per-child eval fails).
                inputs: vec![crate::model::Mapping {
                    source: "=assert(factor, factor instance of number) + item".to_string(),
                    target: "weighted".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-child-io",
            vars(&[
                ("items", Value::List(vec![Value::Int(10)])),
                ("factor", Value::Str("oops".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        matches!(
            active[0].redrive,
            Some(state::IoMappingRedrive::MiChildActivation { .. })
        ),
        "a MI child input failure re-drives the child's activation, got {:?}",
        active[0].redrive
    );
    assert_eq!(active[0].element_id, "each");
    assert_eq!(
        engine.state().jobs.len(),
        0,
        "no job while the child is parked"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `factor` and resolve: the child re-activates, the input maps
    // (`weighted = 5 + 10 = 15`), and its job is created.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("factor".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1, "the child's job is minted on resolution");
    assert_eq!(jobs[0].variables.get("weighted"), Some(&Value::Int(15)));
}

#[test]
fn parallel_multi_instance_fans_out_a_child_per_item() {
    // A parallel multi-instance activity spawns one child (and, for a service
    // task, one job) per item in the input collection, all at once.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
            )]),
        ))
        .unwrap();

    // Three parallel children -> three jobs, each carrying its own loopCounter
    // and bound item in its local variable overlay.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one job per collection item");
    let mut seen: Vec<(i64, i64)> = jobs
        .iter()
        .map(|j| {
            let counter = j
                .variables
                .get("loopCounter")
                .and_then(Value::as_f64)
                .unwrap() as i64;
            let item = j.variables.get("item").and_then(Value::as_f64).unwrap() as i64;
            (counter, item)
        })
        .collect();
    seen.sort();
    assert_eq!(seen, vec![(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn parallel_multi_instance_collects_output_collection_in_order() {
    // Each child's output_element is stored at its index; the body completes
    // only after every child finishes and writes the aggregate collection —
    // ordered by index regardless of completion order.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    // Complete out of collection order to prove index-based aggregation.
    let mut ordered = jobs.clone();
    ordered.sort_by_key(|j| {
        std::cmp::Reverse(
            j.variables
                .get("loopCounter")
                .and_then(Value::as_f64)
                .unwrap() as i64,
        )
    });
    for job in &ordered {
        assert!(!engine.is_completed(key));
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    // Body drained -> the token advanced to the sink task (instance not yet
    // completed, so the aggregate is still observable); results doubled each
    // item in order.
    assert!(!engine.is_completed(key));
    assert!(
        engine
            .state()
            .jobs
            .values()
            .any(|j| j.job_type == "sink-work" && matches!(j.state, state::JobState::Created)),
        "token parked at the sink task"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6)
        ]))
    );
}

#[test]
fn multi_instance_input_mapping_cannot_clobber_loop_counter() {
    // A malicious/careless `zeebe:input` mapping targeting the reserved
    // `loopCounter` binding must not corrupt the engine-owned loop counter:
    // `complete_mi_child` derives each child's output-collection index from it,
    // so a clobbered counter would misindex (or silently drop, since the slot is
    // out of range) every child's output. The engine-owned counter must win, so
    // the aggregate still lands in index order.
    let mut def = multi_instance_service_process(false);
    let each = def.elements.get_mut("each").expect("each element");
    each.io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=loopCounter + 100".to_string(),
            target: "loopCounter".to_string(),
        }],
        outputs: Vec::new(),
    };

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one job per collection item");
    // Each child still sees the engine-owned loop counter (1..=3), not the
    // mapped-over value — the reserved binding is protected from the mapping.
    let mut counters: Vec<i64> = jobs
        .iter()
        .map(|j| {
            j.variables
                .get("loopCounter")
                .and_then(Value::as_f64)
                .unwrap() as i64
        })
        .collect();
    counters.sort();
    assert_eq!(counters, vec![1, 2, 3]);

    for job in &jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    // Every child's output landed at its correct index despite the input mapping
    // aiming at `loopCounter`.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6)
        ]))
    );
}

#[test]
fn multi_instance_child_index_survives_worker_clobbering_loop_counter() {
    // The child's output-collection index is engine-owned runtime state, not
    // derived from the mutable `loopCounter` variable. So even a write path the
    // `zeebe:input` guard cannot see — a worker issuing
    // `SetVariables { local: true }` against its child scope to overwrite
    // `loopCounter` — must not corrupt `outputCollection` indexing: an
    // out-of-range counter would otherwise silently drop the output, and a
    // collided one would overwrite another child's slot. The aggregate must still
    // land in index order.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one job per collection item");

    // Every worker slams the SAME out-of-range `loopCounter` into its own child
    // scope before completing. If completion trusted this variable, all three
    // outputs would target index 99 (out of range) and be dropped.
    for job in &jobs {
        engine
            .apply_command(Command::set_variables_scoped(
                job.element_instance_key,
                vars(&[("loopCounter", Value::Int(99))]),
                true,
            ))
            .unwrap();
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }

    // Each child's output still landed at its true engine-owned index.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6)
        ]))
    );
}

#[test]
fn sequential_multi_instance_runs_children_one_at_a_time() {
    // A sequential multi-instance activity spawns the next child only after the
    // previous one completes.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(true)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(5), Value::Int(6)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Only the first child is active.
    let first = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0]
            .variables
            .get("loopCounter")
            .and_then(Value::as_f64)
            .unwrap() as i64,
        1
    );
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    // Completing it spawns the second, still not the whole loop at once.
    let second = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0]
            .variables
            .get("loopCounter")
            .and_then(Value::as_f64)
            .unwrap() as i64,
        2
    );
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();

    // Loop drained -> parked at the sink; the ordered aggregate is observable.
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(10), Value::Int(12)]))
    );
}

#[test]
fn multi_instance_completion_condition_completes_the_body_early() {
    // A satisfied completion condition cancels the still-running children and
    // completes the body without waiting for them.
    let def = ProcessBuilder::new("mi-cc")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                // Fire as soon as the first child finishes.
                completion_condition: Some("=true".to_string()),
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-cc",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3);
    // Completing ONE child satisfies the condition and ends the body; the other
    // two jobs are canceled with the body.
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert!(engine.is_completed(key));
    assert!(
        engine.state().jobs.values().all(|j| matches!(
            j.state,
            state::JobState::Canceled | state::JobState::Completed
        )),
        "remaining children canceled on early completion"
    );
}

#[test]
fn empty_input_collection_completes_the_multi_instance_body_immediately() {
    // An empty collection (or a non-list / erroring expression) yields zero
    // children; the body completes at once and the token flows on.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // No `handle` jobs created; the body completed at once and the token flowed
    // through to the sink task (parked there, aggregate still observable).
    assert_eq!(engine.activate_jobs("handle", "w", 10, 60_000, 0).len(), 0);
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.state().jobs.len(),
        1,
        "token parked at the sink task"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![]))
    );
}

#[test]
fn multi_instance_children_hold_their_bindings_in_real_child_scopes() {
    // Part C: each parallel child is now a real variable scope (parented to the
    // MI body), holding its `loopCounter`/`inputElement` locally — never leaked
    // to the root instance variables.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Jobs still see their per-child bindings...
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3);
    for job in &jobs {
        assert!(job.variables.get("item").is_some());
        assert!(job.variables.get("loopCounter").is_some());
    }

    // ...but the root scope never received them: the bindings live in dedicated
    // child scopes parented to the multi-instance body.
    let instance = engine.instance(key).unwrap();
    assert!(instance.variables.get("item").is_none());
    assert!(instance.variables.get("loopCounter").is_none());
    let body_key = *instance
        .multi_instances
        .keys()
        .next()
        .expect("an active multi-instance body");
    let children: Vec<Key> = instance
        .scope_parents
        .iter()
        .filter(|&(_, &parent)| parent == body_key)
        .map(|(&child, _)| child)
        .collect();
    assert_eq!(children.len(), 3, "one real scope per child");
    for child in children {
        let locals = instance
            .scope_variables
            .get(&child)
            .expect("child scope carries local bindings");
        assert!(locals.contains_key("item"));
        assert!(locals.contains_key("loopCounter"));
    }
}

#[test]
fn parallel_multi_instance_on_a_subprocess_fans_out_one_scope_per_item() {
    // Agent-fleet / #547 wave nesting: a multi-instance attached to an EMBEDDED
    // SUB-PROCESS fans out one full sub-process scope per collection item, runs
    // each inner task with its bound item, then joins. This is the primitive
    // under "sequential MI over waves wrapping parallel MI over a wave's tasks".
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_subprocess(false)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub",
            vars(&[(
                "tasks",
                Value::List(vec![
                    Value::Str("s549".into()),
                    Value::Str("s550".into()),
                    Value::Str("s551".into()),
                ]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // One inner job per item, all active at once (parallel), each carrying its
    // MI-bound `task` visible inside its own sub-process scope.
    let jobs = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one sub-process child per task");
    let mut seen: Vec<String> = jobs.iter().map(bound_task).collect();
    seen.sort();
    assert_eq!(seen, vec!["s549", "s550", "s551"]);

    // The join waits for every child sub-process; the token has not left the MI
    // body until the last one completes. Complete the children in a deliberately
    // shuffled order (last item first, first item last) so that if aggregation
    // accidentally depended on completion order the assert below would catch it.
    let by_task: std::collections::HashMap<String, u64> =
        jobs.iter().map(|j| (bound_task(j), j.key)).collect();
    for task in ["s551", "s549", "s550"] {
        assert!(!engine.is_completed(key));
        assert!(
            engine
                .state()
                .jobs
                .values()
                .all(|j| j.job_type != "sink-work"),
            "sink not reached until the MI sub-process joins"
        );
        engine
            .apply_command(Command::complete_job(by_task[task]))
            .unwrap();
    }

    // Joined -> token parked at the sink; aggregate collected in index order
    // regardless of completion order.
    assert!(!engine.is_completed(key));
    assert!(
        engine
            .state()
            .jobs
            .values()
            .any(|j| j.job_type == "sink-work" && matches!(j.state, state::JobState::Created)),
        "token joined the MI sub-process and parked at the sink"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("done_tasks"),
        Some(&Value::List(vec![
            Value::Str("s549".into()),
            Value::Str("s550".into()),
            Value::Str("s551".into()),
        ]))
    );
}

#[test]
fn sequential_multi_instance_on_a_subprocess_runs_one_scope_at_a_time() {
    // The sequential form (a "wave" that runs its sub-process bodies one after
    // another) spawns the next child sub-process only after the previous one
    // fully completes.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_subprocess(true)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub",
            vars(&[(
                "tasks",
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let first = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(first.len(), 1, "only the first wave sub-process is active");
    assert_eq!(bound_task(&first[0]), "a");
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    let second = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(
        second.len(),
        1,
        "next sub-process spawned only after the first"
    );
    assert_eq!(bound_task(&second[0]), "b");
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();

    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("done_tasks"),
        Some(&Value::List(vec![
            Value::Str("a".into()),
            Value::Str("b".into())
        ]))
    );
}

#[test]
fn multi_instance_subprocess_applies_its_own_output_mapping_before_collecting() {
    // Zeebe parity: an MI-child sub-process evaluates its own `zeebe:output`
    // mappings on each instance's completion, into that instance's OWN scope, so
    // the loop's `outputElement` can read the mapped value. Here `outputElement`
    // is `=tag`, and `tag` exists ONLY because the sub-process output mapping
    // `tag = n * 10` ran — if it were skipped, `outputElement` would collect null.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            multi_instance_subprocess_with_output_mapping(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub-out",
            vars(&[(
                "nums",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("do-work", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "one sub-process child per item");
    for job in &jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }

    assert!(!engine.is_completed(key), "token parked at the sink");
    // The aggregated collection reflects the sub-process output mapping (n*10),
    // in index order.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("tags"),
        Some(&Value::List(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
        ])),
        "outputElement read the sub-process-mapped local `tag`"
    );
    // The mapped local stays scoped to each child — it does NOT leak to the
    // parent instance scope (only the outputCollection propagates).
    assert_eq!(
        engine.instance(key).unwrap().variables.get("tag"),
        None,
        "the MI-child output mapping must not propagate `tag` to the parent"
    );
}

#[test]
fn multi_instance_task_evaluates_input_mappings_per_child_with_bindings() {
    // Zeebe parity: an MI activity's `zeebe:input` mappings are evaluated on each
    // child's activation, with `inputElement`/`loopCounter` already bound, into
    // the child's own local scope. Here `handle_arg = item * 100 + loopCounter`,
    // so each job carries a value that only exists because the mapping ran with
    // that child's bindings.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            multi_instance_service_with_input_mapping(),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-in",
            vars(&[("items", Value::List(vec![Value::Int(5), Value::Int(6)]))]),
        ))
        .unwrap();

    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 2, "one job per item");
    let mut args: Vec<i64> = jobs
        .iter()
        .map(|j| match j.variables.get("handle_arg") {
            Some(Value::Int(n)) => *n,
            other => panic!("expected a per-child input-mapped `handle_arg`, got {other:?}"),
        })
        .collect();
    args.sort();
    // item=5,loopCounter=1 -> 501 ; item=6,loopCounter=2 -> 602
    assert_eq!(args, vec![501, 602]);
}

#[test]
fn set_variables_local_on_a_multi_instance_child_stays_in_its_scope() {
    // A `local` SetVariables targeting a running MI child scope writes only that
    // child's scope, invisible to the root and to sibling children.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    engine.activate_jobs("handle", "w", 10, 60_000, 0);

    let instance = engine.instance(key).unwrap();
    let body_key = *instance.multi_instances.keys().next().unwrap();
    let mut children: Vec<Key> = instance
        .scope_parents
        .iter()
        .filter(|&(_, &parent)| parent == body_key)
        .map(|(&child, _)| child)
        .collect();
    children.sort();
    let target = children[0];
    let sibling = children[1];

    engine
        .apply_command(Command::set_variables_scoped(
            target,
            vars(&[("note", Value::Str("child-only".into()))]),
            true,
        ))
        .unwrap();

    let instance = engine.instance(key).unwrap();
    assert_eq!(
        instance.scope_variables.get(&target).unwrap().get("note"),
        Some(&Value::Str("child-only".into())),
        "written into the target child scope"
    );
    assert!(
        instance.variables.get("note").is_none(),
        "not propagated to the root"
    );
    assert!(
        instance
            .scope_variables
            .get(&sibling)
            .map(|m| !m.contains_key("note"))
            .unwrap_or(true),
        "not visible to the sibling child"
    );
}

#[test]
fn multi_instance_child_job_type_resolves_against_the_child_scope() {
    // A multi-instance child resolves its `=FEEL` job type against its own scope
    // (the bound `inputElement`), so each child lands on a per-item worker type.
    let model = ProcessBuilder::new("mi-jt")
        .start_event("start")
        .service_task("each", "=\"handle-\" + item")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(model)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-jt",
            vars(&[(
                "items",
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]),
        ))
        .unwrap();

    // Each child job carries its own item-derived type.
    assert_eq!(
        engine.activate_jobs("handle-a", "w", 10, 60_000, 0).len(),
        1
    );
    assert_eq!(
        engine.activate_jobs("handle-b", "w", 10, 60_000, 0).len(),
        1
    );
    // No child fell back to the literal expression text.
    assert!(engine
        .activate_jobs("=\"handle-\" + item", "w", 10, 60_000, 0)
        .is_empty());
}

#[test]
fn multi_instance_body_static_input_mapping_named_like_child_binding_is_applied_verbatim() {
    // A static (non-`=`) io-mapping source is a literal string passed through
    // unevaluated (#1160) — even when its text happens to name a per-child
    // binding (`loopCounter` / the `inputElement`). The body-level dependency
    // scan must not parse it as a FEEL variable reference and skip the mapping:
    // the literal is assigned to the parent scope verbatim, and the per-child
    // pass assigns the same literal (never the child's binding value).
    let def = ProcessBuilder::new("mi-static-src")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "loopCounter".to_string(),
                    target: "mode".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-static-src",
            vars(&[("items", Value::List(vec![Value::Int(1)]))]),
        ))
        .unwrap();

    // The body-level pass assigns the literal verbatim into the body scope,
    // which every child inherits — so each child job sees the literal string,
    // not the child's own `loopCounter` binding (1 for the first child). The
    // per-child pass then re-assigns the same literal.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].variables.get("mode"),
        Some(&Value::Str("loopCounter".into())),
        "a static source is a literal string, never a variable reference"
    );
    assert_eq!(
        jobs[0].variables.get("loopCounter"),
        Some(&Value::Int(1)),
        "the child's own binding is unaffected by the literal mapping"
    );
}

#[test]
fn multi_instance_body_input_mapping_with_local_binding_shadowing_input_element_runs() {
    // A mapping whose only use of the `inputElement` name is LOCALLY BOUND
    // (`for item in …`) is self-contained: it does not read the per-child
    // binding, so the body must evaluate it rather than skip it. Skipping it
    // here would drop the input collection's provider and silently fan out
    // zero children.
    let def = ProcessBuilder::new("mi-local-bind")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=payloads".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=for item in [1, 2] return item".to_string(),
                    target: "payloads".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with("mi-local-bind", vars(&[])))
        .unwrap();

    // The self-contained mapping supplies the input collection: two children.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        2,
        "a locally bound use of the inputElement name must not skip the mapping"
    );
    assert_eq!(jobs[0].variables.get("item"), Some(&Value::Int(1)));
    assert_eq!(jobs[1].variables.get("item"), Some(&Value::Int(2)));
}

#[test]
fn multi_instance_body_input_mapping_with_filter_local_item_binding_runs() {
    // A list filter binds `item` to the element under test over its predicate
    // (eval_index), so `=[1, 2][item > 0]` is self-contained even when the
    // `inputElement` is also named `item`: the predicate does not read the
    // per-child binding. The body must evaluate the mapping rather than skip
    // it — skipping it here would drop the input collection's provider and
    // silently fan out zero children (Copilot review 5778200940).
    let def = ProcessBuilder::new("mi-filter-bind")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=payloads".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=[1, 2][item > 0]".to_string(),
                    target: "payloads".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with("mi-filter-bind", vars(&[])))
        .unwrap();

    // The filter-local mapping supplies the input collection: two children.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        2,
        "a filter-local use of the inputElement name must not skip the mapping"
    );
    assert_eq!(jobs[0].variables.get("item"), Some(&Value::Int(1)));
    assert_eq!(jobs[1].variables.get("item"), Some(&Value::Int(2)));
}

#[test]
fn multi_instance_body_input_mapping_with_filter_over_context_elements_runs() {
    // A filter over context elements binds each element's entries over the
    // predicate (eval_index), so `=[{price: 1}, {price: 2}][price > 1]` is
    // self-contained even when the `inputElement` is also named `price`: the
    // predicate reads the element's own `price`, not the per-child binding.
    // The body must evaluate the mapping rather than skip it — skipping it
    // here would drop the input collection's provider and silently fan out
    // zero children (Copilot review 5778355773).
    let def = ProcessBuilder::new("mi-filter-ctx")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=payloads".to_string(),
                input_element: Some("price".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=[{price: 1}, {price: 2}][price > 1]".to_string(),
                    target: "payloads".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with("mi-filter-ctx", vars(&[])))
        .unwrap();

    // The context-filter mapping supplies the input collection: one child
    // (only the {price: 2} element survives the filter), and the surviving
    // element — a context — is the child's `price` binding.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "a filter over context elements must not skip the mapping"
    );
    let expected: crate::Value =
        Value::Map([("price".to_string(), Value::Int(2))].into_iter().collect());
    assert_eq!(jobs[0].variables.get("price"), Some(&expected));
}

#[test]
fn multi_instance_body_input_mapping_with_bare_var_filter_runs() {
    // A bare-variable subscript that is NOT in scope is not numeric, so the
    // subscript is a filter at runtime and binds `item` over it (eval_index):
    // `=[true, false][item]` is self-contained even when the `inputElement`
    // is also named `item`. The body must evaluate the mapping rather than
    // skip it (Copilot review 5778355773).
    let def = ProcessBuilder::new("mi-bare-var-filter")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=payloads".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=[true, false][item]".to_string(),
                    target: "payloads".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-bare-var-filter",
            vars(&[]),
        ))
        .unwrap();

    // The bare-var filter keeps only the `true` element: one child.
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1, "a bare-var filter must not skip the mapping");
    assert_eq!(jobs[0].variables.get("item"), Some(&Value::Bool(true)));
}
