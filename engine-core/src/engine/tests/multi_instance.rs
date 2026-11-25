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

/// Depth-first termination order guard (#1535, #1528 review): when an early MI
/// completion cancels a still-running sub-process child, the child's
/// `ElementTerminating` must precede every event of its subtree teardown (the
/// inner job's cancellation and the inner element's termination), and its
/// `ElementTerminated` must follow them. Previously the parent pair was appended
/// AFTER the descendant cleanup, so the journal reported descendants terminating
/// before their parent began terminating.
#[test]
fn early_multi_instance_completion_terminates_a_subprocess_child_parent_first() {
    let def = ProcessBuilder::new("mi-sub-order")
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
        .end_event("done")
        .connect("start", "each")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("each", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-sub-order",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let jobs = engine.activate_jobs("work", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 2);
    let events = engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();

    let pos = |pred: &dyn Fn(&Event) -> bool| events.iter().position(pred);
    let child_terminating =
        pos(&|e| matches!(e, Event::ElementTerminating { element_id, .. } if element_id == "each"))
            .expect("the cancelled sub-process child terminates");
    let child_terminated =
        pos(&|e| matches!(e, Event::ElementTerminated { element_id, .. } if element_id == "each"))
            .expect("the cancelled sub-process child terminates");
    let job_canceled = pos(&|e| matches!(e, Event::JobCanceled { .. }))
        .expect("the sibling inner job is cancelled");
    let inner_terminated =
        pos(&|e| matches!(e, Event::ElementTerminated { element_id, .. } if element_id == "inner"))
            .expect("the sibling inner task is terminated");
    assert!(
        child_terminating < job_canceled && child_terminating < inner_terminated,
        "parent TERMINATING precedes its subtree teardown: {events:#?}"
    );
    assert!(
        job_canceled < child_terminated && inner_terminated < child_terminated,
        "parent TERMINATED follows its subtree teardown: {events:#?}"
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
            BTreeMap::from([("factor".to_string(), Value::Int(5))]),
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
fn multi_instance_output_collection_is_body_local_while_looping() {
    // Zeebe `MultiInstanceBodyProcessor.onActivate` creates the named
    // `outputCollection` LOCAL to the body as `[null, ...]` (#1534), and each
    // completing child rewrites its slot there; the flow scope sees the
    // collection only once the body completes. A child must observe it.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_instance_service_process(true)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let body_seed = created.iter().any(|e| {
        matches!(e, Event::ScopedVariablesUpdated { scope_key, variables, .. }
            if *scope_key != key
                && variables.get("results")
                    == Some(&Value::List(vec![Value::Null, Value::Null])))
    });
    assert!(
        body_seed,
        "body-local results seeded with nulls: {created:?}"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        None,
        "not on the flow scope while the body runs"
    );

    let first = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].variables.get("results"),
        Some(&Value::List(vec![Value::Null, Value::Null]))
    );
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();
    let second = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].variables.get("results"),
        Some(&Value::List(vec![Value::Int(2), Value::Null])),
        "first child's slot written into the body-local collection"
    );
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(2), Value::Int(4)]))
    );
}

/// Review r4648913482 regression — the MI body's FIRST `start` execution
/// listener resolves its FEEL `type` against the body view WITH the
/// `outputCollection` seed already overlaid: Zeebe's
/// `MultiInstanceBodyProcessor.onActivate` runs `initializeOutputCollection`
/// before the start listeners fire, so a same-named ANCESTOR binding is
/// shadowed by the body's local null slots — never observed through the seed.
#[test]
fn mi_body_start_listener_type_reads_the_seeded_output_collection() {
    let def = ProcessBuilder::new("mi-listener-seed")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item".to_string()),
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        // The seed shadows the ancestor `results = ["audit"]`: `count(results)`
        // reads the body's freshly-seeded single null slot, so the listener
        // type resolves to `seeded-1` rather than the ancestor's `ancestor-1`.
        .with_listeners(
            "each",
            vec![el(
                ListenerEventType::Start,
                "=\"seeded-\" + string(count(results))",
            )],
            Vec::new(),
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-listener-seed",
            vars(&[
                ("items", Value::List(vec![Value::Int(1)])),
                (
                    "results",
                    Value::List(vec![Value::Str("audit".to_string())]),
                ),
            ]),
        ))
        .unwrap();
    assert!(
        created.iter().any(|e| matches!(
            e,
            Event::ExecutionListenerJobCreated { job_type, .. } if job_type == "seeded-1"
        )),
        "start listener type resolved against the seeded collection, not the ancestor: {created:?}"
    );
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::ExecutionListenerJobCreated { job_type, .. } if job_type == "audit"
        )),
        "the pre-seed ancestor binding must be shadowed: {created:?}"
    );

    // Draining the listener chain fans the loop out; it runs to completion.
    // The body-completion aggregate propagates to the ROOT scope (the only
    // ancestor), where it replaces the pre-existing `results = ["audit"]`
    // binding — Zeebe's propagation writes a same-named ancestor variable.
    let listener = engine.activate_jobs("seeded-1", "w", 10, 60_000, 0);
    assert_eq!(listener.len(), 1, "one body start-listener job");
    engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    complete_one(&mut engine, "handle");
    // The loop drained; the token parks at the sink with the aggregate
    // observable in hot state (ADR 0012 clears variables only on instance
    // completion).
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(1)])),
        "the aggregate replaced the same-named ancestor binding on completion"
    );
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

/// Review r4648913529 regression — the completion condition observes the
/// completing child's `outputCollection` slot ALREADY WRITTEN: Zeebe's
/// `MultiInstanceBodyProcessor.updateOutputCollection` runs before
/// `evaluateCompletionCondition`. A sequential `[1, 2]` loop with output
/// `=item` and condition `=results[1] = 1` must stop after the FIRST child —
/// evaluating against the pre-update view (slot still null) would spawn an
/// unwanted second child. The condition reads the prospective collection
/// through the CHILD's view, so no ancestor may bind the collection's name
/// (an ancestor binding shadows the body collection for the child-scope
/// evaluation, exactly as it does for `outputElement`).
#[test]
fn mi_completion_condition_reads_the_prospective_collection_update() {
    let def = ProcessBuilder::new("mi-cond-prospective")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item".to_string()),
                completion_condition: Some("=results[1] = 1".to_string()),
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-cond-prospective",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The first child completes with output 1; the condition reads
    // `results[1] = 1` against the prospective collection `[1, null]` and
    // fulfils — the body completes WITHOUT spawning the second child, and the
    // token advances to the sink.
    let events = complete_one(&mut engine, "handle");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::MultiInstanceCompleted { .. })),
        "the satisfied condition completes the body after the first child: {events:?}"
    );
    assert!(
        engine
            .activate_jobs("handle", "w", 10, 60_000, 0)
            .is_empty(),
        "no second child is spawned"
    );
    // The aggregate propagated to the flow scope with only the first child's
    // slot collected (the unspawned second slot stays null).
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(1), Value::Null])),
        "only the first child's slot was collected"
    );
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

/// The prospective-collection overlay must not clobber a binding of the
/// collection's name produced by the completing child's own output mappings
/// (review r4648913529): Zeebe applies the inner activity's `zeebe:output`
/// into the child's scope before the body evaluates the completion condition,
/// so a mapped `results` shadows the body collection for the condition,
/// exactly as it does for `outputElement` — child-scope shadowing is
/// preserved. The mapped value must also win over the body-local seed the
/// child view would otherwise inherit.
#[test]
fn mi_completion_condition_preserves_child_local_shadowing() {
    let mut def = multi_instance_service_process(true);
    let each = def.elements.get_mut("each").expect("each element");
    each.multi_instance
        .as_mut()
        .expect("each is multi-instance")
        .completion_condition = Some("=results = \"child\"".to_string());
    // A `zeebe:output` mapping projects `results = "child"` into the
    // completing child's view; the condition must read that mapped value, not
    // the prospective body collection (`[2, null]`).
    each.io = crate::model::IoMapping {
        inputs: Vec::new(),
        outputs: vec![crate::model::Mapping {
            source: "=\"child\"".to_string(),
            target: "results".to_string(),
        }],
    };

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The first child's output mapping satisfies the condition via the
    // in-memory post-output view — the prospective collection overlay ([2,
    // null]) must NOT replace it.
    let events = complete_one(&mut engine, "handle");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::MultiInstanceCompleted { .. })),
        "child-local shadowing fulfils the condition after the first child: {events:?}"
    );
    assert!(
        engine
            .activate_jobs("handle", "w", 10, 60_000, 0)
            .is_empty(),
        "no second child is spawned"
    );
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

/// Review r4649474283 regression — a DOTTED output target (`results.flag`)
/// binds the collection's name ROOT in the completing child's view, so it
/// must shadow the prospective-collection overlay exactly like a plain
/// `results` target. Overlaying the list would make `results.flag` null and
/// the condition would raise an incident instead of fulfilling.
#[test]
fn mi_completion_condition_preserves_dotted_output_target_shadowing() {
    let mut def = ProcessBuilder::new("mi-cond-dotted")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item".to_string()),
                completion_condition: Some("=results.flag = true".to_string()),
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();
    def.elements.get_mut("each").expect("each element").io = crate::model::IoMapping {
        inputs: Vec::new(),
        outputs: vec![crate::model::Mapping {
            source: "=true".to_string(),
            target: "results.flag".to_string(),
        }],
    };

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-cond-dotted",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let events = complete_one(&mut engine, "handle");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::MultiInstanceCompleted { .. })),
        "the dotted-target binding fulfils the condition: {events:?}"
    );
    assert!(engine
        .activate_jobs("handle", "w", 10, 60_000, 0)
        .is_empty());
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

/// Review 6001985210 follow-up — the seeded-view regression above proves the
/// listener's FEEL `type` observes the seed, but the type expression cannot
/// observe the seed's LIST LENGTH distinctly from the ancestor's single
/// `"audit"` string (`count` returns 1 for both). The listener's `retries`
/// expression resolves against the same seeded view
/// (`execution_listener_job_event` -> `resolve_job_props`), so it must shadow
/// the ancestor binding too: `count(results)` reads the body's TWO seeded
/// null slots (retries 2), never the ancestor's single-element list
/// (retries 1). This pins the overlay for the retries half of the listener
/// view with a length-discriminating read.
#[test]
fn mi_body_start_listener_retries_read_the_seeded_output_collection() {
    let def = ProcessBuilder::new("mi-listener-seed-retries")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item".to_string()),
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        .with_listeners(
            "each",
            vec![crate::model::ExecutionListener {
                event_type: ListenerEventType::Start,
                job_type: "audit".to_string(),
                // Seeded view: count([null, null]) = 2; the ancestor view
                // would give count(["audit"]) = 1.
                retries: Some("=count(results)".to_string()),
            }],
            Vec::new(),
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
            "mi-listener-seed-retries",
            vars(&[
                ("items", Value::List(vec![Value::Int(1), Value::Int(2)])),
                (
                    "results",
                    Value::List(vec![Value::Str("audit".to_string())]),
                ),
            ]),
        ))
        .unwrap();
    let listener = created.iter().find_map(|e| match e {
        Event::ExecutionListenerJobCreated {
            job_type, retries, ..
        } if job_type == "audit" => Some(*retries),
        _ => None,
    });
    assert_eq!(
        listener,
        Some(2),
        "listener retries resolved against the seeded two-slot collection, not the ancestor: {created:?}"
    );
}

/// Previously-missed review regression — with `outputElement="=null"` a
/// worker that overwrites the body-local `results` binding (no output
/// mappings, so its variables propagate to the body scope) leaves the
/// accumulator unchanged. The collection write must still restore the body
/// binding, so the NEXT child observes `[null, null]`, not the worker's
/// `[9, 9]`.
#[test]
fn mi_collection_write_restores_a_worker_overwritten_body_binding() {
    let def = ProcessBuilder::new("mi-null-elem")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=null".to_string()),
                completion_condition: None,
                cancel_remaining_instances: false,
                sequential: true,
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
            "mi-null-elem",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();

    let first = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(first.len(), 1);
    engine
        .apply_command(Command::complete_job_with(
            first[0].key,
            vars(&[("results", Value::List(vec![Value::Int(9), Value::Int(9)]))]),
        ))
        .unwrap();
    let second = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(second.len(), 1, "the second child is spawned");
    assert_eq!(
        second[0].variables.get("results"),
        Some(&Value::List(vec![Value::Null, Value::Null])),
        "the next child sees the accumulator, not the worker's overwrite"
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
    let by_task: std::collections::BTreeMap<String, u64> =
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
    // Zeebe merges each child's output mapping into the body WITH propagation
    // (#1982): `tag` is held by no ancestor, so it is created at the process
    // root and each later child updates it — the last completion wins.
    assert_eq!(
        engine.instance(key).unwrap().variables.get("tag"),
        Some(&Value::Int(30)),
        "the MI-child output mapping propagates `tag` to the process scope"
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

/// #1518 — Zeebe parity: an `outputElement` that is a bare variable reference
/// (`=result`) is seeded as a LOCAL (null) variable of every inner instance, so
/// a job result of that name lands in the child's own scope. Without the seed it
/// propagates to the process root, shared by every parallel child: here child
/// A's `t1` writes `result=a`, child B's `t1` overwrites it with `b`, and child A
/// then collects `b`.
#[test]
fn parallel_mi_output_element_variable_is_local_to_each_inner_instance() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:subProcess id="Each">
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
                  outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:startEvent id="ss" />
            <bpmn:serviceTask id="t1">
              <bpmn:extensionElements><zeebe:taskDefinition type="first" /></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="t2">
              <bpmn:extensionElements><zeebe:taskDefinition type="second" /></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="se" />
            <bpmn:sequenceFlow id="i1" sourceRef="ss" targetRef="t1" />
            <bpmn:sequenceFlow id="i2" sourceRef="t1" targetRef="t2" />
            <bpmn:sequenceFlow id="i3" sourceRef="t2" targetRef="se" />
          </bpmn:subProcess>
          <bpmn:serviceTask id="sink">
            <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
          <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
          <bpmn:sequenceFlow id="f3" sourceRef="sink" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[(
                "items",
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Both children's first job writes `result` = its own item, before either child completes.
    let firsts = engine.activate_jobs("first", "w", 10, 60_000, 0);
    assert_eq!(firsts.len(), 2);
    for job in &firsts {
        let item = job.variables.get("item").cloned().unwrap();
        engine
            .apply_command(Command::complete_job_with(
                job.key,
                vars(&[("result", item)]),
            ))
            .unwrap();
    }
    let seconds = engine.activate_jobs("second", "w", 10, 60_000, 0);
    assert_eq!(seconds.len(), 2);
    for job in &seconds {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }

    // Joined: the token is parked at the sink, so the instance scope is inspectable.
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.activate_jobs("sink", "w", 10, 60_000, 0).len(),
        1,
        "MI body joined"
    );
    let vars = &engine.instance(key).unwrap().variables;
    assert_eq!(
        vars.get("results"),
        Some(&Value::List(vec![
            Value::Str("a".into()),
            Value::Str("b".into())
        ])),
        "each child collects its OWN result, in index order"
    );
    assert_eq!(
        vars.get("result"),
        None,
        "the output element variable must not leak to the parent"
    );
}

/// #1518 guard: seeding the output element local must never clobber the input
/// element or `loopCounter` when the output element names it (Zeebe:
/// `shouldNotInitializeOutputElementVariableIfSameNameAsInputElement` /
/// `...IfSetToLoopCounter`).
#[test]
fn mi_output_element_naming_the_input_element_keeps_the_item() {
    assert_eq!(
        collect_output_element("=item"),
        vec![Value::Int(10), Value::Int(20)]
    );
    assert_eq!(
        collect_output_element("=loopCounter"),
        vec![Value::Int(1), Value::Int(2)]
    );
}

fn collect_output_element(output_element: &str) -> Vec<Value> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="Each">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
                  outputCollection="results" outputElement="OUT" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          <bpmn:serviceTask id="sink">
            <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
          <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let xml = xml.replace("OUT", output_element);
    let def = crate::bpmn::parse_bpmn(&xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("items", Value::List(vec![Value::Int(10), Value::Int(20)]))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    for job in engine.activate_jobs("work", "w", 10, 60_000, 0) {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    match engine.instance(key).unwrap().variables.get("results") {
        Some(Value::List(items)) => items.clone(),
        other => panic!("results not collected: {other:?}"),
    }
}

/// #1518 — the output element is child-local for a multi-instance applied
/// DIRECTLY to a service task too: the completion variables must
/// start propagating at the MI child's own scope (where `result` is seeded),
/// not at the enclosing body, or every child collects null and `result` leaks
/// to the parent.
#[test]
fn direct_mi_service_task_collects_each_job_result_without_leaking() {
    let (mut engine, key, _) = direct_mi_task(
        r#"<bpmn:serviceTask id="Each">
            <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
            MI
          </bpmn:serviceTask>"#,
    );
    for job in engine.activate_jobs("work", "w", 10, 60_000, 0) {
        let item = job.variables.get("item").cloned().unwrap();
        engine
            .apply_command(Command::complete_job_with(
                job.key,
                vars(&[("result", item)]),
            ))
            .unwrap();
    }
    assert_direct_mi_collected(&engine, key);
}

fn direct_mi_task(task: &str) -> (Engine, Key, Vec<Event>) {
    let mi = r#"<bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
                  outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>"#;
    let xml = format!(
        r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          {}
          <bpmn:serviceTask id="sink">
            <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
          <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
        </bpmn:process>
      </bpmn:definitions>"#,
        task.replace("MI", mi)
    );
    let def = crate::bpmn::parse_bpmn(&xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[(
                "items",
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]),
        ))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    (engine, key, events)
}

fn assert_direct_mi_collected(engine: &Engine, key: Key) {
    let vars = &engine.instance(key).unwrap().variables;
    assert_eq!(
        vars.get("results"),
        Some(&Value::List(vec![
            Value::Str("a".into()),
            Value::Str("b".into())
        ])),
        "each direct MI child collects its OWN completion result"
    );
    assert_eq!(
        vars.get("result"),
        None,
        "the output element variable must not leak to the parent"
    );
}

/// #1518 review — a script task's `resultVariable` inside an MI sub-process
/// must propagate to the nearest scope defining it. The child seeds
/// `outputElement`'s variable as a local null; a root-only result write would
/// be shadowed by that seed and every child would collect null.
#[test]
fn mi_subprocess_script_result_reaches_the_seeded_child_local() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:subProcess id="Each">
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
                  outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:startEvent id="ss" />
            <bpmn:scriptTask id="calc">
              <bpmn:extensionElements>
                <zeebe:script expression="=item * 2" resultVariable="result" />
              </bpmn:extensionElements>
            </bpmn:scriptTask>
            <bpmn:endEvent id="se" />
            <bpmn:sequenceFlow id="i1" sourceRef="ss" targetRef="calc" />
            <bpmn:sequenceFlow id="i2" sourceRef="calc" targetRef="se" />
          </bpmn:subProcess>
          <bpmn:serviceTask id="sink">
            <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
          <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
        </bpmn:process>
      </bpmn:definitions>"#;
    for items in [vec![Value::Int(10)], vec![Value::Int(10), Value::Int(20)]] {
        let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let expected: Vec<Value> = items
            .iter()
            .map(|v| match v {
                Value::Int(i) => Value::Int(i * 2),
                _ => unreachable!(),
            })
            .collect();
        let key = engine
            .apply_command(Command::create_instance_with(
                "p",
                vars(&[("items", Value::List(items))]),
            ))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        let inst = engine.instance(key).unwrap();
        assert_eq!(inst.variables.get("results"), Some(&Value::List(expected)));
        assert_eq!(inst.variables.get("result"), None, "result leaked to root");
    }
}

/// #1530 review — same shadowing defect through the business-rule-task arm:
/// a DMN result bound under `resultVariable` inside an MI sub-process must
/// reach the child's seeded local, not the root scope.
#[test]
fn mi_subprocess_dmn_result_reaches_the_seeded_child_local() {
    let dmn = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg" name="drg">
      <decision id="doubled" name="Doubled">
        <literalExpression id="le"><text>item * 2</text></literalExpression>
      </decision>
    </definitions>"##;
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:subProcess id="Each">
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
                  outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:startEvent id="ss" />
            <bpmn:businessRuleTask id="decide">
              <bpmn:extensionElements>
                <zeebe:calledDecision decisionId="doubled" resultVariable="result" />
              </bpmn:extensionElements>
            </bpmn:businessRuleTask>
            <bpmn:endEvent id="se" />
            <bpmn:sequenceFlow id="i1" sourceRef="ss" targetRef="decide" />
            <bpmn:sequenceFlow id="i2" sourceRef="decide" targetRef="se" />
          </bpmn:subProcess>
          <bpmn:serviceTask id="sink">
            <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
          <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let items = vec![Value::Int(10), Value::Int(20)];
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![
            crate::dmn::parse_dmn(dmn).unwrap(),
        ]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("items", Value::List(items))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inst = engine.instance(key).unwrap();
    assert_eq!(
        inst.variables.get("results"),
        Some(&Value::List(vec![Value::Int(20), Value::Int(40)]))
    );
    assert_eq!(inst.variables.get("result"), None, "result leaked to root");
}

/// #1530 review r4535034038 — an MI activity whose output mapping reads the
/// child-local `outputElement` variable must not publish `null` to the root at
/// body completion: that name is a per-child binding (seeded local), absent at
/// the body level, so the body pass must tolerate (skip) it exactly like
/// `inputElement`/`loopCounter`.
#[test]
fn mi_body_output_mapping_of_child_local_output_element_does_not_publish_null() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:subProcess id="Each">
            <bpmn:extensionElements>
              <zeebe:ioMapping>
                <zeebe:output source="=result" target="result" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
                  outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:startEvent id="ss" />
            <bpmn:scriptTask id="calc">
              <bpmn:extensionElements>
                <zeebe:script expression="=item * 2" resultVariable="result" />
              </bpmn:extensionElements>
            </bpmn:scriptTask>
            <bpmn:endEvent id="se" />
            <bpmn:sequenceFlow id="i1" sourceRef="ss" targetRef="calc" />
            <bpmn:sequenceFlow id="i2" sourceRef="calc" targetRef="se" />
          </bpmn:subProcess>
          <bpmn:serviceTask id="sink">
            <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
          <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("items", Value::List(vec![Value::Int(10), Value::Int(20)]))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inst = engine.instance(key).unwrap();
    assert_eq!(
        inst.variables.get("results"),
        Some(&Value::List(vec![Value::Int(20), Value::Int(40)]))
    );
    assert_ne!(
        inst.variables.get("result"),
        Some(&Value::Null),
        "body completion published the child-local outputElement as null"
    );
}

/// #1528 (review): cancelling an MI body with N parallel user-task children must
/// emit exactly one `UserTaskCanceled` per child — and do so from a SINGLE
/// element-instance → user-task index built once for the teardown, not by
/// rescanning the whole `state.user_tasks` map once per child (an O(N × tasks)
/// blow-up that stalls the engine on a large completion condition). This test
/// drives a 64-child cancel-remaining completion and asserts the emitted
/// cancellation set is exactly the children's tasks (no duplicates, no orphans),
/// which is the behaviour the shared index must preserve byte-for-byte.
#[test]
fn multi_instance_teardown_cancels_every_child_user_task_once() {
    const N: i64 = 64;
    // start -> each(MI userTask, completionCondition=true, cancelRemaining) -> done
    let def = ProcessBuilder::new("mi-ut-teardown")
        .start_event("start")
        .user_task("each")
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
        .end_event("done")
        .connect("start", "each")
        .connect("each", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let items = Value::List((0..N).map(Value::Int).collect());
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-ut-teardown",
            vars(&[("items", items)]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Every child parked on its own user task.
    let created_tasks: std::collections::BTreeSet<Key> = engine
        .state()
        .user_tasks
        .values()
        .filter(|t| t.state == state::UserTaskState::Created)
        .map(|t| t.key)
        .collect();
    assert_eq!(created_tasks.len(), N as usize, "one user task per child");

    // Completing ONE child's task satisfies the completion condition, so the body
    // tears the other N-1 children down.
    let first = *created_tasks.iter().next().unwrap();
    let events = engine
        .apply_command(Command::complete_user_task(first))
        .unwrap();

    let canceled_events: Vec<Key> = events
        .iter()
        .filter_map(|e| match e {
            Event::UserTaskCanceled { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .collect();
    let canceled: std::collections::BTreeSet<Key> = canceled_events.iter().copied().collect();
    assert_eq!(
        canceled_events.len(),
        canceled.len(),
        "no child's task may be cancelled twice in the journal"
    );
    // The torn-down children are every task except the one we completed.
    let mut expected = created_tasks.clone();
    expected.remove(&first);
    assert_eq!(
        canceled, expected,
        "each still-running child's task is cancelled exactly once (no duplicate, none missed)"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.state != state::UserTaskState::Created),
        "no child user task is left Created after the teardown"
    );
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the loop's runtime record is cleared"
    );

    // The cancelled children are torn down WITHOUT completing: each reports
    // `ElementTerminating`/`ElementTerminated` and NEVER
    // `ElementCompleting`/`ElementCompleted` — the completed lifecycle drives
    // completion listeners, metrics and compensation eligibility for work that
    // did not finish (#1535; #1528 review). The COMPLETED child is only the one
    // whose task we completed.
    let task_to_eik: std::collections::BTreeMap<Key, Key> = engine
        .state()
        .user_tasks
        .values()
        .map(|t| (t.key, t.element_instance_key))
        .collect();
    let mut torn_down: Vec<Key> = expected
        .iter()
        .map(|task| task_to_eik[task])
        .collect::<Vec<_>>();
    torn_down.sort_unstable();
    let mut terminated: Vec<Key> = events
        .iter()
        .filter_map(|e| match e {
            Event::ElementTerminated {
                element_instance_key,
                ..
            } => Some(*element_instance_key),
            _ => None,
        })
        .collect();
    terminated.sort_unstable();
    assert_eq!(
        terminated, torn_down,
        "every cancelled child is reported TERMINATED (one per torn-down child)"
    );
    let terminating: std::collections::BTreeSet<Key> = events
        .iter()
        .filter_map(|e| match e {
            Event::ElementTerminating {
                element_instance_key,
                ..
            } => Some(*element_instance_key),
            _ => None,
        })
        .collect();
    assert_eq!(
        terminating,
        torn_down.iter().copied().collect(),
        "every cancelled child is reported TERMINATING"
    );
    for child_eik in &torn_down {
        assert!(
            !events.iter().any(|e| matches!(
                e,
                Event::ElementCompleting { element_instance_key, .. }
                | Event::ElementCompleted { element_instance_key, .. }
                if element_instance_key == child_eik
            )),
            "a cancelled child must never be reported COMPLETING/COMPLETED ({child_eik})"
        );
    }
}

/// A parallel MI over `items` whose per-child input mapping fails for items
/// `>= fail_from` (`assert` on the item), with an optional body end listener.
fn parallel_mi_with_failing_input(
    id: &str,
    inner: Option<&str>,
    fail_from: i64,
    completion_condition: Option<&str>,
    end_listener: Option<&str>,
) -> crate::model::ProcessDefinition {
    let mut b = ProcessBuilder::new(id).start_event("start");
    b = match inner {
        Some(job_type) => b.service_task("each", job_type),
        None => b.task("each"),
    };
    b = b
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: completion_condition.map(str::to_string),
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: format!("=assert(item, item < {fail_from} or fixed = true)"),
                    target: "mapped".to_string(),
                }],
                outputs: Vec::new(),
            },
        );
    if let Some(listener) = end_listener {
        b = b.with_listeners(
            "each",
            Vec::new(),
            vec![el(ListenerEventType::End, listener)],
        );
    }
    b.end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap()
}

#[test]
fn parallel_mi_siblings_activate_past_a_failing_child_input_mapping() {
    // #1539 review r4650213947: with lazily-chained parallel children, a child
    // whose input mapping parks on an incident must still queue its siblings
    // (no stranded items), and resolving the incident must re-drive THAT child
    // (same key) without re-queueing the siblings (no duplicates).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(parallel_mi_with_failing_input(
            "mi-sib-io",
            Some("handle"),
            1,
            None,
            None,
        )))
        .unwrap();
    // `fail_from = 1` with items [1, 0, 0]: only index 0's mapping fails.
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-sib-io",
            vars(&[
                (
                    "items",
                    Value::List(vec![Value::Int(1), Value::Int(0), Value::Int(0)]),
                ),
                ("fixed", Value::Bool(false)),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "only the first child is parked: {active:?}"
    );
    let parked_child = active[0].element_instance_key;
    assert_eq!(
        engine.state().jobs.len(),
        2,
        "the later siblings activate before the incident is resolved"
    );
    let incident_key = active[0].key;
    engine
        .apply_command(Command::set_variables(
            inst,
            BTreeMap::from([("fixed".to_string(), Value::Bool(true))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "exactly one job per item, no duplicates");
    assert!(
        jobs.iter().any(|j| j.element_instance_key == parked_child),
        "the parked child's key is reused on re-drive"
    );
    let body = engine.state.instances[&inst]
        .multi_instances
        .values()
        .next()
        .unwrap();
    assert_eq!(body.active.len(), 3, "exactly one child per item");
    assert_eq!(body.spawned, 3);
    for job in jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn queued_parallel_sibling_is_dropped_once_the_body_starts_completing() {
    // #1539 review r4650213936: child 1 satisfies the completion condition and
    // the body enters COMPLETING (resting on its end listener) while child 3 is
    // still a queued activation. That queued sibling must be dropped — if it
    // activated (and here its input mapping fails) it would leave an orphaned
    // token + incident that blocks the process from completing.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(parallel_mi_with_failing_input(
            "mi-sib-done",
            None,
            3,
            Some("=loopCounter = 1"),
            Some("audit"),
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-sib-done",
            vars(&[
                (
                    "items",
                    Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                ),
                ("fixed", Value::Bool(false)),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "no incident from a sibling queued past body completion: {:?}",
        engine.active_incidents()
    );
    let listener = engine.activate_jobs("audit", "w", 10, 60_000, 0);
    assert_eq!(listener.len(), 1);
    engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

/// #1767: tearing down an MI body with N still-running children must scan the
/// global resource maps (jobs, timers, subscriptions, incidents, user tasks) a
/// CONSTANT number of times — via the shared per-teardown `TeardownIndex` —
/// never once per child, which made early completion O(N × |resources|).
/// Asserted for both leaf service-task children (jobs) and user-task children.
#[test]
fn early_multi_instance_teardown_scans_resource_maps_independently_of_child_count() {
    fn scans_for(n: i64, user_task: bool) -> (usize, usize) {
        let builder = ProcessBuilder::new("mi-scale").start_event("start");
        let builder = if user_task {
            builder.user_task("each")
        } else {
            builder.service_task("each", "work")
        };
        let def = builder
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
            .end_event("done")
            .connect("start", "each")
            .connect("each", "done")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance_with(
                "mi-scale",
                vars(&[("items", Value::List((0..n).map(Value::Int).collect()))]),
            ))
            .unwrap();
        let key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let finish = if user_task {
            let tasks: Vec<Key> = engine
                .state()
                .user_tasks
                .values()
                .filter(|t| t.state == state::UserTaskState::Created)
                .map(|t| t.key)
                .collect();
            assert_eq!(tasks.len(), n as usize);
            Command::CompleteUserTask {
                user_task_key: tasks[0],
                variables: Default::default(),
            }
        } else {
            let jobs = engine.activate_jobs("work", "W", n as usize, 60_000, 0);
            assert_eq!(jobs.len(), n as usize);
            Command::complete_job(jobs[0].key)
        };
        engine.resource_scan_count.set(0);
        engine.user_task_scan_count.set(0);
        engine.apply_command(finish).unwrap();
        assert!(
            engine.instance(key).is_none()
                || engine.instance(key).unwrap().multi_instances.is_empty(),
            "the body completed early"
        );
        assert!(
            engine
                .state()
                .jobs
                .values()
                .all(|j| j.state != state::JobState::Created
                    && j.state != state::JobState::Activated),
            "every sibling job is cancelled"
        );
        assert!(
            engine
                .state()
                .user_tasks
                .values()
                .all(|t| t.state != state::UserTaskState::Created),
            "every sibling user task is cancelled"
        );
        (
            engine.resource_scan_count.get(),
            engine.user_task_scan_count.get(),
        )
    }
    for user_task in [false, true] {
        let small = scans_for(4, user_task);
        let large = scans_for(64, user_task);
        assert_eq!(
            small, large,
            "teardown resource-map scans must not grow with child count \
             (user_task={user_task}): N=4 {small:?} vs N=64 {large:?}"
        );
    }
}

/// #1767 (review): the `None`-fallback user-task lookup in
/// `cancel_mi_child_events` — the per-child scan every indexed teardown is
/// meant to replace — must increment `user_task_scan_count`, exactly as the
/// `cancel_created_user_tasks_on` fallback already does. Otherwise the scale
/// test above passes while each child still performs one global user-task
/// scan: the very regression the probe exists to catch.
#[test]
fn cancel_mi_child_events_fallback_user_task_lookup_is_counted() {
    let def = ProcessBuilder::new("mi-fallback-probe")
        .start_event("start")
        .user_task("each")
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
        .end_event("done")
        .connect("start", "each")
        .connect("each", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-fallback-probe",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // Two MI children, each parked on its own `Created` user task.
    let mut children: Vec<Key> = engine
        .state()
        .user_tasks
        .values()
        .filter(|t| t.state == state::UserTaskState::Created)
        .map(|t| t.element_instance_key)
        .collect();
    children.sort_unstable();
    assert_eq!(children.len(), 2);
    engine.user_task_scan_count.set(0);
    // The fallback (no shared index) scans the whole user-task map once per
    // child — and the probe must count every one of those scans.
    for &child in &children {
        let events = engine.cancel_mi_child_events(instance_key, child, Vec::new(), None);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::UserTaskCanceled { .. })),
            "the child's parked user task is cancelled"
        );
    }
    assert_eq!(
        engine.user_task_scan_count.get(),
        children.len(),
        "every fallback user-task lookup must be counted"
    );
    // The indexed path scans zero times per child: one index build (a single
    // counted scan) serves every lookup from its buckets.
    engine.user_task_scan_count.set(0);
    let index = engine.teardown_index(instance_key);
    assert_eq!(engine.user_task_scan_count.get(), 1);
    for &child in &children {
        let events = engine.cancel_mi_child_events(instance_key, child, Vec::new(), Some(&index));
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::UserTaskCanceled { .. })),
            "the indexed lookup cancels the same parked user task"
        );
    }
    assert_eq!(
        engine.user_task_scan_count.get(),
        1,
        "indexed lookups must not rescan the user-task map"
    );
}

/// #1767 (adversarial review): call-activity MI children terminate a distinct
/// callee PROCESS INSTANCE each. That discard must read the callees' resources
/// from the shared `TeardownIndex` (one pass per map for all callees) and the
/// callee lookup must use the `children_by_parent` index — never a scan of the
/// global resource / instance maps per callee, which kept early completion of
/// N call-activity children O(N × |resources|).
#[test]
fn early_multi_instance_teardown_of_call_activity_children_scans_independently_of_child_count() {
    fn scans_for(n: i64) -> usize {
        let callee = ProcessBuilder::new("mi-callee")
            .start_event("s")
            .service_task("work", "work")
            .end_event("e")
            .connect("s", "work")
            .connect("work", "e")
            .build()
            .unwrap();
        let parent = ProcessBuilder::new("mi-call-scale")
            .start_event("start")
            .call_activity("each", "mi-callee")
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
            .end_event("done")
            .connect("start", "each")
            .connect("each", "done")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(callee))
            .unwrap();
        engine
            .apply_command(Command::DeployProcess(parent))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance_with(
                "mi-call-scale",
                vars(&[("items", Value::List((0..n).map(Value::Int).collect()))]),
            ))
            .unwrap();
        let key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let jobs = engine.activate_jobs("work", "W", n as usize, 60_000, 0);
        assert_eq!(jobs.len(), n as usize, "one callee job per MI child");
        engine.resource_scan_count.set(0);
        engine
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap();
        assert!(
            engine.instance(key).is_none()
                || engine.instance(key).unwrap().state == ProcessInstanceState::Completed,
            "the parent completed after the early MI body completion"
        );
        assert!(
            engine
                .state()
                .jobs
                .values()
                .all(|j| j.state != state::JobState::Created
                    && j.state != state::JobState::Activated),
            "every sibling callee's job is cancelled"
        );
        engine.resource_scan_count.get()
    }
    let small = scans_for(4);
    let large = scans_for(64);
    assert_eq!(
        small, large,
        "call-activity MI teardown scans must not grow with child count: \
         N=4 {small} vs N=64 {large}"
    );
}

/// #1982 regression (fuzz signature `674af8b5c5f7`) — an MI child's own
/// `zeebe:output` mapping (`result → o3`) is applied when the INNER instance
/// completes and propagates to the process scope (Zeebe creates `o3` at the
/// root), even though its source reads the per-child `outputElement` root.
#[test]
fn mi_child_output_mapping_propagates_to_process_scope() {
    let def = ProcessBuilder::new("mi-out-map")
        .start_event("start")
        .service_task("mi3", "work")
        .with_multi_instance(
            "mi3",
            crate::model::MultiInstance {
                input_collection: "=[1]".to_string(),
                input_element: None,
                output_collection: Some("out3".to_string()),
                output_element: Some("=result".to_string()),
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .with_io(
            "mi3",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=result".to_string(),
                    target: "o3".to_string(),
                }],
            },
        )
        .end_event("end")
        .connect("start", "mi3")
        .connect("mi3", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with("mi-out-map", vars(&[])))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let jobs = engine.activate_jobs("work", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1);

    let evs = engine
        .apply_command(Command::complete_job_with(
            jobs[0].key,
            vars(&[("result", Value::Int(85))]),
        ))
        .unwrap();
    // `o3` is CREATED at the process (root) scope as the inner instance
    // completes — before the child's slot is collected into `out3`.
    let o3_at = evs.iter().position(|e| {
        matches!(e, Event::VariablesUpdated { instance_key, variables }
            if *instance_key == key && variables.get("o3") == Some(&Value::Int(85)))
    });
    let child_done_at = evs
        .iter()
        .position(|e| matches!(e, Event::MultiInstanceChildCompleted { .. }));
    assert!(
        matches!((o3_at, child_done_at), (Some(a), Some(b)) if a < b),
        "o3 propagated to the root on child completion: {evs:#?}"
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, Event::VariablesUpdated { variables, .. }
            if variables.get("out3") == Some(&Value::List(vec![Value::Int(85)])))),
        "out3 still propagated on body completion: {evs:#?}"
    );
    assert!(engine.is_completed(key));
}
