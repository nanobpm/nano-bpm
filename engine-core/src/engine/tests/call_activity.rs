//! `call_activity` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

// zeebe-cells: event:end:terminate
#[test]
fn subprocess_terminate_end_terminates_a_call_activity_child_in_its_scope() {
    // A terminate end inside a sub-process must also reap a call-activity CHILD
    // process instance spawned from that scope — the child is a separate instance
    // `scope_descendants` cannot see, so without explicit termination it (and its
    // jobs) would keep running with no parent token.
    //
    //  orch: start -> sub -> after(svc) -> done
    //  sub:  sub_start -> split =< c1(call "leaf"), trigger(svc) -> inner_stop(terminate) >
    let leaf = ProcessBuilder::new("leaf")
        .start_event("ls")
        .service_task("leaf_work", "leaf-job")
        .end_event("le")
        .connect("ls", "leaf_work")
        .connect("leaf_work", "le")
        .build()
        .unwrap();
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .call_activity("c1", "leaf")
        .contained_in("c1", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("inner_stop")
        .contained_in("inner_stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "c1")
        .connect("split", "trigger")
        .connect("trigger", "inner_stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(leaf)).unwrap();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The call activity spawned a distinct child parked on its own job; the
    // trigger branch parks on its own job.
    let child_key = engine
        .pending_jobs()
        .iter()
        .find(|j| j.job_type == "leaf-job")
        .expect("leaf child parked on its job")
        .instance_key;
    assert_ne!(child_key, instance_key);
    assert!(engine
        .pending_jobs()
        .iter()
        .any(|j| j.job_type == "trigger-job"));

    // Completing the trigger drives its token into the sub-process terminate end.
    let events = complete_one(&mut engine, "trigger-job");

    // The call-activity child in the terminated scope is terminated too, and its
    // job cancelled with it.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == child_key
    )));
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(!engine
        .pending_jobs()
        .iter()
        .any(|j| j.job_type == "leaf-job"));

    // The parent continues past the sub-process; the parent instance itself is
    // NOT terminated.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "after"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == instance_key
    )));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );
}

#[test]
fn top_level_terminate_end_in_a_called_process_completes_the_parent_call_activity() {
    // A terminate end in a CALLED process ends only that child instance (Zeebe/
    // BPMN: terminate never propagates out of its own process). The child is the
    // root scope of its own instance, so a top-level terminate end COMPLETES it
    // (#1085 — `PROCESS -> ELEMENT_COMPLETED`, only inner tokens TERMINATED). The
    // parent's call activity then completes normally and the parent continues on
    // its outgoing flow — the parent must not be left parked forever, nor
    // terminated.
    //
    //  phase (called): ps -> psplit =< work(svc), trg(svc) -> stop(terminate) >
    //  orch:           start -> c1(call "phase") -> end
    let phase = ProcessBuilder::new("phase")
        .start_event("ps")
        .parallel_gateway("psplit")
        .service_task("work", "work-job")
        .service_task("trg", "trg-job")
        .terminate_end_event("stop")
        .connect("ps", "psplit")
        .connect("psplit", "work")
        .connect("psplit", "trg")
        .connect("trg", "stop")
        .build()
        .unwrap();
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(phase)).unwrap();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .pending_jobs()
        .first()
        .expect("child parked on its jobs")
        .instance_key;
    assert_ne!(child_key, parent_key);
    assert_eq!(engine.pending_jobs().len(), 2);

    // Completing the child's trg job drives its token into the top-level
    // terminate end of the called process.
    let events = complete_one(&mut engine, "trg-job");

    // The child completes (its sibling work token cancelled), and its terminal
    // record is COMPLETED, not TERMINATED (#1085)...
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCompleted { instance_key: ik } if *ik == child_key
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == child_key
    )));
    // ...and the terminate does NOT propagate to the parent: the call activity
    // completes and the parent runs to ordinary completion.
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key: ik } if *ik == parent_key
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "c1" && to == "end"
    )));
    assert!(engine.is_completed(parent_key));
    assert!(engine.is_completed(child_key));
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Completed
    );
}

/// Issue #1170 regression — the NORMAL (non-boundary) completion of a
/// multi-instance CALL ACTIVITY must drain the loop. This PR taught
/// `run_mi_child_behaviour` to spawn a real callee process instance per loop
/// item; without a matching completion route a callee that finishes normally is
/// handled by `complete_call_activity`, which takes the activity's outgoing flow
/// and never emits `MultiInstanceChildCompleted` — so the callee's key stays in
/// the body's `active` set and the loop hangs forever. Assert the loop drains,
/// aggregates each callee's output through the MI `outputElement`, and completes.
#[test]
fn multi_instance_call_activity_completes_normally_and_aggregates_output() {
    // callee: cs -> ct("work") -> ce. The "work" job completes with a `result`
    // variable that propagates back (propagateAllChildVariables defaults true).
    let callee = ProcessBuilder::new("callee")
        .start_event("cs")
        .service_task("ct", "work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    // start -> each(MI call "callee", outputElement `=result`) -> sink -> end.
    let def = ProcessBuilder::new("mi-call")
        .start_event("start")
        .call_activity("each", "callee")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=result".to_string()),
                completion_condition: None,
                sequential: false,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("done")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(callee))
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-call",
            vars(&[("items", Value::List(vec![Value::Int(10), Value::Int(20)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Two callee instances, each parked on its "work" job.
    let jobs = engine.activate_jobs("work", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 2, "one callee per loop item");
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    // Complete each callee's job with a distinct `result` echoing its item — out
    // of collection order, to prove index-based aggregation survives the route.
    let mut ordered = jobs.clone();
    ordered.sort_by_key(|j| {
        std::cmp::Reverse(j.variables.get("item").and_then(Value::as_f64).unwrap() as i64)
    });
    for job in &ordered {
        let item = job.variables.get("item").cloned().unwrap();
        assert!(!engine.is_completed(key), "loop must not drain early");
        engine
            .apply_command(Command::complete_job_with_result(
                job.key,
                HashMap::from([("result".to_string(), item)]),
                crate::model::AdHocJobResult::default(),
            ))
            .unwrap();
    }

    // The loop drained: its runtime record is gone and the token advanced to the
    // sink task (so the aggregate is still observable on the still-active
    // instance). Results are ordered by loop index regardless of completion order.
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared — the loop did not hang (#1170)"
    );
    assert!(
        engine
            .state()
            .jobs
            .values()
            .any(|j| j.job_type == "sink-work" && matches!(j.state, state::JobState::Created)),
        "the loop advanced to the activity's outgoing flow exactly once"
    );
    assert_eq!(
        engine.instance(key).unwrap().variables.get("results"),
        Some(&Value::List(vec![Value::Int(10), Value::Int(20)])),
        "each callee's propagated `result` aggregates through the MI outputElement in order"
    );
}

/// Issue #1175 (gap 2) — a CALL-ACTIVITY multi-instance child's `zeebe:input`
/// mappings must be evaluated exactly once, into the isolated child PROCESS
/// scope, and the callee-id `=calledElement` expression must NOT see the callee's
/// own input-mapped locals. `activate_mi_child` used to apply the mappings into
/// the child scope and ALSO hand that mapped view to `spawn_call_activity_child`,
/// which re-applied them (a second evaluation that compounds for a self-
/// referencing mapping) and let the callee-id expression resolve against the
/// contaminated view.
#[test]
fn multi_instance_call_activity_child_applies_inputs_once_and_keeps_callee_id_clean() {
    // callee "target-proc": start -> cw("callee-work") -> end.
    let callee = ProcessBuilder::new("target-proc")
        .start_event("cs")
        .service_task("cw", "callee-work")
        .end_event("ce")
        .connect("cs", "cw")
        .connect("cw", "ce")
        .build()
        .unwrap();
    // Parent: the callee id lives in `callee` = "target-proc". Two inputs:
    //  - `callee` := "WRONG-proc"  (would derail the callee-id if it leaked)
    //  - `n`      := n + 1         (self-referencing — doubles if applied twice)
    let def = ProcessBuilder::new("mi-call")
        .start_event("start")
        .call_activity("each", "=callee")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![
                    crate::model::Mapping {
                        source: "=\"WRONG-proc\"".to_string(),
                        target: "callee".to_string(),
                    },
                    crate::model::Mapping {
                        source: "=n + 1".to_string(),
                        target: "n".to_string(),
                    },
                ],
                outputs: vec![],
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
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-call",
            vars(&[
                ("items", Value::List(vec![Value::Int(1)])),
                ("callee", Value::Str("target-proc".to_string())),
                ("n", Value::Int(1)),
            ]),
        ))
        .unwrap();

    // The callee-id `=callee` resolved against the un-contaminated view
    // ("target-proc"), so a real callee spawned and parked on its work job. On
    // `main` the input-mapped `callee`="WRONG-proc" derailed the lookup into an
    // unknown-callee incident and no child spawned.
    assert!(
        engine.active_incidents().is_empty(),
        "no unknown-callee incident — the callee-id ignored the input-mapped locals (gap 2)"
    );
    let jobs = engine.activate_jobs("callee-work", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1, "the real callee (target-proc) was spawned");
    // The self-referencing `n := n + 1` mapping was applied EXACTLY ONCE into the
    // child seed (1 -> 2). Applied twice (the old double-eval) it would be 3.
    assert_eq!(
        jobs[0].variables.get("n"),
        Some(&Value::Int(2)),
        "call-activity input mappings applied exactly once into the child (gap 2)"
    );
    // And the input mapping did reach the child seed (it seeds the child process).
    assert_eq!(
        jobs[0].variables.get("callee"),
        Some(&Value::Str("WRONG-proc".to_string())),
        "the input mapping still seeds the isolated child process scope"
    );
}

/// Issue #1175 (gap 3) — a failed CALL-ACTIVITY spawn on a multi-instance child
/// must re-drive MI-aware on incident resolution: the retry must re-evaluate the
/// callee id (and the input mappings) against the CHILD's scope, which carries
/// the per-child `inputElement`/`loopCounter` bindings. The generic spawn retry
/// used to evaluate against the enclosing MI-body scope, which does not carry
/// those bindings, so a callee-id `=item` (or a mapping reading `loopCounter`)
/// failed again on resolution and the loop never advanced.
#[test]
fn multi_instance_call_activity_spawn_retry_is_mi_aware() {
    // callee "callee-1": start -> cw("callee-work") -> end.
    let callee = ProcessBuilder::new("callee-1")
        .start_event("cs")
        .service_task("cw", "callee-work")
        .end_event("ce")
        .connect("cs", "cw")
        .connect("cw", "ce")
        .build()
        .unwrap();
    // The callee id is the per-child `inputElement` (`=item`). Its value only
    // exists in the child scope, so a non-MI-aware retry cannot resolve it.
    let def = ProcessBuilder::new("mi-call")
        .start_event("start")
        .call_activity("each", "=item")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .end_event("done")
        .connect("start", "each")
        .connect("each", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    // Deploy ONLY the parent first: the callee is not yet deployed, so the child
    // spawn parks a CalledElementError incident.
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-call",
            vars(&[(
                "items",
                Value::List(vec![Value::Str("callee-1".to_string())]),
            )]),
        ))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the undeployed callee parks one spawn incident"
    );
    assert_eq!(active[0].kind, state::IncidentKind::CalledElementError);
    let incident_key = active[0].key;

    // Deploy the callee, then resolve the incident. The MI-aware retry must
    // re-evaluate `=item` against the child scope (item = "callee-1") and spawn.
    engine
        .apply_command(Command::DeployProcess(callee))
        .unwrap();
    engine
        .apply_command(Command::ResolveIncident {
            incident_key,
            operation_reference: None,
        })
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "the MI-aware spawn retry resolved the callee id from the child bindings (gap 3)"
    );
    let jobs = engine.activate_jobs("callee-work", "W", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "the retry spawned the real callee for the MI child (gap 3)"
    );
}

/// Issue #1170 regression — an EARLY multi-instance completion (satisfied
/// `completionCondition`) that cancels a still-running call-activity child must
/// terminate that child's separate callee process instance, not just the child
/// token. `cancel_mi_child_events` only reaches resources owned by the child
/// element instance, so the distinct callee would otherwise leak (run forever).
#[test]
fn early_multi_instance_completion_terminates_running_call_activity_callees() {
    let callee = ProcessBuilder::new("callee")
        .start_event("cs")
        .service_task("ct", "work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    // A parallel MI call activity whose completion condition fires as soon as ONE
    // child completes — leaving the others' callees still running to be cancelled.
    let def = ProcessBuilder::new("mi-call")
        .start_event("start")
        .call_activity("each", "callee")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: Some("=true".to_string()),
                sequential: false,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("done")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(callee))
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-call",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("work", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 3, "three callee instances, one per loop item");
    fn live_callees(engine: &Engine, parent: Key) -> usize {
        engine
            .state()
            .instances
            .values()
            .filter(|i| {
                i.parent_process_instance_key == Some(parent)
                    && matches!(i.state, ProcessInstanceState::Active)
            })
            .count()
    }
    assert_eq!(
        live_callees(&engine, key),
        3,
        "all three callees are running"
    );

    // Completing one child satisfies `completionCondition` and ends the body
    // early — the other two callees must be terminated, not orphaned.
    let fired = engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert!(
        fired
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceTerminated { .. }))
            .count()
            >= 2,
        "the two still-running callees are terminated on early completion (#1170)"
    );
    assert_eq!(
        live_callees(&engine, key),
        0,
        "no callee process instance leaks past the early completion"
    );
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the loop's runtime record is cleared"
    );
}

#[test]
fn inline_call_activities_expands_a_call_into_an_embedded_subprocess() {
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("phase".to_string(), phase_process("phase", "work"));

    let expanded = orchestrator.inline_call_activities(&library).unwrap();

    // The call activity is now a sub-process whose inner start is the
    // prefixed copy of the callee's start event.
    let c1 = expanded.element("c1").unwrap();
    match &c1.kind {
        ElementKind::SubProcess { start_event } => assert_eq!(start_event, "c1$pstart"),
        other => panic!("expected SubProcess, got {other:?}"),
    }
    // The callee's elements were spliced in, id-prefixed and parented to c1.
    let inner = expanded.element("c1$work").unwrap();
    assert_eq!(inner.parent.as_deref(), Some("c1"));
    match &inner.kind {
        ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, "work"),
        other => panic!("expected ServiceTask, got {other:?}"),
    }
    // No CallActivity kind survives expansion.
    assert!(!expanded
        .elements
        .values()
        .any(|e| matches!(e.kind, ElementKind::CallActivity { .. })));
}

#[test]
fn an_expanded_call_activity_runs_to_completion_through_the_engine() {
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("phase".to_string(), phase_process("phase", "work"));
    let expanded = orchestrator.inline_call_activities(&library).unwrap();

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(expanded))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token enters the inlined sub-process and parks on the phase's job.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].job_type, "work");

    // Completing it drains the sub-process and routes out to the orchestrator end.
    let events = complete_one(&mut engine, "work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "c1" && to == "end"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn inline_call_activities_expands_nested_calls_with_unique_prefixes() {
    // orch -> c1(callee=middle); middle -> m(callee=leaf); leaf -> work.
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "middle")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let middle = ProcessBuilder::new("middle")
        .start_event("pstart")
        .call_activity("m", "leaf")
        .end_event("pend")
        .connect("pstart", "m")
        .connect("m", "pend")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("middle".to_string(), middle);
    library.insert("leaf".to_string(), phase_process("leaf", "work"));

    let expanded = orchestrator.inline_call_activities(&library).unwrap();

    // The leaf's job nests two prefixes deep and is parented to the inner call.
    let leaf_work = expanded.element("c1$m$work").unwrap();
    assert_eq!(leaf_work.parent.as_deref(), Some("c1$m"));
    // Both call activities became sub-processes; none remain as calls.
    assert!(!expanded
        .elements
        .values()
        .any(|e| matches!(e.kind, ElementKind::CallActivity { .. })));
}

#[test]
fn inline_call_activities_rejects_unknown_and_cyclic_callees() {
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "missing")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let empty = std::collections::HashMap::new();
    assert!(orchestrator
        .inline_call_activities(&empty)
        .unwrap_err()
        .contains("unknown process 'missing'"));

    // A self-recursive callee is rejected rather than expanded forever.
    let recursive = ProcessBuilder::new("loop")
        .start_event("start")
        .call_activity("again", "loop")
        .end_event("end")
        .connect("start", "again")
        .connect("again", "end")
        .build()
        .unwrap();
    let mut library = std::collections::HashMap::new();
    library.insert("loop".to_string(), recursive.clone());
    assert!(recursive
        .inline_call_activities(&library)
        .unwrap_err()
        .contains("cycle"));
}

// zeebe-cells: element:CallActivity
#[test]
fn a_call_activity_spawns_a_distinct_child_process_instance_with_parent_linkage() {
    // Native execution (Zeebe parity): a call activity does NOT inline the
    // callee — it spawns a distinct child process instance linked back to the
    // calling instance and its call-activity element instance.
    let mut engine = deploy_native_call(Default::default(), phase_process("phase", "work"));
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // The call-activity element instance the child links back to.
    let call_eik = created
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "c1" => Some(*element_instance_key),
            _ => None,
        })
        .expect("c1 activated");

    // A second, distinct process instance was created for the callee, carrying
    // the parent linkage (C8 parentProcessInstanceKey / parentElementInstanceKey).
    let (child_key, ppik, peik) = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                parent_process_instance_key,
                parent_element_instance_key,
                ..
            } if process_id == "phase" => Some((
                *instance_key,
                *parent_process_instance_key,
                *parent_element_instance_key,
            )),
            _ => None,
        })
        .expect("a child instance of 'phase' was created");
    assert_ne!(child_key, parent_key, "child is a distinct instance");
    assert_eq!(ppik, Some(parent_key));
    assert_eq!(peik, Some(call_eik));

    // Both instances are live; the parent's call-activity token is parked on the
    // child, and the child parks on its own (instance-scoped) job.
    assert!(!engine.is_completed(parent_key));
    assert!(!engine.is_completed(child_key));
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].instance_key, child_key);
    assert_eq!(
        engine
            .instance(child_key)
            .unwrap()
            .parent_process_instance_key,
        Some(parent_key)
    );

    // Completing the child's job runs it to its (none) end; the parent's
    // call-activity token then completes and routes out to the orchestrator end.
    let events = complete_one(&mut engine, "work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCompleted { instance_key } if *instance_key == child_key
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "c1" && to == "end"
    )));
    assert!(engine.is_completed(child_key));
    assert!(engine.is_completed(parent_key));
}

#[test]
fn cancelling_a_call_activity_parent_cancels_its_child() {
    let mut engine = deploy_native_call(Default::default(), phase_process("phase", "work"));
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .pending_jobs()
        .first()
        .expect("child parked on its job")
        .instance_key;
    assert_ne!(child_key, parent_key);

    let cancel = engine
        .apply_command(Command::cancel_instance(parent_key))
        .unwrap();
    // Cancelling the parent cascades to the in-flight child (Zeebe parity).
    assert!(cancel.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key } if *instance_key == parent_key
    )));
    assert!(cancel.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
    )));
    assert_eq!(
        engine.instance(parent_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    // The child's job was cancelled with it — no activatable jobs remain.
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn a_call_activity_propagates_variables_via_io_mappings_across_isolated_scopes() {
    // The callee is a pass-through (pstart -> pend) so it completes on its seed
    // variables, letting us observe both directions of the mapping in one command.
    // Both Zeebe propagation flags are OFF here, so the *only* channel across the
    // isolated scopes is the ioMapping (the parity defaults are exercised by the
    // `call_activity_*propagate*` tests).
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .end_event("pend")
        .connect("pstart", "pend")
        .build()
        .unwrap();
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=orderId".to_string(),
            target: "childOrder".to_string(),
        }],
        outputs: vec![crate::model::Mapping {
            source: "=childOrder".to_string(),
            target: "parentEcho".to_string(),
        }],
    };
    let mut engine = deploy_native_call_with_propagation(io, child, false, false);

    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("orderId", Value::Int(42))]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // The child is seeded ONLY through the input mapping (isolated scope): it
    // sees `childOrder`, not the parent's other variable `orderId`.
    let child_seed = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                process_id,
                variables,
                ..
            } if process_id == "phase" => Some(variables.clone()),
            _ => None,
        })
        .expect("child created");
    assert_eq!(child_seed.get("childOrder"), Some(&Value::Int(42)));
    assert!(
        !child_seed.contains_key("orderId"),
        "parent variables must not leak into the child's isolated scope"
    );

    // The whole orchestration ran to completion; the output mapping projected the
    // child's variable back into the parent as `parentEcho`. The child's own
    // `childOrder` did not leak wholesale into the parent.
    assert!(engine.is_completed(parent_key));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::VariablesUpdated { instance_key, variables }
            if *instance_key == parent_key && variables.get("parentEcho") == Some(&Value::Int(42))
    )));
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == parent_key && variables.contains_key("childOrder")
        )),
        "only the mapped output crosses back, not the child's raw scope"
    );
}

#[test]
fn call_activity_default_propagates_all_parent_and_child_variables() {
    // Zeebe default (both attributes absent ⇒ true), no ioMappings: all visible
    // parent variables cross into the child, and all of the child's final
    // variables merge back into the parent (child wins on a collision).
    let mut engine =
        deploy_native_call_with_propagation(Default::default(), propagating_child(), true, true);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[
                ("orderId", Value::Int(42)),
                ("shared", Value::Str("parent".into())),
            ]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // Parent → child: the whole visible parent scope crossed into the child.
    let seed = child_seed_of(&created);
    assert_eq!(seed.get("orderId"), Some(&Value::Int(42)));
    assert_eq!(seed.get("shared"), Some(&Value::Str("parent".into())));

    // Child → parent: the child's fresh variable crossed back, and the shared
    // collision resolved to the child's value.
    assert!(engine.is_completed(parent_key));
    assert_eq!(
        parent_var_after(&created, parent_key, "childOnly"),
        Some(&Value::Int(99))
    );
    assert_eq!(
        parent_var_after(&created, parent_key, "shared"),
        Some(&Value::Str("child".into())),
        "child wins on a merge-back name collision"
    );
}

#[test]
fn call_activity_propagate_all_parent_false_copies_only_input_mapping_results() {
    // propagateAllParentVariables="false": only the call activity's local
    // variables — its input-mapping results — cross into the child. The child
    // variables still merge back (propagateAllChildVariables defaults true).
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=orderId".to_string(),
            target: "childOrder".to_string(),
        }],
        outputs: Vec::new(),
    };
    let mut engine = deploy_native_call_with_propagation(io, propagating_child(), false, true);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[
                ("orderId", Value::Int(42)),
                ("shared", Value::Str("parent".into())),
            ]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // Only the input mapping seeded the child; the parent's other variables did
    // not cross.
    let seed = child_seed_of(&created);
    assert_eq!(seed.get("childOrder"), Some(&Value::Int(42)));
    assert!(
        !seed.contains_key("orderId") && !seed.contains_key("shared"),
        "with propagateAllParentVariables=false only input-mapping results cross"
    );

    // Child → parent still merges (default true): the child's fresh variable and
    // its own (mapping-seeded) childOrder cross back.
    assert!(engine.is_completed(parent_key));
    assert_eq!(
        parent_var_after(&created, parent_key, "childOnly"),
        Some(&Value::Int(99))
    );
}

#[test]
fn call_activity_propagate_all_child_false_without_output_mappings_copies_nothing_back() {
    // propagateAllChildVariables="false" with no output mappings: nothing crosses
    // back. Parent → child still copies all visible variables (default true).
    let mut engine =
        deploy_native_call_with_propagation(Default::default(), propagating_child(), true, false);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[
                ("orderId", Value::Int(42)),
                ("shared", Value::Str("parent".into())),
            ]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    // Parent → child copied everything.
    let seed = child_seed_of(&created);
    assert_eq!(seed.get("orderId"), Some(&Value::Int(42)));

    // Child → parent copied nothing: the child's fresh variable never appears in
    // the parent, and the shared collision left the parent's value intact.
    assert!(engine.is_completed(parent_key));
    assert_eq!(
        parent_var_after(&created, parent_key, "childOnly"),
        None,
        "no child variable crosses back with propagateAllChildVariables=false and no output mappings"
    );
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == parent_key
                    && variables.get("shared") == Some(&Value::Str("child".into()))
        )),
        "the child's overwrite of the shared variable must not cross back"
    );
}

#[test]
fn call_activity_propagate_all_child_false_still_applies_output_mappings() {
    // propagateAllChildVariables="false" but WITH an output mapping: only the
    // mapped output crosses back (output mappings always apply), the child's raw
    // scope does not.
    let io = crate::model::IoMapping {
        inputs: Vec::new(),
        outputs: vec![crate::model::Mapping {
            source: "=childOnly".to_string(),
            target: "echoed".to_string(),
        }],
    };
    let mut engine = deploy_native_call_with_propagation(io, propagating_child(), true, false);
    let created = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("orderId", Value::Int(42))]),
        ))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    assert!(engine.is_completed(parent_key));
    // The output mapping projected childOnly into the parent as `echoed`…
    assert_eq!(
        parent_var_after(&created, parent_key, "echoed"),
        Some(&Value::Int(99))
    );
    // …but the child's raw variable did not cross back wholesale.
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == parent_key && variables.contains_key("childOnly")
        )),
        "with propagateAllChildVariables=false only the mapped output crosses back"
    );
}

#[test]
fn call_activity_input_mapping_failure_raises_io_mapping_and_respawns_on_resolve() {
    // #946 item 1 (call-activity input): an input `zeebe:ioMapping` on a call
    // activity that fails to evaluate must raise `IO_MAPPING_ERROR` (NOT
    // `ExpressionEvaluation`) and PARK the call activity without spawning the
    // child — not skip straight to `Complete`. Resolution re-drives the *spawn*
    // (`CallActivitySpawn`): the now-fixed input mapping is re-applied and the
    // child process is created, running the orchestration to completion.
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .end_event("pend")
        .connect("pstart", "pend")
        .build()
        .unwrap();
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=orderId + 1".to_string(),
            target: "childOrder".to_string(),
        }],
        outputs: Vec::new(),
    };
    let mut engine = deploy_native_call(io, child);
    let parent = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("orderId", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(
        active[0].redrive,
        Some(state::IoMappingRedrive::CallActivitySpawn),
        "a call-activity input failure re-drives the child spawn, not a generic complete",
    );
    assert_eq!(active[0].element_id, "c1");
    assert!(
        !engine.is_completed(parent),
        "parked before the child spawns"
    );
    // No child process instance was created.
    assert_eq!(
        engine.state().instances.len(),
        1,
        "no child spawned while the input mapping is unresolved"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `orderId` and resolve: the spawn re-drives, the child is created and
    // (being a pass-through) completes, and the orchestration finishes.
    engine
        .apply_command(Command::set_variables(
            parent,
            HashMap::from([("orderId".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(parent).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(parent),
        "the orchestration completes once the child spawns and finishes"
    );
    assert_eq!(
        engine.state().instances.len(),
        2,
        "the child process instance was spawned on resolution"
    );
}

#[test]
fn call_activity_output_mapping_failure_preserves_child_context_on_redrive() {
    // #946 item 3 (call-activity output): an output `zeebe:ioMapping` on a call
    // activity that fails must raise `IO_MAPPING_ERROR`, hold the call activity in
    // COMPLETING, and — critically — *preserve the completed child's variables* on
    // the incident so the re-drive re-projects them (`CallActivityCompletion`).
    // The old generic `Complete` re-drive neither carried the child result nor
    // called `complete_call_activity`, silently dropping the callee context.
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .end_event("pend")
        .connect("pstart", "pend")
        .build()
        .unwrap();
    let io = crate::model::IoMapping {
        inputs: vec![crate::model::Mapping {
            source: "=tag".to_string(),
            target: "childTag".to_string(),
        }],
        outputs: vec![crate::model::Mapping {
            // `childTag` is a string; `string + 1` is a FEEL type error.
            source: "=childTag + 1".to_string(),
            target: "echoed".to_string(),
        }],
    };
    let mut engine = deploy_native_call(io, child);
    let parent = engine
        .apply_command(Command::create_instance_with(
            "orch",
            vars(&[("tag", Value::Str("gamma".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(active[0].element_id, "c1");
    assert!(
        !engine.is_completed(parent),
        "must not complete while the output mapping is unresolved"
    );
    // The completed child's variables are preserved on the incident's re-drive
    // descriptor so completion can be retried against them after the gone child.
    match &active[0].redrive {
        Some(state::IoMappingRedrive::CallActivityCompletion { child_variables }) => {
            assert_eq!(
                child_variables.get("childTag"),
                Some(&Value::Str("gamma".into())),
                "the callee result is captured on the incident for the re-drive"
            );
        }
        other => panic!("expected CallActivityCompletion redrive, got {other:?}"),
    }
}

/// Cancelling a call-activity parent reaps a child an operator suspended:
/// suspension pauses the child, it does not detach it from its parent.
#[test]
fn cancelling_a_call_activity_parent_cancels_its_suspended_child() {
    let mut engine = deploy_native_call(Default::default(), phase_process("phase", "work"));
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine.pending_jobs()[0].instance_key;
    engine
        .apply_command(Command::suspend_instance(child_key))
        .unwrap();
    engine
        .apply_command(Command::cancel_instance(parent_key))
        .unwrap();
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated,
        "a suspended child must not outlive its cancelled parent"
    );
}

/// Terminating a call activity by modification reaps its suspended child.
#[test]
fn terminating_a_call_activity_terminates_its_suspended_child() {
    let mut engine = deploy_native_call(Default::default(), phase_process("phase", "work"));
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine.pending_jobs()[0].instance_key;
    engine
        .apply_command(Command::suspend_instance(child_key))
        .unwrap();
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: parent_key,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["c1".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated,
        "a suspended child must not outlive its terminated call activity"
    );
}
