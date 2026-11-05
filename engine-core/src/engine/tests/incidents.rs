//! `incidents` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn inline_script_task_raises_an_incident_when_the_expression_fails_and_recovers() {
    // A script whose FEEL expression cannot evaluate (type error: string + int)
    // raises an ExpressionEvaluation incident and parks the token — matching
    // Zeebe (and nano's exclusive gateway), not a silent pass-through. Fixing
    // the variable and resolving the incident re-evaluates the script, which
    // then writes the result and lets the instance continue.
    let def = ProcessBuilder::new("scripted-fail")
        .start_event("start")
        .script_task("calc", "=assert(n, n instance of number) + 1", "next")
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "scripted-fail",
            vars(&[("n", Value::Str("oops".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The script failed: one active ExpressionEvaluation incident, no job, and
    // the token is parked (the downstream service task never activated).
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(!engine.is_completed(key));
    let incident_key = engine.incidents()[0].key;

    // Fix the variable to a number and resolve the incident: the script
    // re-evaluates, writes `next`, and the token advances to the service task.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("n".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert_eq!(engine.state().jobs.len(), 1);
    assert_eq!(
        engine.instance(key).unwrap().variables.get("next"),
        Some(&Value::Int(42))
    );
}

#[test]
fn should_raise_incident_when_inclusive_split_matches_no_flow() {
    // No condition holds and there is no default flow: the split parks on a
    // NoMatchingSequenceFlow incident rather than silently dropping the token.
    let def = ProcessBuilder::new("inc-stuck")
        .start_event("s")
        .inclusive_gateway("isplit")
        .end_event("hot")
        .connect("s", "isplit")
        .connect_when("isplit", "hot", "temp > 100")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("temp".to_string(), Value::Int(20))]);
    let events = engine
        .apply_command(Command::create_instance_with("inc-stuck", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised {
            kind: state::IncidentKind::NoMatchingSequenceFlow,
            ..
        }
    )));
    assert!(!engine.is_completed(instance_key));
}

#[test]
fn inclusive_join_incident_redrive_does_not_duplicate_outgoing_routing() {
    // #1168 regression (join redrive / stale bookkeeping). A multi-incoming
    // inclusive gateway that is ALSO a conditional split: both branches arrive,
    // the post-consumption selection matches no flow (and there is no default), so
    // the join parks on a `NoMatchingSequenceFlow` incident. Resolving that
    // incident re-drives `Step::Complete`, which lands in the split-completion
    // path. That path emits `ElementCompleted` but the reducer does NOT clear the
    // join maps on `ElementCompleted`; without an explicit `ParallelJoinReset` the
    // stale open-join entry is fired again by the next arrival and DUPLICATES
    // the outgoing routing. Assert the join routes its outgoing flow exactly once.
    let def = ProcessBuilder::new("inc-redrive")
        .start_event("s")
        .parallel_gateway("psplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .inclusive_gateway("join")
        .end_event("out")
        .connect("s", "psplit")
        .connect("psplit", "a")
        .connect("psplit", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect_when("join", "out", "go")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "inc-redrive",
            HashMap::from([("go".to_string(), Value::Bool(false))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Both branches arrive at the join; with `go=false` the join's single
    // conditional flow matches nothing and no default exists -> incident.
    complete_one(&mut engine, "ja");
    complete_one(&mut engine, "jb");
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the join must park on a single incident");
    assert_eq!(active[0].kind, state::IncidentKind::NoMatchingSequenceFlow);
    assert!(!engine.is_completed(instance_key));
    let incident_key = engine.incidents()[0].key;

    // Fix the variable and resolve: the join re-selects, now routes `out`, and
    // completes. The stale join bookkeeping must be cleared so it does not fire a
    // second time.
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("go".to_string(), Value::Bool(true))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    let routed = resolved
        .iter()
        .filter(|e| matches!(e, Event::SequenceFlowTaken { from, to, .. } if from == "join" && to == "out"))
        .count();
    assert_eq!(
        routed, 1,
        "the join must route its outgoing flow exactly once on redrive, not duplicate it"
    );
    assert_eq!(
        resolved
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceCompleted { .. }))
            .count(),
        1,
        "the instance must complete exactly once"
    );
    assert!(engine.is_completed(instance_key));
    assert!(engine.active_incidents().is_empty());
}

/// Regression (#1168 debugger observability): a ready inclusive join whose
/// outgoing conditions select no flow raises an `IncidentRaised` instead of
/// taking a flow. Originally this came from a quiescence sweep that bypassed
/// the `StepDriver`; the join is now decided at arrival time inside an ordinary
/// step (#1241), and this guards that the incident is still observed by a
/// `BreakCondition::EveryStep` debug session. It drives the second branch's
/// completion under the debugger and asserts the incident is observed in a
/// pause *delta* (a slice the driver was actually consulted on), not merely
/// present in the final tail.
#[test]
fn inclusive_join_incident_is_observed_by_the_step_driver() {
    let def = ProcessBuilder::new("inc-redrive")
        .start_event("s")
        .parallel_gateway("psplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .inclusive_gateway("join")
        .end_event("out")
        .connect("s", "psplit")
        .connect("psplit", "a")
        .connect("psplit", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect_when("join", "out", "go")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "inc-redrive",
            HashMap::from([("go".to_string(), Value::Bool(false))]),
        ))
        .unwrap();

    // First branch arrives at the join via a plain command.
    complete_one(&mut engine, "ja");

    // Drive the SECOND branch's completion under the debugger, single-stepping on
    // every step. Completing `jb` takes the join's last incoming flow: the join
    // consumes its tokens, then raises `NoMatchingSequenceFlow` (go=false, no
    // default) before routing.
    let jb = engine
        .activate_jobs("jb", "test-worker", 10, 60_000, 0)
        .into_iter()
        .next()
        .expect("jb activatable");
    let mut session = engine
        .debug_command_at(
            Command::complete_job(jb.key),
            0,
            vec![BreakCondition::EveryStep],
        )
        .expect("debug complete jb");

    // Walk pause-to-pause; the incident must appear in a pause DELTA (an event
    // slice the driver was actually consulted on), not merely in the final tail.
    let mut incident_observed_at_a_pause = false;
    let mut seen = 0usize;
    while session.is_paused() {
        let delta = &session.log()[seen..];
        if delta
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { .. }))
        {
            incident_observed_at_a_pause = true;
        }
        seen = session.log().len();
        engine.debug_step(&mut session);
    }
    assert!(
        incident_observed_at_a_pause,
        "EveryStep must observe the inclusive-join incident at a pause boundary"
    );
    // Sanity: the incident really was raised on this run.
    assert!(session
        .log()
        .iter()
        .any(|e| matches!(e, Event::IncidentRaised { .. })));
}

/// Issue #1170 regression — firing an interrupting boundary on a multi-instance
/// body must RESOLVE an incident sitting on the body root itself, not only on
/// its swept descendants. `terminate_subprocess_scope` resolves descendant
/// incidents (`scope_teardown_events` -> `resolve_incidents_on`) but never the
/// scope root, so a body parked on a FAILED start-listener job (a `JobNoRetries`
/// incident) would keep the completed instance carrying a stale incident for a
/// vanished body. Assert the incident is resolved when the boundary fires.
#[test]
fn interrupting_boundary_on_a_multi_instance_body_resolves_a_stale_body_incident() {
    let def = ProcessBuilder::new("mi")
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
                sequential: false,
            },
        )
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .with_listeners(
            "each",
            vec![el(ListenerEventType::Start, "mi-start")],
            Vec::new(),
        )
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The body parks on its start-listener job; fail it with no retries so the
    // body element instance carries a JobNoRetries incident.
    let listener = engine.activate_jobs("mi-start", "W", 10, 1_000, 0);
    assert_eq!(
        listener.len(),
        1,
        "the body is parked on a start-listener job"
    );
    engine
        .apply_command(Command::fail_job(listener[0].key, 0, "boom"))
        .unwrap();
    assert_eq!(
        engine.active_incidents().len(),
        1,
        "the failed body start-listener job raised an incident on the body"
    );

    // The boundary fires while the body carries that stale incident.
    let fired = engine.trigger_timers(5_000);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "firing the boundary resolves the incident on the body root"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no stale incident remains on the vanished body"
    );
    assert!(engine.is_completed(key), "the instance completes (#1170)");
}

/// Issue #1170 regression — an EARLY multi-instance completion (satisfied
/// `completionCondition`) that cancels a still-running MI child parked on a ROOT
/// incident (a service task's `JobNoRetries`) must RESOLVE that incident, not
/// merely cancel the job and complete the child. The early-cancel path sweeps a
/// child's *descendants* (`scope_teardown_events` -> `resolve_incidents_on`) but
/// `cancel_mi_child_events` completes the child ROOT via `ElementCompleted`,
/// which touches no incident state — and `ProcessInstanceCompleted` deliberately
/// RETAINS incidents. Without resolving the child-root incident, the completed
/// instance would carry a stale active incident tied to a removed element that a
/// later external resolve could re-drive against a dead token. Assert the
/// incident is resolved on early completion and no active incident survives.
#[test]
fn early_multi_instance_completion_resolves_a_stale_child_root_incident() {
    // start -> each(MI service "handle", parallel, completionCondition = true)
    //          -> sink -> done. Two items: one child completes (firing the
    // condition) while the sibling is parked on a JobNoRetries incident.
    let def = ProcessBuilder::new("mi-incident-early")
        .start_event("start")
        .service_task("each", "handle")
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
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-incident-early",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let jobs = engine.activate_jobs("handle", "W", 10, 60_000, 0);
    assert_eq!(jobs.len(), 2, "each MI child runs a `handle` job");

    // Park the sibling on a JobNoRetries incident (fail with no retries).
    let parked = jobs[1].key;
    engine
        .apply_command(Command::fail_job(parked, 0, "boom"))
        .unwrap();
    assert_eq!(
        engine.active_incidents().len(),
        1,
        "the failed sibling job raised a root incident on its MI child"
    );

    // Completing the OTHER child satisfies `completionCondition` and ends the
    // body early — the parked sibling child is cancelled AND its stale incident
    // must be resolved, not left tied to the removed element.
    let fired = engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "the early completion resolves the parked child's root incident (#1170)"
    );
    assert_eq!(
        engine.state().jobs[&parked].state,
        state::JobState::Canceled,
        "the parked sibling job is cancelled, not resurrected by its resolve"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no stale active incident survives on the removed MI child (#1170)"
    );
    assert!(
        engine.instance(key).unwrap().multi_instances.is_empty(),
        "the loop's runtime record is cleared"
    );
    assert!(
        engine
            .pending_jobs()
            .iter()
            .any(|j| j.job_type == "sink-work"),
        "the early completion advances the body to its outgoing flow (`sink`)"
    );
}

#[test]
fn subprocess_terminate_end_resolves_an_incident_in_its_scope() {
    // A terminate end must resolve an incident parked on a descendant of the
    // torn-down scope. `ElementCompleted` touches no incident state, so without
    // this the instance keeps a stale `hasIncident` for a vanished element and a
    // later external resolve could re-drive the dead token.
    //
    //  sub: sub_start -> split =< work(svc) -> work_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("work", "work-job")
        .contained_in("work", "sub")
        .end_event("work_end")
        .contained_in("work_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "work")
        .connect("work", "work_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Fail the work job with no retries left to park an incident in the scope.
    let work_job = engine.activate_jobs("work-job", "W", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::fail_job(work_job, 0, "boom"))
        .unwrap();
    assert_eq!(engine.instance(key).unwrap().incidents.len(), 1);

    // The sibling terminate fires when `trigger` completes.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "scope teardown must resolve the descendant incident"
    );
    assert!(
        engine.instance(key).unwrap().incidents.is_empty(),
        "the instance must carry no active incident after terminate"
    );
}

#[test]
fn scope_teardown_does_not_resurrect_a_cancelled_incident_job() {
    // Regression: forced scope teardown emits `JobCanceled` and then
    // `IncidentResolved` for the *same* failed job in one batch. `IncidentResolved`
    // must not return the just-cancelled job to `Created`, or the resolution
    // resurrects a job whose element instance is being removed — leaving an
    // activatable job pointing at a dead token. The job must stay `Canceled` and
    // out of the activatable pool.
    //
    //  sub: sub_start -> split =< work(svc) -> work_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("work", "work-job")
        .contained_in("work", "sub")
        .end_event("work_end")
        .contained_in("work_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "work")
        .connect("work", "work_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();

    // Fail the work job with no retries left to park an incident on its element.
    let work_job = engine.activate_jobs("work-job", "W", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::fail_job(work_job, 0, "boom"))
        .unwrap();
    assert_eq!(
        engine.state().jobs.get(&work_job).unwrap().state,
        state::JobState::Failed
    );

    // The sibling terminate fires when `trigger` completes: it cancels the failed
    // job *and* resolves its incident in the same teardown batch.
    complete_one(&mut engine, "trigger-job");

    let job = engine.state().jobs.get(&work_job).unwrap();
    assert_eq!(
        job.state,
        state::JobState::Canceled,
        "the cancelled job must stay Canceled — incident resolution must not resurrect it"
    );
    assert!(
        engine
            .activate_jobs("work-job", "W2", 10, 60_000, 0)
            .is_empty(),
        "a resurrected job would reappear in the activatable pool against a dead token"
    );
}

#[test]
fn top_level_terminate_end_resolves_a_root_scope_incident_and_completes() {
    // #1085: a top-level terminate end completes the instance (COMPLETED, not
    // TERMINATED). Its `ProcessInstanceCompleted` reducer deliberately does NOT
    // close incidents (a normal completion may retain one), so the terminate end
    // must resolve any incident open on the instance itself — otherwise the
    // dead, completed instance keeps a stale `hasIncident`. The cancelled job's
    // incident resolution must also not resurrect the job.
    //
    //  start -> split =< work(svc, failed w/ incident), trigger(svc) -> stop(terminate) >
    let proc = ProcessBuilder::new("term-incident")
        .start_event("start")
        .parallel_gateway("split")
        .service_task("work", "work-job")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "work")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("term-incident"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Fail the work job with no retries to park an incident on its element.
    let work_job = engine.activate_jobs("work-job", "W", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::fail_job(work_job, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);

    // The sibling terminate fires when `trigger` completes: it cancels the failed
    // job, resolves its incident, and completes the whole instance.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(events.contains(&Event::JobCanceled {
        job_key: work_job,
        instance_key: key,
    }));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::IncidentResolved { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));

    // The instance is COMPLETED with no lingering active incident, and the
    // cancelled job was not resurrected by the incident resolution.
    assert!(engine.is_completed(key));
    assert!(engine.active_incidents().is_empty());
    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert_eq!(
        engine.state().jobs.get(&work_job).unwrap().state,
        state::JobState::Canceled
    );
    assert!(engine
        .activate_jobs("work-job", "W2", 10, 60_000, 0)
        .is_empty());
}

#[test]
fn should_raise_an_expression_incident_when_a_condition_cannot_evaluate() {
    // The condition compares a string variable to a number — a FEEL type
    // error — so the gateway raises an ExpressionEvaluation incident rather
    // than silently treating the flow as not taken.
    let def = ProcessBuilder::new("typed")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "name > 10")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([("name".to_string(), Value::Str("ann".into()))]);
    let created = engine
        .apply_command(Command::create_instance_with("typed", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
    assert!(!engine.is_completed(instance_key));
}

// zeebe-cells: incident:CONDITION_ERROR
#[test]
fn should_raise_incident_when_no_exclusive_flow_matches() {
    // Both flows are conditional; neither matches -> incident, token parked.
    let def = ProcessBuilder::new("strict")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .end_event("no_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "d = true")
        .connect_when("g", "no_end", "d = false")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("d".to_string(), Value::Int(7))]);
    let events = engine
        .apply_command(Command::create_instance_with("strict", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(!engine.is_completed(instance_key));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::IncidentRaised { .. })));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
}

// zeebe-cells: incident:JOB_NO_RETRIES
#[test]
fn should_raise_an_incident_when_a_job_fails_with_no_retries_left() {
    // given an activated job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when the worker fails it with no retries left
    let events = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();

    // then an incident is raised, the job parks, and it is not activatable
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { reason, .. } if reason == "boom"
    )));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    assert!(engine.pending_jobs().is_empty());
    assert!(engine
        .activate_jobs("payment", "B", 10, 60_000, 100)
        .is_empty());

    // and the parked job can no longer be completed
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });
}

#[test]
fn should_recover_a_parked_job_by_updating_retries_and_resolving_its_incident() {
    // given a job parked on a no-retries incident
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
    let raised = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();

    // when resolving before retries are restored, it is rejected
    let err = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::IncidentNotResolvable { incident_key: k, .. } if k == incident_key
    ));

    // when retries are updated and the incident resolved
    engine
        .apply_command(Command::update_job_retries(job_key, 2))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // then the incident is retained as resolved and the job is activatable again
    assert!(resolved
        .iter()
        .any(|e| matches!(e, Event::IncidentResolved { .. })));
    assert_eq!(
        engine.incident(incident_key).unwrap().state,
        state::IncidentState::Resolved
    );
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

    // and a worker can pick it up and drive the instance to completion
    let job_key2 = engine.activate_jobs("payment", "B", 1, 60_000, 100)[0].key;
    assert_eq!(job_key2, job_key);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_reject_resolving_an_unknown_incident() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::resolve_incident(999))
        .unwrap_err();
    assert_eq!(err, EngineError::IncidentNotFound { incident_key: 999 });
}

#[test]
fn should_re_raise_a_gateway_incident_when_resolution_still_finds_no_flow() {
    // A non-job incident (no matching exclusive flow) is resolved by
    // re-evaluating the gateway. With the variables unchanged it still
    // matches nothing, so resolution retries the work and a *fresh* incident
    // is raised — the token stays parked rather than silently vanishing.
    let def = ProcessBuilder::new("strict")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "d = true")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("d".to_string(), Value::Bool(false))]);
    let created = engine
        .apply_command(Command::create_instance_with("strict", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let original = engine.incidents()[0].key;
    assert!(engine.incident(original).unwrap().job_key.is_none());

    // when resolved (the gateway is re-evaluated)
    engine
        .apply_command(Command::resolve_incident(original))
        .unwrap();

    // then the original incident is retained as resolved and a new active one
    // replaces it, and the instance has still not completed.
    assert_eq!(
        engine.incident(original).unwrap().state,
        state::IncidentState::Resolved
    );
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_ne!(active[0].key, original);
    assert_eq!(active[0].kind, state::IncidentKind::NoMatchingSequenceFlow);
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
    assert!(!engine.is_completed(instance_key));
}

#[test]
fn should_recover_a_gateway_incident_after_fixing_variables() {
    // given an exclusive gateway parked on a no-matching-flow incident
    let def = ProcessBuilder::new("strict")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes_end")
        .connect("s", "g")
        .connect_when("g", "yes_end", "d = true")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("d".to_string(), Value::Bool(false))]);
    let created = engine
        .apply_command(Command::create_instance_with("strict", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let incident_key = engine.incidents()[0].key;

    // when the operator fixes the variable then resolves the incident
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("d".to_string(), Value::Bool(true))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // then the gateway re-evaluates, matches, and the instance completes; the
    // incident is retained as resolved
    assert_eq!(
        engine.incident(incident_key).unwrap().state,
        state::IncidentState::Resolved
    );
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_retry_the_service_task_when_an_unhandled_error_incident_is_resolved() {
    // given a service task whose worker threw an uncaught business error,
    // parking the token on an unhandled-error incident
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
    let raised = engine
        .apply_command(Command::throw_job_error(job_key, "BOOM", "no boundary"))
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();
    assert!(engine.pending_jobs().is_empty());

    // when the incident is resolved
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // then a fresh job is created for the still-active service task, and a
    // worker can activate and complete it to drive the instance home.
    assert!(resolved
        .iter()
        .any(|e| matches!(e, Event::JobCreated { .. })));
    assert_eq!(
        engine.incident(incident_key).unwrap().state,
        state::IncidentState::Resolved
    );
    assert_eq!(engine.pending_jobs().len(), 1);
    let retry = engine.activate_jobs("payment", "B", 1, 60_000, 100);
    assert_eq!(retry.len(), 1);
    assert_ne!(retry[0].key, job_key);
    engine
        .apply_command(Command::complete_job(retry[0].key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_stamp_an_incident_with_the_command_clock() {
    // given a parked job-incident raised at a known instant
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when failed with no retries at now = 1_700_000_000_000
    let raised = engine
        .apply_command_at(Command::fail_job(job_key, 0, "boom"), 1_700_000_000_000)
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();

    // then the incident records that instant
    assert_eq!(
        engine.incident(incident_key).unwrap().created_at,
        1_700_000_000_000
    );
}

#[test]
fn should_retain_a_resolved_incident_as_an_audit_record() {
    // given a parked job-incident
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;
    let raised = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();
    let incident_key = raised
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();
    engine
        .apply_command(Command::update_job_retries(job_key, 2))
        .unwrap();

    // when resolved with an operation reference at a known instant
    engine
        .apply_command_at(
            Command::resolve_incident_with(incident_key, 4242),
            1_700_000_000_500,
        )
        .unwrap();

    // then the record is retained as resolved with audit metadata
    let incident = engine.incident(incident_key).unwrap();
    assert_eq!(incident.state, state::IncidentState::Resolved);
    assert_eq!(incident.resolved_at, Some(1_700_000_000_500));
    assert_eq!(incident.operation_reference, Some(4242));
    // and it no longer counts as active, so the instance has no open incident
    assert!(engine.active_incidents().is_empty());
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

    // and resolving it again is rejected (already resolved)
    let err = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::IncidentNotResolvable { incident_key: k, .. } if k == incident_key
    ));
}

// zeebe-cells: incident:UNHANDLED_ERROR_EVENT
#[test]
fn should_raise_an_incident_when_a_thrown_error_is_unhandled() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("payment"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when the worker throws an error no boundary catches
    let events = engine
        .apply_command(Command::throw_job_error(job_key, "UNKNOWN", "boom"))
        .unwrap();

    // then an incident is raised and the instance does not complete
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { reason, .. } if reason.contains("UNKNOWN")
    )));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
}

#[test]
fn should_raise_an_incident_when_a_subprocess_error_is_unhandled() {
    // A sub-process with no error boundary: an error thrown inside is
    // unhandled and parks on an incident (the instance does not complete).
    let def = ProcessBuilder::new("sub-plain")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-plain"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;

    let events = engine
        .apply_command(Command::throw_job_error(job_key, "BOOM", "kaboom"))
        .unwrap();

    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { reason, .. } if reason.contains("BOOM")
    )));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);
}

#[test]
fn cancel_closes_an_active_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // Drive the job to a no-retries incident.
    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 0)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command(Command::fail_job(job.key, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);

    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    // The incident is closed and the parked job is cancelled.
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        engine.job(job.key).unwrap().state,
        state::JobState::Canceled
    );
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
}

#[test]
fn a_failing_called_element_expression_raises_an_expression_evaluation_incident() {
    // When `zeebe:calledElement` is a FEEL expression (leading `=`) that cannot
    // be evaluated (missing var, parse error, non-string result), the incident
    // must describe the *expression* failure — not masquerade as an "unknown
    // called process '=…'" lookup miss, which points diagnosis at the wrong
    // thing. Here `=calleeName` references an unbound variable, so evaluation
    // fails and no callee id is ever resolved.
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "=calleeName")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(phase_process("phase", "work")))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();

    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the failed expression parks one incident");
    assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
    let reason = &active[0].reason;
    assert!(
        reason.contains("calledElement") && reason.contains("=calleeName"),
        "incident should name the failing calledElement expression, got: {reason}"
    );
    assert!(
        !reason.contains("unknown called process"),
        "expression failure must not be reported as an unknown-process lookup, got: {reason}"
    );
    // No child instance was spawned and the parent did not complete.
    assert!(!engine.is_completed(parent_key));
    assert!(engine
        .state()
        .instances
        .values()
        .all(|i| i.parent_process_instance_key != Some(parent_key)));
}

// zeebe-cells: incident:CALLED_ELEMENT_ERROR
#[test]
fn an_unknown_called_process_raises_a_called_element_incident_not_expression_eval() {
    // A call activity whose (literal) `calledElement` process id is not deployed
    // is a missing-definition / execution problem, NOT a FEEL/type failure. It
    // must be classified as `CalledElementError` (C8 `CALLED_ELEMENT_ERROR`), so
    // clients filtering incidents by `errorType` can distinguish a missing callee
    // from a genuine expression-evaluation failure (`EXTRACT_VALUE_ERROR`).
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "definitely-not-deployed")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();

    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the unknown callee parks one incident");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::CalledElementError,
        "an unknown called process must be a CalledElementError, not ExpressionEvaluation"
    );
    assert!(
        active[0].reason.contains("unknown called process"),
        "incident should name the missing callee, got: {}",
        active[0].reason
    );
    assert!(!engine.is_completed(parent_key));
}

#[test]
fn input_mapping_eval_failure_raises_io_mapping_incident_and_no_job() {
    // #939: an input `zeebe:ioMapping` whose SOURCE fails to evaluate (here
    // `=x + 1` with `x` bound to a string — a FEEL type error, not a bare
    // missing reference) must raise an `IO_MAPPING_ERROR` incident and PARK the
    // element ACTIVATED — no job, no silent proceed with the target unset.
    // Before the fix the failure was swallowed (`continue`) and the token sailed
    // on. Fixing the variable and resolving the incident re-drives the activation
    // body, which re-applies the now-valid mapping and creates the job.
    let def = ProcessBuilder::new("io-in-fail")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=assert(x, x instance of number) + 1".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "io-in-fail",
            vars(&[("x", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The input mapping failed: exactly one active IoMapping incident, no job,
    // and the token is parked (the service task never enacted its behaviour).
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(engine.state().jobs.len(), 0, "no job while parked");
    assert!(!engine.is_completed(key));
    let incident_key = engine.incidents()[0].key;

    // Fix `x` to a number and resolve: the activation body re-runs, the mapping
    // now evaluates (`y = 42`), and the job is created with the mapped variable.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("x".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert_eq!(engine.state().jobs.len(), 1, "job created on resolution");
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("y"), Some(&Value::Int(42)));
}

#[test]
fn output_mapping_eval_failure_raises_incident_and_does_not_complete() {
    // #939: an output `zeebe:ioMapping` whose SOURCE fails to evaluate at
    // completion must raise an incident and hold the element in the COMPLETING
    // phase rather than completing it with the target silently unset. Resolution
    // re-drives `Complete`, re-evaluating the mapping against the (now-fixed)
    // variables without re-running the job.
    let def = ProcessBuilder::new("io-out-fail")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=assert(bad, bad instance of number) + 1".to_string(),
                    target: "approved".to_string(),
                }],
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "io-out-fail",
            vars(&[("bad", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // The output mapping failed: one active `IoMapping` incident (output-phase,
    // distinguished by `redrive: Completion`; REST `IO_MAPPING_ERROR` — matching
    // Zeebe, which raises `IO_MAPPING_ERROR` for both input and output mapping
    // failures), and the element has NOT completed.
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        !engine.is_completed(key),
        "must not complete on eval failure"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `bad` and resolve: `Complete` re-drives, the mapping evaluates
    // (`approved = 42`), and the instance runs to completion.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("bad".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert!(engine.is_completed(key), "completes after resolution");
    assert_eq!(
        merged_var(&events, "approved"),
        Some(Value::Int(42)),
        "output mapping should surface approved=42; events: {events:?}"
    );
}

// zeebe-cells: incident:IO_MAPPING_ERROR
#[test]
fn message_catch_output_mapping_incident_resolves_via_complete_not_reopen() {
    // Regression: an OUTPUT `zeebe:ioMapping` failure on a message intermediate
    // catch event parks the token in the COMPLETING phase *after* the message
    // has already been correlated and consumed. Resolving the incident must
    // re-drive `Complete` (re-projecting the now-fixed output mapping for the
    // same token), NOT `ReopenCatch` — reopening the subscription would strand
    // the token waiting for a *second* message that will never arrive.
    // `ReopenCatch` is reserved for correlation-key (ACTIVATING) failures, which
    // surface as `ExpressionEvaluation`, never as an output-phase `IoMapping`.
    let def = ProcessBuilder::new("msg-out-fail")
        .start_event("s")
        .message_intermediate_catch_event("await", "approve", "orderId")
        .with_io(
            "await",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=assert(bad, bad instance of number) + 1".to_string(),
                    target: "approved".to_string(),
                }],
            },
        )
        .end_event("e")
        .connect("s", "await")
        .connect("await", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = engine
        .apply_command(Command::create_instance_with(
            "msg-out-fail",
            vars(&[
                ("orderId", Value::Str("A".into())),
                ("bad", Value::Str("oops".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Parked on one open message subscription.
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);

    // Correlating drives completion, which applies the output mapping
    // `=bad + 1` (string + int) and fails: one output-phase `IoMapping` incident,
    // the token held in COMPLETING, and the subscription already consumed.
    engine.correlate_message("approve", "A", HashMap::new(), 0);
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        !engine.is_completed(key),
        "must not complete on eval failure"
    );
    assert!(
        engine
            .message_subscriptions()
            .iter()
            .all(|s| s.state != state::MessageSubscriptionState::Open),
        "the correlated subscription is consumed, not left open"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `bad` and resolve. The regression: resolution must re-drive `Complete`
    // (the token runs to completion), not `ReopenCatch` (which would open a
    // fresh subscription and never complete for lack of a second message).
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("bad".to_string(), Value::Int(41))]),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(key),
        "message-catch output-mapping incident must resolve by completing the \
         token, not reopening the catch; events: {events:?}"
    );
    assert!(
        engine
            .message_subscriptions()
            .iter()
            .all(|s| s.state != state::MessageSubscriptionState::Open),
        "resolution must not reopen the message subscription"
    );
    assert_eq!(
        merged_var(&events, "approved"),
        Some(Value::Int(42)),
        "output mapping should surface approved=42; events: {events:?}"
    );
}

#[test]
fn subprocess_output_mapping_eval_failure_raises_io_mapping_incident() {
    // #939 parity (ports Zeebe `OutputMappingIncidentTest` to a scoped element):
    // an OUTPUT `zeebe:ioMapping` failure on a *sub-process* (not the mainstream
    // service-task path) must raise the `IoMapping` incident kind (REST
    // `IO_MAPPING_ERROR` — the same taxonomy Zeebe raises for both input and
    // output mapping failures) and hold the sub-process in COMPLETING rather than
    // completing it with the target silently unset. This locks the taxonomy
    // relabel across every output path, not just the mainstream element.
    //
    // Recovery is phase-driven (#946): the incident's `Completion` re-drive
    // re-projects the now-fixed output mapping via `Complete` — the same
    // lifecycle as the mainstream output path — without re-running the inner job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(vec![
            crate::model::Mapping {
                source: "=assert(bad, bad instance of number) + 1".to_string(),
                target: "exported".to_string(),
            },
        ])))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "sub-scope",
            vars(&[("seed", Value::Int(4)), ("bad", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // Completing the inner job drives the sub-process into its output-mapping
    // projection, which fails on `=bad + 1` (string + int).
    let _ = complete_one(&mut engine, "work");

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::IoMapping,
        "a sub-process output-mapping failure must raise IO_MAPPING_ERROR, not EXTRACT_VALUE_ERROR",
    );
    assert_eq!(
        active[0].element_id, "sub",
        "the incident parks on the sub-process, not the inner task"
    );
    assert_eq!(
        active[0].redrive,
        Some(state::IoMappingRedrive::Completion),
        "a sub-process output failure re-drives the completion phase",
    );
    assert!(
        !engine.is_completed(inst),
        "the process must not complete while the output mapping is unresolved"
    );

    // Fix `bad` in the sub-process's own scope and resolve. A sub-process
    // projects its output mapping in the drain sweep
    // (`complete_drained_subprocesses`), not `complete`, so its `Completion`
    // re-drive is owned by that sweep rather than a `Complete` step: resolution
    // clears the incident (it does NOT re-drive `Complete`, which would
    // re-evaluate against the already-torn-down inner scope and re-raise a bogus
    // incident) and the instance runs to completion.
    let sub_scope = active[0].element_instance_key;
    let incident_key = active[0].key;
    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("bad".to_string(), Value::Int(41))]),
            true,
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(inst),
        "resolving the sub-process output incident clears the block and the \
         instance completes without a spurious re-raised incident"
    );
}

#[test]
fn multi_instance_body_input_mapping_genuine_failure_raises_incident_and_recovers() {
    // #946 item 2a: a MI *body* input mapping that fails for a reason NOT tied to
    // a per-child binding (`loopCounter` / `inputElement`) is a GENUINE failure —
    // it must raise `IO_MAPPING_ERROR` and spawn no children, rather than
    // `.unwrap_or_default()`-swallowing the error into an empty collection.
    // Resolution re-drives the body activation (`MiBodyActivation`).
    let def = ProcessBuilder::new("mi-body-in")
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
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                // Does NOT reference a per-child binding ⇒ evaluated at the body
                // level; `badFactor` is a string, so it fails genuinely.
                inputs: vec![crate::model::Mapping {
                    source: "=assert(badFactor, badFactor instance of number) + 1".to_string(),
                    target: "scaled".to_string(),
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
            "mi-body-in",
            vars(&[
                ("items", Value::List(vec![Value::Int(10), Value::Int(20)])),
                ("badFactor", Value::Str("oops".into())),
            ]),
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
        Some(state::IoMappingRedrive::MiBodyActivation),
        "a genuine MI body input failure re-drives the body activation",
    );
    assert_eq!(active[0].element_id, "each");
    assert_eq!(
        engine.state().jobs.len(),
        0,
        "no children fanned out on a genuine body input failure"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `badFactor` and resolve: the body fans out its two children (jobs).
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("badFactor".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    let jobs = engine.activate_jobs("handle", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        2,
        "both children fan out once the body re-drives"
    );
}

#[test]
fn multi_instance_body_output_mapping_genuine_failure_raises_incident_and_recovers() {
    // #946 item 2b: a MI *body* output mapping that fails at BODY completion —
    // reached when the loop has no children to evaluate it per-child (an EMPTY
    // input collection: the body drains straight to completion) — is a GENUINE
    // failure. It must raise `IO_MAPPING_ERROR` and hold the body in COMPLETING,
    // rather than `.unwrap_or_default()`-completing with the output silently
    // unset. Resolution re-drives the body completion (`MiBodyCompletion`).
    //
    // (A non-empty loop evaluates the activity's output mapping per-child in
    // `complete_mi_child` — Zeebe applies `zeebe:output` in each child's scope —
    // so a broken source there raises the per-child `Completion` re-drive first;
    // the body-level `MiBodyCompletion` path is the zero-child aggregation case.)
    let def = ProcessBuilder::new("mi-body-out")
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
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: Vec::new(),
                // Evaluated at body completion; `badOut` is a string ⇒ fails.
                outputs: vec![crate::model::Mapping {
                    source: "=assert(badOut, badOut instance of number) + 1".to_string(),
                    target: "summary".to_string(),
                }],
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    // An EMPTY input collection: the body drains straight to its output
    // aggregation with no per-child evaluation in between.
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi-body-out",
            vars(&[
                ("items", Value::List(Vec::new())),
                ("badOut", Value::Str("oops".into())),
            ]),
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
        Some(state::IoMappingRedrive::MiBodyCompletion),
        "a genuine MI body output failure re-drives the body completion",
    );
    assert_eq!(active[0].element_id, "each");
    assert!(!engine.is_completed(inst), "body held in COMPLETING");
    let incident_key = engine.incidents()[0].key;

    // Fix `badOut` and resolve: the body completion re-drives (`summary = 6`) and
    // the process finishes.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("badOut".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    assert!(
        engine.is_completed(inst),
        "the body completes once its output mapping re-drives"
    );
    assert_eq!(
        merged_var(&events, "summary"),
        Some(Value::Int(6)),
        "the re-driven body output mapping projects summary=6; events: {events:?}"
    );
}

#[test]
fn bare_missing_var_input_mapping_assigns_null_without_incident() {
    // Scope guard for #939: a bare reference to a MISSING variable (`=missing`)
    // is FEEL `null` (an `Ok`), NOT an evaluation failure — it must still assign
    // the target as `null` and raise NO incident. Only a source that genuinely
    // errors (parse/type error, operation on a missing value) halts the element.
    let def = ProcessBuilder::new("io-in-null")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=missing".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let _ = create_instance_key(&mut engine, "io-in-null");
    assert!(
        engine.active_incidents().is_empty(),
        "a bare missing-var input (FEEL null) must not raise an incident"
    );
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(
        job.variables.get("y"),
        Some(&Value::Null),
        "the target is assigned null, not dropped"
    );
}

// zeebe-cells: incident:CALLED_DECISION_ERROR
#[test]
fn business_rule_task_unknown_decision_raises_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "missing",
            Some("out".to_string()),
        )))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with("brt", HashMap::new()))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();

    // An incident was raised and the instance is still active (parked).
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised {
            kind: crate::state::IncidentKind::DecisionEvaluation,
            ..
        }
    )));
    assert_eq!(
        engine.instance(inst).map(|i| i.state),
        Some(ProcessInstanceState::Active)
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_output_collection_non_array_target_raises_extract_value_incident() {
    // Zeebe (AdHocSubProcessOutputCollectionBehavior.appendToOutputCollection)
    // raises EXTRACT_VALUE_ERROR when the target is not an array. nano used to
    // silently accumulate into its hidden Vec and overwrite the scalar at
    // completion. The type guard must reject it instead — and for full parity it
    // DEFERS the tool's completion: the incident sits on the TOOL child (so
    // resolving it retries the append), the tool stays active with its scope
    // intact, and the scalar target is left uncorrupted.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_non_array_output_collection_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    // The container input mapping overwrote the seeded array with a scalar.
    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::Int(5)),
        "input mapping set results to a non-array before any tool ran",
    );

    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool = engine
        .activate_jobs("tool", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("tool job");
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("x".to_string()));
    engine
        .apply_command(Command::complete_job_with(tool.key, vars))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "exactly one incident raised");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::ExpressionEvaluation,
        "non-array outputCollection maps to the EXTRACT_VALUE_ERROR taxonomy",
    );
    assert_eq!(
        active[0].element_id, "toolA",
        "incident sits on the tool child, so resolving it re-drives the append",
    );
    assert_eq!(
        active[0].element_instance_key, tool.element_instance_key,
        "incident is parked on the tool's element instance, not the container",
    );
    // The scalar is left untouched — no silent corruption into a list.
    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::Int(5)),
        "the non-array target is not overwritten by a phantom collection",
    );
    assert!(
        !engine.is_completed(inst),
        "container parks on the incident"
    );
    // The tool's completion is DEFERRED (no `AdHocToolCompleted` emitted): it
    // stays in the container's `active` set with its local scope intact so that
    // resolving the incident retries the append against the corrected target —
    // nothing is discarded.
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert!(
        adhoc.active.contains(&tool.element_instance_key),
        "the tool stays active while parked on the incident",
    );
}

#[test]
fn adhoc_output_collection_incident_resolution_retries_the_append() {
    // Full Zeebe parity for the EXTRACT_VALUE_ERROR incident: it is recoverable.
    // After correcting the `outputCollection` target to an array and resolving
    // the incident, the deferred tool completion is re-driven — its output is
    // appended and the tool drains — exactly like Zeebe's retry-on-resolve.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_non_array_output_collection_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool = engine
        .activate_jobs("tool", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("tool job");
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("x".to_string()));
    engine
        .apply_command(Command::complete_job_with(tool.key, vars))
        .unwrap();

    let incident = engine.active_incidents()[0].clone();
    assert_eq!(incident.element_id, "toolA", "parked on the tool child");

    // Correct the target: overwrite the scalar with an empty array in the
    // container's local scope (as an operator would via SetVariables).
    engine
        .apply_command(Command::set_variables_scoped(
            container,
            HashMap::from([("results".to_string(), Value::List(Vec::new()))]),
            true,
        ))
        .unwrap();
    // Resolve the incident → the deferred tool completion is re-driven.
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "the incident is cleared once the append succeeds",
    );
    // The tool's output is now appended to the corrected array.
    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::List(vec![Value::Str("x".to_string())])),
        "resolving the incident retries the append against the fixed target",
    );
    // The tool has drained from the active set (it completed on retry).
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert!(
        !adhoc.active.contains(&tool.element_instance_key),
        "the tool drains once its deferred completion is re-driven",
    );
}

#[test]
fn errored_correlation_key_raises_incident_and_reopens_on_resolve() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_concat_correlation_key()))
        .unwrap();
    // `task` is absent at open time, so `task.id` is null and the concat errors.
    let created = engine
        .apply_command(Command::create_instance_with(
            "concat-corr",
            vars(&[("planKey", Value::Str("plan#1".into()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // No unmatchable subscription is opened; instead an incident is raised.
    assert!(
        engine.message_subscriptions().is_empty(),
        "an unevaluable correlation key must not open a subscription"
    );
    let incident_key = created
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised {
                incident_key, kind, ..
            } if *kind == state::IncidentKind::ExpressionEvaluation => Some(*incident_key),
            _ => None,
        })
        .expect("an ExpressionEvaluation incident is raised");
    assert!(
        !engine.is_completed(instance_key),
        "the token must still be parked"
    );
    assert_eq!(engine.instance(instance_key).unwrap().incidents.len(), 1);

    // Correct the variable so the key can evaluate, then resolve the incident.
    let mut task_map = std::collections::BTreeMap::new();
    task_map.insert("id".to_string(), Value::Str("w1".into()));
    engine
        .apply_command(Command::set_variables(
            instance_key,
            vars(&[("task", Value::Map(task_map))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    // Resolving re-opens the subscription with the now-correct key (it does NOT
    // complete the catch and skip the wait).
    assert!(
        !engine.is_completed(instance_key),
        "the catch must keep waiting"
    );
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1, "the subscription is re-opened on resolve");
    assert_eq!(subs[0].correlation_key, "plan#1:w1");
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());

    // A matching message now correlates and drives the instance to completion.
    engine
        .apply_command(Command::correlate_message("answered", "plan#1:w1"))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

/// Issue #1159 (Copilot review round 4): the new `callActivity` spawn arm must
/// only fire for a BOUND callee. An UNBOUND call-activity tool (no
/// `calledElement`) must pass through to completion — the previous generic arm
/// did — instead of spawning with an empty callee and raising a spurious
/// `CalledElementError`. Activating it raises NO incident, completes the tool
/// (its output mapping projects into the container), and re-emits the agent job.
#[test]
fn adhoc_unbound_call_activity_tool_passes_through_without_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_unbound_call_activity_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "parent");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("Unbound")],
                ..Default::default()
            },
        ))
        .unwrap();

    // The unbound tool spawns NO child process and raises NO incident — it passes
    // straight through to completion.
    assert!(
        engine
            .activate_jobs("probe-child", "W", 10, 1_000, 0)
            .is_empty(),
        "an unbound call-activity tool spawns no child process"
    );
    assert!(
        engine.instance(inst).unwrap().incidents.is_empty(),
        "an unbound call-activity tool passes through — no spurious CalledElementError"
    );

    // It completed like a pass-through tool: its output mapping projected `42`
    // into the container scope, visible to the next agent turn. (`outputElement`
    // reads the child scope, where the tool output target is not set, so the
    // collected entry is null — the projection lands in the container, not the
    // child.)
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted after the unbound tool drained");
    assert_eq!(
        agent2.variables.get("toolResult"),
        Some(&Value::Int(42)),
        "the unbound tool completed and projected its pass-through output into \
         the container, got {:?}",
        agent2.variables.get("toolResult")
    );
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "the parent instance completed");
}

#[test]
fn adhoc_call_activity_tool_spawn_incident_recovers_on_resolve() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_tool_calls_undeployed()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("askedAbout", Value::Str("a mortgage".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();

    // The callee is not deployed: the spawn parks a recoverable incident and no
    // child job is minted.
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the unknown callee parks one incident");
    assert_eq!(active[0].kind, state::IncidentKind::CalledElementError);
    match &active[0].redrive {
        Some(state::IoMappingRedrive::AdHocCallActivitySpawn { child_seed }) => {
            // #1176: an ad-hoc call-activity tool preserves its single-pass input
            // projection on the spawn redrive so the respawn reuses it verbatim.
            assert_eq!(
                child_seed.get("customerRequest"),
                Some(&Value::Str("a mortgage".into())),
                "the spawn incident carries the tool's preserved input projection",
            );
        }
        other => panic!("expected AdHocCallActivitySpawn redrive, got {other:?}"),
    }
    assert!(
        engine
            .activate_jobs("probe-child", "W", 10, 1_000, 0)
            .is_empty(),
        "no child is instantiated while the callee is missing"
    );

    // Deploy the callee and resolve: resolution re-attempts the spawn.
    engine
        .apply_command(Command::DeployProcess(specialist_proc()))
        .unwrap();
    let incident_key = engine.incidents()[0].key;
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "resolving the incident cleared it by spawning the child"
    );
    let child_jobs = engine.activate_jobs("probe-child", "W", 10, 1_000, 0);
    assert_eq!(
        child_jobs.len(),
        1,
        "resolving the spawn incident re-attempted the spawn and instantiated the \
         callee — it did not silently complete the tool"
    );
    assert_eq!(
        child_jobs[0].variables.get("customerRequest"),
        Some(&Value::Str("a mortgage".into())),
        "the retried spawn re-applied the tool's input mapping"
    );
    assert!(!engine.is_completed(inst), "the parent waits for the child");
}

/// Zeebe parity for the #1176 spawn-incident redrive under
/// `propagateAllParentVariables="true"`: when the incident resolves, Zeebe's
/// `CallActivityProcessor.finalizeActivation` re-reads the call activity's
/// CURRENT scope via `copyAllVariablesToProcessInstance`, so a container variable
/// that became visible *after* the incident (here an operator `SetVariables`
/// write, but equally another tool propagating its output) must cross into the
/// child on redrive. The redrive must NOT freeze the first-pass view — it overlays
/// only the preserved single-pass input projection onto a fresh all-parent view.
#[test]
fn adhoc_call_activity_tool_propagate_all_redrive_sees_post_incident_container_var() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_tool_calls_undeployed_propagate_all(),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("askedAbout", Value::Str("a mortgage".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();

    // First spawn parks a recoverable incident: the callee is undeployed.
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the unknown callee parks one incident");
    assert_eq!(active[0].kind, state::IncidentKind::CalledElementError);

    // A container variable becomes visible only AFTER the incident is parked —
    // e.g. an operator sets the variable that unblocks the redrive, or another
    // ad-hoc tool propagates its output into the container.
    engine
        .apply_command(Command::SetVariables {
            scope_key: inst,
            variables: vars(&[("lateVar", Value::Str("added after incident".into()))]),
            local: false,
        })
        .unwrap();

    // Deploy the callee and resolve: the redrive re-attempts the spawn.
    engine
        .apply_command(Command::DeployProcess(specialist_proc()))
        .unwrap();
    let incident_key = engine.incidents()[0].key;
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "resolving the incident cleared it by spawning the child"
    );

    let child = engine
        .activate_jobs("probe-child", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("child spawned on redrive")
        .variables
        .as_ref()
        .clone();

    // The preserved single-pass input projection still crosses (child-local wins).
    assert_eq!(
        child.get("customerRequest"),
        Some(&Value::Str("a mortgage".into())),
        "the redrive re-applied the preserved input projection",
    );
    // propagateAllParentVariables=true → the original parent variable crosses.
    assert_eq!(
        child.get("askedAbout"),
        Some(&Value::Str("a mortgage".into())),
        "propagateAllParentVariables=true crosses the original parent variable",
    );
    // The crux (#1176 propagate=true gap): the variable that became visible AFTER
    // the incident must ALSO cross, because the redrive seeds from a fresh
    // all-parent view — not a frozen first-pass snapshot. Before the fix this was
    // absent (the seed was the frozen first-pass child_vars).
    assert_eq!(
        child.get("lateVar"),
        Some(&Value::Str("added after incident".into())),
        "propagateAllParentVariables=true redrive must see the container variable \
         that became visible after the incident (Zeebe re-reads current scope; the \
         seed is not frozen at first-pass time)",
    );
    assert!(!engine.is_completed(inst), "the parent waits for the child");
}

#[test]
fn adhoc_call_activity_tool_preserves_chained_output_projection_across_type_incident() {
    // Clean completion projects the chain single-pass: `intermediate = <status>`,
    // and `toolCallResult = <intermediate>` reads the child's ORIGINAL variables
    // (no `intermediate`), so it is null.
    let (clean_intermediate, clean_result) = chained_output_container_projection(false);
    assert_eq!(
        clean_intermediate,
        Some(Value::Str("ok".into())),
        "clean completion: intermediate = <status>",
    );
    assert_eq!(
        clean_result,
        Some(Value::Null),
        "clean completion: toolCallResult reads the original child view, so it is \
         exactly null (asserted explicitly so a stray non-null value can't become \
         a non-diagnostic baseline for the redrive comparison below)",
    );

    // The redrive after the output-collection type incident must reproduce the
    // SAME projection — not re-evaluate the chained output mappings against the
    // seeded child scope (which now carries `intermediate`, making
    // `toolCallResult = <intermediate>` = "ok"). This is the #1176 output-side
    // defect.
    let (retry_intermediate, retry_result) = chained_output_container_projection(true);
    assert_eq!(
        retry_result, clean_result,
        "the redrive's toolCallResult must match the clean completion \
         (single-pass output projection preserved, not re-evaluated against the \
         mutated child scope)",
    );
    assert_eq!(
        retry_intermediate, clean_intermediate,
        "the redrive's intermediate must match the clean completion",
    );
}

#[test]
fn subprocess_input_mapping_failure_reenters_the_body_on_resolve() {
    // A sub-process whose INPUT `zeebe:ioMapping` fails parks ACTIVATED on an
    // `IoMapping` incident with NO inner token yet. Fixing the variable and
    // resolving must re-run the activation (#946 `RetryActivation`): re-apply the
    // inputs and ENTER the body (start event → inner job). It must never be
    // mistaken for a drained sub-process and completed straight past its body —
    // that silently skips the sub-process's work (a guard incident an operator
    // repaired would then advance the flow as if the node had run).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "sub-scope",
            vars(&[("seed", Value::Str("oops".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert_eq!(engine.state().jobs.len(), 0, "no inner job while parked");
    // Parked, not skipped: the instance has not run past the sub-process.
    assert!(
        !engine.is_completed(key),
        "a sub-process parked on an input-mapping incident must not complete"
    );
    let incident_key = engine.incidents()[0].key;

    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("seed".to_string(), Value::Int(4))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(engine.instance(key).unwrap().incidents.is_empty());
    assert!(
        !engine.is_completed(key),
        "the sub-process must not complete past its body on resolve"
    );
    let jobs = engine.activate_jobs("work", "w", 1, 60_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "resolving re-entered the body: the inner job exists"
    );
    assert_eq!(jobs[0].variables.get("scoped"), Some(&Value::Int(5)));
}

// ---------------------------------------------------------------------------
// #1390 — job-type FEEL expression failures raise EXTRACT_VALUE_ERROR
// (Zeebe `JobWorkerElementIncidentTest` parity).
// ---------------------------------------------------------------------------

/// A linear process whose single service task's job type is the given raw
/// `zeebe:taskDefinition type` text (a literal or a `=…` FEEL expression).
fn task_with_job_type_expr(job_type: &str) -> ProcessDefinition {
    ProcessBuilder::new("job-type-incident")
        .start_event("start")
        .service_task("task", job_type)
        .end_event("end")
        .connect("start", "task")
        .connect("task", "end")
        .build()
        .unwrap()
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_evaluation_failed() {
    // Zeebe `JobWorkerElementIncidentTest.shouldCreateIncidentIfJobTypeExpressionEvaluationFailed`:
    // a job-type FEEL expression that does not yield a string (a missing
    // variable without `is defined`, or — here — a type error such as
    // concatenating a string with a number, which feel-scala suppresses to
    // `null`, #1392) raises an EXTRACT_VALUE_ERROR incident on the task
    // and creates NO job. nano used to fall back to the literal expression text
    // as the job type, parking the token with no incident and no subscribable
    // worker (#1390).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr(
            r#"= "senior:feature" + (if is defined(model) and model != null then "@" + model else "")"#,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            vars(&[("model", Value::Int(42))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "exactly one incident raised");
    assert_eq!(
        active[0].kind,
        state::IncidentKind::ExtractValueError,
        "a failed job-type expression maps to the EXTRACT_VALUE_ERROR taxonomy",
    );
    assert_eq!(active[0].element_id, "task");
    assert!(
        active[0].reason.contains("to be 'STRING', but was 'NULL'"),
        "the reason names the failed expression and its null result: {}",
        active[0].reason
    );
    assert_eq!(
        engine.state().jobs.len(),
        0,
        "no job is created — in particular none typed with the raw expression text",
    );
    assert!(!engine.is_completed(key), "the token parks on the incident");
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_of_invalid_type() {
    // Zeebe `…shouldCreateIncidentIfJobTypeExpressionOfInvalidType`: a job-type
    // expression that evaluates to a non-string (a context/map here) raises
    // EXTRACT_VALUE_ERROR and creates no job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr("= {a: 1}")))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            HashMap::new(),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    assert_eq!(active[0].element_id, "task");
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(!engine.is_completed(key));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_is_empty_string() {
    // Zeebe `…shouldCreateIncidentIfJobTypeExpressionIsEmptyString`: a job-type
    // expression that evaluates to the empty string raises EXTRACT_VALUE_ERROR
    // and creates no job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr(r#"= """#)))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            HashMap::new(),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    assert!(
        active[0].reason.contains("empty string"),
        "the reason calls out the empty result: {}",
        active[0].reason
    );
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(!engine.is_completed(key));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_of_non_string_scalar_type() {
    // Zeebe `…shouldCreateIncidentIfJobTypeExpressionOfInvalidType` covers the
    // whole non-string class: a job type must BE a string, not merely be
    // renderable as one. Numbers, booleans, lists and temporal values all raise
    // EXTRACT_VALUE_ERROR and create no job — nano's `eval_string` used to
    // coerce them to their FEEL text (`=42` created a job typed `"42"`).
    for expr in [
        "=42",
        "=1.5",
        "=true",
        "=[1, 2]",
        r#"=@"2024-01-01""#,
        r#"=@"P1D""#,
    ] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(task_with_job_type_expr(expr)))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance_with(
                "job-type-incident",
                HashMap::new(),
            ))
            .unwrap();
        let key = created.iter().find_map(|e| e.instance_key()).unwrap();

        let active = engine.active_incidents();
        assert_eq!(
            active.len(),
            1,
            "expression {expr} raises exactly one incident"
        );
        assert_eq!(
            active[0].kind,
            state::IncidentKind::ExtractValueError,
            "expression {expr} maps to the EXTRACT_VALUE_ERROR taxonomy"
        );
        assert_eq!(active[0].element_id, "task");
        assert!(
            active[0].reason.contains("to be 'STRING'"),
            "the reason calls out the non-string result for {expr}: {}",
            active[0].reason
        );
        assert_eq!(
            engine.state().jobs.len(),
            0,
            "expression {expr} creates no job"
        );
        assert!(
            !engine.is_completed(key),
            "expression {expr} parks the token"
        );
    }
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_create_incident_if_job_type_expression_exceeds_max_length() {
    // Zeebe `BpmnJobBehavior.evalTypeExp` (maxWorkerTypeLength, default 32 * 1024): a
    // job-type expression whose result is longer than the cap raises
    // EXTRACT_VALUE_ERROR and creates no job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr("=jobType")))
        .unwrap();
    let long = "x".repeat(MAX_JOB_TYPE_LENGTH + 1);
    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            vars(&[("jobType", Value::Str(long))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    assert!(
        active[0].reason.contains("maximum length of"),
        "the reason calls out the length cap: {}",
        active[0].reason
    );
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(!engine.is_completed(key));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_resolve_incident_after_job_type_expression_evaluation_failed() {
    // Zeebe `…shouldResolveIncidentAfterJobTypeExpressionEvaluationFailed`: the
    // incident is recoverable — after updating the variables the expression
    // reads, resolving it retries the activation and the job is created with
    // the now-resolved type.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr(
            r#"= "senior:feature" + (if is defined(model) and model != null then "@" + model else "")"#,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            vars(&[("model", Value::Int(42))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let incident_key = engine.incidents()[0].key;

    // Fix the variable so the expression evaluates, then resolve: activation is
    // retried and the job appears under the resolved type.
    engine
        .apply_command(Command::set_variables(
            key,
            HashMap::from([("model".to_string(), Value::Str("kimi-k3".into()))]),
        ))
        .unwrap();
    let resolve_events = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let _ = &resolve_events;
    assert!(
        engine.active_incidents().is_empty(),
        "the incident is resolved",
    );
    let jobs = engine.activate_jobs("senior:feature@kimi-k3", "w", 1, 60_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "resolving retried the activation: the job exists under the resolved type",
    );
    assert_eq!(jobs[0].element_id, "task");
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn should_re_raise_incident_if_job_type_expression_still_fails_after_resolve() {
    // Resolving WITHOUT fixing the variables retries the activation, which
    // fails again and raises a fresh incident (Zeebe: the retried evaluation
    // produces a new incident, it does not silently swallow the failure).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr(
            "=jobType + suffix",
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            vars(&[("jobType", Value::Str("work".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let incident_key = engine.incidents()[0].key;

    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the retried activation fails again: a fresh incident is raised",
    );
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    assert_ne!(
        active[0].key, incident_key,
        "the re-raised incident is a new record",
    );
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(!engine.is_completed(key));
}

#[test]
fn job_type_expression_resolves_against_updated_variables() {
    // The happy path from the #1390 repro: a valid model variable yields the
    // concatenated type, and an absent/null model yields the bare type — both
    // without an incident.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_job_type_expr(
            r#"= "senior:feature" + (if is defined(model) and model != null then "@" + model else "")"#,
        )))
        .unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            vars(&[("model", Value::Str("kimi-k3".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        engine
            .activate_jobs("senior:feature@kimi-k3", "w", 1, 60_000, 0)
            .len(),
        1
    );
    engine
        .apply_command(Command::CancelInstance { instance_key: key })
        .unwrap();

    // Absent `model` → the bare type.
    engine
        .apply_command(Command::create_instance_with(
            "job-type-incident",
            HashMap::new(),
        ))
        .unwrap();
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        engine
            .activate_jobs("senior:feature", "w", 1, 60_000, 0)
            .len(),
        1
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_job_type_failure_parks_container_before_any_child_creation() {
    // #1390 review: a tool whose job-type expression fails must be validated
    // BEFORE any child-creation side effect. The incident parks on the
    // CONTAINER with no inner instance, no tool child and no job; resolving it
    // re-activates the tool with its original seed (`AdHocToolJobType`), a
    // clean retry — never a resolve that clears the incident without
    // recreating the job.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // The agent activates toolA while `toolType` is unbound: the job-type
    // expression fails BEFORE the tool child exists.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "exactly one incident raised");
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    assert_eq!(
        active[0].element_instance_key, container,
        "the incident parks on the CONTAINER — the tool child was never created",
    );
    assert_eq!(active[0].element_id, "agent");
    assert!(
        matches!(
            active[0].redrive,
            Some(state::IoMappingRedrive::AdHocToolJobType { .. })
        ),
        "the redrive re-activates the tool with its preserved seed: {:?}",
        active[0].redrive
    );
    assert_eq!(
        engine.state().jobs.len(),
        1,
        "only the (already-completed) agent job exists — no tool job was minted",
    );
    assert!(
        !engine.is_completed(inst),
        "the container parks on the incident"
    );
    let incident_key = active[0].key;

    // Fix the variable and resolve: the tool is re-activated cleanly and its
    // job appears under the resolved type.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("tool".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.active_incidents().is_empty());
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        tool_jobs.len(),
        1,
        "resolving re-activated the tool: its job exists under the resolved type",
    );
    assert_eq!(tool_jobs[0].element_id, "toolA");
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_job_type_re_raise_after_resolve_keeps_container_parked() {
    // Resolving WITHOUT fixing the variable re-activates the tool, which fails
    // again at the same pre-creation gate: a fresh incident on the container,
    // still no child and no job (Zeebe: the retried evaluation produces a new
    // incident rather than silently swallowing the failure).
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let incident_key = engine.incidents()[0].key;

    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the retried tool activation fails again: a fresh incident is raised",
    );
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    assert_ne!(active[0].key, incident_key, "a new incident record");
    assert_eq!(active[0].element_instance_key, container);
    assert_eq!(
        engine.state().jobs.len(),
        1,
        "only the (already-completed) agent job exists — the retry minted no job",
    );
    assert!(!engine.is_completed(inst));
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_job_type_redrive_reapplies_input_mappings_to_original_seed() {
    // #1390 review: the `AdHocToolJobType` redrive must carry the seed captured
    // BEFORE the tool's input mappings ran; otherwise a self-referential
    // mapping (`counter := counter + 1`) double-applies on resolve.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
                <zeebe:ioMapping>
                  <zeebe:input source="=counter + 1" target="counter" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "p",
            HashMap::from([("counter".to_string(), Value::Int(1))]),
        ))
        .unwrap();
    let inst = engine.state().instances.keys().copied().next().unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let incident_key = engine.active_incidents()[0].key;
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("tool".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].variables.get("counter"),
        Some(&Value::Int(2)),
        "input mapping applied once to the original seed, not to its own output",
    );
}

#[test]
fn nested_declarative_adhoc_with_long_id_is_not_job_type_validated() {
    // #1390 review: a nested DECLARATIVE (BPMN_TASK) container creates no
    // agent job, so its element-id fallback "job type" must not be validated —
    // a valid 129-char id must activate its collection, not raise an incident.
    let long_id = format!("n{}", "x".repeat(128));
    let xml = format!(
        r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
            </bpmn:extensionElements>
            <bpmn:adHocSubProcess id="{long_id}">
              <bpmn:extensionElements>
                <zeebe:adHoc activeElementsCollection="=[&quot;inner&quot;]" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="inner">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="inner-work" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#
    );
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(&xml).unwrap().remove(0),
        ))
        .unwrap();
    create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element(&long_id)],
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "{:?}",
        engine.active_incidents()
    );
    assert_eq!(
        engine.activate_jobs("inner-work", "W", 10, 1_000, 0).len(),
        1,
        "the nested declarative container activated its collection",
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_job_type_redrive_applies_scope_corrections_over_the_frozen_seed() {
    // #1390 review: when the invalid type came from the ACTIVATION SEED (the
    // agent passed `toolType = 42` in the activate-element variables), the
    // frozen redrive seed shadows the operator's `SetVariables` correction on
    // the container scope — the seed was captured before any scope write could
    // exist. The redrive must overlay the incident scope's (later) writes onto
    // the seed so the correction wins; otherwise the retry re-applies `42`
    // forever and the incident can never be cleared.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();

    // The agent activates toolA with a seed whose `toolType` is a NUMBER: the
    // job-type expression fails on the seed value itself.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element_with(
                    "toolA",
                    &[("toolType", Value::Int(42))],
                )],
                ..Default::default()
            },
        ))
        .unwrap();
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the seed's non-string type fails");
    assert_eq!(active[0].kind, state::IncidentKind::ExtractValueError);
    let incident_key = active[0].key;

    // The operator corrects `toolType` on the container scope and resolves:
    // the retry must see the correction, not the frozen seed's `42`.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("tool".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "the corrected scope variable wins over the frozen seed: {:?}",
        engine.active_incidents()
    );
    let jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "the retried tool minted its job under the corrected type",
    );
    assert_eq!(jobs[0].element_id, "toolA");
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_job_type_incident_blocks_agent_job_reemit_until_resolved() {
    // #1390 review: a tool whose activation failed before its child was created
    // never entered the container's active set, so completing the turn's OTHER
    // tool drains the set and would re-emit the agent job — letting the agent
    // complete the container out from under the parked retry. The container
    // must park (no new agent job) while the incident is unresolved.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="work-b" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();

    // The agent activates toolA (whose type expression fails) and toolB (fine)
    // in one turn.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);
    let incident_key = engine.active_incidents()[0].key;

    // Completing toolB drains the active set — but the container must NOT
    // re-emit its agent job while toolA's incident is unresolved.
    let tool_b = engine
        .activate_jobs("work-b", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolB")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            tool_b.key,
            HashMap::from([("result".to_string(), Value::Int(1))]),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .is_empty(),
        "no new agent turn while the failed tool's incident is unresolved",
    );
    assert!(!engine.state().instances.is_empty());

    // Resolve (with the type fixed): the failed tool re-activates, and once it
    // drains the agent job is re-emitted for the next turn.
    engine
        .apply_command(Command::set_variables(
            engine.state().instances.keys().copied().next().unwrap(),
            HashMap::from([("toolType".to_string(), Value::Str("work-a".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let tool_a = engine
        .activate_jobs("work-a", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("resolving re-activated toolA");
    engine
        .apply_command(Command::complete_job_with_result(
            tool_a.key,
            HashMap::from([("result".to_string(), Value::Int(2))]),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert_eq!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .len(),
        1,
        "once the incident is resolved and the retried tool drains, the next agent turn is emitted",
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn declarative_adhoc_tool_job_type_incident_blocks_container_completion_until_resolved() {
    // #1390 review: in a DECLARATIVE (BPMN_TASK) container, a tool whose
    // activation failed before its child was created is not in the active set,
    // so the collection draining would complete the container and destroy the
    // state the incident's redrive needs. Completion must park until the
    // incident is resolved.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:adHoc activeElementsCollection="=[&quot;toolA&quot;, &quot;toolB&quot;]" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="work-b" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // toolA's type expression fails on activation; toolB activates fine.
    assert_eq!(engine.active_incidents().len(), 1);
    let incident_key = engine.active_incidents()[0].key;
    let tool_b = engine
        .activate_jobs("work-b", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolB")
        .unwrap();

    // Draining the collection (toolB completes) must NOT complete the
    // container while toolA's incident is unresolved.
    engine
        .apply_command(Command::complete_job_with_result(
            tool_b.key,
            HashMap::new(),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert!(
        !engine.is_completed(inst),
        "the declarative container parks on the failed tool's incident",
    );

    // Fix the type and resolve: toolA re-activates, and completing it lets the
    // container (and the instance) complete.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("work-a".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let tool_a = engine
        .activate_jobs("work-a", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("resolving re-activated toolA");
    engine
        .apply_command(Command::complete_job_with_result(
            tool_a.key,
            HashMap::new(),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "once the retried tool drains, the declarative container completes",
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_job_type_redrive_resolves_against_current_scope_view() {
    // #1390 review: the redrive must resolve the tool against the container's
    // CURRENT scope view, not the frozen activation seed — otherwise a seed
    // value that IS the failure (`toolType = 42`) can never be corrected. The
    // scope chain's current binding of a name wins over the seed, so the
    // operator's `SetVariables` corrections (here `toolType`, and a `counter`
    // the root re-set while the incident parked) are what the retry sees. This
    // mirrors Zeebe, where resolving an incident re-evaluates the element
    // against the current variables.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
                <zeebe:ioMapping>
                  <zeebe:input source="=counter + 1" target="counter" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    // The root's `counter`, which the agent's seed will shadow at activation.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("counter".to_string(), Value::Int(1))]),
        ))
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();

    // The agent seeds `counter = 10` (shadowing the root) and a `toolType`
    // whose non-string value fails the pre-creation gate.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element_with(
                    "toolA",
                    &[("toolType", Value::Int(42)), ("counter", Value::Int(10))],
                )],
                ..Default::default()
            },
        ))
        .unwrap();
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "the seed's non-string type fails");
    let incident_key = active[0].key;

    // The operator corrects the failing `toolType` (and, while the incident is
    // parked, the root's `counter` is the current value the retry resolves
    // against — the frozen seed's shadow does not survive the incident).
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("tool".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "{:?}",
        engine.active_incidents()
    );
    let jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].variables.get("counter"),
        Some(&Value::Int(2)),
        "the retry resolves against the current scope view: the root's counter (1) + 1",
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_cancel_remaining_resolves_container_tool_type_incident() {
    // #1390 review: an agent's `cancelRemainingInstances=true` completion tears
    // the container down, but a pre-creation tool-type failure's incident sits
    // on the CONTAINER itself (not on any created child), so the child sweep
    // misses it. The teardown must resolve it: an active incident must not
    // outlive the container whose retry state it targets. A PARALLEL sibling
    // token keeps the instance alive after the container completes, so the
    // leak is observable (a whole-instance terminate would close the incident
    // on its own).
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:parallelGateway id="fork" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:serviceTask id="sibling">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="sibling-work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e1" />
          <bpmn:endEvent id="e2" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="fork" />
          <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="agent" />
          <bpmn:sequenceFlow id="f3" sourceRef="fork" targetRef="sibling" />
          <bpmn:sequenceFlow id="f4" sourceRef="agent" targetRef="e1" />
          <bpmn:sequenceFlow id="f5" sourceRef="sibling" targetRef="e2" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container_key = agent.element_instance_key;

    // Turn 1: the agent activates toolA, whose type expression fails before its
    // child is created — the incident parks on the CONTAINER.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine.active_incidents().len(),
        1,
        "the pre-creation failure parked an incident on the container",
    );

    // Turn 2: with the incident live, the operator cancels the container via
    // the external activate-activities command (`cancelRemainingInstances`).
    // The teardown must resolve the container's own incident — not leave it
    // active against the removed container's retry state.
    engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container_key,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .unwrap();

    // The container is gone, but the instance lives on (the sibling token is
    // still open), so the incident must have been resolved by the teardown.
    assert!(
        !engine.is_completed(inst),
        "the sibling token keeps the instance alive",
    );
    assert!(
        engine.active_incidents().is_empty(),
        "the cancel teardown resolved the container's own incident: {:?}",
        engine.active_incidents()
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_completion_condition_latches_while_tool_incident_parks_completion() {
    // #1390 review: with `cancelRemainingInstances=false`, a fulfilled
    // completion condition must LATCH when the last created sibling drains
    // while another tool is parked on a pre-creation incident. Otherwise the
    // fulfilment is lost: the incident guard drops the completion request, and
    // if the recovered tool's output makes the condition false again, the
    // container re-emits the agent job instead of completing.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent" cancelRemainingInstances="false">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=done</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="work-b" />
                <zeebe:ioMapping>
                  <zeebe:output source="=done" target="done" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();

    // The agent activates toolA (type expression fails) and toolB (fine).
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);
    let incident_key = engine.active_incidents()[0].key;

    // toolB completes setting `done = true`: the condition is fulfilled, but
    // toolA's incident parks the completion. The fulfilment must LATCH now.
    let tool_b = engine
        .activate_jobs("work-b", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolB")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            tool_b.key,
            HashMap::from([("done".to_string(), Value::Bool(true))]),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert!(
        !engine.is_completed(inst),
        "the incident parks the container completion",
    );
    assert!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .is_empty(),
        "no new agent turn while the incident parks",
    );

    // The operator fixes the type and resolves. toolA re-activates and
    // completes WITHOUT setting `done` — the container scope keeps `done =
    // true`, so this cannot distinguish a latch from a re-evaluation by
    // itself; what must NOT happen is a new agent turn (the lost-latch
    // symptom: the guard dropped the request, the condition re-evaluates
    // against the post-recovery scope, and the container continues instead of
    // completing). Flip `done` false via the recovered tool's output to force
    // the distinction: the latched fulfilment still completes the container.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("work-a".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let tool_a = engine
        .activate_jobs("work-a", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("resolving re-activated toolA");
    engine
        .apply_command(Command::complete_job_with_result(
            tool_a.key,
            HashMap::from([("done".to_string(), Value::Bool(false))]),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "the latched fulfilment completes the container even though the \
         recovered tool's output flipped the condition false",
    );
}

// zeebe-cells: incident:JOB_NO_RETRIES
#[test]
fn adhoc_cancel_remaining_cancels_failed_agent_job_before_resolving_incident() {
    // #1390 review: an external cancel-remaining on a container whose own agent
    // job failed (retries exhausted, `JobNoRetries` parked on the container)
    // must cancel that job before resolving the incident. Resolving alone would
    // resurrect the failed job to `Created` while the container is removed,
    // leaving an orphaned, activatable agent job. A parallel sibling keeps the
    // instance alive so the orphan is observable.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:parallelGateway id="fork" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:serviceTask id="sibling">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="sibling-work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e1" />
          <bpmn:endEvent id="e2" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="fork" />
          <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="agent" />
          <bpmn:sequenceFlow id="f3" sourceRef="fork" targetRef="sibling" />
          <bpmn:sequenceFlow id="f4" sourceRef="agent" targetRef="e1" />
          <bpmn:sequenceFlow id="f5" sourceRef="sibling" targetRef="e2" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    engine
        .apply_command(Command::fail_job(agent.key, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1, "JobNoRetries parked");

    engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: agent.element_instance_key,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .unwrap();

    assert!(!engine.is_completed(inst), "the sibling keeps it alive");
    assert!(
        engine.active_incidents().is_empty(),
        "{:?}",
        engine.active_incidents()
    );
    assert!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .is_empty(),
        "the failed agent job must be cancelled, not resurrected as an orphan",
    );
}

// zeebe-cells: incident:EXTRACT_VALUE_ERROR
#[test]
fn adhoc_tool_redrive_keeps_unseeded_names_inherited() {
    // #1390 review: the redrive must not copy inherited names the seed does
    // not carry into the recovered tool's local scope. With root `result = 0`
    // and an empty seed, completing the recovered tool with `result = 1`
    // updates the enclosing scope; a stale local copy would make
    // `outputElement="=result"` read `0`.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="=toolType" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([
                ("result".to_string(), Value::Int(0)),
                ("toolType".to_string(), Value::Int(42)),
            ]),
        ))
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1);
    let incident_key = active[0].key;
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("tool".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let tool = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool.len(), 1);
    engine
        .apply_command(Command::complete_job_with(
            tool[0].key,
            HashMap::from([("result".to_string(), Value::Int(1))]),
        ))
        .unwrap();
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent re-emitted after the tool drains");
    assert_eq!(
        agent2.variables.get("results"),
        Some(&Value::List(vec![Value::Int(1)])),
        "outputElement reads the tool's fresh result, not a stale local copy",
    );
}
