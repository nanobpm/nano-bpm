//! `boundary` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn variable_spill_excludes_instances_with_an_armed_boundary_timer() {
    // An instance parked on a service-task job that ALSO has an armed boundary
    // timer must NOT be spilled: the timer can resume the flow without job
    // activation (the host's rehydration seam), which would read empty
    // variables. start -> charge (service task, boundary timer PT5S) -> end.
    let def = ProcessBuilder::new("guarded")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_boundary_event("deadline", "charge", 5_000)
        .end_event("end")
        .end_event("expired")
        .connect("start", "charge")
        .connect("charge", "end")
        .connect("deadline", "expired")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = BTreeMap::new();
    vars.insert("k".to_string(), Value::Int(1));
    let events = engine
        .apply_command_at(Command::create_instance_with("guarded", vars), 1_000)
        .unwrap();
    let _key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The instance is parked on the payment job AND has an armed boundary
    // timer, so it is deliberately excluded from spill candidates.
    assert_eq!(
        engine.timers().len(),
        1,
        "a boundary timer is armed while the job is parked"
    );
    assert!(
        engine.spillable_instances(10).is_empty(),
        "instance with an armed boundary timer must not be spillable"
    );
    assert_eq!(engine.resident_spillable_count(), 0);
}

// zeebe-cells: element:BoundaryEvent event:boundary:timer
#[test]
fn should_fire_an_interrupting_timer_boundary_and_cancel_the_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_timer_boundary()))
        .unwrap();

    // Start at t=1000: the token parks on the service task, a job is created,
    // and the boundary timer is armed for due_at = 6000.
    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.timers().len(), 1);
    assert_eq!(engine.timers()[0].due_at, 6_000);
    assert!(!engine.is_completed(instance_key));

    // A tick before the due instant does nothing.
    assert!(engine.trigger_timers(5_999).is_empty());
    assert!(!engine.is_completed(instance_key));

    // At the due instant the timer interrupts the task: the job is cancelled,
    // the boundary's outgoing flow runs, and the instance completes.
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::JobCanceled { job_key: k, .. } if *k == job_key)));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
    )));
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);
}

#[test]
fn should_disarm_a_boundary_timer_when_the_job_completes_first() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_timer_boundary()))
        .unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // Completing the job before the timer fires takes the normal flow and
    // disarms the boundary timer.
    engine.activate_jobs("payment", "w", 1, 60_000, 1_000);
    engine
        .apply_command_at(Command::complete_job(job_key), 2_000)
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);

    // A later tick past the (now disarmed) due instant does nothing.
    assert!(engine.trigger_timers(10_000).is_empty());
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Completed
    );
}

#[test]
fn should_recover_an_armed_boundary_timer_via_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(process_with_timer_boundary()))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command_at(Command::create_instance("ship"), 1_000)
            .unwrap(),
    );

    // Replay: the armed boundary timer and its parked job survive.
    let mut recovered = Engine::replay(log);
    assert_eq!(recovered.timers().len(), 1);
    assert_eq!(recovered.timers()[0].state, state::TimerState::Created);
    let instance_key = recovered.timers()[0].instance_key;
    assert!(!recovered.is_completed(instance_key));

    // The recovered engine fires the boundary on the next due tick.
    recovered.trigger_timers(6_000);
    assert!(recovered.is_completed(instance_key));
    let job = recovered.state().jobs.values().next().unwrap();
    assert_eq!(job.state, state::JobState::Canceled);
}

// zeebe-cells: event:boundary:signal
#[test]
fn should_interrupt_an_activity_via_a_signal_boundary() {
    let def = ProcessBuilder::new("guarded")
        .start_event("start")
        .service_task("work", "do-work")
        .signal_boundary_event("abort", "work", "kill-switch")
        .end_event("done")
        .end_event("aborted")
        .connect("start", "work")
        .connect("work", "done")
        .connect("abort", "aborted")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The task parks on a job, with a boundary signal subscription open.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.signal_subscriptions().len(), 1);

    // Broadcasting the signal interrupts the task (cancels its job) and routes
    // along the boundary's outgoing flow, completing the instance.
    let fired = engine.broadcast_signal("kill-switch", BTreeMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(engine.is_completed(instance_key));
}

// zeebe-cells: event:boundary:message
#[test]
fn should_fire_an_interrupting_message_boundary_and_cancel_the_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_boundary()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "cancellable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the service task; a job and a boundary subscription
    // are created.
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.message_subscriptions().len(), 1);

    // Correlating the boundary message interrupts the task: the job is
    // cancelled, the boundary's outgoing flow runs, the instance completes.
    let fired = engine.correlate_message("order-cancelled", "A", BTreeMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::JobCanceled { job_key: k, .. } if *k == job_key)));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "cancel" && to == "aborted"
    )));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );
}

#[test]
fn should_cancel_a_message_boundary_subscription_when_the_job_completes_first() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_boundary()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "cancellable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // Completing the job before any message arrives takes the normal flow and
    // cancels the boundary subscription.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );

    // A later message for the (now cancelled) subscription correlates nothing.
    let fired = engine.correlate_message("order-cancelled", "A", BTreeMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
}

#[test]
fn should_fire_a_non_interrupting_timer_boundary_without_cancelling_the_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_timer_boundary(),
        ))
        .unwrap();

    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.timers()[0].due_at, 6_000);

    // At the due instant the timer fires but does NOT interrupt: the job
    // survives, the boundary's outgoing flow spawns a parallel token to
    // "reminded", and the instance stays active (the task is still running).
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Created
    );
    assert!(!fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "remind" && to == "reminded"
    )));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);

    // Completing the job then runs the normal flow and finishes the instance.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_fire_a_non_interrupting_message_boundary_for_every_matching_message() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_message_boundary(),
        ))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "notifiable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.message_subscriptions().len(), 1);

    // First message: spawns a parallel token to "notified" without cancelling
    // the job; the subscription stays open and the instance stays active.
    let fired = engine.correlate_message("reminder", "A", BTreeMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Created
    );
    assert!(!fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "notify" && to == "notified"
    )));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );
    assert!(!engine.is_completed(instance_key));

    // A second matching message fires the boundary again (open subscription).
    let fired = engine.correlate_message("reminder", "A", BTreeMap::new(), 0);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "notify" && to == "notified"
    )));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Completing the job runs the normal flow, finishes the instance and
    // cancels the still-open boundary subscription.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
}

// zeebe-cells: element:InclusiveGateway
#[test]
fn inclusive_join_waits_while_a_boundary_that_reaches_it_is_still_armed() {
    // #1168 regression (boundary-event reachability). An inclusive join `j` is
    // reachable from branch `a` (normal flow `a -> j`) AND from branch `b`'s timer
    // boundary `be` (`be -> j`). Crucially, `b`'s *own* completion flows elsewhere
    // (`b -> eb`), so `b` is not a sequence-flow predecessor of `j` — only its
    // armed boundary is. When `a` arrives at the open join while `b` is still
    // parked on its job (boundary armed), the join's guard must NOT fire `j`:
    // the still-armed boundary could yet route a token in, and firing early would
    // then mis-route that later boundary token. When `b` resolves instead (its
    // job completes, disarming the boundary), Zeebe does not re-evaluate `j`:
    // it only evaluates an inclusive join when a token arrives at it, and none
    // does, so `j` holds `a`'s token and the instance stays active (#1241).
    let def = ProcessBuilder::new("inc-boundary")
        .start_event("s")
        .inclusive_gateway("isplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .timer_boundary_event("be", "b", 5_000)
        .inclusive_gateway("j")
        .end_event("e")
        .end_event("eb")
        .connect("s", "isplit")
        .connect("isplit", "a")
        .connect("isplit", "b")
        .connect("a", "j")
        .connect("be", "j")
        .connect("b", "eb")
        .connect("j", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("inc-boundary"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 2);

    // Complete branch `a`; its token reaches the open inclusive join `j`. Branch
    // `b` is still parked with its boundary armed, so `j` must not fire yet.
    let after_a = complete_one(&mut engine, "ja");
    assert!(
        !after_a
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "e")),
        "the inclusive join must not fire while `b`'s armed boundary can still reach it"
    );
    assert!(
        !engine.is_completed(instance_key),
        "the instance must still be running with `b` parked and the join held open"
    );

    // Complete branch `b`; the boundary disarms and `b` flows to its own end.
    // Nothing arrives at `j`, so Zeebe never re-evaluates it.
    let after_b = complete_one(&mut engine, "jb");
    assert!(
        !after_b
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "e")),
        "the join is only evaluated when a token arrives at it"
    );
    assert!(!engine.is_completed(instance_key));
    let instance = engine.instance(instance_key).unwrap();
    assert!(instance
        .join_instances
        .contains_key(&crate::state::JoinKey::root("j")));
}

/// Issue #1170 — regression guard, service-task multi-instance child.
///
/// An interrupting boundary event on a multi-instance activity is armed on the
/// BODY and must tear down the WHOLE loop: every active child (here, each MI
/// child's job), the body's `MultiInstanceState` runtime record, and route the
/// boundary's outgoing flow exactly once. Before the fix,
/// `interrupt_activity_via_boundary` had no multi-instance branch, so the body's
/// `active` set kept the (now-cancelled) child keys and the instance hung
/// `Active` forever after the boundary flow finished.
#[test]
fn interrupting_boundary_on_a_multi_instance_service_task_tears_down_the_whole_loop() {
    // start -> each(MI svc "work") --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
    let def = ProcessBuilder::new("mi-svc")
        .start_event("start")
        .service_task("each", "work")
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
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-svc",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Two parallel children, each with a "work" job; the body registered its
    // multi-instance runtime record and armed the boundary timer on itself.
    let job_keys: Vec<_> = engine.pending_jobs().iter().map(|j| j.key).collect();
    assert_eq!(job_keys.len(), 2, "two MI children each minted a job");
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);
    assert!(engine.timers().iter().any(|t| t.element_id == "each"));

    // The timer interrupts the whole loop.
    let fired = engine.trigger_timers(5_000);
    for job_key in &job_keys {
        assert_eq!(
            engine.state().jobs[job_key].state,
            state::JobState::Canceled,
            "every MI child job is cancelled by the interrupting boundary"
        );
    }
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )),
        "the normal outgoing flow is never taken"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared (no stale active set)"
    );
    assert!(
        engine.is_completed(key),
        "the instance completes after the loop is torn down (#1170)"
    );
}

/// Issue #1170 — regression guard, embedded-sub-process multi-instance child.
///
/// Each MI child opens its own inner token scope with a running job. The
/// interrupting boundary on the body must terminate every child scope (and its
/// inner jobs), clear the body record, and route the boundary flow once.
#[test]
fn interrupting_boundary_on_a_multi_instance_subprocess_tears_down_every_child_scope() {
    // start -> each(MI sub[ sub_start -> inner("work") -> sub_end ]) --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
    let def = ProcessBuilder::new("mi-sub")
        .start_event("start")
        .sub_process("each", "sub_start")
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
        .start_event("sub_start")
        .contained_in("sub_start", "each")
        .service_task("inner", "work")
        .contained_in("inner", "each")
        .end_event("sub_end")
        .contained_in("sub_end", "each")
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-sub",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let job_keys: Vec<_> = engine.pending_jobs().iter().map(|j| j.key).collect();
    assert_eq!(
        job_keys.len(),
        2,
        "each MI sub-process child runs an inner job"
    );
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    let fired = engine.trigger_timers(5_000);
    for job_key in &job_keys {
        assert_eq!(
            engine.state().jobs[job_key].state,
            state::JobState::Canceled,
            "every inner job is cancelled with its child scope"
        );
    }
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )),
        "the normal outgoing flow is never taken"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared"
    );
    assert!(engine.is_completed(key), "the instance completes (#1170)");
}

/// Issue #1170 — regression guard, call-activity multi-instance child.
///
/// Each MI child spawns its own child process instance (Zeebe parity). The
/// interrupting boundary on the body must cancel every spawned child instance,
/// clear the body record, and route the boundary flow once.
#[test]
fn interrupting_boundary_on_a_multi_instance_call_activity_cancels_every_child_instance() {
    // callee: cs -> ct("work") -> ce
    let callee = ProcessBuilder::new("callee")
        .start_event("cs")
        .service_task("ct", "work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    // start -> each(MI call "callee") --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
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
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
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
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Each MI child spawned a "callee" child instance parked on its "work" job.
    let job_keys: Vec<_> = engine.pending_jobs().iter().map(|j| j.key).collect();
    assert_eq!(
        job_keys.len(),
        2,
        "two child process instances, each on a job"
    );
    assert_eq!(engine.instance(key).unwrap().multi_instances.len(), 1);

    let fired = engine.trigger_timers(5_000);
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceTerminated { .. }))
            .count(),
        2,
        "both spawned child process instances are terminated"
    );
    for job_key in &job_keys {
        assert_eq!(
            engine.state().jobs[job_key].state,
            state::JobState::Canceled,
            "each child instance's job is cancelled"
        );
    }
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "done"
        )),
        "the normal outgoing flow is never taken"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared"
    );
    assert!(
        engine.is_completed(key),
        "the parent instance completes (#1170)"
    );
}

/// Issue #1170 regression — a NORMAL multi-instance completion must disarm the
/// boundary events armed on the body. This PR arms an interrupting boundary on
/// the MI body (not per child); `complete_multi_instance_body` /
/// `finalize_multi_instance_body` must cancel those resources when the loop
/// drains, or a later timer could re-enter the boundary flow of an
/// already-completed body.
#[test]
fn normal_multi_instance_completion_disarms_the_body_boundary() {
    // start -> each(MI service "handle") --normal--> sink -> done
    // each --(interrupting timer boundary "timeout")--> escalated
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
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .timer_boundary_event("timeout", "each", 5_000)
        .service_task("sink", "sink-work")
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "done")
        .connect("timeout", "escalated")
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
    let _key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // A timer boundary is armed on the body for the life of the loop.
    let timeout_armed = |engine: &Engine| {
        engine.state().timers.values().any(|t| {
            t.state == state::TimerState::Created
                && matches!(
                    &t.kind,
                    state::TimerKind::InterruptingBoundary { boundary_element_id }
                        if boundary_element_id == "timeout"
                )
        })
    };
    assert!(
        timeout_armed(&engine),
        "the body boundary timer is armed while the loop runs"
    );

    // Drain the loop normally.
    for job in engine.activate_jobs("handle", "W", 10, 60_000, 0) {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }

    // The body's boundary timer is disarmed as the loop completes.
    assert!(
        !timeout_armed(&engine),
        "the body boundary timer is cancelled on normal completion (#1170)"
    );
    // Firing timers now can never re-enter the (completed) boundary flow.
    let fired = engine.trigger_timers(10_000);
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, .. } if from == "timeout"
        )),
        "a late timer must not re-enter the boundary flow of a completed body"
    );
}

// zeebe-cells: event:boundary:error
#[test]
fn should_route_to_an_error_boundary_when_a_job_throws_a_matching_error() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("payment"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when the worker throws the caught business error
    let events = engine
        .apply_command(Command::throw_job_error(
            job_key,
            "CARD_DECLINED",
            "card was declined",
        ))
        .unwrap();

    // then the activity is interrupted and the error path runs to completion
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "declined"
    )));
    // the task's normal outgoing flow was NOT taken
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    // and the job is consumed: completing it afterwards is NOT_FOUND (Zeebe
    // parity, #1401 — the entity is deleted with its terminal settlement).
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotFound { job_key });
}

// zeebe-cells: element:SubProcess
#[test]
fn should_interrupt_an_embedded_subprocess_via_its_error_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_error_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-error"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;

    // The inner job throws a business error the sub-process boundary catches.
    let events = engine
        .apply_command(Command::throw_job_error(job_key, "BUSINESS_ERROR", "boom"))
        .unwrap();

    // The whole sub-process is interrupted: its inner task instance is
    // completed (terminated), the sub-process completes without taking its
    // normal flow, and the boundary routes to the sad-flow path.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ElementTerminated { element_id, .. } if element_id == "inner"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "sad"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    // The interrupted inner job is consumed: completing it is NOT_FOUND
    // (Zeebe parity, #1401).
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotFound { job_key });

    // The instance is not yet complete: it is parked on the sad-flow task.
    assert!(!engine.is_completed(instance_key));
    let sad = complete_one(&mut engine, "sad-flow");
    assert!(sad.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_re_arm_a_non_interrupting_cycle_timer_boundary_on_every_fire() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_cycle_timer_boundary(),
        ))
        .unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("ticker"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.timers()[0].due_at, 6_000);

    // First fire at t=6000: spawns a token to "ticked", does not cancel the
    // job, and re-arms a fresh timer due at 11000 (6000 + 5000).
    let fired = engine.trigger_timers(6_000);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
    )));
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Created
    );
    let armed: Vec<u64> = engine
        .timers()
        .iter()
        .filter(|t| t.state == state::TimerState::Created)
        .map(|t| t.due_at)
        .collect();
    assert_eq!(
        armed,
        vec![11_000],
        "a fresh timer is armed for the next interval"
    );
    assert!(!engine.is_completed(instance_key));

    // Second fire at t=11000: fires again and re-arms for 16000.
    let fired = engine.trigger_timers(11_000);
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "tick" && to == "ticked"
    )));
    let armed: Vec<u64> = engine
        .timers()
        .iter()
        .filter(|t| t.state == state::TimerState::Created)
        .map(|t| t.due_at)
        .collect();
    assert_eq!(armed, vec![16_000]);

    // Completing the job runs the normal flow and disarms the pending timer.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert!(engine
        .timers()
        .iter()
        .all(|t| t.state != state::TimerState::Created));
}

#[test]
fn should_interrupt_an_embedded_subprocess_via_a_timer_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_timer_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command_at(Command::create_instance("sub-timer"), 1_000)
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the inner task; the boundary timer is armed on the
    // sub-process for due_at = 6000.
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.pending_jobs()[0].job_type, "work");
    assert!(engine
        .timers()
        .iter()
        .any(|t| t.due_at == 6_000 && t.element_id == "sub"));

    // At the due instant the timer interrupts the WHOLE sub-process: the
    // inner job is cancelled, the inner task and sub-process complete without
    // taking the normal flow, and the boundary routes to "escalated".
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::ElementTerminated { element_id, .. } if element_id == "inner"
    )));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
    )));
    assert!(!fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_interrupt_an_embedded_subprocess_via_a_message_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_message_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "sub-msg",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;
    assert!(engine
        .message_subscriptions()
        .iter()
        .any(|s| s.element_id == "sub"));

    // Correlating the boundary message interrupts the whole sub-process: the
    // inner job is cancelled, the inner task and sub-process complete, and the
    // boundary routes to "aborted" instead of the normal flow.
    let fired = engine.correlate_message("order-cancelled", "A", BTreeMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::ElementTerminated { element_id, .. } if element_id == "inner"
    )));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "cancel" && to == "aborted"
    )));
    assert!(!fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn interrupting_a_call_activity_via_a_boundary_cancels_its_child() {
    // A boundary event that interrupts a call activity must also cancel the
    // child process instance it spawned. Otherwise the parent token leaves via
    // the boundary flow while the child keeps running — an orphan with no
    // parent token left to complete it. The parent instance itself is NOT
    // terminated; it routes out the interrupting boundary's flow.
    let orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .timer_boundary_event("timeout", "c1", 5_000)
        .end_event("end")
        .end_event("escalated")
        .connect("start", "c1")
        .connect("c1", "end")
        .connect("timeout", "escalated")
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
        .apply_command_at(Command::create_instance("orch"), 1_000)
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .pending_jobs()
        .first()
        .expect("child parked on its job")
        .instance_key;
    assert_ne!(child_key, parent_key);

    // At the boundary timer's due instant it interrupts the call activity,
    // cancels the child instance, and routes the parent to "escalated".
    let fired = engine.trigger_timers(6_000);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
        )),
        "the boundary interrupt cancels the spawned child instead of orphaning it"
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
    )));
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    // The parent was not terminated — it completed via the boundary path.
    assert_ne!(
        engine.instance(parent_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(engine.is_completed(parent_key));
    // No orphaned job survives.
    assert!(engine.pending_jobs().is_empty());
}

/// Review round-2 (#1535): when a boundary interrupt tears down a scope that
/// holds a NESTED call activity (a sub-process whose inner call activity parks a
/// child process instance), the parent's `ElementTerminating` must be emitted
/// BEFORE the descendant call-child's teardown (`discard_instance_events(child)`
/// emits the child's `ProcessInstanceTerminated`). Emitting the child's
/// termination first reverses Zeebe's parent-before-child termination order.
#[test]
fn boundary_teardown_emits_parent_terminating_before_nested_call_child() {
    // mid: start -> sub [ sub_start -> callInner(call activity -> leaf) ]
    //      sub --(interrupting timer boundary "timeout")--> escaped
    let leaf = ProcessBuilder::new("leaf")
        .start_event("lstart")
        .service_task("leafwork", "leaf-job")
        .end_event("lend")
        .connect("lstart", "leafwork")
        .connect("leafwork", "lend")
        .build()
        .unwrap();
    let mid = ProcessBuilder::new("mid")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .call_activity("callInner", "leaf")
        .contained_in("callInner", "sub")
        .timer_boundary_event("timeout", "sub", 5_000)
        .end_event("done")
        .end_event("escaped")
        .connect("start", "sub")
        .connect("sub_start", "callInner")
        .connect("sub", "done")
        .connect("timeout", "escaped")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(leaf)).unwrap();
    engine.apply_command(Command::DeployProcess(mid)).unwrap();
    let created = engine
        .apply_command_at(Command::create_instance("mid"), 1_000)
        .unwrap();
    let parent_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // The nested call activity spawned a grandchild "leaf" instance parked on
    // its job.
    let leaf_key = engine
        .pending_jobs()
        .first()
        .expect("leaf child parked on its job")
        .instance_key;
    assert_ne!(leaf_key, parent_key);

    // Fire the boundary: it interrupts `sub`, tearing down the nested call
    // activity and its parked "leaf" child instance.
    let fired = engine.trigger_timers(6_000);

    // `sub`'s TERMINATING must precede the leaf child instance's TERMINATED.
    let sub_terminating = fired
        .iter()
        .position(
            |e| matches!(e, Event::ElementTerminating { element_id, .. } if element_id == "sub"),
        )
        .expect("sub emits ElementTerminating");
    let leaf_terminated = fired
        .iter()
        .position(|e| matches!(e, Event::ProcessInstanceTerminated { instance_key } if *instance_key == leaf_key))
        .expect("the nested call child is terminated");
    assert!(
        sub_terminating < leaf_terminated,
        "parent TERMINATING (idx {sub_terminating}) precedes the descendant call-child's termination (idx {leaf_terminated}): {fired:?}"
    );
    // And `sub`'s own TERMINATED lands after the child's teardown.
    let sub_terminated = fired
        .iter()
        .position(
            |e| matches!(e, Event::ElementTerminated { element_id, .. } if element_id == "sub"),
        )
        .expect("sub emits ElementTerminated");
    assert!(
        leaf_terminated < sub_terminated,
        "the child's teardown completes before the parent's TERMINATED"
    );
}

/// Review round-2 (#1535): one signal broadcast can reach the SAME boundary
/// event on two concurrent instances of one activity in the SAME flow scope (a
/// parallel split routing two tokens into the same task). Each queued boundary
/// activation must carry its OWN trigger payload: keying the hold by
/// `(flow_scope, boundary_id)` and removing it on the first activation leaves
/// the second activation reading a missing payload — and for an output-mapped
/// boundary its mappings then read stale/absent values. The hold is a per-key
/// FIFO, so each activation pops one.
#[test]
fn one_broadcast_hands_each_same_scope_boundary_host_its_payload() {
    // fork routes TWO tokens into the SAME task "work" (two parallel instances
    // in the root scope), each arming the interrupting signal boundary "abort"
    // (output-mapped `=payload -> mapped`). One broadcast interrupts BOTH.
    let def = ProcessBuilder::new("same-scope-hosts")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("work", "w")
        .signal_boundary_event("abort", "work", "kill-switch")
        .with_io(
            "abort",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=payload".to_string(),
                    target: "mapped".to_string(),
                }],
            },
        )
        .service_task("after", "after-work")
        .connect("start", "fork")
        .connect("fork", "work")
        .connect("fork", "work")
        .connect("abort", "after")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("same-scope-hosts"))
        .unwrap();
    // Two parallel instances of "work" in the root scope each opened a boundary
    // signal subscription.
    let subs = engine.signal_subscriptions();
    assert_eq!(
        subs.len(),
        2,
        "two live same-scope boundary hosts: {subs:?}"
    );

    let fired = engine.broadcast_signal("kill-switch", vars(&[("payload", Value::Int(42))]), 0);

    // Each of the two boundary activations received the payload LOCALLY (the
    // `payload` variable seeded on its own boundary element-instance scope) —
    // not one shared, first-come-only entry.
    let local_payloads = fired
        .iter()
        .filter(|e| matches!(e, Event::ScopedVariablesUpdated { variables, .. } if variables.get("payload") == Some(&Value::Int(42))))
        .count();
    assert_eq!(
        local_payloads, 2,
        "both same-scope boundary hosts received their own payload: {fired:?}"
    );
}

#[test]
fn feel_boundary_timer_evaluates_variable() {
    // A boundary timer with a FEEL timeDuration resolves against the instance
    // variables when the guarded activity is entered.
    let def = ProcessBuilder::new("feel-boundary")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_boundary_event("deadline", "charge", 0)
        .with_timer(
            "deadline",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Duration,
                expr: "=deadline".to_string(),
            },
        )
        .end_event("end")
        .end_event("timedout")
        .connect("start", "charge")
        .connect("charge", "end")
        .connect("deadline", "timedout")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let mut vars = BTreeMap::new();
    vars.insert("deadline".to_string(), Value::Str("PT5S".to_string()));
    engine
        .apply_command_at(Command::create_instance_with("feel-boundary", vars), 1_000)
        .unwrap();

    // Activating the job parks the token on `charge` with the boundary timer armed.
    engine.activate_jobs("payment", "w", 1, 60_000, 1_000);

    // due_at = now(1000) + FEEL("PT5S")=5000 = 6000.
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].due_at, 6_000);
}

// zeebe-cells: event:boundary:conditional
#[test]
fn should_fire_an_interrupting_conditional_boundary_on_a_variable_change() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            true,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded-cond"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The task parks on a job with an open conditional boundary subscription.
    assert_eq!(engine.pending_jobs().len(), 1);
    let job_key = engine.pending_jobs()[0].key;
    assert_eq!(engine.conditional_subscriptions().len(), 1);

    // Flipping `cancel` interrupts the task (cancels its job) and routes along
    // the boundary's outgoing flow, completing the instance.
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("cancel", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::JobCanceled { job_key: k, .. } if *k == job_key
    )));
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "aborted"
    )));
    assert!(engine.is_completed(key));
}

#[test]
fn should_fire_an_interrupting_conditional_boundary_immediately_when_true_on_activation() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            true,
        )))
        .unwrap();
    // `cancel` is already true when the activity activates: the boundary fires on
    // the first evaluation pass, so no job survives and the instance completes.
    let created = engine
        .apply_command(Command::create_instance_with(
            "guarded-cond",
            vars(&[("cancel", Value::Bool(true))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(key));
    assert_eq!(engine.pending_jobs().len(), 0);
}

#[test]
fn should_cancel_a_conditional_boundary_subscription_when_the_job_completes_first() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            true,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded-cond"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // Completing the job before the condition holds takes the normal flow and
    // cancels the boundary subscription.
    engine.activate_jobs("do-work", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(key));
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
}

#[test]
fn should_fire_a_non_interrupting_conditional_boundary_repeatedly() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_boundary(
            false,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded-cond"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // First satisfying change spawns a parallel token without cancelling the job;
    // the subscription stays open.
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("ping", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ConditionalTriggered { .. })));
    assert!(!fired.iter().any(|e| matches!(e, Event::JobCanceled { .. })));
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // A second satisfying update (ping toggled off then on) fires it again.
    engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("ping", Value::Bool(false))]),
        ))
        .unwrap();
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("ping", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ConditionalTriggered { .. })));

    // The job is still active the whole time; completing it finishes the instance.
    engine.activate_jobs("do-work", "w", 1, 60_000, 0);
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(engine.is_completed(key));
}

// --- Issue #1508: an interrupting boundary must resolve the interrupted
// element's own active incident (Zeebe resolves an element instance's
// incidents when the element is terminated) -------------------------------

/// Issue #1508 regression — the plain job-backed-activity branch: an
/// interrupting timer boundary firing on a service task parked on a
/// `JobNoRetries` incident must RESOLVE that incident, not leave it ACTIVE on
/// the completed element (where it is unresolvable — its job is already
/// `CANCELED` — and keeps a stale `hasIncident` on the instance).
#[test]
fn interrupting_boundary_on_a_service_task_resolves_its_job_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_timer_boundary()))
        .unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("ship"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Activate the job, then fail it with no retries: a JobNoRetries incident
    // parks on the service task's element instance.
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 1_000)[0].key;
    engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);
    let incident_key = engine.active_incidents()[0].key;

    // The interrupting boundary fires: the job is cancelled, the incident on
    // the interrupted element is resolved, and the boundary flow runs.
    let fired = engine.trigger_timers(6_000);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled,
        "the interrupted task's job is cancelled"
    );
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::IncidentResolved { incident_key: k, .. } if *k == incident_key
        )),
        "the boundary resolves the incident on the interrupted element (#1508)"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no ACTIVE incident remains on the completed element (#1508)"
    );
    assert!(
        engine.instance(instance_key).is_none()
            || engine.instance(instance_key).unwrap().incidents.is_empty(),
        "the instance's hasIncident index is cleared (#1508)"
    );
    assert!(engine.is_completed(instance_key));
}

/// Issue #1508 regression — a user task parked on a FAILED `creating`
/// task-listener job carries a `JobNoRetries` incident on its own element
/// instance; an interrupting timer boundary firing on the user task must
/// resolve that incident too (the listener job is cancelled with the task).
#[test]
fn interrupting_boundary_on_a_user_task_resolves_its_listener_incident() {
    let def = ProcessBuilder::new("approval-guarded")
        .start_event("start")
        .user_task("review")
        .timer_boundary_event("sla", "review", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "review")
        .connect("review", "done")
        .connect("sla", "escalated")
        .with_task_listeners(
            "review",
            vec![tl(TaskListenerEventType::Creating, "creating-listener")],
        )
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("approval-guarded"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The task parks on its creating-listener job; fail it with no retries so
    // the user task's element instance carries a JobNoRetries incident.
    let listener = engine.activate_jobs("creating-listener", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "the task is parked on its listener job");
    engine
        .apply_command(Command::fail_job(listener[0].key, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);

    // The interrupting boundary fires while the task carries that incident.
    let fired = engine.trigger_timers(6_000);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "the boundary resolves the incident on the interrupted user task (#1508)"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no ACTIVE incident remains on the completed user task (#1508)"
    );
    assert!(engine.is_completed(instance_key));
}

/// Issue #1508 regression — the sub-process-root branch: an incident parked on
/// the sub-process ROOT itself (an output-mapping failure holding it in
/// COMPLETING) must be resolved when an interrupting timer boundary fires on
/// the sub-process. `terminate_subprocess_scope` resolves only *descendant*
/// incidents, never the scope root's.
#[test]
fn interrupting_boundary_on_a_subprocess_resolves_a_root_incident() {
    // start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
    //          sub --(PT5S interrupting timer boundary)--> escalated
    // The sub-process carries an OUTPUT mapping that fails at drain time, so
    // the root parks in COMPLETING with an IoMapping incident while its
    // boundary timer is still armed (disarm runs after the projection).
    let def = ProcessBuilder::new("sub-timer-io")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=assert(bad, bad instance of number) + 1".to_string(),
                    target: "exported".to_string(),
                }],
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .timer_boundary_event("timeout", "sub", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command_at(
            Command::create_instance_with(
                "sub-timer-io",
                vars(&[("bad", Value::Str("oops".into()))]),
            ),
            1_000,
        )
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Completing the inner job drives the sub-process into its output-mapping
    // projection, which fails on `=bad + 1` (string + int): the sub-process
    // ROOT parks in COMPLETING carrying an IoMapping incident, its boundary
    // timer still armed.
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 1_000)[0].key;
    engine
        .apply_command_at(Command::complete_job(job_key), 2_000)
        .unwrap();
    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the failed output mapping parked an incident"
    );
    assert_eq!(
        active[0].element_id, "sub",
        "the incident sits on the sub-process ROOT"
    );
    assert!(
        engine
            .timers()
            .iter()
            .any(|t| t.element_id == "sub" && t.state == state::TimerState::Created),
        "the boundary timer is still armed while the root carries the incident"
    );

    // The interrupting boundary fires while the sub-process root carries that
    // incident.
    let fired = engine.trigger_timers(6_000);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "the boundary resolves the incident on the sub-process root (#1508)"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no ACTIVE incident remains on the completed sub-process (#1508)"
    );
    assert!(engine.is_completed(instance_key));
}

/// Issue #1508 regression — the call-activity branch: an incident parked on
/// the call-activity element itself (a `CalledElementError` — the callee is
/// not deployed, so no child is spawned) must be resolved when an
/// interrupting timer boundary fires on the call activity. The boundary is
/// armed before the spawn attempt, so it is live while the incident parks.
#[test]
fn interrupting_boundary_on_a_call_activity_resolves_its_own_incident() {
    let orchestrator = ProcessBuilder::new("orch-missing-callee")
        .start_event("start")
        .call_activity("c1", "not-deployed")
        .timer_boundary_event("timeout", "c1", 5_000)
        .end_event("end")
        .end_event("escalated")
        .connect("start", "c1")
        .connect("c1", "end")
        .connect("timeout", "escalated")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();
    let created = engine
        .apply_command_at(Command::create_instance("orch-missing-callee"), 1_000)
        .unwrap();
    let parent_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The callee is not deployed: the spawn fails and a CalledElementError
    // incident parks on the call-activity element instance itself, with the
    // boundary timer still armed.
    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the missing callee raised an incident on the call activity"
    );
    assert_eq!(active[0].element_id, "c1");
    assert!(
        engine
            .timers()
            .iter()
            .any(|t| t.element_id == "c1" && t.state == state::TimerState::Created),
        "the boundary timer is still armed while the call activity carries the incident"
    );

    // The interrupting boundary fires while the call activity carries that
    // incident.
    let fired = engine.trigger_timers(6_000);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "the boundary resolves the incident on the call activity (#1508)"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no ACTIVE incident remains on the completed call activity (#1508)"
    );
    assert!(engine.is_completed(parent_key));
}

/// Issue #1508 regression — the error-boundary catch path: an interrupting
/// ERROR boundary caught at an enclosing sub-process must resolve an incident
/// parked on the sub-process ROOT itself, not only the incidents of the
/// descendants `terminate_subprocess_scope` sweeps. The root incident here is
/// raised by a NON-interrupting cycle timer boundary whose re-arm expression
/// no longer evaluates (it reads a variable the throwing job's error
/// variables removed) — it parks on the sub-process root while the inner job
/// is still live to throw.
#[test]
fn error_boundary_caught_at_a_subprocess_resolves_a_root_incident() {
    // start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
    //          sub --(error boundary BUSINESS_ERROR)--> sad -> sad_end
    //          sub --(NON-interrupting FEEL cycle timer "tick")--> ticked
    let def = ProcessBuilder::new("sub-error-root-incident")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .error_boundary_event("catch", "sub", "BUSINESS_ERROR")
        .non_interrupting_timer_cycle_boundary_event("tick", "sub", 5_000)
        .with_timer(
            "tick",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Cycle,
                expr: "=interval".to_string(),
            },
        )
        .service_task("sad", "sad-flow")
        .service_task("ticked", "tick-work")
        .end_event("done")
        .end_event("sad_end")
        .end_event("tick_end")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("catch", "sad")
        .connect("sad", "sad_end")
        .connect("tick", "ticked")
        .connect("ticked", "tick_end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command_at(
            Command::create_instance_with(
                "sub-error-root-incident",
                vars(&[("interval", Value::Str("PT5S".into()))]),
            ),
            1_000,
        )
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 1_000)[0].key;

    // Fire the non-interrupting cycle timer once; then remove the variable its
    // re-arm expression reads so the SECOND fire's re-arm fails and parks an
    // incident on the sub-process ROOT while the inner job is still live.
    let first = engine.trigger_timers(6_000);
    assert!(
        first
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "tick")),
        "the first cycle fired"
    );
    engine
        .apply_command(Command::set_variables(
            instance_key,
            vars(&[("interval", Value::Null)]),
        ))
        .unwrap();
    let second = engine.trigger_timers(11_000);
    assert!(
        second
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { element_id, .. } if element_id == "sub")),
        "the failed re-arm raised an incident on the sub-process root"
    );
    assert_eq!(engine.active_incidents().len(), 1);

    // The inner job throws a business error the sub-process boundary catches.
    // The catch tears the sub-process down — and must resolve the root
    // incident parked on it, not leave it ACTIVE on the completed sub-process.
    let events = engine
        .apply_command(Command::throw_job_error(job_key, "BUSINESS_ERROR", "boom"))
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "the error catch resolves the incident on the sub-process root (#1508)"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no ACTIVE incident remains on the caught sub-process (#1508)"
    );
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "catch" && to == "sad"
    )));
}

/// #1535 (Zeebe parity): an activity interrupted by an interrupting boundary
/// is TERMINATED, never COMPLETED — completion and termination drive
/// different listeners, metrics and compensation eligibility — and a resting
/// user task on it is CANCELED (it must not stay claimable/completable
/// against a vanished element).
#[test]
fn interrupting_boundary_terminates_the_activity_and_cancels_its_user_task() {
    let def = ProcessBuilder::new("ut-timeout")
        .start_event("start")
        .user_task("review")
        .timer_boundary_event("timeout", "review", 5_000)
        .end_event("end")
        .end_event("timed-out")
        .connect("start", "review")
        .connect("review", "end")
        .connect("timeout", "timed-out")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command_at(Command::create_instance("ut-timeout"), 1_000)
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("the user task is created");

    let fired = engine.trigger_timers(6_000);
    let review_lifecycle: Vec<&Event> = fired
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::ElementCompleting { element_id, .. }
                    | Event::ElementCompleted { element_id, .. }
                    | Event::ElementTerminating { element_id, .. }
                    | Event::ElementTerminated { element_id, .. }
                    if element_id == "review"
            )
        })
        .collect();
    assert!(
        matches!(
            review_lifecycle.as_slice(),
            [
                Event::ElementTerminating { .. },
                Event::ElementTerminated { .. }
            ]
        ),
        "the interrupted task is terminated, not completed: {review_lifecycle:?}"
    );
    let canceled_at = fired
        .iter()
        .position(|e| {
            matches!(e, Event::UserTaskCanceled { user_task_key: k, .. } if *k == user_task_key)
        })
        .expect("the interrupted user task is canceled");
    let terminated_at = fired
        .iter()
        .position(
            |e| matches!(e, Event::ElementTerminated { element_id, .. } if element_id == "review"),
        )
        .unwrap();
    assert!(
        canceled_at < terminated_at,
        "USER_TASK CANCELED precedes TERMINATED"
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "timed-out"
    )));
    assert!(engine.is_completed(instance_key));
}

/// #1528 (review): an interrupting boundary on a multi-instance USER TASK tears
/// down N parallel children through `element_teardown_events` with NO caller
/// index (`teardown_idx: None`), which must build its fallback
/// element-instance → `Created` user-task index ONCE and share it across the
/// member list — not rescan the whole `state.user_tasks` map once per child (an
/// O(N × tasks) blow-up). This test drives a 32-child interrupt and asserts the
/// emitted cancellation set is exactly the children's tasks (no duplicates, no
/// orphans), which is the behaviour the shared fallback index must preserve
/// byte-for-byte.
///
/// The scan count is asserted through the test-only `user_task_scan_count`
/// probe, which counts both `None` fallbacks and shared-index builds: the
/// interrupted root's own fallback plus one index build — exactly 2, never N.
#[test]
fn interrupting_boundary_on_a_multi_instance_user_task_cancels_every_child_task_once() {
    const N: i64 = 32;
    // start -> each(MI userTask) --normal--> done
    // each --(interrupting timer boundary "timeout")--> escalated
    let def = ProcessBuilder::new("mi-ut-boundary")
        .start_event("start")
        .user_task("each")
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
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command_at(
            Command::create_instance_with(
                "mi-ut-boundary",
                vars(&[("items", Value::List((0..N).map(Value::Int).collect()))]),
            ),
            1_000,
        )
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Every child parked on its own user task.
    let created_tasks: std::collections::BTreeSet<Key> = engine
        .state()
        .user_tasks
        .values()
        .filter(|t| t.state == state::UserTaskState::Created)
        .map(|t| t.key)
        .collect();
    assert_eq!(created_tasks.len(), N as usize, "one user task per child");

    // The timer interrupts the whole loop.
    engine.user_task_scan_count.set(0);
    let fired = engine.trigger_timers(6_000);
    assert!(
        engine.user_task_scan_count.get() == 2,
        "an N-member interrupt scans `state.user_tasks` exactly twice — the \
         interrupted root's own `None` fallback plus ONE shared index build for \
         its members — never once per member (index builds are counted too): {}",
        engine.user_task_scan_count.get()
    );

    let canceled_events: Vec<Key> = fired
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
    assert_eq!(
        canceled, created_tasks,
        "each child's task is cancelled exactly once (no duplicate, none missed)"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.state != state::UserTaskState::Created),
        "no child user task is left Created after the interrupt"
    );
    assert!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count()
            == 1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        engine.is_completed(instance_key),
        "the instance completes after the loop is torn down"
    );
}

/// The lifecycle records an element emits between activation and its final
/// rest, in order: Completing/Completed/Terminating/Terminated only.
fn lifecycle_of<'e>(events: &'e [Event], id: &str) -> Vec<&'e Event> {
    events
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::ElementCompleting { element_id, .. }
                    | Event::ElementCompleted { element_id, .. }
                    | Event::ElementTerminating { element_id, .. }
                    | Event::ElementTerminated { element_id, .. }
                    if element_id == id
            )
        })
        .collect()
}

/// #1535 (Zeebe parity): an activity interrupted by an interrupting MESSAGE
/// boundary is TERMINATED, never COMPLETED — the same teardown contract as an
/// interrupting timer boundary.
#[test]
fn interrupting_message_boundary_terminates_the_activity() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_boundary()))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "cancellable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();

    let fired = engine.correlate_message("order-cancelled", "A", BTreeMap::new(), 0);
    let charge_lifecycle = lifecycle_of(&fired, "charge");
    assert!(
        matches!(
            charge_lifecycle.as_slice(),
            [
                Event::ElementTerminating { .. },
                Event::ElementTerminated { .. }
            ]
        ),
        "the message-interrupted task is terminated, not completed: {charge_lifecycle:?}"
    );
}

/// #1535 (Zeebe parity): a NON-interrupting boundary does NOT terminate its
/// host — the host completes normally once its work finishes; only the
/// boundary's own parallel token runs.
#[test]
fn non_interrupting_message_boundary_does_not_terminate_the_activity() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_message_boundary(),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "notifiable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // The boundary fires: the host task must NOT be terminated.
    let fired = engine.correlate_message("reminder", "A", BTreeMap::new(), 0);
    let charge_lifecycle = lifecycle_of(&fired, "charge");
    assert!(
        charge_lifecycle.is_empty(),
        "a non-interrupting boundary emits no host lifecycle record: {charge_lifecycle:?}"
    );
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::ElementTerminating { .. } | Event::ElementTerminated { .. }
        )),
        "a non-interrupting boundary terminates nothing: {fired:?}"
    );

    // The host then completes NORMALLY (Completing -> Completed) once its job
    // finishes — never Terminating/Terminated.
    engine.activate_jobs("payment", "w", 1, 60_000, 0);
    let completed = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    let charge_lifecycle = lifecycle_of(&completed, "charge");
    assert!(
        matches!(
            charge_lifecycle.as_slice(),
            [
                Event::ElementCompleting { .. },
                Event::ElementCompleted { .. }
            ]
        ),
        "the uninterrupted host completes, it is not terminated: {charge_lifecycle:?}"
    );
}

/// #1739: `MessageCorrelated` records the correlated message's payload and the
/// triggered boundary event ON THE EVENT, for every subscription kind, so a
/// consumer never has to infer the payload from the variable writes that
/// follow. Covers an interrupting boundary with payload, a non-interrupting
/// boundary, a payload-less message and an intermediate catch.
#[test]
fn message_correlated_records_payload_and_boundary_on_the_event() {
    fn correlated(events: &[Event]) -> (BTreeMap<String, Value>, Option<String>) {
        events
            .iter()
            .find_map(|e| match e {
                Event::MessageCorrelated {
                    variables,
                    boundary_element_id,
                    ..
                } => Some((variables.clone(), boundary_element_id.clone())),
                _ => None,
            })
            .expect("a MessageCorrelated event")
    }
    let payload = vars(&[("reason", Value::Str("fraud".into()))]);

    // Interrupting boundary, with payload.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_boundary()))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "cancellable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let fired = engine.correlate_message("order-cancelled", "A", payload.clone(), 0);
    assert_eq!(correlated(&fired), (payload.clone(), Some("cancel".into())));

    // Non-interrupting boundary, with payload.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_non_interrupting_message_boundary(),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "notifiable",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let fired = engine.correlate_message("reminder", "A", payload.clone(), 0);
    assert_eq!(correlated(&fired), (payload.clone(), Some("notify".into())));
    // Payload-less message on the same (still open) subscription.
    let fired = engine.correlate_message("reminder", "A", BTreeMap::new(), 0);
    assert_eq!(correlated(&fired), (BTreeMap::new(), Some("notify".into())));

    // Intermediate catch: no boundary.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let fired = engine.correlate_message("payment-received", "A", payload.clone(), 0);
    assert_eq!(correlated(&fired), (payload, None));
}

/// #1528 review: the per-teardown user-task index holds only the tearing-down
/// instance's tasks, so an unrelated backlog in OTHER instances is never copied
/// into it (no per-teardown allocation proportional to the global backlog).
#[test]
fn created_user_task_index_is_scoped_to_the_teardown_instance() {
    let def = ProcessBuilder::new("ut-idx")
        .start_event("start")
        .user_task("ask")
        .end_event("done")
        .connect("start", "ask")
        .connect("ask", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut keys = Vec::new();
    for _ in 0..3 {
        let created = engine
            .apply_command(Command::create_instance_with("ut-idx", vars(&[])))
            .unwrap();
        keys.push(created.iter().find_map(|e| e.instance_key()).unwrap());
    }
    let index = engine.teardown_index(keys[0]);
    let tasks: Vec<_> = index.user_tasks.values().flatten().collect();
    assert_eq!(tasks.len(), 1, "only the target instance's task is indexed");
    assert!(tasks.iter().all(|t| t.instance_key == keys[0]));
}

// zeebe-cells: element:InclusiveGateway element:BoundaryEvent event:boundary:timer
#[test]
fn second_boundary_fed_inclusive_join_fires_once_its_boundary_interrupts() {
    // #1730 (differential fuzzer, case seed 2203747932). Two boundary-fed
    // inclusive joins in sequence: `t1[msg b1] -> g1(or) -> t2[timer b2] ->
    // g2(or) -> g3(xor, two flows) -> e`. Completing `t1` reaches `g1`, which
    // fires; once `b2` fires and terminates `t2`, no token can still reach
    // `g2` along `t2`, so `g2` must activate and the instance complete.
    //
    // The recorded scenario routes `g2` over exactly ONE of its two outgoing
    // flows (`route_G2 = ["f7"]`, fuzz-corpus.json:808-817,841-847). Constant
    // FEEL conditions reproduce that single-route choice without depending on
    // the fuzzer harness's variable step: the inclusive split takes only the
    // `true` flow, so exactly one `g2 -> g3` traversal is journaled.
    let def = ProcessBuilder::new("chained-boundary-joins")
        .start_event("s")
        .service_task("t1", "j1")
        .message_boundary_event("b1", "t1", "m1", "=\"k\"")
        .inclusive_gateway("g1")
        .service_task("t2", "j2")
        .timer_boundary_event("b2", "t2", 60_000)
        .inclusive_gateway("g2")
        .exclusive_gateway("g3")
        .end_event("e")
        .connect("s", "t1")
        .connect("t1", "g1")
        .connect("b1", "g1")
        .connect("g1", "t2")
        .connect("t2", "g2")
        .connect("b2", "g2")
        .connect_when("g2", "g3", "=true")
        .connect_when("g2", "g3", "=false")
        .connect("g3", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command_at(Command::create_instance("chained-boundary-joins"), 0)
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    complete_one(&mut engine, "j1");
    assert!(!engine.is_completed(instance_key));
    let due = engine
        .timers()
        .iter()
        .find(|t| t.state == state::TimerState::Created)
        .map(|t| t.due_at)
        .expect("b2 armed");
    let fired = engine.trigger_timers(due);
    assert!(
        engine.is_completed(instance_key),
        "g2 must fire once b2 interrupts t2"
    );
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "g2" && to == "g3"
            ))
            .count(),
        1,
        "g2 selects exactly one outgoing flow, as in the recorded #1730 routing"
    );
}
