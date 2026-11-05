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
    let mut vars = HashMap::new();
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
    let fired = engine.broadcast_signal("kill-switch", HashMap::new(), 0);
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
    let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
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
    let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
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
    let fired = engine.correlate_message("reminder", "A", HashMap::new(), 0);
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
    let fired = engine.correlate_message("reminder", "A", HashMap::new(), 0);
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
    assert!(instance.join_instances.contains_key("j"));
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
    // and the job is consumed: it cannot be completed afterwards
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });
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
        Event::ElementCompleted { element_id, .. } if element_id == "inner"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "sad"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "done"
    )));
    // The interrupted inner job is consumed and cannot be completed.
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });

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
        Event::ElementCompleted { element_id, .. } if element_id == "inner"
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
    let fired = engine.correlate_message("order-cancelled", "A", HashMap::new(), 0);
    assert_eq!(
        engine.state().jobs[&job_key].state,
        state::JobState::Canceled
    );
    assert!(fired.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "inner"
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

    let mut vars = HashMap::new();
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
