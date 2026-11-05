//! `compensation_escalation` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

// zeebe-cells: event:boundary:compensation event:intermediate-throw:compensation
#[test]
fn should_run_the_compensation_handler_when_a_compensation_throw_event_fires() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_compensation()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("trip"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete the compensable activity: it becomes compensable and the token
    // advances to the compensation throw event, which triggers its handler.
    let book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    let events = engine.apply_command(Command::complete_job(book)).unwrap();
    // the handler was activated (its job now exists) but the instance is not
    // done — the throw event rests until the handler completes
    assert!(!engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationSubscriptionCreated { element_id, handler, .. }
            if element_id == "book" && handler == "cancel"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationTriggered { throw_element_id, handlers, .. }
            if throw_element_id == "throw" && handlers == &vec!["cancel".to_string()]
    )));
    // the throw's outgoing flow has NOT been taken yet
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "done")));

    // Complete the compensation handler's job: the throw event completes, routes
    // to `done`, and the instance finishes.
    let cancel = engine.activate_jobs("cancel-job", "w", 1, 60_000, 0)[0].key;
    let events = engine.apply_command(Command::complete_job(cancel)).unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted { handler_element_id, .. }
            if handler_element_id == "cancel"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

/// Defect class (#897): two compensation throw events in the SAME scope can both
/// be waiting on the SAME handler element id. Routing a handler completion back
/// to its throw by `element_id` + scope and then picking `.min()` throw key
/// attributes the SECOND throw's handler completion to the FIRST throw: that
/// throw completes early (its token routes onward while its own handler is still
/// running) and the second throw's wait is left with a pending handler that no
/// active element instance can ever clear, so it waits forever. The fix tracks
/// handler *element-instance keys*: a throw records the exact handler instances
/// it activated, and a completing handler routes to the throw that activated IT.
///
/// Topology (a parallel split runs two compensable activities wired to the same
/// `cancel` handler; each branch then reaches its own compensation throw, so
/// each throw compensates the one activity completed in the scope at that
/// point):
///
/// ```text
/// start -> split =< bookA(svc, comp-boundary -> cancel(svc)) -> throw1 -> done1
///                  bookB(svc, comp-boundary -> cancel(svc)) -> throw2 -> done2 >
/// ```
#[test]
fn two_throws_in_one_scope_route_each_handler_completion_to_its_own_throw() {
    let def = ProcessBuilder::new("comp-two-throw")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("bookA", "book-job")
        .compensation_boundary_event("compA", "bookA", "cancel")
        .compensation_throw_event("throw1")
        .end_event("done1")
        .service_task("bookB", "book-job")
        .compensation_boundary_event("compB", "bookB", "cancel")
        .compensation_throw_event("throw2")
        .end_event("done2")
        .service_task("cancel", "cancel-job")
        .connect("s", "split")
        .connect("split", "bookA")
        .connect("bookA", "throw1")
        .connect("throw1", "done1")
        .connect("split", "bookB")
        .connect("bookB", "throw2")
        .connect("throw2", "done2")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-two-throw"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete bookA first: throw1 fires with only bookA's subscription in the
    // scope and rests on its `cancel` handler. Then bookB: throw2 fires with
    // only bookB's subscription left and rests on its own `cancel` handler.
    // (One job at a time: `activate_jobs` LOCKS what it returns, so a second
    // batch at the same logical instant would not see a job locked by the
    // first.)
    let book_a = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    engine.apply_command(Command::complete_job(book_a)).unwrap();
    let book_b = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    engine.apply_command(Command::complete_job(book_b)).unwrap();

    let throw_of = |engine: &Engine, element_id: &str| {
        engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .iter()
            .find(|(_, w)| w.throw_element_id == element_id)
            .map(|(k, _)| *k)
            .unwrap_or_else(|| panic!("{element_id} is waiting on its `cancel` handler"))
    };
    let t1 = throw_of(&engine, "throw1");
    let t2 = throw_of(&engine, "throw2");

    // Complete the two handler jobs in REVERSE activation order: throw2's
    // handler finishes while throw1's is still running. The completion must
    // retire throw2 (the throw that activated THAT handler instance) — an
    // `element_id` + `.min()` lookup instead attributes it to throw1, which
    // then completes early while its own handler is still running, and throw2
    // is left waiting on a handler instance that no longer exists.
    let cancel_jobs = engine.activate_jobs("cancel-job", "w", 2, 60_000, 0);
    assert_eq!(cancel_jobs.len(), 2, "one handler instance per throw");
    let first = engine
        .apply_command(Command::complete_job(cancel_jobs[1].key))
        .unwrap();
    assert!(
        first.iter().any(|e| matches!(
            e,
            Event::CompensationHandlerCompleted {
                throw_element_instance_key,
                handler_element_id,
                ..
            } if *throw_element_instance_key == t2 && handler_element_id == "cancel"
        )),
        "throw2's handler completion retires throw2, the throw that activated it"
    );
    assert!(
        first.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "throw2" && to == "done2"
        )),
        "throw2 routes onward once its own handler is done"
    );
    assert_eq!(
        throw_of(&engine, "throw1"),
        t1,
        "throw1 is still waiting on its own (still-running) handler"
    );

    let second = engine
        .apply_command(Command::complete_job(cancel_jobs[0].key))
        .unwrap();
    assert!(second.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted {
            throw_element_instance_key,
            handler_element_id,
            ..
        } if *throw_element_instance_key == t1 && handler_element_id == "cancel"
    )));
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .is_empty(),
        "no throw is left waiting on a handler that already completed"
    );
    assert!(
        engine.is_completed(instance_key),
        "both throws completed and routed onward, so the instance finishes"
    );
}

#[test]
fn should_pass_through_a_compensation_throw_event_with_nothing_to_compensate() {
    // start -> throw(compensation) -> done, with a compensable activity that is
    // never reached, so the throw finds nothing to compensate.
    let def = ProcessBuilder::new("noop-comp")
        .start_event("s")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("s", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("noop-comp"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // the throw is a pass-through: it routes straight to `done` and completes
    assert!(engine.is_completed(instance_key));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(!created
        .iter()
        .any(|e| matches!(e, Event::CompensationTriggered { .. })));
}

/// Defect class: a *sub-process*-scoped terminate must also drop the compensation
/// state scoped to the torn-down scope. A completed compensable activity records
/// a `compensable` subscription tagged with the scope it ran in; the whole-
/// instance `ProcessInstanceTerminated` sweep clears both compensation maps, but
/// a scoped terminate only removes the tokens — leaving the subscription attached
/// to the still-live parent until whole-instance eviction (stale state, and a
/// handler that could run for an activity in a scope that no longer exists).
#[test]
fn subprocess_terminate_clears_scoped_compensation_state() {
    //  main: start -> sub -> after(svc) -> done
    //  sub:  sub_start -> split =< book(svc, comp-boundary "book-comp" -> "cancel")
    //                                     -> hold(svc),
    //                             trigger(svc) -> inner_stop(terminate) >
    let def = ProcessBuilder::new("comp-term")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("book", "book-job")
        .contained_in("book", "sub")
        .compensation_boundary_event("book-comp", "book", "cancel")
        .contained_in("book-comp", "sub")
        .service_task("cancel", "cancel-job")
        .contained_in("cancel", "sub")
        .service_task("hold", "hold-job")
        .contained_in("hold", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("inner_stop")
        .contained_in("inner_stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "book")
        .connect("book", "hold")
        .connect("split", "trigger")
        .connect("trigger", "inner_stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-term"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete `book`: it becomes compensable (scoped to the sub-process) and its
    // token advances to `hold`, so the sub-process is still live.
    let events = complete_one(&mut engine, "book-job");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationSubscriptionCreated { element_id, handler, .. }
            if element_id == "book" && handler == "cancel"
    )));
    assert_eq!(
        engine.instance(instance_key).unwrap().compensable.len(),
        1,
        "the completed compensable activity is recorded"
    );

    // The sibling `trigger` drives its token into the terminate end, tearing down
    // the sub-process scope — which must also clear the scoped compensation state.
    let events = complete_one(&mut engine, "trigger-job");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ScopedCompensationCleared { .. })),
        "scope teardown must emit ScopedCompensationCleared"
    );
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensable
            .is_empty(),
        "the compensation subscription scoped to the terminated sub-process must be dropped"
    );
    // The parent continues past the scoped terminate and finishes normally.
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );
    let final_events = complete_one(&mut engine, "after-job");
    assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
}

#[test]
fn should_spawn_a_parallel_token_via_a_non_interrupting_escalation_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_escalation_boundary(
            false, "OVERLOAD",
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-sub"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The throw raises OVERLOAD: a non-interrupting boundary spawns a parallel
    // handler token while the enclosing sub-process keeps running (the throw
    // routes on to its own service task).
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "work"
    )));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));
    assert!(created
        .iter()
        .all(|e| !matches!(e, Event::IncidentRaised { .. })));

    // Both tokens are live: the inner "work" job and the boundary "handle" job.
    let mut job_types: Vec<_> = engine
        .pending_jobs()
        .iter()
        .map(|j| j.job_type.clone())
        .collect();
    job_types.sort();
    assert_eq!(job_types, vec!["handle".to_string(), "work".to_string()]);

    // Draining both tokens completes the instance.
    complete_one(&mut engine, "handle");
    assert!(!engine.is_completed(instance_key));
    complete_one(&mut engine, "work");
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

// zeebe-cells: event:boundary:escalation
#[test]
fn should_interrupt_a_subprocess_via_an_interrupting_escalation_boundary() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_escalation_boundary(
            true, "OVERLOAD",
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-sub"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The interrupting boundary tears the sub-process down: the throw's own
    // outgoing flow is NOT taken, only the boundary path continues.
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));
    assert!(created.iter().all(|e| !matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "work"
    )));
    // Only the handler token survives — the inner "work" job never arms.
    let job_types: Vec<_> = engine
        .pending_jobs()
        .iter()
        .map(|j| j.job_type.clone())
        .collect();
    assert_eq!(job_types, vec!["handle".to_string()]);

    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_catch_any_escalation_via_a_catch_all_boundary() {
    // A boundary with an empty escalation code is a catch-all: it catches the
    // OVERLOAD escalation even though it names no specific code.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_escalation_boundary(
            true, "",
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-sub"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));
    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
}

#[test]
fn interrupting_escalation_boundary_on_a_multi_instance_subprocess_clears_the_body() {
    // #1173 root-teardown guard. An interrupting escalation boundary attached to
    // a MULTI-INSTANCE sub-process must tear the whole loop down AND drop the
    // body's runtime record: `scope_teardown_events` only sweeps descendants, and
    // completing the caught body/child via a bare `ElementCompleted` leaves
    // `MultiInstanceState.active` populated (cleared only by
    // `MultiInstanceCompleted` / `MultiInstanceChildCompleted`), so without the
    // shared root-record teardown the body waits forever on a child already
    // routed to the handler and the instance hangs.
    let def = ProcessBuilder::new("mi-esc")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .with_multi_instance(
            "sub",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .escalation_boundary_event("boundary", "sub", "OVERLOAD")
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "sub")
        .connect("sub_start", "throw")
        .connect("throw", "sub_end")
        .connect("sub", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-esc",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // No incident, and the interrupting boundary routed to the handler.
    assert!(created
        .iter()
        .all(|e| !matches!(e, Event::IncidentRaised { .. })));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));

    // The teardown must clear the multi-instance body's runtime record — a leak
    // here is exactly the #1170/#1173 hang.
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .multi_instances
            .is_empty(),
        "interrupting escalation teardown must clear the multi-instance body record"
    );

    // Draining the handler token(s) completes the instance; a leaked body record
    // would hang it Active forever.
    while engine.pending_jobs().iter().any(|j| j.job_type == "handle") {
        complete_one(&mut engine, "handle");
    }
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn non_interrupting_escalation_boundary_on_a_multi_instance_subprocess_arms_at_the_parent_scope() {
    // #1173 non-interrupting MI-scope guard. A NON-interrupting escalation
    // boundary attached to a MULTI-INSTANCE sub-process must arm its parallel
    // handler token at the boundary's OWN scope (the process root), not inside
    // the multi-instance body scope of the child that threw. The caught element
    // is the MI *child*, so `scope_of(caught_eik)` is the MI body — arming the
    // handler there traps the handler token inside the loop body, so the body
    // never completes (a #1170/#1173-class hang) or the handler routes its
    // top-level `handler_end` in the wrong scope. The non-interrupting branch
    // must mirror the interrupting branch's MI-child -> body redirection.
    let def = ProcessBuilder::new("mi-esc-ni")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .with_multi_instance(
            "sub",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .non_interrupting_escalation_boundary_event("boundary", "sub", "OVERLOAD")
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "sub")
        .connect("sub_start", "throw")
        .connect("throw", "sub_end")
        .connect("sub", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-esc-ni",
            vars(&[("items", Value::List(vec![Value::Int(1)]))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The child throws OVERLOAD: the non-interrupting boundary spawns a parallel
    // handler token while the child keeps running on to its own `sub_end`. No
    // incident, and the boundary routed to the handler.
    assert!(created
        .iter()
        .all(|e| !matches!(e, Event::IncidentRaised { .. })));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));

    // The handler token must be armed at the boundary's OWN (process-root) scope,
    // not inside the multi-instance body scope of the child that threw. `scopes`
    // maps each active element instance to its enclosing sub-process/MI-body
    // scope; a root-scoped element is absent from it. With the pre-fix
    // `scope_of(caught_eik)` (the MI body) the handler would be armed inside the
    // loop body and appear here mapped to the body scope — the wrong level.
    {
        let inst = engine.instance(instance_key).unwrap();
        let handler_eik = *inst
            .active
            .iter()
            .find(|(_, id)| id.as_str() == "handler")
            .map(|(k, _)| k)
            .expect("the non-interrupting handler token must be active");
        assert!(
            !inst.scopes.contains_key(&handler_eik),
            "the non-interrupting handler must arm at the parent (root) scope, \
             not inside the multi-instance body scope"
        );
    }

    // The multi-instance body completes normally (its child is not blocked by
    // the parallel handler token): a handler armed inside the body scope would
    // trap a token in the loop and hang it.
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .multi_instances
            .is_empty(),
        "the multi-instance body must complete independently of the parallel handler"
    );

    // Draining the handler token completes the instance cleanly.
    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_ignore_an_uncaught_escalation() {
    // An escalation with no catching boundary is ignored (BPMN/Zeebe parity):
    // no incident, and the throw simply passes through to its outgoing flow.
    let def = ProcessBuilder::new("esc-uncaught")
        .start_event("start")
        .escalation_throw_event("throw", "OVERLOAD")
        .service_task("after", "after")
        .end_event("done")
        .connect("start", "throw")
        .connect("throw", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-uncaught"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "after"
    )));
    assert!(created
        .iter()
        .all(|e| !matches!(e, Event::IncidentRaised { .. })));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert_eq!(engine.pending_jobs()[0].job_type, "after");

    complete_one(&mut engine, "after");
    assert!(engine.is_completed(instance_key));
}

// zeebe-cells: event:end:escalation
#[test]
fn should_raise_an_escalation_via_an_escalation_end_event() {
    // An escalation END event (an escalation throw carrier with no outgoing
    // flow) raises the escalation and drains its token; an interrupting
    // boundary catches it and tears the sub-process down.
    let def = ProcessBuilder::new("esc-end")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("esc_end", "OVERLOAD")
        .contained_in("esc_end", "sub")
        .escalation_boundary_event("boundary", "sub", "OVERLOAD")
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "sub")
        .connect("sub_start", "esc_end")
        .connect("sub", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-end"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The escalation end event completed and was caught by the boundary.
    assert!(created.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "esc_end"
    )));
    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));
    // The sub-process did not take its normal outgoing flow.
    assert!(created.iter().all(|e| !matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "done"
    )));

    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
}

// zeebe-cells: event:intermediate-throw:escalation
#[test]
fn should_raise_an_escalation_only_after_a_throws_end_listener_drains() {
    // Regression (#1173): an escalation throw with an *end execution listener*
    // defers completion — its `finalize_completion` (which raises the escalation
    // and continues) must run only *after* the listener job chain drains. The
    // existing escalation coverage builds listener-free throws, leaving this
    // replay-sensitive deferred path uncovered. Assert the escalation is not
    // raised while the end listener is pending, then is caught once it drains.
    let def = ProcessBuilder::new("esc-throw-listener")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .escalation_boundary_event("boundary", "sub", "OVERLOAD")
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "sub")
        .connect("sub_start", "throw")
        .connect("throw", "sub_end")
        .connect("sub", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .with_listeners(
            "throw",
            Vec::new(),
            vec![el(ListenerEventType::End, "esc-audit")],
        )
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-throw-listener"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The throw parks on its end listener: the escalation is not raised yet, so
    // the boundary has not fired.
    assert!(
        created.iter().all(|e| !matches!(
            e,
            Event::SequenceFlowTaken { from, .. } if from == "boundary"
        )),
        "escalation must not be raised until the throw's end listener drains"
    );
    assert!(
        engine
            .pending_jobs()
            .iter()
            .any(|j| j.job_type == "esc-audit"),
        "the throw's end execution listener job is pending"
    );

    // Draining the end listener completes the throw, which now raises the
    // escalation; the interrupting boundary catches it and routes to the handler.
    let after = complete_one(&mut engine, "esc-audit");
    assert!(
        after.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
        )),
        "escalation is raised and caught once the end listener drains"
    );

    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_raise_an_escalation_end_event_only_after_its_end_listener_drains() {
    // Companion to the throw case (#1173) for an escalation *end* event carrying
    // an end execution listener: the escalation must be raised only after the
    // listener chain drains, and the catch/continue semantics (boundary → handler,
    // sub-process does not take its normal outgoing flow) must be preserved.
    let def = ProcessBuilder::new("esc-end-listener")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("esc_end", "OVERLOAD")
        .contained_in("esc_end", "sub")
        .escalation_boundary_event("boundary", "sub", "OVERLOAD")
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "sub")
        .connect("sub_start", "esc_end")
        .connect("sub", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .with_listeners(
            "esc_end",
            Vec::new(),
            vec![el(ListenerEventType::End, "esc-audit")],
        )
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-end-listener"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The escalation end event parks on its end listener; the escalation is not
    // raised yet.
    assert!(
        created.iter().all(|e| !matches!(
            e,
            Event::SequenceFlowTaken { from, .. } if from == "boundary"
        )),
        "escalation must not be raised until the end event's listener drains"
    );
    assert!(
        engine
            .pending_jobs()
            .iter()
            .any(|j| j.job_type == "esc-audit"),
        "the escalation end event's end listener job is pending"
    );

    let after = complete_one(&mut engine, "esc-audit");
    // The end event completed and the escalation was caught by the boundary.
    assert!(after.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "esc_end"
    )));
    assert!(after.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));
    // The sub-process did not take its normal outgoing flow.
    assert!(after.iter().all(|e| !matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "done"
    )));

    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_propagate_an_escalation_up_to_an_outer_scope() {
    // An escalation thrown in an inner sub-process with no matching boundary
    // propagates to the nearest enclosing scope that catches it — here the
    // OUTER sub-process's interrupting boundary.
    let def = ProcessBuilder::new("esc-nested")
        .start_event("start")
        .sub_process("outer", "outer_start")
        .start_event("outer_start")
        .contained_in("outer_start", "outer")
        .sub_process("inner", "inner_start")
        .contained_in("inner", "outer")
        .start_event("inner_start")
        .contained_in("inner_start", "inner")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "inner")
        .end_event("inner_end")
        .contained_in("inner_end", "inner")
        .end_event("outer_end")
        .contained_in("outer_end", "outer")
        .escalation_boundary_event("boundary", "outer", "OVERLOAD")
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "outer")
        .connect("outer_start", "inner")
        .connect("inner_start", "throw")
        .connect("throw", "inner_end")
        .connect("inner", "outer_end")
        .connect("outer", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-nested"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "boundary" && to == "handler"
    )));
    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_prefer_an_exact_escalation_code_over_a_catch_all() {
    // Two boundaries on the same sub-process: an exact OVERLOAD match and a
    // catch-all. The exact code wins (Zeebe parity), so only its path runs.
    let def = ProcessBuilder::new("esc-exact")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .escalation_boundary_event("b_exact", "sub", "OVERLOAD")
        .non_interrupting_escalation_boundary_event("b_catchall", "sub", "")
        .service_task("handler_exact", "handle-exact")
        .service_task("handler_catchall", "handle-catchall")
        .end_event("done")
        .end_event("exact_end")
        .end_event("catchall_end")
        .connect("start", "sub")
        .connect("sub_start", "throw")
        .connect("throw", "sub_end")
        .connect("sub", "done")
        .connect("b_exact", "handler_exact")
        .connect("b_catchall", "handler_catchall")
        .connect("handler_exact", "exact_end")
        .connect("handler_catchall", "catchall_end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("esc-exact"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(created.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "b_exact" && to == "handler_exact"
    )));
    assert!(created.iter().all(|e| !matches!(
        e,
        Event::SequenceFlowTaken { from, .. } if from == "b_catchall"
    )));
    complete_one(&mut engine, "handle-exact");
    assert!(engine.is_completed(instance_key));
}

#[test]
fn xml_declared_escalation_boundary_end_listener_fires_end_to_end() {
    // #1197: an `end` execution listener declared in BPMN XML on an ESCALATION
    // boundary event must be parsed AND fire when the boundary triggers — it is
    // re-attached by id at build even though the escalation branch `continue`s
    // the boundary-build loop (the re-attach loop runs separately over a
    // pre-collected vec), and the escalation boundary runs the shared listener
    // gate on completion, deferring the handler route behind the listener job.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss" />
      <bpmn:intermediateThrowEvent id="thr">
        <bpmn:escalationEventDefinition escalationRef="esc" />
      </bpmn:intermediateThrowEvent>
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="a" sourceRef="ss" targetRef="thr" />
      <bpmn:sequenceFlow id="b" sourceRef="thr" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="eb" attachedToRef="sub" cancelActivity="false">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="eb-audit-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:escalationEventDefinition escalationRef="esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="handled" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="done" />
    <bpmn:sequenceFlow id="f3" sourceRef="eb" targetRef="handled" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The non-interrupting escalation boundary triggered on creation (the inner
    // throw raises OVERLOAD). Its parsed end listener parks the handler route.
    assert!(
        created
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "the parsed escalation-boundary end listener must create a listener job"
    );
    assert!(
        !created.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "handled"
        )),
        "escalation boundary must not route to its handler until its end listener completes"
    );

    let audit = engine.activate_jobs("eb-audit-end", "W", 10, 1_000, 0);
    assert_eq!(
        audit.len(),
        1,
        "one escalation-boundary end-listener job from XML"
    );
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "handled"
        )),
        "handler route taken after the parsed escalation-boundary end listener completes"
    );
    let _ = inst;
}
