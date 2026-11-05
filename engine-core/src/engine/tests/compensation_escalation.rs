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

    // Complete the two book jobs one at a time (`activate_jobs` LOCKS what it
    // returns, so a second batch at the same logical instant would not see a
    // job locked by the first). Each completion fires that branch's throw,
    // which rests on its own `cancel` handler instance. The fork takes its
    // flows in Zeebe's reverse document order (#1244), so the branch that
    // activates first is NOT necessarily `bookA` — read back which throw each
    // completion fired instead of assuming the declaration order.
    let first_book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    engine
        .apply_command(Command::complete_job(first_book))
        .unwrap();
    let second_book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    engine
        .apply_command(Command::complete_job(second_book))
        .unwrap();

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

    // Complete the two handler jobs in REVERSE activation order: the second
    // throw's handler finishes while the first throw's is still running. The
    // completion must retire the throw that activated THAT handler instance —
    // an `element_id` + `.min()` lookup instead attributes it to the other
    // throw, which then completes early while its own handler is still
    // running, and the second throw is left waiting on a handler instance
    // that no longer exists. Map each handler job to the throw that activated
    // it through the wait's `pending_handler_keys` (#897), so the scenario
    // does not depend on which branch the fork happened to activate first.
    let cancel_jobs = engine.activate_jobs("cancel-job", "w", 2, 60_000, 0);
    assert_eq!(cancel_jobs.len(), 2, "one handler instance per throw");
    let owner_throw_of = |engine: &Engine, handler_instance_key: Key| {
        engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .iter()
            .find(|(_, w)| w.pending_handler_keys.contains(&handler_instance_key))
            .map(|(k, _)| *k)
            .unwrap_or_else(|| {
                panic!("handler instance {handler_instance_key} has an owning throw")
            })
    };
    // `activate_jobs` plans in priority-descending, key-ascending order, so
    // index 1 is the SECOND-activated handler — completing it first is the
    // reverse-activation-order race this scenario needs.
    let first_done = &cancel_jobs[1];
    let first_throw = owner_throw_of(&engine, first_done.element_instance_key);
    let first_throw_id = engine.instance(instance_key).unwrap().compensation_waits[&first_throw]
        .throw_element_id
        .clone();
    let second_done = &cancel_jobs[0];
    let second_throw = owner_throw_of(&engine, second_done.element_instance_key);
    let second_throw_id = engine.instance(instance_key).unwrap().compensation_waits[&second_throw]
        .throw_element_id
        .clone();
    assert_ne!(
        first_throw, second_throw,
        "the two handler jobs belong to the two distinct throws"
    );

    let first = engine
        .apply_command(Command::complete_job(first_done.key))
        .unwrap();
    assert!(
        first.iter().any(|e| matches!(
            e,
            Event::CompensationHandlerCompleted {
                throw_element_instance_key,
                handler_element_id,
                ..
            } if *throw_element_instance_key == first_throw && handler_element_id == "cancel"
        )),
        "the completed handler retires the throw that activated it"
    );
    assert!(
        first.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. }
                if *from == first_throw_id && to == &format!("done{}", &first_throw_id[5..])
        )),
        "that throw routes onward once its own handler is done"
    );
    assert_eq!(
        throw_of(&engine, &second_throw_id),
        second_throw,
        "the other throw is still waiting on its own (still-running) handler"
    );

    let second = engine
        .apply_command(Command::complete_job(second_done.key))
        .unwrap();
    assert!(second.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted {
            throw_element_instance_key,
            handler_element_id,
            ..
        } if *throw_element_instance_key == second_throw && handler_element_id == "cancel"
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
                cancel_remaining_instances: true,
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
                cancel_remaining_instances: true,
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

/// A compensation wait restored from a pre-#897 snapshot/journal carries no
/// `pending_handler_keys`. Its handlers, completing under the current build,
/// still emit a keyed `CompensationHandlerCompleted`; the reducer must fall back
/// to element-id matching for such an unbound wait, or the throw stays blocked
/// forever after every handler has finished.
#[test]
fn legacy_unbound_wait_completes_when_its_handlers_finish() {
    let def = ProcessBuilder::new("comp-legacy")
        .start_event("s")
        .service_task("bookA", "book-job")
        .compensation_boundary_event("compA", "bookA", "cancelA")
        .service_task("bookB", "book-job")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .compensation_throw_event("throw")
        .end_event("done")
        .service_task("cancelA", "cancel-job")
        .service_task("cancelB", "cancel-job")
        .connect("s", "bookA")
        .connect("bookA", "bookB")
        .connect("bookB", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-legacy"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    for _ in 0..2 {
        let job = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
        engine.apply_command(Command::complete_job(job)).unwrap();
    }
    // Simulate the pre-#897 restored shape: the wait has no handler bindings.
    let instance = engine.state.instances.get_mut(&instance_key).unwrap();
    assert_eq!(instance.compensation_waits.len(), 1);
    for wait in instance.compensation_waits.values_mut() {
        assert_eq!(wait.pending_handlers.len(), 2);
        wait.pending_handler_keys.clear();
    }
    let cancel_jobs = engine.activate_jobs("cancel-job", "w", 2, 60_000, 0);
    assert_eq!(cancel_jobs.len(), 2);
    for job in cancel_jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .is_empty(),
        "the legacy wait drains once both handlers complete"
    );
    assert!(engine.is_completed(instance_key));
}

/// One throw waiting on TWO handlers of the same element id, keyed by handler
/// element-instance key (#897): completing them in REVERSE activation order
/// must retire exactly the completed instance's key each time — the throw stays
/// parked on the still-running handler after the first completion and drains
/// only after the second. (The two-throw test exercises one handler per wait;
/// the legacy test clears the keys; this is the keyed single-throw case.)
#[test]
fn single_throw_with_two_keyed_handlers_completes_in_reverse_order() {
    let def = ProcessBuilder::new("comp-two-handlers")
        .start_event("s")
        .service_task("bookA", "book-job")
        .compensation_boundary_event("compA", "bookA", "cancel")
        .service_task("bookB", "book-job")
        .compensation_boundary_event("compB", "bookB", "cancel")
        .compensation_throw_event("throw")
        .end_event("done")
        .service_task("cancel", "cancel-job")
        .connect("s", "bookA")
        .connect("bookA", "bookB")
        .connect("bookB", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-two-handlers"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    for _ in 0..2 {
        let job = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
        engine.apply_command(Command::complete_job(job)).unwrap();
    }
    let (throw_eik, bound) = {
        let (throw_eik, wait) = engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .iter()
            .next()
            .expect("the throw is waiting on both handlers");
        assert_eq!(wait.pending_handlers, vec!["cancel".to_string(); 2]);
        assert_eq!(wait.pending_handler_keys.len(), 2);
        (*throw_eik, wait.pending_handler_keys.clone())
    };
    // The handler jobs' element-instance keys ARE the keys the throw is bound
    // to; complete them in reverse activation order.
    let cancel_jobs = engine.activate_jobs("cancel-job", "w", 2, 60_000, 0);
    assert_eq!(
        cancel_jobs.len(),
        2,
        "one handler instance per subscription"
    );
    assert_eq!(
        cancel_jobs
            .iter()
            .map(|j| j.element_instance_key)
            .collect::<std::collections::BTreeSet<_>>(),
        bound
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        "the wait is bound to exactly the two activated handler instances"
    );

    // First completion (the SECOND-activated handler): exactly its key is
    // retired; the throw stays parked on the first handler's key.
    let events = engine
        .apply_command(Command::complete_job(cancel_jobs[1].key))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted {
            throw_element_instance_key,
            handler_element_instance_key: Some(k),
            ..
        } if *throw_element_instance_key == throw_eik && *k == cancel_jobs[1].element_instance_key
    )));
    let wait = &engine.instance(instance_key).unwrap().compensation_waits[&throw_eik];
    assert_eq!(
        wait.pending_handler_keys,
        vec![cancel_jobs[0].element_instance_key],
        "only the completed handler's key was retired"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "throw")),
        "the throw is still waiting on the first handler"
    );

    // Second completion drains the wait and routes the throw onward.
    let events = engine
        .apply_command(Command::complete_job(cancel_jobs[0].key))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .is_empty());
    assert!(engine.is_completed(instance_key));
}

/// A `ModifyInstance` that terminates (or replaces) a compensation handler a
/// throw event is still waiting on would leave the throw bound to the
/// terminated handler's element-instance key forever: the replacement gets a
/// NEW key, so its completion retires nothing (#897). The modification is
/// rejected outright — there is no binding-replacement event to re-point the
/// wait at — and, being validated before anything is emitted, applies nothing.
#[test]
fn modify_rejects_terminating_a_handler_a_throw_is_waiting_on() {
    let def = ProcessBuilder::new("comp-modify")
        .start_event("s")
        .service_task("book", "book-job")
        .compensation_boundary_event("comp", "book", "cancel")
        .compensation_throw_event("throw")
        .end_event("done")
        .service_task("cancel", "cancel-job")
        .connect("s", "book")
        .connect("book", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-modify"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    engine.apply_command(Command::complete_job(book)).unwrap();
    let handler = engine.activate_jobs("cancel-job", "w", 1, 60_000, 0)[0].clone();
    let handler_eik = handler.element_instance_key;
    assert!(engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .values()
        .any(|w| w.pending_handler_keys == vec![handler_eik]));

    // The replacement attempt: terminate the waiting handler and activate a
    // fresh one in its place. Rejected — the throw's binding would dangle.
    let err = engine
        .apply_command(Command::modify_instance(
            instance_key,
            vec![ActivateElementInstruction::new("cancel")],
            vec![handler_eik],
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::ModificationInstructionInvalid { .. }),
        "terminating a handler a throw waits on is rejected: {err:?}"
    );
    // A bare terminate (no replacement) is rejected too, and by-id termination
    // resolves to the same bound instance, so it is rejected as well.
    for cmd in [
        Command::modify_instance(instance_key, vec![], vec![handler_eik]),
        Command::ModifyInstance {
            instance_key,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["cancel".to_string()],
            move_instructions: vec![],
        },
    ] {
        assert!(
            matches!(
                engine.apply_command(cmd),
                Err(EngineError::ModificationInstructionInvalid { .. })
            ),
            "every terminate path that reaches the bound handler is rejected"
        );
    }

    // The rejection applied nothing: the handler is still active, the wait is
    // intact, and completing the handler still drains the throw normally.
    let inst = engine.instance(instance_key).unwrap();
    assert_eq!(inst.compensation_waits.len(), 1);
    assert!(
        inst.compensation_waits
            .values()
            .next()
            .unwrap()
            .pending_handler_keys
            == vec![handler_eik]
    );
    engine
        .apply_command(Command::complete_job(handler.key))
        .unwrap();
    assert!(engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .is_empty());
    assert!(engine.is_completed(instance_key));
}

/// Defect class (#897, multi-instance variant): a compensation handler activity
/// that carries multi-instance loop characteristics runs as an MI *body*, not a
/// single activity. The throw mints the handler's element-instance key BEFORE
/// activation and journals it in `CompensationTriggered::handler_instance_keys`;
/// if the MI activation then mints a DIFFERENT body key, the persisted wait binds
/// a key no handler activation ever uses, and the body's completion (which takes
/// the activity's outgoing flows) never routes back to the throw — the throw
/// waits forever and the instance hangs Active.
///
/// The fix carries the pre-minted key through as the MI body key and routes the
/// body's completion back to the waiting throw (by that instance key).
///
/// Topology: start -> book(svc, comp-boundary -> cancel(svc, MI over `items`))
///                  -> throw -> done
#[test]
fn multi_instance_compensation_handler_completion_retires_the_throw_wait() {
    let def = ProcessBuilder::new("mi-comp")
        .start_event("s")
        .service_task("book", "book-job")
        .compensation_boundary_event("book-comp", "book", "cancel")
        .service_task("cancel", "cancel-job")
        .with_multi_instance(
            "cancel",
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
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("s", "book")
        .connect("book", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi-comp",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete the compensable activity: the throw fires and activates the
    // multi-instance handler (its body fans out one `cancel-job` child per item).
    let book = engine.activate_jobs("book-job", "w", 1, 60_000, 0)[0].key;
    let events = engine.apply_command(Command::complete_job(book)).unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationTriggered { throw_element_id, .. } if throw_element_id == "throw"
    )));
    // The throw is resting on its handler; nothing routed onward yet.
    assert!(!engine.is_completed(instance_key));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done")));
    // The wait binds the handler instance the throw activated — the MI body.
    {
        let inst = engine.instance(instance_key).unwrap();
        assert_eq!(inst.compensation_waits.len(), 1);
        let wait = inst.compensation_waits.values().next().unwrap();
        assert_eq!(wait.pending_handlers, vec!["cancel".to_string()]);
        assert_eq!(wait.pending_handler_keys.len(), 1);
    }

    // Run both MI children to completion. Each completes back into the loop; the
    // LAST one completes the body, which must retire the throw's wait and route
    // the throw onward to `done`. Activate BOTH child jobs up front (a parallel
    // body fans all children out at once).
    let cancels = engine.activate_jobs("cancel-job", "w", 10, 60_000, 0);
    assert_eq!(
        cancels.len(),
        2,
        "parallel body fans out one child per item"
    );
    engine
        .apply_command(Command::complete_job(cancels[0].key))
        .unwrap();
    assert!(
        !engine.is_completed(instance_key),
        "one of two handler children done: the throw must still be waiting"
    );
    let events = engine
        .apply_command(Command::complete_job(cancels[1].key))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted { handler_element_id, .. }
            if handler_element_id == "cancel"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .is_empty(),
        "the handler body's completion must retire the throw's wait"
    );
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

/// #901 (Zeebe `BpmnCompensationSubscriptionBehaviour.triggerCompensation`): a
/// compensation throw with no `activityRef` compensates the WHOLE scope it runs
/// in — every compensable activity recorded in that scope, not only the ones
/// sitting directly in it. A completed sub-process that contains compensable
/// activities records its own (handlerless) *scope subscription*; the throw
/// triggers it and, through it, every subscription nested inside
/// (`triggerCompensationFromTopToBottom`), running each activity's handler. The
/// throw rests until every triggered handler has completed.
///
/// Topology (mirrors Zeebe's `compensation-embedded-subprocess.bpmn` /
/// `shouldTriggerCompensationHandlerInSubprocesses`):
///
/// ```text
/// start -> sub[ sub_start -> split =< bookA(svc, comp -> cancelA),
///                                   sub2[ sub2_start -> bookB(svc, comp -> cancelB) -> sub2_end ] >
///                join -> sub_end ]
///       -> throw(compensation) -> done
/// ```
// zeebe-cells: event:boundary:compensation event:intermediate-throw:compensation
#[test]
fn whole_scope_throw_triggers_handlers_in_nested_subprocesses() {
    let def = ProcessBuilder::new("comp-whole-scope")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("bookA", "book-a-job")
        .contained_in("bookA", "sub")
        .compensation_boundary_event("compA", "bookA", "cancelA")
        .contained_in("compA", "sub")
        .service_task("cancelA", "cancel-a-job")
        .contained_in("cancelA", "sub")
        .sub_process("sub2", "sub2_start")
        .contained_in("sub2", "sub")
        .start_event("sub2_start")
        .contained_in("sub2_start", "sub2")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "sub2")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .contained_in("compB", "sub2")
        .service_task("cancelB", "cancel-b-job")
        .contained_in("cancelB", "sub2")
        .end_event("sub2_end")
        .contained_in("sub2_end", "sub2")
        .parallel_gateway("join")
        .contained_in("join", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "bookA")
        .connect("bookA", "join")
        .connect("split", "sub2")
        .connect("sub2_start", "bookB")
        .connect("bookB", "sub2_end")
        .connect("sub2", "join")
        .connect("join", "sub_end")
        .connect("sub", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-whole-scope"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete both compensable activities (bookA in `sub`, bookB in the nested
    // `sub2`). Each records its subscription in its OWN scope; each enclosing
    // sub-process that holds a subscription records its own scope subscription
    // on completion (Zeebe `isFlowScopeWithSubscriptions`). The sub-process
    // completing routes the token to the throw, which must trigger BOTH
    // handlers — bookB's nested one included — through the scope subscriptions.
    complete_one(&mut engine, "book-a-job");
    assert!(
        !engine.is_completed(instance_key),
        "the parallel sibling branch is still running"
    );
    let events = complete_one(&mut engine, "book-b-job");
    // Trigger order is top-to-bottom: [sub, sub2].
    let scope_subs: Vec<Key> = events
        .iter()
        .find_map(|e| match e {
            Event::CompensationTriggered {
                scope_subscriptions,
                ..
            } => Some(scope_subscriptions.clone()),
            _ => None,
        })
        .expect("the throw fires");
    assert_eq!(scope_subs.len(), 2, "sub and the nested sub2 are triggered");
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::CompensationTriggered { throw_element_id, handlers, .. }
                if throw_element_id == "throw"
                    && handlers.len() == 2
                    && handlers.iter().any(|h| h == "cancelA")
                    && handlers.iter().any(|h| h == "cancelB")
        )),
        "the whole-scope throw must trigger BOTH in-scope handlers, including the nested one"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "throw")),
        "the throw rests until every triggered handler completes"
    );

    // Both handler jobs are live; completing only the first must NOT route the
    // throw onward.
    let mut cancels = engine.activate_jobs("cancel-a-job", "w", 10, 60_000, 0);
    cancels.extend(engine.activate_jobs("cancel-b-job", "w", 10, 60_000, 0));
    assert_eq!(cancels.len(), 2, "one handler job per compensable activity");
    let events = engine
        .apply_command(Command::complete_job(cancels[0].key))
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "throw")),
        "one handler still outstanding: the throw must keep waiting"
    );
    // cancelB (inside sub2, inside sub) is still running, so NEITHER scope
    // subscription may be reported drained yet (review r4477477377).
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::CompensationScopeSubscriptionCompleted { .. })),
        "a scope subscription must not complete while a handler inside it is running"
    );

    // Completing the second handler retires the wait and routes the throw
    // onward; the instance finishes.
    let events = engine
        .apply_command(Command::complete_job(cancels[1].key))
        .unwrap();
    // Bottom-to-top: the innermost scope (sub2) retires before its parent (sub).
    let retired: Vec<Key> = events
        .iter()
        .filter_map(|e| match e {
            Event::CompensationScopeSubscriptionCompleted {
                scope_subscription_key,
                ..
            } => Some(*scope_subscription_key),
            _ => None,
        })
        .collect();
    let mut expected = scope_subs.clone();
    expected.reverse();
    assert_eq!(
        retired, expected,
        "scope subscriptions retire bottom-to-top"
    );
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .is_empty(),
        "no throw is left waiting once every handler completed"
    );
    assert!(
        engine
            .instance(instance_key)
            .unwrap()
            .compensable
            .is_empty(),
        "every subscription in the compensated scope was consumed"
    );
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

/// #901 ordering: within one scope the handlers run in subscription-creation
/// order (Zeebe iterates its compensation-subscription column family by record
/// key — the order the activities completed and recorded their subscriptions) —
/// and the throw still rests until ALL of them complete.
///
/// ```text
/// start -> bookA(svc, comp -> cancelA) -> bookB(svc, comp -> cancelB)
///       -> throw(compensation) -> done
/// ```
#[test]
fn whole_scope_throw_runs_handlers_in_creation_order_and_rests_for_all() {
    let def = ProcessBuilder::new("comp-scope-order")
        .start_event("s")
        .service_task("bookA", "book-a-job")
        .compensation_boundary_event("compA", "bookA", "cancelA")
        .service_task("cancelA", "cancel-a-job")
        .service_task("bookB", "book-b-job")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .service_task("cancelB", "cancel-b-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("s", "bookA")
        .connect("bookA", "bookB")
        .connect("bookB", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-scope-order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    complete_one(&mut engine, "book-a-job");
    let events = complete_one(&mut engine, "book-b-job");
    let triggered = events
        .iter()
        .find_map(|e| match e {
            Event::CompensationTriggered {
                throw_element_id,
                handlers,
                ..
            } if throw_element_id == "throw" => Some(handlers.clone()),
            _ => None,
        })
        .expect("the throw triggers both handlers");
    assert_eq!(
        triggered,
        vec!["cancelA".to_string(), "cancelB".to_string()],
        "handlers run in subscription-creation order (Zeebe column-family order)"
    );

    // Both handler jobs are live at once (the whole scope is triggered, not one
    // activity per throw): one `cancel-a-job` and one `cancel-b-job`.
    let cancel_a_jobs = engine.activate_jobs("cancel-a-job", "w", 10, 60_000, 0);
    assert_eq!(cancel_a_jobs.len(), 1, "cancelA was activated");
    let cancel_b_jobs = engine.activate_jobs("cancel-b-job", "w", 10, 60_000, 0);
    assert_eq!(cancel_b_jobs.len(), 1, "cancelB was activated");
    // The throw rests until BOTH handlers have completed: it has not routed
    // onward yet, and its wait tracks both outstanding handler instances.
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "throw")),
        "the throw rests until every triggered handler completes"
    );
    let wait = engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .values()
        .next()
        .expect("the throw is waiting on its handlers")
        .clone();
    assert_eq!(wait.pending_handlers.len(), 2);

    // Completing the first (newest) handler alone does not route it onward.
    let cancel_b = cancel_b_jobs[0].key;
    let events = engine
        .apply_command(Command::complete_job(cancel_b))
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "throw")),
        "cancelA is still outstanding: the throw must keep waiting"
    );
    assert!(!engine.is_completed(instance_key));

    let cancel_a = cancel_a_jobs[0].key;
    let events = engine
        .apply_command(Command::complete_job(cancel_a))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(engine.is_completed(instance_key));
}

/// Defect-class guard for the whole-scope wait (#901): a throw that fans out
/// into a NESTED scope tracks not just its handler instances but the in-flight
/// *scope subscriptions* it triggered. A scoped terminate that tears down the
/// nested scope while the throw is still waiting must retire the throw's whole
/// wait — otherwise the throw would stay parked on a scope that no longer
/// exists and the instance would hang `Active` forever.
///
/// ```text
/// start -> split =< sub[ sub_start -> bookB(svc, comp -> cancelB) -> sub_end ],
///                   killer(svc) -> throw(compensation) -> stop(terminate) >
/// ```
/// `bookB` completes (recording its subscription inside `sub`), then the
/// sibling branch drives the root token into the compensation throw: the throw
/// triggers `cancelB` THROUGH the sub-process's scope subscription and rests on
/// it. Cancelling the instance while that handler is still running must not
/// strand the throw's wait.
#[test]
fn terminate_while_a_whole_scope_throw_is_in_flight_leaves_no_wait_behind() {
    let def = ProcessBuilder::new("comp-throw-terminated")
        .start_event("start")
        .parallel_gateway("split")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "sub")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .contained_in("compB", "sub")
        .service_task("cancelB", "cancel-b-job")
        .contained_in("cancelB", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .service_task("killer", "killer-job")
        .terminate_end_event("stop")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "split")
        .connect("split", "sub")
        .connect("sub_start", "bookB")
        .connect("bookB", "sub_end")
        .connect("split", "killer")
        .connect("killer", "throw")
        .connect("throw", "stop")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-throw-terminated"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete the compensable activity inside `sub`: its subscription is
    // recorded in the sub-process scope, and `sub` records its own scope
    // subscription when it drains.
    complete_one(&mut engine, "book-b-job");
    // The sibling branch drives the root token into the compensation throw:
    // the throw triggers cancelB THROUGH the sub-process scope subscription and
    // rests on it.
    let events = complete_one(&mut engine, "killer-job");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationTriggered { throw_element_id, handlers, .. }
            if throw_element_id == "throw" && handlers == &vec!["cancelB".to_string()]
    )));
    assert!(
        !engine.is_completed(instance_key),
        "the throw rests on the in-flight handler"
    );

    // Now cancel the whole instance while the throw is still resting on the
    // in-flight handler: the in-flight compensation must not strand the wait.
    let events = engine
        .apply_command(Command::CancelInstance { instance_key })
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(
        engine.instance(instance_key).is_none()
            || engine
                .instance(instance_key)
                .unwrap()
                .compensation_waits
                .is_empty(),
        "no compensation wait survives the terminate"
    );
}

/// #901 scope isolation (Zeebe `shouldNotTriggerCompensationOnParentScope`): a
/// throw inside a sub-process scope compensates only THAT scope — the
/// already-completed compensable activity in the PARENT scope is untouched and
/// its handler never runs.
///
/// ```text
/// start -> bookA(svc, comp -> cancelA)          (root scope)
///       -> sub[ sub_start -> bookB(svc, comp -> cancelB) -> throw(comp) -> sub_end ]
///       -> done
/// ```
#[test]
fn throw_inside_a_subprocess_compensates_only_its_own_scope() {
    let def = ProcessBuilder::new("comp-scope-isolation")
        .start_event("start")
        .service_task("bookA", "book-a-job")
        .compensation_boundary_event("compA", "bookA", "cancelA")
        .service_task("cancelA", "cancel-a-job")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "sub")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .contained_in("compB", "sub")
        .service_task("cancelB", "cancel-b-job")
        .contained_in("cancelB", "sub")
        .compensation_throw_event("throw")
        .contained_in("throw", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "bookA")
        .connect("bookA", "sub")
        .connect("sub_start", "bookB")
        .connect("bookB", "throw")
        .connect("throw", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-scope-isolation"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    complete_one(&mut engine, "book-a-job");
    let events = complete_one(&mut engine, "book-b-job");
    let triggered = events
        .iter()
        .find_map(|e| match e {
            Event::CompensationTriggered {
                throw_element_id,
                handlers,
                ..
            } if throw_element_id == "throw" => Some(handlers.clone()),
            _ => None,
        })
        .expect("the inner throw fires");
    assert_eq!(
        triggered,
        vec!["cancelB".to_string()],
        "only the in-scope handler is triggered; the parent-scope activity is not compensable from here"
    );

    // Completing the inner handler drains the inner throw; the sub-process
    // drains and completes, and the instance finishes WITHOUT ever activating
    // cancelA. bookA's subscription is consumed by the sub-process's own
    // completion (Zeebe deletes a flow scope's subscriptions when the scope
    // finishes uncompensated) — critically, WITHOUT running its handler.
    let cancel_b = engine.activate_jobs("cancel-b-job", "w", 1, 60_000, 0)[0].key;
    let events = engine
        .apply_command(Command::complete_job(cancel_b))
        .unwrap();
    assert!(
        events.contains(&Event::ProcessInstanceCompleted { instance_key }),
        "the sub-process completes and the instance finishes"
    );
    assert!(
        engine
            .activate_jobs("cancel-a-job", "w", 1, 60_000, 0)
            .is_empty(),
        "the parent-scope handler never ran"
    );
}

/// Review r4477477333 (#901): a whole-scope throw whose ONLY handler sits in a
/// nested sub-process must still resume when that sole handler completes — the
/// scope subscription drains in the same batch, so the throw completes rather
/// than being left parked with no wait to resume it.
///
/// ```text
/// start -> sub[ sub_start -> bookB(svc, comp -> cancelB) -> sub_end ]
///       -> throw(compensation) -> done
/// ```
#[test]
fn whole_scope_throw_resumes_when_its_sole_nested_handler_completes() {
    let def = ProcessBuilder::new("comp-sole-nested")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "sub")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .contained_in("compB", "sub")
        .service_task("cancelB", "cancel-b-job")
        .contained_in("cancelB", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "bookB")
        .connect("bookB", "sub_end")
        .connect("sub", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-sole-nested"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    complete_one(&mut engine, "book-b-job");
    assert!(
        !engine.is_completed(instance_key),
        "the throw rests on cancelB"
    );
    let events = complete_one(&mut engine, "cancel-b-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::CompensationScopeSubscriptionCompleted { .. })));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "throw" && to == "done"
    )));
    assert!(engine.is_completed(instance_key));
    assert!(engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .is_empty());
}

/// Review r4477477412 (#901): `ScopedCompensationCleared` on a SURVIVING
/// parent wait must drop only the torn-down scope-subscription reference
/// (keeping its parent alignment), preserve unrelated outstanding work, and
/// drop a wait it leaves with nothing outstanding. Exercised on the reducer
/// directly (and via replay into a fresh state) — whole-instance cancel clears
/// every wait regardless, so it cannot witness this path.
#[test]
fn scoped_compensation_clear_retires_only_the_torn_down_scope_reference() {
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "work-job")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let out = engine.apply_command(Command::create_instance("p")).unwrap();
    let instance_key = out.iter().find_map(|e| e.instance_key()).unwrap();
    let mut state = engine.state.clone();
    let created = engine.state.clone();
    assert!(state.instances.contains_key(&instance_key));
    let trigger = |throw: Key, handlers: Vec<&str>, keys: Vec<Key>, subs: Vec<Key>| {
        Event::CompensationTriggered {
            instance_key,
            throw_element_instance_key: throw,
            throw_element_id: format!("throw{throw}"),
            scope: 0,
            handlers: handlers.into_iter().map(String::from).collect(),
            consumed: vec![],
            handler_scopes: vec![0; keys.len()],
            handler_instance_keys: keys,
            scope_subscription_parents: vec![0; subs.len()],
            scope_subscriptions: subs,
        }
    };
    // Wait 10: rests on a handler AND scope subs 50 (torn down) + 60 (kept).
    // Wait 20: rests ONLY on scope sub 50 (torn down) -> emptied -> dropped.
    let journal = vec![
        trigger(10, vec!["h"], vec![11], vec![50, 60]),
        trigger(20, vec![], vec![], vec![50]),
        Event::ScopedCompensationCleared {
            instance_key,
            scopes: vec![50],
        },
    ];
    for event in &journal {
        crate::state::apply(&mut state, event);
    }
    let check = |state: &State| {
        let waits = &state.instances[&instance_key].compensation_waits;
        assert!(!waits.contains_key(&20), "an emptied wait is dropped");
        let w = waits.get(&10).expect("the surviving wait is kept");
        assert_eq!(w.pending_scope_subscriptions, vec![60]);
        assert_eq!(w.pending_scope_subscription_parents, vec![0]);
        assert_eq!(w.pending_handlers, vec!["h".to_string()]);
        assert_eq!(w.pending_handler_keys, vec![11]);
    };
    check(&state);
    // Replay parity: the same journal over the pre-trigger state again.
    let mut replayed = created;
    for event in &journal {
        crate::state::apply(&mut replayed, event);
    }
    check(&replayed);
}

/// Review r4477622238 (#901): a COMPLETED multi-instance sub-process must
/// record scope subscriptions for each child (in the body's scope) and for the
/// body (in its own scope), so a later whole-scope throw reaches the handlers
/// nested inside every child. The MI child / body completion paths bypass the
/// plain sub-process completion that records them.
///
/// ```text
/// start -> each*[ sub_start -> bookB(svc, comp -> cancelB) -> sub_end ]
///       -> throw(compensation) -> done
/// ```
#[test]
fn whole_scope_throw_reaches_handlers_inside_a_completed_multi_instance_subprocess() {
    for sequential in [true, false] {
        let def = ProcessBuilder::new("comp-mi-sub")
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
                    sequential,
                },
            )
            .start_event("sub_start")
            .contained_in("sub_start", "each")
            .service_task("bookB", "book-b-job")
            .contained_in("bookB", "each")
            .compensation_boundary_event("compB", "bookB", "cancelB")
            .contained_in("compB", "each")
            .service_task("cancelB", "cancel-b-job")
            .contained_in("cancelB", "each")
            .end_event("sub_end")
            .contained_in("sub_end", "each")
            .compensation_throw_event("throw")
            .end_event("done")
            .connect("start", "each")
            .connect("sub_start", "bookB")
            .connect("bookB", "sub_end")
            .connect("each", "throw")
            .connect("throw", "done")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let mut items = HashMap::new();
        items.insert(
            "items".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        );
        let created = engine
            .apply_command(Command::create_instance_with("comp-mi-sub", items))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        let activate_all = |engine: &mut Engine, job_type: &str| -> Vec<Key> {
            engine
                .activate_jobs(job_type, "test-worker", 10, 60_000, 0)
                .into_iter()
                .map(|j| j.key)
                .collect()
        };
        // Sequential children run one at a time, parallel ones together.
        loop {
            let jobs = activate_all(&mut engine, "book-b-job");
            if jobs.is_empty() {
                break;
            }
            for job in jobs {
                engine.apply_command(Command::complete_job(job)).unwrap();
            }
        }
        assert!(
            !engine.is_completed(instance_key),
            "sequential={sequential}: the throw must rest on both nested cancelB handlers"
        );
        let handlers = activate_all(&mut engine, "cancel-b-job");
        assert_eq!(
            handlers.len(),
            2,
            "sequential={sequential}: one cancelB per completed child"
        );
        engine
            .apply_command(Command::complete_job(handlers[0]))
            .unwrap();
        assert!(!engine.is_completed(instance_key));
        engine
            .apply_command(Command::complete_job(handlers[1]))
            .unwrap();
        assert!(engine.is_completed(instance_key), "sequential={sequential}");
        assert!(engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .is_empty());
    }
}

/// Review r4477754066 (#901): a sub-process with its OWN compensation handler
/// (explicit boundary) must not also record a handlerless scope subscription —
/// the explicit handler overrides the nested ones, so a whole-scope throw runs
/// only `cancelSub`, never the nested `cancelB`.
#[test]
fn whole_scope_throw_honours_an_explicit_subprocess_handler_over_nested_ones() {
    let def = ProcessBuilder::new("comp-explicit-sub")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "sub")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .contained_in("compB", "sub")
        .service_task("cancelB", "cancel-b-job")
        .contained_in("cancelB", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .compensation_boundary_event("compSub", "sub", "cancelSub")
        .service_task("cancelSub", "cancel-sub-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "bookB")
        .connect("bookB", "sub_end")
        .connect("sub", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-explicit-sub"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let events = complete_one(&mut engine, "book-b-job");
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::CompensationSubscriptionCreated {
                scope_subscription: true,
                ..
            }
        )),
        "an explicitly-handled sub-process records no scope subscription"
    );
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ElementActivated { element_id, .. } if element_id == "cancelB"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ElementActivated { element_id, .. } if element_id == "cancelSub"
    )));
    complete_one(&mut engine, "cancel-sub-job");
    assert!(engine.is_completed(instance_key));
}

/// Review r4477894358 (#901): a completed MULTI-INSTANCE sub-process whose
/// activity carries its OWN compensation handler must record that explicit
/// boundary subscription when the BODY completes — the body completion paths
/// bypass the plain sub-process registration, and `scope_subscription_event`
/// returns `None` for an explicitly-handled element, so without routing through
/// `subprocess_compensation_subscriptions` a later whole-scope throw skips the
/// handler entirely. Covers the listener-free body completion
/// (`complete_multi_instance_body`), sequential and parallel.
///
/// ```text
/// start -> each*[ sub_start -> bookB(svc) -> sub_end ]  (compSub on `each` -> cancelSub)
///       -> throw(compensation) -> done
/// ```
#[test]
fn whole_scope_throw_reaches_the_explicit_handler_of_a_completed_multi_instance_subprocess() {
    for sequential in [true, false] {
        let def = ProcessBuilder::new("comp-mi-explicit")
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
                    sequential,
                },
            )
            .start_event("sub_start")
            .contained_in("sub_start", "each")
            .service_task("bookB", "book-b-job")
            .contained_in("bookB", "each")
            .end_event("sub_end")
            .contained_in("sub_end", "each")
            .compensation_boundary_event("compSub", "each", "cancelSub")
            .service_task("cancelSub", "cancel-sub-job")
            .compensation_throw_event("throw")
            .end_event("done")
            .connect("start", "each")
            .connect("sub_start", "bookB")
            .connect("bookB", "sub_end")
            .connect("each", "throw")
            .connect("throw", "done")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let created = engine
            .apply_command(Command::create_instance_with(
                "comp-mi-explicit",
                HashMap::from([(
                    "items".to_string(),
                    Value::List(vec![Value::Int(1), Value::Int(2)]),
                )]),
            ))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

        // Run both children to completion; the body then completes and must
        // record the explicit `cancelSub` boundary subscription (never a
        // handlerless scope subscription — the explicit handler overrides).
        loop {
            let jobs: Vec<Key> = engine
                .activate_jobs("book-b-job", "test-worker", 10, 60_000, 0)
                .into_iter()
                .map(|j| j.key)
                .collect();
            if jobs.is_empty() {
                break;
            }
            for job in jobs {
                engine.apply_command(Command::complete_job(job)).unwrap();
            }
        }
        assert!(
            !engine.is_completed(instance_key),
            "sequential={sequential}: the throw must rest on the body's cancelSub handler"
        );
        let handlers = engine.activate_jobs("cancel-sub-job", "test-worker", 10, 60_000, 0);
        assert_eq!(
            handlers.len(),
            1,
            "sequential={sequential}: the explicit handler runs exactly once for the body"
        );
        engine
            .apply_command(Command::complete_job(handlers[0].key))
            .unwrap();
        assert!(engine.is_completed(instance_key), "sequential={sequential}");
        assert!(engine
            .instance(instance_key)
            .unwrap()
            .compensation_waits
            .is_empty());
    }
}

/// Review r4477894358 (#901), sibling sweep: a multi-instance CHILD carrying
/// its own compensation boundary records the explicit handler subscription (in
/// the body's scope) on completion — never the handlerless scope subscription
/// alongside it — so a later whole-scope throw runs the child's handler once
/// per completed child and never fans out into the child's nested handlers.
///
/// ```text
/// start -> each*[ sub_start -> bookB(svc, comp -> cancelB) -> sub_end ]
///         (compChild on `each` -> cancelChild) -> throw(compensation) -> done
/// ```
#[test]
fn whole_scope_throw_runs_an_explicit_handler_on_each_completed_mi_child() {
    let def = ProcessBuilder::new("comp-mi-child-explicit")
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
                sequential: false,
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "each")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "each")
        .compensation_boundary_event("compB", "bookB", "cancelB")
        .contained_in("compB", "each")
        .service_task("cancelB", "cancel-b-job")
        .contained_in("cancelB", "each")
        .end_event("sub_end")
        .contained_in("sub_end", "each")
        .compensation_boundary_event("compChild", "each", "cancelChild")
        .service_task("cancelChild", "cancel-child-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "each")
        .connect("sub_start", "bookB")
        .connect("bookB", "sub_end")
        .connect("each", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "comp-mi-child-explicit",
            HashMap::from([(
                "items".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let children: Vec<Key> = engine
        .activate_jobs("book-b-job", "test-worker", 10, 60_000, 0)
        .into_iter()
        .map(|j| j.key)
        .collect();
    assert_eq!(children.len(), 2);
    let mut child_subscriptions = 0usize;
    let mut scope_subscriptions = 0usize;
    for job in children {
        let events = engine.apply_command(Command::complete_job(job)).unwrap();
        for event in &events {
            match event {
                // The child's explicit boundary subscription, recorded in the
                // BODY's scope — one per completed child.
                Event::CompensationSubscriptionCreated {
                    handler,
                    scope,
                    scope_subscription: false,
                    ..
                } if handler == "cancelChild" && *scope != 0 => child_subscriptions += 1,
                // The explicit handler overrides the nested ones: no child
                // ever records a handlerless scope subscription.
                Event::CompensationSubscriptionCreated {
                    scope_subscription: true,
                    ..
                } => scope_subscriptions += 1,
                _ => {}
            }
        }
    }
    assert_eq!(
        child_subscriptions, 2,
        "each completed child records its explicit handler subscription"
    );
    assert_eq!(scope_subscriptions, 0);

    // The whole-scope throw compensates the body through its OWN explicit
    // handler exactly once: a scope carrying an explicit handler compensates
    // only through it (BPMN 2.0 §10.6.5) — the throw never reaches past it
    // into the children's subscriptions, so neither a second cancelChild nor
    // the nested cancelB is activated.
    let handlers = engine.activate_jobs("cancel-child-job", "test-worker", 10, 60_000, 0);
    assert_eq!(
        handlers.len(),
        1,
        "the body's explicit handler compensates the whole activity once"
    );
    assert!(engine
        .activate_jobs("cancel-b-job", "test-worker", 10, 60_000, 0)
        .is_empty());
    engine
        .apply_command(Command::complete_job(handlers[0].key))
        .unwrap();
    assert!(engine.is_completed(instance_key));
    assert!(engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .is_empty());
}

/// Review r4477894358 (#901): the END-LISTENER body completion path
/// (`finalize_multi_instance_body`) must register the explicit handler the same
/// way — the body parks in COMPLETING while its `end` chain drains, so the
/// subscription is journaled only when the deferred completion fires.
#[test]
fn whole_scope_throw_reaches_the_explicit_handler_after_the_mi_body_end_listener_drains() {
    let def = ProcessBuilder::new("comp-mi-explicit-listener")
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
                sequential: false,
            },
        )
        .with_listeners(
            "each",
            Vec::new(),
            vec![el(ListenerEventType::End, "mi-audit")],
        )
        .start_event("sub_start")
        .contained_in("sub_start", "each")
        .service_task("bookB", "book-b-job")
        .contained_in("bookB", "each")
        .end_event("sub_end")
        .contained_in("sub_end", "each")
        .compensation_boundary_event("compSub", "each", "cancelSub")
        .service_task("cancelSub", "cancel-sub-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "each")
        .connect("sub_start", "bookB")
        .connect("bookB", "sub_end")
        .connect("each", "throw")
        .connect("throw", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "comp-mi-explicit-listener",
            HashMap::from([(
                "items".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Activate both children up front: completing the first child re-locks the
    // sibling's job, so a complete-one/activate-one loop starves the second.
    let children: Vec<Key> = engine
        .activate_jobs("book-b-job", "test-worker", 10, 60_000, 0)
        .into_iter()
        .map(|j| j.key)
        .collect();
    assert_eq!(children.len(), 2);
    for job in children {
        engine.apply_command(Command::complete_job(job)).unwrap();
    }
    // Both children done: the body parks in COMPLETING on its end listener —
    // the throw has not fired and no handler subscription exists yet.
    assert!(engine
        .pending_jobs()
        .iter()
        .any(|j| j.job_type == "mi-audit"));
    assert!(!engine
        .pending_jobs()
        .iter()
        .any(|j| j.job_type == "cancel-sub-job"));

    // Draining the end listener fires the deferred body completion, which must
    // record the explicit boundary subscription; the throw then activates it.
    complete_one(&mut engine, "mi-audit");
    let handler = complete_one(&mut engine, "cancel-sub-job");
    assert!(handler.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted { handler_element_id, .. }
            if handler_element_id == "cancelSub"
    )));
    assert!(engine.is_completed(instance_key));
    assert!(engine
        .instance(instance_key)
        .unwrap()
        .compensation_waits
        .is_empty());
}

/// Review r4478182821 (#901): a compensation handler that is itself an
/// embedded sub-process must, on completion, retire the throw's wait (routing
/// through `resume_compensation_throw`) — on both the listener-free drained
/// path and the end-listener (`finalize_subprocess`) path.
///
/// ```text
/// start -> book(svc, comp -> undo[ u_start -> undoTask -> u_end ]) -> throw -> done
/// ```
fn sub_process_handler_def(with_end_listener: bool) -> crate::model::ProcessDefinition {
    let mut b = ProcessBuilder::new("comp-sub-handler")
        .start_event("start")
        .service_task("book", "book-job")
        .compensation_boundary_event("comp", "book", "undo")
        .sub_process("undo", "u_start")
        .start_event("u_start")
        .contained_in("u_start", "undo")
        .service_task("undoTask", "undo-job")
        .contained_in("undoTask", "undo")
        .end_event("u_end")
        .contained_in("u_end", "undo")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "book")
        .connect("book", "throw")
        .connect("u_start", "undoTask")
        .connect("undoTask", "u_end")
        .connect("throw", "done");
    if with_end_listener {
        b = b.with_listeners(
            "undo",
            Vec::new(),
            vec![el(ListenerEventType::End, "undo-audit")],
        );
    }
    b.build().unwrap()
}

fn run_sub_process_handler(with_end_listener: bool) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(sub_process_handler_def(
            with_end_listener,
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("comp-sub-handler"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    complete_one(&mut engine, "book-job");
    let mut events = complete_one(&mut engine, "undo-job");
    if with_end_listener {
        assert!(!engine.is_completed(instance_key));
        events = complete_one(&mut engine, "undo-audit");
    }
    assert!(events.iter().any(|e| matches!(
        e,
        Event::CompensationHandlerCompleted { handler_element_id, .. }
            if handler_element_id == "undo"
    )));
    assert!(
        engine.is_completed(instance_key),
        "the throw resumes and the instance completes"
    );
    let inst = engine.instance(instance_key).unwrap();
    assert!(inst.compensation_waits.is_empty());
    assert!(inst.incidents.is_empty());
}

#[test]
fn sub_process_compensation_handler_retires_the_throw_wait() {
    run_sub_process_handler(false);
}

#[test]
fn sub_process_compensation_handler_with_end_listener_retires_the_throw_wait() {
    run_sub_process_handler(true);
}
