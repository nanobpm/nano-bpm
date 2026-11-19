//! Error end events (#1592): an `endEvent` carrying an `errorEventDefinition`
//! throws the BPMN error on completion, resolved to a catch exactly like a
//! worker `ThrowJobError` (Zeebe `CatchEventAnalyzer` parity) — the shared
//! [`Engine::find_error_catch`] walk covers error boundaries and error event
//! sub-processes, an exact code beats a catch-all, an event sub-process beats a
//! boundary of equal match, and an uncaught error parks the token on an
//! `UNHANDLED_ERROR` incident.
use super::*;

/// `s -> sub[ sub_s -> err_end(error CODE) ] -> after(job "after") -> e`, with
/// an error boundary `bnd` (code `CODE`) on `sub` routing to `handler` (job
/// "handle") -> `handler_end`.
fn sub_with_error_end() -> ProcessBuilder {
    ProcessBuilder::new("p")
        .start_event("s")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .error_end_event("err_end", "E1")
        .contained_in("sub_s", "sub")
        .contained_in("err_end", "sub")
        .error_boundary_event("bnd", "sub", "E1")
        .service_task("handler", "handle")
        .service_task("after", "after")
        .end_event("e")
        .end_event("handler_end")
        .connect("s", "sub")
        .connect("sub_s", "err_end")
        .connect("sub", "after")
        .connect("after", "e")
        .connect("bnd", "handler")
        .connect("handler", "handler_end")
}

fn start(engine: &mut Engine, def: ProcessDefinition) -> (Key, Vec<Event>) {
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine.apply_command(Command::create_instance("p")).unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    (inst, events)
}

fn activated(events: &[Event], id: &str) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == id))
}

#[test]
fn error_end_event_is_caught_by_an_error_boundary_on_the_enclosing_subprocess() {
    // The error end event throws on completion; the interrupting boundary on
    // the enclosing sub-process catches it, tears the sub-process scope down
    // and routes to the handler. The sub-process's normal outgoing flow is NOT
    // taken.
    let mut engine = Engine::new();
    let (inst, events) = start(&mut engine, sub_with_error_end().build().unwrap());

    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "err_end"
        )),
        "the error end event completed"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "sub"
        )),
        "the caught sub-process completed (interrupted)"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "handler"
        )),
        "the boundary's outgoing flow ran"
    );
    assert!(
        events.iter().all(|e| !matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "after"
        )),
        "the sub-process's normal outgoing flow was not taken"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "a caught error raises no incident"
    );

    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(inst));
}

#[test]
fn uncaught_error_end_event_raises_an_unhandled_error_incident() {
    // No catch anywhere: the token parks on an UNHANDLED_ERROR incident — the
    // same lifecycle as an unhandled job-thrown error (the job is consumed,
    // never the token), so the instance stays active and resolving the
    // incident re-drives the completion.
    let mut engine = Engine::new();
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .error_end_event("err_end", "E1")
        .connect("s", "err_end")
        .build()
        .unwrap();
    let (inst, events) = start(&mut engine, def);

    assert!(
        events.iter().all(|e| !matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "err_end"
        )),
        "an uncaught throw does NOT complete the end event — the token parks"
    );
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "exactly one incident");
    assert_eq!(
        incidents[0].kind,
        crate::state::IncidentKind::UnhandledError
    );
    assert_eq!(incidents[0].element_id, "err_end");
    assert!(
        incidents[0].reason.contains("E1"),
        "the reason names the thrown code: {}",
        incidents[0].reason
    );
    assert!(
        !engine.is_completed(inst),
        "the instance parks on the incident"
    );

    // Resolving the incident re-drives the completion: the throw re-resolves
    // (still uncaught — the model is unchanged), so a fresh incident is raised
    // and the instance stays parked, exactly like the job-error lifecycle.
    let incident_key = incidents[0].key;
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(
        resolved.iter().any(|e| matches!(
            e,
            Event::IncidentRaised { kind, .. } if *kind == crate::state::IncidentKind::UnhandledError
        )),
        "resolution re-drives the throw, which is still uncaught"
    );
    assert!(!engine.is_completed(inst));
}

#[test]
fn error_end_event_is_caught_by_an_error_event_subprocess() {
    // An error event sub-process at the process root catches the throw
    // (always interrupting): the ESP runs and the instance completes with it.
    let mut engine = Engine::new();
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .error_end_event("err_end", "E1")
        .event_sub_process(
            "esp",
            "esp_start",
            crate::model::EventSubProcessTrigger::Error {
                error_code: Some("E1".to_string()),
            },
            true,
        )
        .start_event("esp_start")
        .service_task("esp_task", "esp")
        .end_event("esp_end")
        .connect("s", "err_end")
        .connect("esp_start", "esp_task")
        .connect("esp_task", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_task", "esp")
        .contained_in("esp_end", "esp")
        .build()
        .unwrap();
    let (inst, events) = start(&mut engine, def);

    assert!(activated(&events, "esp"), "error ESP activated");
    assert!(engine.active_incidents().is_empty(), "no incident");
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst), "scope completes with the ESP");
}

#[test]
fn error_end_event_catch_prefers_the_innermost_scope_over_an_outer_exact_esp() {
    // Scope-depth check: an exact-code error boundary on the sub-process AND an
    // exact-code error event sub-process at the root: the walk finds the inner
    // boundary first (the innermost scope wins before the root's ESP is even
    // offered).
    let mut engine = Engine::new();
    let def = sub_with_error_end()
        // Repurpose: the boundary catches ALL codes; an exact ESP sits at root.
        .event_sub_process(
            "esp",
            "esp_start",
            crate::model::EventSubProcessTrigger::Error {
                error_code: Some("E1".to_string()),
            },
            true,
        )
        .start_event("esp_start")
        .service_task("esp_task", "esp")
        .end_event("esp_end")
        .connect("esp_start", "esp_task")
        .connect("esp_task", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_task", "esp")
        .contained_in("esp_end", "esp")
        .build()
        .unwrap();
    let (inst, events) = start(&mut engine, def);

    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "handler"
        )),
        "the inner boundary catches before the root ESP is offered"
    );
    assert!(!activated(&events, "esp"));
    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(inst));
}

#[test]
fn error_end_event_catch_prefers_an_exact_code_over_a_catch_all_at_the_same_scope() {
    // Specificity check at ONE scope level: a catch-all error boundary and an
    // exact-code (`E1`) boundary both attached to `sub`. The catch-all is
    // declared FIRST so declaration order alone would pick it; the exact code
    // must win regardless.
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .error_end_event("err_end", "E1")
        .contained_in("sub_s", "sub")
        .contained_in("err_end", "sub")
        .error_boundary_event("bnd_all", "sub", "")
        .error_boundary_event("bnd", "sub", "E1")
        .service_task("all_handler", "handle_all")
        .service_task("handler", "handle")
        .end_event("e")
        .end_event("all_end")
        .end_event("handler_end")
        .connect("s", "sub")
        .connect("sub_s", "err_end")
        .connect("sub", "e")
        .connect("bnd_all", "all_handler")
        .connect("all_handler", "all_end")
        .connect("bnd", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let (inst, events) = start(&mut engine, def);

    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "handler"
        )),
        "the exact-code boundary catches over the same-scope catch-all"
    );
    assert!(!activated(&events, "all_handler"), "catch-all not taken");
    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(inst));
}

#[test]
fn error_end_event_throw_waits_for_its_end_listener_to_drain() {
    // An error end event carrying an *end execution listener* defers
    // completion: the error must be thrown only after the listener chain
    // drains (the deferred `finalize_completion` path), never while the
    // listener job is pending.
    let def = sub_with_error_end()
        .with_listeners(
            "err_end",
            Vec::new(),
            vec![el(ListenerEventType::End, "err-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let (inst, events) = start(&mut engine, def);

    assert!(
        events.iter().all(|e| !matches!(
            e,
            Event::SequenceFlowTaken { from, .. } if from == "bnd"
        )),
        "the error must not be thrown until the end listener drains"
    );
    assert!(
        engine
            .pending_jobs()
            .iter()
            .any(|j| j.job_type == "err-audit"),
        "the error end event's end listener job is pending"
    );

    let after = complete_one(&mut engine, "err-audit");
    assert!(
        after.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "handler"
        )),
        "the error is thrown and caught once the end listener drains"
    );

    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(inst));
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
}

#[test]
fn error_end_event_inside_a_multi_instance_child_is_caught_on_the_body() {
    // An error end event inside a multi-instance sub-process child: the error
    // boundary is armed on the loop's BODY, so the catch must tear down the
    // WHOLE loop (every sibling child and the body's runtime record), not just
    // the throwing child — mirroring the escalation-throw MI redirection
    // (#1173, the #1170 MI class).
    let def = ProcessBuilder::new("p")
        .start_event("s")
        .sub_process("sub", "sub_s")
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
        .start_event("sub_s")
        .error_end_event("err_end", "E1")
        .contained_in("sub_s", "sub")
        .contained_in("err_end", "sub")
        .error_boundary_event("bnd", "sub", "E1")
        .service_task("handler", "handle")
        .end_event("handler_end")
        .connect("s", "sub")
        .connect("sub_s", "err_end")
        .connect("bnd", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = BTreeMap::new();
    vars.insert(
        "items".to_string(),
        Value::List(vec![Value::Int(1), Value::Int(2)]),
    );
    let events = engine
        .apply_command(Command::create_instance_with("p", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Both children threw and were caught on the body: the whole loop was torn
    // down and the boundary's handler activated (once — the body is interrupted
    // by the first catch; the second child's throw finds the scope torn down).
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "handler"
        )),
        "the boundary on the MI body caught the throw"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no incident: {:?}",
        engine.active_incidents()
    );
    complete_one(&mut engine, "handle");
    assert!(engine.is_completed(inst));
}

/// Resolving an uncaught error end event's incident must re-run ONLY the catch
/// walk: the end event already applied its output mappings and drained its end
/// listeners before parking, so neither may be repeated (#1592 review).
fn assert_retry_reruns_only_the_throw(resolved: &[Event]) {
    assert!(
        resolved.iter().any(|e| matches!(
            e,
            Event::IncidentRaised { kind, .. } if *kind == crate::state::IncidentKind::UnhandledError
        )),
        "the retried throw is still uncaught, so a fresh incident is raised"
    );
    for e in resolved {
        assert!(
            !matches!(
                e,
                Event::ElementCompleting { .. }
                    | Event::ExecutionListenerJobCreated { .. }
                    | Event::VariablesUpdated { .. }
            ),
            "resolution must not repeat outputs/listeners/lifecycle: {e:?}"
        );
    }
}

fn uncaught_err_end_with_output() -> ProcessBuilder {
    ProcessBuilder::new("p")
        .start_event("s")
        .error_end_event("err_end", "E1")
        .with_io(
            "err_end",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=1".to_string(),
                    target: "out".to_string(),
                }],
            },
        )
        .connect("s", "err_end")
}

#[test]
fn resolving_an_uncaught_error_end_incident_does_not_rerun_output_mappings() {
    let mut engine = Engine::new();
    let (inst, _) = start(&mut engine, uncaught_err_end_with_output().build().unwrap());
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1);
    let resolved = engine
        .apply_command(Command::resolve_incident(incidents[0].key))
        .unwrap();
    assert_retry_reruns_only_the_throw(&resolved);
    assert!(!engine.is_completed(inst));
}

#[test]
fn resolving_a_deferred_uncaught_error_end_incident_does_not_refire_end_listeners() {
    let def = uncaught_err_end_with_output()
        .with_listeners(
            "err_end",
            Vec::new(),
            vec![el(ListenerEventType::End, "err-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let (inst, _) = start(&mut engine, def);
    complete_one(&mut engine, "err-audit");
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "uncaught after the listener drains");
    let resolved = engine
        .apply_command(Command::resolve_incident(incidents[0].key))
        .unwrap();
    assert_retry_reruns_only_the_throw(&resolved);
    assert!(
        engine
            .pending_jobs()
            .iter()
            .all(|j| j.job_type != "err-audit"),
        "the end listener is not re-armed"
    );
    assert!(!engine.is_completed(inst));
}
