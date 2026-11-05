//! Zeebe execution-order parity (#1244): the order the engine takes outgoing
//! flows in, gateway completion inside the activation's batch, and joins that
//! write no element-instance record until they are ready. Each property is also
//! pinned against real Zeebe by a `formal/traces` scenario
//! (`parallel-fork-join-tasks`, `inclusive-join-pass-through-race`).

use super::*;

fn deploy_and_run(
    def: ProcessDefinition,
    vars_in: HashMap<String, Value>,
) -> (Engine, Key, Vec<Event>) {
    let mut log = Vec::new();
    let id = def.id.clone();
    let mut engine = Engine::new();
    log.extend(engine.apply_command(Command::DeployProcess(def)).unwrap());
    let created = engine
        .apply_command(Command::create_instance_with(id, vars_in))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    log.extend(created);
    (engine, inst, log)
}

/// The targets of the `SequenceFlowTaken` events from `from`, in order.
fn taken_from<'a>(events: &'a [Event], from: &str) -> Vec<&'a str> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::SequenceFlowTaken { from: f, to, .. } if f == from => Some(to.as_str()),
            _ => None,
        })
        .collect()
}

/// Every lifecycle event of `element_id`, as `(variant name)`.
fn lifecycle_of(events: &[Event], element_id: &str) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::ElementActivating { element_id: id, .. } if id == element_id => {
                Some("Activating")
            }
            Event::ElementActivated { element_id: id, .. } if id == element_id => Some("Activated"),
            Event::ElementCompleting { element_id: id, .. } if id == element_id => {
                Some("Completing")
            }
            Event::ElementCompleted { element_id: id, .. } if id == element_id => Some("Completed"),
            _ => None,
        })
        .collect()
}

/// `start -> fork -> {a(ja), b(jb)} -> join -> done`, flows declared `a` first.
fn fork_join(join_inclusive: bool) -> ProcessDefinition {
    let builder = ProcessBuilder::new("fork-join")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("a", "ja")
        .service_task("b", "jb");
    let builder = if join_inclusive {
        builder.inclusive_gateway("join")
    } else {
        builder.parallel_gateway("join")
    };
    builder
        .end_event("done")
        .connect("start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "done")
        .build()
        .unwrap()
}

/// Zeebe's `getOutgoing()` lists sibling flows in reverse document order (its
/// `ModelWalker` pushes children with `addFirst`), and a fork takes them in
/// that order: the last-declared branch activates first.
#[test]
fn parallel_fork_takes_flows_in_reverse_document_order() {
    let (_, _, log) = deploy_and_run(fork_join(false), HashMap::new());
    assert_eq!(taken_from(&log, "fork"), vec!["b", "a"]);
    let activations: Vec<&str> = log
        .iter()
        .filter_map(|e| match e {
            Event::ElementActivating { element_id, .. }
                if element_id == "a" || element_id == "b" =>
            {
                Some(element_id.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(activations, vec!["b", "a"]);
}

/// The declared index (and so the persisted `IncomingFlow::ordinal`) is
/// unchanged by the execution order: only the iteration order is reversed.
#[test]
fn execution_order_keeps_declaration_indices() {
    let def = fork_join(false);
    let fork = def.element("fork").unwrap();
    let order: Vec<(usize, &str)> = fork
        .outgoing_in_execution_order()
        .map(|(i, f)| (i, f.to.as_str()))
        .collect();
    assert_eq!(order, vec![(1, "b"), (0, "a")]);
    assert_eq!(fork.outgoing[0].to, "a", "declaration order is preserved");
}

/// An exclusive gateway evaluates its conditions in reverse document order and
/// takes the first that holds: of two matching flows the one declared last
/// wins (Zeebe's `ExclusiveGatewayProcessor.findSequenceFlowToTake`).
#[test]
fn exclusive_gateway_takes_the_last_declared_matching_flow() {
    let def = ProcessBuilder::new("xor")
        .start_event("start")
        .exclusive_gateway("g")
        .end_event("first")
        .end_event("second")
        .end_event("other")
        .connect("start", "g")
        .connect_when("g", "first", "=x > 0")
        .connect_when("g", "second", "=x > 1")
        .connect_default("g", "other")
        .build()
        .unwrap();
    let (_, _, log) = deploy_and_run(def, vars(&[("x", Value::Int(5))]));
    assert_eq!(taken_from(&log, "g"), vec!["second"]);
}

/// A non-default flow without a condition (a nano extension: Zeebe rejects
/// such a model at deploy) is a fallback: taken only when no condition holds,
/// the first declared one first, ahead of the explicit default.
#[test]
fn exclusive_gateway_unconditional_flow_is_a_fallback() {
    let build = || {
        ProcessBuilder::new("xor-fallback")
            .start_event("start")
            .exclusive_gateway("g")
            .end_event("plain1")
            .end_event("plain2")
            .end_event("cond")
            .end_event("dflt")
            .connect("start", "g")
            .connect("g", "plain1")
            .connect("g", "plain2")
            .connect_when("g", "cond", "=x = 1")
            .connect_default("g", "dflt")
            .build()
            .unwrap()
    };
    let (_, _, log) = deploy_and_run(build(), vars(&[("x", Value::Int(1))]));
    assert_eq!(
        taken_from(&log, "g"),
        vec!["cond"],
        "a holding condition wins"
    );
    let (_, _, log) = deploy_and_run(build(), vars(&[("x", Value::Int(2))]));
    assert_eq!(
        taken_from(&log, "g"),
        vec!["plain1"],
        "first declared fallback"
    );
}

/// A gateway completes inside its activation's batch (Zeebe's gateway
/// processors complete in `onActivate`), so a sibling activation queued before
/// the gateway's own completion runs only after the gateway took its flows.
#[test]
fn gateway_completes_inline_before_queued_siblings() {
    // fork takes `g` first (declared last), then `t`.
    let def = ProcessBuilder::new("inline")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("t", "jt")
        .exclusive_gateway("g")
        .service_task("after_g", "jg")
        .connect("start", "fork")
        .connect("fork", "t")
        .connect("fork", "g")
        .connect("g", "after_g")
        .build()
        .unwrap();
    let (_, _, log) = deploy_and_run(def, HashMap::new());
    let position = |pred: &dyn Fn(&Event) -> bool| log.iter().position(pred).unwrap();
    let g_completed =
        position(&|e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == "g"));
    let t_activating = position(
        &|e| matches!(e, Event::ElementActivating { element_id, .. } if element_id == "t"),
    );
    let g_taken = position(&|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "g"));
    assert!(
        g_completed < t_activating && g_taken < t_activating,
        "the gateway completes and takes its flow before the queued sibling activates: {log:?}"
    );
    // The sibling still activates before the gateway's target (FIFO commands).
    let after_g_activating = position(
        &|e| matches!(e, Event::ElementActivating { element_id, .. } if element_id == "after_g"),
    );
    assert!(t_activating < after_g_activating);
}

/// A join that is not ready writes no element-instance record (Zeebe rejects
/// the `ACTIVATE_ELEMENT` command): it activates exactly once, when ready, and
/// the waiting token keeps the process instance alive meanwhile.
#[test]
fn join_writes_no_lifecycle_until_ready() {
    for inclusive in [false, true] {
        let (mut engine, inst, log) = deploy_and_run(fork_join(inclusive), HashMap::new());
        assert!(lifecycle_of(&log, "join").is_empty());
        let first = complete_one(&mut engine, "ja");
        assert!(
            lifecycle_of(&first, "join").is_empty(),
            "an unready join must not activate (inclusive={inclusive}): {first:?}"
        );
        assert!(
            first
                .iter()
                .any(|e| matches!(e, Event::ParallelJoinOpened { scope: Some(0), .. })),
            "the waiting token is parked in the root scope"
        );
        assert_eq!(
            engine.instance(inst).unwrap().state,
            ProcessInstanceState::Active,
            "the parked token keeps the instance alive"
        );
        let second = complete_one(&mut engine, "jb");
        assert_eq!(
            lifecycle_of(&second, "join"),
            vec!["Activating", "Activated", "Completing", "Completed"],
            "inclusive={inclusive}"
        );
        assert_eq!(
            engine.instance(inst).unwrap().state,
            ProcessInstanceState::Completed
        );
    }
}

/// Tearing down a scope that holds a parked join drops its waiting tokens
/// without any element-instance record for the join (there is none to end).
#[test]
fn scope_teardown_writes_no_lifecycle_for_a_parked_join() {
    let def = ProcessBuilder::new("sub-join")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("fork")
        .contained_in("fork", "sub")
        .service_task("a", "ja")
        .contained_in("a", "sub")
        .service_task("b", "jb")
        .contained_in("b", "sub")
        .parallel_gateway("join")
        .contained_in("join", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap();
    let (mut engine, inst, _) = deploy_and_run(def, HashMap::new());
    complete_one(&mut engine, "ja");
    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["sub".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert!(
        lifecycle_of(&events, "join").is_empty(),
        "the parked join has no lifecycle to end: {events:?}"
    );
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ParallelJoinReset { element_id, .. } if element_id == "join")));
    let instance = engine.instance(inst).unwrap();
    assert!(instance.parked_joins.is_empty());
    assert!(instance.join_instances.is_empty());
}

/// A parked join's key is not an element instance: it cannot be named in a
/// terminate instruction.
#[test]
fn a_parked_join_key_cannot_be_terminated_by_key() {
    let (mut engine, inst, _) = deploy_and_run(fork_join(false), HashMap::new());
    let events = complete_one(&mut engine, "ja");
    let parked = events
        .iter()
        .find_map(|e| match e {
            Event::ParallelJoinOpened {
                element_instance_key,
                ..
            } => Some(*element_instance_key),
            _ => None,
        })
        .unwrap();
    let err = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![parked],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::ElementInstanceNotFound { .. }),
        "{err:?}"
    );
}

/// A journal written before #1244 activated an open join visibly
/// (`ElementActivating`/`ElementActivated`, then `ParallelJoinOpened` without
/// a scope). Replayed under this build, that join fires on the next arrival
/// without activating a second time.
#[test]
fn legacy_visibly_opened_join_replays_and_fires_once() {
    let (mut engine, inst, mut log) = deploy_and_run(fork_join(false), HashMap::new());
    log.extend(complete_one(&mut engine, "ja"));
    let legacy: Vec<Event> = log
        .into_iter()
        .flat_map(|e| match e {
            Event::ParallelJoinOpened {
                instance_key,
                element_instance_key,
                element_id,
                scope: Some(scope),
            } => vec![
                Event::ElementActivating {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    scope,
                },
                Event::ElementActivated {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    scope,
                },
                Event::ParallelJoinOpened {
                    instance_key,
                    element_instance_key,
                    element_id,
                    scope: None,
                },
            ],
            other => vec![other],
        })
        .collect();
    // Round-trip through the persisted JSON shape, as a recovering host does.
    #[cfg(feature = "serde")]
    let legacy: Vec<Event> = legacy
        .iter()
        .map(|e| {
            let mut json = serde_json::to_value(e).unwrap();
            if let Some(opened) = json.get_mut("ParallelJoinOpened") {
                opened.as_object_mut().unwrap().remove("scope");
            }
            crate::decode_event_json(&json.to_string()).unwrap()
        })
        .collect();
    let mut engine = Engine::replay(legacy);
    assert!(engine.instance(inst).unwrap().parked_joins.is_empty());
    let events = complete_one(&mut engine, "jb");
    assert_eq!(
        lifecycle_of(&events, "join"),
        vec!["Completing", "Completed"],
        "the visibly opened join is taken over, not activated again"
    );
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}
