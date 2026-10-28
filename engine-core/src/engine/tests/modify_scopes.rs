//! #1374 `engine-modify-scopes`: Zeebe `ProcessInstanceModificationModifyProcessor`
//! parity for `ModifyInstance` — nested-scope activation (find-or-create flow
//! scopes), ancestor selection, scoped variable instructions, terminate by
//! element id with flow-scope cascade, and the matching rejections. Ported from
//! Zeebe `ModifyProcessInstanceTest`, `ModifyProcessInstanceRejectionTest` and
//! `ModifyProcessInstanceVariablesTest`.
use super::*;
use crate::command::VariableInstruction;

/// `s -> outer[ os -> inner[ is -> t(work) -> ie ] -> oe ] -> e`, plus a
/// disconnected root-level task `r` (activation target only).
fn nested() -> ProcessDefinition {
    ProcessBuilder::new("nested")
        .start_event("s")
        .sub_process("outer", "os")
        .start_event("os")
        .contained_in("os", "outer")
        .sub_process("inner", "is")
        .contained_in("inner", "outer")
        .start_event("is")
        .contained_in("is", "inner")
        .service_task("t", "work")
        .contained_in("t", "inner")
        .end_event("ie")
        .contained_in("ie", "inner")
        .end_event("oe")
        .contained_in("oe", "outer")
        .service_task("r", "root-work")
        .end_event("e")
        .connect("s", "outer")
        .connect("os", "inner")
        .connect("is", "t")
        .connect("t", "ie")
        .connect("inner", "oe")
        .connect("outer", "e")
        .build()
        .unwrap()
}

/// `start -> a(jobA) -> sub[ ss -> b(jobB) -> se ] -> end`, with an
/// interrupting timer boundary `bt` on `sub` leading to `late`.
fn task_then_sub() -> ProcessDefinition {
    ProcessBuilder::new("task-sub")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .timer_boundary_event("bt", "sub", 60_000)
        .end_event("end")
        .end_event("late")
        .connect("start", "a")
        .connect("a", "sub")
        .connect("ss", "b")
        .connect("b", "se")
        .connect("sub", "end")
        .connect("bt", "late")
        .build()
        .unwrap()
}

fn start(engine: &mut Engine, def: ProcessDefinition, vars_in: HashMap<String, Value>) -> Key {
    let id = def.id.clone();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(id, vars_in))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
}

/// Every active element instance of `element_id`, sorted.
fn eiks(engine: &Engine, inst: Key, element_id: &str) -> Vec<Key> {
    let mut keys: Vec<Key> = engine
        .instance(inst)
        .unwrap()
        .active
        .iter()
        .filter(|(_, id)| id.as_str() == element_id)
        .map(|(k, _)| *k)
        .collect();
    keys.sort_unstable();
    keys
}

fn scope_of(engine: &Engine, inst: Key, eik: Key) -> Key {
    engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&eik)
        .copied()
        .unwrap_or(0)
}

fn activated(events: &[Event], element_id: &str) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::ElementActivated { element_id: id, .. } if id == element_id))
        .count()
}

fn modify(inst: Key, activate: Vec<ActivateElementInstruction>) -> Command {
    Command::ModifyInstance {
        instance_key: inst,
        activate_instructions: activate,
        terminate_instructions: vec![],
        terminate_element_ids: vec![],
        move_instructions: vec![],
    }
}

fn under(element_id: &str, ancestor: Key) -> ActivateElementInstruction {
    ActivateElementInstruction {
        element_id: element_id.into(),
        ancestor_element_instance_key: Some(ancestor),
        ..Default::default()
    }
}

fn complete_all(engine: &mut Engine, job_type: &str) {
    let jobs = engine.activate_jobs(job_type, "w", 10, 60_000, 0);
    assert!(!jobs.is_empty(), "no {job_type} jobs");
    for job in jobs {
        engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
    }
}

// ---- Activation inside flow scopes -----------------------------------------

/// Zeebe `shouldActivateInsideExistingFlowScope`.
#[test]
fn activates_inside_the_existing_flow_scope() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let inner = eiks(&engine, inst, "inner")[0];

    let events = engine
        .apply_command(modify(inst, vec![ActivateElementInstruction::new("t")]))
        .unwrap();

    assert_eq!(activated(&events, "inner"), 0, "existing scope reused");
    let ts = eiks(&engine, inst, "t");
    assert_eq!(ts.len(), 2);
    assert!(ts.iter().all(|t| scope_of(&engine, inst, *t) == inner));
}

/// Zeebe `shouldActivateElementsInsideNestedNonExistingFlowScopes` +
/// `shouldCreateEventSubscriptionsWhenActivatingElementsInsideNonExistingFlowScopes`:
/// a missing flow scope is created WITHOUT running its start event, its
/// boundary events are armed, and it completes normally once drained.
#[test]
fn creates_missing_flow_scope_without_its_start_event() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());
    let a = eiks(&engine, inst, "a")[0];
    let timers_before = engine.state().timers.len();

    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![ActivateElementInstruction::new("b")],
            terminate_instructions: vec![a],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap();

    assert_eq!(activated(&events, "sub"), 1);
    assert_eq!(activated(&events, "ss"), 0, "start event not activated");
    let sub = eiks(&engine, inst, "sub")[0];
    let b = eiks(&engine, inst, "b");
    assert_eq!(b.len(), 1);
    assert_eq!(scope_of(&engine, inst, b[0]), sub);
    assert_eq!(
        engine.state().timers.len(),
        timers_before + 1,
        "boundary armed"
    );
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );

    complete_all(&mut engine, "jobB");
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

/// Zeebe `shouldActivateElementsInsideSameNonExistingFlowScope`: two
/// instructions needing the same missing scope share one new instance of it.
#[test]
fn instructions_share_a_flow_scope_created_by_the_same_modification() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());

    let events = engine
        .apply_command(modify(
            inst,
            vec![
                ActivateElementInstruction::new("b"),
                ActivateElementInstruction::new("b"),
            ],
        ))
        .unwrap();

    assert_eq!(activated(&events, "sub"), 1);
    let sub = eiks(&engine, inst, "sub")[0];
    let b = eiks(&engine, inst, "b");
    assert_eq!(b.len(), 2);
    assert!(b.iter().all(|k| scope_of(&engine, inst, *k) == sub));
}

/// Zeebe `shouldActivateInsideSpecificFlowScopeUsingAncestorSelection` and
/// `shouldActivateInsideNestedSpecificFlowScopeUsingAncestorSelection`.
#[test]
fn ancestor_selects_the_flow_scope_instance() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    // A second `outer` (with its own `inner` and `t`).
    engine
        .apply_command(modify(inst, vec![ActivateElementInstruction::new("outer")]))
        .unwrap();
    let outers = eiks(&engine, inst, "outer");
    let inners = eiks(&engine, inst, "inner");
    assert_eq!((outers.len(), inners.len()), (2, 2));

    // Direct flow-scope instance as ancestor.
    engine
        .apply_command(modify(inst, vec![under("t", inners[1])]))
        .unwrap();
    let in_second: Vec<Key> = eiks(&engine, inst, "t")
        .into_iter()
        .filter(|t| scope_of(&engine, inst, *t) == inners[1])
        .collect();
    assert_eq!(in_second.len(), 2);

    // A higher ancestor: the missing `inner` is created beneath it.
    let events = engine
        .apply_command(modify(inst, vec![under("t", outers[0])]))
        .unwrap();
    assert_eq!(activated(&events, "inner"), 1);
    assert_eq!(activated(&events, "outer"), 0);
    let new_inner = *eiks(&engine, inst, "inner").last().unwrap();
    assert_eq!(scope_of(&engine, inst, new_inner), outers[0]);
}

/// The process instance key as ancestor = the root: the whole chain is created.
#[test]
fn process_instance_key_as_ancestor_creates_the_whole_chain() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let events = engine
        .apply_command(modify(inst, vec![under("t", inst)]))
        .unwrap();
    assert_eq!(activated(&events, "outer"), 1);
    assert_eq!(activated(&events, "inner"), 1);
    assert_eq!(activated(&events, "os"), 0);
    assert_eq!(activated(&events, "is"), 0);
    assert_eq!(eiks(&engine, inst, "outer").len(), 2);
}

// ---- Rejections ------------------------------------------------------------

/// Zeebe `shouldRejectActivationWhenFlowScopeHasMultipleInstances` (message).
#[test]
fn rejects_ambiguous_flow_scope() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    engine
        .apply_command(modify(inst, vec![ActivateElementInstruction::new("outer")]))
        .unwrap();
    let err = engine
        .apply_command(modify(inst, vec![ActivateElementInstruction::new("t")]))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AmbiguousFlowScope { .. }),
        "{err:?}"
    );
    assert_eq!(
        err.to_string(),
        "Expected to modify instance of process 'nested' but it contains one or more activate \
         instructions for an element that has a flow scope with more than one active instance: \
         't'. Can't decide in which instance of the flow scope the element should be activated. \
         Please specify an ancestor element instance key for this activate instruction."
    );
}

/// Zeebe `shouldRejectCommandWhenAncestorScopeIsNotFound` (message).
#[test]
fn rejects_unknown_ancestor() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let err = engine
        .apply_command(modify(inst, vec![under("t", 424_242)]))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AncestorScopeNotFound { .. }),
        "{err:?}"
    );
    assert_eq!(
        err.to_string(),
        "Expected to modify instance of process 'nested' but it contains one or more activate \
         instructions with an ancestor scope key that does not exist, or is not in an active \
         state: '424242'"
    );
}

/// Zeebe `shouldRejectCommandWhenAncestorIsNotAnAncestorOfElement` (message).
#[test]
fn rejects_ancestor_that_is_not_an_ancestor() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let t = eiks(&engine, inst, "t")[0];
    let err = engine
        .apply_command(modify(inst, vec![under("t", t)]))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AncestorNotAnAncestor { .. }),
        "{err:?}"
    );
    assert!(
        err.to_string().starts_with(
            "Expected to modify instance of process 'nested' but it contains one or more \
             activate instructions with an ancestor scope key that is not an ancestor of the \
             element to activate"
        ),
        "{err}"
    );
    assert!(err.to_string().contains(&format!("'{t}'")), "{err}");
}

/// Zeebe `shouldRejectActivationOfUnsupportedElementType` (start / boundary).
#[test]
fn rejects_non_activatable_targets() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());
    for (element, ty) in [
        ("ss", "START_EVENT"),
        ("start", "START_EVENT"),
        ("bt", "BOUNDARY_EVENT"),
    ] {
        let err = engine
            .apply_command(modify(inst, vec![ActivateElementInstruction::new(element)]))
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::ElementNotActivatable { element_id, .. } if element_id == element),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!(
                "Expected to modify instance of process 'task-sub' but it contains one or more \
                 activate instructions for elements that are unsupported: '{element}'. The \
                 activation of elements with type '{ty}' is not supported."
            )
        );
    }
}

/// Zeebe `shouldRejectActivationOfElementInsideMultiInstanceBody`: creating a
/// multi-instance flow scope through modification is unsupported.
#[test]
fn rejects_activation_that_would_create_a_multi_instance_scope() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        multi_instance_subprocess(false),
        vars(&[("tasks", Value::List(vec![Value::Str("x".into())]))]),
    );
    // Drain the MI sub-process so no instance of `wave` is active.
    complete_all(&mut engine, "do-work");
    assert!(eiks(&engine, inst, "wave").is_empty());
    let err = engine
        .apply_command(modify(inst, vec![ActivateElementInstruction::new("impl")]))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::ElementNotActivatable { element_id, .. } if element_id == "impl"),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("multi-instance element 'wave'"),
        "{err}"
    );
}

/// Zeebe `shouldUseAncestorSelectionInsideMultiInstances`: activating inside
/// an existing multi-instance child (selected by ancestor) is supported, and
/// one active child alone is not ambiguous (the body is not a flow-scope
/// instance of the inner element).
#[test]
fn activates_inside_an_existing_multi_instance_child() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        multi_instance_subprocess(false),
        vars(&[(
            "tasks",
            Value::List(vec![Value::Str("x".into()), Value::Str("y".into())]),
        )]),
    );
    let child = scope_of(&engine, inst, eiks(&engine, inst, "impl")[0]);
    engine
        .apply_command(modify(inst, vec![under("impl", child)]))
        .unwrap();
    let in_child = eiks(&engine, inst, "impl")
        .into_iter()
        .filter(|k| scope_of(&engine, inst, *k) == child)
        .count();
    assert_eq!(in_child, 2);
}

// ---- Variable instructions -------------------------------------------------

/// Zeebe `ModifyProcessInstanceVariablesTest`: global, flow-scope-local and
/// element-local variable instructions.
#[test]
fn variable_instructions_target_the_matching_scope() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());
    let vi = |scope: Option<&str>, name: &str| VariableInstruction {
        variables: vars(&[(name, Value::Int(1))]),
        scope_id: scope.map(str::to_string),
    };
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variable_instructions: vec![
                    vi(None, "global"),
                    vi(Some("task-sub"), "global2"),
                    vi(Some("sub"), "in_sub"),
                    vi(Some("b"), "in_b"),
                ],
                ..Default::default()
            }],
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    let sub = eiks(&engine, inst, "sub")[0];
    let b = eiks(&engine, inst, "b")[0];
    assert!(instance.variables.contains_key("global"));
    assert!(instance.variables.contains_key("global2"));
    assert!(!instance.variables.contains_key("in_sub"));
    assert!(!instance.variables.contains_key("in_b"));
    assert!(instance.scope_variables[&sub].contains_key("in_sub"));
    assert!(instance.scope_variables[&b].contains_key("in_b"));
    assert!(!instance.scope_variables[&sub].contains_key("in_b"));
}

/// Zeebe `shouldRejectCommandWhenVariableScopeIsNotFound` /
/// `...NotFlowScope` (messages).
#[test]
fn rejects_invalid_variable_scopes() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());
    let with_scope = |scope: &str| {
        modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[("x", Value::Int(1))]),
                    scope_id: Some(scope.into()),
                }],
                ..Default::default()
            }],
        )
    };
    let err = engine.apply_command(with_scope("nope")).unwrap_err();
    assert!(
        matches!(err, EngineError::ModificationInstructionInvalid { .. }),
        "{err:?}"
    );
    assert!(
        err.to_string().ends_with(
            "variable instructions with a scope element id that could not be found: 'nope'"
        ),
        "{err}"
    );
    let err = engine.apply_command(with_scope("a")).unwrap_err();
    assert!(
        err.to_string().ends_with(
            "variable instructions with a scope element that doesn't belong to the activating \
             element's flow scope. These variables should be set before or after the \
             modification."
        ),
        "{err}"
    );
    assert!(eiks(&engine, inst, "b").is_empty(), "atomic");
}

// ---- Terminate by element id / flow-scope cascade --------------------------

/// Zeebe `shouldTerminateAllElementInstancesForGivenElementId` (multi-instance).
#[test]
fn terminate_by_id_terminates_every_instance_including_mi_children() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        multi_instance_service_process(false),
        vars(&[(
            "items",
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        )]),
    );
    assert_eq!(eiks(&engine, inst, "each").len(), 4, "body + 3 children");

    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["each".into()],
            move_instructions: vec![],
        })
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    assert!(instance.active.is_empty(), "{:?}", instance.active);
    assert_eq!(instance.state, ProcessInstanceState::Terminated);
    assert!(engine
        .state()
        .jobs
        .values()
        .filter(|j| j.instance_key == inst)
        .all(|j| j.state == state::JobState::Canceled));
}

/// Zeebe `ModifyProcessInstanceTerminationTest`: terminating the last child of
/// nested flow scopes terminates them (not completes — no outgoing flow is
/// taken) up to the process instance.
#[test]
fn terminating_the_last_child_terminates_the_flow_scopes() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["t".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(activated(&events, "oe"), 0);
    assert_eq!(activated(&events, "e"), 0);
    assert!(events.iter().any(
        |e| matches!(e, Event::ProcessInstanceTerminated { instance_key } if *instance_key == inst)
    ));
    assert!(engine.instance(inst).unwrap().active.is_empty());
}

/// The cascade stops at a scope (here the root) that an activation needs.
#[test]
fn cascade_stops_at_a_scope_required_by_an_activation() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![ActivateElementInstruction::new("r")],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["t".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(
        activated(&events, "e"),
        0,
        "outer terminated, not completed"
    );
    assert!(eiks(&engine, inst, "outer").is_empty());
    assert_eq!(eiks(&engine, inst, "r").len(), 1);
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
}

/// Zeebe `shouldTerminateAndActivateElementInTheSameScope`: the scope an
/// activation lands in is kept alive although its last child is terminated.
#[test]
fn terminate_and_activate_in_the_same_scope_keeps_the_scope() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let inner = eiks(&engine, inst, "inner")[0];
    let t = eiks(&engine, inst, "t")[0];
    engine
        .apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction::new("t")],
            vec![t],
        ))
        .unwrap();
    assert_eq!(eiks(&engine, inst, "inner"), vec![inner]);
    let ts = eiks(&engine, inst, "t");
    assert_eq!(ts.len(), 1);
    assert_ne!(ts[0], t);
}

/// Zeebe `ERROR_MESSAGE_ACTIVATION_FLOW_SCOPE_TERMINATED`: activating inside a
/// flow scope instance that the same command terminates is rejected.
#[test]
fn rejects_activation_inside_a_terminated_flow_scope() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let outer = eiks(&engine, inst, "outer")[0];
    let err = engine
        .apply_command(Command::modify_instance(
            inst,
            vec![ActivateElementInstruction::new("t")],
            vec![outer],
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::ModificationInstructionInvalid { .. }),
        "{err:?}"
    );
    assert!(
        err.to_string()
            .contains("whose required flow scope instance is also being terminated"),
        "{err}"
    );
    assert_eq!(eiks(&engine, inst, "outer"), vec![outer], "atomic");
}

/// Atomicity: a valid terminate bundled with an invalid activation applies
/// nothing.
#[test]
fn a_rejected_instruction_applies_nothing() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let t = eiks(&engine, inst, "t")[0];
    let before = engine.state().jobs.len();
    let err = engine.apply_command(Command::ModifyInstance {
        instance_key: inst,
        activate_instructions: vec![ActivateElementInstruction::new("r"), under("t", 424_242)],
        terminate_instructions: vec![t],
        terminate_element_ids: vec!["t".into()],
        move_instructions: vec![],
    });
    assert!(err.is_err());
    assert_eq!(eiks(&engine, inst, "t"), vec![t]);
    assert!(eiks(&engine, inst, "r").is_empty());
    assert_eq!(engine.state().jobs.len(), before);
}

/// Unknown element ids in terminate-by-id are a no-op (Zeebe parity: there
/// is simply no active instance to terminate).
#[test]
fn terminate_by_unknown_id_is_a_no_op() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, nested(), HashMap::new());
    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["ghost".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert!(events.is_empty(), "{events:?}");
    assert_eq!(eiks(&engine, inst, "t").len(), 1);
}

/// Zeebe sets an activation's element-scoped variables BEFORE the element
/// activates: the target's input mappings see them.
#[test]
fn element_scoped_variables_are_visible_to_the_targets_input_mappings() {
    let def = ProcessBuilder::new("seeded")
        .start_event("s")
        .service_task("a", "jobA")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=local + 1".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "a")
        .connect("a", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    let events = engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "t".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[("local", Value::Int(41))]),
                    scope_id: Some("t".into()),
                }],
                ..Default::default()
            }],
        ))
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { .. })),
        "input mapping must see the element-scoped variable: {events:?}"
    );
    let t = eiks(&engine, inst, "t")[0];
    let scope = &engine.instance(inst).unwrap().scope_variables[&t];
    assert_eq!(scope.get("local"), Some(&Value::Int(41)));
    assert_eq!(scope.get("y"), Some(&Value::Int(42)));
    assert_eq!(engine.activate_jobs("work", "w", 10, 60_000, 0).len(), 1);
}

/// Activations run in instruction order even when a later instruction seeds
/// element-scoped variables.
#[test]
fn element_scoped_activation_keeps_instruction_order() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());
    let events = engine
        .apply_command(modify(
            inst,
            vec![
                ActivateElementInstruction {
                    element_id: "a".into(),
                    ..Default::default()
                },
                ActivateElementInstruction {
                    element_id: "b".into(),
                    variable_instructions: vec![VariableInstruction {
                        variables: vars(&[("in_b", Value::Int(1))]),
                        scope_id: Some("b".into()),
                    }],
                    ..Default::default()
                },
            ],
        ))
        .unwrap();
    let job_types: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            Event::JobCreated { job_type, .. } => Some(job_type.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(job_types, vec!["jobA", "jobB"]);
}

/// A multi-instance target's element-scoped variables seed the MI body scope
/// BEFORE its activation evaluates: the input collection reads them.
#[test]
fn element_scoped_variables_feed_a_multi_instance_input_collection() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        multi_instance_service_process(false),
        vars(&[("items", Value::List(vec![Value::Int(1)]))]),
    );
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "each".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[(
                        "items",
                        Value::List(vec![Value::Int(7), Value::Int(8), Value::Int(9)]),
                    )]),
                    scope_id: Some("each".into()),
                }],
                ..Default::default()
            }],
        ))
        .unwrap();
    // Original body + 1 child, plus the new body + 3 children.
    assert_eq!(eiks(&engine, inst, "each").len(), 2 + 4);
}

/// Terminating one parallel MI child's last inner token releases the child
/// from its body's bookkeeping, so the sibling's completion still completes
/// the body (aggregating its output) and the token moves on.
#[test]
fn terminating_one_mi_child_still_lets_the_body_complete() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        multi_instance_subprocess(false),
        vars(&[(
            "tasks",
            Value::List(vec![Value::Str("x".into()), Value::Str("y".into())]),
        )]),
    );
    let first = eiks(&engine, inst, "impl")[0];
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![first],
            terminate_element_ids: vec![],
            move_instructions: vec![],
        })
        .unwrap();
    complete_all(&mut engine, "do-work");
    let instance = engine.instance(inst).unwrap();
    assert!(instance.multi_instances.is_empty(), "MI record left behind");
    assert!(instance.variables.contains_key("done_tasks"));
    assert_eq!(eiks(&engine, inst, "sink").len(), 1);
}

/// Resolving an input-mapping incident on a seeded element re-evaluates
/// against the same seeded element-local view as the first attempt.
#[test]
fn retry_of_a_seeded_activation_keeps_its_element_locals() {
    let def = ProcessBuilder::new("seeded-retry")
        .start_event("s")
        .service_task("a", "jobA")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=local + missing".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "a")
        .connect("a", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    let events = engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "t".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[("local", Value::Int(40))]),
                    scope_id: Some("t".into()),
                }],
                ..Default::default()
            }],
        ))
        .unwrap();
    let incident = events
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .expect("`missing` is absent: incident");
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("missing", Value::Int(2))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident))
        .unwrap();
    let t = eiks(&engine, inst, "t")[0];
    let scope = &engine.instance(inst).unwrap().scope_variables[&t];
    assert_eq!(scope.get("y"), Some(&Value::Int(42)));
    assert_eq!(engine.activate_jobs("work", "w", 10, 60_000, 0).len(), 1);
}

/// A created flow scope's boundary events are armed AFTER the instruction's
/// variables are written, so a correlation key supplied by the modification
/// is captured. (Like an ordinary activation, the boundary evaluates in the
/// host's enclosing scope view, so a global / outer-scope variable is used.)
#[test]
fn created_scope_boundaries_see_modification_variables() {
    let def = ProcessBuilder::new("boundary-key")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .message_boundary_event("mb", "sub", "cancel", "orderId")
        .end_event("end")
        .end_event("late")
        .connect("start", "a")
        .connect("a", "sub")
        .connect("ss", "b")
        .connect("b", "se")
        .connect("sub", "end")
        .connect("mb", "late")
        .build()
        .unwrap();
    {
        let mut engine = Engine::new();
        let inst = start(&mut engine, def, HashMap::new());
        engine
            .apply_command(modify(
                inst,
                vec![ActivateElementInstruction {
                    element_id: "b".into(),
                    variable_instructions: vec![VariableInstruction {
                        variables: vars(&[("orderId", Value::Str("o-1".into()))]),
                        scope_id: None,
                    }],
                    ..Default::default()
                }],
            ))
            .unwrap();
        let keys: Vec<&str> = engine
            .state()
            .message_subscriptions
            .values()
            .filter(|s| s.instance_key == inst && s.element_id == "sub")
            .map(|s| s.correlation_key.as_str())
            .collect();
        assert_eq!(keys, vec!["o-1"]);
    }
}
