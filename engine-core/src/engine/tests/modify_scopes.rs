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

/// Root-scope aliases — an absent scope, an empty scope id and the process id
/// all name the process root — are applied in REQUEST order, so a later
/// instruction overwrites an earlier one whichever alias it uses (Zeebe
/// `ModifyProcessInstanceVariablesTest` overwrite semantics). The REST mapper
/// relies on this: it passes variable instructions through verbatim.
#[test]
fn mixed_root_scope_variable_aliases_apply_in_request_order() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, task_then_sub(), HashMap::new());
    let vi = |scope: Option<&str>, value: i64| VariableInstruction {
        variables: vars(&[("x", Value::Int(value))]),
        scope_id: scope.map(str::to_string),
    };
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variable_instructions: vec![vi(Some("task-sub"), 1), vi(None, 2), vi(Some(""), 3)],
                ..Default::default()
            }],
        ))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("x"),
        Some(&Value::Int(3)),
        "the last root-scope instruction wins"
    );

    // And the reverse order: the explicit process-id alias wins when last.
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variable_instructions: vec![vi(None, 4), vi(Some("task-sub"), 5)],
                ..Default::default()
            }],
        ))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("x"),
        Some(&Value::Int(5)),
        "request order, not alias kind, decides"
    );
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
                    source: "=local + assert(missing, missing != null)".to_string(),
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

/// Preflight must validate each activation against the state the command
/// reaches when that activation applies — including EARLIER instructions'
/// variable writes. Here the first instruction stages root `k`; the second
/// opens `sub`, whose boundary correlation key `=k` resolves only through that
/// staged write. Application arms the boundary AFTER the write lands, so the
/// command is valid and preflight must accept it (#1398 review).
#[test]
fn preflight_sees_an_earlier_instructions_staged_root_write() {
    let def = ProcessBuilder::new("boundary-staged")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .message_boundary_event("mb", "sub", "cancel", "=k")
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
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    engine
        .apply_command(modify(
            inst,
            vec![
                ActivateElementInstruction {
                    element_id: "a".into(),
                    variables: vars(&[("k", Value::Str("o-1".into()))]),
                    ..Default::default()
                },
                ActivateElementInstruction {
                    element_id: "b".into(),
                    ..Default::default()
                },
            ],
        ))
        .unwrap();
    let keys: Vec<&str> = engine
        .state()
        .message_subscriptions
        .values()
        .filter(|s| s.instance_key == inst && s.element_id == "sub")
        .map(|s| s.correlation_key.as_str())
        .collect();
    assert_eq!(
        keys,
        vec!["o-1"],
        "the boundary armed against the earlier instruction's staged write"
    );
}

/// The mirror image: preflight must not ACCEPT a command whose boundary
/// expression fails only once an earlier instruction's staged write is in
/// view — otherwise the failure surfaces as an incident AFTER the earlier
/// instruction's writes and activations were already emitted, breaking the
/// command's all-or-nothing guarantee (#1398 review).
#[test]
fn preflight_rejects_a_boundary_failing_only_under_an_earlier_staged_write() {
    let def = ProcessBuilder::new("boundary-staged-bad")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .message_boundary_event("mb", "sub", "cancel", "=k")
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
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    // `k` is a NUMBER: the correlation key needs a STRING/number-renderable —
    // but a MAP value fails extraction outright, and only the earlier
    // instruction's staged write brings it into view.
    let result = engine.apply_command(modify(
        inst,
        vec![
            ActivateElementInstruction {
                element_id: "a".into(),
                variables: vars(&[(
                    "k",
                    Value::Map(std::collections::BTreeMap::from([(
                        "nested".to_string(),
                        Value::Int(1),
                    )])),
                )]),
                ..Default::default()
            },
            ActivateElementInstruction {
                element_id: "b".into(),
                ..Default::default()
            },
        ],
    ));
    let err = result.expect_err("the boundary fails under the staged write");
    assert!(
        format!("{err:?}").contains("Expected to subscribe to catch event(s) of 'sub'"),
        "{err:?}"
    );
    // All-or-nothing: the rejected command emits nothing — no staged root
    // write, no created scope, no boundary subscription.
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("k"),
        None,
        "the earlier instruction's root write was not applied"
    );
    assert!(
        eiks(&engine, inst, "sub").is_empty(),
        "no flow scope was created"
    );
    assert!(
        engine
            .state()
            .message_subscriptions
            .values()
            .all(|s| s.instance_key != inst),
        "no boundary subscription was armed"
    );
}

/// Activations of one command share the flow scopes the command creates
/// (application reuses them), so a later activation targeting an element
/// directly under such a scope must RESOLVE into it: two instructions for
/// sibling elements of one not-yet-existing sub-process land in ONE `sub`
/// instance, not two (#1398 review).
#[test]
fn preflight_reuses_a_scope_created_by_an_earlier_instruction() {
    let def = ProcessBuilder::new("scope-reuse")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .service_task("c", "jobC")
        .contained_in("c", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .end_event("end")
        .connect("start", "a")
        .connect("a", "sub")
        .connect("ss", "b")
        .connect("ss", "c")
        .connect("b", "se")
        .connect("c", "se")
        .connect("sub", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    engine
        .apply_command(modify(
            inst,
            vec![
                ActivateElementInstruction {
                    element_id: "b".into(),
                    ..Default::default()
                },
                ActivateElementInstruction {
                    element_id: "c".into(),
                    ..Default::default()
                },
            ],
        ))
        .unwrap();
    assert_eq!(
        eiks(&engine, inst, "sub").len(),
        1,
        "both activations share the one created sub-process scope"
    );
    let sub = eiks(&engine, inst, "sub")[0];
    for id in ["b", "c"] {
        let keys = eiks(&engine, inst, id);
        assert_eq!(keys.len(), 1, "one {id} instance");
        assert_eq!(
            scope_of(&engine, inst, keys[0]),
            sub,
            "{id} lands in the shared created scope"
        );
    }
}

/// A nested created scope's boundary is checked against the view that
/// includes the OUTER scope's staged locals: the first instruction opens
/// `outer` (staging `k` as its local) and `inner` (whose boundary key is
/// `=k`). Application arms `inner`'s boundary after `outer`'s local is
/// written, so the command is valid — preflight must carry the outer scope's
/// staged write into the nested scope's view (#1398 review).
#[test]
fn preflight_carries_an_outer_created_scopes_locals_into_a_nested_scope() {
    let def = ProcessBuilder::new("nested-staged")
        .start_event("start")
        .service_task("a", "jobA")
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
        .message_boundary_event("mb", "inner", "cancel", "=k")
        .end_event("end")
        .end_event("late")
        .connect("start", "a")
        .connect("a", "outer")
        .connect("os", "inner")
        .connect("is", "t")
        .connect("t", "ie")
        .connect("inner", "oe")
        .connect("outer", "end")
        .connect("mb", "late")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "t".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[("k", Value::Str("o-1".into()))]),
                    scope_id: Some("outer".into()),
                }],
                ..Default::default()
            }],
        ))
        .unwrap();
    let keys: Vec<&str> = engine
        .state()
        .message_subscriptions
        .values()
        .filter(|s| s.instance_key == inst && s.element_id == "inner")
        .map(|s| s.correlation_key.as_str())
        .collect();
    assert_eq!(
        keys,
        vec!["o-1"],
        "the nested scope's boundary saw the outer scope's staged local"
    );
}

/// The preflight checks a created scope's boundaries against the SAME
/// parent-scope view application arms them in: a correlation key supplied only
/// as a local of the new sub-process itself is not visible to its own
/// boundary, so the command is rejected up front — never accepted with the
/// subscription silently dropped.
#[test]
fn created_scope_own_locals_do_not_satisfy_its_boundary_preflight() {
    let def = ProcessBuilder::new("boundary-own-local")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .message_boundary_event("mb", "sub", "cancel", "=k")
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
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    let result = engine.apply_command(modify(
        inst,
        vec![ActivateElementInstruction {
            element_id: "b".into(),
            variable_instructions: vec![VariableInstruction {
                variables: vars(&[("k", Value::Str("o-1".into()))]),
                scope_id: Some("sub".into()),
            }],
            ..Default::default()
        }],
    ));
    let err = result.expect_err("the boundary cannot see the scope's own local");
    assert!(
        format!("{err:?}").contains("Expected to subscribe to catch event(s) of 'sub'"),
        "{err:?}"
    );
}

/// Terminating a half-open parallel join by element id resets its join
/// bookkeeping: the remaining branch's arrival must not fire the join against
/// the cancelled branch's arrival.
#[test]
fn terminating_a_waiting_join_discards_its_arrivals() {
    let def = ProcessBuilder::new("join-mod")
        .start_event("start")
        .parallel_gateway("split")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .end_event("done")
        .connect("start", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    complete_all(&mut engine, "ja");
    assert_eq!(eiks(&engine, inst, "join").len(), 1, "join is waiting");
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["join".into()],
            move_instructions: vec![],
        })
        .unwrap();
    let events = {
        let jobs = engine.activate_jobs("jb", "w", 10, 60_000, 0);
        assert_eq!(jobs.len(), 1);
        engine
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap()
    };
    assert_eq!(
        activated(&events, "done"),
        0,
        "join fired on a cancelled arrival"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .join_flows_taken(&crate::state::JoinKey::root("join")),
        1
    );
}

/// A multi-instance body parked on an input-collection incident (active, but
/// not yet registered in `multi_instances`) is still a body: it is neither an
/// implicit flow scope nor a selectable ancestor for its inner elements.
#[test]
fn an_incident_parked_multi_instance_body_is_not_a_flow_scope() {
    let mut def = multi_instance_subprocess(false);
    def.elements.get_mut("wave").unwrap().io = crate::model::IoMapping {
        // A genuine body-level input failure (`badFactor` is a string).
        inputs: vec![crate::model::Mapping {
            source: "=assert(badFactor, badFactor instance of number) + 1".to_string(),
            target: "scaled".to_string(),
        }],
        outputs: Vec::new(),
    };
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        def,
        vars(&[
            ("tasks", Value::List(vec![Value::Str("x".into())])),
            ("badFactor", Value::Str("oops".into())),
        ]),
    );
    let bodies = eiks(&engine, inst, "wave");
    assert_eq!(bodies.len(), 1, "body parked on its incident");
    assert!(engine.instance(inst).unwrap().multi_instances.is_empty());

    let err = engine
        .apply_command(modify(inst, vec![ActivateElementInstruction::new("impl")]))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::ElementNotActivatable { element_id, .. } if element_id == "impl"),
        "{err:?}"
    );
    let err = engine
        .apply_command(modify(inst, vec![under("impl", bodies[0])]))
        .unwrap_err();
    assert!(
        err.to_string().contains("inside a multi-instance body"),
        "{err}"
    );
    assert!(eiks(&engine, inst, "impl").is_empty());
}

// ---- Element-local variables at completion / on joins / on retries ---------

fn seeded(element_id: &str, locals: &[(&str, Value)]) -> ActivateElementInstruction {
    ActivateElementInstruction {
        element_id: element_id.into(),
        variable_instructions: vec![VariableInstruction {
            variables: vars(locals),
            scope_id: Some(element_id.into()),
        }],
        ..Default::default()
    }
}

/// A script task's completion-time expression reads the element's own
/// (seeded) scope: the element-local `x` shadows the root `x`.
#[test]
fn completion_time_script_reads_element_local_variables() {
    let def = ProcessBuilder::new("seeded-script")
        .start_event("s")
        .service_task("a", "jobA")
        .script_task("t", "=x + 1", "y")
        .service_task("w", "jobW")
        .connect("s", "a")
        .connect("a", "t")
        .connect("t", "w")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, vars(&[("x", Value::Int(1))]));
    engine
        .apply_command(modify(inst, vec![seeded("t", &[("x", Value::Int(41))])]))
        .unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().variables.get("y"),
        Some(&Value::Int(42)),
        "the script must evaluate against its element-local scope"
    );
}

/// A gateway's flow selection reads the gateway's own (seeded) scope.
#[test]
fn gateway_selection_reads_element_local_variables() {
    let def = ProcessBuilder::new("seeded-gw")
        .start_event("s")
        .service_task("a", "jobA")
        .exclusive_gateway("g")
        .service_task("yes", "jobYes")
        .service_task("no", "jobNo")
        .connect("s", "a")
        .connect("a", "g")
        .connect_when("g", "yes", "=x = 41")
        .connect_default("g", "no")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, vars(&[("x", Value::Int(1))]));
    engine
        .apply_command(modify(inst, vec![seeded("g", &[("x", Value::Int(41))])]))
        .unwrap();
    assert_eq!(eiks(&engine, inst, "yes").len(), 1, "took the local branch");
    assert!(eiks(&engine, inst, "no").is_empty());
}

/// `start -> fork -> {a(ja), b(jb)} -> join(parallel) -> done(jd)`.
fn parallel_join() -> ProcessDefinition {
    ProcessBuilder::new("pjoin")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .service_task("done", "jd")
        .connect("start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "done")
        .build()
        .unwrap()
}

/// A seeded join activation is queued behind the command's terminations like
/// every other activation: terminating the waiting join and activating it with
/// locals in one command must not fire it on the cancelled arrival.
#[test]
fn seeded_join_activation_runs_after_terminations() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, parallel_join(), HashMap::new());
    complete_all(&mut engine, "ja");
    assert_eq!(eiks(&engine, inst, "join").len(), 1, "join is waiting");
    let events = engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![seeded("join", &[("l", Value::Int(1))])],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["join".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(
        activated(&events, "done"),
        0,
        "join fired on a cancelled arrival: {events:?}"
    );
    let join = eiks(&engine, inst, "join");
    assert_eq!(join.len(), 1, "the activation waits on a fresh join");
    assert_eq!(
        engine.instance(inst).unwrap().scope_variables[&join[0]].get("l"),
        Some(&Value::Int(1)),
        "the waiting join keeps its locals"
    );
}

/// `start -> fork -> {a(ja), b(jb)} -> join(inclusive) -> yes [=l = 1] | no`.
fn inclusive_join() -> ProcessDefinition {
    ProcessBuilder::new("ijoin")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .inclusive_gateway("join")
        .service_task("yes", "jyes")
        .service_task("no", "jno")
        .connect("start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect_when("join", "yes", "=l = 1")
        .connect_default("join", "no")
        .build()
        .unwrap()
}

/// Reusing a waiting join (which emits no `ElementActivated`) still seeds it
/// before it completes and routes.
#[test]
fn reused_waiting_join_routes_with_its_locals() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, inclusive_join(), HashMap::new());
    complete_all(&mut engine, "ja");
    assert_eq!(eiks(&engine, inst, "join").len(), 1, "join is waiting");
    engine
        .apply_command(modify(inst, vec![seeded("join", &[("l", Value::Int(1))])]))
        .unwrap();
    assert_eq!(eiks(&engine, inst, "yes").len(), 1, "routed on the local");
    assert!(eiks(&engine, inst, "no").is_empty());
}

/// A fresh join accepted on its first activation is seeded before it routes.
#[test]
fn fresh_join_routes_with_its_locals() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, inclusive_join(), HashMap::new());
    complete_all(&mut engine, "ja");
    complete_all(&mut engine, "jb");
    // Both branches arrived without the local: the default route was taken.
    assert_eq!(eiks(&engine, inst, "no").len(), 1);
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![seeded("join", &[("l", Value::Int(1))])],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["no".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert_eq!(eiks(&engine, inst, "yes").len(), 1, "routed on the local");
}

/// A call activity seeded with element-local variables keeps them on a spawn
/// retry: the re-driven input mapping evaluates in the element's own view.
#[test]
fn call_activity_spawn_retry_keeps_element_locals() {
    let child = ProcessBuilder::new("phase")
        .start_event("pstart")
        .service_task("work", "child-work")
        .end_event("pend")
        .connect("pstart", "work")
        .connect("work", "pend")
        .build()
        .unwrap();
    let parent = ProcessBuilder::new("orch")
        .start_event("start")
        .service_task("a", "jobA")
        .call_activity("c1", "phase")
        .with_io(
            "c1",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=local + assert(missing, missing != null)".to_string(),
                    target: "childOrder".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "a")
        .connect("a", "c1")
        .connect("c1", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(child)).unwrap();
    let inst = start(&mut engine, parent, HashMap::new());
    engine
        .apply_command(modify(
            inst,
            vec![seeded("c1", &[("local", Value::Int(1))])],
        ))
        .unwrap();
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "input mapping parks on `missing`");
    let incident_key = incidents[0].key;
    engine
        .apply_command(Command::set_variables(
            inst,
            vars(&[("missing", Value::Int(41))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "the retry must see the element-local `local`: {:?}",
        engine.active_incidents()
    );
    let child = engine
        .state()
        .instances
        .values()
        .find(|i| i.process_id == "phase")
        .expect("child spawned on retry");
    assert_eq!(child.variables.get("childOrder"), Some(&Value::Int(42)));
}

/// `start -> sub[ ss -> fork -> {a(ja), b(jb)} -> join(parallel) -> se ] -> end`.
fn join_in_sub() -> ProcessDefinition {
    let mut b = ProcessBuilder::new("join-sub")
        .start_event("start")
        .sub_process("sub", "ss")
        .start_event("ss")
        .parallel_gateway("fork")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .end_event("se")
        .end_event("end");
    for id in ["ss", "fork", "a", "b", "join", "se"] {
        b = b.contained_in(id, "sub");
    }
    b.connect("start", "sub")
        .connect("ss", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "se")
        .connect("sub", "end")
        .build()
        .unwrap()
}

/// Join bookkeeping is keyed per flow-scope instance (#1580, Zeebe parity), so
/// activating a join under a flow-scope instance other than the one its
/// waiting instance lives in opens an INDEPENDENT join there: the waiting
/// join keeps its arrivals untouched and neither fires (each has one of its
/// two flows). Both the plain and the element-local-variable activation paths.
#[test]
fn join_activation_in_another_flow_scope_opens_an_independent_join() {
    for locals in [false, true] {
        let mut engine = Engine::new();
        let inst = start(&mut engine, join_in_sub(), HashMap::new());
        complete_all(&mut engine, "ja");
        let waiting = eiks(&engine, inst, "join");
        assert_eq!(waiting.len(), 1, "join is waiting");
        let waiting_scope = scope_of(&engine, inst, waiting[0]);
        // A second `sub` instance, created by activating `b` from the root.
        engine
            .apply_command(modify(inst, vec![under("b", inst)]))
            .unwrap();
        let subs = eiks(&engine, inst, "sub");
        assert_eq!(subs.len(), 2, "two sub instances");
        let other = *subs.iter().find(|s| **s != waiting_scope).unwrap();
        let mut instruction = under("join", other);
        if locals {
            instruction.variable_instructions = vec![VariableInstruction {
                variables: HashMap::from([("l".to_string(), Value::Int(1))]),
                scope_id: Some("join".into()),
            }];
        }
        let events = engine
            .apply_command(modify(inst, vec![instruction]))
            .unwrap_or_else(|e| panic!("locals={locals}: {e}"));
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, Event::ParallelJoinFired { .. })),
            "locals={locals}: neither scope's join has both flows"
        );
        let instance = engine.instance(inst).unwrap();
        let waiting_key = crate::state::JoinKey::new(waiting_scope, "join");
        let other_key = crate::state::JoinKey::new(other, "join");
        assert_eq!(
            instance.join_flows_taken(&waiting_key),
            1,
            "locals={locals}"
        );
        assert_eq!(instance.join_flows_taken(&other_key), 1, "locals={locals}");
        assert_eq!(
            instance.join_instances.get(&waiting_key),
            Some(&waiting[0]),
            "locals={locals}: the waiting join is untouched"
        );
        assert_eq!(eiks(&engine, inst, "join").len(), 2, "locals={locals}");
    }
}

/// Two activations of the same join under DIFFERENT sub-process instances
/// land in two independent joins (#1580, Zeebe parity): each sees one
/// unidentified arrival of its two flows, so neither fires.
#[test]
fn planned_join_activations_across_scopes_open_independent_joins() {
    for locals in [false, true] {
        let mut engine = Engine::new();
        let inst = start(&mut engine, join_in_sub(), HashMap::new());
        // A second `sub` instance, created by activating `a` from the root.
        engine
            .apply_command(modify(inst, vec![under("a", inst)]))
            .unwrap();
        let subs = eiks(&engine, inst, "sub");
        assert_eq!(subs.len(), 2, "two sub instances");
        assert!(eiks(&engine, inst, "join").is_empty(), "no join waiting");
        let activate = |ancestor: Key| {
            let mut instruction = under("join", ancestor);
            if locals {
                instruction.variable_instructions = vec![VariableInstruction {
                    variables: HashMap::from([("l".to_string(), Value::Int(1))]),
                    scope_id: Some("join".into()),
                }];
            }
            instruction
        };
        let events = engine
            .apply_command(modify(inst, vec![activate(subs[0]), activate(subs[1])]))
            .unwrap_or_else(|e| panic!("locals={locals}: {e}"));
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, Event::ParallelJoinFired { .. })),
            "locals={locals}: arrivals in two scopes never combine"
        );
        let instance = engine.instance(inst).unwrap();
        for sub in &subs {
            assert_eq!(
                instance.join_flows_taken(&crate::state::JoinKey::new(*sub, "join")),
                1,
                "locals={locals}: one arrival per scope instance"
            );
        }
        assert_eq!(eiks(&engine, inst, "join").len(), 2, "locals={locals}");
    }
}

/// `start -> fork -> {sub1[ s1 -> j1(parallel) -> e1 ], sub2[ s2 -> j2(parallel) -> e2 ]}`
/// with each join awaiting a second (never-taken) branch so it stays waiting.
fn two_joins_in_two_subs() -> ProcessDefinition {
    let mut b = ProcessBuilder::new("two-joins")
        .start_event("start")
        .parallel_gateway("fork")
        .sub_process("sub1", "s1")
        .start_event("s1")
        .service_task("t1", "jt1")
        .parallel_gateway("j1")
        .end_event("e1")
        .sub_process("sub2", "s2")
        .start_event("s2")
        .service_task("t2", "jt2")
        .parallel_gateway("j2")
        .end_event("e2")
        .end_event("end1")
        .end_event("end2");
    for id in ["s1", "t1", "j1", "e1"] {
        b = b.contained_in(id, "sub1");
    }
    for id in ["s2", "t2", "j2", "e2"] {
        b = b.contained_in(id, "sub2");
    }
    b.connect("start", "fork")
        .connect("fork", "sub1")
        .connect("fork", "sub2")
        .connect("s1", "t1")
        .connect("s1", "j1")
        .connect("t1", "j1")
        .connect("j1", "e1")
        .connect("s2", "t2")
        .connect("s2", "j2")
        .connect("t2", "j2")
        .connect("j2", "e2")
        .connect("sub1", "end1")
        .connect("sub2", "end2")
        .build()
        .unwrap()
}

/// Distinct join elements keep separately keyed bookkeeping, so activating
/// `j1` in one sub-process instance and `j2` in another must be accepted even
/// though the two activations land in different flow scopes.
#[test]
fn allows_distinct_join_activations_in_different_scopes() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, two_joins_in_two_subs(), HashMap::new());
    let sub1 = eiks(&engine, inst, "sub1");
    let sub2 = eiks(&engine, inst, "sub2");
    assert_eq!((sub1.len(), sub2.len()), (1, 1));
    engine
        .apply_command(modify(
            inst,
            vec![under("j1", sub1[0]), under("j2", sub2[0])],
        ))
        .expect("distinct joins in different scopes must be accepted");
}

/// The allowed counterpart: two activations of the same join whose resolution
/// shares the to-be-created flow-scope instance land together — the command
/// opens ONE `sub` for both, and the two arrivals legitimately fire the join
/// (both tokens genuinely arrived in that one scope).
#[test]
fn allows_planned_join_activations_sharing_a_created_scope() {
    let mut engine = Engine::new();
    let inst = start(&mut engine, join_in_sub(), HashMap::new());
    let events = engine
        .apply_command(modify(
            inst,
            vec![
                under("join", inst),
                ActivateElementInstruction {
                    element_id: "join".into(),
                    ancestor_element_instance_key: Some(inst),
                    variable_instructions: vec![VariableInstruction {
                        variables: HashMap::from([("l".to_string(), Value::Int(1))]),
                        scope_id: Some("join".into()),
                    }],
                    ..Default::default()
                },
            ],
        ))
        .unwrap();
    assert_eq!(
        activated(&events, "sub"),
        1,
        "one shared created scope: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::ParallelJoinFired { .. }))
            .count(),
        1,
        "two arrivals in one shared scope fire the join once: {events:?}"
    );
    assert_eq!(
        activated(&events, "se"),
        1,
        "the fired join routes onward: {events:?}"
    );
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active,
        "the original branch's tasks are still active"
    );
    assert_eq!(
        activated(&events, "end"),
        1,
        "the created sub-process drains to the process end: {events:?}"
    );
}

/// `pre` → ad-hoc container `agent` whose tools are a service task and an
/// embedded sub-process tool `toolSub` (containing `inner`).
fn pre_then_adhoc() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="adhoc-mod">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="pre">
            <bpmn:extensionElements><zeebe:taskDefinition type="pre" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:subProcess id="toolSub">
              <bpmn:startEvent id="ss" />
              <bpmn:serviceTask id="inner">
                <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="se" />
              <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="inner" />
              <bpmn:sequenceFlow id="sf2" sourceRef="inner" targetRef="se" />
            </bpmn:subProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="pre" />
          <bpmn:sequenceFlow id="f2" sourceRef="pre" targetRef="agent" />
          <bpmn:sequenceFlow id="f3" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

#[test]
fn rejects_activation_that_would_open_an_inactive_adhoc_container() {
    // Copilot on #1418: opening an ad-hoc container through the generic
    // flow-scope path skips its ad-hoc lifecycle (`AdHocActivated`, tool
    // ownership), stranding the container once the inner flow completes.
    // Fail closed for every element nested in an inactive container.
    for element in ["inner", "toolSub"] {
        let mut engine = Engine::new();
        let inst = start(&mut engine, pre_then_adhoc(), HashMap::new());
        let err = engine
            .apply_command(modify(
                inst,
                vec![ActivateElementInstruction {
                    element_id: element.into(),
                    ..Default::default()
                }],
            ))
            .unwrap_err();
        assert!(
            matches!(
                &err,
                EngineError::ElementNotActivatable { reason, .. } if reason.contains("ad-hoc")
            ),
            "{element}: got {err:?}"
        );
        assert!(eiks(&engine, inst, "agent").is_empty(), "nothing opened");
    }
}

fn nested_boundary_def() -> ProcessDefinition {
    // `outer` (running, parked on `wait`) contains `inner`, whose message
    // boundary correlates on `=k`. `outer` declares a local `k` via its input
    // mapping only when `withLocal` is set.
    let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
      <bpmn:message id="msg" name="cancel"><bpmn:extensionElements><zeebe:subscription correlationKey="=k" /></bpmn:extensionElements></bpmn:message>
      <bpmn:process id="nested-staged" isExecutable="true">
        <bpmn:startEvent id="start" />
        <bpmn:subProcess id="outer">
          <bpmn:extensionElements><zeebe:ioMapping><zeebe:input source="=if withLocal then &quot;local&quot; else null" target="k" /></zeebe:ioMapping></bpmn:extensionElements>
          <bpmn:startEvent id="os" />
          <bpmn:serviceTask id="wait"><bpmn:extensionElements><zeebe:taskDefinition type="w" /></bpmn:extensionElements></bpmn:serviceTask>
          <bpmn:subProcess id="inner">
            <bpmn:startEvent id="is" />
            <bpmn:serviceTask id="b"><bpmn:extensionElements><zeebe:taskDefinition type="jb" /></bpmn:extensionElements></bpmn:serviceTask>
            <bpmn:sequenceFlow id="i1" sourceRef="is" targetRef="b" />
          </bpmn:subProcess>
          <bpmn:boundaryEvent id="mb" attachedToRef="inner"><bpmn:messageEventDefinition messageRef="msg" /></bpmn:boundaryEvent>
          <bpmn:endEvent id="late" />
          <bpmn:sequenceFlow id="o1" sourceRef="os" targetRef="wait" />
          <bpmn:sequenceFlow id="o2" sourceRef="mb" targetRef="late" />
        </bpmn:subProcess>
        <bpmn:endEvent id="end" />
        <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="outer" />
        <bpmn:sequenceFlow id="f2" sourceRef="outer" targetRef="end" />
      </bpmn:process></bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn inner_subscription_keys(engine: &Engine, inst: Key) -> Vec<String> {
    engine
        .state()
        .message_subscriptions
        .values()
        .filter(|s| s.instance_key == inst && s.element_id == "inner")
        .map(|s| s.correlation_key.clone())
        .collect()
}

/// A variable instruction on an EXISTING ancestor scope (`outer`) is applied
/// before the created scope's (`inner`) boundaries arm, so the preflight must
/// see it too — rejecting would refuse a command that applies cleanly
/// (#1398 review).
#[test]
fn staged_write_to_existing_ancestor_scope_satisfies_boundary_preflight() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        nested_boundary_def(),
        vars(&[("withLocal", Value::Bool(false))]),
    );
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[("k", Value::Str("o-1".into()))]),
                    scope_id: Some("outer".into()),
                }],
                ..Default::default()
            }],
        ))
        .expect("the staged ancestor write is visible to the boundary");
    assert_eq!(inner_subscription_keys(&engine, inst), vec!["o-1"]);
}

/// A staged ROOT write must not override a nearer existing local of the same
/// name in the preflight view: application arms against `outer`'s local `k`,
/// so an unusable root `k` must not reject the command (#1398 review).
#[test]
fn staged_root_write_does_not_override_nearer_local_in_boundary_preflight() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        nested_boundary_def(),
        vars(&[("withLocal", Value::Bool(true))]),
    );
    engine
        .apply_command(modify(
            inst,
            vec![ActivateElementInstruction {
                element_id: "b".into(),
                variable_instructions: vec![VariableInstruction {
                    variables: vars(&[("k", Value::List(vec![]))]),
                    scope_id: None,
                }],
                ..Default::default()
            }],
        ))
        .expect("the boundary reads outer's local `k`, not the root write");
    assert_eq!(inner_subscription_keys(&engine, inst), vec!["local"]);
}

/// An earlier instruction's write to an EXISTING scope instance is visible
/// only on that instance's lineage. With two running `outer` instances, a
/// write to `outer` A must not satisfy the boundary of an `inner` created
/// under `outer` B — B's own `k` is null, so application would fail arming
/// after A's write already landed. Preflight must reject the whole command
/// (#1398 review).
#[test]
fn staged_write_to_one_existing_scope_instance_is_invisible_to_a_sibling_instance() {
    let mut engine = Engine::new();
    let inst = start(
        &mut engine,
        nested_boundary_def(),
        vars(&[("withLocal", Value::Bool(false))]),
    );
    engine
        .apply_command(modify(inst, vec![under("wait", inst)]))
        .unwrap();
    let outers = eiks(&engine, inst, "outer");
    assert_eq!(outers.len(), 2, "two running outer instances");
    let mut write_a = under("b", outers[0]);
    write_a.variable_instructions = vec![VariableInstruction {
        variables: vars(&[("k", Value::Str("o-1".into()))]),
        scope_id: Some("outer".into()),
    }];
    let result = engine.apply_command(modify(inst, vec![write_a, under("b", outers[1])]));
    let err = result.expect_err("outer B's boundary key is still null");
    assert!(
        format!("{err:?}").contains("Expected to subscribe to catch event(s) of 'inner'"),
        "{err:?}"
    );
    assert!(eiks(&engine, inst, "inner").is_empty(), "nothing applied");
    assert!(inner_subscription_keys(&engine, inst).is_empty());
}

/// Created-scope locals shadow EXISTING ancestor locals: `outer` (existing)
/// holds an unusable map `k`; the first instruction creates `middle` with a
/// local `k = "ready"`; the second creates `inner` under that reused `middle`.
/// `inner`'s boundary reads `middle`'s `k`, so the command is valid
/// (#1398 review).
#[test]
fn created_scope_local_shadows_existing_ancestor_local_in_preflight() {
    let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
      <bpmn:message id="msg" name="cancel"><bpmn:extensionElements><zeebe:subscription correlationKey="=k" /></bpmn:extensionElements></bpmn:message>
      <bpmn:process id="shadow" isExecutable="true">
        <bpmn:startEvent id="start" />
        <bpmn:subProcess id="outer">
          <bpmn:extensionElements><zeebe:ioMapping><zeebe:input source="={a: 1}" target="k" /></zeebe:ioMapping></bpmn:extensionElements>
          <bpmn:startEvent id="os" />
          <bpmn:serviceTask id="wait"><bpmn:extensionElements><zeebe:taskDefinition type="w" /></bpmn:extensionElements></bpmn:serviceTask>
          <bpmn:subProcess id="middle">
            <bpmn:startEvent id="ms" />
            <bpmn:serviceTask id="m"><bpmn:extensionElements><zeebe:taskDefinition type="jm" /></bpmn:extensionElements></bpmn:serviceTask>
            <bpmn:subProcess id="inner">
              <bpmn:startEvent id="is" />
              <bpmn:serviceTask id="b"><bpmn:extensionElements><zeebe:taskDefinition type="jb" /></bpmn:extensionElements></bpmn:serviceTask>
              <bpmn:sequenceFlow id="i1" sourceRef="is" targetRef="b" />
            </bpmn:subProcess>
            <bpmn:boundaryEvent id="mb" attachedToRef="inner"><bpmn:messageEventDefinition messageRef="msg" /></bpmn:boundaryEvent>
            <bpmn:endEvent id="late" />
            <bpmn:sequenceFlow id="m1" sourceRef="ms" targetRef="m" />
            <bpmn:sequenceFlow id="m2" sourceRef="mb" targetRef="late" />
          </bpmn:subProcess>
          <bpmn:sequenceFlow id="o1" sourceRef="os" targetRef="wait" />
        </bpmn:subProcess>
        <bpmn:endEvent id="end" />
        <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="outer" />
        <bpmn:sequenceFlow id="f2" sourceRef="outer" targetRef="end" />
      </bpmn:process></bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    engine
        .apply_command(modify(
            inst,
            vec![
                ActivateElementInstruction {
                    element_id: "m".into(),
                    variable_instructions: vec![VariableInstruction {
                        variables: vars(&[("k", Value::Str("ready".into()))]),
                        scope_id: Some("middle".into()),
                    }],
                    ..Default::default()
                },
                ActivateElementInstruction {
                    element_id: "b".into(),
                    ..Default::default()
                },
            ],
        ))
        .expect("inner's boundary reads middle's local `k`");
    assert_eq!(eiks(&engine, inst, "middle").len(), 1, "middle is shared");
    assert_eq!(inner_subscription_keys(&engine, inst), vec!["ready"]);
}

/// Two activations in one command sharing a sub-process the command creates:
/// application creates (and arms) the scope once, on the first activation, so
/// the boundary preflight runs only there. A later instruction that changes the
/// variable the boundary reads must not re-check (and reject) the already
/// captured subscription (#1398 review).
#[test]
fn shared_created_scope_boundary_is_checked_only_on_first_creation() {
    let def = ProcessBuilder::new("boundary-shared-scope")
        .start_event("start")
        .service_task("a", "jobA")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .parallel_gateway("fork")
        .contained_in("fork", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .service_task("c", "jobC")
        .contained_in("c", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .end_event("se2")
        .contained_in("se2", "sub")
        .message_boundary_event("mb", "sub", "cancel", "=k")
        .end_event("end")
        .end_event("late")
        .connect("start", "a")
        .connect("a", "sub")
        .connect("ss", "fork")
        .connect("fork", "b")
        .connect("fork", "c")
        .connect("b", "se")
        .connect("c", "se2")
        .connect("sub", "end")
        .connect("mb", "late")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let inst = start(&mut engine, def, HashMap::new());
    engine
        .apply_command(modify(
            inst,
            vec![
                ActivateElementInstruction {
                    element_id: "b".into(),
                    variable_instructions: vec![VariableInstruction {
                        variables: vars(&[("k", Value::Str("ready".into()))]),
                        scope_id: None,
                    }],
                    ..Default::default()
                },
                ActivateElementInstruction {
                    element_id: "c".into(),
                    variable_instructions: vec![VariableInstruction {
                        variables: vars(&[("k", Value::Null)]),
                        scope_id: None,
                    }],
                    ..Default::default()
                },
            ],
        ))
        .expect("the shared scope armed once, with k = \"ready\"");
    let keys: Vec<&str> = engine
        .state()
        .message_subscriptions
        .values()
        .filter(|s| s.instance_key == inst && s.element_id == "sub")
        .map(|s| s.correlation_key.as_str())
        .collect();
    assert_eq!(keys, vec!["ready"]);
}
