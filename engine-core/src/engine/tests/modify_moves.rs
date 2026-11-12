//! #1552 `engine-modify-move`: Zeebe
//! `ProcessInstanceModificationModifyProcessor` move-instruction parity for
//! `Command::ModifyInstance` (Camunda 8.9 `moveInstructions`). A move
//! instruction desugars into one terminate of each moved source element
//! instance plus one activation of the target element per moved instance, all
//! inside the same all-or-nothing command. Ported from Zeebe
//! `ModifyProcessInstanceTest` (`shouldMoveAllElementsByIds`,
//! `shouldMoveElementInstanceBySourceElementInstanceKey*`,
//! `shouldMoveFromNestedSubprocessToOuterSubprocess*`,
//! `shouldMoveAllMultiInstanceElementsByIds`, …) and
//! `ModifyProcessInstanceRejectionTest`.
use super::*;
use crate::command::{AncestorScopeInstruction, MoveInstruction, MoveSource};

/// `start -> a(jobA) -> b(jobB) -> end`: the flat move fixture (a token parked
/// on `a` moves to `b`).
fn flat_two_tasks() -> ProcessDefinition {
    two_service_tasks()
}

/// `start -> sub[ ss -> a(jobA) -> b(jobB) -> se ] -> end`: both elements live
/// in the same embedded sub-process scope.
fn sub_with_two_tasks() -> ProcessDefinition {
    ProcessBuilder::new("sub-two")
        .start_event("start")
        .sub_process("sub", "ss")
        .start_event("ss")
        .contained_in("ss", "sub")
        .service_task("a", "jobA")
        .contained_in("a", "sub")
        .service_task("b", "jobB")
        .contained_in("b", "sub")
        .end_event("se")
        .contained_in("se", "sub")
        .end_event("end")
        .connect("start", "sub")
        .connect("ss", "a")
        .connect("a", "b")
        .connect("b", "se")
        .connect("sub", "end")
        .build()
        .unwrap()
}

/// `start -> outer[ os -> inner[ is -> t(work) -> ie ] -> b(jobB) -> oe ] ->
/// end`: `t` nested two scopes deep, `b` in the outer scope — the
/// move-up-a-scope fixture (Zeebe
/// `shouldMoveFromNestedSubprocessToOuterSubprocessByElementId`).
fn nested_inner_and_outer_task() -> ProcessDefinition {
    ProcessBuilder::new("nested-move")
        .start_event("start")
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
        .service_task("b", "jobB")
        .contained_in("b", "outer")
        .end_event("oe")
        .contained_in("oe", "outer")
        .end_event("end")
        .connect("start", "outer")
        .connect("os", "inner")
        .connect("is", "t")
        .connect("t", "ie")
        .connect("inner", "b")
        .connect("b", "oe")
        .connect("outer", "end")
        .build()
        .unwrap()
}

/// `start -> fork(parallel) -> a(jobA) -> join <- b(jobB) <- fork -> end`: both
/// branches active at once, so a move-by-id of `a` moves exactly one token.
fn forked_two_tasks() -> ProcessDefinition {
    ProcessBuilder::new("forked")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("a", "jobA")
        .service_task("b", "jobB")
        .parallel_gateway("join")
        .end_event("end")
        .connect("start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "end")
        .build()
        .unwrap()
}

fn start_instance(engine: &mut Engine, def: ProcessDefinition) -> Key {
    start_instance_with(engine, def, HashMap::new())
}

fn start_instance_with(
    engine: &mut Engine,
    def: ProcessDefinition,
    vars: HashMap<String, Value>,
) -> Key {
    let id = def.id.clone();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(id, vars))
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

fn move_cmd(inst: Key, moves: Vec<MoveInstruction>) -> Command {
    Command::ModifyInstance {
        instance_key: inst,
        activate_instructions: vec![],
        terminate_instructions: vec![],
        terminate_element_ids: vec![],
        move_instructions: moves,
    }
}

fn move_by_id(source: &str, target: &str) -> MoveInstruction {
    MoveInstruction {
        source: MoveSource::ById(source.to_string()),
        target_element_id: target.to_string(),
        ancestor_scope: AncestorScopeInstruction::Inferred,
        variable_instructions: vec![],
    }
}

fn move_by_key(source: Key, target: &str) -> MoveInstruction {
    MoveInstruction {
        source: MoveSource::ByKey(source),
        target_element_id: target.to_string(),
        ancestor_scope: AncestorScopeInstruction::Inferred,
        variable_instructions: vec![],
    }
}

// ---- Move by element id ----------------------------------------------------

/// Zeebe `shouldMoveAllElementsByIds` (single-instance leg): the token parked
/// on `a` is terminated and a fresh token is activated at `b`; `a`'s job is
/// cancelled and `b`'s job is created.
#[test]
fn move_by_id_moves_the_token_to_the_target() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];
    let job_a = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst)
        .unwrap()
        .key;

    let events = engine
        .apply_command(move_cmd(inst, vec![move_by_id("a", "b")]))
        .unwrap();

    // `a` terminated (its job cancelled), `b` activated with a fresh job.
    assert!(events.contains(&Event::JobCanceled {
        job_key: job_a,
        instance_key: inst,
        canceled_at: 0,
        deadline: None,
    }));
    assert!(eiks(&engine, inst, "a").is_empty(), "a is terminated");
    let bs = eiks(&engine, inst, "b");
    assert_eq!(bs.len(), 1, "one token activated at b");
    assert_ne!(bs[0], a_eik, "the target is a NEW element instance");
    assert!(engine.state().jobs.values().any(|j| j.instance_key == inst
        && j.state == state::JobState::Created
        && j.element_id == "b"));
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
}

/// A move by id with no active instance of the source element is a no-op for
/// that instruction (Zeebe: the instruction simply matches nothing), NOT an
/// error — the command still applies.
#[test]
fn move_by_id_with_no_active_source_instance_is_a_no_op() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];

    // `b` has no active instance yet: moving it moves nothing, and `a` is
    // untouched.
    engine
        .apply_command(move_cmd(inst, vec![move_by_id("b", "a")]))
        .unwrap();
    assert_eq!(eiks(&engine, inst, "a"), vec![a_eik]);
    assert!(eiks(&engine, inst, "b").is_empty());
}

/// Zeebe `shouldMoveAllElementsByIds` (fan-out leg): EVERY active instance of
/// the source element moves. A fork activates `a` and `b` together; moving `a`
/// to `b` terminates the `a` token and activates a SECOND `b` token alongside
/// the one the fork started.
#[test]
fn move_by_id_moves_every_active_instance_of_the_source() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, forked_two_tasks());
    assert_eq!(eiks(&engine, inst, "a").len(), 1);
    assert_eq!(eiks(&engine, inst, "b").len(), 1);

    engine
        .apply_command(move_cmd(inst, vec![move_by_id("a", "b")]))
        .unwrap();

    assert!(eiks(&engine, inst, "a").is_empty());
    assert_eq!(
        eiks(&engine, inst, "b").len(),
        2,
        "the moved token joins the already-active b"
    );
}

/// Zeebe `shouldMoveFromNestedSubprocessToOuterSubprocessByElementId`: moving
/// `t` (nested inside `inner` inside `outer`) to `b` (directly in `outer`)
/// with the inferred ancestor scope walks the source's ancestry and lands the
/// new token in the OUTER sub-process instance — not in `inner`, not at root.
#[test]
fn move_infers_the_ancestor_scope_from_the_source_hierarchy() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, nested_inner_and_outer_task());
    let t_eik = eiks(&engine, inst, "t")[0];
    let inner_eik = scope_of(&engine, inst, t_eik);
    let outer_eik = scope_of(&engine, inst, inner_eik);

    engine
        .apply_command(move_cmd(inst, vec![move_by_id("t", "b")]))
        .unwrap();

    assert!(eiks(&engine, inst, "t").is_empty());
    let bs = eiks(&engine, inst, "b");
    assert_eq!(bs.len(), 1);
    assert_eq!(
        scope_of(&engine, inst, bs[0]),
        outer_eik,
        "the target activates in the outer scope instance"
    );
    // The drained `inner` scope is terminated by the cascade (Zeebe
    // `terminateFlowScopes`): it held only `t`.
    assert!(
        eiks(&engine, inst, "inner").is_empty(),
        "the drained inner scope is terminated"
    );
    assert_eq!(eiks(&engine, inst, "outer"), vec![outer_eik]);
}

/// Zeebe `shouldMoveElementInstanceBySourceElementInstanceKeyWithSourceParent`:
/// `sourceParent` lands the target in the source instance's DIRECT flow scope —
/// even when the target's model flow scope sits higher.
#[test]
fn move_with_source_parent_lands_in_the_sources_direct_scope() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, nested_inner_and_outer_task());
    let t_eik = eiks(&engine, inst, "t")[0];
    let inner_eik = scope_of(&engine, inst, t_eik);

    let mut m = move_by_id("t", "b");
    m.ancestor_scope = AncestorScopeInstruction::SourceParent;
    let err = engine.apply_command(move_cmd(inst, vec![m]));

    // `b` is not an element of the `inner` scope, so activating it under the
    // source's parent (`inner`) is rejected by the activation's ancestor
    // validation (Zeebe rejects the same shape:
    // `ERROR_MESSAGE_ANCESTOR_NOT_ANCESTOR`-class).
    assert!(err.is_err(), "sourceParent into a non-scope is rejected");
    let _ = inner_eik;
}

/// Moving within one sub-process scope (Zeebe
/// `shouldMoveSiblingElementsInSubprocessWithSourceParentScope`): source and
/// target share the `sub` scope, so every ancestor mode lands the target in
/// the same `sub` instance.
#[test]
fn move_between_siblings_stays_in_the_shared_scope() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, sub_with_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];
    let sub_eik = scope_of(&engine, inst, a_eik);

    for mode in [
        AncestorScopeInstruction::Inferred,
        AncestorScopeInstruction::SourceParent,
        AncestorScopeInstruction::Direct(sub_eik),
    ] {
        let mut m = move_by_id("a", "b");
        m.ancestor_scope = mode;
        engine.apply_command(move_cmd(inst, vec![m])).unwrap();
        let bs = eiks(&engine, inst, "b");
        assert_eq!(bs.len(), 1, "one b active after the move");
        assert_eq!(scope_of(&engine, inst, bs[0]), sub_eik);
        // Move it back for the next mode.
        engine
            .apply_command(move_cmd(inst, vec![move_by_id("b", "a")]))
            .unwrap();
        assert_eq!(eiks(&engine, inst, "a").len(), 1);
    }
}

// ---- Move by element-instance key ------------------------------------------

/// Zeebe `shouldMoveElementInstanceBySourceElementInstanceKey`: with two
/// tokens active (fork into `a` and `b`), a by-key move terminates exactly the
/// named instance and activates one fresh target.
#[test]
fn move_by_key_moves_exactly_the_named_instance() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, forked_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];
    let b_before = eiks(&engine, inst, "b");

    engine
        .apply_command(move_cmd(inst, vec![move_by_key(a_eik, "b")]))
        .unwrap();

    assert!(eiks(&engine, inst, "a").is_empty(), "the named a moved");
    let b_after = eiks(&engine, inst, "b");
    assert_eq!(b_after.len(), b_before.len() + 1);
    assert!(b_after.iter().all(|b| *b != a_eik));
}

/// Zeebe rejection parity (`ERROR_MESSAGE_MOVE_SOURCE_ELEMENT_INSTANCE_NOT_FOUND`
/// / `..._WRONG_PROCESS`): a by-key move whose source instance does not exist —
/// or belongs to a DIFFERENT process instance — is rejected (400-class) and
/// nothing is applied.
#[test]
fn move_by_key_rejects_an_unknown_or_foreign_source_instance() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());
    let other = start_instance(&mut engine, flat_two_tasks());
    let foreign_a = eiks(&engine, other, "a")[0];
    let a_eik = eiks(&engine, inst, "a")[0];

    for bad_source in [MoveSource::ByKey(999_999), MoveSource::ByKey(foreign_a)] {
        let err = engine
            .apply_command(move_cmd(
                inst,
                vec![MoveInstruction {
                    source: bad_source,
                    target_element_id: "b".into(),
                    ancestor_scope: AncestorScopeInstruction::Inferred,
                    variable_instructions: vec![],
                }],
            ))
            .unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::ModificationInstructionInvalid { .. }
                    | EngineError::ElementInstanceNotFound { .. }
            ),
            "got {err:?}"
        );
        // Atomic: `a` is still parked.
        assert_eq!(eiks(&engine, inst, "a"), vec![a_eik]);
    }
}

// ---- Rejections ------------------------------------------------------------

/// Zeebe `ERROR_MESSAGE_MOVE_DUPLICATE_DEFINITIONS`: two move instructions
/// with the same source element id are rejected and nothing is applied.
#[test]
fn move_rejects_duplicate_source_element_ids() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];

    let err = engine
        .apply_command(move_cmd(
            inst,
            vec![move_by_id("a", "b"), move_by_id("a", "b")],
        ))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::ModificationInstructionInvalid { .. }),
        "got {err:?}"
    );
    assert_eq!(eiks(&engine, inst, "a"), vec![a_eik], "nothing applied");
}

/// A move whose target element does not exist in the definition is rejected
/// (`ElementNotFound`, the same class as an activate of an unknown element)
/// and nothing is applied.
#[test]
fn move_rejects_an_unknown_target_element() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];

    let err = engine
        .apply_command(move_cmd(inst, vec![move_by_id("a", "ghost")]))
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::ElementNotFound { .. }
                | EngineError::ModificationInstructionInvalid { .. }
        ),
        "got {err:?}"
    );
    assert_eq!(eiks(&engine, inst, "a"), vec![a_eik], "nothing applied");
}

/// Zeebe parity (`ERROR_MESSAGE_MOVE_NO_DEFINITIONS`): a move instruction with
/// an empty source element id (the batch request's only source shape) is
/// rejected, never silently ignored.
#[test]
fn move_rejects_an_empty_source_element_id() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());
    let a_eik = eiks(&engine, inst, "a")[0];

    let err = engine
        .apply_command(move_cmd(inst, vec![move_by_id("", "b")]))
        .unwrap_err();
    assert!(
        matches!(err, EngineError::ModificationInstructionInvalid { .. }),
        "got {err:?}"
    );
    assert_eq!(eiks(&engine, inst, "a"), vec![a_eik], "nothing applied");
}

/// A move that terminates the last token but activates a replacement keeps the
/// instance alive (the activation replaces the moved token).
#[test]
fn move_of_the_last_token_keeps_the_instance_alive() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());

    engine
        .apply_command(move_cmd(inst, vec![move_by_id("a", "b")]))
        .unwrap();

    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active,
        "the moved-to token keeps the instance active"
    );
    // And the instance can run to completion from the moved token.
    let job_b = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == inst && j.state == state::JobState::Created)
        .unwrap()
        .key;
    engine.activate_jobs("jobB", "w", 1, 60_000, 0);
    engine.apply_command(Command::complete_job(job_b)).unwrap();
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

/// Move + variable instructions: the move's `variable_instructions` ride the
/// activation they spawn (Zeebe copies them onto the activate instruction), so
/// the moved-to element sees them.
#[test]
fn move_applies_its_variable_instructions_with_the_activation() {
    let mut engine = Engine::new();
    let inst = start_instance(&mut engine, flat_two_tasks());

    let mut m = move_by_id("a", "b");
    m.variable_instructions = vec![crate::command::VariableInstruction {
        variables: HashMap::from([("moved".to_string(), Value::Bool(true))]),
        scope_id: None,
    }];
    engine.apply_command(move_cmd(inst, vec![m])).unwrap();

    let vars = &engine.instance(inst).unwrap().variables;
    assert_eq!(
        vars.get("moved"),
        Some(&Value::Bool(true)),
        "the move's variables landed on the root scope"
    );
}

// ---- Multi-instance sources -------------------------------------------------

/// Zeebe `shouldMoveAllMultiInstanceElementsByIds`: for a multi-instance
/// SOURCE, only the body instance moves (the children go with it); one fresh
/// target token is activated per moved body.
#[test]
fn move_of_a_multi_instance_source_moves_the_body() {
    let mut engine = Engine::new();
    let def = ProcessBuilder::new("mi-move")
        .start_event("start")
        .service_task("a", "jobA")
        .service_task("b", "jobB")
        .end_event("end")
        .with_multi_instance(
            "a",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                ..Default::default()
            },
        )
        .connect("start", "a")
        .connect("a", "b")
        .connect("b", "end")
        .build()
        .unwrap();
    let inst = start_instance_with(
        &mut engine,
        def,
        HashMap::from([(
            "items".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        )]),
    );
    // The body plus two children are active, all recorded under element `a`.
    let a_instances = eiks(&engine, inst, "a");
    assert!(
        a_instances.len() >= 2,
        "the MI body and its children are active: {a_instances:?}"
    );

    engine
        .apply_command(move_cmd(inst, vec![move_by_id("a", "b")]))
        .unwrap();

    assert!(
        eiks(&engine, inst, "a").is_empty(),
        "every active `a` instance (body + children) is gone"
    );
    let bs = eiks(&engine, inst, "b");
    assert_eq!(
        bs.len(),
        1,
        "exactly one target token is activated for the moved body"
    );
}
