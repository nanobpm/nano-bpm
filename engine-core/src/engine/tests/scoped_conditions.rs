//! #1335 — every condition the engine evaluates on an element's behalf
//! (exclusive / inclusive gateway flow conditions, conditional intermediate
//! catch events on arrival AND on re-evaluation, a multi-instance completion
//! condition) must read the variables visible FROM THAT ELEMENT'S SCOPE
//! (`variables_for_element`: the scope's locals layered over its ancestors),
//! never the root variables alone. Zeebe evaluates each of these against the
//! element instance's own scope key.
//!
//! The defect class: a variable that lives only in a sub-process (or
//! multi-instance child) scope is invisible to a condition evaluated against
//! the root, so the wrong branch is taken or the token never moves.
use super::*;

/// A sub-process `sub` declaring a local `status` (input `=null`), whose inner
/// `work` service task's job result `status` therefore propagates into the
/// SUB-PROCESS scope (nearest defining scope), not the root. After `work` the
/// token reaches `probe` — the element under test — built by `inner`.
fn scoped_status_process(
    id: &str,
    inner: impl FnOnce(ProcessBuilder) -> ProcessBuilder,
) -> ProcessDefinition {
    let builder = ProcessBuilder::new(id)
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=null".to_string(),
                    target: "status".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("work", "work")
        .contained_in("work", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "work")
        .connect("sub", "done");
    inner(builder).build().unwrap()
}

/// Two service tasks `yes` / `no` inside `sub`, each followed by its own end.
fn yes_no_tails(builder: ProcessBuilder) -> ProcessBuilder {
    builder
        .service_task("yes", "yes")
        .contained_in("yes", "sub")
        .service_task("no", "no")
        .contained_in("no", "sub")
        .end_event("yes_end")
        .contained_in("yes_end", "sub")
        .end_event("no_end")
        .contained_in("no_end", "sub")
        .connect("yes", "yes_end")
        .connect("no", "no_end")
}

fn complete_with(engine: &mut Engine, job_type: &str, vars_in: HashMap<String, Value>) {
    let job = engine
        .activate_jobs(job_type, "test-worker", 10, 60_000, 0)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no activatable job of type {job_type}"));
    engine
        .apply_command(Command::complete_job_with(job.key, vars_in))
        .unwrap();
}

fn has_job(engine: &mut Engine, job_type: &str) -> bool {
    !engine
        .activate_jobs(job_type, "probe-worker", 10, 60_000, 0)
        .is_empty()
}

fn root_var(engine: &Engine, instance_key: Key, name: &str) -> Option<Value> {
    engine.variables(instance_key).get(name).cloned()
}

#[test]
fn exclusive_gateway_in_subprocess_sees_subprocess_local_variable() {
    let def = scoped_status_process("xgw-scoped", |b| {
        yes_no_tails(b)
            .exclusive_gateway("gw")
            .contained_in("gw", "sub")
            .connect("work", "gw")
            .connect_when("gw", "yes", "=status = \"done\"")
            .connect_default("gw", "no")
    });
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let ik = create_instance_key(&mut engine, "xgw-scoped");
    complete_with(
        &mut engine,
        "work",
        vars(&[("status", Value::Str("done".to_string()))]),
    );
    // The write landed in the sub-process scope, not the root …
    assert_eq!(root_var(&engine, ik, "status"), None);
    // … and the gateway inside the sub-process still sees it.
    assert!(
        has_job(&mut engine, "yes"),
        "gateway must see the sub-process-local `status`"
    );
    assert!(!has_job(&mut engine, "no"));
}

#[test]
fn inclusive_gateway_in_subprocess_sees_subprocess_local_variable() {
    let def = scoped_status_process("igw-scoped", |b| {
        yes_no_tails(b)
            .inclusive_gateway("gw")
            .contained_in("gw", "sub")
            .connect("work", "gw")
            .connect_when("gw", "yes", "=status = \"done\"")
            .connect_default("gw", "no")
    });
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let ik = create_instance_key(&mut engine, "igw-scoped");
    complete_with(
        &mut engine,
        "work",
        vars(&[("status", Value::Str("done".to_string()))]),
    );
    assert_eq!(root_var(&engine, ik, "status"), None);
    assert!(
        has_job(&mut engine, "yes"),
        "gateway must see the sub-process-local `status`"
    );
    assert!(!has_job(&mut engine, "no"));
}

/// Arrival path: the condition already holds (in the sub-process scope) when
/// the token reaches the conditional catch, so it passes straight through.
#[test]
fn conditional_catch_in_subprocess_passes_on_arrival_with_local_variable() {
    let def = scoped_status_process("cond-arrival-scoped", |b| {
        b.conditional_intermediate_catch_event("gate", "=status = \"done\"")
            .contained_in("gate", "sub")
            .service_task("after", "after")
            .contained_in("after", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .connect("work", "gate")
            .connect("gate", "after")
            .connect("after", "sub_end")
    });
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let ik = create_instance_key(&mut engine, "cond-arrival-scoped");
    complete_with(
        &mut engine,
        "work",
        vars(&[("status", Value::Str("done".to_string()))]),
    );
    assert_eq!(root_var(&engine, ik, "status"), None);
    assert!(
        has_job(&mut engine, "after"),
        "conditional catch must see the sub-process-local `status` on arrival"
    );
}

/// Re-evaluation path: the catch is already parked when a parallel branch's
/// job writes the sub-process-local variable. The scoped write must trigger a
/// re-evaluation, and that re-evaluation must read the catch's scope.
#[test]
fn conditional_catch_in_subprocess_fires_on_local_variable_change() {
    let def = ProcessBuilder::new("cond-reeval-scoped")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=null".to_string(),
                    target: "status".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("fork")
        .contained_in("fork", "sub")
        .service_task("work", "work")
        .contained_in("work", "sub")
        .conditional_intermediate_catch_event("gate", "=status = \"done\"")
        .contained_in("gate", "sub")
        .service_task("after", "after")
        .contained_in("after", "sub")
        .end_event("work_end")
        .contained_in("work_end", "sub")
        .end_event("after_end")
        .contained_in("after_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "fork")
        .connect("fork", "work")
        .connect("fork", "gate")
        .connect("work", "work_end")
        .connect("gate", "after")
        .connect("after", "after_end")
        .connect("sub", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let ik = create_instance_key(&mut engine, "cond-reeval-scoped");
    assert!(!has_job(&mut engine, "after"), "gate must park first");
    complete_with(
        &mut engine,
        "work",
        vars(&[("status", Value::Str("done".to_string()))]),
    );
    assert_eq!(root_var(&engine, ik, "status"), None);
    assert!(
        has_job(&mut engine, "after"),
        "a sub-process-scoped write must re-evaluate the parked catch in its scope"
    );
}

/// A multi-instance completion condition is evaluated in the completing
/// CHILD's scope (Zeebe: `childContext.getElementInstanceKey()`), so the bound
/// `inputElement` is visible to it.
#[test]
fn multi_instance_completion_condition_sees_child_scope() {
    let def = ProcessBuilder::new("mi-cc-scoped")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: Some("=item = 2".to_string()),
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-cc-scoped",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    complete_one(&mut engine, "handle");
    complete_one(&mut engine, "handle");
    assert!(
        has_job(&mut engine, "sink-work"),
        "completion condition `item = 2` must see the child-scoped `item` and end the loop"
    );
    assert!(
        !has_job(&mut engine, "handle"),
        "third child must not start"
    );
}

/// A non-interrupting root boundary `=ping = true` whose handler sub-process
/// writes a sub-process-LOCAL `ping`: that write is invisible to the boundary's
/// (root) scope, so it must not re-trigger it. Before the fix the scoped write
/// re-fired the unchanged root condition, spawning a new handler per write.
#[test]
fn local_write_invisible_to_a_root_boundary_does_not_retrigger_it() {
    let def = ProcessBuilder::new("ni-cond-local")
        .start_event("start")
        .service_task("t", "t-work")
        .non_interrupting_conditional_boundary_event("b", "t", "=ping = true")
        .sub_process("h", "h_start")
        .with_io(
            "h",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=null".to_string(),
                    target: "ping".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .start_event("h_start")
        .contained_in("h_start", "h")
        .service_task("hw", "h-work")
        .contained_in("hw", "h")
        .end_event("h_end")
        .contained_in("h_end", "h")
        .end_event("t_end")
        .end_event("b_end")
        .connect("start", "t")
        .connect("t", "t_end")
        .connect("b", "h")
        .connect("h_start", "hw")
        .connect("hw", "h_end")
        .connect("h", "b_end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let ik = create_instance_key(&mut engine, "ni-cond-local");
    let fired = engine
        .apply_command(Command::set_variables(
            ik,
            vars(&[("ping", Value::Bool(true))]),
        ))
        .unwrap();
    let triggers = |evs: &[Event]| {
        evs.iter()
            .filter(|e| matches!(e, Event::ConditionalTriggered { .. }))
            .count()
    };
    assert_eq!(triggers(&fired), 1, "one root change fires exactly once");
    let job = engine
        .activate_jobs("h-work", "w", 10, 60_000, 0)
        .into_iter()
        .next()
        .expect("handler job");
    let out = engine
        .apply_command(Command::complete_job_with(
            job.key,
            vars(&[("ping", Value::Bool(true))]),
        ))
        .unwrap();
    assert_eq!(
        triggers(&out),
        0,
        "a handler-local `ping` write must not re-trigger the root boundary"
    );
}

/// The completion condition reads the same post-output view as
/// `outputElement`: a value produced only by the MI element's output mapping
/// must be visible to it.
#[test]
fn multi_instance_completion_condition_sees_output_mapped_value() {
    let def = ProcessBuilder::new("mi-cc-output")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: Some("=stop = true".to_string()),
                cancel_remaining_instances: true,
                sequential: true,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=item = 2".to_string(),
                    target: "stop".to_string(),
                }],
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi-cc-output",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();
    complete_one(&mut engine, "handle");
    complete_one(&mut engine, "handle");
    assert!(
        has_job(&mut engine, "sink-work"),
        "completion condition must see the output-mapped `stop`"
    );
    assert!(
        !has_job(&mut engine, "handle"),
        "third child must not start"
    );
}

/// Candidate selection is driven by the scope each referenced name RESOLVES to
/// from the subscription's element, never by a same-named write anywhere else.
/// A parked catch inside a sub-process that declares a local `status` resolves
/// `status` to that sub-process scope, so a root `status` write — even buried
/// in a large batch of unrelated root writes — must not re-trigger it; only a
/// write into the sub-process scope may.
///
/// The same test bounds the filter's WORK (Copilot #1419): each referenced
/// name costs ONE write lookup per subscription, so a batch of N unrelated
/// writes is inspected once, never scanned N times.
#[test]
fn reevaluation_matches_only_writes_in_the_referenced_names_resolved_scope() {
    let def = ProcessBuilder::new("cond-reeval-shadowed")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=null".to_string(),
                    target: "status".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .conditional_intermediate_catch_event("gate", "=status = \"done\"")
        .contained_in("gate", "sub")
        .service_task("after", "after")
        .contained_in("after", "sub")
        .end_event("after_end")
        .contained_in("after_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "gate")
        .connect("gate", "after")
        .connect("after", "after_end")
        .connect("sub", "done")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let ik = create_instance_key(&mut engine, "cond-reeval-shadowed");
    assert!(!has_job(&mut engine, "after"), "gate must park first");

    // A large batch of root writes: 64 unrelated names plus a ROOT `status`
    // write the parked catch cannot see (its sub-process local shadows the
    // root). Selecting the catch's candidacy costs ONE scope resolution for its
    // single referenced name — never one per changed variable.
    let mut batch = vars(&[("status", Value::Str("done".to_string()))]);
    for i in 0..64 {
        batch.insert(format!("noise{i}"), Value::Int(i));
    }
    crate::engine::resolve::reset_conditional_write_checks();
    let out = engine
        .apply_command(Command::set_variables(ik, batch))
        .unwrap();
    assert!(
        out.iter()
            .all(|e| !matches!(e, Event::ConditionalTriggered { .. })),
        "a root `status` write is invisible to the catch's shadowed scope"
    );
    assert_eq!(
        crate::engine::resolve::conditional_write_checks(),
        1,
        "candidate selection inspects each referenced name once, \
         not every changed variable"
    );
    assert!(!has_job(&mut engine, "after"));

    // The same name written into the sub-process scope the catch resolves
    // `status` to DOES re-evaluate it.
    let gate_eik = engine
        .conditional_subscriptions()
        .first()
        .map(|s| s.element_instance_key)
        .expect("parked conditional subscription");
    let sub_scope = *engine.instance(ik).unwrap().scopes.get(&gate_eik).unwrap();
    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            vars(&[("status", Value::Str("done".to_string()))]),
            true,
        ))
        .unwrap();
    assert!(
        has_job(&mut engine, "after"),
        "a write into the referenced name's resolved scope must fire the catch"
    );
}
