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
