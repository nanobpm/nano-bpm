//! Error and escalation event sub-process triggers (#1555, slice
//! `esp-error-escalation`) — Zeebe `CatchEventAnalyzer` parity.
//!
//! A thrown error / escalation walks outward from the throwing element
//! instance through its enclosing scopes. At each scope the candidates are the
//! event sub-processes *contained in* that scope and the boundary events
//! *attached to* it; an exact code beats a catch-all, and at equal
//! specificity an event sub-process beats a boundary event (Zeebe's
//! `ExecutableActivity.getEvents()` lists event sub-process starts first).
use super::*;
use crate::model::EventSubProcessTrigger as T;

fn err(code: Option<&str>) -> T {
    T::Error {
        error_code: code.map(str::to_string),
    }
}
fn esc(code: Option<&str>) -> T {
    T::Escalation {
        escalation_code: code.map(str::to_string),
    }
}

/// Adds event sub-process `id` (start `{id}_start` -> service task `{id}_task`
/// of job type `{id}` -> `{id}_end`), optionally contained in `parent`.
fn with_esp(
    b: ProcessBuilder,
    id: &str,
    trigger: T,
    interrupting: bool,
    parent: Option<&str>,
) -> ProcessBuilder {
    let (s, t, e) = (
        format!("{id}_start"),
        format!("{id}_task"),
        format!("{id}_end"),
    );
    let b = b
        .event_sub_process(id, s.clone(), trigger, interrupting)
        .start_event(s.clone())
        .service_task(t.clone(), id)
        .end_event(e.clone())
        .connect(s.clone(), t.clone())
        .connect(t.clone(), e.clone())
        .contained_in(s, id)
        .contained_in(t, id)
        .contained_in(e, id);
    match parent {
        Some(p) => b.contained_in(id, p),
        None => b,
    }
}

/// `s -> work(job "work") -> e` at the root.
fn root_task() -> ProcessBuilder {
    ProcessBuilder::new("p")
        .start_event("s")
        .service_task("work", "work")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
}

/// `s -> sub[ sub_s -> work(job "work") -> sub_e ] -> after(job "after") -> e`.
fn sub_task() -> ProcessBuilder {
    ProcessBuilder::new("p")
        .start_event("s")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .service_task("work", "work")
        .end_event("sub_e")
        .contained_in("sub_s", "sub")
        .contained_in("work", "sub")
        .contained_in("sub_e", "sub")
        .service_task("after", "after")
        .end_event("e")
        .connect("s", "sub")
        .connect("sub_s", "work")
        .connect("work", "sub_e")
        .connect("sub", "after")
        .connect("after", "e")
}

fn start(engine: &mut Engine, def: ProcessDefinition) -> Key {
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    create_instance_key(engine, "p")
}

fn throw_error(engine: &mut Engine, code: &str, vars_in: HashMap<String, Value>) -> Vec<Event> {
    let job = engine.activate_jobs("work", "w", 10, 60_000, 0)[0].key;
    engine
        .apply_command(Command::throw_job_error_with(job, code, "boom", vars_in))
        .unwrap()
}

fn activated(events: &[Event], id: &str) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == id))
}

fn job_types(engine: &Engine) -> Vec<String> {
    let mut v: Vec<String> = engine
        .pending_jobs()
        .iter()
        .map(|j| j.job_type.clone())
        .collect();
    v.sort();
    v
}

// --- Errors --------------------------------------------------------------

#[test]
fn job_error_is_caught_by_a_root_error_event_subprocess_by_code() {
    let mut engine = Engine::new();
    let def = with_esp(root_task(), "esp", err(Some("E1")), true, None)
        .build()
        .unwrap();
    let inst = start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", vars(&[("reason", Value::Int(42))]));

    assert!(activated(&events, "esp"), "error ESP activated");
    assert!(engine.active_incidents().is_empty(), "no incident");
    assert_eq!(job_types(&engine), vec!["esp".to_string()]);
    // Error variables land in the event sub-process's scope (Zeebe parity).
    assert_eq!(io_var(&engine, inst, "reason"), Some(Value::Int(42)));
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst), "scope completes with the ESP");
}

#[test]
fn catch_all_error_event_subprocess_catches_any_code() {
    let mut engine = Engine::new();
    let def = with_esp(root_task(), "esp", err(None), true, None)
        .build()
        .unwrap();
    let inst = start(&mut engine, def);
    let events = throw_error(&mut engine, "WHATEVER", HashMap::new());
    assert!(activated(&events, "esp"));
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst));
}

#[test]
fn exact_error_code_event_subprocess_beats_a_catch_all() {
    let mut engine = Engine::new();
    let b = with_esp(root_task(), "any", err(None), true, None);
    let def = with_esp(b, "exact", err(Some("E1")), true, None)
        .build()
        .unwrap();
    start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(activated(&events, "exact"));
    assert!(!activated(&events, "any"));
}

#[test]
fn job_error_in_embedded_subprocess_is_caught_by_its_error_event_subprocess() {
    let mut engine = Engine::new();
    let def = with_esp(sub_task(), "esp", err(Some("E1")), true, Some("sub"))
        .build()
        .unwrap();
    let inst = start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(activated(&events, "esp"));
    complete_one(&mut engine, "esp");
    // The sub-process completed via its ESP and the main flow continued.
    assert_eq!(job_types(&engine), vec!["after".to_string()]);
    complete_one(&mut engine, "after");
    assert!(engine.is_completed(inst));
}

#[test]
fn error_propagates_from_a_subprocess_to_a_root_event_subprocess() {
    let mut engine = Engine::new();
    let def = with_esp(sub_task(), "esp", err(Some("E1")), true, None)
        .build()
        .unwrap();
    let inst = start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(activated(&events, "esp"));
    assert_eq!(job_types(&engine), vec!["esp".to_string()]);
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst));
    assert!(engine.active_incidents().is_empty());
}

#[test]
fn inner_error_event_subprocess_beats_an_error_boundary_on_the_subprocess() {
    // Zeebe: at the sub-process scope, its event sub-processes precede the
    // boundary events attached to it.
    let mut engine = Engine::new();
    let b = sub_task()
        .error_boundary_event("bnd", "sub", "E1")
        .end_event("bnd_end")
        .connect("bnd", "bnd_end");
    let def = with_esp(b, "esp", err(Some("E1")), true, Some("sub"))
        .build()
        .unwrap();
    start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(activated(&events, "esp"));
    assert!(!activated(&events, "bnd"));
}

#[test]
fn error_boundary_on_the_throwing_task_beats_an_enclosing_event_subprocess() {
    // The walk starts at the throwing task: its own boundary is innermost.
    let mut engine = Engine::new();
    let b = sub_task()
        .error_boundary_event("bnd", "work", "E1")
        .end_event("bnd_end")
        .contained_in("bnd", "sub")
        .contained_in("bnd_end", "sub")
        .connect("bnd", "bnd_end");
    let def = with_esp(b, "esp", err(Some("E1")), true, Some("sub"))
        .build()
        .unwrap();
    start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(activated(&events, "bnd"));
    assert!(!activated(&events, "esp"));
}

#[test]
fn exact_error_boundary_beats_a_catch_all_event_subprocess_at_the_same_scope() {
    let mut engine = Engine::new();
    let b = sub_task()
        .error_boundary_event("bnd", "sub", "E1")
        .end_event("bnd_end")
        .connect("bnd", "bnd_end");
    let def = with_esp(b, "esp", err(None), true, Some("sub"))
        .build()
        .unwrap();
    start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(activated(&events, "bnd"));
    assert!(!activated(&events, "esp"));
}

#[test]
fn unmatched_error_raises_an_unhandled_error_incident() {
    let mut engine = Engine::new();
    let def = with_esp(root_task(), "esp", err(Some("OTHER")), true, None)
        .build()
        .unwrap();
    start(&mut engine, def);
    let events = throw_error(&mut engine, "E1", HashMap::new());
    assert!(!activated(&events, "esp"));
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1);
    assert_eq!(
        incidents[0].kind,
        crate::state::IncidentKind::UnhandledError
    );
}

// --- Escalations -----------------------------------------------------------

/// `s -> throw(esc CODE) -> work(job "work") -> e`, optionally inside `sub`.
fn esc_root() -> ProcessBuilder {
    ProcessBuilder::new("p")
        .start_event("s")
        .escalation_throw_event("throw", "ESC")
        .service_task("work", "work")
        .end_event("e")
        .connect("s", "throw")
        .connect("throw", "work")
        .connect("work", "e")
}

fn esc_sub() -> ProcessBuilder {
    ProcessBuilder::new("p")
        .start_event("s")
        .sub_process("sub", "sub_s")
        .start_event("sub_s")
        .escalation_throw_event("throw", "ESC")
        .service_task("work", "work")
        .end_event("sub_e")
        .contained_in("sub_s", "sub")
        .contained_in("throw", "sub")
        .contained_in("work", "sub")
        .contained_in("sub_e", "sub")
        .service_task("after", "after")
        .end_event("e")
        .connect("s", "sub")
        .connect("sub_s", "throw")
        .connect("throw", "work")
        .connect("work", "sub_e")
        .connect("sub", "after")
        .connect("after", "e")
}

fn create(engine: &mut Engine, def: ProcessDefinition) -> (Key, Vec<Event>) {
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine.apply_command(Command::create_instance("p")).unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    (inst, events)
}

#[test]
fn interrupting_escalation_event_subprocess_in_a_process() {
    let mut engine = Engine::new();
    let def = with_esp(esc_root(), "esp", esc(Some("ESC")), true, None)
        .build()
        .unwrap();
    let (inst, events) = create(&mut engine, def);
    assert!(activated(&events, "esp"));
    assert!(!activated(&events, "work"), "throw's flow not taken");
    assert_eq!(job_types(&engine), vec!["esp".to_string()]);
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst));
}

#[test]
fn non_interrupting_escalation_event_subprocess_in_a_process() {
    let mut engine = Engine::new();
    let def = with_esp(esc_root(), "esp", esc(Some("ESC")), false, None)
        .build()
        .unwrap();
    let (inst, events) = create(&mut engine, def);
    assert!(activated(&events, "esp"));
    assert!(activated(&events, "work"), "throw continues");
    assert_eq!(
        job_types(&engine),
        vec!["esp".to_string(), "work".to_string()]
    );
    complete_one(&mut engine, "work");
    assert!(!engine.is_completed(inst), "scope waits for the ESP");
    complete_one(&mut engine, "esp");
    assert!(engine.is_completed(inst));
}

#[test]
fn interrupting_escalation_event_subprocess_in_an_embedded_subprocess() {
    let mut engine = Engine::new();
    let def = with_esp(esc_sub(), "esp", esc(Some("ESC")), true, Some("sub"))
        .build()
        .unwrap();
    let (inst, events) = create(&mut engine, def);
    assert!(activated(&events, "esp"));
    assert!(!activated(&events, "work"));
    complete_one(&mut engine, "esp");
    assert_eq!(job_types(&engine), vec!["after".to_string()]);
    complete_one(&mut engine, "after");
    assert!(engine.is_completed(inst));
}

#[test]
fn non_interrupting_catch_all_escalation_event_subprocess_in_an_embedded_subprocess() {
    let mut engine = Engine::new();
    let def = with_esp(esc_sub(), "esp", esc(None), false, Some("sub"))
        .build()
        .unwrap();
    let (inst, events) = create(&mut engine, def);
    assert!(activated(&events, "esp"));
    assert!(activated(&events, "work"));
    complete_one(&mut engine, "esp");
    complete_one(&mut engine, "work");
    complete_one(&mut engine, "after");
    assert!(engine.is_completed(inst));
}

#[test]
fn inner_escalation_event_subprocess_beats_an_escalation_boundary_on_the_subprocess() {
    let mut engine = Engine::new();
    let b = esc_sub()
        .escalation_boundary_event("bnd", "sub", "ESC")
        .end_event("bnd_end")
        .connect("bnd", "bnd_end");
    let def = with_esp(b, "esp", esc(Some("ESC")), true, Some("sub"))
        .build()
        .unwrap();
    let (_, events) = create(&mut engine, def);
    assert!(activated(&events, "esp"));
    assert!(!activated(&events, "bnd"));
}

#[test]
fn exact_escalation_boundary_beats_a_catch_all_event_subprocess() {
    let mut engine = Engine::new();
    let b = esc_sub()
        .escalation_boundary_event("bnd", "sub", "ESC")
        .end_event("bnd_end")
        .connect("bnd", "bnd_end");
    let def = with_esp(b, "esp", esc(None), true, Some("sub"))
        .build()
        .unwrap();
    let (_, events) = create(&mut engine, def);
    assert!(activated(&events, "bnd"));
    assert!(!activated(&events, "esp"));
}

#[test]
fn escalation_propagates_from_a_subprocess_to_a_root_event_subprocess() {
    let mut engine = Engine::new();
    let def = with_esp(esc_sub(), "esp", esc(Some("ESC")), false, None)
        .build()
        .unwrap();
    let (inst, events) = create(&mut engine, def);
    assert!(activated(&events, "esp"));
    assert!(activated(&events, "work"));
    complete_one(&mut engine, "esp");
    complete_one(&mut engine, "work");
    complete_one(&mut engine, "after");
    assert!(engine.is_completed(inst));
}

#[test]
fn unmatched_escalation_is_ignored() {
    let mut engine = Engine::new();
    let def = with_esp(esc_root(), "esp", esc(Some("OTHER")), true, None)
        .build()
        .unwrap();
    let (_, events) = create(&mut engine, def);
    assert!(!activated(&events, "esp"));
    assert!(activated(&events, "work"));
    assert!(engine.active_incidents().is_empty());
}

#[test]
fn error_boundary_without_a_code_is_a_catch_all() {
    // Zeebe: an error boundary with no `errorRef` catches any error code.
    let mut engine = Engine::new();
    let def = root_task()
        .error_boundary_event("bnd", "work", "")
        .end_event("bnd_end")
        .connect("bnd", "bnd_end")
        .build()
        .unwrap();
    let inst = start(&mut engine, def);
    let events = throw_error(&mut engine, "ANY", HashMap::new());
    assert!(activated(&events, "bnd"));
    assert!(engine.is_completed(inst));
}

#[test]
fn error_variables_land_in_the_embedded_subprocess_hosting_the_event_subprocess() {
    let mut engine = Engine::new();
    let def = with_esp(sub_task(), "esp", err(None), true, Some("sub"))
        .build()
        .unwrap();
    start(&mut engine, def);
    throw_error(&mut engine, "E1", vars(&[("reason", Value::Int(7))]));
    let job = engine.activate_jobs("esp", "w", 10, 60_000, 0).remove(0);
    assert_eq!(job.variables.get("reason"), Some(&Value::Int(7)));
}
