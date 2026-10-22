//! Integration tests against the public API only — the surface a mobile FFI
//! layer or the REST server would call.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use nanobpmn_engine_core::{Command, Engine, Event, ProcessBuilder};

/// Drives a two-service-task process and asserts the token advances one job at a
/// time, completing only after both jobs are done.
#[test]
fn two_service_tasks_complete_in_order() {
    let process = ProcessBuilder::new("fulfilment")
        .start_event("start")
        .service_task("reserve", "inventory")
        .service_task("ship", "logistics")
        .end_event("end")
        .connect("start", "reserve")
        .connect("reserve", "ship")
        .connect("ship", "end")
        .build()
        .expect("valid process");

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .expect("deploy");

    let created = engine
        .apply_command(Command::create_instance("fulfilment"))
        .expect("create");
    let instance_key = created.iter().find_map(Event::instance_key).unwrap();

    // Parked on the first task only.
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].job_type, "inventory");
    assert!(!engine.is_completed(instance_key));

    // Activate then complete the first job -> token advances to the second task.
    let first = engine.pending_jobs()[0].key;
    engine.activate_jobs("inventory", "worker-1", 10, 60_000, 0);
    engine
        .apply_command(Command::complete_job(first))
        .expect("complete first");
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].job_type, "logistics");
    assert!(!engine.is_completed(instance_key));

    // Activate then complete the second job -> instance finishes.
    let second = engine.pending_jobs()[0].key;
    engine.activate_jobs("logistics", "worker-1", 10, 60_000, 0);
    let events = engine
        .apply_command(Command::complete_job(second))
        .expect("complete second");
    assert!(engine.is_completed(instance_key));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
}
