//! Parser-to-runtime integration coverage for `sendTask` and `inclusiveGateway`
//! (#1168). The inline `bpmn.rs` tests only assert the *parsed* definition shape;
//! these deploy the parsed BPMN XML into a real [`Engine`] and drive it to
//! completion, so a parser/runtime regression cannot pass while the advertised
//! executed behaviour is broken (the gap flagged at `bpmn.rs:705` / `bpmn.rs:678`
//! and `model.rs:544`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::HashMap;

use nanobpmn_engine_core::{bpmn::parse_bpmn, Command, Engine, Event, Value};

/// The `sendTask` model, shared verbatim with the `@nanobpm/engine-wasm`
/// package-level end-to-end probe (`engine-wasm/tests/agent-instance-e2e/
/// element-support.mjs`) so the native and browser execution surfaces exercise
/// one canonical diagram — no drifting second copy.
const SEND_TASK_XML: &str = include_str!("fixtures/send-task.bpmn");

/// A `sendTask` parsed from BPMN XML is executed by a job worker exactly like a
/// service task: deploying it, starting an instance, activating its job and
/// completing it must drive the process to `ProcessInstanceCompleted`.
#[test]
fn send_task_parsed_from_bpmn_activates_a_job_and_completes() {
    let xml = SEND_TASK_XML;

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(parse_bpmn(xml).unwrap().remove(0)))
        .unwrap();
    engine
        .apply_command(Command::create_instance("notify"))
        .unwrap();

    let job = engine
        .activate_jobs("notifier", "W", 1, 1_000, 0)
        .pop()
        .expect("the send task must activate a single worker job");
    assert_eq!(job.retries, 4);
    assert_eq!(job.element_id, "send");

    let events = engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })),
        "completing the send-task job must complete the process"
    );
}

const INCLUSIVE_XML: &str = include_str!("fixtures/inclusive-gateway.bpmn");

/// An `inclusiveGateway` split/join parsed from BPMN XML routes and completes at
/// runtime, honouring the conditional branch when its condition holds and the
/// parsed `default` flow otherwise.
#[test]
fn inclusive_gateway_parsed_from_bpmn_routes_conditional_and_default() {
    for (go, expected_job) in [(true, "ta"), (false, "tb")] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                parse_bpmn(INCLUSIVE_XML).unwrap().remove(0),
            ))
            .unwrap();
        engine
            .apply_command(Command::create_instance_with(
                "review",
                HashMap::from([("go".to_string(), Value::Bool(go))]),
            ))
            .unwrap();

        // The other branch must not have activated a job.
        let other = if go { "tb" } else { "ta" };
        assert!(
            engine.activate_jobs(other, "W", 1, 1_000, 0).is_empty(),
            "go={go}: only the selected inclusive branch may activate"
        );
        let job = engine
            .activate_jobs(expected_job, "W", 1, 1_000, 0)
            .pop()
            .unwrap_or_else(|| panic!("go={go}: the selected branch must activate a job"));

        let events = engine
            .apply_command(Command::complete_job(job.key))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })),
            "go={go}: the inclusive join must synchronise and complete the process"
        );
    }
}

/// The `InclusiveGateway` variant is part of the serialized `EngineSnapshot`
/// shape. Deploying a model that contains it and round-tripping the snapshot
/// through serde must reconstruct an identical engine — guarding the serialized
/// shape of the new variant (the golden/round-trip gap flagged at `model.rs:544`).
#[cfg(feature = "serde")]
#[test]
fn inclusive_gateway_snapshot_round_trips_through_serde() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            parse_bpmn(INCLUSIVE_XML).unwrap().remove(0),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "review",
            HashMap::from([("go".to_string(), Value::Bool(true))]),
        ))
        .unwrap();

    let snapshot = serde_json::to_value(engine.snapshot()).unwrap();
    let restored = Engine::from_snapshot(serde_json::from_value(snapshot).unwrap());
    assert_eq!(
        restored.state(),
        engine.state(),
        "an engine carrying an InclusiveGateway must round-trip through a serde snapshot"
    );
}
