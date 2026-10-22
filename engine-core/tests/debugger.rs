//! Prototype debugger tests (issue #646): stepping + breakpoints over the engine's
//! production step semantics.
//!
//! The load-bearing invariant is **RTC parity**: a debug run resumed (or
//! single-stepped) to quiescence must emit byte-identical events to a plain atomic
//! `apply_command`. That is what proves the debugger reuses the real execution path
//! rather than a divergent copy — the whole point of driving the same `run_with`
//! loop with a different `StepDriver`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use nanobpmn_engine_core::{BreakCondition, Command, Engine, Event, ProcessBuilder};

/// A two-service-task process: start -> reserve(inventory) -> ship(logistics) -> end.
fn fulfilment() -> nanobpmn_engine_core::ProcessDefinition {
    ProcessBuilder::new("fulfilment")
        .start_event("start")
        .service_task("reserve", "inventory")
        .service_task("ship", "logistics")
        .end_event("end")
        .connect("start", "reserve")
        .connect("reserve", "ship")
        .connect("ship", "end")
        .build()
        .expect("valid process")
}

fn deployed_engine() -> Engine {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(fulfilment()))
        .expect("deploy");
    engine
}

/// Resuming a debug run with no breakpoints drains to quiescence and yields exactly
/// the events a plain `apply_command` would — no divergence between the two paths.
#[test]
fn debug_run_to_completion_matches_apply_command() {
    let atomic = {
        let mut engine = deployed_engine();
        engine
            .apply_command(Command::create_instance("fulfilment"))
            .expect("create")
    };

    let debugged = {
        let mut engine = deployed_engine();
        let session = engine
            .debug_command_at(Command::create_instance("fulfilment"), 0, Vec::new())
            .expect("debug create");
        assert!(
            !session.is_paused(),
            "no breakpoints => the run drains to quiescence"
        );
        session.into_log()
    };

    assert_eq!(
        atomic, debugged,
        "RTC parity: debug run == atomic apply_command"
    );
}

/// Single-stepping from the start to quiescence produces the same event log as the
/// atomic command — the debugger advances the real run one step at a time, it does
/// not simulate it.
#[test]
fn single_stepping_to_completion_matches_apply_command() {
    let atomic = {
        let mut engine = deployed_engine();
        engine
            .apply_command(Command::create_instance("fulfilment"))
            .expect("create")
    };

    let mut engine = deployed_engine();
    // Pause after the very first step by breaking on every step.
    let mut session = engine
        .debug_command_at(
            Command::create_instance("fulfilment"),
            0,
            vec![BreakCondition::EveryStep],
        )
        .expect("debug create");
    assert!(session.is_paused(), "pauses after the first step");
    assert!(
        matches!(
            session.log().first(),
            Some(Event::ProcessInstanceCreated { .. })
        ),
        "the first step creates the instance"
    );

    // Drive it one step at a time until it drains.
    let mut steps = 1; // the initial resume already took one step
    while session.is_paused() {
        engine.debug_step(&mut session);
        steps += 1;
        assert!(steps < 1000, "single-stepping must terminate");
    }

    assert_eq!(
        atomic,
        session.into_log(),
        "RTC parity: single-stepped run == atomic apply_command"
    );
}

/// A breakpoint on an element activation pauses the command mid-flight — before it
/// has parked the job — and resuming from there completes it identically.
#[test]
fn breakpoint_pauses_before_the_job_is_parked() {
    let full = {
        let mut engine = deployed_engine();
        engine
            .apply_command(Command::create_instance("fulfilment"))
            .expect("create")
    };

    let mut engine = deployed_engine();
    let mut session = engine
        .debug_command_at(
            Command::create_instance("fulfilment"),
            0,
            vec![BreakCondition::ElementActivated("start".to_string())],
        )
        .expect("debug create");

    assert!(session.is_paused(), "paused at start-event activation");
    assert!(
        session.log().iter().any(|e| matches!(
            e,
            Event::ElementActivated { element_id, .. } if element_id == "start"
        )),
        "the pause point includes the start event's activation"
    );
    assert!(
        !session
            .log()
            .iter()
            .any(|e| matches!(e, Event::JobCreated { .. })),
        "no job has been created yet at the start-event breakpoint"
    );
    assert!(
        session.log().len() < full.len(),
        "paused mid-command: fewer events than the finished run \
         (paused {} < full {})",
        session.log().len(),
        full.len(),
    );
    assert!(
        engine.pending_jobs().is_empty(),
        "the inventory job has not been parked yet at the breakpoint"
    );

    // Resume: no more breakpoints fire, so it drains and finishes.
    engine.debug_resume(&mut session);
    assert!(!session.is_paused(), "resumed to quiescence");
    assert_eq!(
        full,
        session.into_log(),
        "RTC parity: paused-then-resumed run == atomic apply_command"
    );
    assert_eq!(engine.pending_jobs().len(), 1, "parked on the reserve job");
    assert_eq!(engine.pending_jobs()[0].job_type, "inventory");
}

/// A `ProcessCompleted` breakpoint must actually pause the debug run at the
/// instance-completion event. This guards issue #647: completion is emitted by the
/// engine's terminal tail, which historically ran *after* `run_with` returned — so
/// the stepping driver never observed it and the breakpoint was dead. Folding
/// completion into the fixpoint drain point makes it observable. Uses a
/// wait-state-free process (`start -> end`) that runs to completion within the
/// single create command.
#[test]
fn process_completed_breakpoint_pauses_at_completion() {
    fn straight_through() -> nanobpmn_engine_core::ProcessDefinition {
        ProcessBuilder::new("straight")
            .start_event("start")
            .end_event("end")
            .connect("start", "end")
            .build()
            .expect("valid process")
    }

    let atomic = {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(straight_through()))
            .expect("deploy");
        engine
            .apply_command(Command::create_instance("straight"))
            .expect("create")
    };
    assert!(
        atomic
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })),
        "sanity: the wait-state-free process completes within the create command"
    );

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(straight_through()))
        .expect("deploy");
    let mut session = engine
        .debug_command_at(
            Command::create_instance("straight"),
            0,
            vec![BreakCondition::ProcessCompleted],
        )
        .expect("debug create");

    assert!(
        session.is_paused(),
        "the ProcessCompleted breakpoint pauses the run at completion"
    );
    assert!(
        matches!(
            session.log().last(),
            Some(Event::ProcessInstanceCompleted { .. })
        ),
        "paused exactly at the process-completion event"
    );

    // Resuming from the completion pause drains the (idempotent) tail and finishes,
    // with the full log identical to the atomic command: completion is emitted
    // exactly once (RTC parity holds across the folded completion step).
    engine.debug_resume(&mut session);
    assert!(
        !session.is_paused(),
        "resumed past completion to quiescence"
    );
    assert_eq!(
        atomic,
        session.into_log(),
        "RTC parity: completion emitted exactly once across pause + resume"
    );
}

/// A breakpoint on an embedded sub-process's completion must pause. The
/// sub-process's own `ElementCompleted` is emitted by `complete_drained_subprocesses`
/// during the fixpoint drain sweep — not by a `process_step` — so this guards the
/// drain-sweep driver consult. Same defect class as #647's `ProcessCompleted`:
/// events emitted outside the step loop must still be observable to breakpoints,
/// or the breakpoint silently misses them.
#[test]
fn element_completed_breakpoint_pauses_on_subprocess_drain_completion() {
    fn nested() -> nanobpmn_engine_core::ProcessDefinition {
        // start -> sub{ sub_start -> sub_end } -> done, no wait states: the whole
        // run completes within the create command, and `sub` completes via the
        // drain sweep (its inner scope empties, then it is swept to completion).
        ProcessBuilder::new("nested")
            .start_event("start")
            .sub_process("sub", "sub_start")
            .start_event("sub_start")
            .contained_in("sub_start", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .end_event("done")
            .connect("start", "sub")
            .connect("sub_start", "sub_end")
            .connect("sub", "done")
            .build()
            .expect("valid process")
    }

    let atomic = {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(nested()))
            .expect("deploy");
        engine
            .apply_command(Command::create_instance("nested"))
            .expect("create")
    };
    assert!(
        atomic.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "sub"
        )),
        "sanity: the sub-process completes within the create command"
    );

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested()))
        .expect("deploy");
    let mut session = engine
        .debug_command_at(
            Command::create_instance("nested"),
            0,
            vec![BreakCondition::ElementCompleted("sub".to_string())],
        )
        .expect("debug create");

    assert!(
        session.is_paused(),
        "the sub-process ElementCompleted breakpoint pauses the run"
    );
    assert!(
        session.log().iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "sub"
        )),
        "the pause point includes the sub-process completion event"
    );

    engine.debug_resume(&mut session);
    assert!(!session.is_paused(), "resumed to quiescence");
    assert_eq!(
        atomic,
        session.into_log(),
        "RTC parity: identical log across the drain-sweep pause + resume"
    );
}
