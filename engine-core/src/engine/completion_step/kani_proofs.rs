//! Kani harnesses for the completion-step kernel (#2049).
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! (`formal/kani/check.sh completion_plan_total_and_sound`,
//! `escalation_plan_total_and_sound`).

use super::*;

fn any_kind() -> CompletionKind {
    match kani::any::<u8>() % 5 {
        0 => CompletionKind::LinkThrow,
        1 => CompletionKind::EscalationThrow,
        2 => CompletionKind::ErrorEnd,
        3 => CompletionKind::EventBasedGateway,
        _ => CompletionKind::Other,
    }
}

#[kani::proof]
fn completion_plan_total_and_sound() {
    let phase = if kani::any() {
        Phase::Inline
    } else {
        Phase::Deferred
    };
    let handler: bool = kani::any();
    let kind = any_kind();
    let plan = completion_plan(phase, handler, kind);
    // A pending compensation handler always routes back, never registers.
    assert_eq!(handler, plan.route == CompletionRoute::CompensationHandler);
    if handler {
        assert!(!plan.register_compensation);
    }
    // Only an event-based gateway's flows race.
    if let CompletionRoute::TakeFlows { race } = plan.route {
        assert_eq!(race, kind == CompletionKind::EventBasedGateway);
    }
    // Every non-handler completion is routed by its kind alone.
    if !handler {
        assert_eq!(
            plan.route == CompletionRoute::LinkHandoff,
            kind == CompletionKind::LinkThrow
        );
        assert_eq!(
            plan.route == CompletionRoute::ThrowError,
            kind == CompletionKind::ErrorEnd
        );
    }
    // Taking flows always registers compensation first.
    if matches!(plan.route, CompletionRoute::TakeFlows { .. }) {
        assert!(plan.register_compensation);
    }
    kani::cover!(phase == Phase::Deferred && kind == CompletionKind::ErrorEnd && !handler);
    kani::cover!(plan.route == CompletionRoute::TakeFlows { race: true });
}

#[kani::proof]
fn escalation_plan_total_and_sound() {
    let interrupting: bool = kani::any();
    let catch = match kani::any::<u8>() % 3 {
        0 => EscalationCatch::Uncaught,
        1 => EscalationCatch::EventSubProcess { interrupting },
        _ => EscalationCatch::Boundary { interrupting },
    };
    let plan = escalation_plan(catch);
    let caught = catch != EscalationCatch::Uncaught;
    // Escalation is non-critical: the throw continues unless an interrupting
    // catch takes the token.
    assert_eq!(plan.take_throw_flows, !(caught && interrupting));
    assert_eq!(plan.action == EscalationAction::None, !caught);
    kani::cover!(plan.action == EscalationAction::InterruptAndActivateBoundary);
    kani::cover!(plan.action == EscalationAction::ActivateEventSubProcess);
}
