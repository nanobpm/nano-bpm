//! Equivalence of the completion-step kernel with the pre-#2049 engine
//! control flow, exhaustively over every input (all inputs are finite).
//!
//! Each oracle restates, in its original shape, the branch order of the
//! engine code the kernel replaced: the inline tail of `Engine::complete`,
//! `Engine::finalize_completion`, `Engine::take_completion_flows`,
//! `Engine::raise_escalation_from_throw` and the `Engine::activate` prelude.

use super::*;

const KINDS: [CompletionKind; 5] = [
    CompletionKind::LinkThrow,
    CompletionKind::EscalationThrow,
    CompletionKind::ErrorEnd,
    CompletionKind::EventBasedGateway,
    CompletionKind::Other,
];

/// An observable step of the old code, in emission order.
#[derive(Debug, PartialEq, Eq)]
enum Op {
    Register,
    Route(CompletionRoute),
}

fn race_route(kind: CompletionKind) -> CompletionRoute {
    CompletionRoute::TakeFlows {
        race: kind == CompletionKind::EventBasedGateway,
    }
}

/// Old inline tail of `complete`: handler check, then registration, then the
/// link / escalation / error checks, then the flows.
fn old_inline(handler: bool, kind: CompletionKind) -> Vec<Op> {
    if handler {
        return vec![Op::Route(CompletionRoute::CompensationHandler)];
    }
    let mut ops = vec![Op::Register];
    if kind == CompletionKind::LinkThrow {
        ops.push(Op::Route(CompletionRoute::LinkHandoff));
        return ops;
    }
    if kind == CompletionKind::EscalationThrow {
        ops.push(Op::Route(CompletionRoute::RaiseEscalation));
        return ops;
    }
    if kind == CompletionKind::ErrorEnd {
        ops.push(Op::Route(CompletionRoute::ThrowError));
        return ops;
    }
    ops.push(Op::Route(race_route(kind)));
    ops
}

/// Old `finalize_completion`: handler, link, escalation, error, and only then
/// registration before the flows.
fn old_deferred(handler: bool, kind: CompletionKind) -> Vec<Op> {
    if handler {
        return vec![Op::Route(CompletionRoute::CompensationHandler)];
    }
    if kind == CompletionKind::LinkThrow {
        return vec![Op::Route(CompletionRoute::LinkHandoff)];
    }
    if kind == CompletionKind::EscalationThrow {
        return vec![Op::Route(CompletionRoute::RaiseEscalation)];
    }
    if kind == CompletionKind::ErrorEnd {
        return vec![Op::Route(CompletionRoute::ThrowError)];
    }
    vec![Op::Register, Op::Route(race_route(kind))]
}

fn ops_of(plan: CompletionPlan) -> Vec<Op> {
    let mut ops = Vec::new();
    if plan.register_compensation {
        ops.push(Op::Register);
    }
    ops.push(Op::Route(plan.route));
    ops
}

#[test]
fn completion_plan_matches_old_paths() {
    for handler in [false, true] {
        for kind in KINDS {
            assert_eq!(
                ops_of(completion_plan(Phase::Inline, handler, kind)),
                old_inline(handler, kind),
                "inline {handler} {kind:?}"
            );
            assert_eq!(
                ops_of(completion_plan(Phase::Deferred, handler, kind)),
                old_deferred(handler, kind),
                "deferred {handler} {kind:?}"
            );
        }
    }
}

/// An observable step of the old escalation code, in emission order.
#[derive(Debug, PartialEq, Eq)]
enum EscOp {
    ThrowFlows,
    Action(EscalationAction),
}

fn old_escalation(catch: EscalationCatch) -> Vec<EscOp> {
    match catch {
        EscalationCatch::EventSubProcess { interrupting: true } => {
            vec![EscOp::Action(EscalationAction::TriggerEventSubProcess)]
        }
        EscalationCatch::EventSubProcess {
            interrupting: false,
        } => vec![
            EscOp::ThrowFlows,
            EscOp::Action(EscalationAction::ActivateEventSubProcess),
        ],
        EscalationCatch::Uncaught => vec![EscOp::ThrowFlows],
        EscalationCatch::Boundary {
            interrupting: false,
        } => vec![
            EscOp::ThrowFlows,
            EscOp::Action(EscalationAction::ActivateBoundary),
        ],
        EscalationCatch::Boundary { interrupting: true } => {
            vec![EscOp::Action(
                EscalationAction::InterruptAndActivateBoundary,
            )]
        }
    }
}

#[test]
fn escalation_plan_matches_old_path() {
    let catches = [
        EscalationCatch::Uncaught,
        EscalationCatch::EventSubProcess {
            interrupting: false,
        },
        EscalationCatch::EventSubProcess { interrupting: true },
        EscalationCatch::Boundary {
            interrupting: false,
        },
        EscalationCatch::Boundary { interrupting: true },
    ];
    for catch in catches {
        let plan = escalation_plan(catch);
        let mut ops = Vec::new();
        if plan.take_throw_flows {
            ops.push(EscOp::ThrowFlows);
        }
        if plan.action != EscalationAction::None {
            ops.push(EscOp::Action(plan.action));
        }
        assert_eq!(ops, old_escalation(catch), "{catch:?}");
    }
}

#[test]
fn boundary_host_redirects_mi_children_only() {
    assert_eq!(boundary_host(true), BoundaryHost::MultiInstanceBody);
    assert_eq!(boundary_host(false), BoundaryHost::CaughtActivity);
}

#[test]
fn activation_route_matches_old_prelude() {
    for is_join in [false, true] {
        for race in [false, true] {
            let expected = if is_join {
                ActivationRoute::Join
            } else {
                ActivationRoute::Activate { tag_race: race }
            };
            assert_eq!(activation_route(is_join, race), expected);
        }
    }
}
