//! Activate / complete / finalize step decisions as a pure kernel (#2049,
//! part of #1983).
//!
//! The *decision* half of `Engine::activate`, the inline (listener-free) tail
//! of `Engine::complete`, `Engine::finalize_completion` and
//! `Engine::raise_escalation_from_throw`: given the element kind (as a
//! fieldless [`CompletionKind`]) and a few booleans the collector read from
//! `State`, decide which steps to emit and in which order. The *gathering*
//! half — resolving the element kind, looking up pending compensation
//! handlers, resolving link catches and escalation catches, and building the
//! events — stays in the engine. Keep this module dependency-free and in the
//! subset Aeneas extracts (AGENTS.md, "Write Provable Code").

/// How an activation is dispatched (`Engine::activate`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActivationRoute {
    /// A parallel / inclusive join: guarded by `activate_join`.
    Join,
    /// A fresh element instance; `tag_race` tags its `ElementActivating` with
    /// the event-based gateway activation that armed it (#1676).
    Activate { tag_race: bool },
}

/// `Engine::activate`'s dispatch.
pub fn activation_route(is_join: bool, has_race: bool) -> ActivationRoute {
    if is_join {
        ActivationRoute::Join
    } else {
        ActivationRoute::Activate { tag_race: has_race }
    }
}

/// What the completion routing needs to know about the completing element.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompletionKind {
    /// `LinkIntermediateThrowEvent`.
    LinkThrow,
    /// `EscalationThrowEvent`.
    EscalationThrow,
    /// `ErrorEndEvent`.
    ErrorEnd,
    /// `EventBasedGateway`: its outgoing activations race (#1676).
    EventBasedGateway,
    /// Every other element: take the outgoing flows.
    Other,
}

/// Where the completion is decided.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// `Engine::complete`, no end listeners: `ElementCompleted` is inline.
    Inline,
    /// `Engine::finalize_completion`, after the end-listener chain drained.
    Deferred,
}

/// Where the completed token goes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompletionRoute {
    /// A compensation handler completed: route back to its throw event.
    CompensationHandler,
    /// Hand the token to the matching link catch event.
    LinkHandoff,
    /// Raise the escalation (`raise_escalation_from_throw`).
    RaiseEscalation,
    /// Throw the BPMN error (`throw_error_from_end_event`).
    ThrowError,
    /// Take every outgoing flow; `race` tags the activations with this
    /// completing event-based gateway's instance.
    TakeFlows { race: bool },
}

/// The completion decision.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CompletionPlan {
    /// Emit `completion_compensation_subscriptions` before routing.
    pub register_compensation: bool,
    /// Where the token goes.
    pub route: CompletionRoute,
}

/// Decides a completion. `pending_handler` is whether the completing instance
/// is a compensation handler a throw event waits on.
///
/// The two phases differ only in compensation registration, exactly as the
/// engine paths did before extraction: inline completion registers for every
/// non-handler element; the deferred finalize registers only on the
/// take-flows route.
pub fn completion_plan(
    phase: Phase,
    pending_handler: bool,
    kind: CompletionKind,
) -> CompletionPlan {
    if pending_handler {
        return CompletionPlan {
            register_compensation: false,
            route: CompletionRoute::CompensationHandler,
        };
    }
    let route = match kind {
        CompletionKind::LinkThrow => CompletionRoute::LinkHandoff,
        CompletionKind::EscalationThrow => CompletionRoute::RaiseEscalation,
        CompletionKind::ErrorEnd => CompletionRoute::ThrowError,
        CompletionKind::EventBasedGateway => CompletionRoute::TakeFlows { race: true },
        CompletionKind::Other => CompletionRoute::TakeFlows { race: false },
    };
    let register_compensation = match phase {
        Phase::Inline => true,
        Phase::Deferred => matches!(route, CompletionRoute::TakeFlows { .. }),
    };
    CompletionPlan {
        register_compensation,
        route,
    }
}

/// What the escalation catch walk found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EscalationCatch {
    /// No catch: escalation is non-critical, the throw passes through.
    Uncaught,
    /// An escalation event sub-process hosted by an enclosing scope.
    EventSubProcess { interrupting: bool },
    /// An escalation boundary on an enclosing activity.
    Boundary { interrupting: bool },
}

/// The step the catch contributes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EscalationAction {
    /// Nothing beyond the throw's own flows.
    None,
    /// Queue `Step::TriggerEventSubProcess` (interrupting ESP).
    TriggerEventSubProcess,
    /// Activate the event sub-process in its host scope (non-interrupting).
    ActivateEventSubProcess,
    /// Activate the boundary next to the caught activity (non-interrupting).
    ActivateBoundary,
    /// Tear the caught activity down, then activate the boundary.
    InterruptAndActivateBoundary,
}

/// Which instance a boundary catch acts on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BoundaryHost {
    /// The caught activity itself.
    CaughtActivity,
    /// The caught activity is a multi-instance child: the boundary is armed on
    /// the loop's body (#1173, the #1170 MI class).
    MultiInstanceBody,
}

/// The escalation decision. When `take_throw_flows`, the throw's outgoing
/// flows are emitted before the action's step.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EscalationPlan {
    /// Route the throw's own token onward.
    pub take_throw_flows: bool,
    /// The catch's step.
    pub action: EscalationAction,
}

/// Decides an escalation throw's routing from its catch.
pub fn escalation_plan(catch: EscalationCatch) -> EscalationPlan {
    let (take_throw_flows, action) = match catch {
        EscalationCatch::Uncaught => (true, EscalationAction::None),
        EscalationCatch::EventSubProcess { interrupting: true } => {
            (false, EscalationAction::TriggerEventSubProcess)
        }
        EscalationCatch::EventSubProcess {
            interrupting: false,
        } => (true, EscalationAction::ActivateEventSubProcess),
        EscalationCatch::Boundary { interrupting: true } => {
            (false, EscalationAction::InterruptAndActivateBoundary)
        }
        EscalationCatch::Boundary {
            interrupting: false,
        } => (true, EscalationAction::ActivateBoundary),
    };
    EscalationPlan {
        take_throw_flows,
        action,
    }
}

/// The instance a boundary catch acts on.
pub fn boundary_host(caught_is_mi_child: bool) -> BoundaryHost {
    if caught_is_mi_child {
        BoundaryHost::MultiInstanceBody
    } else {
        BoundaryHost::CaughtActivity
    }
}

#[cfg(kani)]
mod kani_proofs;
#[cfg(test)]
mod tests;
