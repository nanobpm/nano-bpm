//! Split-gateway outgoing-flow selection as a pure kernel (#1983).
//!
//! The *decision* half of exclusive / inclusive gateway routing (Zeebe's
//! `ExclusiveGatewayProcessor.findSequenceFlowToTake` and
//! `InclusiveGatewayProcessor.findSequenceFlowsToTake`, plus Nano's
//! unconditional-flow fallback). The *gathering* half — resolving the element,
//! ordering its flows in execution order and evaluating each condition against
//! the variable scope — stays in `Engine::select_exclusive_flow` /
//! `Engine::select_inclusive_flows`, which then map the positions returned here
//! back to flow indices. Keep this module dependency-free and in the subset
//! Aeneas extracts (AGENTS.md, "Write Provable Code").
//!
//! Input: one [`Guard`] per outgoing flow, **in execution order** (reverse
//! document order). Outputs are positions into that slice.
//!
//! Conditions are pure FEEL, so evaluating all of them up front (the collector)
//! and scanning in order here selects exactly what the old lazy loop did: a
//! condition that errors after the first holding one is never looked at.

/// What the collector learned about one outgoing flow.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Guard {
    /// The gateway's explicit `default` flow: a fallback only.
    Default,
    /// A non-default flow without a condition (Nano extension): taken by an
    /// inclusive gateway always, by an exclusive one when nothing holds.
    Unconditional,
    /// A condition that evaluated `true`.
    Holds,
    /// A condition that evaluated `false`.
    Fails,
    /// A condition that failed to evaluate.
    Errors,
}

/// The outcome of a selection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Route {
    /// Exclusive: take the flow at this position. Inclusive: take every
    /// position marked in the `selected` mask.
    Take(usize),
    /// No flow matched and there is no default flow.
    NoMatch,
    /// The condition at this position failed to evaluate.
    ConditionError(usize),
}

/// Exclusive gateway: the first holding condition wins; otherwise the
/// last-scanned (first-declared) unconditional flow; otherwise the default.
pub fn select_exclusive(guards: &[Guard]) -> Route {
    let mut default_flow: Option<usize> = None;
    let mut unconditional: Option<usize> = None;
    let mut i = 0;
    while i < guards.len() {
        match guards[i] {
            Guard::Default => default_flow = Some(i),
            Guard::Unconditional => unconditional = Some(i),
            Guard::Holds => return Route::Take(i),
            Guard::Fails => {}
            Guard::Errors => return Route::ConditionError(i),
        }
        i += 1;
    }
    match unconditional {
        Some(u) => Route::Take(u),
        None => match default_flow {
            Some(d) => Route::Take(d),
            None => Route::NoMatch,
        },
    }
}

/// Inclusive gateway: marks in `selected` every unconditional or holding flow,
/// or only the default flow when none is. `selected` must be at least as long
/// as `guards` (a shorter mask is never written past its end). Returns
/// `Take(count)` with the number of marked flows (never 0), or the failure; on
/// failure the mask's contents are unspecified.
pub fn select_inclusive(guards: &[Guard], selected: &mut [bool]) -> Route {
    let mut default_flow: Option<usize> = None;
    let mut count: usize = 0;
    let mut i = 0;
    while i < guards.len() {
        let take = match guards[i] {
            Guard::Default => {
                default_flow = Some(i);
                false
            }
            Guard::Unconditional | Guard::Holds => true,
            Guard::Fails => false,
            Guard::Errors => return Route::ConditionError(i),
        };
        if let Some(slot) = selected.get_mut(i) {
            *slot = take;
        }
        if take {
            count = count.saturating_add(1);
        }
        i += 1;
    }
    if count == 0 {
        match default_flow {
            Some(d) => {
                if let Some(slot) = selected.get_mut(d) {
                    *slot = true;
                }
                Route::Take(1)
            }
            None => Route::NoMatch,
        }
    } else {
        Route::Take(count)
    }
}

#[cfg(kani)]
mod kani_proofs;
#[cfg(test)]
mod tests;
