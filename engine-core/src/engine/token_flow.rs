//! `TokenFlow.tla` token invariants over the extracted kernels (#2050, part
//! of #1983).
//!
//! A bounded token-flow step machine whose every *decision* is delegated to
//! the production kernels — [`completion_step::activation_route`] /
//! [`completion_step::completion_plan`] (activate / complete),
//! [`gateway_route::select_exclusive`] / [`gateway_route::select_inclusive`]
//! (split routing) and [`join_guard::join_decision`] /
//! [`join_guard::has_active_path`] (join readiness). The machine itself holds
//! only the bookkeeping `TokenFlow.tla` holds (`pending`, `joinTokens`, the
//! element instances), so the invariants it checks are statements about what
//! the kernels' decisions do to that state:
//!
//! - [`no_instance_active_and_completed`]: no element instance is both active
//!   and completed (a completed instance is never reactivated, a slot is never
//!   reused);
//! - [`tokens_accounted`]: every token is an active element instance or a taken
//!   flow (in transit, or held on a join's incoming flow), checked against a
//!   ghost count updated only from the kernels' outputs.
//!
//! Compiled only for tests and Kani: it is a model, not an engine path, so it
//! cannot drift production behaviour. The unit tests (`token_flow/tests.rs`)
//! run it over many random graphs and step sequences; the Kani harnesses
//! (`token_flow/kani_proofs.rs`) prove the invariants for every graph and step
//! sequence within a small bound.

use super::completion_step::{self, ActivationRoute, CompletionKind, CompletionRoute, Phase};
use super::gateway_route::{self, Guard, Route};
use super::join_guard::{self, JoinDecision, JoinKind};

/// Nodes in the bounded graph.
pub const NODES: usize = 3;
/// Sequence flows in the bounded graph.
pub const FLOWS: usize = 3;
/// Element-instance slots (an instance key is never reused).
pub const SLOTS: usize = 3;

/// The element kinds the machine distinguishes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// A wait state: activates and stays active until [`Action::CompleteTask`].
    Task,
    /// Exclusive gateway (split): one outgoing flow.
    Exclusive,
    /// Inclusive gateway: a join when it has ≥ 2 incoming flows; a split
    /// taking a non-empty subset.
    Inclusive,
    /// Parallel gateway: a join when it has ≥ 2 incoming flows; takes all.
    Parallel,
    /// None end event (pass-through over any outgoing flows).
    End,
}

/// A bounded process graph: flow `f` runs `src[f] -> tgt[f]`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Graph {
    pub kind: [Kind; NODES],
    pub src: [usize; FLOWS],
    pub tgt: [usize; FLOWS],
}

/// The token bookkeeping of one process instance.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TokenState {
    /// Element instance `i` is active.
    pub active: [bool; SLOTS],
    /// Element instance `i` has completed.
    pub completed: [bool; SLOTS],
    /// The node element instance `i` instantiates.
    pub node: [usize; SLOTS],
    /// Queued activations per taken flow (`pending`).
    pub pending: [u32; FLOWS],
    /// Tokens counted on a join's incoming flow (`joinTokens`).
    pub arrivals: [u32; FLOWS],
    /// Ghost: tokens alive, updated only from kernel outputs.
    pub ghost: u32,
}

/// One step of the machine. `guards[k]` is the guard of the gateway's `k`-th
/// outgoing flow (in flow-index order), used only by exclusive / inclusive
/// routing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    /// Drain one queued activation of flow `f` (`Engine::activate`).
    Drain { flow: usize, guards: [Guard; FLOWS] },
    /// A job worker completes the task instance in `slot`.
    CompleteTask { slot: usize },
}

/// Why a step did not apply (the state is then unchanged).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Refused {
    /// The action is not enabled in this state.
    NotEnabled,
    /// Every instance slot is used (the bound, not a defect).
    OutOfSlots,
}

impl Graph {
    fn in_range(&self) -> bool {
        let mut f = 0;
        while f < FLOWS {
            if self.src[f] >= NODES || self.tgt[f] >= NODES {
                return false;
            }
            f += 1;
        }
        true
    }

    fn incoming(&self, n: usize) -> usize {
        let mut count = 0;
        let mut f = 0;
        while f < FLOWS {
            if self.tgt[f] == n {
                count += 1;
            }
            f += 1;
        }
        count
    }

    /// A parallel / inclusive gateway with ≥ 2 incoming flows (`IsJoin`).
    pub fn is_join(&self, n: usize) -> bool {
        n < NODES
            && matches!(self.kind[n], Kind::Parallel | Kind::Inclusive)
            && self.incoming(n) >= 2
    }

    /// Flow `f` delivers into a join, so its token is held in `arrivals`.
    pub fn feeds_join(&self, f: usize) -> bool {
        f < FLOWS && self.is_join(self.tgt[f])
    }
}

impl TokenState {
    /// One activation queued on `start_flow` (the instance-creation token).
    pub fn new(graph: &Graph, start_flow: usize) -> Option<TokenState> {
        if !graph.in_range() || start_flow >= FLOWS {
            return None;
        }
        let mut s = TokenState {
            active: [false; SLOTS],
            completed: [false; SLOTS],
            node: [0; SLOTS],
            pending: [0; FLOWS],
            arrivals: [0; FLOWS],
            ghost: 1,
        };
        take_flow(graph, &mut s, start_flow);
        Some(s)
    }
}

/// `take_flow`: queue the activation and, into a join, count the arrival.
fn take_flow(graph: &Graph, s: &mut TokenState, f: usize) {
    s.pending[f] = s.pending[f].saturating_add(1);
    if graph.feeds_join(f) {
        s.arrivals[f] = s.arrivals[f].saturating_add(1);
    }
}

fn free_slot(s: &TokenState) -> Option<usize> {
    let mut i = 0;
    while i < SLOTS {
        if !s.active[i] && !s.completed[i] {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn complete_slot(s: &mut TokenState, slot: usize) {
    s.active[slot] = false;
    s.completed[slot] = true;
    s.ghost = s.ghost.saturating_sub(1);
}

/// The completing instance in `slot` routes its token through the kernels.
/// On an incident (no match, condition error) it stays active.
fn complete_and_route(graph: &Graph, s: &mut TokenState, slot: usize, guards: &[Guard; FLOWS]) {
    let n = s.node[slot];
    let plan = completion_step::completion_plan(Phase::Inline, false, CompletionKind::Other);
    if !matches!(plan.route, CompletionRoute::TakeFlows { .. }) {
        return;
    }
    let mut out = [0usize; FLOWS];
    let mut outs = 0;
    let mut f = 0;
    while f < FLOWS {
        if graph.src[f] == n {
            out[outs] = f;
            outs += 1;
        }
        f += 1;
    }
    let mut take = [false; FLOWS];
    match graph.kind[n] {
        Kind::Exclusive => match gateway_route::select_exclusive(&guards[..outs]) {
            Route::Take(pos) if pos < outs => take[pos] = true,
            _ => return,
        },
        Kind::Inclusive => match gateway_route::select_inclusive(&guards[..outs], &mut take) {
            Route::Take(_) => {}
            _ => return,
        },
        Kind::Task | Kind::Parallel | Kind::End => {
            let mut k = 0;
            while k < outs {
                take[k] = true;
                k += 1;
            }
        }
    }
    complete_slot(s, slot);
    let mut k = 0;
    while k < outs {
        if take[k] {
            take_flow(graph, s, out[k]);
            s.ghost = s.ghost.saturating_add(1);
        }
        k += 1;
    }
}

/// Capacity of the live-source list: one per instance slot and per flow.
const LIVE: usize = SLOTS + FLOWS;

/// The live sources of `has_active_path` — active instances and the targets
/// of queued activations — in a fixed-capacity array (no `Vec` growth for
/// Kani to model); returns the array and its length.
fn live_sources(graph: &Graph, s: &TokenState) -> ([usize; LIVE], usize) {
    let mut live = [0usize; LIVE];
    let mut n = 0;
    let mut i = 0;
    while i < SLOTS {
        if s.active[i] {
            live[n] = s.node[i];
            n += 1;
        }
        i += 1;
    }
    let mut f = 0;
    while f < FLOWS {
        if s.pending[f] > 0 {
            live[n] = graph.tgt[f];
            n += 1;
        }
        f += 1;
    }
    (live, n)
}

/// Join readiness, exactly as `Engine::activate_join` splits it: the arrival
/// count decision first, the active-path search only when it is needed.
fn join_fires(graph: &Graph, s: &TokenState, j: usize) -> bool {
    let mut incoming = [0usize; FLOWS];
    let mut count = 0;
    let mut f = 0;
    while f < FLOWS {
        if graph.tgt[f] == j {
            incoming[count] = f;
            count += 1;
        }
        f += 1;
    }
    let taken = join_guard::distinct_taken(&incoming[..count], &s.arrivals);
    let kind = if graph.kind[j] == Kind::Parallel {
        JoinKind::Parallel
    } else {
        JoinKind::Inclusive
    };
    match join_guard::join_decision(kind, taken, count) {
        JoinDecision::Accept => true,
        JoinDecision::Reject => false,
        JoinDecision::AcceptUnlessActivePath => {
            // `has_active_path` takes the engine's adjacency (`&[Vec<_>]`).
            // Each list is pre-sized to `FLOWS`, so building it never
            // reallocates (no growth path for Kani to explore).
            let mut succ: [Vec<(usize, usize)>; NODES] = [
                Vec::with_capacity(FLOWS),
                Vec::with_capacity(FLOWS),
                Vec::with_capacity(FLOWS),
            ];
            let mut g = 0;
            while g < FLOWS {
                succ[graph.src[g]].push((graph.tgt[g], g));
                g += 1;
            }
            let (live, n) = live_sources(graph, s);
            !join_guard::has_active_path(&succ, &live[..n], j, &s.arrivals)
        }
    }
}

/// Applies one action; on `Err` the state is unchanged.
pub fn step(graph: &Graph, s: &mut TokenState, action: Action) -> Result<(), Refused> {
    match action {
        Action::CompleteTask { slot } => {
            if slot >= SLOTS || !s.active[slot] || graph.kind[s.node[slot]] != Kind::Task {
                return Err(Refused::NotEnabled);
            }
            complete_and_route(graph, s, slot, &[Guard::Unconditional; FLOWS]);
            Ok(())
        }
        Action::Drain { flow, guards } => {
            if flow >= FLOWS || s.pending[flow] == 0 {
                return Err(Refused::NotEnabled);
            }
            let n = graph.tgt[flow];
            let Some(slot) = free_slot(s) else {
                return Err(Refused::OutOfSlots);
            };
            s.pending[flow] -= 1;
            match completion_step::activation_route(graph.is_join(n), false) {
                ActivationRoute::Join => {
                    if !join_fires(graph, s, n) {
                        // The token stays held on the join's flow.
                        return Ok(());
                    }
                    let mut f = 0;
                    while f < FLOWS {
                        if graph.tgt[f] == n && s.arrivals[f] > 0 {
                            s.arrivals[f] -= 1;
                            s.ghost = s.ghost.saturating_sub(1);
                        }
                        f += 1;
                    }
                    s.ghost = s.ghost.saturating_add(1);
                }
                ActivationRoute::Activate { .. } => {}
            }
            s.active[slot] = true;
            s.node[slot] = n;
            if graph.kind[n] != Kind::Task {
                complete_and_route(graph, s, slot, &guards);
            }
            Ok(())
        }
    }
}

/// `TokenFlow.tla`: no element instance is both active and completed.
pub fn no_instance_active_and_completed(s: &TokenState) -> bool {
    let mut i = 0;
    while i < SLOTS {
        if s.active[i] && s.completed[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// `TokenFlow.tla`: every token is an active element instance or a taken
/// flow — in transit (`pending`) or held on a join (`joinTokens`) — and only
/// a join's incoming flows hold arrivals.
pub fn tokens_accounted(graph: &Graph, s: &TokenState) -> bool {
    let mut tokens: u32 = 0;
    let mut i = 0;
    while i < SLOTS {
        if s.active[i] {
            tokens = tokens.saturating_add(1);
        }
        i += 1;
    }
    let mut f = 0;
    while f < FLOWS {
        if graph.feeds_join(f) {
            tokens = tokens.saturating_add(s.arrivals[f]);
        } else {
            if s.arrivals[f] != 0 {
                return false;
            }
            tokens = tokens.saturating_add(s.pending[f]);
        }
        f += 1;
    }
    tokens == s.ghost
}

#[cfg(kani)]
mod kani_proofs;
#[cfg(test)]
mod tests;
