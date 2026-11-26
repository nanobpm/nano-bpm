//! The join guard as a pure kernel (#2010): Zeebe's `ZeebeJoinReady` for a
//! parallel or inclusive join over a dense, index-based graph view.
//!
//! This module is the *decision* half of the join guard; the *gathering* half
//! (live-source set, scope filtering, the #1989 doomed-boundary substitution,
//! in-transit activations, the id → index mapping) stays in
//! [`Engine::has_active_path_to`](super::Engine) and `activate_join`. Keep this
//! module dependency-free and in the subset Aeneas extracts (see AGENTS.md,
//! "Write Provable Code"): it is the only input of the Charon/Aeneas extraction
//! `formal/lean/Refinement/Extracted/` and of the Lean proof
//! `formal/lean/Refinement/Refinement/Proof.lean`. `formal/lean/Refinement/extract.sh`
//! regenerates the extraction and CI fails on any diff, so every change here
//! re-runs the proof.
//!
//! Graph encoding: nodes are `0..succ.len()`; `succ[n]` lists `(target, flow)`
//! for every edge out of `n` — an outgoing sequence flow (`flow` is the flow's
//! index into `taken`), a host → attached boundary event edge, or a link throw
//! → same-scope link catch edge (`flow` is [`NO_FLOW`] for the last two).

/// The `flow` of an edge that is not a sequence flow (boundary attachment or
/// link throw → catch). Such an edge is never a taken flow into the join.
pub const NO_FLOW: usize = usize::MAX;

/// The two synchronising joins: a parallel or inclusive gateway with more than
/// one incoming flow.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Parallel,
    Inclusive,
}

/// What the guard needs to decide a join activation once the arrivals are
/// counted (Zeebe's `ProcessInstanceStateTransitionGuard`
/// `canActivateParallelGateway` / `canActivateInclusiveGateway`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JoinDecision {
    /// Accept the activation: every incoming flow has been taken.
    Accept,
    /// Reject the activation: the tokens keep waiting.
    Reject,
    /// An inclusive join with at least one, but not every, flow taken: accept
    /// exactly when no live path can still reach the join
    /// ([`has_active_path`]).
    AcceptUnlessActivePath,
}

/// The counting half of the guard. `taken` is the number of distinct incoming
/// flows holding a token plus the unidentified arrivals; `incoming` the number
/// of incoming flows.
pub fn join_decision(kind: JoinKind, taken: usize, incoming: usize) -> JoinDecision {
    if taken >= incoming {
        JoinDecision::Accept
    } else {
        match kind {
            JoinKind::Parallel => JoinDecision::Reject,
            JoinKind::Inclusive => {
                if taken > 0 {
                    JoinDecision::AcceptUnlessActivePath
                } else {
                    JoinDecision::Reject
                }
            }
        }
    }
}

/// Whether the edge `(target, flow)` may be followed: every edge except a flow
/// into `join` that has already been taken (`taken[flow] > 0`), since each
/// incoming flow needs to be taken only once.
pub fn edge_open(target: usize, flow: usize, join: usize, taken: &[u32]) -> bool {
    if target != join {
        return true;
    }
    match taken.get(flow) {
        Some(count) => *count == 0,
        None => true,
    }
}

/// One BFS step: enqueues every unvisited target of an open edge in `edges`,
/// marking it visited. Returns `true` as soon as an open edge reaches `join`
/// (the join is never enqueued).
fn enqueue_open_successors(
    edges: &[(usize, usize)],
    join: usize,
    taken: &[u32],
    visited: &mut [bool],
    queue: &mut Vec<usize>,
) -> bool {
    let mut i = 0;
    while i < edges.len() {
        let (target, flow) = edges[i];
        if edge_open(target, flow, join, taken) {
            if target == join {
                return true;
            }
            if target < visited.len() && !visited[target] {
                visited[target] = true;
                queue.push(target);
            }
        }
        i += 1;
    }
    false
}

/// The graph search of Zeebe's `BpmnInclusiveGatewayBehavior`
/// (`hasActivePathToTheGateway`): can a token at `source` reach `join` over
/// open edges ([`edge_open`])?
///
/// A breadth-first search with an append-only queue read through a `head`
/// cursor (no `pop`, which Aeneas does not model) and a visited set marked on
/// enqueue, so every node enters the queue at most once and the queue never
/// outgrows `succ.len()`. Nodes outside `0..succ.len()` have no successors.
pub fn path_reaches_join(
    succ: &[Vec<(usize, usize)>],
    source: usize,
    join: usize,
    taken: &[u32],
) -> bool {
    if source == join {
        return true;
    }
    if source >= succ.len() {
        return false;
    }
    let mut visited = vec![false; succ.len()];
    visited[source] = true;
    let mut queue = vec![source];
    let mut head = 0;
    while head < queue.len() {
        let node = queue[head];
        head += 1;
        if enqueue_open_successors(&succ[node], join, taken, &mut visited, &mut queue) {
            return true;
        }
    }
    false
}

/// `ZeebeHasActivePath`: does any live source other than the join itself reach
/// the join? (Zeebe ignores the join as a source: its own arrival is already
/// counted in `taken`.)
pub fn has_active_path(
    succ: &[Vec<(usize, usize)>],
    live: &[usize],
    join: usize,
    taken: &[u32],
) -> bool {
    let mut i = 0;
    while i < live.len() {
        if live[i] != join && path_reaches_join(succ, live[i], join, taken) {
            return true;
        }
        i += 1;
    }
    false
}

/// How many of the join's `incoming` flows hold a token.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "part of the proven `join_ready` composition (formal/lean/Refinement)"
    )
)]
pub fn distinct_taken(incoming: &[usize], taken: &[u32]) -> usize {
    let mut count: usize = 0;
    let mut i = 0;
    while i < incoming.len() {
        let flow = incoming[i];
        if flow < taken.len() && taken[flow] > 0 {
            // `count <= i < incoming.len()`: cannot overflow.
            count += 1;
        }
        i += 1;
    }
    count
}

/// `ZeebeJoinReady`, parallel and inclusive: the whole guard over the dense
/// view. `incoming` are the join's incoming flows (indices into `taken`),
/// `live` the live sources and `unidentified` the arrivals with no known
/// incoming flow, which count towards "every flow taken" as in
/// `Engine::activate_join`.
///
/// The engine evaluates the same decision in two halves —
/// [`join_decision`] on its own arrival count, then [`has_active_path`] only
/// when needed — so it builds the graph view only for inclusive joins; this
/// composition is the statement the Lean proof is about.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the proven composition (formal/lean/Refinement); the engine calls its halves"
    )
)]
pub fn join_ready(
    succ: &[Vec<(usize, usize)>],
    kind: JoinKind,
    join: usize,
    incoming: &[usize],
    live: &[usize],
    taken: &[u32],
    unidentified: usize,
) -> bool {
    let arrived = distinct_taken(incoming, taken).saturating_add(unidentified);
    match join_decision(kind, arrived, incoming.len()) {
        JoinDecision::Accept => true,
        JoinDecision::Reject => false,
        JoinDecision::AcceptUnlessActivePath => !has_active_path(succ, live, join, taken),
    }
}

#[cfg(test)]
mod tests;
