//! Kani harnesses for the `TokenFlow.tla` token invariants over the kernels
//! (#2050, part of #1983).
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`.
//!
//! The invariants are proved *inductively*, one transition per harness, so no
//! harness unrolls a multi-step run: the initial state satisfies them
//! (`token_flow_initial_state_satisfies_invariants`), and from **any** bounded
//! state that satisfies them one drain
//! (`token_flow_drain_preserves_invariants`) or one task completion
//! (`token_flow_complete_task_preserves_invariants`) preserves them. The
//! inclusive join's active-path search is an oracle here (see
//! `token_flow::active_path`); the search is proved in `join_guard`.

use super::*;

fn any_kind() -> Kind {
    match kani::any::<u8>() % 5 {
        0 => Kind::Task,
        1 => Kind::Exclusive,
        2 => Kind::Inclusive,
        3 => Kind::Parallel,
        _ => Kind::End,
    }
}

fn any_guard() -> Guard {
    match kani::any::<u8>() % 5 {
        0 => Guard::Default,
        1 => Guard::Unconditional,
        2 => Guard::Holds,
        3 => Guard::Fails,
        _ => Guard::Errors,
    }
}

/// Any in-range graph of `NODES` nodes and `FLOWS` flows.
fn any_graph() -> Graph {
    let graph = Graph {
        kind: [any_kind(), any_kind(), any_kind()],
        src: [kani::any(), kani::any(), kani::any()],
        tgt: [kani::any(), kani::any(), kani::any()],
    };
    kani::assume(graph.in_range());
    graph
}

/// Largest per-flow count in an arbitrary state: far below saturation, so the
/// saturating counters never clamp inside one step.
const MAX_COUNT: u32 = 2;

/// Any state satisfying both invariants, with bounded counters and every
/// instance on an in-range node.
fn any_state(graph: &Graph) -> TokenState {
    let mut s = TokenState {
        active: [kani::any(), kani::any(), kani::any()],
        completed: [kani::any(), kani::any(), kani::any()],
        node: [kani::any(), kani::any(), kani::any()],
        pending: [kani::any(), kani::any(), kani::any()],
        arrivals: [kani::any(), kani::any(), kani::any()],
        ghost: kani::any(),
    };
    let mut i = 0;
    while i < SLOTS {
        kani::assume(s.node[i] < NODES);
        i += 1;
    }
    let mut f = 0;
    while f < FLOWS {
        kani::assume(s.pending[f] <= MAX_COUNT && s.arrivals[f] <= MAX_COUNT);
        f += 1;
    }
    s.ghost = s.ghost % 16;
    kani::assume(no_instance_active_and_completed(&s));
    kani::assume(tokens_accounted(graph, &s));
    s
}

/// Field-wise state equality with explicit bounded loops (the derived
/// `PartialEq` lowers the arrays to `memcmp`, whose byte loop outruns the
/// harness unwind).
fn same_state(a: &TokenState, b: &TokenState) -> bool {
    let mut i = 0;
    while i < SLOTS {
        if a.active[i] != b.active[i] || a.completed[i] != b.completed[i] || a.node[i] != b.node[i]
        {
            return false;
        }
        i += 1;
    }
    let mut f = 0;
    while f < FLOWS {
        if a.pending[f] != b.pending[f] || a.arrivals[f] != b.arrivals[f] {
            return false;
        }
        f += 1;
    }
    a.ghost == b.ghost
}

fn assert_invariants(graph: &Graph, s: &TokenState) {
    assert!(no_instance_active_and_completed(s));
    assert!(tokens_accounted(graph, s));
}

/// Base case: the instance-creation state satisfies both invariants on every
/// graph and start flow (a start flow out of range is refused).
#[kani::proof]
#[kani::unwind(4)]
fn token_flow_initial_state_satisfies_invariants() {
    let graph = any_graph();
    let start: usize = kani::any();
    let made = TokenState::new(&graph, start);
    if let Some(s) = made {
        assert_invariants(&graph, &s);
    }
    kani::cover!(made.is_some(), "an instance is created");
}

/// Inductive step, drain: from any bounded state satisfying both invariants,
/// on any graph (any kinds and wiring, inclusive joins included), draining any flow with any
/// guards keeps both invariants, and a refused drain changes nothing.
#[kani::proof]
// Every loop runs over NODES = FLOWS = SLOTS = 3 elements: unwind 4 is complete.
#[kani::unwind(4)]
fn token_flow_drain_preserves_invariants() {
    let graph = any_graph();
    let mut s = any_state(&graph);
    let flow: usize = kani::any();
    kani::assume(flow < FLOWS);
    let before = s;
    let action = Action::Drain {
        flow,
        guards: [any_guard(), any_guard(), any_guard()],
    };
    let applied = step(&graph, &mut s, action).is_ok();
    if !applied {
        assert!(same_state(&s, &before));
    }
    assert_invariants(&graph, &s);
    kani::cover!(
        applied && graph.is_join(graph.tgt[flow]) && s.ghost != before.ghost,
        "a join fires"
    );
    kani::cover!(
        applied
            && graph.is_join(graph.tgt[flow])
            && graph.kind[graph.tgt[flow]] == Kind::Inclusive
            && s.ghost == before.ghost,
        "an inclusive join holds the token"
    );
    kani::cover!(
        applied && graph.kind[graph.tgt[flow]] == Kind::Exclusive && s.ghost == before.ghost,
        "an exclusive split routes the token on"
    );
}

/// Inductive step, task completion: from any bounded state satisfying both
/// invariants, on any graph, completing any instance slot keeps both
/// invariants, and a refused completion changes nothing.
#[kani::proof]
#[kani::unwind(4)]
fn token_flow_complete_task_preserves_invariants() {
    let graph = any_graph();
    let mut s = any_state(&graph);
    let slot: usize = kani::any();
    kani::assume(slot < SLOTS);
    let before = s;
    let applied = step(&graph, &mut s, Action::CompleteTask { slot }).is_ok();
    if !applied {
        assert!(same_state(&s, &before));
    }
    assert_invariants(&graph, &s);
    kani::cover!(applied && s.completed[slot], "a task completes");
}
