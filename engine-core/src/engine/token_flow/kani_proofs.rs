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
//! (`token_flow_complete_task_preserves_invariants`) preserves them. The two
//! inclusive-join harnesses pin the join's fire / wait decision on fixed
//! graphs.

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

/// Any in-range graph of `NODES` nodes and `FLOWS` flows. Inclusive gateways
/// appear as splits only: the inclusive join's active-path search has its own
/// fixed-graph harnesses, which keeps the BFS out of the symbolic-graph ones.
fn any_graph() -> Graph {
    let graph = Graph {
        kind: [any_kind(), any_kind(), any_kind()],
        src: [kani::any(), kani::any(), kani::any()],
        tgt: [kani::any(), kani::any(), kani::any()],
    };
    kani::assume(graph.in_range());
    let mut n = 0;
    while n < NODES {
        kani::assume(!(graph.kind[n] == Kind::Inclusive && graph.is_join(n)));
        n += 1;
    }
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
/// on any graph (inclusive gateways as splits), draining any flow with any
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
        assert!(s == before);
    }
    assert_invariants(&graph, &s);
    kani::cover!(
        applied && graph.is_join(graph.tgt[flow]) && s.ghost != before.ghost,
        "a parallel join fires"
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
        assert!(s == before);
    }
    assert_invariants(&graph, &s);
    kani::cover!(applied && s.completed[slot], "a task completes");
}

/// The inclusive join (`join_decision` + `has_active_path`): split 0 feeds
/// join 1 over flows 0 and 1, the join feeds end 2. From any reachable
/// arrival pattern, one arbitrary drain keeps both invariants.
#[kani::proof]
#[kani::unwind(4)]
fn token_flow_inclusive_join_invariants_preserved() {
    let graph = Graph {
        kind: [Kind::Inclusive, Kind::Inclusive, Kind::End],
        src: [0, 0, 1],
        tgt: [1, 1, 2],
    };
    let Some(mut s) = TokenState::new(&graph, 0) else {
        return;
    };
    // Optionally the split also took flow 1, still in transit or drained.
    if kani::any() {
        s.arrivals[1] = 1;
        s.ghost += 1;
        if kani::any() {
            s.pending[1] = 1;
        }
    }
    // Optionally the first arrival was already drained and held.
    if kani::any() {
        s.pending[0] = 0;
    }
    assert_invariants(&graph, &s);
    let action = Action::Drain {
        flow: kani::any(),
        guards: [any_guard(), any_guard(), any_guard()],
    };
    let before = s;
    let applied = step(&graph, &mut s, action).is_ok();
    if !applied {
        assert!(s == before);
    }
    assert_invariants(&graph, &s);
    kani::cover!(
        applied && s.arrivals == [0, 0, 0] && before.arrivals == [1, 0, 0],
        "the inclusive join fires with one flow taken"
    );
}

/// The inclusive join waits while a live sibling can still reach a missing
/// incoming flow: split 0 feeds join 1 directly (flow 0) and via task 2
/// (flow 2 then flow 1). With the flow-0 arrival present and task 2 live
/// (queued or active), draining flow 0 must hold the token, not fire.
#[kani::proof]
#[kani::unwind(4)]
fn token_flow_inclusive_join_waits_for_live_sibling() {
    let graph = Graph {
        kind: [Kind::Inclusive, Kind::Inclusive, Kind::Task],
        src: [0, 2, 0],
        tgt: [1, 1, 2],
    };
    let Some(mut s) = TokenState::new(&graph, 0) else {
        return;
    };
    let sibling_live: bool = kani::any();
    if sibling_live {
        s.ghost += 1;
        if kani::any() {
            s.pending[2] = 1;
        } else {
            s.active[0] = true;
            s.node[0] = 2;
        }
    }
    assert_invariants(&graph, &s);
    let before = s;
    let applied = step(
        &graph,
        &mut s,
        Action::Drain {
            flow: 0,
            guards: [any_guard(), any_guard(), any_guard()],
        },
    )
    .is_ok();
    assert!(applied);
    assert_invariants(&graph, &s);
    let fired = s.arrivals[0] == 0;
    assert!(
        fired != sibling_live,
        "waits iff a live sibling can reach the join"
    );
    kani::cover!(
        sibling_live && !fired && before.arrivals[0] == 1,
        "join waits"
    );
    kani::cover!(!sibling_live && fired, "join fires");
}
