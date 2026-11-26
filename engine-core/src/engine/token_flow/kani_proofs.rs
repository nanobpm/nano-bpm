//! Kani harnesses for the `TokenFlow.tla` token invariants over the kernels
//! (#2050, part of #1983).
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! (`formal/kani/check.sh token_flow_invariants_preserved`,
//! `token_flow_inclusive_join_invariants_preserved`).

use super::*;

fn any_kind(allow_inclusive: bool) -> Kind {
    match kani::any::<u8>() % 5 {
        0 => Kind::Task,
        1 => Kind::Exclusive,
        2 if allow_inclusive => Kind::Inclusive,
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

fn any_action() -> Action {
    if kani::any() {
        Action::CompleteTask { slot: kani::any() }
    } else {
        Action::Drain {
            flow: kani::any(),
            guards: [any_guard(), any_guard(), any_guard()],
        }
    }
}

fn assert_invariants(graph: &Graph, s: &TokenState) {
    assert!(no_instance_active_and_completed(s));
    assert!(tokens_accounted(graph, s));
}

/// Every graph of 3 nodes / 3 flows (any kinds, any wiring, inclusive
/// gateways as splits only — the inclusive join's active-path search has its
/// own harness), any start flow, then two arbitrary steps: both invariants
/// hold after each step, and a refused step leaves the state unchanged.
#[kani::proof]
// Every loop runs over FLOWS = 3 or SLOTS = 4 elements: unwind 5 is complete.
#[kani::unwind(5)]
fn token_flow_invariants_preserved() {
    let graph = Graph {
        kind: [any_kind(true), any_kind(true), any_kind(true)],
        src: [kani::any(), kani::any(), kani::any()],
        tgt: [kani::any(), kani::any(), kani::any()],
    };
    let mut n = 0;
    while n < NODES {
        kani::assume(!(graph.kind[n] == Kind::Inclusive && graph.is_join(n)));
        n += 1;
    }
    let start: usize = kani::any();
    let Some(mut s) = TokenState::new(&graph, start) else {
        return;
    };
    assert_invariants(&graph, &s);
    let mut fired = false;
    let mut k = 0;
    while k < 2 {
        let before = s;
        let action = any_action();
        match step(&graph, &mut s, action) {
            Ok(()) => {
                if let Action::Drain { flow, .. } = action {
                    fired |= graph.is_join(graph.tgt[flow]) && s.ghost != before.ghost;
                }
            }
            Err(_) => assert!(s == before),
        }
        assert_invariants(&graph, &s);
        k += 1;
    }
    kani::cover!(fired, "a parallel join fires");
    kani::cover!(s.completed[1], "two instances complete");
}

/// The inclusive join (`join_decision` + `has_active_path`): split 0 feeds
/// join 1 over flows 0 and 1, the join feeds end 2. From any reachable
/// arrival pattern, one arbitrary drain keeps both invariants.
#[kani::proof]
#[kani::unwind(5)]
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
#[kani::unwind(5)]
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
