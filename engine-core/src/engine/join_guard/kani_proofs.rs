//! Kani harness for the pure join guard kernel (#2010).
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! (run with `formal/kani/check.sh join_ready_kernel_total_and_exact`). The
//! kernel's full functional correctness — `join_ready` equals Zeebe's
//! `ZeebeJoinReady` for every graph — is the Lean proof in
//! `formal/lean/Refinement/Proof.lean`; this harness checks the properties the
//! Lean model abstracts away on the machine-integer code itself: no panic, no
//! overflow, and the `unidentified` arrivals (a Nano-only input) counted with
//! saturation.

use super::{distinct_taken, join_ready, JoinKind, NO_FLOW};

/// Nodes in the symbolic graph (one symbolic out-edge each).
const NODES: usize = 2;

/// One symbolic edge: a target up to one past the graph (exercising the
/// out-of-range guard) carrying one of the two flows or none.
fn any_edge() -> (usize, usize) {
    let target: usize = kani::any();
    let flow: usize = kani::any();
    kani::assume(target <= NODES);
    kani::assume(flow < 2 || flow == NO_FLOW);
    (target, flow)
}

#[kani::proof]
#[kani::unwind(4)]
fn join_ready_kernel_total_and_exact() {
    let succ: Vec<Vec<(usize, usize)>> = vec![vec![any_edge()], vec![any_edge()]];
    let taken: [u32; 2] = [kani::any(), kani::any()];
    let kind = if kani::any() {
        JoinKind::Parallel
    } else {
        JoinKind::Inclusive
    };
    let join: usize = kani::any();
    kani::assume(join < NODES);
    let incoming = [0usize, 1];
    let live: [usize; 1] = [kani::any()];
    kani::assume(live[0] <= NODES);
    let unidentified: usize = kani::any();

    let ready = join_ready(&succ, kind, join, &incoming, &live, &taken, unidentified);

    let arrived = distinct_taken(&incoming, &taken).saturating_add(unidentified);
    if arrived >= incoming.len() {
        assert!(ready, "every incoming flow accounted for must accept");
    }
    if arrived == 0 {
        assert!(!ready, "no arrival must never accept");
    }
    if kind == JoinKind::Parallel {
        assert_eq!(ready, arrived >= incoming.len());
    }
    kani::cover!(
        ready && arrived < incoming.len(),
        "an inclusive join accepts with a flow pending once no live path reaches it"
    );
    kani::cover!(!ready && arrived > 0, "a join with an arrival still waits");
}

/// #2024: once at least one but not every incoming flow of an inclusive join
/// is taken, the join fires iff no live token can still reach an untaken
/// incoming flow; with every flow taken it fires regardless of live tokens.
/// The reachability side is a closed-form oracle over a one-edge graph (node 0
/// → its edge; node 1 is the join, whose out-edges are never followed), so
/// CBMC runs one search — the two-search form ran out of memory.
#[kani::proof]
#[kani::unwind(4)]
fn inclusive_join_ready_iff_no_active_path() {
    let edge = any_edge();
    let succ: Vec<Vec<(usize, usize)>> = vec![vec![edge], Vec::new()];
    let taken: [u32; 2] = [kani::any(), kani::any()];
    let join: usize = 1;
    let incoming = [0usize, 1];
    let live: [usize; 1] = [kani::any()];
    kani::assume(live[0] <= NODES);

    let ready = join_ready(
        &succ,
        JoinKind::Inclusive,
        join,
        &incoming,
        &live,
        &taken,
        0,
    );
    let arrived = distinct_taken(&incoming, &taken);
    let (target, flow) = edge;
    let untaken = flow >= taken.len() || taken[flow] == 0;
    let active = live[0] == 0 && target == join && untaken;
    if arrived > 0 && arrived < incoming.len() {
        assert_eq!(
            ready, !active,
            "partial inclusive join fires iff no live path"
        );
    } else {
        assert_eq!(ready, arrived >= incoming.len());
    }
    kani::cover!(
        arrived > 0 && arrived < incoming.len() && active && !ready,
        "a live path holds a partially-taken inclusive join"
    );
    kani::cover!(
        arrived > 0 && arrived < incoming.len() && live[0] == 0 && !active && ready,
        "a live token that cannot reach an untaken flow does not hold the join"
    );
}
