//! Pins the pure join guard (#2010): the kernel against a declarative
//! reachability oracle (`ZeebeJoinReady`, as stated in
//! `formal/lean/Refinement/Zeebe.lean`), and the collector + kernel against the
//! string-keyed BFS the engine used before the refactor, so the refactor is
//! behaviour-preserving by test as well as by proof.

use std::collections::{BTreeSet, VecDeque};

use super::*;
use crate::engine::JoinGraph;
use crate::model::{ElementKind, IncomingFlow, ProcessBuilder, ProcessDefinition};
use crate::state::FlowArrivals;

/// A tiny deterministic generator (SplitMix64), so the corpus is reproducible
/// without a dev-dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// The oracle: `j` is reachable from `src` over open edges, by fixpoint
/// iteration over the whole node set (`ZeebePathReaches`).
fn reaches_oracle(succ: &[Vec<(usize, usize)>], src: usize, join: usize, taken: &[u32]) -> bool {
    let mut reached: BTreeSet<usize> = BTreeSet::from([src]);
    loop {
        let mut next = reached.clone();
        for &n in &reached {
            if n == join {
                continue;
            }
            for &(t, f) in succ.get(n).map(Vec::as_slice).unwrap_or(&[]) {
                let blocked = t == join && taken.get(f).is_some_and(|c| *c > 0);
                if !blocked {
                    next.insert(t);
                }
            }
        }
        if next == reached {
            return reached.contains(&join);
        }
        reached = next;
    }
}

/// `ZeebeJoinReady` over the dense view, written as the TLA+/Lean predicate.
fn zeebe_join_ready(
    succ: &[Vec<(usize, usize)>],
    kind: JoinKind,
    join: usize,
    incoming: &[usize],
    live: &[usize],
    taken: &[u32],
) -> bool {
    let tk = |f: usize| taken.get(f).copied().unwrap_or(0);
    let parallel_ready = incoming.iter().all(|&f| tk(f) >= 1);
    match kind {
        JoinKind::Parallel => parallel_ready,
        JoinKind::Inclusive => {
            let some_taken = incoming.iter().any(|&f| tk(f) > 0);
            let active = live
                .iter()
                .any(|&m| m != join && reaches_oracle(succ, m, join, taken));
            parallel_ready || (some_taken && !active)
        }
    }
}

fn random_dense(rng: &mut Rng) -> (Vec<Vec<(usize, usize)>>, Vec<u32>) {
    let nodes = 1 + rng.below(7);
    let mut succ = vec![Vec::new(); nodes];
    let mut flows = 0;
    for edges in &mut succ {
        for _ in 0..rng.below(4) {
            // Occasionally an out-of-range target (no successors) or a
            // non-flow edge.
            let target = rng.below(nodes + 1);
            let flow = if rng.below(4) == 0 {
                NO_FLOW
            } else {
                flows += 1;
                flows - 1
            };
            edges.push((target, flow));
        }
    }
    let taken = (0..flows + rng.below(2))
        .map(|_| rng.below(3) as u32)
        .collect();
    (succ, taken)
}

#[test]
fn join_decision_table() {
    use JoinDecision::*;
    use JoinKind::*;
    assert!(join_decision(Parallel, 2, 2) == Accept);
    assert!(join_decision(Parallel, 3, 2) == Accept);
    assert!(join_decision(Parallel, 1, 2) == Reject);
    assert!(join_decision(Parallel, 0, 2) == Reject);
    assert!(join_decision(Inclusive, 2, 2) == Accept);
    assert!(join_decision(Inclusive, 1, 2) == AcceptUnlessActivePath);
    assert!(join_decision(Inclusive, 0, 2) == Reject);
    assert!(join_decision(Inclusive, 0, 0) == Accept);
}

#[test]
fn path_reaches_join_matches_reachability_oracle() {
    let mut rng = Rng(0x2010);
    for _ in 0..20_000 {
        let (succ, taken) = random_dense(&mut rng);
        let join = rng.below(succ.len() + 1);
        let source = rng.below(succ.len() + 1);
        assert_eq!(
            path_reaches_join(&succ, source, join, &taken),
            reaches_oracle(&succ, source, join, &taken),
            "succ={succ:?} taken={taken:?} source={source} join={join}"
        );
    }
}

#[test]
fn join_ready_matches_zeebe_join_ready() {
    let mut rng = Rng(0x0201_0000);
    for _ in 0..20_000 {
        let (succ, taken) = random_dense(&mut rng);
        let join = rng.below(succ.len());
        let incoming: Vec<usize> = succ
            .iter()
            .flatten()
            .filter(|(t, f)| *t == join && *f != NO_FLOW)
            .map(|(_, f)| *f)
            .collect();
        let live: Vec<usize> = (0..rng.below(4))
            .map(|_| rng.below(succ.len() + 1))
            .collect();
        for kind in [JoinKind::Parallel, JoinKind::Inclusive] {
            assert_eq!(
                join_ready(&succ, kind, join, &incoming, &live, &taken, 0),
                zeebe_join_ready(&succ, kind, join, &incoming, &live, &taken),
                "kind={kind:?} succ={succ:?} taken={taken:?} live={live:?} join={join}",
                kind = kind == JoinKind::Parallel
            );
        }
    }
}

#[test]
fn unidentified_arrivals_count_towards_every_flow_taken() {
    // Two incoming flows, none taken, two unidentified arrivals: Nano's
    // `activate_join` accepts (outside the Zeebe parity claim, SPIKE.md).
    let succ = vec![vec![(2, 0)], vec![(2, 1)], vec![]];
    assert!(join_ready(
        &succ,
        JoinKind::Parallel,
        2,
        &[0, 1],
        &[],
        &[0, 0],
        2
    ));
    assert!(!join_ready(
        &succ,
        JoinKind::Parallel,
        2,
        &[0, 1],
        &[],
        &[0, 0],
        1
    ));
}

/// The engine's string-keyed BFS before #2010, kept verbatim as the
/// equivalence reference for the collector + kernel.
fn legacy_path_reaches_join(
    process: &ProcessDefinition,
    source: &str,
    join: &str,
    taken: &FlowArrivals,
) -> bool {
    let mut visited: BTreeSet<&str> = BTreeSet::new();
    let mut to_visit: VecDeque<&str> = VecDeque::from([source]);
    while let Some(node) = to_visit.pop_front() {
        if node == join {
            return true;
        }
        if !visited.insert(node) {
            continue;
        }
        let Some(element) = process.element(node) else {
            continue;
        };
        if let ElementKind::LinkIntermediateThrowEvent { link_name } = &element.kind {
            to_visit.extend(process.elements.values().filter_map(|other| {
                matches!(&other.kind, ElementKind::LinkIntermediateCatchEvent { link_name: name }
                    if name == link_name && other.parent == element.parent)
                .then_some(other.id.as_str())
            }));
            continue;
        }
        for (index, flow) in element.outgoing.iter().enumerate() {
            let taken_into_join = flow.to == join
                && process
                    .incoming_flow(node, index)
                    .is_some_and(|(_, incoming)| taken.count(&incoming) > 0);
            if !taken_into_join {
                to_visit.push_back(flow.to.as_str());
            }
        }
        to_visit.extend(process.elements.values().filter_map(|other| {
            other
                .kind
                .attached_to()
                .is_some_and(|host| host == node)
                .then_some(other.id.as_str())
        }));
    }
    false
}

/// A random process over tasks, inclusive gateways, link throws/catches and
/// boundary events, some inside a sub-process, with random flows (parallel
/// flows between the same two elements included).
fn random_process(rng: &mut Rng) -> Option<ProcessDefinition> {
    let count = 2 + rng.below(7);
    let ids: Vec<String> = (0..count).map(|i| format!("n{i}")).collect();
    let mut b = ProcessBuilder::new("p")
        .start_event("start")
        .sub_process("sp", "sp_start");
    b = b.start_event("sp_start").contained_in("sp_start", "sp");
    let mut tasks = Vec::new();
    for id in &ids {
        b = match rng.below(6) {
            0 | 1 => {
                tasks.push(id.clone());
                b.task(id.as_str())
            }
            2 => b.inclusive_gateway(id.as_str()),
            3 => b.link_intermediate_throw_event(id.as_str(), format!("L{}", rng.below(2))),
            4 => b.link_intermediate_catch_event(id.as_str(), format!("L{}", rng.below(2))),
            _ => b.parallel_gateway(id.as_str()),
        };
        if rng.below(3) == 0 {
            b = b.contained_in(id.as_str(), "sp");
        }
    }
    for (i, host) in tasks.iter().enumerate() {
        if rng.below(2) == 0 {
            let boundary = format!("b{i}");
            b = b.error_boundary_event(boundary.as_str(), host.as_str(), "E");
            let to = &ids[rng.below(ids.len())];
            b = b.connect(boundary.as_str(), to.as_str());
        }
    }
    b = b.connect("start", ids[0].as_str());
    for _ in 0..rng.below(2 * count + 1) {
        let from = &ids[rng.below(ids.len())];
        let to = &ids[rng.below(ids.len())];
        b = b.connect(from.as_str(), to.as_str());
    }
    b.build().ok()
}

#[test]
fn collector_and_kernel_match_the_legacy_bfs() {
    let mut rng = Rng(0x00C0_11EC);
    let mut checked = 0;
    let mut reached = 0;
    for _ in 0..20_000 {
        let Some(process) = random_process(&mut rng) else {
            continue;
        };
        let ids: Vec<&str> = process.elements.keys().map(String::as_str).collect();
        let join = ids[rng.below(ids.len())];
        // Random arrivals over the join's incoming flows.
        let mut arrivals = FlowArrivals::default();
        for element in process.elements.values() {
            for (index, flow) in element.outgoing.iter().enumerate() {
                if flow.to == join && rng.below(2) == 0 {
                    if let Some((_, incoming)) = process.incoming_flow(&element.id, index) {
                        arrivals.add(&incoming, 1 + rng.below(2));
                    }
                }
            }
        }
        // A stale arrival on a flow that does not exist must not matter.
        if rng.below(8) == 0 {
            arrivals.record(&IncomingFlow {
                from: "ghost".into(),
                ordinal: 0,
            });
        }
        let Some(graph) = JoinGraph::collect(&process, join, &arrivals) else {
            panic!("join {join} is an element");
        };
        for source in ids.iter().copied().chain(["unknown"]) {
            if source == join {
                continue;
            }
            let legacy = legacy_path_reaches_join(&process, source, join, &arrivals);
            let kernel = graph
                .index_of(source)
                .is_some_and(|s| has_active_path(&graph.succ, &[s], graph.join, &graph.taken));
            assert_eq!(
                legacy, kernel,
                "process={process:?} source={source} join={join}"
            );
            checked += 1;
            reached += usize::from(legacy);
        }
    }
    // Non-vacuity: both answers occur often.
    assert!(checked > 20_000, "checked {checked}");
    assert!(
        reached > checked / 20 && reached < checked * 19 / 20,
        "reached {reached}/{checked}"
    );
}
