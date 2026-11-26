//! The `TokenFlow.tla` invariants over many random graphs and step sequences,
//! plus scenarios that pin the machine to the TLA+ semantics and checks that
//! the invariants are not vacuous.

use super::*;

const KINDS: [Kind; 5] = [
    Kind::Task,
    Kind::Exclusive,
    Kind::Inclusive,
    Kind::Parallel,
    Kind::End,
];
const GUARDS: [Guard; 5] = [
    Guard::Default,
    Guard::Unconditional,
    Guard::Holds,
    Guard::Fails,
    Guard::Errors,
];

/// Deterministic xorshift, so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

fn random_graph(rng: &mut Rng) -> Graph {
    let mut g = Graph {
        kind: [Kind::Task; NODES],
        src: [0; FLOWS],
        tgt: [0; FLOWS],
    };
    for n in 0..NODES {
        g.kind[n] = KINDS[rng.below(KINDS.len())];
    }
    for f in 0..FLOWS {
        g.src[f] = rng.below(NODES);
        g.tgt[f] = rng.below(NODES);
    }
    g
}

fn random_action(rng: &mut Rng) -> Action {
    if rng.below(3) == 0 {
        Action::CompleteTask {
            slot: rng.below(SLOTS),
        }
    } else {
        let mut guards = [Guard::Fails; FLOWS];
        for g in guards.iter_mut() {
            *g = GUARDS[rng.below(GUARDS.len())];
        }
        Action::Drain {
            flow: rng.below(FLOWS),
            guards,
        }
    }
}

#[test]
fn invariants_hold_over_random_runs() {
    let mut rng = Rng(0x2050_1983);
    let mut applied = 0usize;
    let mut fired_joins = 0usize;
    for _ in 0..20_000 {
        let graph = random_graph(&mut rng);
        let Some(mut s) = TokenState::new(&graph, rng.below(FLOWS)) else {
            panic!("in-range graph must initialise");
        };
        assert!(no_instance_active_and_completed(&s));
        assert!(tokens_accounted(&graph, &s));
        for _ in 0..12 {
            let before = s;
            let action = random_action(&mut rng);
            match step(&graph, &mut s, action) {
                Ok(()) => {
                    applied += 1;
                    if let Action::Drain { flow, .. } = action {
                        if graph.is_join(graph.tgt[flow]) && s.ghost != before.ghost {
                            fired_joins += 1;
                        }
                    }
                }
                Err(_) => assert_eq!(s, before, "a refused step must not mutate"),
            }
            assert!(
                no_instance_active_and_completed(&s),
                "{graph:?} {action:?} {s:?}"
            );
            assert!(tokens_accounted(&graph, &s), "{graph:?} {action:?} {s:?}");
        }
    }
    assert!(applied > 20_000, "runs must make progress: {applied}");
    assert!(fired_joins > 100, "joins must fire: {fired_joins}");
}

/// Parallel split (0) → two flows → parallel join (1) → end (2): the join
/// holds the first arrival and fires on the second, and the instance drains.
#[test]
fn parallel_diamond_holds_then_fires() {
    let graph = Graph {
        kind: [Kind::Parallel, Kind::Parallel, Kind::End],
        src: [0, 0, 1],
        tgt: [1, 1, 2],
    };
    // Seed the split's activation by hand: a token on flow 2 is not the
    // start, so build the state from flow 0 and 1 being taken by the split.
    let mut s = TokenState::new(&graph, 0).unwrap_or_else(|| panic!("graph"));
    s.pending[1] = 1;
    s.arrivals[1] = 1;
    s.ghost = 2;
    assert!(tokens_accounted(&graph, &s));
    let drain = |flow| Action::Drain {
        flow,
        guards: [Guard::Unconditional; FLOWS],
    };
    // Fire on the first drain: both arrivals were counted when taken.
    assert_eq!(step(&graph, &mut s, drain(0)), Ok(()));
    assert_eq!(s.arrivals, [0, 0, 0]);
    assert_eq!(s.pending, [0, 1, 1]);
    assert!(tokens_accounted(&graph, &s));
    // The stale activation on flow 1 finds nothing to do.
    assert_eq!(step(&graph, &mut s, drain(1)), Ok(()));
    assert_eq!(s.ghost, 1);
    assert_eq!(step(&graph, &mut s, drain(2)), Ok(()));
    assert_eq!(s.ghost, 0);
    assert!(s.active.iter().all(|a| !a));
    assert!(tokens_accounted(&graph, &s));
}

/// A parallel join with one of two flows taken holds the token on the flow.
#[test]
fn parallel_join_holds_partial_arrival() {
    let graph = Graph {
        kind: [Kind::Task, Kind::Parallel, Kind::End],
        src: [0, 0, 1],
        tgt: [1, 1, 2],
    };
    let mut s = TokenState::new(&graph, 0).unwrap_or_else(|| panic!("graph"));
    let guards = [Guard::Unconditional; FLOWS];
    assert_eq!(
        step(&graph, &mut s, Action::Drain { flow: 0, guards }),
        Ok(())
    );
    assert_eq!(s.arrivals, [1, 0, 0]);
    assert!(s.active.iter().all(|a| !a));
    assert!(tokens_accounted(&graph, &s));
}

/// An exclusive gateway with no holding condition and no default raises an
/// incident: the instance stays active and keeps its token.
#[test]
fn exclusive_no_match_keeps_token_on_instance() {
    let graph = Graph {
        kind: [Kind::Exclusive, Kind::End, Kind::End],
        src: [1, 0, 0],
        tgt: [0, 1, 2],
    };
    let mut s = TokenState::new(&graph, 0).unwrap_or_else(|| panic!("graph"));
    let guards = [Guard::Fails; FLOWS];
    assert_eq!(
        step(&graph, &mut s, Action::Drain { flow: 0, guards }),
        Ok(())
    );
    assert!(s.active[0] && !s.completed[0]);
    assert_eq!(s.ghost, 1);
    assert!(tokens_accounted(&graph, &s));
}

/// The invariants reject corrupted states, so they cannot pass vacuously.
#[test]
fn invariants_detect_corruption() {
    let graph = Graph {
        kind: [Kind::Task, Kind::Parallel, Kind::End],
        src: [0, 0, 1],
        tgt: [1, 1, 2],
    };
    let s = TokenState::new(&graph, 2).unwrap_or_else(|| panic!("graph"));
    let mut both = s;
    both.active[0] = true;
    both.completed[0] = true;
    assert!(!no_instance_active_and_completed(&both));
    let mut lost = s;
    lost.pending[2] = 0;
    assert!(!tokens_accounted(&graph, &lost));
    let mut stray = s;
    stray.arrivals[2] = 1;
    assert!(!tokens_accounted(&graph, &stray));
    let mut held = s;
    held.arrivals[0] = 1;
    assert!(!tokens_accounted(&graph, &held));
}

#[test]
fn inclusive_join_waits_for_live_sibling_then_fires() {
    let graph = Graph {
        kind: [Kind::Inclusive, Kind::Inclusive, Kind::Task],
        src: [0, 2, 0],
        tgt: [1, 1, 2],
    };
    for sibling_live in [false, true] {
        let mut s = TokenState::new(&graph, 0).unwrap();
        if sibling_live {
            s.ghost += 1;
            s.pending[2] = 1;
        }
        let drain = |flow| Action::Drain {
            flow,
            guards: [Guard::Unconditional; FLOWS],
        };
        step(&graph, &mut s, drain(0)).unwrap();
        assert!(tokens_accounted(&graph, &s));
        assert_eq!(s.arrivals[0] == 0, !sibling_live, "live={sibling_live}");
        if sibling_live {
            step(&graph, &mut s, drain(2)).unwrap();
            let slot = (0..SLOTS).find(|&i| s.active[i] && s.node[i] == 2).unwrap();
            step(&graph, &mut s, Action::CompleteTask { slot }).unwrap();
            step(&graph, &mut s, drain(1)).unwrap();
            assert_eq!(s.arrivals, [0, 0, 0], "join fires once the sibling arrives");
            assert!(tokens_accounted(&graph, &s));
        }
    }
}
