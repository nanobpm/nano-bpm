//! Kani proof harnesses for the state kernel (#1960): join token bookkeeping
//! ([`FlowArrivals`]) and the job index maintainer. They prove over
//! the bounded stand-ins in `kani_model` (#1994).
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! for what is proven, the bounds and how to run them.

use super::kani_model::{assert_job_index_lockstep, BoundedArrivals, JobIndexes};
use super::*;

/// Bound on the number of nondeterministic operations per harness.
const STEPS: usize = 2;

/// The join invariant over the stand-in: every stored count is positive and
/// the entries are strictly sorted by flow.
fn assert_bounded_join_invariant<const N: usize>(arrivals: &BoundedArrivals<N>) {
    let entries = arrivals.as_slice();
    for i in 0..entries.len() {
        assert!(entries[i].1 > 0, "a stored join token count is positive");
        if i > 0 {
            assert!(entries[i - 1].0 < entries[i].0, "strictly sorted by flow");
        }
    }
}

/// Any interleaving of arrivals (`record`/`add`) and join firings
/// (`consume_one_each`) preserves the join invariant, never underflows or
/// overflows, and conserves tokens: a firing consumes exactly one token per
/// distinct flow (Zeebe's "Tetris principle") and an arrival adds exactly its
/// tokens to exactly its flow.
///
/// Proven over [`BoundedArrivals`], the fixed-array stand-in for
/// [`FlowArrivals`] (#1994): the heap-backed `Vec<(IncomingFlow, usize)>` ran
/// the CI runner out of memory. `kani_model::refinement` checks the stand-in
/// agrees with `FlowArrivals` on every operation sequence up to a bound.
#[kani::proof]
#[kani::unwind(4)]
#[kani::solver(cadical)]
fn flow_arrivals_preserve_join_invariant() {
    // FLOWS = 2 with 3 steps lets two arrivals fill every flow before a
    // third step fires the join; the `cover!` below fails the proof if the
    // firing branch ever becomes unreachable (vacuous) again.
    const FLOWS: u8 = 2;
    const JOIN_STEPS: usize = 3;
    let mut ghost = [0usize; FLOWS as usize];
    let mut arrivals = BoundedArrivals::<{ FLOWS as usize }>::default();
    for _ in 0..JOIN_STEPS {
        let f: u8 = kani::any();
        kani::assume(f < FLOWS);
        match kani::any::<u8>() % 3 {
            0 => {
                arrivals.record(f);
                ghost[usize::from(f)] += 1;
            }
            1 => {
                let tokens = usize::from(kani::any::<u8>() % 3);
                arrivals.add(f, tokens);
                ghost[usize::from(f)] += tokens;
            }
            _ => {
                // The engine fires a join only once every incoming flow holds a
                // token (`Engine::activate_join`).
                kani::assume(arrivals.distinct_flows() == usize::from(FLOWS));
                arrivals.consume_one_each();
                kani::cover!(true, "join firing branch is reachable");
                for g in &mut ghost {
                    *g -= 1;
                }
            }
        }
        let mut distinct = 0;
        for g in 0..FLOWS {
            assert!(arrivals.count(g) == ghost[usize::from(g)]);
            distinct += usize::from(ghost[usize::from(g)] > 0);
        }
        assert!(arrivals.distinct_flows() == distinct);
        assert_bounded_join_invariant(&arrivals);
        assert!(arrivals.is_empty() == (arrivals.distinct_flows() == 0));
    }
}

/// The job whose indexes the lockstep harness tracks.
const KEY: Key = 7;

/// Any job state.
fn any_job_state() -> JobState {
    match kani::any::<u8>() % 6 {
        0 => JobState::Created,
        1 => JobState::Activated,
        2 => JobState::Failed,
        3 => JobState::Errored,
        4 => JobState::Canceled,
        _ => JobState::Completed,
    }
}

/// The job index maintainer [`types::resync_job_index`], which every job
/// applier arm calls after changing a job's state, keeps the derived indexes in
/// lockstep with the job across any consecutive state changes (any of the six
/// job states, with or without a retry backoff, at any priority): only a
/// `Created`, non-deferred job is activatable, exactly a deferred one sits in
/// both deferred indexes at its backoff deadline, and exactly an `Activated`
/// one is in `activated_jobs`.
///
/// Proven over [`JobIndexes`], the bounded stand-in for the `BTreeMap`/
/// `BTreeSet` indexes (#1994): CBMC was still building the production formula
/// after 7 minutes. The membership decision is shared with production
/// ([`types::job_index_membership`]); `kani_model::refinement` checks the
/// stand-in agrees with `resync_job_index` on every transition sequence up to
/// a bound.
#[kani::proof]
#[kani::unwind(4)]
#[kani::solver(cadical)]
fn job_index_resync_keeps_lockstep() {
    let priority: i32 = kani::any();
    let order = types::activation_order(priority, KEY);
    let mut indexes = JobIndexes::default();
    indexes.resync(KEY, priority, JobState::Created, None);
    for _ in 0..STEPS {
        let job_state = any_job_state();
        let backoff = kani::any::<bool>().then(kani::any::<u64>);
        indexes.resync(KEY, priority, job_state, backoff);
        assert_job_index_lockstep(&indexes, KEY, order, job_state, backoff);
    }
}

/// The parallel-diamond join (`P2` with incoming flows from `A` and `B`), as the
/// pure guard `Engine::activate_join` applies to the Rust bookkeeping: tokens
/// arrive over either flow in any order, and the join fires exactly when every
/// incoming flow has been taken (`distinct_flows() >= incoming`). Firing
/// consumes one token per flow and keeps the surplus (Tetris principle), so the
/// join never fires more often than the scarcer flow delivered, and never
/// before both flows delivered once.
///
/// Proven over [`BoundedArrivals`] (#1994): over the heap-backed
/// `FlowArrivals` with `String` flow ids this harness ran within seconds of the
/// CI box and was killed during SSA conversion on a hosted runner.
/// `kani_model::refinement` checks the stand-in agrees with `FlowArrivals`.
#[kani::proof]
#[kani::unwind(4)]
#[kani::solver(cadical)]
fn parallel_join_fires_only_when_every_flow_taken() {
    const INCOMING: usize = 2;
    const ARRIVALS: usize = 3;
    let (a, b) = (0u8, 1u8);
    let mut arrivals = BoundedArrivals::<INCOMING>::default();
    let (mut from_a, mut from_b, mut fired) = (0usize, 0usize, 0usize);
    for _ in 0..ARRIVALS {
        if kani::any() {
            arrivals.record(a);
            from_a += 1;
        } else {
            arrivals.record(b);
            from_b += 1;
        }
        if arrivals.distinct_flows() >= INCOMING {
            arrivals.consume_one_each();
            kani::cover!(true, "join fires");
            fired += 1;
        }
        assert!(fired == from_a.min(from_b), "fires once per full round");
        assert!(arrivals.count(a) == from_a - fired);
        assert!(arrivals.count(b) == from_b - fired);
        assert!(
            arrivals.distinct_flows() < INCOMING,
            "a ready join never waits"
        );
        assert_bounded_join_invariant(&arrivals);
    }
}
