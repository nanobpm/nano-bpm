//! Kani proof harnesses for the state kernel (#1960): join token bookkeeping
//! ([`FlowArrivals`]) and the job index maintainer.
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! for what is proven, the bounds and how to run them.

use super::*;
use crate::model::IncomingFlow;

/// Bound on the number of nondeterministic operations per harness (the
/// parallel-join harness uses its own [`JOIN_STEPS`]).
const STEPS: usize = 2;

/// Arrivals in the parallel-join harness: three, so a firing can leave a
/// surplus token behind (`A, A, B` fires once and keeps one `A`).
const JOIN_STEPS: usize = 3;

/// Parallel flows from the same source (`ParallelDuplicateFlows`-style), told
/// apart only by their ordinal — the case the flow ordering must not collapse.
fn flow(ordinal: usize) -> IncomingFlow {
    IncomingFlow {
        from: "a".to_string(),
        ordinal,
    }
}

/// `TokenFlow.tla` join invariants restated over the Rust bookkeeping: every
/// stored count is positive (a join token count is never negative, and a zero
/// count is never kept), and the entries are strictly sorted by flow (one
/// entry per flow, deterministic serialized form).
fn assert_join_invariant(arrivals: &FlowArrivals) {
    let mut prev: Option<&IncomingFlow> = None;
    for (flow, count) in arrivals.iter() {
        assert!(count > 0, "a stored join token count is positive");
        if let Some(p) = prev {
            assert!(p < flow, "join arrivals are strictly sorted by flow");
        }
        prev = Some(flow);
    }
}

/// Any interleaving of arrivals (`record`/`add`) and join firings
/// (`consume_one_each`) preserves the join invariant, never underflows or
/// overflows, and conserves tokens: a firing consumes exactly one token per
/// distinct flow (Zeebe's "Tetris principle") and an arrival adds exactly its
/// tokens to exactly its flow.
#[kani::proof]
#[kani::unwind(3)]
#[kani::solver(cadical)]
fn flow_arrivals_preserve_join_invariant() {
    // Both identities are built once: re-allocating a symbolic flow per step
    // (and summing the whole map per assertion) pushed the solver past the CI
    // runner's memory. A ghost count per identity is the oracle instead.
    let (f0, f1) = (flow(0), flow(1));
    let (mut g0, mut g1) = (0usize, 0usize);
    // Seed both flows with 1..=2 tokens so a firing is reachable on the very
    // first step, with or without surplus; with an empty start, STEPS = 2
    // operations could never populate both flows *and* fire.
    let s0 = 1 + usize::from(kani::any::<u8>() % 2);
    let s1 = 1 + usize::from(kani::any::<u8>() % 2);
    let mut arrivals = FlowArrivals::default();
    arrivals.add(&f0, s0);
    arrivals.add(&f1, s1);
    g0 += s0;
    g1 += s1;
    for _ in 0..STEPS {
        // Branch to a concrete flow and ghost: a symbolically indexed flow
        // reference made the solver alias every allocation (runner OOM).
        let first = kani::any::<bool>();
        match kani::any::<u8>() % 3 {
            0 => {
                if first {
                    arrivals.record(&f0);
                    g0 += 1;
                } else {
                    arrivals.record(&f1);
                    g1 += 1;
                }
            }
            1 => {
                let tokens = usize::from(kani::any::<u8>() % 3);
                if first {
                    arrivals.add(&f0, tokens);
                    g0 += tokens;
                } else {
                    arrivals.add(&f1, tokens);
                    g1 += tokens;
                }
            }
            _ => {
                // The engine fires a join only once every incoming flow holds a
                // token (`Engine::activate_join`), so a firing always sees both
                // flows. (Firing over a symbolic-length entry list made the
                // solver exhaust the CI runner's memory.)
                kani::assume(arrivals.distinct_flows() == 2);
                arrivals.consume_one_each();
                g0 -= 1;
                g1 -= 1;
                kani::cover!(g0 + g1 > 0, "a firing keeps surplus tokens");
                kani::cover!(g0 + g1 == 0, "a firing drains both flows");
            }
        }
        assert!(arrivals.count(&f0) == g0);
        assert!(arrivals.count(&f1) == g1);
        assert!(arrivals.distinct_flows() == usize::from(g0 > 0) + usize::from(g1 > 0));
        assert_join_invariant(&arrivals);
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

/// Moves job `KEY` to any state, with or without a retry backoff, and re-syncs
/// its indexes through the single canonical maintainer.
fn any_job_transition(state: &mut State) {
    if let Some(job) = state.jobs.get_mut(&KEY) {
        job.state = any_job_state();
        job.retry_backoff_until = kani::any::<bool>().then(kani::any);
    }
    types::resync_job_index(state, KEY);
}

/// The job index maintainer [`resync_job_index`], which every job applier arm
/// calls after changing a job's state, keeps the derived indexes in lockstep
/// with the job across any two consecutive state changes (any pair of the
/// six job states, with or without a retry backoff, at any priority): only a
/// `Created`, non-deferred job is activatable, exactly a deferred one sits in
/// both deferred indexes at its backoff deadline, and exactly an `Activated`
/// one is in `activated_jobs`. Driving `state::apply` (even one concrete
/// `JobCreated`) did not fit the CI box, so the harness builds the job directly
/// and proves the transition every applier arm funnels through.
#[kani::proof]
#[kani::unwind(3)]
#[kani::solver(cadical)]
fn job_index_resync_keeps_lockstep() {
    let mut state = State::default();
    let priority: i32 = kani::any();
    state.jobs.insert(
        KEY,
        Job {
            key: KEY,
            instance_key: 1,
            element_instance_key: 2,
            element_id: "A".to_string(),
            job_type: "t".to_string(),
            state: JobState::Created,
            worker: None,
            deadline: None,
            activated_at: None,
            activation_timeout: None,
            lease_token: None,
            durable_activation: false,
            activated: false,
            retries: 3,
            priority,
            created_at: 0,
            kind: JobKind::BpmnElement,
            error_message: None,
            error_code: None,
            has_failed_with_retries_left: false,
            retry_backoff_until: None,
            settled_tokens: Vec::new(),
            terminal_end: None,
            secret_provenance: Vec::new(),
        },
    );
    let order = types::activation_order(priority, KEY);
    for _ in 0..STEPS {
        any_job_transition(&mut state);
        assert_job_indexes(&state, order);
    }
}

/// The job-index lockstep invariant for job `KEY`.
fn assert_job_indexes(state: &State, order: (i64, Key)) {
    let Some(job) = state.jobs.get(&KEY) else {
        panic!("a state change never drops the job");
    };
    let activated = job.state == JobState::Activated;
    assert!(activated == state.activated_jobs.contains(&KEY));
    let deferred = job.state == JobState::Created && job.retry_backoff_until.is_some();
    let activatable = state
        .activatable_jobs
        .get("t")
        .is_some_and(|set| set.contains(&order));
    assert!(activatable == (job.state == JobState::Created && !deferred));
    assert!(state.activatable_jobs.len() == usize::from(activatable));
    let until = job.retry_backoff_until.unwrap_or(0);
    assert!(deferred == (state.deferred_job_deadlines.get(&KEY) == Some(&until)));
    assert!(deferred == state.deferred_jobs.contains(&(until, KEY)));
    assert!(state.deferred_jobs.len() == usize::from(deferred));
}

/// The parallel-diamond join (`P2` with incoming flows from `A` and `B`), as the
/// pure guard `Engine::activate_join` applies to the Rust bookkeeping: tokens
/// arrive over either flow in any order, and the join fires exactly when every
/// incoming flow has been taken (`distinct_flows() >= incoming`). Firing
/// consumes one token per flow and keeps the surplus (Tetris principle), so the
/// join never fires more often than the scarcer flow delivered, and never
/// before both flows delivered once.
#[kani::proof]
#[kani::unwind(4)]
#[kani::solver(cadical)]
fn parallel_join_fires_only_when_every_flow_taken() {
    const INCOMING: usize = 2;
    let a = IncomingFlow {
        from: "A".to_string(),
        ordinal: 0,
    };
    let b = IncomingFlow {
        from: "B".to_string(),
        ordinal: 0,
    };
    let mut arrivals = FlowArrivals::default();
    let (mut from_a, mut from_b, mut fired) = (0usize, 0usize, 0usize);
    for _ in 0..JOIN_STEPS {
        if kani::any() {
            arrivals.record(&a);
            from_a += 1;
        } else {
            arrivals.record(&b);
            from_b += 1;
        }
        if arrivals.distinct_flows() >= INCOMING {
            arrivals.consume_one_each();
            fired += 1;
            kani::cover!(
                !arrivals.is_empty(),
                "the join fires with a surplus token left behind"
            );
        }
        assert!(fired == from_a.min(from_b), "fires once per full round");
        assert!(arrivals.count(&a) == from_a - fired);
        assert!(arrivals.count(&b) == from_b - fired);
        assert!(arrivals.distinct_flows() < INCOMING, "a ready join never waits");
        assert_join_invariant(&arrivals);
    }
}
