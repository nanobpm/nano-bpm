//! Kani proof harnesses for the state kernel (#1960): join token bookkeeping
//! ([`FlowArrivals`]) and the job lease/complete applier.
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! for what is proven, the bounds and how to run them.

use super::*;
use crate::event::Event;
use crate::model::IncomingFlow;

/// Bound on the number of nondeterministic operations per harness.
const STEPS: usize = 2;

/// One of three incoming-flow identities: two parallel flows from the same
/// source (`ParallelDuplicateFlows`-style, distinct ordinals) and one from a
/// different source.
fn any_flow() -> IncomingFlow {
    match kani::any::<u8>() % 3 {
        0 => IncomingFlow {
            from: "a".to_string(),
            ordinal: 0,
        },
        1 => IncomingFlow {
            from: "a".to_string(),
            ordinal: 1,
        },
        _ => IncomingFlow {
            from: "b".to_string(),
            ordinal: 0,
        },
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

fn total(arrivals: &FlowArrivals) -> usize {
    arrivals.iter().map(|(_, c)| c).sum()
}

/// Any interleaving of arrivals (`record`/`add`) and join firings
/// (`consume_one_each`) preserves the join invariant, never underflows or
/// overflows, and conserves tokens: a firing consumes exactly one token per
/// distinct flow (Zeebe's "Tetris principle") and an arrival adds exactly its
/// tokens to exactly its flow.
#[kani::proof]
#[kani::unwind(3)]
fn flow_arrivals_preserve_join_invariant() {
    let mut arrivals = FlowArrivals::default();
    for _ in 0..STEPS {
        let before = total(&arrivals);
        match kani::any::<u8>() % 3 {
            0 => {
                let flow = any_flow();
                let had = arrivals.count(&flow);
                arrivals.record(&flow);
                assert!(arrivals.count(&flow) == had + 1);
                assert!(total(&arrivals) == before + 1);
            }
            1 => {
                let flow = any_flow();
                let tokens: usize = kani::any();
                kani::assume(tokens <= 4);
                let had = arrivals.count(&flow);
                arrivals.add(&flow, tokens);
                assert!(arrivals.count(&flow) == had + tokens);
                assert!(total(&arrivals) == before + tokens);
            }
            _ => {
                let distinct = arrivals.distinct_flows();
                arrivals.consume_one_each();
                assert!(total(&arrivals) == before - distinct);
            }
        }
        assert_join_invariant(&arrivals);
        assert!(arrivals.is_empty() == (arrivals.distinct_flows() == 0));
    }
}

/// A nondeterministic job-family event for job `KEY`.
fn any_job_event() -> Event {
    const KEY: Key = 7;
    match kani::any::<u8>() % 5 {
        0 => Event::JobActivated {
            job_key: KEY,
            instance_key: 1,
            durable: kani::any(),
            worker: if kani::any() {
                "w".to_string()
            } else {
                String::new()
            },
            deadline: kani::any(),
            activated_at: if kani::any() { Some(kani::any()) } else { None },
            fetch_variables: Vec::new(),
            lease_token: if kani::any() {
                Some("t".to_string())
            } else {
                None
            },
        },
        1 => Event::JobLockExpired {
            job_key: KEY,
            instance_key: 1,
        },
        2 => Event::JobCompleted {
            job_key: KEY,
            instance_key: 1,
            created_at: 0,
            job_type: "t".to_string(),
            worker: None,
            completed_at: kani::any(),
            deadline: if kani::any() { Some(kani::any()) } else { None },
        },
        3 => Event::JobFailed {
            job_key: KEY,
            instance_key: 1,
            retries: kani::any(),
            worker: None,
            error_message: None,
            retry_backoff_until: if kani::any() { Some(kani::any()) } else { None },
        },
        _ => Event::JobRetriesUpdated {
            job_key: KEY,
            instance_key: 1,
            retries: kani::any(),
            operation_reference: None,
        },
    }
}

/// The job lease/complete lifecycle, driven through the real state applier
/// ([`apply`]) by any sequence of job events with arbitrary deadlines,
/// timestamps and retries: no panic or arithmetic overflow, and the derived
/// job indexes stay in lockstep with the job's state — an `Activated` job holds
/// a lock (`deadline`) and is in `activated_jobs`; only a `Created`,
/// non-deferred job is activatable; a frozen activation timeout never exceeds
/// its deadline; a `Completed` job holds no lock.
#[kani::proof]
#[kani::unwind(3)]
fn job_lease_lifecycle_keeps_indexes_in_lockstep() {
    const KEY: Key = 7;
    let mut state = State::default();
    let priority: i32 = kani::any();
    apply(
        &mut state,
        &Event::JobCreated {
            job_key: KEY,
            instance_key: 1,
            element_instance_key: 2,
            element_id: "A".to_string(),
            job_type: "t".to_string(),
            created_at: 0,
            priority,
            retries: kani::any(),
            secret_provenance: Vec::new(),
        },
    );
    let order = types::activation_order(priority, KEY);
    for _ in 0..STEPS {
        apply(&mut state, &any_job_event());
        let Some(job) = state.jobs.get(&KEY) else {
            panic!("the job is never dropped by a job-family event");
        };
        let activated = job.state == JobState::Activated;
        assert!(activated == state.activated_jobs.contains(&KEY));
        if activated {
            assert!(job.deadline.is_some(), "an activated job holds a lock");
        }
        if let (Some(timeout), Some(deadline)) = (job.activation_timeout, job.deadline) {
            assert!(timeout <= deadline);
        }
        let deferred = job.state == JobState::Created && job.retry_backoff_until.is_some();
        let activatable = state
            .activatable_jobs
            .get("t")
            .is_some_and(|set| set.contains(&order));
        assert!(activatable == (job.state == JobState::Created && !deferred));
        assert!(deferred == state.deferred_job_deadlines.contains_key(&KEY));
        if job.state == JobState::Completed {
            assert!(job.deadline.is_none() && job.worker.is_none());
        }
    }
}

/// The parallel-diamond join (`P2` with incoming flows from `A` and `B`), as the
/// pure guard `Engine::activate_join` applies to the Rust bookkeeping: tokens
/// arrive over either flow in any order, and the join fires exactly when every
/// incoming flow has been taken (`distinct_flows() >= incoming`). Firing
/// consumes one token per flow and keeps the surplus (Tetris principle), so the
/// join never fires more often than the scarcer flow delivered, and never
/// before both flows delivered once.
#[kani::proof]
#[kani::unwind(3)]
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
    for _ in 0..STEPS {
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
        }
        assert!(fired == from_a.min(from_b), "fires once per full round");
        assert!(arrivals.count(&a) == from_a - fired);
        assert!(arrivals.count(&b) == from_b - fired);
        assert!(arrivals.distinct_flows() < INCOMING, "a ready join never waits");
        assert_join_invariant(&arrivals);
    }
}
