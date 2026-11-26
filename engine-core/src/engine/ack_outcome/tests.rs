//! Equivalence of the extracted kernel with the pre-#2037 inline derivation
//! of `Engine::emit_correlation_ack`, exhaustively over every input.
use super::*;

/// The pre-extraction derivation, transcribed verbatim from
/// `emit_correlation_ack` (#1949) over the same facts: `subscription` is the
/// `Option` the engine read, the subscription's fields its components.
fn pre_extraction(f: AckFacts) -> AckOutcome {
    struct Sub {
        stays_open: bool,
        recorded: bool,
        correlated: bool,
        opening: bool,
    }
    let subscription = f.subscription_present.then_some(Sub {
        stays_open: f.stays_open,
        recorded: f.message_recorded,
        correlated: f.correlated,
        opening: f.opening,
    });
    let consumed = subscription.as_ref().is_some_and(|s| {
        if s.stays_open {
            s.recorded
        } else {
            s.correlated
        }
    });
    let waiting = subscription.as_ref().is_some_and(|s| s.opening);
    let accepted = consumed && waiting;
    let deferred = !consumed && waiting && f.instance_suspended;
    AckOutcome {
        consumed,
        accepted,
        deferred,
    }
}

fn all_facts() -> Vec<AckFacts> {
    (0u32..64)
        .map(|bits| {
            let b = |i: u32| bits & (1 << i) != 0;
            AckFacts {
                subscription_present: b(0),
                stays_open: b(1),
                message_recorded: b(2),
                correlated: b(3),
                opening: b(4),
                instance_suspended: b(5),
            }
        })
        .collect()
}

#[test]
fn kernel_matches_the_pre_extraction_derivation_exhaustively() {
    for facts in all_facts() {
        assert_eq!(ack_outcome(facts), pre_extraction(facts), "{facts:?}");
    }
}

#[test]
fn outcome_invariants_hold_exhaustively() {
    for facts in all_facts() {
        let o = ack_outcome(facts);
        assert!(!o.accepted || o.consumed, "{facts:?}");
        assert!(!(o.deferred && o.consumed), "{facts:?}");
        assert!(!(o.deferred && o.accepted), "{facts:?}");
        if o.deferred {
            assert!(facts.instance_suspended, "{facts:?}");
        }
    }
}
