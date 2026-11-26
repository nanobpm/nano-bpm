//! Kani harness for the pure ack-outcome kernel (#2037). See
//! `formal/kani/README.md`.
use super::*;

/// Over every input: an accepted answer is consumed, a deferred one is
/// neither consumed nor accepted and only happens on a suspended instance,
/// and an absent subscription answers nothing. `cover!`s keep the accepted,
/// deferred and consumed-but-not-accepted branches non-vacuous.
#[kani::proof]
fn ack_outcome_kernel_is_consistent() {
    let facts = AckFacts {
        subscription_present: kani::any(),
        stays_open: kani::any(),
        message_recorded: kani::any(),
        correlated: kani::any(),
        opening: kani::any(),
        instance_suspended: kani::any(),
    };
    let o = ack_outcome(facts);
    assert!(!o.accepted || o.consumed);
    assert!(!(o.deferred && o.consumed));
    assert!(!(o.deferred && o.accepted));
    assert!(!o.deferred || facts.instance_suspended);
    assert!(
        facts.subscription_present
            || o == AckOutcome {
                consumed: false,
                accepted: false,
                deferred: false,
            }
    );
    kani::cover!(o.accepted);
    kani::cover!(o.deferred);
    kani::cover!(o.consumed && !o.accepted);
}
