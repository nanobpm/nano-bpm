//! Pure decision kernel for an event-based gateway trigger (#1963 / #2004).
//!
//! An event-based gateway rests in `ACTIVATED` holding one subscription per
//! outgoing catch target, all owned by the gateway's element instance and
//! tagged with the target catch element id (Zeebe
//! `EventBasedGatewayProcessor.onActivate`). When one of them fires, the
//! engine asks this kernel whether to accept the trigger. Zeebe records the
//! trigger on the gateway's event scope with `interrupting` semantics: the
//! first trigger wins and any later one is rejected
//! (`EventScopeInstanceState`). The engine gathers the inputs: whether the
//! gateway still holds its subscriptions, whether a trigger is already
//! recorded, and the index of the firing catch among the gateway's
//! subscription targets. The kernel takes no `Engine` or `State`.

/// What the engine does with a fired gateway-owned subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatewayTrigger {
    /// The first trigger: record it, delete the other subscriptions, complete
    /// the gateway and activate the triggered catch directly.
    Accept,
    /// A later trigger, or one for a catch that is not a subscription target:
    /// drop it without touching the gateway.
    Reject,
}

/// Decides a gateway trigger. `targets` is the number of catch targets the
/// gateway holds subscriptions for (0 once a trigger deleted them), `catch`
/// the firing catch's index among them (`None` when it is not one), and
/// `already_triggered` whether the gateway already recorded a trigger.
pub(crate) fn decide(
    targets: usize,
    catch: Option<usize>,
    already_triggered: bool,
) -> GatewayTrigger {
    if already_triggered {
        return GatewayTrigger::Reject;
    }
    match catch {
        Some(index) if index < targets => GatewayTrigger::Accept,
        _ => GatewayTrigger::Reject,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exhaustive over a small bound: accept exactly the first trigger for a
    /// real target.
    #[test]
    fn accepts_only_first_trigger_of_a_target() {
        for targets in 0..4usize {
            for catch in [None, Some(0), Some(1), Some(2), Some(3), Some(usize::MAX)] {
                for already in [false, true] {
                    let expected = !already && catch.is_some_and(|c| c < targets);
                    assert_eq!(
                        decide(targets, catch, already) == GatewayTrigger::Accept,
                        expected,
                        "targets={targets} catch={catch:?} already={already}"
                    );
                }
            }
        }
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// The trigger kernel is total and accepts exactly the first trigger of a
    /// real subscription target (first trigger wins, later ones rejected).
    #[kani::proof]
    fn event_gateway_trigger_first_wins() {
        let targets: usize = kani::any();
        let catch: Option<usize> = kani::any();
        let already: bool = kani::any();
        let decision = decide(targets, catch, already);
        kani::cover!(decision == GatewayTrigger::Accept);
        kani::cover!(decision == GatewayTrigger::Reject);
        if already {
            assert!(decision == GatewayTrigger::Reject);
        }
        if decision == GatewayTrigger::Accept {
            assert!(!already);
            assert!(matches!(catch, Some(c) if c < targets));
        }
    }
}
