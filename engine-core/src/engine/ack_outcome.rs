//! The ack-paced delivery answer as a pure kernel (#2037): what the instance
//! partition answers an ack-paced buffered delivery (#1949, Zeebe
//! `CORRELATING`) with — `consumed`, `accepted`, `deferred`.
//!
//! This module is the *decision* half; the *gathering* half (looking up the
//! instance partition's durable subscription record and the instance's
//! lifecycle state) stays in `Engine::emit_correlation_ack`. Keep it
//! dependency-free and in the subset Aeneas extracts (see AGENTS.md, "Write
//! Provable Code"): plain `bool` inputs, no panics, no allocation.

/// The durable facts the answer is derived from, as the collector read them
/// from the instance partition's subscription record and instance.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AckFacts {
    /// The instance partition still holds the subscription record.
    pub subscription_present: bool,
    /// The subscription is persistent (non-interrupting boundary / event
    /// sub-process): it stays open after a trigger.
    pub stays_open: bool,
    /// A persistent subscription recorded the delivered message among its
    /// correlated message keys.
    pub message_recorded: bool,
    /// The subscription is `Correlated` (a one-shot one settled).
    pub correlated: bool,
    /// The subscription is `Opening`: it still waits for messages.
    pub opening: bool,
    /// The instance is suspended.
    pub instance_suspended: bool,
}

/// The answer to one ack-paced delivery.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AckOutcome {
    /// The token took this message.
    pub consumed: bool,
    /// Consumed AND the subscription still waits for more.
    pub accepted: bool,
    /// Not consumed because the instance is suspended while its scope still
    /// waits: the message partition keeps the subscription and the message.
    pub deferred: bool,
}

/// The single canonical derivation of an ack-paced delivery's answer:
///
/// - `consumed`: a persistent subscription recorded the message; a one-shot
///   one settled (`Correlated`) — only this delivery can have done that,
///   because the message partition sends a one-shot subscription exactly one.
/// - `accepted`: consumed AND the subscription still waits (`Opening`).
/// - `deferred`: not consumed, still waiting, and the instance is suspended.
///
/// An absent subscription is neither consumed nor waiting.
pub fn ack_outcome(facts: AckFacts) -> AckOutcome {
    if !facts.subscription_present {
        return AckOutcome {
            consumed: false,
            accepted: false,
            deferred: false,
        };
    }
    let consumed = if facts.stays_open {
        facts.message_recorded
    } else {
        facts.correlated
    };
    let waiting = facts.opening;
    AckOutcome {
        consumed,
        accepted: consumed && waiting,
        deferred: !consumed && waiting && facts.instance_suspended,
    }
}

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod kani_proofs;
