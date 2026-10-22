//! The leader-durable **fence epoch** register (ADR 0019).
//!
//! Every partition carries a monotonic fence value `(epoch, leader)` — the
//! "term/lock" of the leadership handoff + reclaim protocol. A node claims the
//! partition by promoting itself into the register at a fresh epoch, and peers
//! adopt the winning value, fencing off any stale leader. The two decisions
//! that keep leadership single-valued live here as one canonical, side-effect-
//! free implementation so the gateway binary (`ServerImpl::handle_promotion`,
//! `ServerImpl::next_promotion_epoch` in `server/src/main.rs`) and the
//! conformance test share exactly one fence rule — no drift surface:
//!
//! * [`wins`] — the total order over register values: a strictly higher epoch
//!   wins, and an equal epoch is broken by the **lowest node id**. The
//!   deterministic tie-break collapses a symmetric multi-way split (two
//!   survivors that each promote at the same epoch) back to a single leader.
//! * [`next_epoch`] — the epoch a node claims when it (self-)promotes: it
//!   re-asserts the SAME epoch if it already holds the register (idempotent
//!   reclaim re-entry — an unconditional `+1` there is the unbounded epoch
//!   climb / `leader_reject` storm), else overtakes a peer at `cur_epoch + 1`.
//!
//! The register's safety and reclaim-liveness properties are model-checked in
//! `formal/tla/raft/RaftHandoff.tla` (the `RaftHandoff` TLA+ spec). This module
//! is the anti-drift anchor for that spec: `tests` below replays the spec's
//! transition system against these exact functions, so the model and the
//! production fence rule cannot silently diverge.

/// Sentinel `leader` for a register with no known leader (`u64::MAX`). Any real
/// node id compares below it, so the very first promotion (at epoch >= 1) always
/// wins over the implicit `(0, NO_LEADER)` incumbent state.
pub const NO_LEADER: u64 = u64::MAX;

/// Does an inbound promotion `(epoch, leader)` win the fence over the current
/// register value `(cur_epoch, cur_leader)`?
///
/// `true` iff the promotion is strictly newer, or ties the epoch with a lower
/// node id (the deterministic tie-break). This is the exact predicate
/// `ServerImpl::handle_promotion` uses to decide whether to adopt an announced
/// promotion (and, on adopting a value that names a different leader, fence a
/// stale leader down).
#[inline]
#[must_use]
pub fn wins(cur_epoch: u64, cur_leader: u64, epoch: u64, leader: u64) -> bool {
    epoch > cur_epoch || (epoch == cur_epoch && leader < cur_leader)
}

/// The epoch node `me` claims when it (self-)promotes a partition whose current
/// register value is `(cur_epoch, cur_leader)`.
///
/// Re-asserts `cur_epoch` when `me` already holds the register (the reclaim path
/// can legitimately re-enter before its self-election surfaces in the metrics
/// the recovery tick reads); otherwise overtakes the incumbent at `cur_epoch +
/// 1`. Tying the increment to "overtake a peer", not "promote again", keeps the
/// reclaim epoch deterministic (`incumbent + 1`) regardless of scheduling
/// jitter. Mirrors `ServerImpl::next_promotion_epoch`.
#[inline]
#[must_use]
pub fn next_epoch(cur_epoch: u64, cur_leader: u64, me: u64) -> u64 {
    if cur_leader == me {
        cur_epoch
    } else {
        // Exhaustion policy: the fence epoch is strictly monotone, so an
        // unchecked `+ 1` at `u64::MAX` would wrap to 0 and regress the fence —
        // an older announcement could then win and stale leadership be accepted.
        // `cur_epoch` is not bounded below `u64::MAX` here (it can be adopted
        // directly from a peer's `Promote` frame), so we fail closed rather than
        // corrupt the fence. The space is unreachable in practice (it takes
        // 2^64 overtakes to exhaust), so this is a defence-in-depth assertion,
        // not a live failure mode.
        #[allow(
            clippy::expect_used,
            reason = "defence-in-depth: exhausting the u64 fence-epoch space is unreachable; wrapping would silently regress the fence"
        )]
        cur_epoch.checked_add(1).expect(
            "fence epoch space exhausted at u64::MAX; refusing to wrap and regress the fence",
        )
    }
}

#[cfg(test)]
mod tests;
