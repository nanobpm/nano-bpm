//! Cluster create-placement protection & load-awareness (ADR 0014).
//!
//! By default nanobpmn places `createProcessInstance` by **blind round-robin**
//! over every partition ([`crate::partition::PartitionRouter::next_create_placement`]):
//! it ignores whether the chosen owner is saturated, and a forwarded create
//! bypasses the owner's own admission gates. That is fine when every node has
//! equal capacity and load arrives evenly, but under **heterogeneous resources**
//! or **skewed ingress** it lets one node be overrun while peers sit idle.
//!
//! This module adds two staged behaviours, selected by `NANOBPMN_CREATE_PLACEMENT`
//! (**default `balanced`** — the engine self-optimizes unless explicitly told
//! `off`):
//!
//! - **`protect`** — every node self-protects: a forwarded create is subjected to
//!   the owner's admission gates and, if the owner is saturated, *shed* back to the
//!   ingress node, which **reroutes** to another owner (and only 503s the client
//!   when the whole cluster is saturated — true global backpressure). This closes
//!   the "forwarded creates skip admission" hole so no node is overrun by work
//!   placed on it.
//! - **`balanced`** (default) — implies `protect`, and additionally makes placement
//!   **load-aware**: each node periodically gossips a composite load index to its
//!   peers, and placement weights owners inversely to load so new creates flow to
//!   nodes with real headroom (work-conserving balance) instead of piling onto a
//!   saturated owner. A stale/absent peer hint is treated as full headroom, so an
//!   unprobed peer still receives traffic; the reactive `protect` shed is the
//!   backstop when the hint is wrong.
//!
//! `off` restores blind round-robin. On a single node every mode collapses to a
//! no-op (placement returns "local" with ≤ 1 partition; gossip is idle without
//! peers), so the `balanced` default is byte-identical on a single node and adds
//! zero overhead there.

/// Runtime mode for cluster create placement, from `NANOBPMN_CREATE_PLACEMENT`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlacementMode {
    /// Blind round-robin, forwarded creates ungated (historical behaviour).
    Off,
    /// Forwarded creates honour the owner's admission gates; ingress reroutes
    /// around a shedding owner.
    Protect,
    /// `Protect` plus load-aware, load-gossiped weighted placement.
    Balanced,
}

impl PlacementMode {
    /// Whether forwarded creates are gated on the owner and rerouted on shed.
    /// True for `Protect` and `Balanced`.
    pub fn protects(self) -> bool {
        matches!(self, PlacementMode::Protect | PlacementMode::Balanced)
    }

    /// Whether placement is load-aware (weighted) and load is gossiped between
    /// peers. True only for `Balanced`.
    pub fn balances(self) -> bool {
        matches!(self, PlacementMode::Balanced)
    }

    /// Human-readable description for the startup log.
    pub fn describe(self) -> &'static str {
        match self {
            PlacementMode::Off => "off (blind round-robin; forwarded creates ungated)",
            PlacementMode::Protect => {
                "protect (forwarded creates honour the owner's admission gates; reroute on shed)"
            }
            PlacementMode::Balanced => {
                "balanced (protect + load-aware weighted placement with peer load gossip)"
            }
        }
    }
}

/// Parse `NANOBPMN_CREATE_PLACEMENT`. **Defaults to `Balanced`** when unset (the
/// self-optimizing posture: the engine load-balances and self-protects creates
/// on its own — see the design philosophy in the README). Single node and every
/// `protect`/`balanced` path collapse to a no-op without peers, so this default
/// is byte-identical on a single node. An operator can still pin an explicit
/// stage, and `off` restores blind round-robin. Unrecognised values fail toward
/// the self-optimizing default rather than silently disabling it.
pub fn parse_placement_mode(raw: Option<&str>) -> PlacementMode {
    let Some(raw) = raw else {
        return PlacementMode::Balanced;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" | "0" | "false" | "no" | "none" | "disable" | "disabled" => PlacementMode::Off,
        "protect" | "1" | "on" | "true" | "yes" => PlacementMode::Protect,
        // "balanced" / "2" / aliases, and anything unrecognised, resolve to the
        // self-optimizing default.
        _ => PlacementMode::Balanced,
    }
}

/// Sentinel load index meaning "this node is shedding creates right now" — it is
/// never chosen by weighted placement (weight 0). Larger than any realistic
/// backlog and safely below `i64::MAX` so arithmetic on it cannot overflow.
pub const SHED_LOAD: i64 = i64::MAX / 2;

/// Numerator for the inverse-load weight. A node with load 0 gets this weight; a
/// node with load `l` gets `WEIGHT_SCALE / (l + 1)`, so weight decreases with
/// load and differentiates strongly across an order-of-magnitude load gap
/// (e.g. load 100 → ~9901, load 4000 → ~250, a ~40× steer) while never dividing
/// by zero. The load index is a create-acceptance-headroom occupancy
/// (`ServerImpl::create_occupancy_index`, in the gateway binary), not a resident
/// backlog count.
const WEIGHT_SCALE: u128 = 1_000_000;

/// Inverse-load placement weight for an owner with composite load index `load`.
/// A shedding owner ([`SHED_LOAD`] or above) gets weight 0 — never placed on.
/// Otherwise the weight is `WEIGHT_SCALE / (load + 1)`, monotonically decreasing
/// in load with a floor at load 0.
pub fn placement_weight(load: i64) -> u128 {
    if load >= SHED_LOAD {
        return 0;
    }
    let l = load.max(0) as u128;
    WEIGHT_SCALE / (l + 1)
}

/// Smooth weighted round-robin (SWRR) selection over `weights`, nginx-style.
/// `current` is the persistent smoothing state (same length as `weights`, one
/// slot per candidate, carried across calls); it is mutated in place. Returns the
/// chosen index, or `None` when every weight is 0 (all owners shedding) — the
/// caller then falls back to plain rotation, letting the reactive shed/reroute
/// layer handle protection.
///
/// SWRR is the right primitive here because it degenerates to an **exact
/// round-robin when the weights are equal** (the idle-cluster common case — so
/// creates spread evenly across every owner), yet spreads **proportionally and
/// smoothly** (no clumping) the moment loads diverge, steering new work toward the
/// owners with the most headroom. Unlike a banded counter sweep it never needs a
/// long run of calls to "unstick" from the first candidate: every call advances.
///
/// Algorithm: add each candidate's weight to its running `current`; pick the
/// non-shedding candidate with the greatest `current`; subtract the total weight
/// from the winner. A zero-weight (shedding/tried) candidate is never selected —
/// its `current` only stalls, never wins.
pub fn swrr_pick(weights: &[u128], current: &mut [i128]) -> Option<usize> {
    debug_assert_eq!(weights.len(), current.len());
    let total: i128 = weights.iter().map(|&w| w as i128).sum();
    if total == 0 {
        return None;
    }
    let mut best: Option<usize> = None;
    for i in 0..weights.len() {
        current[i] += weights[i] as i128;
        let dominated = match best {
            Some(b) => current[i] <= current[b],
            None => false,
        };
        if weights[i] > 0 && !dominated {
            best = Some(i);
        }
    }
    let b = best?;
    current[b] -= total;
    Some(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_to_balanced_and_reads_stages() {
        // Unset ⇒ the self-optimizing default.
        assert_eq!(parse_placement_mode(None), PlacementMode::Balanced);
        // Unrecognised fails toward the default rather than disabling.
        assert_eq!(
            parse_placement_mode(Some("nonsense")),
            PlacementMode::Balanced
        );
        // Explicit opt-out restores blind round-robin.
        assert_eq!(parse_placement_mode(Some("off")), PlacementMode::Off);
        assert_eq!(parse_placement_mode(Some("0")), PlacementMode::Off);
        assert_eq!(parse_placement_mode(Some("false")), PlacementMode::Off);
        assert_eq!(
            parse_placement_mode(Some("protect")),
            PlacementMode::Protect
        );
        assert_eq!(
            parse_placement_mode(Some(" PROTECT ")),
            PlacementMode::Protect
        );
        assert_eq!(parse_placement_mode(Some("1")), PlacementMode::Protect);
        assert_eq!(
            parse_placement_mode(Some("balanced")),
            PlacementMode::Balanced
        );
        assert_eq!(parse_placement_mode(Some("2")), PlacementMode::Balanced);
    }

    #[test]
    fn mode_capability_flags() {
        assert!(!PlacementMode::Off.protects());
        assert!(!PlacementMode::Off.balances());
        assert!(PlacementMode::Protect.protects());
        assert!(!PlacementMode::Protect.balances());
        assert!(PlacementMode::Balanced.protects());
        assert!(PlacementMode::Balanced.balances());
    }

    #[test]
    fn weight_decreases_with_load_and_zeroes_when_shedding() {
        assert_eq!(placement_weight(SHED_LOAD), 0);
        assert_eq!(placement_weight(i64::MAX), 0);
        assert_eq!(placement_weight(0), WEIGHT_SCALE);
        // Monotonically decreasing; a deep node gets far less than a shallow one.
        assert!(placement_weight(100) > placement_weight(4000));
        assert!(placement_weight(4000) > 0);
        // Negative (shouldn't happen) clamps to the load-0 weight, never panics.
        assert_eq!(placement_weight(-5), WEIGHT_SCALE);
    }

    #[test]
    fn swrr_equal_weights_is_exact_round_robin() {
        // The idle-cluster common case: equal weights ⇒ strict rotation, every
        // call advancing to the next owner (no clumping on the first candidate).
        let w = [5u128, 5, 5];
        let mut cur = [0i128; 3];
        let picks: Vec<usize> = (0..9).map(|_| swrr_pick(&w, &mut cur).unwrap()).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn swrr_is_proportional_and_smooth() {
        // 3:1 weight ⇒ owner 0 picked ~3× as often as owner 1 over a window, and
        // the lighter owner still gets a regular share (smooth, not starved).
        let w = [3u128, 1];
        let mut cur = [0i128; 2];
        let picks: Vec<usize> = (0..400).map(|_| swrr_pick(&w, &mut cur).unwrap()).collect();
        let zeros = picks.iter().filter(|&&i| i == 0).count();
        let ones = picks.iter().filter(|&&i| i == 1).count();
        assert_eq!(zeros, 300);
        assert_eq!(ones, 100);
    }

    #[test]
    fn swrr_never_picks_a_shedding_owner() {
        // Middle owner is shedding (weight 0): it is never selected, and the other
        // two still rotate.
        let w = [4u128, 0, 4];
        let mut cur = [0i128; 3];
        let picks: Vec<usize> = (0..20).map(|_| swrr_pick(&w, &mut cur).unwrap()).collect();
        assert!(!picks.contains(&1), "a shedding owner is never placed on");
        assert!(picks.contains(&0) && picks.contains(&2));
    }

    #[test]
    fn swrr_none_when_all_shedding() {
        let mut cur = [0i128; 3];
        assert_eq!(swrr_pick(&[0, 0, 0], &mut cur), None);
    }
}
