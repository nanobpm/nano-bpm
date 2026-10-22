//! **BPMN best-practices conformance score** — ported from the
//! `camunda-consulting/bpmn-layout-bakeoff` project's `src/metrics/conformance.js`.
//!
//! Each of the eight rules scores a rendered layout on a specific geometric
//! best-practice from the BPMN literature (Silver *Method & Style*, 7PMG,
//! Camunda / bpmn.io, Trisotech, graph-drawing aesthetics literature —
//! Purchase, Effinger). Rules return a value in `[0.0, 1.0]`; the overall
//! score is a weighted average using [`RULE_WEIGHTS`] (which mirrors
//! `config/default.config.js`'s `CONFORMANCE_RULES` in the bake-off).
//!
//! **Why here, not in [`crate::conformance`]** — the two "conformance"
//! ideas in ProcessOS are distinct: [`crate::conformance`] compares the
//! *design* against *observed traces* (process mining); this module scores
//! the *rendered* diagram against BPMN drawing best-practices.
//!
//! Nano's [`crate::layout::SemanticAnnotations`] supplies the happy-path
//! answer (`flows[kind=primary]`), so we don't need the bake-off's
//! `pathClass` propagation — we already have it.

use std::collections::{HashMap, HashSet};

use crate::layout::geom::{proper_intersect, segment_intersects_rect, Geom, NodeKind};
use crate::layout::schema::{FlowKind, SemanticAnnotations};

/// Ordered list of the rules that compose the overall conformance score,
/// together with the weight and the citation. Weights sum to 1.0 and match
/// `config/default.config.js` in the bake-off; changing them changes the
/// declared ranking, so they're kept in one place for reproducibility.
pub const RULE_WEIGHTS: &[(&str, f64, &str)] = &[
    ("flowDirection", 0.22, "Silver M&S; Camunda; Effinger"),
    ("happyPathStraight", 0.18, "Camunda; Trisotech"),
    ("gatewayAlignment", 0.16, "Camunda; Purchase"),
    ("flowOrientation", 0.14, "Trisotech; Purchase"),
    ("labelClearance", 0.12, "Silver M&S; Trisotech"),
    ("portDirection", 0.08, "Camunda"),
    ("artifactClearance", 0.06, "Silver M&S"),
    ("canonicalSizing", 0.04, "Camunda"),
];

/// Per-rule breakdown returned alongside the overall score.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConformanceReport {
    /// Weighted mean of the per-rule scores using [`RULE_WEIGHTS`].
    pub score: f64,
    /// Per-rule score in insertion order matching [`RULE_WEIGHTS`].
    pub rules: Vec<RuleScore>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RuleScore {
    pub key: &'static str,
    pub label: &'static str,
    pub weight: f64,
    pub score: f64,
    pub source: &'static str,
}

const ROW_PX: f64 = 110.0;

/// Compute the conformance score for a laid-out BPMN document.
pub fn score(geom: &Geom, ann: &SemanticAnnotations) -> ConformanceReport {
    let happy_ids = happy_path_ids(ann);
    let scores: [(&str, &str, f64); 8] = [
        ("flowDirection", "Left-to-right flow", flow_direction(geom)),
        (
            "happyPathStraight",
            "Happy-path straightness",
            happy_path_straight(geom, &happy_ids),
        ),
        (
            "gatewayAlignment",
            "Gateway symmetry",
            gateway_alignment(geom),
        ),
        (
            "flowOrientation",
            "Orthogonal routing",
            flow_orientation(geom),
        ),
        (
            "labelClearance",
            "No lines through nodes",
            label_clearance(geom),
        ),
        (
            "portDirection",
            "Correct connection sides",
            port_direction(geom),
        ),
        (
            "artifactClearance",
            "Annotation/group clarity",
            artifact_clearance(geom),
        ),
        (
            "canonicalSizing",
            "Default element sizes",
            canonical_sizing(geom),
        ),
    ];
    let mut rules: Vec<RuleScore> = Vec::with_capacity(scores.len());
    let mut total = 0.0;
    let mut wsum = 0.0;
    for &(key, label, s) in &scores {
        let Some(&(_, w, src)) = RULE_WEIGHTS.iter().find(|r| r.0 == key) else {
            continue;
        };
        total += w * s;
        wsum += w;
        rules.push(RuleScore {
            key,
            label,
            weight: w,
            score: s,
            source: src,
        });
    }
    let final_score = if wsum > 0.0 { total / wsum } else { 0.0 };
    ConformanceReport {
        score: final_score,
        rules,
    }
}

fn happy_path_ids(ann: &SemanticAnnotations) -> HashSet<String> {
    let mut out = HashSet::new();
    for f in &ann.flows {
        if f.kind == FlowKind::Primary {
            for id in &f.nodes {
                out.insert(id.clone());
            }
        }
    }
    out
}

/// Sequence flows should progress left→right.
pub fn flow_direction(geom: &Geom) -> f64 {
    let seq: Vec<_> = geom
        .edges
        .iter()
        .filter(|e| e.is_sequence_flow && e.waypoints.len() >= 2)
        .collect();
    if seq.is_empty() {
        return 1.0;
    }
    let mut forward = 0usize;
    for e in &seq {
        let (Some(a), Some(b)) = (e.waypoints.first(), e.waypoints.last()) else {
            continue;
        };
        if b.0 > a.0 - 1.0 {
            forward += 1;
        }
    }
    forward as f64 / seq.len() as f64
}

/// Happy-path nodes should share one horizontal baseline (within ½ROW).
pub fn happy_path_straight(geom: &Geom, happy: &HashSet<String>) -> f64 {
    if happy.is_empty() {
        return 1.0;
    }
    let ys: Vec<f64> = geom
        .leaves()
        .filter(|n| happy.contains(&n.id))
        .map(|n| n.cy())
        .collect();
    if ys.len() < 2 {
        return 1.0;
    }
    let mean = ys.iter().sum::<f64>() / ys.len() as f64;
    let ok = ys
        .iter()
        .filter(|&&y| (y - mean).abs() <= ROW_PX * 0.5)
        .count();
    ok as f64 / ys.len() as f64
}

/// Split/join gateway y ≈ mean(branch y).
pub fn gateway_alignment(geom: &Geom) -> f64 {
    let gateways: Vec<_> = geom
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Gateway)
        .collect();
    if gateways.is_empty() {
        return 1.0;
    }
    let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut inc: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in geom.edges.iter().filter(|e| e.is_sequence_flow) {
        out.entry(e.source.as_str())
            .or_default()
            .push(e.target.as_str());
        inc.entry(e.target.as_str())
            .or_default()
            .push(e.source.as_str());
    }
    let mut sum = 0.0;
    let mut cnt = 0.0;
    for g in gateways {
        let outs: Vec<f64> = out
            .get(g.id.as_str())
            .map(|v| {
                v.iter()
                    .filter_map(|id| geom.node(id).map(|n| n.cy()))
                    .collect()
            })
            .unwrap_or_default();
        let ins: Vec<f64> = inc
            .get(g.id.as_str())
            .map(|v| {
                v.iter()
                    .filter_map(|id| geom.node(id).map(|n| n.cy()))
                    .collect()
            })
            .unwrap_or_default();
        let branches = if outs.len() >= 2 {
            outs
        } else if ins.len() >= 2 {
            ins
        } else {
            continue;
        };
        let mean_y = branches.iter().sum::<f64>() / branches.len() as f64;
        sum += 1.0 - (mean_y - g.cy()).abs().min(2.0 * ROW_PX) / (2.0 * ROW_PX);
        cnt += 1.0;
    }
    if cnt > 0.0 {
        sum / cnt
    } else {
        1.0
    }
}

/// Fraction of edge segments that are axis-aligned (Δx<2 or Δy<2).
pub fn flow_orientation(geom: &Geom) -> f64 {
    let mut ortho = 0usize;
    let mut total = 0usize;
    for e in &geom.edges {
        let w = &e.waypoints;
        for i in 0..w.len().saturating_sub(1) {
            total += 1;
            let dx = (w[i + 1].0 - w[i].0).abs();
            let dy = (w[i + 1].1 - w[i].1).abs();
            if dx < 2.0 || dy < 2.0 {
                ortho += 1;
            }
        }
    }
    if total == 0 {
        1.0
    } else {
        ortho as f64 / total as f64
    }
}

/// Sequence flows shouldn't pass through unrelated node boxes.
pub fn label_clearance(geom: &Geom) -> f64 {
    let seq: Vec<_> = geom.edges.iter().filter(|e| e.is_sequence_flow).collect();
    if seq.is_empty() {
        return 1.0;
    }
    let boxes: Vec<_> = geom.leaves().collect();
    let mut clean = 0usize;
    for e in &seq {
        let mut through = false;
        for i in 0..e.waypoints.len().saturating_sub(1) {
            for b in &boxes {
                if b.id == e.source || b.id == e.target {
                    continue;
                }
                if segment_intersects_rect(e.waypoints[i], e.waypoints[i + 1], b) {
                    through = true;
                    break;
                }
            }
            if through {
                break;
            }
        }
        if !through {
            clean += 1;
        }
    }
    clean as f64 / seq.len() as f64
}

/// Edges should leave source right/bottom, enter target left/top.
pub fn port_direction(geom: &Geom) -> f64 {
    let seq: Vec<_> = geom
        .edges
        .iter()
        .filter(|e| e.is_sequence_flow && e.waypoints.len() >= 2)
        .collect();
    if seq.is_empty() {
        return 1.0;
    }
    let mut good = 0usize;
    for e in &seq {
        let Some(s) = geom.node(&e.source) else {
            good += 1;
            continue;
        };
        let Some(t) = geom.node(&e.target) else {
            good += 1;
            continue;
        };
        let (Some(a), Some(b)) = (e.waypoints.first(), e.waypoints.last()) else {
            continue;
        };
        let leave_ok = (a.0 - (s.x + s.w)).abs() < 4.0 || (a.1 - (s.y + s.h)).abs() < 4.0;
        let enter_ok = (b.0 - t.x).abs() < 4.0 || (b.1 - t.y).abs() < 4.0;
        if leave_ok && enter_ok {
            good += 1;
        }
    }
    good as f64 / seq.len() as f64
}

/// Boundary events should dock on their host, not on unrelated shapes.
pub fn artifact_clearance(geom: &Geom) -> f64 {
    let arts: Vec<_> = geom
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::BoundaryEvent)
        .collect();
    if arts.is_empty() {
        return 1.0;
    }
    let flow: Vec<_> = geom
        .nodes
        .iter()
        .filter(|n| matches!(n.kind, NodeKind::Task | NodeKind::Gateway))
        .collect();
    let mut ok = 0usize;
    for a in &arts {
        let clashes = flow
            .iter()
            .filter(|b| crate::layout::geom::rect_overlap_area(a, b) > 0.0)
            .count();
        if clashes <= 1 {
            ok += 1;
        }
    }
    ok as f64 / arts.len() as f64
}

/// Tasks/events/gateways should be at their canonical BPMN sizes.
pub fn canonical_sizing(geom: &Geom) -> f64 {
    let relevant: Vec<_> = geom
        .nodes
        .iter()
        .filter_map(|n| n.kind.canonical_size().map(|sz| (n, sz)))
        .collect();
    if relevant.is_empty() {
        return 1.0;
    }
    let mut ok = 0usize;
    for (n, (w, h)) in &relevant {
        if (n.w - w).abs() <= 6.0 && (n.h - h).abs() <= 6.0 {
            ok += 1;
        }
    }
    ok as f64 / relevant.len() as f64
}

#[allow(dead_code)]
fn _keep_proper_intersect_referenced() -> bool {
    proper_intersect((0.0, 0.0), (1.0, 0.0), (0.5, -1.0), (0.5, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::geom::parse;

    #[test]
    fn perfect_layout_scores_high() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let g = parse(bpmn);
        let ann: SemanticAnnotations =
            serde_json::from_str(include_str!("../../fixtures/layout/tiny.annotations.json"))
                .unwrap();
        let r = score(&g, &ann);
        assert_eq!(r.rules.len(), RULE_WEIGHTS.len());
        assert!(
            r.score > 0.55,
            "expected reasonable conformance for tiny fixture, got {}",
            r.score
        );
    }

    #[test]
    fn flow_direction_forward_edges_score_full() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let g = parse(bpmn);
        assert_eq!(flow_direction(&g), 1.0);
    }

    #[test]
    fn flow_orientation_axis_aligned_scores_full() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let g = parse(bpmn);
        assert_eq!(flow_orientation(&g), 1.0);
    }
}
