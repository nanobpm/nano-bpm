//! **Deterministic polish pass** — port of the bake-off's
//! `src/engines/polish.js`. Runs after a solver has produced its BPMN and
//! nudges the layout toward the conformance rules by (a) snapping nodes to a
//! coarse grid, (b) centring split/join gateways on the mean y of their
//! branches, and (c) aligning happy-path nodes to a shared baseline.
//!
//! **Non-regressing guarantee.** Every candidate is scored by
//! [`crate::layout::conformance::score`] and [`crate::layout::gates::overlap_ratio`]
//! before being accepted; only the *first* candidate that improves-or-holds
//! both is shipped. When both candidates regress the base layout is
//! returned untouched with `meta.reverted = true`.
//!
//! No LLM, no randomness — this is the safe deterministic starting point
//! before we contemplate a stochastic refinement loop.

use std::collections::{HashMap, HashSet};

use crate::layout::conformance;
use crate::layout::gates;
use crate::layout::geom::{parse as parse_geom, Geom, NodeKind};
use crate::layout::schema::{FlowKind, SemanticAnnotations};

const GRID: f64 = 10.0;

/// Metadata about what the polish pass did.
#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct PolishMeta {
    /// Number of node moves applied.
    pub moves: usize,
    /// Number of happy-path aligns applied.
    pub aligns: usize,
    /// True when the candidate set all regressed and the base was kept.
    pub reverted: bool,
    /// Which candidate index won (0 = grid + gateway + happy, 1 = grid +
    /// gateway only). `-1` when reverted.
    pub candidate: i32,
}

/// Apply the polish pass to a fully-emitted BPMN document.
///
/// Returns `(polished_xml, meta)`. The XML is either the modified DI with
/// grid-snap / gateway-centering / happy-path aligns applied, or the
/// original if no candidate improved on it. Callers should re-parse the
/// returned XML if they need to reason about the final geometry — the
/// polish pass does not return a fresh [`Geom`] to avoid the caller
/// depending on our internal shape representation.
pub fn polish(xml: &str, ann: &SemanticAnnotations) -> (String, PolishMeta) {
    let base = parse_geom(xml);
    if base.nodes.is_empty() {
        return (xml.to_string(), PolishMeta::default());
    }
    let base_overlap = gates::overlap_ratio(&base);
    let base_conf = conformance::score(&base, ann).score;

    let all_moves = build_all_moves(&base, ann);
    let gateway_and_grid_only = all_moves
        .iter()
        .filter(|m| !m.kind.is_happy_align())
        .cloned()
        .collect::<Vec<_>>();

    for (idx, candidate) in [&all_moves, &gateway_and_grid_only].iter().enumerate() {
        let applied = apply_moves(xml, candidate);
        let g = parse_geom(&applied);
        let overlap_ok = gates::overlap_ratio(&g) <= base_overlap + 1e-4;
        let conf_ok = conformance::score(&g, ann).score >= base_conf - 1e-9;
        if overlap_ok && conf_ok {
            let (m, a) = count_kinds(candidate);
            return (
                applied,
                PolishMeta {
                    moves: m,
                    aligns: a,
                    reverted: false,
                    candidate: idx as i32,
                },
            );
        }
    }
    (
        xml.to_string(),
        PolishMeta {
            reverted: true,
            candidate: -1,
            ..PolishMeta::default()
        },
    )
}

#[derive(Debug, Clone)]
struct Move {
    id: String,
    dx: f64,
    dy: f64,
    kind: MoveKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveKind {
    GridSnap,
    GatewayCentring,
    HappyAlign,
}

impl MoveKind {
    fn is_happy_align(self) -> bool {
        matches!(self, MoveKind::HappyAlign)
    }
}

fn count_kinds(moves: &[Move]) -> (usize, usize) {
    let mut m = 0;
    let mut a = 0;
    for mv in moves {
        if mv.kind.is_happy_align() {
            a += 1;
        } else {
            m += 1;
        }
    }
    (m, a)
}

fn build_all_moves(base: &Geom, ann: &SemanticAnnotations) -> Vec<Move> {
    let mut moves: Vec<Move> = Vec::new();

    // 1. Gateway centring on mean y of branches.
    let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut inc: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in base.edges.iter().filter(|e| e.is_sequence_flow) {
        out.entry(e.source.as_str())
            .or_default()
            .push(e.target.as_str());
        inc.entry(e.target.as_str())
            .or_default()
            .push(e.source.as_str());
    }
    for g in base.nodes.iter().filter(|n| n.kind == NodeKind::Gateway) {
        let outs: Vec<f64> = out
            .get(g.id.as_str())
            .map(|v| {
                v.iter()
                    .filter_map(|id| base.node(id).map(|n| n.cy()))
                    .collect()
            })
            .unwrap_or_default();
        let ins: Vec<f64> = inc
            .get(g.id.as_str())
            .map(|v| {
                v.iter()
                    .filter_map(|id| base.node(id).map(|n| n.cy()))
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
        let dy = mean_y - g.cy();
        if dy.abs() > 0.5 {
            moves.push(Move {
                id: g.id.clone(),
                dx: 0.0,
                dy,
                kind: MoveKind::GatewayCentring,
            });
        }
    }

    // 2. Grid-snap movable nodes.
    for n in base
        .nodes
        .iter()
        .filter(|n| n.kind.is_leaf() || n.kind == NodeKind::SubProcess)
    {
        let dx = (n.x / GRID).round() * GRID - n.x;
        let dy = (n.y / GRID).round() * GRID - n.y;
        if dx.abs() > 0.01 || dy.abs() > 0.01 {
            moves.push(Move {
                id: n.id.clone(),
                dx,
                dy,
                kind: MoveKind::GridSnap,
            });
        }
    }

    // 3. Happy-path align: pull happy nodes toward the mean y of the
    //    happy chain (bake-off polish only aligns nodes actually on the
    //    happy sequence chain — we approximate that by intersecting
    //    happy-flow membership with the leaves).
    let happy: HashSet<String> = ann
        .flows
        .iter()
        .filter(|f| f.kind == FlowKind::Primary)
        .flat_map(|f| f.nodes.iter().cloned())
        .collect();
    let happy_nodes: Vec<_> = base.leaves().filter(|n| happy.contains(&n.id)).collect();
    if happy_nodes.len() >= 2 {
        let mean_y = happy_nodes.iter().map(|n| n.cy()).sum::<f64>() / happy_nodes.len() as f64;
        for n in &happy_nodes {
            let dy = mean_y - n.cy();
            if dy.abs() > 0.5 {
                moves.push(Move {
                    id: n.id.clone(),
                    dx: 0.0,
                    dy,
                    kind: MoveKind::HappyAlign,
                });
            }
        }
    }
    moves
}

/// Rewrite `<dc:Bounds x="…" y="…"/>` on every shape in the moves list, and
/// re-write the endpoints of any edge whose source or target moved so its
/// polyline still meets the shape mid-right / mid-left. Internal waypoints
/// are preserved (no re-routing) — the bake-off does full re-routing here,
/// but a targeted endpoint update keeps this pass small and safe. If the
/// endpoint update breaks orthogonality the non-regressing gate will
/// reject the candidate.
fn apply_moves(xml: &str, moves: &[Move]) -> String {
    if moves.is_empty() {
        return xml.to_string();
    }
    // Collapse per-id moves (a node can appear in gateway + grid + happy).
    let mut delta: HashMap<String, (f64, f64)> = HashMap::new();
    for m in moves {
        let e = delta.entry(m.id.clone()).or_insert((0.0, 0.0));
        e.0 += m.dx;
        e.1 += m.dy;
    }

    let mut out = String::with_capacity(xml.len() + 128);
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmndi:BPMNShape") {
        let s = cursor + rel;
        let bounds_start = xml[s..]
            .find("<dc:Bounds")
            .map(|k| s + k)
            .unwrap_or(xml.len());
        let bounds_end = xml[bounds_start..]
            .find("/>")
            .map(|k| bounds_start + k + 2)
            .unwrap_or(xml.len());
        out.push_str(&xml[cursor..bounds_start]);
        let block = &xml[bounds_start..bounds_end];
        let shape_block = &xml[s..bounds_start];
        let id = attr(shape_block, "bpmnElement").unwrap_or_default();
        if let Some(&(dx, dy)) = delta.get(&id) {
            let (x, y, w, h) = parse_bounds(block).unwrap_or((0.0, 0.0, 0.0, 0.0));
            let new_x = x + dx;
            let new_y = y + dy;
            out.push_str(&format!(
                "<dc:Bounds x=\"{:.0}\" y=\"{:.0}\" width=\"{:.0}\" height=\"{:.0}\"/>",
                new_x, new_y, w, h
            ));
        } else {
            out.push_str(block);
        }
        cursor = bounds_end;
    }
    out.push_str(&xml[cursor..]);

    // Re-parse for the edge endpoint rewrite.
    let geom = parse_geom(&out);
    if geom.edges.is_empty() {
        return out;
    }
    reroute_edges(&out, &geom, &delta)
}

fn reroute_edges(xml: &str, geom: &Geom, delta: &HashMap<String, (f64, f64)>) -> String {
    let mut result = String::with_capacity(xml.len() + 64);
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmndi:BPMNEdge") {
        let s = cursor + rel;
        let close = xml[s..]
            .find("</bpmndi:BPMNEdge>")
            .map(|k| s + k)
            .unwrap_or(xml.len());
        let block = &xml[s..close];
        let edge_id = attr(block, "bpmnElement").unwrap_or_default();
        let edge = geom.edges.iter().find(|e| e.id == edge_id);
        // Only rewrite when at least one endpoint moved.
        let touched = edge
            .map(|e| delta.contains_key(&e.source) || delta.contains_key(&e.target))
            .unwrap_or(false);
        if !touched {
            result.push_str(&xml[cursor..close]);
            cursor = close;
            continue;
        }
        let Some(e) = edge else {
            result.push_str(&xml[cursor..close]);
            cursor = close;
            continue;
        };
        let Some(src) = geom.node(&e.source) else {
            result.push_str(&xml[cursor..close]);
            cursor = close;
            continue;
        };
        let Some(tgt) = geom.node(&e.target) else {
            result.push_str(&xml[cursor..close]);
            cursor = close;
            continue;
        };
        // Write everything up to the `<bpmndi:BPMNEdge …>` open (attributes
        // preserved so `bpmnElement` stays intact).
        let attrs_end = xml[s..close].find('>').map(|k| s + k + 1).unwrap_or(close);
        result.push_str(&xml[cursor..attrs_end]);
        // Emit fresh waypoints: source right-mid → target left-mid, with a
        // single elbow when they differ vertically.
        let ax = src.x + src.w;
        let ay = src.cy();
        let bx = tgt.x;
        let by = tgt.cy();
        result.push_str(&format!("<di:waypoint x=\"{ax:.0}\" y=\"{ay:.0}\"/>"));
        if (ay - by).abs() > 0.5 {
            let mid = (ax + bx) * 0.5;
            result.push_str(&format!("<di:waypoint x=\"{mid:.0}\" y=\"{ay:.0}\"/>"));
            result.push_str(&format!("<di:waypoint x=\"{mid:.0}\" y=\"{by:.0}\"/>"));
        }
        result.push_str(&format!("<di:waypoint x=\"{bx:.0}\" y=\"{by:.0}\"/>"));
        cursor = close;
    }
    result.push_str(&xml[cursor..]);
    result
}

fn parse_bounds(block: &str) -> Option<(f64, f64, f64, f64)> {
    Some((
        attr(block, "x")?.parse().ok()?,
        attr(block, "y")?.parse().ok()?,
        attr(block, "width")?.parse().ok()?,
        attr(block, "height")?.parse().ok()?,
    ))
}

fn attr(span: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=\"");
    let s = span.find(&needle)? + needle.len();
    let e = span[s..].find('"').map(|k| s + k)?;
    Some(span[s..e].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polish_never_regresses_or_reverts() {
        // We don't assert *what* polish does on the tiny fixture — it may
        // apply zero changes (grid-snapped already) or accept the first
        // candidate — but we DO assert it never lowers the score.
        let xml = include_str!("../../fixtures/layout/tiny.bpmn");
        let ann: SemanticAnnotations =
            serde_json::from_str(include_str!("../../fixtures/layout/tiny.annotations.json"))
                .unwrap();
        let base_geom = parse_geom(xml);
        let base_score = conformance::score(&base_geom, &ann).score;
        let base_overlap = gates::overlap_ratio(&base_geom);

        let (polished, meta) = polish(xml, &ann);
        let g = parse_geom(&polished);
        let s = conformance::score(&g, &ann).score;
        let o = gates::overlap_ratio(&g);
        assert!(
            s >= base_score - 1e-9,
            "polish must not lower conformance: {} -> {} (meta {:?})",
            base_score,
            s,
            meta
        );
        assert!(
            o <= base_overlap + 1e-4,
            "polish must not raise overlap: {} -> {} (meta {:?})",
            base_overlap,
            o,
            meta
        );
    }
}
