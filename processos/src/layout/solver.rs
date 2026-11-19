//! **Row-bias solver** — the "physics" that turns [`SemanticAnnotations`] into a
//! `node_id -> preferred_row` map consumed by
//! [`crate::bpmn_model::definition_to_xml_with_row_bias`].
//!
//! # v0 algorithm (deliberately simple)
//!
//! 1. **Flow band assignment.** For every annotated flow, every node it names
//!    gets `bias[node] = flow.kind.target_row()`. When a node is claimed by
//!    multiple flows the highest-[`FlowKind::priority`] flow wins.
//! 2. **Cluster attraction.** For every cluster, blend each member's bias
//!    toward the *cluster centroid* (mean of member biases) with weight
//!    `affinity` (0 = ignored, 1 = collapse to centroid).
//!
//! This is **not** a physics simulation — it's a two-pass projection. The
//! reason: (a) the underlying Sugiyama assigns integer rows with collision
//! avoidance, so sub-row precision is wasted; (b) determinism matters for LLM
//! iteration (same annotations → identical layout). If v0 turns out too coarse
//! we can layer a proper constraint solver (WebCola-style projections) or
//! Rapier-driven refinement on top — but *this* is the seam to prove first.

use std::collections::BTreeMap;

use super::schema::{FlowKind, SemanticAnnotations};

/// Compute a `node_id -> preferred_row` bias map from semantic annotations.
///
/// Returned rows are in the same coordinate system as
/// [`crate::bpmn_model::desired_row`] (integer-ish grid rows; positive = down
/// the page). Nodes not mentioned by any flow or cluster don't appear in the
/// map — the caller falls back to the predecessor-mean heuristic for them.
pub fn compute_row_bias(ann: &SemanticAnnotations) -> BTreeMap<String, f64> {
    let mut bias: BTreeMap<String, f64> = BTreeMap::new();
    let mut node_kind: BTreeMap<String, FlowKind> = BTreeMap::new();

    // Pass 1 — flow bands. Higher-priority kinds override lower-priority ones
    // so the last write wins deterministically regardless of flow order in the
    // input JSON.
    for flow in &ann.flows {
        for node in &flow.nodes {
            let take = match node_kind.get(node) {
                None => true,
                Some(prev) => flow.kind.priority() > prev.priority(),
            };
            if take {
                node_kind.insert(node.clone(), flow.kind);
                bias.insert(node.clone(), flow.kind.target_row());
            }
        }
    }

    // Pass 2 — cluster attraction. Blend each member's current bias toward the
    // cluster centroid. Members without a flow-derived bias start at 0.0
    // (centerline default) so a cluster of unannotated nodes stays coherent.
    for cluster in &ann.clusters {
        if cluster.nodes.is_empty() {
            continue;
        }
        let affinity = cluster.affinity.clamp(0.0, 1.0);
        if affinity <= 0.0 {
            continue;
        }
        let centroid: f64 = cluster
            .nodes
            .iter()
            .map(|n| bias.get(n).copied().unwrap_or(0.0))
            .sum::<f64>()
            / cluster.nodes.len() as f64;
        for node in &cluster.nodes {
            let cur = bias.get(node).copied().unwrap_or(0.0);
            let blended = cur * (1.0 - affinity) + centroid * affinity;
            bias.insert(node.clone(), blended);
        }
    }

    bias
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::schema::{AnnotatedFlow, Cluster, FlowKind};

    fn ann() -> SemanticAnnotations {
        SemanticAnnotations {
            flows: vec![
                AnnotatedFlow {
                    id: "happy".into(),
                    kind: FlowKind::Primary,
                    nodes: vec!["A".into(), "B".into(), "C".into()],
                },
                AnnotatedFlow {
                    id: "err".into(),
                    kind: FlowKind::Exception,
                    nodes: vec!["B".into(), "Fix".into()],
                },
                AnnotatedFlow {
                    id: "esc".into(),
                    kind: FlowKind::Escalation,
                    nodes: vec!["Notify".into()],
                },
            ],
            clusters: vec![],
            roles: Default::default(),
            costs: Default::default(),
            times: Default::default(),
        }
    }

    #[test]
    fn primary_nodes_go_to_centerline() {
        let b = compute_row_bias(&ann());
        assert_eq!(b.get("A"), Some(&0.0));
        assert_eq!(b.get("C"), Some(&0.0));
    }

    #[test]
    fn exception_beats_primary_on_shared_node() {
        // B appears in both flows; exception has higher priority.
        let b = compute_row_bias(&ann());
        assert_eq!(b.get("B"), Some(&FlowKind::Exception.target_row()));
        assert_eq!(b.get("Fix"), Some(&FlowKind::Exception.target_row()));
    }

    #[test]
    fn escalation_pulls_above_centerline() {
        let b = compute_row_bias(&ann());
        assert!(*b.get("Notify").unwrap() < 0.0);
    }

    #[test]
    fn unannotated_nodes_are_absent_so_caller_falls_back() {
        let b = compute_row_bias(&ann());
        assert!(!b.contains_key("SomeOtherNode"));
    }

    #[test]
    fn cluster_pulls_members_toward_centroid() {
        // Two nodes: one on centerline (0), one on exception band (+2).
        // With affinity 1.0 they should collapse to their mean (+1).
        let a = SemanticAnnotations {
            flows: vec![
                AnnotatedFlow {
                    id: "p".into(),
                    kind: FlowKind::Primary,
                    nodes: vec!["N1".into()],
                },
                AnnotatedFlow {
                    id: "e".into(),
                    kind: FlowKind::Exception,
                    nodes: vec!["N2".into()],
                },
            ],
            clusters: vec![Cluster {
                id: "c".into(),
                nodes: vec!["N1".into(), "N2".into()],
                affinity: 1.0,
            }],
            roles: Default::default(),
            costs: Default::default(),
            times: Default::default(),
        };
        let b = compute_row_bias(&a);
        let centroid = (0.0 + FlowKind::Exception.target_row()) / 2.0;
        assert!((b["N1"] - centroid).abs() < 1e-9);
        assert!((b["N2"] - centroid).abs() < 1e-9);
    }

    #[test]
    fn zero_affinity_cluster_is_noop() {
        let a = SemanticAnnotations {
            flows: vec![AnnotatedFlow {
                id: "p".into(),
                kind: FlowKind::Primary,
                nodes: vec!["N1".into(), "N2".into()],
            }],
            clusters: vec![Cluster {
                id: "c".into(),
                nodes: vec!["N1".into(), "N2".into()],
                affinity: 0.0,
            }],
            roles: Default::default(),
            costs: Default::default(),
            times: Default::default(),
        };
        let b = compute_row_bias(&a);
        assert_eq!(b["N1"], 0.0);
        assert_eq!(b["N2"], 0.0);
    }
}
