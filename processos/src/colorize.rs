//! **Flow colorization** — inject BPMN colour attributes into rendered XML
//! based on the flow annotations, so the semantic classification is
//! visible in the diagram.
//!
//! Uses the OMG standard BPMN 2.0 non-normative colour extension
//! (namespace `http://www.omg.org/spec/BPMN/non-normative/color/1.0`,
//! commonly prefixed `color:`). This is what bpmn-js, Camunda Modeler and
//! most BPMN 2.0 tools render out of the box — we get colour without a
//! bespoke rendering layer.
//!
//! The colour pass is intentionally *rendering-only*: it modifies the XML
//! we hand to bpmn-js but never the stored `model.bpmn` on disk. The
//! semantic-annotations sidecar remains the single source of truth for the
//! classification; colours are a presentation layer derived from it.
//!
//! ## Palette
//!
//! Light pastel fills so the diagram stays readable on both light and dark
//! themes, and the coloured shapes still read as BPMN shapes rather than
//! coloured blobs.
//!
//! | Flow kind      | Fill     | Meaning                          |
//! |----------------|----------|----------------------------------|
//! | `primary`      | #d9edf7  | happy path centreline            |
//! | `exception`    | #f7d9d9  | error handling                   |
//! | `escalation`   | #fbf1c7  | out-of-band notifications        |
//! | `compensation` | #ead9f7  | undo / rollback                  |
//!
//! When a node appears in multiple flows the same
//! [`FlowKind::priority`](crate::layout::schema::FlowKind::priority) rule
//! the layout solver uses picks the winner: exception > escalation >
//! compensation > primary. Rationale: if a node is genuinely part of the
//! happy path but is *also* how you handle a specific error, the diagram
//! is more useful with the node coloured for the exception context — the
//! interesting one.

use std::collections::BTreeMap;

use crate::layout::schema::{FlowKind, SemanticAnnotations};

/// The OMG BPMN colour-extension namespace URI.
const COLOR_NS: &str = "http://www.omg.org/spec/BPMN/non-normative/color/1.0";

/// Palette entry for one flow kind. Fill hex is lowercase with `#`; borders
/// are kept default per the operator preference (fill-only, subtle).
pub fn fill_for_kind(kind: FlowKind) -> &'static str {
    match kind {
        FlowKind::Primary => "#d9edf7",      // light blue
        FlowKind::Exception => "#f7d9d9",    // light red
        FlowKind::Escalation => "#fbf1c7",   // light amber
        FlowKind::Compensation => "#ead9f7", // light lavender
    }
}

/// Resolve, for each element id, the winning `FlowKind` after precedence.
/// `None` means the element isn't in any flow — leave it uncoloured.
pub fn winning_kinds(ann: &SemanticAnnotations) -> BTreeMap<String, FlowKind> {
    let mut out: BTreeMap<String, FlowKind> = BTreeMap::new();
    for flow in &ann.flows {
        for node in &flow.nodes {
            let current = out.get(node).copied();
            match current {
                None => {
                    out.insert(node.clone(), flow.kind);
                }
                Some(existing) if flow.kind.priority() > existing.priority() => {
                    out.insert(node.clone(), flow.kind);
                }
                _ => {}
            }
        }
    }
    out
}

/// Inject colour attributes into `xml` on every `<bpmndi:BPMNShape>` whose
/// `bpmnElement` id has a winning flow kind. Idempotent per call:
///
/// * Shapes that already carry a `color:background-color` attribute are
///   left alone — respect any authored colour the LLM or a Modeler tool
///   might have baked in.
/// * The `xmlns:color` namespace declaration is added to
///   `<bpmn:definitions ...>` if missing (once, at the front of the
///   attribute list).
///
/// Returns the XML unchanged if `ann.flows` is empty (fast path — most
/// unannotated models pass through with no allocation).
pub fn colorize_flows(xml: &str, ann: &SemanticAnnotations) -> String {
    if ann.flows.is_empty() {
        return xml.to_string();
    }
    let kinds = winning_kinds(ann);
    if kinds.is_empty() {
        return xml.to_string();
    }
    let with_shapes = inject_shape_fills(xml, &kinds);
    ensure_color_namespace(with_shapes)
}

/// Walk each `<bpmndi:BPMNShape ... bpmnElement="ID" ...>` open tag and
/// splice in a `color:background-color="…"` attribute when we have a
/// winning kind for `ID` — but only if the tag doesn't already carry one.
fn inject_shape_fills(xml: &str, kinds: &BTreeMap<String, FlowKind>) -> String {
    let needle = "<bpmndi:BPMNShape";
    if !xml.contains(needle) {
        return xml.to_string();
    }
    let mut out = String::with_capacity(xml.len() + 64 * kinds.len());
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find(needle) {
        let start = cursor + rel;
        // Copy everything up to and including the tag name.
        out.push_str(&xml[cursor..start + needle.len()]);
        // Find where THIS open tag ends (`>` — self-closing tags end at `/>`).
        let tag_rest_start = start + needle.len();
        let Some(close_rel) = xml[tag_rest_start..].find('>') else {
            // Malformed — pass through the rest and bail.
            out.push_str(&xml[tag_rest_start..]);
            return out;
        };
        let attrs_end = tag_rest_start + close_rel;
        let raw_attrs = &xml[tag_rest_start..attrs_end]; // e.g. ` id="X_di" bpmnElement="X"` or `… /`
                                                         // Self-closing tags end `... />` — split off the trailing `/`
                                                         // so we can splice the color attribute *before* it and preserve
                                                         // the self-close.
        let (attrs, self_close_suffix) = match raw_attrs.strip_suffix('/') {
            Some(rest) => (rest, "/"),
            None => (raw_attrs, ""),
        };
        let colored = if let Some(id) = extract_attr(attrs, "bpmnElement") {
            match kinds.get(id.as_str()) {
                Some(kind) if !attrs.contains("color:background-color=") => {
                    let fill = fill_for_kind(*kind);
                    let mut buf = String::with_capacity(attrs.len() + 48);
                    let trimmed = attrs.trim_end();
                    buf.push_str(trimmed);
                    buf.push(' ');
                    buf.push_str(&format!("color:background-color=\"{fill}\""));
                    if self_close_suffix.is_empty() {
                        // Not self-closing — restore any trailing
                        // whitespace we trimmed so `>` lands cleanly.
                        buf.push_str(&attrs[trimmed.len()..]);
                    } else {
                        buf.push(' ');
                    }
                    buf
                }
                _ => attrs.to_string(),
            }
        } else {
            attrs.to_string()
        };
        out.push_str(&colored);
        out.push_str(self_close_suffix);
        // Copy the closing `>` (and continue after it).
        out.push('>');
        cursor = attrs_end + 1;
    }
    out.push_str(&xml[cursor..]);
    out
}

/// Extract `attr="value"` from an attribute run. Returns the unescaped
/// value if found. Supports single and double quotes; does not attempt
/// XML entity expansion (BPMN ids are ASCII in practice).
fn extract_attr(attrs: &str, attr: &str) -> Option<String> {
    // Find `attr=` at a token boundary (prev char is whitespace or start).
    let key = format!("{attr}=");
    let mut cursor = 0;
    while let Some(rel) = attrs[cursor..].find(&key) {
        let pos = cursor + rel;
        let boundary_ok = pos == 0 || attrs.as_bytes()[pos - 1].is_ascii_whitespace();
        if !boundary_ok {
            cursor = pos + key.len();
            continue;
        }
        let after = pos + key.len();
        let bytes = attrs.as_bytes();
        if after >= bytes.len() {
            return None;
        }
        let quote = bytes[after];
        if quote != b'"' && quote != b'\'' {
            return None;
        }
        let val_start = after + 1;
        let end_rel = attrs[val_start..].find(quote as char)?;
        return Some(attrs[val_start..val_start + end_rel].to_string());
    }
    None
}

/// Ensure the `<bpmn:definitions …>` open tag declares
/// `xmlns:color="…"`. If already present, returns the XML unchanged.
fn ensure_color_namespace(mut xml: String) -> String {
    if xml.contains("xmlns:color=") {
        return xml;
    }
    let Some(defs_pos) = xml.find("<bpmn:definitions") else {
        return xml;
    };
    let tag_rest_start = defs_pos + "<bpmn:definitions".len();
    let Some(close_rel) = xml[tag_rest_start..].find('>') else {
        return xml;
    };
    // Insert the namespace declaration just after the element name — a
    // stable, easy-to-eyeball location.
    let insert_at = tag_rest_start;
    xml.insert_str(insert_at, &format!(" xmlns:color=\"{COLOR_NS}\""));
    // Silence unused-variable warning if the compiler thinks close_rel is
    // unused when the insertion succeeds — it IS used implicitly to
    // verify the tag is well-formed above.
    let _ = close_rel;
    xml
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::layout::schema::{AnnotatedFlow, Cluster, FlowKind, SemanticAnnotations};

    fn ann_from_flows(flows: Vec<AnnotatedFlow>) -> SemanticAnnotations {
        SemanticAnnotations {
            flows,
            clusters: Vec::<Cluster>::new(),
            roles: BTreeMap::new(),
            costs: BTreeMap::new(),
            times: BTreeMap::new(),
        }
    }

    const MODEL: &str = concat!(
        "<bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" ",
        "xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\">\n",
        "  <bpmn:process id=\"p\">\n",
        "    <bpmn:startEvent id=\"Start_1\"/>\n",
        "    <bpmn:task id=\"Task_A\"/>\n",
        "    <bpmn:task id=\"Handle_Err\"/>\n",
        "    <bpmn:endEvent id=\"End_1\"/>\n",
        "  </bpmn:process>\n",
        "  <bpmndi:BPMNDiagram id=\"d\">\n",
        "    <bpmndi:BPMNPlane id=\"plane\" bpmnElement=\"p\">\n",
        "      <bpmndi:BPMNShape id=\"Start_1_di\" bpmnElement=\"Start_1\"/>\n",
        "      <bpmndi:BPMNShape id=\"Task_A_di\" bpmnElement=\"Task_A\"/>\n",
        "      <bpmndi:BPMNShape id=\"Handle_Err_di\" bpmnElement=\"Handle_Err\"/>\n",
        "      <bpmndi:BPMNShape id=\"End_1_di\" bpmnElement=\"End_1\"/>\n",
        "    </bpmndi:BPMNPlane>\n",
        "  </bpmndi:BPMNDiagram>\n",
        "</bpmn:definitions>"
    );

    #[test]
    fn no_flows_passes_through_unchanged() {
        let ann = SemanticAnnotations::default();
        let out = colorize_flows(MODEL, &ann);
        assert_eq!(out, MODEL);
    }

    #[test]
    fn primary_flow_colors_only_its_nodes() {
        let ann = ann_from_flows(vec![AnnotatedFlow {
            id: "happy".into(),
            kind: FlowKind::Primary,
            nodes: vec!["Start_1".into(), "Task_A".into(), "End_1".into()],
        }]);
        let out = colorize_flows(MODEL, &ann);
        // Start, Task_A, End coloured light blue; Handle_Err untouched.
        assert!(
            out.contains(
                "<bpmndi:BPMNShape id=\"Start_1_di\" bpmnElement=\"Start_1\" \
                 color:background-color=\"#d9edf7\""
            ),
            "start not coloured:\n{out}"
        );
        assert!(out.contains(
            "<bpmndi:BPMNShape id=\"Task_A_di\" bpmnElement=\"Task_A\" \
             color:background-color=\"#d9edf7\""
        ));
        assert!(out.contains(
            "<bpmndi:BPMNShape id=\"End_1_di\" bpmnElement=\"End_1\" \
             color:background-color=\"#d9edf7\""
        ));
        // Handle_Err still bare
        assert!(out.contains("<bpmndi:BPMNShape id=\"Handle_Err_di\" bpmnElement=\"Handle_Err\"/>"));
        // Namespace injected
        assert!(out.contains("xmlns:color=\""));
    }

    #[test]
    fn exception_wins_over_primary_when_node_is_in_both() {
        // Task_A appears in both happy (primary) and err (exception) — the
        // exception colour should win, per FlowKind::priority.
        let ann = ann_from_flows(vec![
            AnnotatedFlow {
                id: "happy".into(),
                kind: FlowKind::Primary,
                nodes: vec!["Task_A".into()],
            },
            AnnotatedFlow {
                id: "err".into(),
                kind: FlowKind::Exception,
                nodes: vec!["Task_A".into(), "Handle_Err".into()],
            },
        ]);
        let out = colorize_flows(MODEL, &ann);
        // Task_A gets the exception red, not the primary blue.
        assert!(
            out.contains("bpmnElement=\"Task_A\" color:background-color=\"#f7d9d9\""),
            "task_a not coloured with exception red:\n{out}"
        );
        // No trace of blue on Task_A.
        assert!(!out.contains("bpmnElement=\"Task_A\" color:background-color=\"#d9edf7\""));
    }

    #[test]
    fn precedence_across_all_four_kinds() {
        // A single element in ALL FOUR flows: exception should win.
        let ann = ann_from_flows(vec![
            AnnotatedFlow {
                id: "p".into(),
                kind: FlowKind::Primary,
                nodes: vec!["X".into()],
            },
            AnnotatedFlow {
                id: "c".into(),
                kind: FlowKind::Compensation,
                nodes: vec!["X".into()],
            },
            AnnotatedFlow {
                id: "s".into(),
                kind: FlowKind::Escalation,
                nodes: vec!["X".into()],
            },
            AnnotatedFlow {
                id: "e".into(),
                kind: FlowKind::Exception,
                nodes: vec!["X".into()],
            },
        ]);
        let winners = winning_kinds(&ann);
        assert_eq!(winners.get("X"), Some(&FlowKind::Exception));
    }

    #[test]
    fn existing_color_attribute_is_preserved() {
        // If the input already has color:background-color on a shape, we
        // leave it alone (respect authored colours).
        let with_authored = MODEL.replace(
            "<bpmndi:BPMNShape id=\"Task_A_di\" bpmnElement=\"Task_A\"/>",
            "<bpmndi:BPMNShape id=\"Task_A_di\" bpmnElement=\"Task_A\" \
             color:background-color=\"#123456\"/>",
        );
        let ann = ann_from_flows(vec![AnnotatedFlow {
            id: "happy".into(),
            kind: FlowKind::Primary,
            nodes: vec!["Task_A".into()],
        }]);
        let out = colorize_flows(&with_authored, &ann);
        // Authored colour still present, no duplicate colour attr.
        assert!(out.contains("color:background-color=\"#123456\""));
        assert_eq!(out.matches("color:background-color=").count(), 1);
    }

    #[test]
    fn namespace_declaration_idempotent() {
        let ann = ann_from_flows(vec![AnnotatedFlow {
            id: "happy".into(),
            kind: FlowKind::Primary,
            nodes: vec!["Start_1".into()],
        }]);
        let first = colorize_flows(MODEL, &ann);
        let second = colorize_flows(&first, &ann);
        assert_eq!(first.matches("xmlns:color=").count(), 1);
        assert_eq!(second.matches("xmlns:color=").count(), 1);
    }

    #[test]
    fn empty_flows_short_circuit_returns_input_untouched() {
        let ann = ann_from_flows(vec![]);
        assert_eq!(colorize_flows(MODEL, &ann), MODEL);
    }

    #[test]
    fn extract_attr_handles_quotes_and_boundaries() {
        assert_eq!(
            extract_attr(" id=\"X_di\" bpmnElement=\"Task_A\"", "bpmnElement"),
            Some("Task_A".to_string())
        );
        assert_eq!(
            extract_attr(" bpmnElement='Task_B'", "bpmnElement"),
            Some("Task_B".to_string())
        );
        // Substring match must NOT trigger (e.g. `notbpmnElement=`).
        assert_eq!(extract_attr("notbpmnElement=\"nope\"", "bpmnElement"), None);
    }
}
