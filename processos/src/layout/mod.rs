//! **Semantic BPMN layout** — an experimental replacement for the standard
//! left-to-right auto-layout when the LLM that authored (or classified) the
//! model can tell us which nodes belong to the happy path, which handle
//! errors, which escalate, and which compensate.
//!
//! ## Pipeline
//! ```text
//!   BPMN xml + SemanticAnnotations
//!         │
//!         ▼
//!   parse (nanobpmn_engine_core::bpmn::parse_bpmn)
//!         │
//!         ▼
//!   compute_row_bias  ← the semantic contribution lives here
//!         │
//!         ▼
//!   definition_to_xml_with_row_bias
//!     (existing Sugiyama rank + orthogonal routing +
//!      DI emit, biased by the row map)
//!         │
//!         ├──► BPMN xml with new DI
//!         └──► render_debug_svg  (band overlays, cluster hulls, colour-coded nodes)
//! ```
//!
//! ## Why this shape
//! Processos already ships a Sugiyama-ish auto-layout with orthogonal edge
//! routing (see [`crate::bpmn_model::append_diagram`]). The v0 semantic layer
//! is a *y-row bias* — a `node_id -> preferred_row` map that overrides the
//! predecessor-mean heuristic *only for the nodes the LLM annotated*. Every
//! other layout concern (x-ranking, edge routing, label placement) stays under
//! the existing solver.
//!
//! This deliberately keeps the semantic surface tiny (see [`schema`]) so the
//! LLM iteration loop is fast: change the prompt → regenerate annotations →
//! re-run layout → eyeball the debug SVG.

pub mod annotate;
pub mod conformance;
pub mod debug_svg;
pub mod field;
pub mod gates;
pub mod geom;
pub mod guarded;
pub mod polish;
pub mod schema;
pub mod solver;

use std::collections::BTreeMap;

use nanobpmn_engine_core::bpmn::parse_bpmn;
#[allow(unused_imports)] // re-exported public API
pub use schema::{AnnotatedFlow, Cluster, Cost, FlowKind, Role, SemanticAnnotations, Time};

use crate::bpmn_model::definition_to_xml_with_row_bias;

/// Which layout solver to use. `RowBias` is the deterministic
/// Sugiyama-adjacent row-bias projection (production default). `Field` is
/// the experimental unified charged-particle simulation — same semantic
/// annotations, very different aesthetics. See [`field`] for the physics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Solver {
    RowBias,
    Field,
}

impl Solver {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "rowbias" | "row-bias" | "row_bias" => Ok(Solver::RowBias),
            "field" | "physics" => Ok(Solver::Field),
            other => Err(format!(
                "unknown solver '{other}'; expected 'rowbias' or 'field'"
            )),
        }
    }
}

/// End-to-end using the default (row-bias) solver.
#[allow(dead_code)] // convenience wrapper for library consumers; CLI uses layout_with
pub fn layout(xml: &str, ann: &SemanticAnnotations) -> Result<LayoutOutput, String> {
    layout_with(xml, ann, Solver::RowBias)
}

/// End-to-end with an explicitly chosen solver. Output shape is identical
/// regardless of solver.
pub fn layout_with(
    xml: &str,
    ann: &SemanticAnnotations,
    solver: Solver,
) -> Result<LayoutOutput, String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("parse_bpmn: {e:?}"))?;
    // parse_bpmn returns a Vec<ProcessDefinition>; v0 handles the first one.
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "no process definition in BPMN document".to_string())?;
    let (out_xml, diagnostics, node_forces) = match solver {
        Solver::RowBias => {
            let bias = solver::compute_row_bias(ann);
            let xml =
                definition_to_xml_with_row_bias(&def, &std::collections::BTreeMap::new(), &bias);
            (xml, None, None)
        }
        Solver::Field => {
            let field_out = field::simulate(&def, ann);
            let xml = emit_bpmn_from_field(&def, &field_out);
            (
                xml,
                Some(field_out.diagnostics),
                Some(field_out.node_forces),
            )
        }
    };
    let svg = match &node_forces {
        Some(forces) => debug_svg::render_debug_svg_with_forces(&out_xml, ann, Some(forces)),
        None => debug_svg::render_debug_svg(&out_xml, ann),
    };
    Ok(LayoutOutput {
        bpmn_xml: out_xml,
        debug_svg: svg,
        field_diagnostics: diagnostics,
        node_forces,
    })
}

/// Run *both* solvers on the same input and stitch their debug SVGs into a
/// single side-by-side document — handy for eyeballing which physics choice
/// wins on a given fixture.
pub fn layout_side_by_side(xml: &str, ann: &SemanticAnnotations) -> Result<String, String> {
    let rb = layout_with(xml, ann, Solver::RowBias)?;
    let fs = layout_with(xml, ann, Solver::Field)?;
    Ok(debug_svg::stitch_side_by_side(
        &rb.debug_svg,
        "rowbias",
        &fs.debug_svg,
        "field (Fromme)",
    ))
}

/// Convert a settled field simulation into a BPMN XML document. Reuses the
/// existing serializer for the process body (so we get a full, valid BPMN
/// with sequence-flow declarations Camunda Modeler etc. can resolve) and
/// swaps its auto-generated `<bpmndi:BPMNDiagram>` for one built from the
/// field output. Edge waypoints are written *verbatim* — no
/// post-orthogonalisation, since the point of the emergent-orthogonality
/// experiment is to see what actually settles rather than snap it to a grid.
fn emit_bpmn_from_field(
    def: &nanobpmn_engine_core::ProcessDefinition,
    out: &field::FieldOutput,
) -> String {
    use std::fmt::Write as _;

    // 1) Get a complete, valid BPMN via the existing serializer. It emits the
    //    process body (events/tasks/gateways/sequence flows) and an
    //    auto-generated DI we're about to replace.
    let base = definition_to_xml_with_row_bias(
        def,
        &std::collections::BTreeMap::new(),
        &std::collections::BTreeMap::new(),
    );

    // 2) Extract the serializer's `(sourceRef, targetRef) -> flow_id` map so
    //    we can rewrite our synthesized `SRC__TGT` edge ids to match. Without
    //    this the DI would reference edges the model doesn't declare and
    //    Modeler would show `unresolved reference` warnings.
    let mut flow_id_by_pair: BTreeMap<(String, String), String> = BTreeMap::new();
    for chunk in base.split("<bpmn:sequenceFlow ").skip(1) {
        let attrs_end = chunk.find(['>', '/']).unwrap_or(chunk.len());
        let attrs = &chunk[..attrs_end];
        let extract = |key: &str| -> Option<String> {
            let needle = format!("{key}=\"");
            let s = attrs.find(&needle)? + needle.len();
            let e = attrs[s..].find('"')? + s;
            Some(attrs[s..e].to_string())
        };
        if let (Some(id), Some(src), Some(tgt)) =
            (extract("id"), extract("sourceRef"), extract("targetRef"))
        {
            flow_id_by_pair.insert((src, tgt), id);
        }
    }

    // 3) Build the replacement DI block from the field output.
    let mut di = String::new();
    let _ = writeln!(di, "  <bpmndi:BPMNDiagram id=\"BPMNDiagram_1\">");
    let _ = writeln!(
        di,
        "    <bpmndi:BPMNPlane id=\"BPMNPlane_1\" bpmnElement=\"{}\">",
        def.id
    );
    for id in def.elements.keys() {
        if let Some(&(cx, cy)) = out.nodes.get(id) {
            let (w, h) = field_node_dims(&def.elements[id]);
            let _ = writeln!(
                di,
                "      <bpmndi:BPMNShape id=\"{0}_di\" bpmnElement=\"{0}\">\n\
                 \x20       <dc:Bounds x=\"{1:.0}\" y=\"{2:.0}\" width=\"{3:.0}\" height=\"{4:.0}\"/>\n\
                 \x20     </bpmndi:BPMNShape>",
                id,
                cx - w * 0.5,
                cy - h * 0.5,
                w,
                h,
            );
        }
    }
    for (src_id, el) in &def.elements {
        for f in &el.outgoing {
            let synthesized_key = format!("{}__{}", src_id, f.to);
            let Some(waypoints) = out.edges.get(&synthesized_key) else {
                continue;
            };
            let flow_id = flow_id_by_pair
                .get(&(src_id.clone(), f.to.clone()))
                .cloned()
                .unwrap_or(synthesized_key);
            let _ = writeln!(
                di,
                "      <bpmndi:BPMNEdge id=\"{eid}_di\" bpmnElement=\"{eid}\">",
                eid = flow_id
            );
            for &(x, y) in waypoints {
                let _ = writeln!(di, "        <di:waypoint x=\"{x:.0}\" y=\"{y:.0}\"/>");
            }
            let _ = writeln!(di, "      </bpmndi:BPMNEdge>");
        }
    }
    let _ = writeln!(di, "    </bpmndi:BPMNPlane>");
    let _ = writeln!(di, "  </bpmndi:BPMNDiagram>");

    // 4) Splice: replace the base document's `<bpmndi:BPMNDiagram>...</bpmndi:BPMNDiagram>`
    //    block with the freshly-built one. If the base has no diagram (shouldn't
    //    happen but be defensive), append before `</bpmn:definitions>`.
    let open_tag = "<bpmndi:BPMNDiagram";
    let close_tag = "</bpmndi:BPMNDiagram>";
    if let (Some(open), Some(close_rel)) = (base.find(open_tag), base.find(close_tag)) {
        let close = close_rel + close_tag.len();
        // Trim any leading whitespace on the same line as the open tag so we
        // don't leave a ragged indent behind.
        let line_start = base[..open].rfind('\n').map(|i| i + 1).unwrap_or(0);
        let mut result = String::with_capacity(base.len() + di.len());
        result.push_str(&base[..line_start]);
        result.push_str(&di);
        // Skip trailing newline after the closing tag if the DI already ends with one.
        let mut tail_start = close;
        if base.as_bytes().get(tail_start).copied() == Some(b'\n') {
            tail_start += 1;
        }
        result.push_str(&base[tail_start..]);
        result
    } else {
        base.replace("</bpmn:definitions>", &format!("{di}</bpmn:definitions>"))
    }
}

fn field_node_dims(el: &nanobpmn_engine_core::Element) -> (f64, f64) {
    use nanobpmn_engine_core::ElementKind::*;
    match el.kind {
        StartEvent
        | EndEvent
        | TerminateEndEvent
        | IntermediateThrowEvent
        | TimerIntermediateCatchEvent { .. }
        | MessageIntermediateCatchEvent { .. }
        | SignalIntermediateCatchEvent { .. }
        | ConditionalIntermediateCatchEvent { .. }
        | MessageStartEvent { .. }
        | TimerStartEvent { .. }
        | ErrorBoundaryEvent { .. }
        | TimerBoundaryEvent { .. }
        | MessageBoundaryEvent { .. }
        | SignalBoundaryEvent { .. }
        | ConditionalBoundaryEvent { .. }
        | CompensationBoundaryEvent { .. }
        | EscalationThrowEvent { .. }
        | EscalationBoundaryEvent { .. }
        | CompensationThrowEvent => (36.0, 36.0),
        ExclusiveGateway | ParallelGateway | InclusiveGateway => (50.0, 50.0),
        _ => (110.0, 80.0),
    }
}

/// Result of a semantic layout run.
pub struct LayoutOutput {
    /// BPMN 2.0 XML with an auto-generated `<bpmndi:BPMNDiagram>` reflecting
    /// the semantic bands.
    pub bpmn_xml: String,
    /// A standalone SVG debug view — band backgrounds, cluster hulls, and
    /// nodes coloured by their annotated flow kind. Never embedded in the
    /// BPMN document; write to a sidecar file for inspection.
    pub debug_svg: String,
    /// Present only when the field solver was used — steps to convergence,
    /// pinned oscillators, final kinetic energy. Handy for debugging the sim
    /// without instrumenting it.
    pub field_diagnostics: Option<field::SimDiagnostics>,
    /// Present only when the field solver was used — the last-step net force
    /// vector per node. Sourced from [`field::FieldOutput::node_forces`].
    #[allow(dead_code)] // exposed for library consumers; not read by CLI directly
    pub node_forces: Option<std::collections::BTreeMap<String, (f64, f64)>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end smoke: give the pipeline a trivial 3-node BPMN and confirm
    /// the annotated exception node lands *below* its (primary) predecessor
    /// in the emitted DI — the whole point of the feature.
    #[test]
    fn exception_lands_below_primary_predecessor() {
        let bpmn = include_str!("../../fixtures/layout/tiny.bpmn");
        let ann_json = include_str!("../../fixtures/layout/tiny.annotations.json");
        let ann: SemanticAnnotations = serde_json::from_str(ann_json).expect("annotations parse");
        let out = layout(bpmn, &ann).expect("layout runs");

        let y_of = |id: &str| -> f64 {
            let needle = format!("bpmnElement=\"{id}\"");
            let s = out.bpmn_xml.find(&needle).expect("shape present");
            let after = &out.bpmn_xml[s..];
            let ys = after.find("y=\"").expect("y attr") + 3;
            let ye = after[ys..].find('"').expect("y close") + ys;
            after[ys..ye].parse().expect("y numeric")
        };

        let happy = y_of("TaskB");
        let handle = y_of("HandleError");
        assert!(
            handle > happy + 50.0,
            "exception node HandleError (y={handle}) should drop below happy-path TaskB (y={happy})"
        );
    }
}
