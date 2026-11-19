//! **Debug SVG renderer** — parses the auto-generated BPMN DI back out of the
//! emitted XML and paints it as an SVG with the semantic overlays that made
//! the difference: band backgrounds (primary / exception / escalation), cluster
//! hulls, and node fills coloured by their annotated flow kind. This is *not* a
//! BPMN renderer — it's a layout-inspection tool.
//!
//! The parser is deliberately regex-lite (string slicing) and only reads what
//! [`crate::bpmn_model::append_diagram`] writes; if the DI format changes those
//! two functions move in lockstep.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use super::schema::{FlowKind, SemanticAnnotations};

const BAND_COLORS: &[(FlowKind, &str)] = &[
    (FlowKind::Escalation, "#fef3c7"),   // amber-50
    (FlowKind::Primary, "#dcfce7"),      // green-100
    (FlowKind::Exception, "#fee2e2"),    // red-100
    (FlowKind::Compensation, "#e0e7ff"), // indigo-100
];
const NODE_FILL: &[(FlowKind, &str)] = &[
    (FlowKind::Primary, "#16a34a"),
    (FlowKind::Exception, "#dc2626"),
    (FlowKind::Escalation, "#d97706"),
    (FlowKind::Compensation, "#4f46e5"),
];
const CLUSTER_STROKE: &str = "#7c3aed";

/// Render a debug SVG for a semantically-laid-out BPMN document.
///
/// # Arguments
/// * `bpmn_xml` — the output of
///   [`crate::bpmn_model::definition_to_xml_with_row_bias`]. The BPMNDiagram
///   section is required (this function panics-free returns an empty SVG if
///   the input has no diagram, so callers can still write the file).
/// * `ann` — the annotations that drove the layout, used to colour and band.
pub fn render_debug_svg(bpmn_xml: &str, ann: &SemanticAnnotations) -> String {
    render_debug_svg_with_forces(bpmn_xml, ann, None)
}

/// Same as [`render_debug_svg`] but overlays a small arrow at each node
/// showing its net force vector from the last simulation step. Only used by
/// the Fromme (field) solver output. `node_forces` maps `node_id -> (fx, fy)`
/// in the same coordinate system as the BPMN DI.
pub fn render_debug_svg_with_forces(
    bpmn_xml: &str,
    ann: &SemanticAnnotations,
    node_forces: Option<&BTreeMap<String, (f64, f64)>>,
) -> String {
    let shapes = parse_shapes(bpmn_xml);
    let edges = parse_edges(bpmn_xml);
    let node_kinds = flatten_flow_kinds(ann);

    // Extents — pad by 60px on all sides to give bands somewhere to bleed out.
    let (min_x, min_y, max_x, max_y) = extents(&shapes, &edges);
    let pad = 60.0;
    let w = (max_x - min_x + 2.0 * pad).max(200.0);
    let h = (max_y - min_y + 2.0 * pad).max(200.0);
    let shift_x = pad - min_x;
    let shift_y = pad - min_y;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w:.0}\" height=\"{h:.0}\" \
         viewBox=\"0 0 {w:.0} {h:.0}\" font-family=\"system-ui, sans-serif\" font-size=\"11\">"
    );
    out.push_str(
        "  <defs>\n    <marker id=\"arrow\" viewBox=\"0 0 10 10\" refX=\"9\" refY=\"5\" \
         markerUnits=\"strokeWidth\" markerWidth=\"6\" markerHeight=\"6\" orient=\"auto\">\n\
         \x20     <path d=\"M0,0 L10,5 L0,10 z\" fill=\"#7c3aed\"/>\n    </marker>\n  </defs>\n",
    );

    // --- Semantic band backgrounds ---------------------------------------
    // Group node y-centres by their kind, then paint a translucent stripe
    // spanning the diagram's horizontal extent. Bands only appear for kinds
    // that actually have members — no visual noise for unused categories.
    let mut band_bounds: HashMap<FlowKind, (f64, f64)> = HashMap::new();
    for (id, s) in &shapes {
        if let Some(&kind) = node_kinds.get(id) {
            let cy = s.y + s.h / 2.0 + shift_y;
            let entry = band_bounds
                .entry(kind)
                .or_insert((f64::INFINITY, f64::NEG_INFINITY));
            entry.0 = entry.0.min(cy);
            entry.1 = entry.1.max(cy);
        }
    }
    for (kind, color) in BAND_COLORS {
        if let Some(&(lo, hi)) = band_bounds.get(kind) {
            let y = lo - 40.0;
            let bh = (hi - lo).max(0.0) + 80.0;
            let _ = writeln!(
                out,
                "  <rect x=\"0\" y=\"{y:.0}\" width=\"{w:.0}\" height=\"{bh:.0}\" fill=\"{color}\" opacity=\"0.5\"/>"
            );
            let label = format!("{kind:?}").to_lowercase();
            let _ = writeln!(
                out,
                "  <text x=\"6\" y=\"{ty:.0}\" fill=\"#374151\" font-weight=\"600\">{label}</text>",
                ty = y + 14.0
            );
        }
    }

    // --- Cluster hulls (axis-aligned bounding boxes for v0) --------------
    for cluster in &ann.clusters {
        let mut lo_x = f64::INFINITY;
        let mut lo_y = f64::INFINITY;
        let mut hi_x = f64::NEG_INFINITY;
        let mut hi_y = f64::NEG_INFINITY;
        let mut any = false;
        for n in &cluster.nodes {
            if let Some(s) = shapes.get(n) {
                lo_x = lo_x.min(s.x + shift_x);
                lo_y = lo_y.min(s.y + shift_y);
                hi_x = hi_x.max(s.x + s.w + shift_x);
                hi_y = hi_y.max(s.y + s.h + shift_y);
                any = true;
            }
        }
        if any {
            let inset = -12.0;
            let _ = writeln!(
                out,
                "  <rect x=\"{x:.0}\" y=\"{y:.0}\" width=\"{ww:.0}\" height=\"{hh:.0}\" \
                 fill=\"none\" stroke=\"{CLUSTER_STROKE}\" stroke-dasharray=\"4 3\" stroke-width=\"1.5\" rx=\"6\"/>",
                x = lo_x + inset,
                y = lo_y + inset,
                ww = hi_x - lo_x - 2.0 * inset,
                hh = hi_y - lo_y - 2.0 * inset,
            );
            let _ = writeln!(
                out,
                "  <text x=\"{x:.0}\" y=\"{y:.0}\" fill=\"{CLUSTER_STROKE}\" font-size=\"10\">{lbl}</text>",
                x = lo_x + inset,
                y = lo_y + inset - 4.0,
                lbl = xml_escape(&cluster.id),
            );
        }
    }

    // --- Edges -----------------------------------------------------------
    for edge in &edges {
        if edge.waypoints.len() < 2 {
            continue;
        }
        let mut d = String::new();
        for (i, (x, y)) in edge.waypoints.iter().enumerate() {
            let cmd = if i == 0 { 'M' } else { 'L' };
            let _ = write!(d, "{cmd}{:.0},{:.0} ", x + shift_x, y + shift_y);
        }
        let _ = writeln!(
            out,
            "  <path d=\"{d}\" fill=\"none\" stroke=\"#374151\" stroke-width=\"1.5\"/>"
        );
    }

    // --- Nodes -----------------------------------------------------------
    for (id, s) in &shapes {
        let fill = node_kinds
            .get(id)
            .and_then(|k| NODE_FILL.iter().find(|(kk, _)| kk == k).map(|(_, c)| *c))
            .unwrap_or("#e5e7eb"); // gray-200 for unannotated
        let _ = writeln!(
            out,
            "  <rect x=\"{x:.0}\" y=\"{y:.0}\" width=\"{ww:.0}\" height=\"{hh:.0}\" rx=\"6\" \
             fill=\"{fill}\" fill-opacity=\"0.85\" stroke=\"#111827\" stroke-width=\"1\"/>",
            x = s.x + shift_x,
            y = s.y + shift_y,
            ww = s.w,
            hh = s.h
        );
        let _ = writeln!(
            out,
            "  <text x=\"{cx:.0}\" y=\"{cy:.0}\" text-anchor=\"middle\" fill=\"#111827\">{id_esc}</text>",
            cx = s.x + s.w / 2.0 + shift_x,
            cy = s.y + s.h / 2.0 + 4.0 + shift_y,
            id_esc = xml_escape(id),
        );
    }

    // --- Force-vector overlay (Fromme diagnostics) ----------------------
    // Draw a small arrow at each node's centre pointing along its last-step
    // net force vector. Magnitude is scaled so the biggest arrow is ~60px.
    if let Some(forces) = node_forces {
        let max_mag = forces
            .values()
            .map(|(fx, fy)| (fx * fx + fy * fy).sqrt())
            .fold(0.0_f64, f64::max)
            .max(1e-6);
        let scale = 60.0 / max_mag;
        for (id, s) in &shapes {
            if let Some(&(fx, fy)) = forces.get(id) {
                let cx = s.x + s.w / 2.0 + shift_x;
                let cy = s.y + s.h / 2.0 + shift_y;
                let ex = cx + fx * scale;
                let ey = cy + fy * scale;
                if (fx * fx + fy * fy).sqrt() > max_mag * 0.02 {
                    let _ = writeln!(
                        out,
                        "  <line x1=\"{cx:.0}\" y1=\"{cy:.0}\" x2=\"{ex:.0}\" y2=\"{ey:.0}\" \
                         stroke=\"#7c3aed\" stroke-width=\"1.5\" marker-end=\"url(#arrow)\" opacity=\"0.85\"/>"
                    );
                }
            }
        }
    }

    out.push_str("</svg>\n");
    out
}

/// Combine two independently-rendered debug SVGs side by side into a single
/// document. Each pane is captioned. Used by
/// [`super::layout_side_by_side`] to compare the row-bias and Fromme solvers
/// on the same fixture.
pub fn stitch_side_by_side(
    left_svg: &str,
    left_label: &str,
    right_svg: &str,
    right_label: &str,
) -> String {
    let (lw, lh) = svg_dims(left_svg);
    let (rw, rh) = svg_dims(right_svg);
    let gap = 40.0;
    let cap_h = 24.0;
    let w = lw + gap + rw;
    let h = lh.max(rh) + cap_h;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w:.0}\" height=\"{h:.0}\" \
         viewBox=\"0 0 {w:.0} {h:.0}\" font-family=\"system-ui, sans-serif\" font-size=\"12\">"
    );
    let _ = writeln!(
        out,
        "  <text x=\"8\" y=\"16\" font-weight=\"700\" fill=\"#111827\">{}</text>",
        xml_escape(left_label)
    );
    let _ = writeln!(
        out,
        "  <text x=\"{lx:.0}\" y=\"16\" font-weight=\"700\" fill=\"#111827\">{lbl}</text>",
        lx = lw + gap + 8.0,
        lbl = xml_escape(right_label)
    );
    let _ = writeln!(out, "  <g transform=\"translate(0,{:.0})\">", cap_h);
    out.push_str(&strip_svg_wrapper(left_svg));
    out.push_str("  </g>\n");
    let _ = writeln!(
        out,
        "  <g transform=\"translate({tx:.0},{ty:.0})\">",
        tx = lw + gap,
        ty = cap_h
    );
    out.push_str(&strip_svg_wrapper(right_svg));
    out.push_str("  </g>\n");
    out.push_str("</svg>\n");
    out
}

fn svg_dims(svg: &str) -> (f64, f64) {
    let w = attr_f64(svg, "width").unwrap_or(800.0);
    let h = attr_f64(svg, "height").unwrap_or(600.0);
    (w, h)
}

fn attr_f64(s: &str, name: &str) -> Option<f64> {
    let needle = format!(" {name}=\"");
    let start = s.find(&needle)? + needle.len();
    let rest = &s[start..];
    let end = rest.find('"')?;
    rest[..end].parse().ok()
}

fn strip_svg_wrapper(svg: &str) -> String {
    // Drop the outer <svg ...> and </svg> tags so the body can be embedded.
    let open_end = svg.find('>').map(|i| i + 1).unwrap_or(0);
    let close_start = svg.rfind("</svg>").unwrap_or(svg.len());
    svg[open_end..close_start].to_string()
}

#[derive(Debug, Clone, Copy)]
struct Shape {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

#[derive(Debug, Clone)]
struct Edge {
    waypoints: Vec<(f64, f64)>,
}

fn parse_shapes(xml: &str) -> HashMap<String, Shape> {
    let mut out = HashMap::new();
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmndi:BPMNShape") {
        let s = cursor + rel;
        let close = xml[s..].find("</bpmndi:BPMNShape>").map(|k| s + k);
        let block = match close {
            Some(e) => &xml[s..e],
            None => &xml[s..],
        };
        if let (Some(id), Some(sh)) = (attr(block, "bpmnElement"), parse_bounds(block)) {
            out.insert(id, sh);
        }
        cursor = close.unwrap_or(xml.len());
    }
    out
}

fn parse_edges(xml: &str) -> Vec<Edge> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmndi:BPMNEdge") {
        let s = cursor + rel;
        let close = xml[s..].find("</bpmndi:BPMNEdge>").map(|k| s + k);
        let block = match close {
            Some(e) => &xml[s..e],
            None => &xml[s..],
        };
        let mut waypoints = Vec::new();
        let mut inner = 0;
        while let Some(k) = block[inner..].find("<di:waypoint") {
            let ws = inner + k;
            let we = block[ws..]
                .find("/>")
                .map(|i| ws + i)
                .unwrap_or(block.len());
            let wp = &block[ws..we];
            if let (Some(x), Some(y)) = (
                attr(wp, "x").and_then(|s| s.parse::<f64>().ok()),
                attr(wp, "y").and_then(|s| s.parse::<f64>().ok()),
            ) {
                waypoints.push((x, y));
            }
            inner = we;
        }
        if !waypoints.is_empty() {
            out.push(Edge { waypoints });
        }
        cursor = close.unwrap_or(xml.len());
    }
    out
}

fn parse_bounds(block: &str) -> Option<Shape> {
    let bs = block.find("<dc:Bounds")?;
    let be = block[bs..]
        .find("/>")
        .map(|i| bs + i)
        .unwrap_or(block.len());
    let span = &block[bs..be];
    Some(Shape {
        x: attr(span, "x")?.parse().ok()?,
        y: attr(span, "y")?.parse().ok()?,
        w: attr(span, "width")?.parse().ok()?,
        h: attr(span, "height")?.parse().ok()?,
    })
}

fn attr(span: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=\"");
    let s = span.find(&needle)? + needle.len();
    let e = span[s..].find('"').map(|k| s + k)?;
    Some(span[s..e].to_string())
}

fn extents(shapes: &HashMap<String, Shape>, edges: &[Edge]) -> (f64, f64, f64, f64) {
    let mut lo_x = f64::INFINITY;
    let mut lo_y = f64::INFINITY;
    let mut hi_x = f64::NEG_INFINITY;
    let mut hi_y = f64::NEG_INFINITY;
    for s in shapes.values() {
        lo_x = lo_x.min(s.x);
        lo_y = lo_y.min(s.y);
        hi_x = hi_x.max(s.x + s.w);
        hi_y = hi_y.max(s.y + s.h);
    }
    for e in edges {
        for &(x, y) in &e.waypoints {
            lo_x = lo_x.min(x);
            lo_y = lo_y.min(y);
            hi_x = hi_x.max(x);
            hi_y = hi_y.max(y);
        }
    }
    if !lo_x.is_finite() {
        return (0.0, 0.0, 100.0, 100.0);
    }
    (lo_x, lo_y, hi_x, hi_y)
}

fn flatten_flow_kinds(ann: &SemanticAnnotations) -> HashMap<String, FlowKind> {
    let mut out: HashMap<String, FlowKind> = HashMap::new();
    for f in &ann.flows {
        for n in &f.nodes {
            let take = out.get(n).is_none_or(|k| f.kind.priority() > k.priority());
            if take {
                out.insert(n.clone(), f.kind);
            }
        }
    }
    out
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
