//! **Layout geometry extraction** — pulls a typed shape/edge/kind view out of
//! a BPMN 2.0 XML document with a `<bpmndi:BPMNDiagram>` block.
//!
//! Every metric in [`crate::layout::conformance`], every correctness gate in
//! [`crate::layout::gates`], and the polish pass in
//! [`crate::layout::polish`] consume a [`Geom`] rather than raw XML: that
//! way we parse once and only once. The parser reuses the same tag-hunt
//! strategy as [`crate::layout::debug_svg`] — a proper XML DOM would be more
//! robust, but the DI we emit is very regular and the metrics are advisory,
//! so a tolerant scanner is fine (and dependency-free).
//!
//! The element **kind** (`task` / `event` / `gateway` / `subProcess` /
//! `boundaryEvent` / `pool` / `lane`) is discovered by scanning the process
//! body tags: the conformance rules and correctness gates need this because
//! e.g. containers legitimately enclose leaves (so they don't count as node
//! overlap) and only leaves matter for the `canonicalSizing` rule.

use std::collections::BTreeMap;

/// A single shape lifted from `<bpmndi:BPMNShape>` — top-left origin, width
/// and height in BPMN coordinate units. Coordinates are `f64` because the
/// polish pass and centroid calculations need sub-pixel arithmetic.
#[derive(Debug, Clone)]
pub struct GeomNode {
    pub id: String,
    pub kind: NodeKind,
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
}

impl GeomNode {
    #[allow(dead_code)] // symmetric with cy(), exposed for future callers
    pub fn cx(&self) -> f64 {
        self.x + self.w * 0.5
    }
    pub fn cy(&self) -> f64 {
        self.y + self.h * 0.5
    }
}

/// A single sequence-flow / message-flow edge lifted from `<bpmndi:BPMNEdge>`.
#[derive(Debug, Clone)]
pub struct GeomEdge {
    pub id: String,
    pub source: String,
    pub target: String,
    /// Ordered list of `(x, y)` polyline vertices.
    pub waypoints: Vec<(f64, f64)>,
    /// True when the underlying BPMN tag was `<bpmn:sequenceFlow>` — every
    /// conformance rule that looks at *directed* flow reasoning restricts
    /// itself to sequence flows (message flows and associations are not
    /// expected to obey L→R or straight-happy-path constraints).
    pub is_sequence_flow: bool,
}

/// Coarse BPMN element classification — matches the buckets used by the
/// bake-off's `NON_OVERLAP_KINDS`, `CANON`, `MOVABLE` sets. Anything unknown
/// falls into `Other` and is ignored by the geometry-scoped metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeKind {
    Task,
    Event,
    BoundaryEvent,
    Gateway,
    SubProcess,
    Pool,
    Lane,
    Other,
}

impl NodeKind {
    /// Container kinds legitimately enclose leaves — used by
    /// `overlapArea` and the artifact-clearance rule.
    #[allow(dead_code)]
    pub fn is_container(self) -> bool {
        matches!(self, NodeKind::Pool | NodeKind::Lane | NodeKind::SubProcess)
    }
    /// Nodes that "should never overlap another node" per bake-off
    /// `NON_OVERLAP_KINDS`. Excludes containers, boundary events (dock on
    /// host border by design), and artifacts (n/a for Nano today).
    pub fn is_leaf(self) -> bool {
        matches!(self, NodeKind::Task | NodeKind::Event | NodeKind::Gateway)
    }
    /// Canonical `(width, height)` for the `canonicalSizing` rule.
    /// `None` for kinds without a canonical BPMN size (containers).
    pub fn canonical_size(self) -> Option<(f64, f64)> {
        match self {
            NodeKind::Task => Some((100.0, 80.0)),
            NodeKind::Event | NodeKind::BoundaryEvent => Some((36.0, 36.0)),
            NodeKind::Gateway => Some((50.0, 50.0)),
            _ => None,
        }
    }
}

/// A parsed geometric view of a BPMN document. `bounds` is the min/max
/// enclosing rectangle across all shapes + edge waypoints.
#[derive(Debug, Clone, Default)]
pub struct Geom {
    pub nodes: Vec<GeomNode>,
    pub edges: Vec<GeomEdge>,
    pub by_id: BTreeMap<String, usize>,
    #[allow(dead_code)] // exposed for future callers (report writer, refinement passes)
    pub bounds: Bounds,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Bounds {
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
}

impl Geom {
    /// Lookup a shape by its BPMN element id.
    pub fn node(&self, id: &str) -> Option<&GeomNode> {
        self.by_id.get(id).and_then(|&i| self.nodes.get(i))
    }
    /// Leaf shapes only (nothing that legitimately encloses other nodes).
    pub fn leaves(&self) -> impl Iterator<Item = &GeomNode> {
        self.nodes.iter().filter(|n| n.kind.is_leaf())
    }
}

/// Parse a BPMN XML document into a [`Geom`]. Missing / malformed DI yields
/// an empty result — the metrics degrade gracefully to zero rather than
/// erroring out (the score endpoint reports the metric as unavailable).
pub fn parse(xml: &str) -> Geom {
    let kinds = parse_element_kinds(xml);
    let mut nodes: Vec<GeomNode> = Vec::new();

    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmndi:BPMNShape") {
        let s = cursor + rel;
        let close = xml[s..].find("</bpmndi:BPMNShape>").map(|k| s + k);
        let self_close = xml[s..]
            .find("/>")
            .map(|k| s + k)
            .filter(|&k| close.is_none_or(|c| k < c));
        let block = match (close, self_close) {
            (Some(c), _) => &xml[s..c],
            (None, Some(sc)) => &xml[s..sc],
            (None, None) => &xml[s..],
        };
        if let (Some(id), Some((x, y, w, h))) = (attr(block, "bpmnElement"), parse_bounds(block)) {
            let kind = kinds.get(&id).copied().unwrap_or(NodeKind::Other);
            nodes.push(GeomNode {
                id,
                kind,
                x,
                y,
                w,
                h,
            });
        }
        cursor = close.unwrap_or_else(|| self_close.unwrap_or(xml.len())) + 1;
        if cursor >= xml.len() {
            break;
        }
    }

    let mut edges: Vec<GeomEdge> = Vec::new();
    let refs = parse_flow_refs(xml);
    let seq_ids = parse_sequence_flow_ids(xml);

    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmndi:BPMNEdge") {
        let s = cursor + rel;
        let close = xml[s..].find("</bpmndi:BPMNEdge>").map(|k| s + k);
        let block = match close {
            Some(c) => &xml[s..c],
            None => &xml[s..],
        };
        let Some(id) = attr(block, "bpmnElement") else {
            cursor = close.unwrap_or(xml.len()) + 1;
            continue;
        };
        let mut waypoints: Vec<(f64, f64)> = Vec::new();
        let mut inner = 0;
        while let Some(k) = block[inner..].find("<di:waypoint") {
            let ws = inner + k;
            let we = block[ws..]
                .find("/>")
                .map(|i| ws + i)
                .unwrap_or(block.len());
            let wp = &block[ws..we];
            if let (Some(x), Some(y)) = (
                attr(wp, "x").and_then(|v| v.parse::<f64>().ok()),
                attr(wp, "y").and_then(|v| v.parse::<f64>().ok()),
            ) {
                waypoints.push((x, y));
            }
            inner = we + 2;
        }
        let (source, target) = refs.get(&id).cloned().unwrap_or_default();
        edges.push(GeomEdge {
            id: id.clone(),
            source,
            target,
            waypoints,
            is_sequence_flow: seq_ids.contains(&id),
        });
        cursor = close.unwrap_or(xml.len()) + 1;
        if cursor >= xml.len() {
            break;
        }
    }

    let by_id: BTreeMap<String, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id.clone(), i))
        .collect();
    let bounds = compute_bounds(&nodes, &edges);
    Geom {
        nodes,
        edges,
        by_id,
        bounds,
    }
}

fn compute_bounds(nodes: &[GeomNode], edges: &[GeomEdge]) -> Bounds {
    let mut lo_x = f64::INFINITY;
    let mut lo_y = f64::INFINITY;
    let mut hi_x = f64::NEG_INFINITY;
    let mut hi_y = f64::NEG_INFINITY;
    for n in nodes {
        lo_x = lo_x.min(n.x);
        lo_y = lo_y.min(n.y);
        hi_x = hi_x.max(n.x + n.w);
        hi_y = hi_y.max(n.y + n.h);
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
        return Bounds::default();
    }
    Bounds {
        x: lo_x,
        y: lo_y,
        w: hi_x - lo_x,
        h: hi_y - lo_y,
    }
}

/// Scan the process body for `<bpmn:{kind} id="…">` opening tags so every
/// shape's bpmnElement can be tagged with a coarse [`NodeKind`]. We only
/// need the leading prefix of the tag name (an `event`-suffixed tag → Event,
/// `gateway`-suffixed → Gateway, etc.) — this keeps the scanner independent
/// of the specific event / gateway variant (start / end / exclusive / …).
fn parse_element_kinds(xml: &str) -> BTreeMap<String, NodeKind> {
    let mut out = BTreeMap::new();
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmn:") {
        let s = cursor + rel;
        let after = &xml[s + 6..]; // skip "<bpmn:"
        let name_end = after
            .find(|c: char| c.is_whitespace() || c == '>' || c == '/')
            .unwrap_or(after.len());
        let tag = &after[..name_end];
        // Move cursor past the opening angle bracket regardless of match.
        let close = xml[s..].find('>').map(|k| s + k + 1).unwrap_or(xml.len());
        cursor = close;
        let kind = classify_tag(tag);
        if !matches!(kind, NodeKind::Other) {
            let block = &xml[s..close];
            if let Some(id) = attr(block, "id") {
                out.insert(id, kind);
            }
        }
    }
    out
}

fn classify_tag(tag: &str) -> NodeKind {
    // Order matters: boundaryEvent must be checked BEFORE the generic "Event"
    // suffix so a boundary event is not mis-tagged as a plain event.
    if tag.eq_ignore_ascii_case("boundaryEvent") {
        NodeKind::BoundaryEvent
    } else if tag.eq_ignore_ascii_case("subProcess")
        || tag.eq_ignore_ascii_case("adHocSubProcess")
        || tag.eq_ignore_ascii_case("transaction")
        || tag.eq_ignore_ascii_case("callActivity")
    {
        NodeKind::SubProcess
    } else if tag.eq_ignore_ascii_case("participant") || tag.eq_ignore_ascii_case("collaboration") {
        NodeKind::Pool
    } else if tag.eq_ignore_ascii_case("lane") {
        NodeKind::Lane
    } else if tag.ends_with("Event") || tag.ends_with("event") {
        NodeKind::Event
    } else if tag.ends_with("Gateway") || tag.ends_with("gateway") {
        NodeKind::Gateway
    } else if tag.ends_with("Task") || tag.ends_with("task") {
        NodeKind::Task
    } else {
        NodeKind::Other
    }
}

fn parse_flow_refs(xml: &str) -> BTreeMap<String, (String, String)> {
    let mut out = BTreeMap::new();
    for candidate in [
        "<bpmn:sequenceFlow",
        "<bpmn:messageFlow",
        "<bpmn:association",
    ] {
        let mut cursor = 0;
        while let Some(rel) = xml[cursor..].find(candidate) {
            let s = cursor + rel;
            let close = xml[s..].find('>').map(|k| s + k).unwrap_or(xml.len());
            let block = &xml[s..close];
            if let (Some(id), Some(src), Some(tgt)) = (
                attr(block, "id"),
                attr(block, "sourceRef"),
                attr(block, "targetRef"),
            ) {
                out.insert(id, (src, tgt));
            }
            cursor = close + 1;
            if cursor >= xml.len() {
                break;
            }
        }
    }
    out
}

fn parse_sequence_flow_ids(xml: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<bpmn:sequenceFlow") {
        let s = cursor + rel;
        let close = xml[s..].find('>').map(|k| s + k).unwrap_or(xml.len());
        let block = &xml[s..close];
        if let Some(id) = attr(block, "id") {
            out.insert(id);
        }
        cursor = close + 1;
        if cursor >= xml.len() {
            break;
        }
    }
    out
}

fn parse_bounds(block: &str) -> Option<(f64, f64, f64, f64)> {
    let bs = block.find("<dc:Bounds")?;
    let be = block[bs..]
        .find("/>")
        .map(|i| bs + i)
        .unwrap_or(block.len());
    let span = &block[bs..be];
    Some((
        attr(span, "x")?.parse().ok()?,
        attr(span, "y")?.parse().ok()?,
        attr(span, "width")?.parse().ok()?,
        attr(span, "height")?.parse().ok()?,
    ))
}

fn attr(span: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=\"");
    let s = span.find(&needle)? + needle.len();
    let e = span[s..].find('"').map(|k| s + k)?;
    Some(span[s..e].to_string())
}

/// Axis-aligned rectangle overlap area between two nodes.
pub fn rect_overlap_area(a: &GeomNode, b: &GeomNode) -> f64 {
    let ox = (a.x + a.w).min(b.x + b.w) - a.x.max(b.x);
    let oy = (a.y + a.h).min(b.y + b.h) - a.y.max(b.y);
    if ox <= 0.0 || oy <= 0.0 {
        0.0
    } else {
        ox * oy
    }
}

/// True when segment `p1→p2` crosses (or is enclosed by) rect `r`.
pub fn segment_intersects_rect(p1: (f64, f64), p2: (f64, f64), r: &GeomNode) -> bool {
    let inside = |p: (f64, f64)| p.0 >= r.x && p.0 <= r.x + r.w && p.1 >= r.y && p.1 <= r.y + r.h;
    if inside(p1) || inside(p2) {
        return true;
    }
    let c1 = (r.x, r.y);
    let c2 = (r.x + r.w, r.y);
    let c3 = (r.x + r.w, r.y + r.h);
    let c4 = (r.x, r.y + r.h);
    proper_intersect(p1, p2, c1, c2)
        || proper_intersect(p1, p2, c2, c3)
        || proper_intersect(p1, p2, c3, c4)
        || proper_intersect(p1, p2, c4, c1)
}

fn cross(o: (f64, f64), a: (f64, f64), b: (f64, f64)) -> f64 {
    (a.0 - o.0) * (b.1 - o.1) - (a.1 - o.1) * (b.0 - o.0)
}

/// Cheap proper-intersection test — good enough for the label-clearance rule.
pub fn proper_intersect(a: (f64, f64), b: (f64, f64), c: (f64, f64), d: (f64, f64)) -> bool {
    let d1 = cross(c, d, a);
    let d2 = cross(c, d, b);
    let d3 = cross(a, b, c);
    let d4 = cross(a, b, d);
    ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    const XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
  xmlns:di="http://www.omg.org/spec/DD/20100524/DI">
  <bpmn:process id="P">
    <bpmn:startEvent id="Start"/>
    <bpmn:serviceTask id="TaskA"/>
    <bpmn:exclusiveGateway id="G1"/>
    <bpmn:endEvent id="End"/>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="TaskA"/>
    <bpmn:sequenceFlow id="F2" sourceRef="TaskA" targetRef="G1"/>
    <bpmn:sequenceFlow id="F3" sourceRef="G1"    targetRef="End"/>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="D">
    <bpmndi:BPMNPlane id="Pl" bpmnElement="P">
      <bpmndi:BPMNShape id="Start_di" bpmnElement="Start">
        <dc:Bounds x="100" y="100" width="36" height="36"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="TaskA_di" bpmnElement="TaskA">
        <dc:Bounds x="200" y="80" width="100" height="80"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="G1_di" bpmnElement="G1">
        <dc:Bounds x="360" y="93" width="50" height="50"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_di" bpmnElement="End">
        <dc:Bounds x="460" y="100" width="36" height="36"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="F1_di" bpmnElement="F1">
        <di:waypoint x="136" y="118"/><di:waypoint x="200" y="120"/>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="F2_di" bpmnElement="F2">
        <di:waypoint x="300" y="120"/><di:waypoint x="360" y="118"/>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="F3_di" bpmnElement="F3">
        <di:waypoint x="410" y="118"/><di:waypoint x="460" y="118"/>
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>"#;

    #[test]
    fn parses_shapes_and_edges_with_kinds() {
        let g = parse(XML);
        assert_eq!(g.nodes.len(), 4);
        assert_eq!(g.edges.len(), 3);
        assert_eq!(g.node("Start").unwrap().kind, NodeKind::Event);
        assert_eq!(g.node("TaskA").unwrap().kind, NodeKind::Task);
        assert_eq!(g.node("G1").unwrap().kind, NodeKind::Gateway);
        assert!(g.edges.iter().all(|e| e.is_sequence_flow));
        assert_eq!(g.edges[0].source, "Start");
        assert_eq!(g.edges[0].target, "TaskA");
    }

    #[test]
    fn overlap_area_is_zero_for_disjoint_rects() {
        let g = parse(XML);
        let start = g.node("Start").unwrap();
        let task = g.node("TaskA").unwrap();
        assert_eq!(rect_overlap_area(start, task), 0.0);
    }
}
