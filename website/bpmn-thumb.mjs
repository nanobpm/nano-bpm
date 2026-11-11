// A decorative thumbnail of a BPMN diagram, drawn from the diagram's own DI
// (shape bounds + edge waypoints). The home page's example cards use it on the
// compiled demo flows (website/demo/src/<id>.bpmn), so each card's graphic is
// derived from the very model the live demo runs and can never drift from it.

/** Element types by id, from the semantic model (`<bpmn:userTask id="…">` etc.). */
function elementTypes(xml) {
  const types = new Map();
  for (const m of xml.matchAll(/<bpmn:(\w+)\b[^>]*\bid="([^"]+)"/g)) types.set(m[2], m[1]);
  return types;
}

const num = (s) => Number.parseFloat(s);

/** Shapes `{ id, type, x, y, w, h }` and edges `[[x, y], …]` from the DI. */
export function parseDiagram(xml) {
  const types = elementTypes(xml);
  const shapes = [];
  for (const m of xml.matchAll(/<bpmndi:BPMNShape\b[^>]*\bbpmnElement="([^"]+)"[^>]*>([\s\S]*?)<\/bpmndi:BPMNShape>/g)) {
    const b = m[2].match(/<dc:Bounds\b([^>]*)\/>/);
    if (!b) continue;
    const at = (k) => num(b[1].match(new RegExp(`\\b${k}="([^"]+)"`))[1]);
    shapes.push({ id: m[1], type: types.get(m[1]) ?? "unknown", x: at("x"), y: at("y"), w: at("width"), h: at("height") });
  }
  const edges = [];
  for (const m of xml.matchAll(/<bpmndi:BPMNEdge\b[^>]*>([\s\S]*?)<\/bpmndi:BPMNEdge>/g)) {
    const pts = [...m[1].matchAll(/<di:waypoint\b[^>]*\bx="([^"]+)"[^>]*\by="([^"]+)"/g)].map((p) => [num(p[1]), num(p[2])]);
    if (pts.length >= 2) edges.push(pts);
  }
  return { shapes, edges };
}

const kind = (type) =>
  /Event$/.test(type) ? "event" : /Gateway$/.test(type) ? "gateway" : type === "subProcess" ? "container" : "activity";

/** An inline, aria-hidden SVG of the diagram (no labels: it's a picture, not a reader). */
export function diagramThumbSvg(xml, { className = "flow-thumb" } = {}) {
  const { shapes, edges } = parseDiagram(xml);
  if (shapes.length === 0) throw new Error("diagramThumbSvg: the BPMN has no DI shapes");
  const xs = [...shapes.flatMap((s) => [s.x, s.x + s.w]), ...edges.flat().map((p) => p[0])];
  const ys = [...shapes.flatMap((s) => [s.y, s.y + s.h]), ...edges.flat().map((p) => p[1])];
  const pad = 12;
  const x0 = Math.min(...xs) - pad;
  const y0 = Math.min(...ys) - pad;
  const w = Math.max(...xs) + pad - x0;
  const h = Math.max(...ys) + pad - y0;
  const r = (n) => Math.round(n * 10) / 10;
  const parts = [];
  for (const s of shapes.filter((s) => kind(s.type) === "container")) {
    parts.push(`<rect class="t-box" x="${r(s.x)}" y="${r(s.y)}" width="${r(s.w)}" height="${r(s.h)}" rx="10"/>`);
  }
  for (const e of edges) parts.push(`<polyline class="t-flow" points="${e.map(([x, y]) => `${r(x)},${r(y)}`).join(" ")}"/>`);
  for (const s of shapes) {
    const cx = s.x + s.w / 2;
    const cy = s.y + s.h / 2;
    const k = kind(s.type);
    if (k === "event") {
      parts.push(`<circle class="t-event${s.type === "endEvent" ? " t-end" : ""}" cx="${r(cx)}" cy="${r(cy)}" r="${r(s.w / 2)}"/>`);
    } else if (k === "gateway") {
      parts.push(`<polygon class="t-gw" points="${r(cx)},${r(s.y)} ${r(s.x + s.w)},${r(cy)} ${r(cx)},${r(s.y + s.h)} ${r(s.x)},${r(cy)}"/>`);
    } else if (k === "activity") {
      parts.push(`<rect class="t-task t-${s.type}" x="${r(s.x)}" y="${r(s.y)}" width="${r(s.w)}" height="${r(s.h)}" rx="8"/>`);
    }
  }
  return `<svg class="${className}" viewBox="${r(x0)} ${r(y0)} ${r(w)} ${r(h)}" preserveAspectRatio="xMidYMid meet" aria-hidden="true" focusable="false">${parts.join("")}</svg>`;
}
