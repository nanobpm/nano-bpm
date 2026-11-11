// The example cards' thumbnails are drawn from each demo flow's compiled BPMN DI
// (website/bpmn-thumb.mjs): one SVG mark per diagram shape, one line per flow.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { diagramThumbSvg, parseDiagram } from "./bpmn-thumb.mjs";
import { DEMO_FLOWS } from "./demo-flows.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const bpmn = (id) => readFileSync(join(here, "demo", "src", `${id}.bpmn`), "utf8");
const count = (s, re) => (s.match(re) ?? []).length;

for (const f of DEMO_FLOWS) {
  test(`${f.id}: the thumbnail draws every shape and flow of the compiled diagram`, () => {
    const xml = bpmn(f.id);
    const { shapes, edges } = parseDiagram(xml);
    const flows = count(xml, /<bpmn:sequenceFlow\b/g);
    assert.equal(edges.length, flows, "one DI edge per sequence flow");
    const svg = diagramThumbSvg(xml);
    assert.equal(count(svg, /<polyline /g), flows);
    assert.equal(count(svg, /<(rect|circle|polygon) /g), shapes.length);
    assert.match(svg, /aria-hidden="true"/);
  });
}

test("a BPMN without DI is rejected, not drawn blank", () => {
  assert.throws(() => diagramThumbSvg('<bpmn:process id="p"><bpmn:startEvent id="s"/></bpmn:process>'), /no DI shapes/);
});
