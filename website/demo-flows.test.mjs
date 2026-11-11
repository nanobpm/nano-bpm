// Drift guard for the landing page's flow examples (website/demo-flows.mjs).
// Each example's code (website/snippets/<snippet>) and the run the in-browser demo
// plays (website/demo/src/<id>.bpmn, compiled from that snippet by
// website/scripts/compile-demo-flows.mjs) must stay one flow: a step added to the
// snippet without regenerating the BPMN — or the reverse — fails here.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { DEMO_FLOWS } from "./demo-flows.mjs";
import { buildSite } from "./test-build.mjs";

const read = (rel) => readFileSync(new URL(rel, import.meta.url), "utf8");

/** Step names declared in a snippet's flow region: run / task / human steps. */
function snippetSteps(src) {
  const region = src.split("//#region flow")[1]?.split("//#endregion flow")[0];
  assert.ok(region, "snippet has no `flow` region");
  return new Set([...region.matchAll(/\.(?:run|task|human)\(\s*"([^"]+)"/g)].map((m) => m[1]));
}

/** Step element ids in compiled BPMN: service and user tasks. */
function bpmnSteps(xml) {
  return new Set([...xml.matchAll(/<bpmn:(?:serviceTask|userTask) id="([^"]+)"/g)].map((m) => m[1]));
}

for (const f of DEMO_FLOWS) {
  test(`${f.id}: the demo BPMN is compiled from snippets/${f.snippet} (same steps)`, () => {
    const xml = read(`./demo/src/${f.id}.bpmn`);
    assert.match(xml, new RegExp(`GENERATED from website/snippets/${f.snippet.replace(".", "\\.")}`));
    assert.match(xml, new RegExp(`<bpmn:process id="${f.id}"`));
    assert.match(xml, /<bpmndi:BPMNDiagram/, "the demo BPMN needs DI to render");
    const fromCode = [...snippetSteps(read(`./snippets/${f.snippet}`))].sort();
    const fromBpmn = [...bpmnSteps(xml)].sort();
    assert.deepEqual(fromBpmn, fromCode, "regenerate: node website/scripts/compile-demo-flows.mjs");
  });

  test(`${f.id}: the browser demo has a scenario for it`, () => {
    assert.match(read("./demo/src/scenarios.ts"), new RegExp(`id: "${f.id}"`));
  });
}

test("the home page shows every example with its code and its live run", () => {
  const html = buildSite()("index.html");
  for (const f of DEMO_FLOWS) {
    assert.ok(html.includes(`id="tab-ex-${f.id}"`), `${f.id}: no example tab`);
    assert.ok(html.includes(`data-src="/demo/?embed=1&amp;scenario=${f.id}"`), `${f.id}: no live run`);
    assert.ok(html.includes(f.summary), `${f.id}: no summary`);
  }
});
