// Drift guard for the landing page's flow examples (website/demo-flows.mjs).
// Each example's code (website/snippets/<snippet>) and the run the in-browser demo
// plays (website/demo/src/<id>.bpmn, compiled from that snippet by
// website/scripts/compile-demo-flows.mjs) must stay one flow: a step added to the
// snippet without regenerating the BPMN — or the reverse — fails here.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { readStamp, sourceHash, stampedBpmn } from "./demo-flow-stamp.mjs";
import { DEMO_FLOWS } from "./demo-flows.mjs";
import { buildSite } from "./test-build.mjs";

const read = (rel) => readFileSync(new URL(rel, import.meta.url), "utf8");
const SNIPPETS = fileURLToPath(new URL("./snippets", import.meta.url));

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

  test(`${f.id}: the demo BPMN is stamped with the current code and is unedited`, () => {
    const stamp = readStamp(read(`./demo/src/${f.id}.bpmn`));
    assert.ok(stamp, "no provenance stamp: regenerate with website/scripts/compile-demo-flows.mjs");
    assert.equal(stamp.source, sourceHash(SNIPPETS, f), "snippet changed since the BPMN was compiled: regenerate");
    assert.equal(stamp.bodyHash, stamp.bpmn, "BPMN was edited by hand: regenerate instead");
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

// The stamp guard catches every kind of drift, not just step-name changes: an
// execution-only edit to the code (e.g. a multi-instance outputElement), an edit
// to a dep, and a hand edit to the BPMN all fail it.
test("the provenance stamp detects execution-only code edits, dep edits and BPMN hand edits", () => {
  const dir = mkdtempSync(join(tmpdir(), "demo-stamp-"));
  try {
    const flow = { snippet: "a.ts", deps: ["b.ts"] };
    writeFileSync(join(dir, "a.ts"), 'flow.map("r", { outputElement: "found" });');
    writeFileSync(join(dir, "b.ts"), "export const x = 1;");
    const text = stampedBpmn(flow, sourceHash(dir, flow), "<bpmn:definitions/>\n");
    const ok = readStamp(text);
    assert.equal(ok.source, sourceHash(dir, flow));
    assert.equal(ok.bodyHash, ok.bpmn);

    writeFileSync(join(dir, "a.ts"), 'flow.map("r", { outputElement: "lens" });');
    assert.notEqual(sourceHash(dir, flow), ok.source, "execution-only snippet edit");
    writeFileSync(join(dir, "a.ts"), 'flow.map("r", { outputElement: "found" });');
    writeFileSync(join(dir, "b.ts"), "export const x = 2;");
    assert.notEqual(sourceHash(dir, flow), ok.source, "dep edit");

    const edited = readStamp(text.replace("<bpmn:definitions/>", "<bpmn:definitions id=\"x\"/>"));
    assert.notEqual(edited.bodyHash, edited.bpmn, "BPMN hand edit");
    assert.equal(readStamp("<bpmn:definitions/>"), null, "unstamped BPMN");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
