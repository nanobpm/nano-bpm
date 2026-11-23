import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DocumentLoadGate, gatedImport } from "./documentLoadGate.ts";

// Issue #1048 wiring regression: reproduces the editor/workspace race
// deterministically. The editor's serial load queue is held on the initial
// blank load; the workspace delivers the document BEFORE onReady; then the
// blank load finishes, onReady fires and the readiness effect re-delivers the
// same document. The document must be imported exactly once.

function serialQueue<I>(instance: I) {
  let chain: Promise<unknown> = Promise.resolve();
  return (loader: (i: I) => Promise<unknown>): Promise<void> => {
    const run = chain.then(() => loader(instance)).then(() => {});
    chain = run.catch(() => {});
    return run;
  };
}

test("content delivered before onReady, then re-delivered on readiness, imports once", async () => {
  const editor = {};
  const gate = new DocumentLoadGate<object>();
  const runLoad = serialQueue(editor);
  let releaseBlank!: () => void;
  const held = new Promise<void>((r) => (releaseBlank = r));
  const blank = runLoad(() => held);
  let imports = 0;
  const importDoc = async () => {
    imports += 1;
  };
  const first = gatedImport(gate, editor, "<a/>", runLoad, importDoc);
  releaseBlank();
  await blank; // onReady → the workspace's readiness effect re-runs:
  const rerun = gatedImport(gate, editor, "<a/>", runLoad, importDoc);
  await Promise.all([first, rerun]);
  assert.equal(imports, 1);
});

test("a duplicate delivery shares the original import's failure", async () => {
  const editor = {};
  const gate = new DocumentLoadGate<object>();
  const runLoad = serialQueue(editor);
  const boom = new Error("bad xml");
  const importDoc = async () => {
    throw boom;
  };
  const a = gatedImport(gate, editor, "<a/>", runLoad, importDoc);
  const b = gatedImport(gate, editor, "<a/>", runLoad, importDoc);
  await assert.rejects(a, boom);
  await assert.rejects(b, boom);
  // The failure re-armed the gate: a later delivery imports again.
  let retried = false;
  await gatedImport(gate, editor, "<a/>", runLoad, async () => {
    retried = true;
  });
  assert.ok(retried);
});

// Structural guard: every editor's import path must go through the shared
// gatedImport wiring (removing it from any editor re-opens the race).
const here = dirname(fileURLToPath(import.meta.url));
for (const [file, method] of [
  ["BpmnModeler.tsx", "importXml"],
  ["DmnModeler.tsx", "importXml"],
  ["FormEditor.tsx", "importSchema"],
] as const) {
  test(`${file} ${method} delegates to gatedImport`, () => {
    const src = readFileSync(join(here, "../components", file), "utf8");
    const body = src.slice(src.indexOf(`      ${method}(`));
    const end = body.indexOf("\n      },");
    assert.match(
      body.slice(0, end),
      /return gatedImport\(\s*loadGateRef\.current,/,
    );
  });
}
