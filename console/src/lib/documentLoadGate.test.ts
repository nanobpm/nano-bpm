import { test } from "node:test";
import assert from "node:assert/strict";
import { DocumentLoadGate } from "./documentLoadGate.ts";

// Issue #1048: ProjectWorkspace's content-import effect re-runs when the lazy
// editor flips `editorReady`. When the file content landed after the modeler
// mounted but before its initial blank load finished, the SAME document was
// imported twice; a selection made between the two imports was wiped by the
// second one (the panel reset to the process root, dropping the "Agent task"
// group). The gate makes delivering one document to one editor instance
// idempotent.

test("a repeated request for the same document on the same instance is skipped", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  assert.equal(gate.shouldLoad(m, "<a/>"), true);
  assert.equal(gate.shouldLoad(m, "<a/>"), false);
});

test("a different document always loads", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  assert.equal(gate.shouldLoad(m, "<b/>"), true);
  assert.equal(gate.shouldLoad(m, "<a/>"), true);
});

test("a fresh editor instance (e.g. StrictMode remount) loads the same document", () => {
  const gate = new DocumentLoadGate<object>();
  gate.shouldLoad({}, "<a/>");
  assert.equal(gate.shouldLoad({}, "<a/>"), true);
});

test("invalidate() (an edit, a blank load or a failed import) re-arms the same document", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidate();
  assert.equal(gate.shouldLoad(m, "<a/>"), true);
});
