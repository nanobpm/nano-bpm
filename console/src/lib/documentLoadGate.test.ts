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

test("invalidate() (an edit or a blank load) re-arms the same document", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidate();
  assert.equal(gate.shouldLoad(m, "<a/>"), true);
});

// A failed import must only re-arm the document that failed. When import(A) is
// queued, then import(B) records B in the gate, A's failure must NOT clear B's
// record — otherwise a duplicate delivery of B imports again and wipes the
// selection (the very double-import the gate exists to prevent).
test("invalidateIf() clears the record only when it still names the failed document", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.shouldLoad(m, "<b/>");
  // A's import fails after B was recorded: B's record must survive.
  gate.invalidateIf(m, "<a/>");
  assert.equal(gate.shouldLoad(m, "<b/>"), false);
});

test("invalidateIf() re-arms the document whose import actually failed", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidateIf(m, "<a/>");
  assert.equal(gate.shouldLoad(m, "<a/>"), true);
});

test("invalidateIf() ignores a failure attributed to another editor instance", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidateIf({}, "<a/>");
  assert.equal(gate.shouldLoad(m, "<a/>"), false);
});
