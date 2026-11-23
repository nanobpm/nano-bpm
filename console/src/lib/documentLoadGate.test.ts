import { test } from "node:test";
import assert from "node:assert/strict";
import { DocumentLoadGate, type LoadAttempt } from "./documentLoadGate.ts";

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
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
  assert.notEqual(gate.shouldLoad(m, "<a/>"), null);
});

test("a different document always loads", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  assert.equal(gate.shouldLoad(m, "<b/>"), null);
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
});

test("a fresh editor instance (e.g. StrictMode remount) loads the same document", () => {
  const gate = new DocumentLoadGate<object>();
  gate.shouldLoad({}, "<a/>");
  assert.equal(gate.shouldLoad({}, "<a/>"), null);
});

test("invalidate() (an edit or a blank load) re-arms the same document", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidate();
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
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
  assert.notEqual(gate.shouldLoad(m, "<b/>"), null);
});

test("invalidateIf() re-arms the document whose import actually failed", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidateIf(m, "<a/>");
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
});

test("invalidateIf() ignores a failure attributed to another editor instance", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.invalidateIf({}, "<a/>");
  assert.notEqual(gate.shouldLoad(m, "<a/>"), null);
});

// The duplicate-delivery race the gate must NOT introduce: a repeat request
// arriving while the original import is still in flight must await the
// ORIGINAL promise — rejection included. Resolving the duplicate immediately
// would let a caller (e.g. a repeated XML→Visual click clearing the XML-dirty
// flag) treat the document as loaded even when the import later fails.
test("a duplicate request while the import is in flight returns the original promise", async () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
  // The caller starts the load and registers its promise, as the editors do.
  let release!: () => void;
  const inFlight = new Promise<void>((resolve) => {
    release = resolve;
  });
  gate.settle(m, "<a/>", inFlight);
  const duplicate = gate.shouldLoad(m, "<a/>");
  assert.equal(duplicate, inFlight);
  let settled = false;
  void duplicate!.then(() => {
    settled = true;
  });
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(settled, false, "duplicate must not resolve early");
  release();
  await duplicate;
  assert.equal(settled, true);
});

test("a duplicate of a held, then REJECTED import rejects with the same error", async () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
  let fail!: (e: unknown) => void;
  const inFlight = new Promise<void>((_resolve, reject) => {
    fail = reject;
  });
  // The editors attach their own catch before registering, so the registered
  // promise's rejection is always handled; mirror that here.
  const registered = inFlight.catch((e: unknown) => {
    throw e;
  });
  registered.catch(() => {});
  gate.settle(m, "<a/>", registered);
  const duplicate = gate.shouldLoad(m, "<a/>");
  assert.equal(duplicate, registered);
  const boom = new Error("boom");
  fail(boom);
  await assert.rejects(duplicate!, (e: unknown) => e === boom);
});

test("a duplicate after the import settled is an already-resolved no-op", async () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  gate.settle(m, "<a/>", Promise.resolve());
  await gate.shouldLoad(m, "<a/>");
});

test("settle() is a no-op when the record no longer names that pair", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  gate.shouldLoad(m, "<a/>");
  // A newer document supersedes the record before A's load registers.
  gate.shouldLoad(m, "<b/>");
  gate.settle(m, "<a/>", new Promise<void>(() => {}));
  // B's record must not capture A's promise.
  assert.notEqual(gate.shouldLoad(m, "<b/>"), null);
});

// A → B → A on the same editor: the FIRST A's import fails after the second A
// was recorded. The stale failure must not clear the newer A's record — the
// gate would otherwise re-import A on the next duplicate delivery and wipe the
// selection, the very double-import this gate exists to prevent. Comparing
// document identity is not enough here (both attempts name "<a/>"); the record
// must be keyed by load ATTEMPT.
test("a stale failure in an A → B → A sequence does not clear the newer A's record", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  const a1: { current?: LoadAttempt } = {};
  gate.shouldLoad(m, "<a/>", a1);
  gate.shouldLoad(m, "<b/>");
  const a2: { current?: LoadAttempt } = {};
  gate.shouldLoad(m, "<a/>", a2);
  // The first A's import rejects late; only ITS record may be cleared.
  gate.invalidateIf(m, "<a/>", a1.current);
  assert.notEqual(gate.shouldLoad(m, "<a/>"), null);
  // Sanity: the second attempt's own failure still re-arms A.
  gate.invalidateIf(m, "<a/>", a2.current);
  assert.equal(gate.shouldLoad(m, "<a/>"), null);
});

// The same stale-attempt class on the registration path: a superseded
// attempt's late settle() must not capture its promise into the newer record
// (a duplicate would then await the STALE import, whose rejection belongs to
// an attempt the gate no longer records).
test("a stale settle() in an A → B → A sequence does not capture into the newer A's record", () => {
  const gate = new DocumentLoadGate<object>();
  const m = {};
  const a1: { current?: LoadAttempt } = {};
  gate.shouldLoad(m, "<a/>", a1);
  gate.shouldLoad(m, "<b/>");
  const a2: { current?: LoadAttempt } = {};
  gate.shouldLoad(m, "<a/>", a2);
  let releaseStale!: () => void;
  const stale = new Promise<void>((resolve) => {
    releaseStale = resolve;
  });
  gate.settle(m, "<a/>", stale, a1.current);
  // The newer record must not have captured the stale promise.
  const duplicate = gate.shouldLoad(m, "<a/>");
  assert.notEqual(duplicate, stale);
  // And the newer attempt's own settle still registers normally.
  let releaseNewer!: () => void;
  const newer = new Promise<void>((resolve) => {
    releaseNewer = resolve;
  });
  gate.settle(m, "<a/>", newer, a2.current);
  assert.equal(gate.shouldLoad(m, "<a/>"), newer);
  releaseStale();
  releaseNewer();
});
