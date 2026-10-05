import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createMarketFetchGate,
  marketFetchBegin,
  marketFetchEnd,
} from "./marketFetchGate.ts";

test("a first request runs; a concurrent poll is dropped", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, {}), "run");
  // A background poll tick while the fetch is in flight adds nothing.
  assert.equal(marketFetchBegin(gate, {}), "drop");
  assert.deepEqual(marketFetchEnd(gate), { runQueued: null });
  // Once settled, the next poll runs again.
  assert.equal(marketFetchBegin(gate, {}), "run");
});

test("Check now during an active poll is queued, not dropped", () => {
  const gate = createMarketFetchGate();
  // A background poll is in flight…
  assert.equal(marketFetchBegin(gate, {}), "run");
  // …when the user clicks "Check now": the forced refresh must not be
  // silently dropped (the user would get the cached listing they asked to
  // bypass) — it is queued behind the in-flight poll.
  assert.equal(marketFetchBegin(gate, { force: true }), "queued");
  // A second click while one is already queued is a no-op.
  assert.equal(marketFetchBegin(gate, { force: true }), "drop");
  // When the poll settles, the queued forced refresh runs.
  assert.deepEqual(marketFetchEnd(gate), { runQueued: { force: true } });
  assert.equal(marketFetchBegin(gate, { force: true }), "run");
  // …and nothing remains queued behind it.
  assert.deepEqual(marketFetchEnd(gate), { runQueued: null });
});

test("a post-mutation reload during an active poll is queued, not dropped", () => {
  const gate = createMarketFetchGate();
  // A background poll is in flight…
  assert.equal(marketFetchBegin(gate, {}), "run");
  // …when an install/update/removal completes and asks for a reload. A
  // background fetch already in flight may have captured pre-mutation state,
  // so this reload MUST NOT be dropped — it is queued (non-forced: a mutation
  // changes local install state, not registry metadata, so no TTL bypass).
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "queued");
  // When the poll settles, the queued mutation reload runs — non-forced.
  assert.deepEqual(marketFetchEnd(gate), { runQueued: { force: false } });
});

test("a queued Check now outranks an already-queued mutation reload", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, {}), "run");
  // A mutation reload queues first (non-forced)…
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "queued");
  // …then a "Check now" escalates the single queued slot to forced.
  assert.equal(marketFetchBegin(gate, { force: true }), "queued");
  assert.deepEqual(marketFetchEnd(gate), { runQueued: { force: true } });
});

test("a forced request on an idle gate runs immediately", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, { force: true }), "run");
  assert.deepEqual(marketFetchEnd(gate), { runQueued: null });
});
