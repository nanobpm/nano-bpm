import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createMarketFetchGate,
  marketFetchBegin,
  marketFetchEnd,
  marketFetchQueueWaiter,
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

test("a queued must-run request's waiter resolves only when the drained run settles", () => {
  const gate = createMarketFetchGate();
  // A background poll is in flight…
  assert.equal(marketFetchBegin(gate, {}), "run");
  // …when a post-mutation reload is queued behind it. Its caller registers a
  // waiter so it can await the reload (the install handler must keep `busy`
  // set until the reload reflecting its mutation has actually run).
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "queued");
  let resolved = false;
  marketFetchQueueWaiter(gate, () => {
    resolved = true;
  });
  // The poll settles: the queued reload is reported but has NOT run yet, so the
  // waiter MUST still be pending — resolving here would clear `busy` against
  // pre-mutation state (#1330).
  assert.deepEqual(marketFetchEnd(gate), { runQueued: { force: false } });
  assert.equal(
    resolved,
    false,
    "waiter must not resolve before the drain runs",
  );
  // The drain run starts and settles — now, and only now, the waiter resolves.
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "run");
  assert.deepEqual(marketFetchEnd(gate), { runQueued: null });
  assert.equal(
    resolved,
    true,
    "waiter resolves once the drained reload settles",
  );
});

test("must-run requests coalesced into the queue all await the same drain", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, {}), "run");
  // First mutation reload queues…
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "queued");
  let first = false;
  let second = false;
  marketFetchQueueWaiter(gate, () => {
    first = true;
  });
  // …a second mutation reload coalesces into the queue (returns "drop") but is
  // still must-run, so it registers a waiter on the same drain.
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "drop");
  marketFetchQueueWaiter(gate, () => {
    second = true;
  });
  assert.deepEqual(marketFetchEnd(gate), { runQueued: { force: false } });
  assert.equal(first, false);
  assert.equal(second, false);
  // The drain run settles — both coalesced waiters resolve together.
  assert.equal(marketFetchBegin(gate, { mustRun: true }), "run");
  assert.deepEqual(marketFetchEnd(gate), { runQueued: null });
  assert.equal(first, true, "first coalesced waiter resolves");
  assert.equal(second, true, "second coalesced waiter resolves");
});

test("a redundant background poll registers no waiter and leaves the queue empty", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, {}), "run");
  // A plain poll tick while a fetch is in flight is dropped — the view resolves
  // it immediately and registers no waiter.
  assert.equal(marketFetchBegin(gate, {}), "drop");
  assert.deepEqual(marketFetchEnd(gate), { runQueued: null });
  assert.deepEqual(gate.queuedWaiters, []);
  assert.deepEqual(gate.drainingWaiters, []);
});
