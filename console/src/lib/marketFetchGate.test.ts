import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createMarketFetchGate,
  marketFetchBegin,
  marketFetchEnd,
} from "./marketFetchGate.ts";

test("a first request runs; a concurrent poll is dropped", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, false), "run");
  // A background poll tick while the fetch is in flight adds nothing.
  assert.equal(marketFetchBegin(gate, false), "drop");
  marketFetchEnd(gate);
  // Once settled, the next poll runs again.
  assert.equal(marketFetchBegin(gate, false), "run");
});

test("Check now during an active poll is queued, not dropped", () => {
  const gate = createMarketFetchGate();
  // A background poll is in flight…
  assert.equal(marketFetchBegin(gate, false), "run");
  // …when the user clicks "Check now": the forced refresh must not be
  // silently dropped (the user would get the cached listing they asked to
  // bypass) — it is queued behind the in-flight poll.
  assert.equal(marketFetchBegin(gate, true), "queued");
  // A second click while one is already queued is a no-op.
  assert.equal(marketFetchBegin(gate, true), "drop");
  // When the poll settles, the queued forced refresh runs.
  assert.deepEqual(marketFetchEnd(gate), { runQueuedForce: true });
  assert.equal(marketFetchBegin(gate, true), "run");
  // …and nothing remains queued behind it.
  assert.deepEqual(marketFetchEnd(gate), { runQueuedForce: false });
});

test("a forced request on an idle gate runs immediately", () => {
  const gate = createMarketFetchGate();
  assert.equal(marketFetchBegin(gate, true), "run");
  assert.deepEqual(marketFetchEnd(gate), { runQueuedForce: false });
});
