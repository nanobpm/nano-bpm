import { test } from "node:test";
import assert from "node:assert/strict";
import { fmtDuration, headlineDuration } from "./traceTime.ts";

const job = (
  o: Partial<Parameters<typeof headlineDuration>[0]["job"] & object>,
) => ({
  createdAt: 0,
  activatedAt: null,
  completedAt: null,
  waitMs: null,
  queueMs: null,
  serviceMs: null,
  ...o,
});

test("completed job with observed activation headlines service time, not queue time", () => {
  const h = headlineDuration({
    durationMs: 300_500,
    job: job({
      createdAt: 0,
      activatedAt: 500,
      completedAt: 300_500,
      queueMs: 500,
      serviceMs: 300_000,
      waitMs: 300_500,
    }),
  });
  assert.equal(h.ms, 300_000);
  assert.equal(h.label, fmtDuration(300_000));
  assert.ok(!h.label.includes("q"));
  assert.equal(h.running, false);
  assert.equal(h.queueMs, 500);
  assert.match(h.title, /service 5m 0s · queue 500 ms/);
});

test("job without observed activation headlines wait time", () => {
  const h = headlineDuration({
    durationMs: 9000,
    job: job({ createdAt: 1000, completedAt: 4000, waitMs: 3000 }),
  });
  assert.equal(h.ms, 3000);
  assert.equal(h.label, "3.00 s");
  assert.equal(h.queueMs, null);
  assert.match(h.title, /activation not observed/);
});

test("activated but incomplete job shows elapsed time, marked running", () => {
  const h = headlineDuration(
    {
      durationMs: null,
      job: job({ createdAt: 0, activatedAt: 500, queueMs: 500 }),
    },
    10_500,
  );
  assert.equal(h.ms, 10_000);
  assert.equal(h.running, true);
  assert.equal(h.label, "10.00 s …");
  assert.match(h.title, /^running 10\.00 s · queue 500 ms$/);
});

test("non-job element headlines durationMs", () => {
  const h = headlineDuration({ durationMs: 42 });
  assert.equal(h.ms, 42);
  assert.equal(h.label, "42 ms");
  assert.equal(h.running, false);
});
