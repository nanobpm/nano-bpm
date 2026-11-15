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

// #1557 review: a job left unfinished when its trace ENDED (canceled work —
// the trace store has no JobCanceled arm, so completedAt stays null) must not
// keep counting up to the viewer's clock.

test("unfinished job in an ENDED trace stops at the trace end, not now", () => {
  const h = headlineDuration(
    {
      durationMs: null,
      job: job({ createdAt: 0, activatedAt: 500, queueMs: 500 }),
    },
    // The viewer's clock is days past the trace end; the headline must not
    // reach it.
    10_000_000,
    fmtDuration,
    2_500,
  );
  assert.equal(h.ms, 2_000);
  assert.equal(h.running, false);
  assert.equal(h.label, "2.00 s");
  assert.match(h.title, /^ended 2\.00 s · queue 500 ms$/);
});

test("unfinished job never activated in an ENDED trace measures to the trace end", () => {
  const h = headlineDuration(
    { durationMs: null, job: job({ createdAt: 100 }) },
    10_000_000,
    fmtDuration,
    1_100,
  );
  assert.equal(h.ms, 1_000);
  assert.equal(h.running, false);
  assert.equal(h.label, "1.00 s");
  assert.match(h.title, /^ended \(not yet activated\) 1\.00 s$/);
});

test("unfinished job with NO known trace end keeps counting to now (running)", () => {
  // traceEndedAt null = the trace is still active (or its end is unknown), so
  // the job is treated as in flight: elapsed-to-now, marked running. The
  // stopped/unknown fallback only applies once the trace's end is known.
  const h = headlineDuration(
    {
      durationMs: null,
      job: job({ createdAt: 0, activatedAt: 500, queueMs: 500 }),
    },
    10_000_000,
    fmtDuration,
    null,
  );
  assert.equal(h.ms, 9_999_500);
  assert.equal(h.running, true);
  assert.equal(h.label, "166m 39s …");
  assert.match(h.title, /^running 166m 39s · queue 500 ms$/);
});

// #1557 review: the simulation's timestamps are logical step indexes, so the
// caller passes the trace's own clock (current step) as `now` — an unfinished
// simulated job then shows elapsed STEPS, not ~1.8 trillion wall-clock ms.

test("unfinished simulated job measures elapsed logical steps, not wall clock", () => {
  const stepFmt = (n: number | null) =>
    n == null ? "—" : `${n} step${n === 1 ? "" : "s"}`;
  const h = headlineDuration(
    {
      durationMs: null,
      job: job({ createdAt: 2, activatedAt: 5, queueMs: 3 }),
    },
    // Current simulation step (event index), NOT Date.now().
    9,
    stepFmt,
  );
  assert.equal(h.ms, 4);
  assert.equal(h.running, true);
  assert.equal(h.label, "4 steps …");
  assert.match(h.title, /^running 4 steps · queue 3 steps$/);
});
