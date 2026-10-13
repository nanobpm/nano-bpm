// Unit tests for precondition resolution and registry filtering (ADR 0049 §3).
//
// The repair path is the one that matters most: it is the structural fix for the
// spike's defect of promising "hit Run to boot the engine" on a host with no
// JavaScript runtime, using data (`nodeAvailable`) the console already had in
// hand.
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/resolution.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  clearJourneys,
  journeysFor,
  registerJourney,
  resolveSteps,
} from "./registry.ts";
import {
  hasCluster,
  hasJsRuntime,
  hasProject,
  hasTraces,
  isPolling,
} from "./preconditions.ts";
import type { Journey, Step, TourContext } from "./types.ts";

function ctx(over: Partial<TourContext> = {}): TourContext {
  return {
    profile: "studio",
    route: "/projects",
    projects: [],
    nodeAvailable: true,
    templates: [],
    extensions: [],
    scratch: {},
    ...over,
  };
}

const note = (id: string, over: Partial<Step> = {}): Step =>
  ({ kind: "note", id, title: id, body: id, ...over }) as Step;

function journey(over: Partial<Journey> = {}): Journey {
  return {
    id: "j",
    title: "J",
    blurb: "b",
    profiles: ["studio"],
    steps: [note("a")],
    successEvent: () => true,
    ...over,
  };
}

// ── preconditions ────────────────────────────────────────────────────────────

test("hasJsRuntime: ok with Node, repair without it", () => {
  assert.equal(hasJsRuntime.test(ctx()), "ok");
  assert.equal(hasJsRuntime.test(ctx({ nodeAvailable: true })), "ok");
  assert.equal(
    hasJsRuntime.test(ctx({ nodeAvailable: false })),
    "repair",
    "with no runtime the Run step must repair into an install hint, never promise Run",
  );
});

test("hasCluster: absent nodeCount reads as single-node (assume the lesser reality)", () => {
  assert.equal(hasCluster.test(ctx()), "repair");
  assert.equal(hasCluster.test(ctx({ nodeCount: 1 })), "repair");
  assert.equal(hasCluster.test(ctx({ nodeCount: 3 })), "ok");
});

test("hasTraces / hasProject skip rather than repair when there is nothing to show", () => {
  assert.equal(hasTraces.test(ctx()), "skip");
  assert.equal(hasTraces.test(ctx({ traceCount: 2 })), "ok");
  assert.equal(hasProject.test(ctx()), "skip");
  assert.equal(
    hasProject.test(ctx({ projects: [{ name: "p" } as never] })),
    "ok",
  );
});

test("isPolling: false when the consumer source has not been registered", () => {
  // Before #404 lands there is no `consumers` field, and a handoff step must
  // stay self-reported rather than auto-advance on a signal we cannot see.
  assert.equal(isPolling("t")(ctx()), false);
  assert.equal(
    isPolling("t")(ctx({ consumers: [{ jobType: "t", worker: "w" }] })),
    true,
  );
  assert.equal(
    isPolling("t")(ctx({ consumers: [{ jobType: "other", worker: "w" }] })),
    false,
  );
});

// ── resolution ───────────────────────────────────────────────────────────────

test("resolveSteps: passes ungated steps through, preserving authored indices", () => {
  const j = journey({ steps: [note("a"), note("b"), note("c")] });
  const r = resolveSteps(j, ctx());
  assert.deepEqual(
    r.steps.map((s) => [s.step.id, s.authoredIndex]),
    [
      ["a", 0],
      ["b", 1],
      ["c", 2],
    ],
  );
  assert.equal(r.skipped.length, 0);
  assert.equal(r.repaired.length, 0);
});

test("resolveSteps: substitutes the repair step and reports it", () => {
  const j = journey({
    steps: [
      note("run", {
        precondition: hasJsRuntime,
        repair: note("install-runtime"),
      }),
    ],
  });
  const r = resolveSteps(j, ctx({ nodeAvailable: false }));
  assert.equal(r.steps.length, 1);
  assert.equal(r.steps[0].step.id, "install-runtime");
  assert.equal(r.steps[0].repaired, "has-js-runtime");
  // The authored index is preserved so a resume stored against the authored list
  // still lands on the right place.
  assert.equal(r.steps[0].authoredIndex, 0);
  assert.deepEqual(r.repaired, [
    { stepId: "run", preconditionId: "has-js-runtime" },
  ]);
});

test("resolveSteps: a repair verdict with no repair step skips instead of showing the original", () => {
  // Showing the original would assert exactly what the precondition just said is
  // untrue, so skipping is the safe degradation.
  const j = journey({ steps: [note("run", { precondition: hasJsRuntime })] });
  const r = resolveSteps(j, ctx({ nodeAvailable: false }));
  assert.equal(r.steps.length, 0);
  assert.deepEqual(r.skipped, [
    { stepId: "run", preconditionId: "has-js-runtime" },
  ]);
});

test("resolveSteps: skipped steps drop out but later authored indices are unchanged", () => {
  const j = journey({
    steps: [note("a"), note("traces", { precondition: hasTraces }), note("c")],
  });
  const r = resolveSteps(j, ctx());
  assert.deepEqual(
    r.steps.map((s) => [s.step.id, s.authoredIndex]),
    [
      ["a", 0],
      ["c", 2],
    ],
  );
  assert.deepEqual(r.skipped, [
    { stepId: "traces", preconditionId: "has-traces" },
  ]);
});

// ── registry filtering ───────────────────────────────────────────────────────

test("journeysFor: filters by profile", () => {
  clearJourneys();
  registerJourney(journey({ id: "studio-only", profiles: ["studio"] }));
  registerJourney(journey({ id: "observe-only", profiles: ["observe"] }));
  registerJourney(journey({ id: "both", profiles: ["studio", "observe"] }));

  assert.deepEqual(
    journeysFor("studio").map((j) => j.id),
    ["studio-only", "both"],
  );
  assert.deepEqual(
    journeysFor("observe").map((j) => j.id),
    ["observe-only", "both"],
  );
  clearJourneys();
});

test("journeysFor: a journey-level precondition removes it from the picker entirely", () => {
  clearJourneys();
  registerJourney(journey({ id: "always" }));
  registerJourney(journey({ id: "needs-traces", preconditions: [hasTraces] }));

  // Without a context we cannot evaluate gates, so list everything rather than
  // hide a journey that might well be available.
  assert.equal(journeysFor("studio").length, 2);
  assert.deepEqual(
    journeysFor("studio", ctx()).map((j) => j.id),
    ["always"],
  );
  assert.deepEqual(
    journeysFor("studio", ctx({ traceCount: 1 })).map((j) => j.id),
    ["always", "needs-traces"],
  );
  clearJourneys();
});
