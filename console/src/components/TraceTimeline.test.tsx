import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
// Import through the `file:` dep SYMLINK (`node_modules/@nanobpm/process-explorer`)
// rather than the package specifier (whose `exports` map exposes only the
// barrel — which re-exports `BpmnViewer` and would drag `bpmn-js`, a
// bundler-only specifier, into the node test runner) or a `../process-explorer`
// relative path (which, under `preserveSymlinks`, resolves the package's own
// `react` import against the wrong tree). The symlink path keeps both the node
// resolver and `tsc -p tsconfig.test.json` resolving against the console's
// node_modules — the same way the tsconfig.test.json `include` does.
import {
  TraceTimeline,
  msFmt,
} from "../../node_modules/@nanobpm/process-explorer/src/TraceTimeline.tsx";
import type { InstanceTrace } from "../../node_modules/@nanobpm/process-explorer/src/gen/types.gen.ts";
import { foldSimTrace, stepFmt } from "../lib/simTrace.ts";
import type { WasmEvent } from "../lib/simTrace.ts";

// #1557 review: the trace row headline must use the trace's OWN clock domain.
// A production trace is wall-clock ms; the in-browser simulation's timestamps
// are logical event-sequence indexes, so handing `Date.now()` to a row there
// headlines ~1.8 trillion "steps". And a job left unfinished when its trace
// ENDED (canceled work) must stop at the trace end, not count up to today.

const baseTrace = (o: Partial<InstanceTrace>): InstanceTrace => ({
  instanceKey: "1",
  processId: "p",
  version: null,
  businessId: null,
  tags: [],
  startedAt: 0,
  endedAt: null,
  durationMs: null,
  outcome: "active",
  elements: [],
  incidents: [],
  path: [],
  ...o,
});

const runningJobElement = (createdAt: number, activatedAt: number) => ({
  elementId: "task",
  elementInstanceKey: "10",
  scope: "0",
  enteredAt: createdAt,
  exitedAt: null,
  durationMs: null,
  incidents: 0,
  job: {
    type: "t",
    worker: "w",
    createdAt,
    activatedAt,
    completedAt: null,
    waitMs: null,
    queueMs: activatedAt - createdAt,
    serviceMs: null,
    attempts: 1,
    failures: 0,
  },
});

test("simulation trace: an unfinished job headlines elapsed steps, not wall clock", () => {
  const events: WasmEvent[] = [
    { seq: 1, type: "ProcessInstanceCreated", instance_key: "1" },
    {
      seq: 2,
      type: "JobCreated",
      instance_key: "1",
      element_instance_key: "10",
      element_id: "task",
      job_key: "100",
      job_type: "t",
    },
    { seq: 5, type: "JobActivated", instance_key: "1", job_key: "100", worker: "w" },
  ] as unknown as WasmEvent[];
  const trace = foldSimTrace(events, "1", "p");
  const html = renderToStaticMarkup(
    createElement(TraceTimeline, { trace, fmt: stepFmt }),
  );
  // Elapsed = current step (5) − activatedAt (5) = 0 steps, and crucially NOT
  // a wall-clock count-up (Date.now() − 5 ≈ 1.7e12 would render as a huge
  // "… steps" figure).
  assert.match(html, /0 steps …/);
  assert.doesNotMatch(html, /\d{6,} steps/);
});

test("simulation trace: a later event advances the elapsed step headline", () => {
  const events: WasmEvent[] = [
    { seq: 1, type: "ProcessInstanceCreated", instance_key: "1" },
    {
      seq: 2,
      type: "JobCreated",
      instance_key: "1",
      element_instance_key: "10",
      element_id: "task",
      job_key: "100",
      job_type: "t",
    },
    { seq: 5, type: "JobActivated", instance_key: "1", job_key: "100", worker: "w" },
    { seq: 9, type: "ElementActivated", instance_key: "1", element_instance_key: "11", element_id: "next" },
  ] as unknown as WasmEvent[];
  const trace = foldSimTrace(events, "1", "p");
  const html = renderToStaticMarkup(createElement(TraceTimeline, { trace, fmt: stepFmt }));
  // now = last folded step (9); activated at 5 → 4 steps elapsed.
  assert.match(html, /4 steps …/);
});

test("production trace: a canceled job in an ENDED trace stops at the trace end", () => {
  const trace = baseTrace({
    startedAt: 0,
    endedAt: 2_500,
    outcome: "terminated",
    elements: [runningJobElement(0, 500)],
  });
  const html = renderToStaticMarkup(createElement(TraceTimeline, { trace, fmt: msFmt }));
  // endedAt (2_500) − activatedAt (500) = 2.00 s, and NOT a count-up to
  // Date.now() (which would be minutes/hours and still carry the running "…").
  assert.match(html, /2\.00 s/);
  assert.doesNotMatch(html, /2\.00 s …/);
});
