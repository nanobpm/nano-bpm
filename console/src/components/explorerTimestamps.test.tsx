import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
// Imported through the `file:` dep symlink — see TraceTimeline.test.tsx for why.
import { TraceTimeline } from "../../node_modules/@nanobpm/process-explorer/src/TraceTimeline.tsx";
import type { TraceTimeFmt } from "../../node_modules/@nanobpm/process-explorer/src/TraceTimeline.tsx";
import { InstanceRow } from "../../node_modules/@nanobpm/process-explorer/src/InstanceRow.tsx";
import { InstanceTimes } from "../../node_modules/@nanobpm/process-explorer/src/InstanceTimes.tsx";
import { fmtClock } from "../../node_modules/@nanobpm/process-explorer/src/traceTime.ts";
import type {
  Instance,
  InstanceTrace,
  TraceElement,
  TraceJob,
} from "../../node_modules/@nanobpm/process-explorer/src/gen/types.gen.ts";

// #1950: the API delivers every timestamp, but the explorer rendered none of the
// instance/element wall-clock times. These tests pin each one to a rendering
// component, and the guard at the bottom makes a NEW timestamp field on the
// generated types fail until it is rendered (here) or explicitly justified.

/** A recognisable clock label, so assertions don't depend on the time zone. */
const tagFmt: TraceTimeFmt = {
  duration: (n) => (n == null ? "—" : `${n}d`),
  clock: (n) => `⟨${n}⟩`,
  origin: "0",
  now: () => 0,
};

const instance = (o: Partial<Instance> = {}): Instance => ({
  key: "2251799813685249",
  process_id: "order",
  process_definition_key: "1",
  version: 3,
  state: "Active",
  start_date_ms: 1_700_000_000_000,
  has_incident: false,
  business_id: null,
  suspendedDate: null,
  tags: [],
  parent_process_instance_key: null,
  parent_element_instance_key: null,
  ...o,
});

const job = (o: Partial<TraceJob> = {}): TraceJob => ({
  type: "t",
  worker: "w",
  createdAt: 100,
  activatedAt: 200,
  completedAt: 300,
  waitMs: 200,
  queueMs: 100,
  serviceMs: 100,
  attempts: 1,
  failures: 0,
  ...o,
});

const element = (o: Partial<TraceElement> = {}): TraceElement => ({
  elementId: "task",
  elementInstanceKey: "10",
  scope: "0",
  enteredAt: 50,
  exitedAt: 400,
  durationMs: 350,
  incidents: 0,
  job: null,
  ...o,
});

const trace = (o: Partial<InstanceTrace> = {}): InstanceTrace => ({
  instanceKey: "1",
  processId: "p",
  version: null,
  businessId: null,
  tags: [],
  startedAt: 0,
  endedAt: 500,
  durationMs: 500,
  outcome: "completed",
  elements: [],
  incidents: [],
  path: [],
  ...o,
});

const timeline = (t: InstanceTrace): string =>
  renderToStaticMarkup(createElement(TraceTimeline, { trace: t, fmt: tagFmt }));

const decode = (html: string): string =>
  html
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"');

// --- 1. instance list ------------------------------------------------------

for (const card of [false, true]) {
  test(`instance row (${card ? "card" : "list"}) shows the start time with its age as tooltip`, () => {
    const ms = 1_700_000_000_000;
    const html = renderToStaticMarkup(
      createElement(InstanceRow, {
        inst: instance({ start_date_ms: ms }),
        selected: false,
        onSelect: () => {},
        card,
      }),
    );
    assert.ok(html.includes(fmtClock(ms)), html);
    assert.match(html, /title="started [^"]+ ago"/);
  });
}

// --- 2. instance detail header ---------------------------------------------

test("instance header shows started, ended and total duration", () => {
  const html = renderToStaticMarkup(
    createElement(InstanceTimes, {
      startedAt: 1000,
      endedAt: 4000,
      durationMs: 3000,
      fmt: tagFmt,
    }),
  );
  assert.ok(html.includes("started ⟨1000⟩"), html);
  assert.ok(html.includes("ended ⟨4000⟩"), html);
  assert.ok(html.includes("took 3000d"), html);
});

test("instance header of a running instance shows no end time", () => {
  const html = renderToStaticMarkup(
    createElement(InstanceTimes, { startedAt: 1000, fmt: tagFmt }),
  );
  assert.ok(html.includes("started ⟨1000⟩"));
  assert.doesNotMatch(html, /ended|took/);
});

test("instance header shows when a suspended instance was suspended", () => {
  const iso = "2026-01-02T03:04:05.000Z";
  const html = renderToStaticMarkup(
    createElement(InstanceTimes, { startedAt: 1, suspendedDate: iso }),
  );
  assert.ok(html.includes(`suspended ${fmtClock(Date.parse(iso))}`), html);
});

// --- 3. trace timeline ------------------------------------------------------

test("trace row shows its entered-at clock time and the bar tooltip gives entered → exited plus duration", () => {
  const html = decode(timeline(trace({ elements: [element()] })));
  assert.ok(html.includes(">⟨50⟩<"), html);
  assert.ok(html.includes("task · ⟨50⟩ → ⟨400⟩ · 350d"), html);
});

test("an element still running shows an open-ended bar tooltip", () => {
  const html = decode(
    timeline(
      trace({
        endedAt: null,
        elements: [element({ exitedAt: null, durationMs: null })],
      }),
    ),
  );
  assert.ok(html.includes("task · ⟨50⟩ → …"), html);
});

test("job queue/service tooltips carry created/activated/completed clock times", () => {
  const html = decode(timeline(trace({ elements: [element({ job: job() })] })));
  assert.ok(html.includes("created ⟨100⟩ → activated ⟨200⟩"), html);
  assert.ok(html.includes("activated ⟨200⟩ → completed ⟨300⟩"), html);
});

test("job wait tooltip (activation unobserved) carries created/completed clock times", () => {
  const html = decode(
    timeline(
      trace({ elements: [element({ job: job({ activatedAt: null }) })] }),
    ),
  );
  assert.ok(html.includes("created ⟨100⟩ → completed ⟨300⟩"), html);
});

// --- 4. defect-class guard: every timestamp field is surfaced -------------

const S = 777_001; // sentinel timestamp
const SI = 1_712_345_678_000; // sentinel wall-clock epoch

/** Each delivered timestamp field → a render that must show it (given S/SI). */
const RENDERED: Record<string, () => boolean> = {
  "Instance.start_date_ms": () =>
    renderToStaticMarkup(
      createElement(InstanceRow, {
        inst: instance({ start_date_ms: SI }),
        selected: false,
        onSelect: () => {},
      }),
    ).includes(fmtClock(SI)),
  "Instance.suspendedDate": () =>
    renderToStaticMarkup(
      createElement(InstanceTimes, {
        startedAt: 0,
        suspendedDate: new Date(SI).toISOString(),
      }),
    ).includes(`suspended ${fmtClock(SI)}`),
  // TraceSummary and InstanceTrace share InstanceTimes for their lifespan.
  "TraceSummary.startedAt": () =>
    renderToStaticMarkup(
      createElement(InstanceTimes, { startedAt: S, fmt: tagFmt }),
    ).includes(`⟨${S}⟩`),
  "TraceSummary.endedAt": () =>
    renderToStaticMarkup(
      createElement(InstanceTimes, { startedAt: 0, endedAt: S, fmt: tagFmt }),
    ).includes(`⟨${S}⟩`),
  // The trace detail header renders InstanceTrace through InstanceTimes too.
  "InstanceTrace.startedAt": () =>
    renderToStaticMarkup(
      createElement(InstanceTimes, { startedAt: S, fmt: tagFmt }),
    ).includes(`⟨${S}⟩`),
  "InstanceTrace.endedAt": () =>
    renderToStaticMarkup(
      createElement(InstanceTimes, { startedAt: 0, endedAt: S, fmt: tagFmt }),
    ).includes(`⟨${S}⟩`),
  "TraceElement.enteredAt": () =>
    timeline(trace({ elements: [element({ enteredAt: S })] })).includes(
      `⟨${S}⟩`,
    ),
  "TraceElement.exitedAt": () =>
    timeline(trace({ elements: [element({ exitedAt: S })] })).includes(
      `⟨${S}⟩`,
    ),
  "TraceJob.createdAt": () =>
    timeline(
      trace({ elements: [element({ job: job({ createdAt: S }) })] }),
    ).includes(`⟨${S}⟩`),
  "TraceJob.activatedAt": () =>
    timeline(
      trace({ elements: [element({ job: job({ activatedAt: S }) })] }),
    ).includes(`⟨${S}⟩`),
  "TraceJob.completedAt": () =>
    timeline(
      trace({ elements: [element({ job: job({ completedAt: S }) })] }),
    ).includes(`⟨${S}⟩`),
  "TraceIncident.raisedAt": () =>
    timeline(
      trace({
        incidents: [
          {
            elementId: "task",
            elementInstanceKey: "10",
            kind: "k",
            reason: "r",
            raisedAt: S,
            resolvedAt: null,
          },
        ],
      }),
    ).includes(`⟨${S}⟩`),
  "TraceIncident.resolvedAt": () =>
    timeline(
      trace({
        incidents: [
          {
            elementId: "task",
            elementInstanceKey: "10",
            kind: "k",
            reason: "r",
            raisedAt: 1,
            resolvedAt: S,
          },
        ],
      }),
    ).includes(`⟨${S}⟩`),
};

/** Timestamp fields deliberately not shown, each with its justification. */
const NOT_DISPLAYED: Record<string, string> = {};

/** Name-matched fields that are spans, not points in time (rendered via
 * `fmtDuration`, outside this guard's scope). */
const DURATIONS = new Set([
  "TraceSummary.durationMs",
  "InstanceTrace.durationMs",
  "TraceElement.durationMs",
  "TraceJob.waitMs",
  "TraceJob.queueMs",
  "TraceJob.serviceMs",
]);

const GUARDED_TYPES = [
  "Instance",
  "TraceSummary",
  "InstanceTrace",
  "TraceElement",
  "TraceJob",
  "TraceIncident",
];

function timestampFields(): string[] {
  const src = readFileSync(
    new URL(
      "../../node_modules/@nanobpm/process-explorer/src/gen/types.gen.ts",
      import.meta.url,
    ),
    "utf8",
  );
  const out: string[] = [];
  for (const name of GUARDED_TYPES) {
    const m = new RegExp(`export type ${name} = \\{([\\s\\S]*?)\\n\\};`).exec(
      src,
    );
    assert.ok(m, `type ${name} not found in types.gen.ts`);
    for (const f of m[1].matchAll(/^ {4}(\w+)\??:/gm)) {
      if (/(At|_ms|Ms|Date)$/.test(f[1])) out.push(`${name}.${f[1]}`);
    }
  }
  return out;
}

test("every delivered timestamp field is rendered or explicitly justified (#1950)", () => {
  const fields = timestampFields();
  assert.ok(fields.length >= 10, `parsed too few fields: ${fields}`);
  for (const f of fields) {
    if (DURATIONS.has(f)) continue;
    if (f in NOT_DISPLAYED) {
      assert.ok(
        NOT_DISPLAYED[f].trim().length > 0,
        `${f}: empty justification`,
      );
      continue;
    }
    const check = RENDERED[f];
    assert.ok(
      check,
      `${f} is delivered by the API but neither rendered nor on NOT_DISPLAYED`,
    );
    assert.ok(check(), `${f}: render check did not find the value`);
  }
  // No stale entries: every classified field still exists.
  for (const k of [
    ...Object.keys(RENDERED),
    ...Object.keys(NOT_DISPLAYED),
    ...DURATIONS,
  ]) {
    assert.ok(fields.includes(k), `${k} is no longer a field — drop it`);
  }
});
