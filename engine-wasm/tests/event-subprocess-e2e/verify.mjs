// Real-WASM end-to-end guard for event sub-processes (#1555; promoted in #1616
// from the hand-run PR #1597 "wasm probe"). Drives the CI-built
// `@nanobpm/engine-wasm` TestEngine — the console/Bojtos execution surface —
// through BOTH shipped entrypoints (lean + read-model), so a wasm build that
// parses but does not execute event sub-processes fails CI instead of slipping
// through on the native-only engine-core tests.
//
// Model (`event-subprocess.bpmn`, with DI): service task `work`; a
// NON-interrupting timer ESP (`PT1M` -> `notify` task); an INTERRUPTING
// message ESP (`cancel`, correlated on `orderId`).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const xml = readFileSync(new URL("./event-subprocess.bpmn", import.meta.url), "utf8");

const ONE_MINUTE_MS = 60_000;

for (const variant of ["lean", "readmodel"]) {
  const entry = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entry);
  initSync({ module: readFileSync(require.resolve(
    `@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`,
  )) });

  const engine = new TestEngine();
  engine.deploy(xml);
  engine.createInstance("esp_e2e", JSON.stringify({ orderId: "o-1" }));

  const instanceOf = (snap) => snap.instances.find((i) => i.processId === "esp_e2e");
  const activeIds = (snap) =>
    new Set((instanceOf(snap)?.activeElements ?? []).map((a) => a.elementId));
  const jobTypes = (snap) => new Set(snap.jobs.map((j) => j.jobType));
  const completedCount = (snap, id) =>
    snap.elementStats.find((s) => s.elementId === id)?.completed ?? 0;

  // 1. The main flow parks on `work`; the reminder has not fired yet.
  let snap = JSON.parse(engine.snapshot());
  assert.ok(jobTypes(snap).has("work"), `${variant}: 'work' job must be created`);
  assert.ok(activeIds(snap).has("work"), `${variant}: 'work' must be active`);
  assert.ok(!activeIds(snap).has("notify"), `${variant}: 'notify' must not run before the timer`);

  // 2. Past the 1m timer the NON-interrupting ESP runs alongside `work`.
  snap = JSON.parse(engine.advanceTime(ONE_MINUTE_MS + 1));
  const active = activeIds(snap);
  assert.ok(active.has("work") && active.has("notify"),
    `${variant}: non-interrupting timer ESP must run alongside work; active = ${[...active]}`);
  assert.ok(jobTypes(snap).has("work") && jobTypes(snap).has("notify"),
    `${variant}: 'work' and 'notify' jobs must both be live`);

  // 3. The INTERRUPTING message ESP terminates the siblings and completes the
  //    instance through ITS end event, not the main end.
  snap = JSON.parse(engine.correlateMessage("cancel", "o-1", "{}"));
  const inst = instanceOf(snap);
  assert.equal(inst?.state, "Completed",
    `${variant}: instance must complete via the interrupting ESP`);
  assert.deepEqual(inst.activeElements, [], `${variant}: no element may remain active`);
  assert.equal(completedCount(snap, "esp_cancel_end"), 1,
    `${variant}: must complete via esp_cancel_end`);
  assert.equal(completedCount(snap, "main_end"), 0, `${variant}: main_end must not be reached`);
  // Termination: the engine journals a torn-down element as Completing/Completed
  // after canceling its job, so "terminated, not completed normally" is
  // observable as: its job was canceled (never completed by a worker) and its
  // outgoing flow was never taken.
  assert.equal(snap.jobs.length, 0, `${variant}: the 'work'/'notify' jobs must be canceled`);
  const events = JSON.parse(engine.events());
  assert.equal(events.filter((e) => e.type === "JobCanceled").length, 2,
    `${variant}: both the 'work' and 'notify' jobs must be canceled`);
  assert.ok(!events.some((e) => e.type === "JobCompleted"),
    `${variant}: no job may complete normally`);
  const taken = new Set(snap.takenSequenceFlows.map((f) => `${f.from}->${f.to}`));
  assert.ok(!taken.has("work->main_end"), `${variant}: 'work' must be terminated, not left normally`);
  assert.ok(!taken.has("notify->esp_reminder_end"),
    `${variant}: 'notify' must be terminated, not left normally`);

  console.log(`[${variant}] event sub-process e2e OK (non-interrupting timer + interrupting message)`);
}
