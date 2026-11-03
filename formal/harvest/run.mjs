#!/usr/bin/env node
// Runs the harvested Zeebe engine-test scenarios differentially against Nano
// (#1249) and classifies each as runnable / diverging / out-of-scope.
//
//   node formal/harvest/run.mjs [--corpus formal/harvest/corpus] [--engine <path-to-@nanobpm/engine-wasm>]
//
// For every normalised model (`corpus/bpmn/<id>.bpmn` + `corpus/scenarios/<id>.json`):
//
//   * deploy the BPMN to a fresh Nano `TestEngine` (the engine-wasm backend the
//     parity suite's scenario driver, #1240 slice 5, also targets);
//   * create an instance and drive the scenario: complete each `Created` job in
//     the scenario's `jobCompletionOrder`, until the instance completes or no
//     progress is possible;
//   * classify the outcome:
//       runnable     — the instance deployed, ran, and completed cleanly;
//       diverging    — Nano accepted and ran the model but the observable
//                      outcome differs from the Zeebe-implied expectation (the
//                      instance did NOT complete, raised an incident, or left
//                      jobs/elements stranded). Each is a parity issue candidate;
//       out-of-scope — Nano REJECTED the deploy (the model uses surface Nano
//                      does not implement) or the scenario needs a driver
//                      capability this slice does not drive (messages, timers).
//
// The result feeds report.mjs. This runner is hermetic: it uses the committed
// engine-wasm artifacts and does no network I/O.

import { existsSync, readFileSync, readdirSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));

export class RunError extends Error {}

/** Load the committed engine-wasm TestEngine (lean baseline build), initialising
 * the wasm module synchronously from the committed artifact. */
export async function loadEngine(enginePath) {
  const pkgDir = enginePath ?? resolve(here, '..', '..', 'engine-wasm', 'pkg');
  const entry = join(pkgDir, 'lean', 'nanobpmn_engine.js');
  const wasmFile = join(pkgDir, 'lean', 'nanobpmn_engine_bg.wasm');
  if (!existsSync(entry) || !existsSync(wasmFile)) {
    throw new RunError(
      `engine-wasm artifacts not found at ${pkgDir}/lean — run \`make console-wasm\` first`);
  }
  const mod = await import(pathToFileURL(entry).href);
  const TestEngine = mod.TestEngine ?? mod.default?.TestEngine;
  mod.initSync({ module: readFileSync(wasmFile) });
  return TestEngine;
}

const MAX_STEPS = 200; // runaway guard: a healthy scenario completes well within this

/**
 * Run one scenario against a fresh TestEngine. Returns
 *   { status: 'runnable'|'diverging'|'out-of-scope', detail, observed }
 */
export function runScenario(TestEngine, id, xml, scenario) {
  const eng = new TestEngine();
  try {
    // Deploy — a parse/validation rejection means the model uses surface Nano
    // does not implement: out of scope for the differential run.
    try {
      eng.deploy(xml);
    } catch (e) {
      return { status: 'out-of-scope', detail: `nano-rejects-deploy: ${String(e?.message ?? e).slice(0, 160)}` };
    }

    // A scenario with message/timer steps needs a driver capability this slice
    // does not drive — record out of scope rather than drive it wrongly.
    if ((scenario.messageCorrelation?.length ?? 0) > 0 || (scenario.timerTicks?.length ?? 0) > 0) {
      return { status: 'out-of-scope', detail: 'scenario-needs-message-or-timer-driver' };
    }
    // A scenario that needs variables / event-based / catch-event / sub-process
    // driving (tagged at normalise time) cannot be driven deterministically by
    // this slice's empty-variable runner. Record it out-of-scope — a driver
    // limitation, NOT a Nano divergence — so a data-dependent route is never
    // mis-reported as a parity defect.
    if ((scenario.needs?.length ?? 0) > 0) {
      return { status: 'out-of-scope', detail: `driver-capability: ${scenario.needs.join(', ')}` };
    }

    let snap;
    try {
      snap = JSON.parse(eng.createInstance(scenario.process, '{}'));
    } catch (e) {
      return { status: 'diverging', detail: `createInstance-threw: ${String(e?.message ?? e).slice(0, 160)}` };
    }

    const order = scenario.jobCompletionOrder ?? [];
    const completed = [];
    let steps = 0;
    // Drive jobs in the scenario's declared order. A healthy model surfaces each
    // scheduled job as `Created` in turn; completing it advances the instance.
    for (const elementId of order) {
      if (steps++ > MAX_STEPS) {
        return { status: 'diverging', detail: 'runaway: exceeded step guard', observed: { completed } };
      }
      const job = (snap.jobs ?? []).find((j) => j.elementId === elementId && j.state === 'Created');
      if (!job) {
        // The job the scenario expects is not waiting. Either the instance
        // already completed (fine if this was the last) or the route diverged.
        const done = (snap.instances ?? []).every((i) => i.completed);
        if (done) break;
        return {
          status: 'diverging',
          detail: `expected-job-not-created: ${elementId}`,
          observed: { completed, waitingJobs: (snap.jobs ?? []).map((j) => j.elementId) },
        };
      }
      snap = JSON.parse(eng.completeJob(job.key, '{}'));
      completed.push(elementId);
    }

    const instances = snap.instances ?? [];
    const allDone = instances.length > 0 && instances.every((i) => i.completed);
    const openIncidents = (snap.incidents ?? []).length;
    const activeLeft = instances.flatMap((i) => i.activeElements ?? []).length;

    if (allDone && openIncidents === 0 && activeLeft === 0) {
      return { status: 'runnable', detail: 'completed-cleanly', observed: { completed } };
    }
    return {
      status: 'diverging',
      detail: allDone
        ? `completed-with-residue: incidents=${openIncidents} active=${activeLeft}`
        : 'instance-did-not-complete',
      observed: { completed, openIncidents, activeLeft, allDone },
    };
  } finally {
    eng.free?.();
  }
}

export async function runCorpus(corpusDir, enginePath) {
  const TestEngine = await loadEngine(enginePath);
  const bpmnDir = join(corpusDir, 'bpmn');
  const scenDir = join(corpusDir, 'scenarios');
  const results = [];
  for (const f of readdirSync(bpmnDir).filter((x) => x.endsWith('.bpmn')).sort()) {
    const id = f.replace(/\.bpmn$/, '');
    const scenFile = join(scenDir, `${id}.json`);
    if (!existsSync(scenFile)) continue;
    const xml = readFileSync(join(bpmnDir, f), 'utf8');
    const scenario = JSON.parse(readFileSync(scenFile, 'utf8'));
    const r = runScenario(TestEngine, id, xml, scenario);
    results.push({ id, processId: scenario.process, ...r });
  }
  return results;
}

async function main() {
  const args = process.argv.slice(2);
  const cIdx = args.indexOf('--corpus');
  const corpusDir = cIdx >= 0 ? args[cIdx + 1] : join(here, 'corpus');
  const eIdx = args.indexOf('--engine');
  const enginePath = eIdx >= 0 ? args[eIdx + 1] : undefined;

  const results = await runCorpus(corpusDir, enginePath);
  const counts = results.reduce((m, r) => { m[r.status] = (m[r.status] ?? 0) + 1; return m; }, {});
  mkdirSync(corpusDir, { recursive: true });
  writeFileSync(join(corpusDir, 'run-results.json'), JSON.stringify(results, null, 2) + '\n');
  console.log(JSON.stringify({ total: results.length, ...counts }, null, 2));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((e) => { console.error(e); process.exit(1); });
}
