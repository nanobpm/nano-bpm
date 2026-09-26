// Scenario driver for the two-backend parity runner (#1260).
//
// ONE driver, TWO backends: it plays an identical scenario (a deploy, a start,
// then an ordered list of steps) against any backend implementing the common
// interface (`nano-backend.mjs`, `camunda-backend.mjs`) and returns a normalised
// observation. Timing is serialised STEP BY STEP — each step fully settles
// before the next is issued — which is what makes the Zeebe side deterministic.
// There are no retries anywhere.

import { readFileSync } from "node:fs";
import { join } from "node:path";

/**
 * A scenario is a directory under the corpus containing a `scenario.json`:
 *
 *   {
 *     "name": "linear-service-task",
 *     "bpmn": "process.bpmn",         // resource next to scenario.json
 *     "processId": "Process_...",
 *     "variables": { },               // start variables (optional)
 *     "steps": [                      // ordered, serialised
 *       { "op": "activateAndComplete", "jobType": "a", "variables": { "a": 1 } },
 *       { "op": "correlateMessage", "name": "M", "correlationKey": "k", "variables": {} },
 *       { "op": "broadcastSignal", "name": "S", "variables": {} },
 *       { "op": "advanceTime", "ms": 1000 }
 *     ],
 *     "expect": { "completed": true, "variables": { }, "completedElements": { } }
 *   }
 */
export function loadScenario(dir) {
  const scenario = JSON.parse(readFileSync(join(dir, "scenario.json"), "utf8"));
  scenario.dir = dir;
  scenario.xml = readFileSync(join(dir, scenario.bpmn), "utf8");
  return scenario;
}

async function dispatchStep(backend, handle, step) {
  switch (step.op) {
    case "activateAndComplete":
      return backend.activateAndComplete(handle, step.jobType, step.variables ?? {});
    case "correlateMessage":
      return backend.correlateMessage(
        handle,
        step.name,
        step.correlationKey,
        step.variables ?? {},
      );
    case "broadcastSignal":
      return backend.broadcastSignal(handle, step.name, step.variables ?? {});
    case "advanceTime":
      return backend.advanceTime(handle, step.ms);
    default:
      throw new Error(`unknown scenario step op: ${JSON.stringify(step.op)}`);
  }
}

/**
 * Run one scenario against one backend, returning its normalised observation.
 * The backend is reset first so a shared/in-process engine starts clean.
 */
export async function runScenario(backend, scenario) {
  await backend.reset();
  await backend.deploy(scenario.xml);
  const handle = await backend.start(scenario.processId, scenario.variables ?? {});
  for (const step of scenario.steps ?? []) {
    await dispatchStep(backend, handle, step);
  }
  return backend.observe(handle);
}
