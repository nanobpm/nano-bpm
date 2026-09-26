// Nano backend for the two-backend parity runner (#1260).
//
// Drives the `engine-wasm` read-model `TestEngine` in-process. The read-model
// engine is the gateway's C8-style REST read surface compiled to wasm, so its
// `searchVariables` returns the identical `{ name, value }` shape as Zeebe's
// `/v2/variables/search` — the two variable surfaces normalise the same way.
//
// The `TestEngine` runs every command to BPMN run-to-completion (RTC)
// quiescence synchronously, so nano needs no timing serialisation at all: after
// each command the engine is quiescent. It exposes the full event stream, so
// nano authoritatively `provides` every observation field.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  bump,
  emptyObservation,
  variablesFromSearchItems,
} from "./observation.mjs";

const PKG_JS = fileURLToPath(
  new URL(
    "../../../engine-wasm/pkg/readmodel/nanobpmn_engine.js",
    import.meta.url,
  ),
);
const PKG_WASM = fileURLToPath(
  new URL(
    "../../../engine-wasm/pkg/readmodel/nanobpmn_engine_bg.wasm",
    import.meta.url,
  ),
);

let modulePromise;
async function loadModule() {
  if (!modulePromise) {
    modulePromise = (async () => {
      const mod = await import(PKG_JS);
      // `--target web` glue init()s via fetch, which does not work for a
      // `file://` wasm under Node; initSync with the raw bytes does.
      mod.initSync({ module: readFileSync(PKG_WASM) });
      return mod;
    })();
  }
  return modulePromise;
}

export class NanoBackend {
  static id = "nano";

  constructor() {
    this.provides = new Set([
      "completed",
      "variables",
      "completedElements",
      "sequenceFlows",
      "jobsCreated",
      "incidents",
    ]);
  }

  async init() {
    const mod = await loadModule();
    this.engine = new mod.TestEngine();
    return this;
  }

  async deploy(xml) {
    this.engine.deploy(xml);
  }

  async start(processId, variables) {
    const res = JSON.parse(
      this.engine.createInstance(processId, JSON.stringify(variables ?? {})),
    );
    return { processInstanceKey: String(res.created) };
  }

  async activateAndComplete(_handle, jobType, variables) {
    // RTC is synchronous, so the job is already `Created` and activatable.
    const jobs = JSON.parse(
      this.engine.activateJobs(jobType, Number.MAX_SAFE_INTEGER, 60_000, "parity-runner"),
    );
    for (const job of jobs) {
      this.engine.completeJob(String(job.key), JSON.stringify(variables ?? {}));
    }
    return jobs.length;
  }

  async correlateMessage(_handle, name, correlationKey, variables) {
    this.engine.correlateMessage(name, correlationKey, JSON.stringify(variables ?? {}));
  }

  async broadcastSignal(_handle, name, variables) {
    this.engine.broadcastSignal(name, JSON.stringify(variables ?? {}));
  }

  async advanceTime(_handle, ms) {
    this.engine.advanceTime(ms);
  }

  async observe(handle) {
    const pik = handle.processInstanceKey;
    const events = JSON.parse(this.engine.events());
    const obs = emptyObservation();

    for (const ev of events) {
      if (String(ev.instance_key) !== pik) continue;
      switch (ev.type) {
        case "ElementCompleted":
          bump(obs.completedElements, ev.element_id);
          break;
        case "SequenceFlowTaken":
          bump(obs.sequenceFlows, `${ev.from}->${ev.to}`);
          break;
        case "JobCreated":
          bump(obs.jobsCreated, ev.job_type);
          break;
        case "ProcessInstanceCompleted":
          obs.completed = true;
          break;
        default:
          break;
      }
    }

    const snapshot = JSON.parse(this.engine.snapshot());
    for (const incident of snapshot.incidents ?? []) {
      if (String(incident.processInstanceKey ?? incident.instance_key) === pik) {
        bump(obs.incidents, incident.type ?? incident.errorType ?? "UNKNOWN");
      }
    }

    const vars = JSON.parse(
      this.engine.searchVariables(JSON.stringify({ processInstanceKey: pik })),
    );
    obs.variables = variablesFromSearchItems(vars.items);

    return obs;
  }

  async reset() {
    this.engine.reset();
  }

  async close() {
    // in-process; nothing to tear down.
  }
}
