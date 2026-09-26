// Nano backend for the two-backend parity runner (#1260).
//
// Drives the `engine-wasm` read-model `TestEngine` in-process. Final process
// variables are read from the read-model `searchVariables` surface, but
// restricted to the ROOT process-instance scope (`scopeKey == processInstanceKey`)
// so the observation matches Camunda's `awaitCompletion` response — the C8
// completion response carries the root-scope variables, NOT nested subprocess /
// call-activity scopes (#1260 review). (The snapshot's `instance.variables` is
// unusable here: it is emptied once the instance completes, which is exactly the
// state the parity runner observes.)
//
// `searchVariables` truncates long values (`isTruncated`) and has no untruncated
// opt-out yet, so a truncated value cannot be compared faithfully. Rather than
// emit a lying (truncated) observation that would silently diverge from Camunda's
// full value, `rootVariablesFromSearch` throws on a truncated root-scope
// variable: adding an untruncated, scope-filtered variable API to engine-wasm is
// the documented extension surface for long-value scenarios (#1260 review).
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
  MAX_JOBS_TO_ACTIVATE,
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
    // Use the SAME activation cap as the Camunda backend so both engines drive
    // the identical step sequence on a model with many same-type jobs (#1260).
    const jobs = JSON.parse(
      this.engine.activateJobs(jobType, MAX_JOBS_TO_ACTIVATE, 60_000, "parity-runner"),
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
    Object.assign(obs.incidents, incidentsFromSnapshot(snapshot, pik));

    const vars = JSON.parse(
      this.engine.searchVariables(JSON.stringify({ processInstanceKey: pik })),
    );
    obs.variables = rootVariablesFromSearch(vars.items, pik);

    return obs;
  }

  async reset() {
    this.engine.reset();
  }

  async close() {
    // in-process; nothing to tear down.
  }
}

/**
 * The active-incident multiset for one process instance, read from a read-model
 * snapshot. The snapshot's `IncidentDto` is camelCased (`engine-wasm/src/lib.rs`
 * `IncidentDto`), so the instance is keyed by `instanceKey` and the incident
 * class by `kind` — NOT `processInstanceKey`/`instance_key` or `type`/`errorType`
 * (which never exist, so a mis-read silently records every incident under
 * `UNKNOWN` and matches no instance — #1260 review).
 */
export function incidentsFromSnapshot(snapshot, pik) {
  const counts = {};
  for (const incident of snapshot.incidents ?? []) {
    if (String(incident.instanceKey) !== String(pik)) continue;
    bump(counts, incident.kind ?? "UNKNOWN");
  }
  return counts;
}

/**
 * The complete, root-scope variables for one process instance, read from the
 * read-model `searchVariables` items. Only the root scope
 * (`scopeKey == processInstanceKey == pik`) is kept, matching Camunda's
 * `awaitCompletion` response. Throws on a truncated root-scope value: the search
 * surface has no untruncated opt-out yet, so a truncated preview cannot be
 * compared faithfully against Camunda's full value and must fail loudly rather
 * than silently diverge (#1260 review).
 */
export function rootVariablesFromSearch(items, pik) {
  const rootItems = (items ?? []).filter(
    (i) =>
      String(i.processInstanceKey) === String(pik) &&
      String(i.scopeKey) === String(pik),
  );
  for (const item of rootItems) {
    if (item.isTruncated) {
      throw new Error(
        `variable '${item.name}' is truncated by the read-model search surface; ` +
          `the parity observation cannot faithfully compare it against Camunda's ` +
          `full value. Add an untruncated, scope-filtered variable API to ` +
          `engine-wasm before using long values in a scenario.`,
      );
    }
  }
  return variablesFromSearchItems(rootItems);
}
