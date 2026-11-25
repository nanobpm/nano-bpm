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
      "cancelled",
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
    // Parity: the Camunda adapter THROWS when its long-poll yields no job of this
    // type, so nano must fail identically instead of silently `return 0`ing. A
    // step with no activatable job means the two backends did not execute the same
    // step — a malformed or regressed scenario — and must fail loudly, not slip
    // through to pass its oracle on a mismatched run (#1260 review).
    if (jobs.length === 0) {
      throw new Error(
        `no '${jobType}' job was activatable — the nano engine reached RTC ` +
          `quiescence without creating this job (the Camunda adapter throws in ` +
          `the same situation). A parity step must drive the same job on both ` +
          `backends; a malformed or regressed scenario must fail, not proceed.`,
      );
    }
    for (const job of jobs) {
      this.engine.completeJob(String(job.key), JSON.stringify(variables ?? {}));
    }
    return jobs.length;
  }

  // Complete EXACTLY ONE job of `jobType` (#1246): the bounded-exhaustive
  // scenarios address a single job activation, so a run that holds two
  // same-type jobs (a parallel split through an exclusive merge) is still
  // driven deterministically — one completion per step, in the reference's
  // order. Throws like `activateAndComplete` when no job is activatable.
  async completeOneJob(_handle, jobType, variables) {
    const jobs = JSON.parse(
      this.engine.activateJobs(jobType, 1, 60_000, "parity-runner"),
    );
    if (jobs.length === 0) {
      throw new Error(
        `no '${jobType}' job was activatable — the nano engine reached RTC ` +
          `quiescence without creating this job (the Camunda adapter throws in ` +
          `the same situation). A parity step must drive the same job on both ` +
          `backends; a malformed or regressed scenario must fail, not proceed.`,
      );
    }
    this.engine.completeJob(String(jobs[0].key), JSON.stringify(variables ?? {}));
    return 1;
  }

  // Set (override) process-instance variables mid-run (#1246): a per-activation
  // route schedule is delivered as the gateway's `route_<id>` list before the
  // activation that reads it. Nano's setVariables applies to the instance's
  // root scope synchronously (RTC), so the next step's conditions read it.
  async setVariables(handle, variables) {
    this.engine.setVariables(
      handle.processInstanceKey,
      JSON.stringify(variables ?? {}),
      false,
    );
  }

  async correlateMessage(_handle, name, correlationKey, variables) {
    this.engine.correlateMessage(name, correlationKey, JSON.stringify(variables ?? {}));
  }

  // A BUFFERED correlation (#1246): the message reaches the subscription that the
  // previous step opens, however late it opens. Nano runs every command to RTC
  // quiescence synchronously, so that subscription is already open when this
  // step runs and an immediate correlation is the same thing — the barrier the
  // Camunda adapter needs (`settled`) is free here: the correlation itself has
  // already settled the follow-on work by the time it returns.
  async publishMessage(handle, name, correlationKey, variables) {
    return this.correlateMessage(handle, name, correlationKey, variables);
  }

  async broadcastSignal(_handle, name, variables) {
    this.engine.broadcastSignal(name, JSON.stringify(variables ?? {}));
  }

  async advanceTime(_handle, ms) {
    this.engine.advanceTime(ms);
  }

  // Cancel (terminate) the running instance (#1673): every token is discarded,
  // pending jobs/timers/subscriptions cancelled, and the instance transitions
  // to terminated — the `ProcessInstanceTerminated` event `observe` reads.
  async cancelInstance(handle) {
    this.engine.cancelInstance(handle.processInstanceKey);
  }

  async observe(handle) {
    const pik = handle.processInstanceKey;
    const events = JSON.parse(this.engine.events());
    const obs = emptyObservation();
    // Interrupted (terminated) element instances (#1673). nano's journal has
    // no element-termination event: an activity interrupted by a boundary
    // event is closed with `ElementCompleting`/`ElementCompleted` right after
    // its job's `JobCanceled`. Zeebe records that as ELEMENT_TERMINATED, never
    // a completion — so an element instance whose job was cancelled is not
    // counted as completed here. (Whole-instance cancellation emits no
    // per-element `ElementCompleted`, so it needs no translation.)
    const jobElement = new Map();
    const terminated = new Set();

    for (const ev of events) {
      if (String(ev.instance_key) !== pik) continue;
      switch (ev.type) {
        case "JobCanceled":
          if (jobElement.has(String(ev.job_key))) terminated.add(jobElement.get(String(ev.job_key)));
          break;
        case "ElementCompleted":
          if (terminated.has(String(ev.element_instance_key))) break;
          bump(obs.completedElements, ev.element_id);
          break;
        case "SequenceFlowTaken":
          bump(obs.sequenceFlows, `${ev.from}->${ev.to}`);
          break;
        case "JobCreated":
          jobElement.set(String(ev.job_key), String(ev.element_instance_key));
          bump(obs.jobsCreated, ev.job_type);
          break;
        case "ProcessInstanceCompleted":
          obs.completed = true;
          break;
        case "ProcessInstanceTerminated":
          // The instance was terminated (a `cancelInstance` step, #1673) —
          // distinct from completion: a terminated instance never completes.
          obs.cancelled = true;
          break;
        default:
          break;
      }
    }

    const snapshot = JSON.parse(this.engine.snapshot());
    Object.assign(obs.incidents, incidentsFromSnapshot(snapshot, pik));

    // `searchVariables` paginates: follow `endCursor` until the last page so
    // an instance with more rows than one page is observed in full.
    const varItems = [];
    let after;
    for (;;) {
      const page = { limit: 10000 };
      if (after !== undefined) page.after = after;
      const res = JSON.parse(
        this.engine.searchVariables(
          JSON.stringify({ filter: { processInstanceKey: String(pik) }, page }),
        ),
      );
      varItems.push(...res.items);
      const next = res.page?.endCursor;
      if (res.items.length === 0 || next == null || next === after) break;
      after = next;
    }
    obs.variables = rootVariablesFromSearch(varItems, pik);
    // The bounded-exhaustive scenarios drive routing through `route_*` start /
    // set variables — the driver's own schedule, not a process outcome. Drop
    // them from the observation so the oracle compares the variables the
    // PROCESS produced (#1246).
    for (const k of Object.keys(obs.variables)) {
      if (k.startsWith("route_")) delete obs.variables[k];
    }

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
