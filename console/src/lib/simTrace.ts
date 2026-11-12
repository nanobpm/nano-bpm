import type {
  InstanceTrace,
  TraceElement,
  TraceIncident,
  TraceJob,
  TraceOutcome,
} from "../gen";
import type { WasmEvent } from "@nanobpm/bojtos-kit";

// `WasmEvent` (the shape of `engine.events()`) now lives in the engine contract
// package; re-export it so existing `../lib/simTrace` importers stay unchanged.
export type { WasmEvent };

function s(v: unknown): string {
  return v == null ? "" : String(v);
}

/// Fold the in-browser simulation's event log into the same `InstanceTrace`
/// shape the production Traces tab renders — so the modeler test-run can reuse
/// the exact timeline visualization. The simulation's virtual clock only moves
/// on an explicit "advance time", so timestamps here are the event **sequence
/// index** (a logical step), not wall-clock milliseconds; the timeline is given
/// a step formatter to label them honestly.
///
/// Mirrors the server-side fold in `server/src/console/trace.rs`: element lanes
/// come from `ElementActivating`/`ElementActivated`/`ElementCompleted`, the path
/// from the first sighting of each element, and the job queue/service split from
/// `JobCreated`/`JobActivated`/`JobCompleted`.
export function foldSimTrace(
  events: WasmEvent[],
  instanceKey: string,
  processId: string,
): InstanceTrace {
  const elements: TraceElement[] = [];
  const byEik = new Map<string, number>(); // element_instance_key -> index
  const jobToEik = new Map<string, string>(); // job_key -> element_instance_key
  const incidents: TraceIncident[] = [];
  const path: string[] = [];

  let startedAt = 0;
  let lastSeq = 0;
  let endedAt: number | null = null;
  let outcome: TraceOutcome = "active";

  const element = (eik: string, elementId: string, at: number): number => {
    let idx = byEik.get(eik);
    if (idx === undefined) {
      idx = elements.length;
      byEik.set(eik, idx);
      elements.push({
        elementId,
        elementInstanceKey: eik,
        scope: "0",
        enteredAt: at,
        exitedAt: null,
        durationMs: null,
        incidents: 0,
        job: null,
      });
    }
    return idx;
  };

  for (const e of events) {
    if (s(e.instance_key) !== instanceKey) continue;
    const at = e.seq;
    lastSeq = Math.max(lastSeq, at);
    switch (e.type) {
      case "ProcessInstanceCreated":
        startedAt = at;
        break;
      case "ElementActivating": {
        const eik = s(e.element_instance_key);
        const fresh = !byEik.has(eik);
        element(eik, s(e.element_id), at);
        if (fresh) path.push(s(e.element_id));
        break;
      }
      case "ElementActivated":
        element(s(e.element_instance_key), s(e.element_id), at);
        break;
      case "ElementCompleted": {
        const idx = byEik.get(s(e.element_instance_key));
        if (idx !== undefined) elements[idx].exitedAt = at;
        break;
      }
      case "JobCreated": {
        const eik = s(e.element_instance_key);
        const idx = element(eik, s(e.element_id), at);
        jobToEik.set(s(e.job_key), eik);
        const job: TraceJob = {
          type: s(e.job_type),
          worker: null,
          createdAt: at,
          activatedAt: null,
          completedAt: null,
          waitMs: null,
          queueMs: null,
          serviceMs: null,
          attempts: 0,
          failures: 0,
        };
        elements[idx].job = job;
        break;
      }
      case "JobActivated": {
        const eik = jobToEik.get(s(e.job_key));
        if (eik === undefined) break;
        const job = elements[byEik.get(eik)!].job;
        if (job && job.activatedAt == null) {
          job.activatedAt = at;
          job.worker = s(e.worker) || null;
          job.attempts += 1;
        }
        break;
      }
      case "JobCompleted": {
        const eik = jobToEik.get(s(e.job_key));
        if (eik === undefined) break;
        const job = elements[byEik.get(eik)!].job;
        if (job) job.completedAt = at;
        break;
      }
      case "JobFailed": {
        const eik = jobToEik.get(s(e.job_key));
        if (eik === undefined) break;
        const job = elements[byEik.get(eik)!].job;
        if (job) job.failures += 1;
        break;
      }
      case "IncidentRaised": {
        const eik = s(e.element_instance_key);
        const idx = byEik.get(eik);
        if (idx !== undefined) elements[idx].incidents += 1;
        incidents.push({
          elementId: s(e.element_id),
          elementInstanceKey: eik,
          kind: s(e.kind),
          reason: s(e.reason),
          raisedAt: at,
          resolvedAt: null,
        });
        break;
      }
      case "ProcessInstanceCompleted":
        endedAt = at;
        outcome = "completed";
        break;
      case "ProcessInstanceTerminated":
        endedAt = at;
        outcome = "terminated";
        break;
      default:
        break;
    }
  }

  // Derive durations (in logical steps).
  for (const el of elements) {
    if (el.exitedAt != null) el.durationMs = el.exitedAt - el.enteredAt;
    const j = el.job;
    if (j) {
      if (j.completedAt != null) j.waitMs = j.completedAt - j.createdAt;
      if (j.activatedAt != null) j.queueMs = j.activatedAt - j.createdAt;
      if (j.activatedAt != null && j.completedAt != null)
        j.serviceMs = j.completedAt - j.activatedAt;
    }
  }

  const trace: InstanceTrace = {
    instanceKey,
    processId,
    version: null,
    businessId: null,
    tags: [],
    startedAt,
    endedAt,
    durationMs: endedAt != null ? endedAt - startedAt : null,
    outcome,
    elements,
    incidents,
    path,
  };
  LAST_STEP.set(trace, Math.max(lastSeq, startedAt));
  return trace;
}

/// The latest folded event sequence per simulation trace. Every event (even
/// one that moves no element/job endpoint, e.g. `JobFailed`) advances the
/// simulation clock, so it is recorded at fold time rather than reconstructed
/// from endpoints.
const LAST_STEP = new WeakMap<InstanceTrace, number>();

/// Step formatter for the simulation timeline: logical steps, not milliseconds.
/// `now` is the trace's current step (its last event's sequence index), so an
/// unfinished simulated job headlines elapsed STEPS — never the browser's
/// wall-clock `Date.now()` (which in this domain would read as ~1.8 trillion
/// "steps"). No wall-clock tick: the simulation clock only moves when a new
/// event folds in, which already re-renders.
export const stepFmt = {
  duration: (n: number | null): string =>
    n == null ? "—" : `${n} step${n === 1 ? "" : "s"}`,
  clock: (n: number): string => `#${n}`,
  origin: "step 0",
  now: (trace: InstanceTrace): number => trace.endedAt ?? lastStep(trace),
};

/// The current simulation step: the latest folded event's sequence index.
function lastStep(trace: InstanceTrace): number {
  return LAST_STEP.get(trace) ?? trace.startedAt;
}
