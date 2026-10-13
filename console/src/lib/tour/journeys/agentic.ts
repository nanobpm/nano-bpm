// The agentic-SDLC journeys — "orchestrate a coding-agent harness" (ADR 0049 §6,
// journeys 0a/0b; ADR 0046 agent-as-worker).
//
// These are the two journeys that make Nano's actual pitch concrete: you author a
// *durable* multi-round loop around an external coding-agent harness — the agent
// does the work, the engine owns the waiting — then you hire a harness against it
// and watch an instance park on a real, engine-visible human-approval catch event.
//
// The split is deliberate and load-bearing. ADR 0049 caps a journey at five steps
// and forbids padding a detour into one, so the single "close the agentic loop"
// story is two journeys divided exactly at the terminal boundary:
//
//   - 0a `agentic-author` — five steps, entirely in the console: scaffold the
//     Code-first workflow template, read its three verbs, see the BPMN the engine
//     DERIVED from that code (the differentiator over code-only durable-execution
//     engines), and Run it into a deployed, polling worker host.
//   - 0b `agentic-hire` — three steps spanning the console AND the terminal:
//     hire a harness against the job type the `w.task` step emits, start an
//     instance, watch it reach the agent step and park on the human signal, then
//     resume it to completion and hand off to the full reference app.
//
// Studio-only: both operate on a project workspace (author, model, run), which
// the observe build strips entirely (ADR 0034). Node-safety: the only runtime
// imports at module scope are the pure registry/anchors/precondition helpers; the
// browser-only API client (`../../gen`, `../../lib/api`) is reached through a
// dynamic `import()` INSIDE the context source, so importing this file to register
// its journeys — as the guard tests do — never drags an `EventSource`/fetch client
// into Node.

import { registerJourney } from "../registry.ts";
import { registerContextSource } from "../context.ts";
import { hasJsRuntime } from "../preconditions.ts";
import { TOUR_ANCHOR, templateAnchor, tourSelector } from "../tourAnchors.ts";
import type { Journey, Predicate, TourContext } from "../types";

/**
 * The console `workflow-starter` template (server-side `WORKFLOW_EXAMPLE_TS`)
 * scaffolds a flow with id `pr-review`: `w.run("fetchDiff")` →
 * `w.task("review")` (external, job type `pr-review:review`) →
 * `w.signal("humanApproval")` → `w.run("merge")`. Deriving the journey's success
 * signals from these exact strings — rather than restating them — keeps them
 * pinned to the template the journey actually scaffolds.
 */
const WORKFLOW_STARTER_TEMPLATE = "workflow-starter";
const PR_REVIEW_FLOW = "pr-review";

// ---------------------------------------------------------------------------
// Success predicates (pure — unit tested in agentic.test.ts).
// ---------------------------------------------------------------------------

/**
 * 0a succeeded when the worker host is actually up: the project reports running
 * AND its run log shows the deploy + the polling worker host. The template's
 * `main.ts` logs `deployed pr-review` and `worker host running against <url>`, so
 * matching either — pinned to the `pr-review` flow id rather than a bare
 * `deployed`, which any project's output could contain — distinguishes "the
 * process booted and is polling for jobs" from "the user merely pressed Run and
 * it errored out". Both halves are required: `running` alone flickers true during
 * a crash-loop start, and the log alone can be a stale tail from a previous run.
 */
export const agenticAuthorSucceeded: Predicate = (ctx: TourContext) => {
  const rs = ctx.runState;
  if (!rs?.running) return false;
  return /worker host running|deployed\s+pr-review/i.test(rs.output);
};

/**
 * 0b succeeded when an instance reached the agent step and then parked on the
 * human-approval catch event. The REST surface exposes no active-element list, so
 * the browser-only context source below synthesizes a `parked: humanApproval`
 * marker into the run output when it observes an active `pr-review` instance that
 * has advanced past its `review` job with no incident. This predicate stays pure:
 * it only reads that marker, so it is testable with a synthetic context and never
 * depends on the (best-effort, browser-only) observation that produced it.
 */
export const agenticHireSucceeded: Predicate = (ctx: TourContext) =>
  /parked:\s*humanApproval/i.test(ctx.runState?.output ?? "");

// ---------------------------------------------------------------------------
// Journeys.
// ---------------------------------------------------------------------------

export const agenticAuthor: Journey = {
  id: "agentic-author",
  title: "Author a durable agent loop",
  blurb:
    "Build a durable, multi-round loop around a coding-agent harness — with a human approval gate.",
  persona: "author a durable, human-gated loop around a coding-agent harness",
  profiles: ["studio"],
  successEvent: agenticAuthorSucceeded,
  nextJourneys: ["agentic-hire"],
  steps: [
    {
      kind: "note",
      id: "author-intro",
      title: "The agent does the work; the engine owns the waiting",
      body: "You are about to build a durable, multi-round loop around a coding-agent harness, with a human approval gate. The harness does the actual work; Nano owns the orchestration — the retries, the rounds, and the days-long wait for a human — as a real, crash-safe process.",
    },
    {
      kind: "spotlight",
      id: "author-template",
      route: "/projects",
      selector: tourSelector(templateAnchor(WORKFLOW_STARTER_TEMPLATE)),
      optional: true,
      title: "Start from the Code-first workflow",
      body: "New project → the Code-first workflow template. It scaffolds a runnable PR-review loop: a flow file plus a worker host, no diagram and no task-type wiring to hand-write.",
      side: "bottom",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "author-flow-file",
      selector: tourSelector(TOUR_ANCHOR.flowEditor),
      optional: true,
      title: "Three verbs, one loop",
      body: "The flow file is the whole payload: w.run is an app-hosted step, w.task is the EXTERNAL seam — your coding-agent harness — and w.signal is a durable, engine-visible wait for a human. That is the orchestration, authored as code.",
      side: "right",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "author-model-view",
      selector: tourSelector(TOUR_ANCHOR.modelView),
      optional: true,
      title: "You didn't draw this",
      body: "Open Model. The BPMN was DERIVED from your code — and that catch event is a real, engine-visible durable wait, not a comment. This is what a code-only durable-execution engine cannot show you: the loop as an inspectable model.",
      side: "bottom",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "author-run",
      selector: tourSelector(TOUR_ANCHOR.runOutput),
      optional: true,
      precondition: hasJsRuntime,
      title: "Deployed, and polling for work",
      body: "The run output shows the flow deployed and a worker host running — the engine is now polling for the pr-review:review job on the harness's behalf. Nothing runs the agent yet; the loop is durable and waiting. Next: hire one.",
      side: "top",
      align: "start",
      repair: {
        kind: "note",
        id: "author-run-repair",
        title: "First, a JavaScript runtime",
        body: "This template runs on Node, and the console doesn't see Node on your PATH yet. Install Node ≥ 22.6 (nodejs.org), reopen the project, and the worker host will boot from the run output.",
      },
    },
  ],
};

export const agenticHire: Journey = {
  id: "agentic-hire",
  title: "Hire an agent and close the loop",
  blurb:
    "Point a coding-agent harness at the loop, start an instance, and drive it through the human gate.",
  persona: "hire a coding agent and drive an instance through the human gate",
  profiles: ["studio"],
  successEvent: agenticHireSucceeded,
  steps: [
    {
      kind: "handoff",
      id: "hire-harness",
      title: "Hire a harness for the job",
      body: "In a terminal, hire a coding-agent harness and point it at the loop. The rank and capability you give it map to the job type the w.task step emits — for this template that job type is pr-review:review. `hire` registers the worker; `work` starts it polling.",
      copy: 'c8ctl nano hire --name coder --rank senior --command "claude" && c8ctl nano work coder',
      copyLabel: "Copy commands",
    },
    {
      kind: "spotlight",
      id: "hire-parks",
      route: "/explorer",
      selector: tourSelector(TOUR_ANCHOR.explorerNav),
      title: "It reaches the agent, then waits for you",
      body: "Start an instance and watch it in Explorer. It advances to the review step, the harness services it — then the instance PARKS on the humanApproval signal. That park is a durable catch event: the process is idle, costs nothing, and survives a restart while it waits for a human.",
      side: "right",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "hire-resume",
      route: "/explorer",
      selector: tourSelector(TOUR_ANCHOR.explorerNav),
      title: "Resume the signal, close the loop",
      body: "Publish the humanApproval signal and the parked instance resumes and completes the loop. That is the whole agentic pattern: durable orchestration around an external agent, gated by a human. For the full reference app, open urban-pr-review-codefirst — the same loop, production-shaped.",
      side: "right",
      align: "start",
    },
  ],
};

registerJourney(agenticAuthor);
registerJourney(agenticHire);

// ---------------------------------------------------------------------------
// Context source: `runState` for the scaffolded workflow-starter project.
//
// Browser-only and best-effort. It publishes `{ running, output }` for the
// journey's project so the pure predicates above have something to read. It never
// runs in Node (the guard tests import this module only for journey registration;
// this function is invoked solely by the runner in a browser), and every I/O path
// is guarded + dynamically imported so a failing fetch contributes nothing rather
// than taking down a sibling journey's preconditions (see enrichContext).
// ---------------------------------------------------------------------------

/** Bounded run-log + observation tail kept per project across context refreshes. */
interface RunTail {
  es?: EventSource;
  log: string;
  marker: string;
  lastPollMs: number;
  polling: boolean;
}

const MAX_TAIL = 8000;
const PARK_POLL_MS = 2500;
const tails = new Map<string, RunTail>();

function clip(s: string): string {
  return s.length > MAX_TAIL ? s.slice(-MAX_TAIL) : s;
}

/**
 * Close and drop every open tail except (optionally) one to keep. Called when the
 * journey's project is gone, changed, or we are outside studio — so a project
 * that disappears from `ctx.projects` (deleted, renamed, or an observe refresh)
 * never leaves an `EventSource` streaming forever.
 */
function closeTailsExcept(keep?: string): void {
  for (const [name, tail] of tails) {
    if (name === keep) continue;
    tail.es?.close();
    tails.delete(name);
  }
}

/** Ensure an SSE run-log tail is open while running, and closed once it stops. */
async function ensureTail(name: string, running: boolean): Promise<RunTail> {
  let tail = tails.get(name);
  if (!tail) {
    tail = { log: "", marker: "", lastPollMs: 0, polling: false };
    tails.set(name, tail);
  }
  if (running && !tail.es) {
    const { projectLogs } = await import("../../api");
    tail.es = projectLogs(name, (line) => {
      const t = tails.get(name);
      if (t) t.log = clip(t.log + line.text + "\n");
    });
  } else if (!running && tail.es) {
    tail.es.close();
    tail.es = undefined;
    tail.marker = "";
  }
  return tail;
}

/**
 * Job states the engine reports as still open — waiting for a worker (`Created`)
 * or picked up and in flight (`Activated`). A `/instances/{key}` detail lists a
 * process's jobs INCLUDING completed ones, so a state check is mandatory: without
 * it a serviced (completed) job reads as still open. Mirrors `InstanceDetail.tsx`'s
 * active-job filter. Anything not in this set (Completed, Failed, …) is not open.
 */
const OPEN_JOB_STATES = new Set(["Created", "Activated"]);

/** Minimal shapes the pure park decision needs (a subset of the gen types). */
interface ParkJob {
  job_type: string;
  state: string;
}
interface ParkInstance {
  state: string;
  has_incident: boolean;
}

/**
 * Pure decision: has this `pr-review` instance advanced past the agent step and
 * parked on the single downstream `humanApproval` catch event?
 *
 * REST exposes no active-element list, so we infer it structurally: a catch event
 * has NO job, so a live instance parked on one has no open job at all. The caller
 * scopes this to `pr-review` instances, so "active, no incident, no open job" can
 * only be the `humanApproval` wait.
 *
 * Checking *any* open job (not just the `review` one) matters in both directions:
 * an open job that is the not-yet-serviced `review` means still AT the agent step
 * (not parked), and an open job that is the downstream `merge` `w.run` means the
 * signal already resumed the instance (past the wait) — a review-only check would
 * false-positive there. The job-STATE filter is equally load-bearing: the detail
 * lists completed jobs, so a serviced `review` must read as closed or the marker,
 * and thus `agenticHireSucceeded`, is never satisfiable.
 */
export function isParkedOnHumanApproval(
  instance: ParkInstance,
  jobs: ParkJob[],
): boolean {
  if (/complete|terminat/i.test(instance.state)) return false;
  if (instance.has_incident) return false;
  const hasOpenJob = jobs.some((j) => OPEN_JOB_STATES.has(j.state));
  return !hasOpenJob;
}

/**
 * Best-effort "parked on humanApproval" detection against live REST. Debounced so
 * a fast-polling handoff step doesn't hammer `/instances`; the actual decision is
 * the pure `isParkedOnHumanApproval` above (unit tested).
 */
async function refreshParkMarker(tail: RunTail): Promise<void> {
  const now = Date.now();
  if (tail.polling || now - tail.lastPollMs < PARK_POLL_MS) return;
  tail.polling = true;
  tail.lastPollMs = now;
  try {
    const { listInstances, getInstance } = await import("../../../gen");
    const page = (
      await listInstances({ query: { pageSize: 50 }, throwOnError: true })
    ).data;
    const active = (page?.items ?? []).filter(
      (i) =>
        i.process_id === PR_REVIEW_FLOW && !/complete|terminat/i.test(i.state),
    );
    let parked = false;
    for (const inst of active) {
      if (inst.has_incident) continue;
      const detail = (
        await getInstance({ path: { key: inst.key }, throwOnError: true })
      ).data;
      if (isParkedOnHumanApproval(inst, detail?.jobs ?? [])) {
        parked = true;
        break;
      }
    }
    tail.marker = parked ? "\n[tour] parked: humanApproval\n" : "";
  } catch {
    // Best-effort: an unreachable /instances endpoint leaves the marker as-is.
  } finally {
    tail.polling = false;
  }
}

registerContextSource(async (ctx): Promise<Partial<TourContext>> => {
  // Studio-only: both agentic journeys are studio, so never open an SSE stream
  // or poll `/instances` in an observe build — `enrichContext` runs every source
  // regardless of profile, so the guard has to live here.
  if (ctx.profile !== "studio") {
    closeTailsExcept();
    return {};
  }

  const candidates = ctx.projects.filter(
    (p) => p.template === WORKFLOW_STARTER_TEMPLATE,
  );
  // `listProjects()` order is filesystem-dependent, so when several starter
  // projects exist, pick a stable, intent-aligned one: the running project the
  // journey is about, else the most recently touched.
  const project =
    candidates.find((p) => p.running) ??
    [...candidates].sort((a, b) => b.updatedMs - a.updatedMs)[0];
  if (!project) {
    closeTailsExcept();
    return {};
  }
  // Prune any tail left over from a different (renamed/removed) project.
  closeTailsExcept(project.name);
  const running = project.running;

  // Node / non-DOM: publish the flag we already have, no I/O.
  if (typeof EventSource === "undefined") {
    return { runState: { running, output: "" } };
  }

  const tail = await ensureTail(project.name, running);
  if (running) void refreshParkMarker(tail);
  return { runState: { running, output: tail.log + tail.marker } };
});
