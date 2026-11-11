import type { BojtosControls, JobHandler, Snapshot } from "@nanobpm/bojtos-react";
import convergenceLoopBpmn from "./convergence-loop.bpmn?raw";
import publishVideoBpmn from "./publish-video.bpmn?raw";
import reviewBpmn from "./review.bpmn?raw";
import { makeWorkers, type ReviewStep } from "./workers";

export type LogKind = "submit" | "task" | "review" | "msg" | "done";

/** What a scenario's driver may do: engine commands, narration, paced waits. */
export interface DriveCtx {
  bojtos: BojtosControls;
  /** The deployed process id (from the live deployment). */
  processId: string;
  log(kind: LogKind, text: string): void;
  /**
   * Variables a command set that the snapshot may never show: completing the
   * last user task finishes the instance in the same command, and a completed
   * instance's variables leave the snapshot.
   */
  noteVars(vars: Record<string, unknown>): void;
  /**
   * Sleep one animation beat, holding while paused. Rejects with
   * {@link Cancelled} once the run is cancelled, so no driver can act on a
   * replacement session after a wait — every `await beat()` is a cancellation
   * point by construction, not by a per-call-site check.
   */
  beat(): Promise<void>;
  cancelled(): boolean;
}

/**
 * One runnable demo: a real BPMN model plus a scripted stand-in for the agents
 * and humans in it. The engine underneath is never scripted — every token move,
 * gateway decision and variable update is the wasm engine executing the model.
 */
export interface Scenario {
  id: string;
  /** Picker label. */
  label: string;
  /** Live header, after "Live: ". */
  title: string;
  bpmn: string;
  /** A long, flat model: stack the diagram full-width above the panels. */
  wide?: boolean;
  /** Runs one instance to completion (or until cancelled). */
  drive(ctx: DriveCtx): Promise<void>;
}

/** Thrown by {@link DriveCtx.beat} when the run was cancelled during the wait. */
export class Cancelled extends Error {
  constructor() {
    super("scenario run cancelled");
  }
}

/** Safety bound on worker rounds per run, so a model change can't spin forever. */
const MAX_ROUNDS_PER_RUN = 80;

// ── PR review convergence loop (urban-pr-review) ─────────────────────────────

const PR_NUMBER = 4821;
const PR_KEY = `pr-${PR_NUMBER}`;

const prReview: Scenario = {
  id: "pr-review",
  label: "Urban app: PR review loop",
  title: "PR review convergence loop",
  bpmn: convergenceLoopBpmn,
  async drive({ bojtos, processId, log, beat, cancelled }) {
    const counter = { i: 0 };
    const workers = makeWorkers(counter, (step: ReviewStep, round: number) =>
      log("review", `Round ${round + 1} — ${reviewLabel(step)}: ${step.summary}`),
    );
    log("submit", `PR #${PR_NUMBER} submitted for review`);
    let snap = bojtos.createInstance(
      processId,
      JSON.stringify({ prNumber: PR_NUMBER, prKey: PR_KEY, round: 0, maxRounds: 3 }),
    );
    await beat();
    let guard = 0;
    while (!cancelled() && snap && snap.completedInstances < 1 && guard++ < MAX_ROUNDS_PER_RUN) {
      snap = (await bojtos.stepWorkers(workers))?.snapshot ?? snap;
      if (cancelled()) return;
      await beat();
      // Auto-publish the messages a real app would, keyed by prKey.
      if (snap.activeElementIds.includes("wait-review")) {
        log("msg", "App published message → review-ready");
        snap = bojtos.correlateMessage("review-ready", PR_KEY, "{}") ?? snap;
        await beat();
      } else if (snap.activeElementIds.includes("wait-answer")) {
        log("msg", "Human answered escalation → escalation-answered");
        snap =
          bojtos.correlateMessage(
            "escalation-answered",
            PR_KEY,
            JSON.stringify({ answer: "soft-delete" }),
          ) ?? snap;
        await beat();
      }
    }
    if (!cancelled() && done(snap)) log("done", "Converged — pull request approved ✅");
  },
};

function reviewLabel(step: ReviewStep): string {
  if (step.status === "converged") return "approved";
  if (step.status === "needs_input") return "needs input";
  return "changes requested";
}

// ── Nano Agent Graph Engine examples (compiled from website/snippets/*.ts) ────

/** A scripted human answer at a user task. */
interface Answer {
  log: string;
  vars: Record<string, unknown>;
}

/**
 * Drive a flow that has user tasks: step the scripted workers, and whenever a
 * user task is waiting, complete it with the next scripted answer for that element.
 */
async function driveFlow(
  { bojtos, processId, log, noteVars, beat, cancelled }: DriveCtx,
  run: {
    start: Record<string, unknown>;
    startLog: string;
    workers: Record<string, JobHandler>;
    answers: Record<string, Answer[]>;
    doneLog: string;
  },
) {
  const asked: Record<string, number> = {};
  // Collect what each worker returns in the current step. Only the step that
  // ENDS the instance is noted (DriveCtx.noteVars): its root writes are the
  // ones no later snapshot shows. While the instance runs, the engine snapshot
  // is the source of truth, so branch-local outputs of multi-instance children
  // (e.g. review's `found` / `verdict`) never reach the root variables panel.
  let stepResults: Record<string, unknown>[] = [];
  const workers: Record<string, JobHandler> = {};
  for (const [type, handler] of Object.entries(run.workers)) {
    workers[type] = async (job) => {
      const result = await handler(job);
      if (result && typeof result === "object") stepResults.push(result as Record<string, unknown>);
      return result;
    };
  }
  log("submit", run.startLog);
  let snap = bojtos.createInstance(processId, JSON.stringify(run.start));
  await beat();
  let guard = 0;
  while (!cancelled() && snap && snap.completedInstances < 1 && guard++ < MAX_ROUNDS_PER_RUN) {
    stepResults = [];
    snap = (await bojtos.stepWorkers(workers))?.snapshot ?? snap;
    if (cancelled()) return;
    if (done(snap)) for (const r of stepResults) noteVars(r);
    await beat();
    const task = snap.userTasks.find((t) => t.state === "Created" && run.answers[t.elementId]);
    if (task) {
      const list = run.answers[task.elementId];
      const n = asked[task.elementId] ?? 0;
      asked[task.elementId] = n + 1;
      const answer = list[Math.min(n, list.length - 1)];
      log("msg", answer.log);
      snap = bojtos.completeUserTask(task.key, JSON.stringify(answer.vars)) ?? snap;
      if (done(snap)) noteVars(answer.vars);
      await beat();
    }
  }
  if (!cancelled() && done(snap)) log("done", run.doneLog);
}

// review (website/snippets/nage-review.ts)

const FILES = ["src/api/orders.ts", "src/billing/invoice.ts", "src/auth/session.ts"];
const LENSES = ["correctness", "contracts and callers", "tests", "duplicated logic"];

const OLD_CALLER = "invoice.ts still calls createOrder() with the old signature";

/** What each lens raises, pass by pass. Every reviewer sees the whole changeset. */
const RAISED: Record<string, string[][]> = {
  correctness: [["quantity is not validated in createOrder()", OLD_CALLER, "session token is logged"], [], []],
  "contracts and callers": [[OLD_CALLER], [OLD_CALLER], []],
  tests: [["no test covers quantity = 0"], [], []],
  "duplicated logic": [["tax rounding is duplicated in orders.ts and invoice.ts"], [], []],
};

/** Findings the adversarial challenger refutes (with its reason). */
const REFUTED: Record<string, string> = {
  "session token is logged": "only in a test fixture",
  "tax rounding is duplicated in orders.ts and invoice.ts": "both call the shared roundTax()",
};

export const REVIEW_ANSWERS: Answer[] = [
  {
    log: "You answered the escalation → fix: “add an overload, deprecate the old one”",
    vars: { decision: "fix", guidance: "Add an overload and deprecate the old signature" },
  },
];

/**
 * Scripted stand-ins for the review flow's agents. Pass 1: four lenses raise six
 * findings, triage merges a duplicate, the challenger refutes two, the fix agent
 * fixes two and escalates the API change. Pass 2: it applies your guidance.
 * Pass 3: clean.
 */
export function makeReviewWorkers(log: DriveCtx["log"]): Record<string, JobHandler> {
  let pass = -1;
  return {
    "review:changeset": () => {
      pass += 1;
      log("task", `git diff: ${FILES.length} files changed; ${LENSES.length} reviewers, one per lens`);
      return { files: FILES, lenses: LENSES };
    },
    // Reads the branch-local `lens` (the MI input element): parallel branches
    // share instance scope for anything they don't declare locally.
    "senior:review": (job) => {
      const lens = String(job.variables.lens);
      const found = RAISED[lens]?.[pass] ?? [];
      log("review", `Reviewer (${lens}): ${found.length ? found.join("; ") : "no issues"}`);
      return { found };
    },
    "review:triage": (job) => {
      const reviews = (job.variables.reviews as string[][] | undefined) ?? [];
      const raised = reviews.flat();
      const findings = [...new Set(raised)];
      if (raised.length > findings.length) {
        log("task", `Triage: ${raised.length} findings, ${findings.length} after merging duplicates`);
      }
      return { findings };
    },
    "senior:challenge": (job) => {
      const finding = String(job.variables.finding);
      const refuted = REFUTED[finding];
      log("review", refuted ? `Challenger refuted “${finding}”: ${refuted}` : `Challenger upheld “${finding}”`);
      return { verdict: refuted ? "" : finding };
    },
    "senior:fix": (job) => {
      const upheld = ((job.variables.verdicts as string[] | undefined) ?? []).filter(Boolean);
      if (upheld.length === 0) {
        log("review", "Fix agent: nothing left to fix");
        return { fixState: "clean" };
      }
      if (pass === 0) {
        log("review", "Fix agent added validation and a test; escalating the API change");
        return {
          fixState: "stuck",
          question: "Fixing invoice.ts means changing createOrder(), which the mobile app also calls. Bump the API, or add an overload?",
        };
      }
      log("review", "Fix agent added the overload, deprecated the old signature and pushed");
      return { fixState: "fixed" };
    },
  };
}

const review: Scenario = {
  id: "review",
  label: "Review a branch",
  title: "review agent graph",
  bpmn: reviewBpmn,
  wide: true,
  drive: (ctx) =>
    driveFlow(ctx, {
      start: {},
      startLog: "Started review on feat/orders",
      workers: makeReviewWorkers(ctx.log),
      answers: { escalate: REVIEW_ANSWERS },
      doneLog: "Clean review, every finding fixed — ready to merge ✅",
    }),
};

// publish-video (website/snippets/publish-video.ts)

const VIDEO = "/Users/you/Movies/launch-talk.mp4";

/** The human's answers at the `review` user task, in order. */
export const VIDEO_REVIEWS: Answer[] = [
  {
    log: "You reviewed the draft → redo: “Lead with the live demo”",
    vars: { decision: "redo", feedback: "Lead with the live demo", thumbnail: "none" },
  },
  {
    log: "You reviewed the redo → upload, thumbnail 2",
    vars: { decision: "upload", thumbnail: "2" },
  },
];

/**
 * Scripted stand-ins for publish-video's steps. The first description draft
 * fails its check once, so the run shows the agent retry loop as well as the
 * human redo loop.
 */
export function makeVideoWorkers(log: DriveCtx["log"]): Record<string, JobHandler> {
  let describeChecks = 0;
  return {
    "publish-video:transcribe": () => {
      log("task", "Local Whisper model transcribed 14:32 of audio");
      return { title: "launch-talk", srtPath: "launch-talk.srt" };
    },
    "publish-video:correct": () => {
      log("task", "Fixed 3 misheard product names");
      return { corrections: 3 };
    },
    "senior:describe": () => {
      log("review", "Agent drafted the title, description, chapters and tags");
      return {};
    },
    "publish-video:check-description": () => {
      describeChecks += 1;
      if (describeChecks === 1) {
        log("msg", "Check failed: no CHAPTERS block → back to the agent");
        return { describeState: "retry", errors: "no CHAPTERS block" };
      }
      log("task", "Description checked");
      return {
        ytTitle: "Agent graphs on your laptop — launch talk",
        ytDescription: "How to run an agent fleet on one laptop…",
        describeState: "done",
        errors: "",
      };
    },
    "senior:thumbnails": () => {
      log("review", "Agent proposed 3 thumbnails");
      return { thumbnailCount: 3 };
    },
    "publish-video:upload": () => ({ youtubeUrl: "https://youtu.be/…" }),
    "publish-video:save": () => ({}),
  };
}

const publishVideo: Scenario = {
  id: "publish-video",
  label: "Publish a video",
  title: "publish-video agent graph",
  bpmn: publishVideoBpmn,
  wide: true,
  drive: (ctx) =>
    driveFlow(ctx, {
      start: { videoPath: VIDEO },
      startLog: `Started publish-video for ${VIDEO.split("/").pop()}`,
      workers: makeVideoWorkers(ctx.log),
      answers: { review: VIDEO_REVIEWS },
      doneLog: "Uploaded to YouTube as private ✅",
    }),
};

function done(snap: Snapshot | null): boolean {
  return !!snap && snap.completedInstances >= 1;
}

export const SCENARIOS: Scenario[] = [review, publishVideo, prReview];

/** `?scenario=<id>` selects the starting scenario; unknown ids fall back to the first. */
export function initialScenario(search: string): Scenario {
  const id = new URLSearchParams(search).get("scenario");
  return SCENARIOS.find((s) => s.id === id) ?? SCENARIOS[0];
}
