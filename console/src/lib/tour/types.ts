// The guided-journey contract (ADR 0049).
//
// A *journey* is a short, outcome-shaped onboarding path: at most five steps
// that leave a real result behind (a project that ran, an instance that
// completed, a served page that answered) rather than a set of tooltips naming
// tabs. Journeys replace the profile-keyed step list the tour spike shipped,
// because the build profile is NOT the persona: `studio` and `observe` are a
// build-time split, while the users who matter most all land in the *same*
// studio build wanting materially different first sessions.
//
// Everything here is deliberately data, not behaviour, with one exception:
// preconditions and success predicates are functions over a `TourContext`
// snapshot. That is what lets a step ask "would this action actually work?"
// instead of promising something the console already knows it cannot deliver.
//
// Type-only imports throughout: this module must stay free of runtime imports
// so `../profile` (which reads the Vite-injected `__STUDIO__` global at import
// time) is never pulled into a Node unit test.

import type { ConsoleProfile } from "../profile";
import type { Extension, ProjectSummary, ProjectTemplate } from "../../gen";

/**
 * A snapshot of everything a precondition or success predicate may read.
 *
 * Most of it comes from ONE `listProjects()` call, which already returns the
 * projects, both runtime-availability flags, the template menu and the
 * installed extensions.
 *
 * Fields below the required block are **optional on purpose**: they are
 * contributed by whichever slice needs them (topology adds `nodeCount`, the
 * agentic journey adds `runState`, #404's consumer panel adds `consumers`), so
 * every predicate MUST tolerate `undefined` rather than assume a fully
 * populated context. See `registerContextSource` in ./context.ts.
 */
export interface TourContext {
  profile: ConsoleProfile;
  /** Current react-router pathname. */
  route: string;
  projects: ProjectSummary[];
  nodeAvailable: boolean;
  templates: ProjectTemplate[];
  extensions: Extension[];
  /** Cluster size from topology; undefined until a source supplies it. */
  nodeCount?: number;
  /** Run state of the project a journey is working in. */
  runState?: { running: boolean; output: string };
  /** Number of captured traces, for gating the optional trace steps. */
  traceCount?: number;
  /**
   * External job-worker consumers the engine currently sees (#404). Lets a
   * handoff step verify that a hired agent harness actually connected.
   */
  consumers?: { jobType: string; worker: string }[];
  /**
   * Journey-scoped scratch space — e.g. the name of the project a journey just
   * scaffolded, so a later step can assert against it. Cleared when a journey
   * starts.
   */
  scratch: Record<string, unknown>;
}

/** A pure question asked of a context snapshot. Must tolerate absent optionals. */
export type Predicate = (ctx: TourContext) => boolean;

/**
 * What a precondition decided:
 *
 * - `ok`     — show the step as authored.
 * - `skip`   — omit it entirely (nothing useful to show: no traces yet).
 * - `repair` — show the step's `repair` replacement instead. This is the case
 *   that matters: the target exists, but the action would fail, so the honest
 *   step is "here is how to get a runtime", never "hit Run".
 */
export type PreconditionResult = "ok" | "skip" | "repair";

export interface Precondition {
  /** Stable id, so a skip/repair decision is identifiable in analytics. */
  id: string;
  test: (ctx: TourContext) => PreconditionResult;
}

interface StepBase {
  /** Stable across edits — this is the analytics key. */
  id: string;
  title: string;
  body: string;
  /** Navigate here before showing the step. Must be an absolute path. */
  route?: string;
  precondition?: Precondition;
  /** Substituted when `precondition` returns "repair". */
  repair?: Step;
  /** Advisory: a step whose absence does not weaken the journey. */
  optional?: boolean;
}

/** Highlights a `data-tour` anchor. */
export interface SpotlightStep extends StepBase {
  kind: "spotlight";
  /** Derive with `tourSelector(TOUR_ANCHOR.x)` — never a literal string. */
  selector: string;
  side?: "top" | "right" | "bottom" | "left";
  align?: "start" | "center" | "end";
}

/** Anchorless, centered — framing, with nothing to point at. */
export interface NoteStep extends StepBase {
  kind: "note";
}

/**
 * Hands off to something outside the console: a terminal command to run, or a
 * URL to open. The net-new step kind, and the reason the headless-local-dev and
 * agent-hiring journeys can be expressed honestly at all — neither of them
 * happens entirely inside the console.
 *
 * `copy` is rendered as inert, explicitly-copied text. Nothing here is ever
 * executed; the console has no shell, and a pack-contributed handoff step
 * (ADR 0049 §7) must never become an execution vector.
 */
export interface HandoffStep extends StepBase {
  kind: "handoff";
  /** The command or URL offered for copying. */
  copy: string;
  /** Button label; defaults to "Copy". */
  copyLabel?: string;
  /**
   * Fired at most once per rendered handoff, after the user actually copies
   * `copy` (a successful copy, not a mere step advance). Lets a first-party
   * journey latch its outcome signal — e.g. journey 1's "the base URL was
   * taken" — from the real copy action in the journey flow rather than
   * depending on a separate affordance. Absent from pack-contributed handoff
   * steps by construction: those arrive as JSON data and cannot carry a
   * function, so this is never a pack execution vector. The renderer swallows
   * any throw, so a latch failing never breaks the copy UX.
   */
  onCopied?: () => void;
  /**
   * When present, the step auto-advances as soon as this becomes true (polled
   * while the step is showing). When absent, the user self-reports via
   * "I've done it" — honest, since the console cannot observe a terminal.
   */
  verify?: Predicate;
}

export type Step = SpotlightStep | NoteStep | HandoffStep;

export interface Journey {
  id: string;
  title: string;
  /** One line for the picker card (#411). */
  blurb: string;
  /**
   * First-person outcome for the startup persona panel (#464), completing the
   * sentence "I want to …" (e.g. "run a local Camunda-compatible engine"). The
   * panel lists offerable journeys as personas the user self-selects, replacing
   * the CLI's `?tour=` link-spray. Falls back to `title` when absent.
   */
  persona?: string;
  /** Profiles this journey is offered in — a filter, no longer the branch. */
  profiles: ConsoleProfile[];
  /** Journey-level gate: not offered at all when any of these is not `ok`. */
  preconditions?: Precondition[];
  /** At most five. A journey needing a detour is split, not padded. */
  steps: Step[];
  /**
   * What must actually have happened for this journey to have worked. Recorded
   * alongside completion so "finished the steps" and "achieved the outcome" stay
   * distinguishable — they are different numbers and only the second matters.
   */
  successEvent: Predicate;
  /** Offered on completion, each still subject to its own preconditions. */
  nextJourneys?: string[];
}

/** Emitted by the runner for analytics; `stepId` is absent on journey-level events. */
export type JourneyEvent =
  | { type: "start"; journeyId: string }
  | { type: "step"; journeyId: string; stepId: string; index: number }
  | { type: "skip"; journeyId: string; stepId: string; preconditionId: string }
  | {
      type: "repair";
      journeyId: string;
      stepId: string;
      preconditionId: string;
    }
  | { type: "complete"; journeyId: string; succeeded: boolean }
  | { type: "abandon"; journeyId: string; stepId?: string; index: number };
