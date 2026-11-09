// The driver.js adapter — the ONLY module that imports driver.js.
//
// ADR 0049 is deliberately runner-agnostic: journeys, preconditions, handoffs and
// predicates are the contract, and the runner renders it. Keeping the dependency
// behind this one seam is what lets the `@reactour/tour` comparison from #393
// finish later without touching a single journey file.
//
// driver.js earns its place on two built-ins this console genuinely needs:
// `waitForElement` (a step's target may mount asynchronously over a
// CSS-transformed bpmn-js/monaco canvas) and `skipMissingElement` (a target that
// never appears is skipped rather than hanging the journey). What it cannot do is
// drive react-router or wait on application state — that is this file's job.

import {
  driver,
  type Driver,
  type DriveStep,
  type PopoverDOM,
} from "driver.js";
import "driver.js/dist/driver.css";
import "./tour.css";
import type {
  HandoffStep,
  Journey,
  JourneyEvent,
  Step,
  TourContext,
} from "./types";
import { resolveSteps, type ResolvedStep } from "./registry.ts";
import { copyText } from "@nanobpm/process-explorer/clipboard";

/** How long to wait for a step's target to mount before skipping it. */
const WAIT_FOR_ELEMENT_MS = 5000;

/**
 * How often a handoff step re-checks its `verify` predicate.
 *
 * Matched to the 2s cadence #404's consumer panel already polls at, so a
 * context source backed by that panel's cache adds no extra requests.
 */
const VERIFY_POLL_MS = 2000;

export interface RunnerDeps {
  /** Router navigation — driver.js cannot touch the router. */
  navigate: (route: string) => void;
  /** Live pathname, read at call time so we never navigate redundantly. */
  getRoute: () => string;
  /** A fresh context snapshot; used for step resolution and `verify` polling. */
  getContext: () => Promise<TourContext>;
  onEvent?: (event: JourneyEvent) => void;
  /** Persist the resume point (authored index, so it survives step edits). */
  onStep?: (journeyId: string, authoredIndex: number) => void;
  onFinish?: (
    journeyId: string,
    outcome: "complete" | "abandon",
    detail: { succeeded?: boolean; authoredIndex: number },
  ) => void;
}

export interface JourneyRunner {
  /** Start a journey, optionally resuming at an authored step index. */
  start: (journey: Journey, fromAuthoredIndex?: number) => Promise<void>;
  stop: () => void;
  isActive: () => boolean;
  /** Tear down without recording an abandon (unmount, HMR). */
  dispose: () => void;
}

export function createJourneyRunner(deps: RunnerDeps): JourneyRunner {
  let instance: Driver | null = null;
  let current: { journey: Journey; steps: ResolvedStep[] } | null = null;
  let verifyTimer: number | undefined;
  // Index most recently reported, so the two paths that can notice a step change
  // (driver.js's onHighlighted, and our own advance) never double-report one.
  let lastReported = -1;
  // Set while we tear down deliberately, so driver.js's onDestroyed can tell an
  // app-initiated teardown from the user finishing or dismissing the journey.
  let disposing = false;

  const emit = (event: JourneyEvent) => deps.onEvent?.(event);

  const clearVerify = () => {
    if (verifyTimer !== undefined) {
      window.clearInterval(verifyTimer);
      verifyTimer = undefined;
    }
  };

  const goToRoute = (step: Step | undefined) => {
    // Skip when already there: consecutive same-route steps would otherwise
    // push redundant history entries and break the journey's Back button.
    if (step?.route && step.route !== deps.getRoute())
      deps.navigate(step.route);
  };

  const authoredIndexAt = (index: number): number =>
    current?.steps[index]?.authoredIndex ?? 0;

  /**
   * Arm a handoff step's `verify` poll.
   *
   * The console cannot observe a terminal, so a handoff either self-reports (the
   * user clicks "I've done it") or is verified indirectly by watching
   * application state for the *result* of the command. Only the latter can
   * auto-advance.
   */
  const armVerify = (step: HandoffStep, atIndex: number) => {
    clearVerify();
    if (!step.verify) return;
    verifyTimer = window.setInterval(() => {
      void deps.getContext().then((ctx) => {
        if (!instance?.isActive()) return;
        if (instance.getActiveIndex() !== atIndex) return;
        if (step.verify?.(ctx)) {
          clearVerify();
          advance(atIndex);
        }
      });
    }, VERIFY_POLL_MS);
  };

  /**
   * Announce the step now showing: analytics, the resume point, and arming a
   * handoff's verify poll.
   *
   * Called from two places because neither is sufficient alone. driver.js's
   * `onHighlighted` fires only for steps that highlight an ELEMENT, so an
   * anchorless `note` step — every journey's opening frame — would otherwise
   * never be recorded, leaving the first step unresumable. `advance` covers
   * those; `lastReported` keeps the overlap idempotent.
   */
  const reportStep = (index: number) => {
    if (index === lastReported) return;
    const resolved = current?.steps[index];
    if (!resolved || !current) return;
    lastReported = index;
    emit({
      type: "step",
      journeyId: current.journey.id,
      stepId: resolved.step.id,
      index,
    });
    deps.onStep?.(current.journey.id, resolved.authoredIndex);
    clearVerify();
    if (resolved.step.kind === "handoff") armVerify(resolved.step, index);
  };

  const advance = (fromIndex: number) => {
    if (!current || !instance) return;
    const next = current.steps[fromIndex + 1];
    goToRoute(next?.step);
    instance.moveNext();
    // Read the index driver.js actually landed on — skipMissingElement may have
    // jumped further than one step.
    reportStep(instance.getActiveIndex() ?? fromIndex + 1);
  };

  /** Render the copyable command/URL into a handoff step's popover. */
  const renderHandoff = (step: HandoffStep, popover: PopoverDOM) => {
    const box = document.createElement("div");
    box.className = "nano-tour-handoff";

    const code = document.createElement("code");
    // textContent, never innerHTML: `copy` may come from a pack-contributed
    // journey (ADR 0049 §7) and must never be able to inject markup.
    code.textContent = step.copy;
    box.appendChild(code);

    const button = document.createElement("button");
    button.type = "button";
    button.className = "nano-tour-copy";
    button.textContent = step.copyLabel ?? "Copy";
    // Latch the first-party outcome hook at most once per rendered handoff, and
    // never let it break the copy UX: `onCopied` is a first-party latch (packs
    // are data and cannot supply one), but onboarding is not worth throwing over.
    let notifiedCopied = false;
    button.addEventListener("click", () => {
      void copyText(step.copy).then((ok) => {
        if (ok && !notifiedCopied) {
          notifiedCopied = true;
          try {
            step.onCopied?.();
          } catch {
            /* a latch failing just reads as not-yet-achieved */
          }
        }
        button.textContent = ok ? "Copied" : "Select and copy";
        if (!ok) selectText(code);
        window.setTimeout(() => {
          button.textContent = step.copyLabel ?? "Copy";
        }, 2000);
      });
    });
    box.appendChild(button);

    popover.description.appendChild(box);
  };

  const toDriveStep = (resolved: ResolvedStep, index: number): DriveStep => {
    const step = resolved.step;
    const isHandoff = step.kind === "handoff";
    return {
      // Omit `element` entirely for anchorless steps — driver.js's contract for
      // a centered step is no element at all, not `element: undefined`.
      ...(step.kind === "spotlight" ? { element: step.selector } : {}),
      popover: {
        title: step.title,
        description: step.body,
        ...(step.kind === "spotlight"
          ? { side: step.side, align: step.align }
          : {}),
        // A handoff with no `verify` is explicitly self-reported; say so on the
        // button rather than pretending the console checked.
        ...(isHandoff && !step.verify ? { nextBtnText: "I've done it" } : {}),
        onPopoverRender: (popover) => {
          if (isHandoff) renderHandoff(step, popover);
        },
        onNextClick: () => {
          clearVerify();
          advance(index);
        },
        onPrevClick: () => {
          clearVerify();
          const prev = current?.steps[index - 1];
          goToRoute(prev?.step);
          instance?.movePrevious();
          if (instance) reportStep(instance.getActiveIndex() ?? index - 1);
        },
      },
    };
  };

  const start = async (journey: Journey, fromAuthoredIndex?: number) => {
    if (instance?.isActive()) return;

    const ctx = await deps.getContext();
    const resolution = resolveSteps(journey, ctx);
    if (resolution.steps.length === 0) return;

    for (const s of resolution.skipped) {
      emit({
        type: "skip",
        journeyId: journey.id,
        stepId: s.stepId,
        preconditionId: s.preconditionId,
      });
    }
    for (const r of resolution.repaired) {
      emit({
        type: "repair",
        journeyId: journey.id,
        stepId: r.stepId,
        preconditionId: r.preconditionId,
      });
    }

    current = { journey, steps: resolution.steps };

    // Resume maps an authored index onto the resolved list; if that exact step
    // was skipped this time, land on the next surviving one rather than restart.
    let startIndex = 0;
    if (fromAuthoredIndex !== undefined && fromAuthoredIndex > 0) {
      const found = resolution.steps.findIndex(
        (s) => s.authoredIndex >= fromAuthoredIndex,
      );
      startIndex = found === -1 ? resolution.steps.length - 1 : found;
    }

    const created = driver({
      showProgress: resolution.steps.length > 1,
      allowClose: true,
      overlayOpacity: 0.55,
      stagePadding: 6,
      stageRadius: 8,
      waitForElement: WAIT_FOR_ELEMENT_MS,
      skipMissingElement: true,
      popoverClass: "nano-tour",
      nextBtnText: "Next",
      prevBtnText: "Back",
      doneBtnText: "Done",
      steps: resolution.steps.map(toDriveStep),
      onHighlighted: () => reportStep(created.getActiveIndex() ?? 0),
      onDestroyed: () => {
        // Fires both when the user finishes/dismisses AND when we tear down on
        // unmount. Only the former is an outcome worth recording: dispose()
        // clears `instance` first and sets `disposing`, so a mid-journey refresh
        // or StrictMode remount leaves the journey resumable instead of
        // recording a spurious abandon.
        clearVerify();
        if (disposing || instance !== created) return;
        const journeyRef = current?.journey;
        const index = created.getActiveIndex() ?? 0;
        const authoredIndex = authoredIndexAt(index);
        instance = null;
        current = null;
        if (!journeyRef) return;

        const finishedAll = index >= resolution.steps.length - 1;
        if (!finishedAll) {
          emit({
            type: "abandon",
            journeyId: journeyRef.id,
            stepId: resolution.steps[index]?.step.id,
            index,
          });
          deps.onFinish?.(journeyRef.id, "abandon", { authoredIndex });
          return;
        }
        // Completion and *success* are different questions: the user can walk
        // every step without the outcome having happened. Record both so the
        // difference stays visible — only the second one matters.
        void deps.getContext().then((finalCtx) => {
          const succeeded = safePredicate(journeyRef.successEvent, finalCtx);
          emit({ type: "complete", journeyId: journeyRef.id, succeeded });
          deps.onFinish?.(journeyRef.id, "complete", {
            succeeded,
            authoredIndex,
          });
        });
      },
    });

    instance = created;
    lastReported = -1;
    emit({ type: "start", journeyId: journey.id });
    goToRoute(resolution.steps[startIndex]?.step);
    created.drive(startIndex);
    // The opening step is usually an anchorless note, which onHighlighted does
    // not cover — report it here so the journey is resumable from step one.
    reportStep(startIndex);
  };

  return {
    start,
    stop: () => {
      clearVerify();
      instance?.destroy();
    },
    isActive: () => instance?.isActive() ?? false,
    dispose: () => {
      clearVerify();
      const created = instance;
      disposing = true;
      instance = null;
      current = null;
      created?.destroy();
      disposing = false;
    },
  };
}

function safePredicate(p: (ctx: TourContext) => boolean, ctx: TourContext) {
  try {
    return p(ctx);
  } catch {
    return false;
  }
}

function selectText(node: HTMLElement): void {
  try {
    const range = document.createRange();
    range.selectNodeContents(node);
    const selection = window.getSelection();
    selection?.removeAllRanges();
    selection?.addRange(range);
  } catch {
    /* selection is a nicety, not a requirement */
  }
}
