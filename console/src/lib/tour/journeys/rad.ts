// Journey 2 — 90-second RAD fullstack prototype (an Urban app). ADR 0049 §6.
//
// The most console-native journey, and the one that makes ADR 0022's thesis
// tangible: *the binding is the product*. Delphi (ADR 0022's lineage — Adam
// Urban, Turbo Pascal onwards) turned a business problem into a running program
// in an afternoon by unifying a form painter, an event model, data-aware
// controls and a native compiler into one loop — drop a control, write the
// handler, ship a single `.exe`. Urban is that loop for a process app: one
// project holds the process, the screen, the app's own data and the code, and
// it compiles to a single binary that runs with no external server.
//
// So this journey walks a *fullstack* prototype end to end in five steps:
// pick the Urban App template, see the four parts of a fullstack app in the file
// tree, edit a screen without writing a frontend (the Page Composer — ADR 0042,
// the Delphi Form Designer), Run it into a working served app, and see in the
// Explorer the instance that the served UI started.
//
// Import-safe for the Node journey guards: only `registerJourney` /
// `registerContextSource` (pure map/set adds) run at module top level. Everything
// that reads `window` or the network lives inside a function body, invoked only
// in the browser.

import { registerJourney } from "../registry.ts";
import { registerContextSource } from "../context.ts";
import { hasJsRuntime } from "../preconditions.ts";
import { TOUR_ANCHOR, templateAnchor, tourSelector } from "../tourAnchors.ts";
import {
  URBAN_APP_DEFAULT_PORT,
  readServedAppUrl,
  servedAppUrl,
} from "../../servedApp.ts";
import type { Journey, Predicate, Step, TourContext } from "../types";

// Re-exported so tests and callers can reach the pure helpers from the journey
// module; the definitions live in the side-effect-free `lib/servedApp.ts` so the
// workspace UI can import them without pulling in journey registration.
export {
  URBAN_APP_DEFAULT_PORT,
  servedAppPort,
  servedAppUrl,
} from "../../servedApp.ts";
export type { PortConfig } from "../../servedApp.ts";

export const RAD_JOURNEY_ID = "rad-fullstack";

/** The scaffold template this journey starts from (the Urban App card, #411). */
export const RAD_TEMPLATE_ID = "urban-starter";

/** The reference app the exit teaser points at (jwulf/urban-pr-review, ADR 0022's witness). */
const REFERENCE_APP_URL = "https://github.com/jwulf/urban-pr-review";

/**
 * The live origin, or a placeholder when there is no `window` (a Node unit test
 * importing this module for the journey guards). The placeholder is never shown
 * to a user: in the browser this resolves to the real served origin.
 */
function currentOrigin(): string {
  return typeof window !== "undefined" && window.location?.origin
    ? window.location.origin
    : "http://127.0.0.1:8080";
}

// ---------------------------------------------------------------------------
// Outcome signals — "the served app answered on its port AND an instance exists
// that the UI started" (ADR 0049 records completion and *success* separately).
//
// Both are surfaced into `scratch` by a context source so `successEvent` stays a
// pure predicate over the snapshot, unit-testable by constructing a context.
// ---------------------------------------------------------------------------

/** Scratch keys the success predicate reads; also the sources' output shape. */
export const SCRATCH_SERVED_ANSWERED = "rad.servedAnswered";
export const SCRATCH_INSTANCE_STARTED = "rad.instanceStarted";

/**
 * `successEvent`: the served app answered on its port AND at least one process
 * instance exists (the one the served UI's action started). Pure over the
 * snapshot — both booleans arrive via the sources below.
 */
export const radSucceeded: Predicate = (ctx) =>
  ctx.scratch[SCRATCH_SERVED_ANSWERED] === true &&
  ctx.scratch[SCRATCH_INSTANCE_STARTED] === true;

// The served-app probe and the instance count both back only the success
// predicate, so they are cheap and generously cached: at most one probe every
// few seconds while a tour is open.
const PROBE_TTL_MS = 4000;

let servedCache: { at: number; answered: boolean } | null = null;
let servedInflight: Promise<boolean> | null = null;
let instancesCache: { at: number; any: boolean } | null = null;
let instancesInflight: Promise<boolean> | null = null;

/**
 * Did the served app answer on its port? Probed `no-cors`, because the app sets
 * no CORS headers for the console origin: an opaque response that *resolves*
 * (rather than throwing a network error) is enough to know the port is live.
 */
async function probeServed(url: string): Promise<boolean> {
  if (typeof fetch !== "function") return false;
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 1500);
    try {
      await fetch(url, { mode: "no-cors", signal: controller.signal });
      return true;
    } finally {
      clearTimeout(timer);
    }
  } catch {
    return false;
  }
}

async function anyInstance(): Promise<boolean> {
  if (typeof fetch !== "function") return false;
  // Raw origin-relative fetch (not the generated client) so this module stays
  // import-safe for the Node journey guards — a directory import of `../../gen`
  // breaks node --test's ESM loader. `/console/api` is the console client base.
  try {
    const res = await fetch(
      `${currentOrigin()}/console/api/instances?pageSize=1`,
      {
        headers: { accept: "application/json" },
      },
    );
    if (!res.ok) return false;
    const page = (await res.json()) as { total?: number; items?: unknown[] };
    const total = page.total ?? page.items?.length ?? 0;
    return total > 0;
  } catch {
    return false;
  }
}

/** Surface "an instance exists" into scratch. */
async function instanceSource(ctx: TourContext): Promise<Partial<TourContext>> {
  const now = Date.now();
  if (!instancesCache || now - instancesCache.at >= PROBE_TTL_MS) {
    const any = await (instancesInflight ??= anyInstance().finally(() => {
      instancesInflight = null;
    }));
    instancesCache = { at: Date.now(), any };
  }
  return {
    scratch: { ...ctx.scratch, [SCRATCH_INSTANCE_STARTED]: instancesCache.any },
  };
}

/**
 * Surface "the served app answered" into scratch. The workspace publishes the
 * running app's real served URL (its configured port) via `rememberServedAppUrl`
 * while an Urban App runs, so the probe hits the *actual* port; when nothing is
 * published (no app running, or a reload before the workspace re-published), fall
 * back to the default-port URL for this origin.
 */
async function servedSource(ctx: TourContext): Promise<Partial<TourContext>> {
  const url =
    readServedAppUrl() ?? servedAppUrl(currentOrigin(), URBAN_APP_DEFAULT_PORT);
  const now = Date.now();
  if (!servedCache || now - servedCache.at >= PROBE_TTL_MS) {
    const answered = await (servedInflight ??= probeServed(url).finally(() => {
      servedInflight = null;
    }));
    servedCache = { at: Date.now(), answered };
  }
  return {
    scratch: {
      ...ctx.scratch,
      [SCRATCH_SERVED_ANSWERED]: servedCache.answered,
    },
  };
}

// ---------------------------------------------------------------------------
// The journey.
// ---------------------------------------------------------------------------

/** The install-hint shown in place of the served-app payoff when Run cannot work. */
const runtimeRepair: Step = {
  kind: "note",
  id: "served-app-needs-runtime",
  title: "Run needs a JavaScript runtime",
  body: 'This machine has no Node on its PATH, so Run — which boots the app and its web server — cannot start. Install <a href="https://nodejs.org/" target="_blank" rel="noreferrer noopener">Node</a> ≥ 22.6, then reopen this journey. Authoring the process, the screen and the data still works without a runtime; only serving the app needs one.',
};

export const rad: Journey = {
  id: RAD_JOURNEY_ID,
  title: "Prototype a fullstack app in 90 seconds",
  blurb:
    "Process, screens, the app's own database and workers — one project, compiled to a single binary. No frontend, no external server.",
  persona:
    "prototype a fullstack app — process, screens, database and workers in one binary",
  // Studio-only: this is the maker journey. The lean `observe` build strips the
  // Projects, editor and Run surfaces every step here points at (ADR 0034).
  profiles: ["studio"],
  successEvent: radSucceeded,
  nextJourneys: ["localdev"],
  steps: [
    {
      kind: "spotlight",
      id: "urban-template",
      route: "/projects",
      // #411 has landed, so this anchor exists — but the template cards only
      // render once the New Project gallery is OPEN, which arriving at /projects
      // does not do. So the target is legitimately absent until the user opens the
      // gallery, and the step is `optional` for the same reason #408's equivalent
      // is: driver.js drops it rather than stalling, and the e2e anchor guard
      // (#417) reports it in its coverage ledger instead of failing on a journey
      // that is behaving correctly.
      optional: true,
      selector: tourSelector(templateAnchor(RAD_TEMPLATE_ID)),
      title: "Start from the Urban App",
      body: "New project → the <strong>Urban App</strong> template. Urban is Nano's RAD face (ADR 0022): the position that <em>the binding is the product</em> — one artifact ties the process, the screens, the data and the code together and compiles to a single binary that runs with no external server.",
      side: "bottom",
      align: "start",
    },
    {
      kind: "spotlight",
      // Workspace-only: no route, because this journey does not know the name of
      // the project you just scaffolded. Open it and skipMissingElement lands the
      // spotlight; on a fresh install with nothing open, the step is dropped.
      id: "four-parts",
      selector: tourSelector(TOUR_ANCHOR.fileTree),
      title: "The four parts of a fullstack app",
      body: "One project, four parts: the <strong>process</strong> (<code>resources/processes/*.bpmn</code>), the <strong>screen</strong> (<code>pages/*.page.json</code>, ADR 0042), the app's <strong>own SQLite</strong> (<code>db/migrations/</code> + typed accessors via <code>@nanobpm/domain</code>, ADR 0024 — state at rest), and the <strong>code</strong> (<code>workers/*/worker.ts</code>). The model is the form, the handler is the code, and the binding between them is derived.",
      side: "right",
      align: "start",
      optional: true,
    },
    {
      kind: "spotlight",
      id: "edit-screen",
      selector: tourSelector(TOUR_ANCHOR.pageEditor),
      title: "Change a screen without writing a frontend",
      body: "This is the Page Composer (ADR 0042) — the Delphi Form Designer for a process app. Compose a screen from data-aware controls (a submit box beside a live list beside a status line), and the runtime serves it: no hand-written frontend, no JSON API. Edit it here and the app's UI changes.",
      side: "right",
      align: "start",
      optional: true,
    },
    {
      kind: "spotlight",
      id: "run-and-serve",
      // The payoff: a working fullstack app. Spotlight the served-app link — the
      // element the workspace renders while the app runs, whose href IS the app's
      // actual port (read from the project config, never hardcoded here). Gated on
      // a runtime, because Run cannot boot the server without one; the repair is an
      // install hint rather than a broken promise (ADR 0049's founding case).
      selector: tourSelector(TOUR_ANCHOR.servedApp),
      title: "Run it — a working fullstack app",
      body: "Hit <strong>Run</strong> and the whole app boots: the engine, the app-hosted workers, and its own web server. This link opens your running app on its own port — a real fullstack app you never wrote a line of frontend for. That is the RAD loop: drop a control, write the handler, ship the binary.",
      side: "bottom",
      align: "start",
      precondition: hasJsRuntime,
      repair: runtimeRepair,
    },
    {
      kind: "spotlight",
      id: "explorer-instance",
      route: "/explorer",
      selector: tourSelector(TOUR_ANCHOR.explorerNav),
      title: "See what the UI started",
      body: `Every action in the served UI drives the engine: the submit box you saw <em>created a process instance</em>, and here it is — its variables and current state. Next: give the app a db and deploy it — <a href="${REFERENCE_APP_URL}" target="_blank" rel="noreferrer noopener">urban-pr-review</a> is a full Urban app built this way.`,
      side: "right",
      align: "start",
    },
  ],
};

registerJourney(rad);
registerContextSource(instanceSource);
registerContextSource(servedSource);
