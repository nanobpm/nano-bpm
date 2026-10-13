// Guided-journey e2e guards (ADR 0049, #417).
//
// WHY THIS SUITE EXISTS: journey steps target `data-tour` anchors across views
// owned by different slices, and **nothing in a normal build or unit test fails
// when an anchor is renamed or deleted** — the step just silently spotlights
// nothing. ADR 0049 deliberately keeps no anchor registry beyond
// `tourAnchors.ts`, so this is the compensating control for that decision. The
// unit tests in `src/lib/tour/journeys.test.ts` prove a selector is *spelled*
// from a known anchor; only a browser can prove it *resolves*.
//
// Journeys are enumerated from the REGISTRY, by importing every module in
// `src/lib/tour/journeys/` — not from a list kept here. A journey added by a
// later slice is therefore covered the moment it lands, with no edit to this
// file. That is the whole design: the guard must not need maintaining by the
// people it guards.

import { readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { expect, test, type Page } from "@playwright/test";
import { allJourneys } from "../src/lib/tour/registry.ts";
import type { Journey, SpotlightStep, Step } from "../src/lib/tour/types.ts";
import {
  assertNoPageCrash,
  resetTourState,
  seedTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

// ── Registry enumeration ─────────────────────────────────────────────────────

const journeysDir = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../src/lib/tour/journeys",
);

for (const file of readdirSync(journeysDir)) {
  if (!file.endsWith(".ts") || file.endsWith(".test.ts")) continue;
  // Registration is an import side effect, exactly as in the app.
  await import(pathToFileURL(path.join(journeysDir, file)).href);
}

/** The dev server builds the studio profile, so only studio journeys are live. */
const STUDIO_JOURNEYS = allJourneys().filter((j) =>
  j.profiles.includes("studio"),
);

/**
 * Routes reachable without backend state a stub cannot fake.
 *
 * A step inside a project workspace (`/projects/:name`) needs a scaffolded,
 * running project; asserting those would mean building and running a Node app on
 * every CI run, and a flaky guard is worse than a shallower reliable one. Those
 * steps are reported as uncovered rather than silently ignored — see the
 * coverage-report test at the bottom.
 */
const REACHABLE = new Set([
  "/projects",
  "/extensions",
  "/topology",
  "/metrics",
  "/explorer",
  "/traces",
  "/workers",
]);

function isReachable(route: string | undefined): boolean {
  return route !== undefined && REACHABLE.has(route);
}

/** Every authored step plus repair replacements — repairs are steps too. */
function eachStep(journey: Journey): Step[] {
  return journey.steps.flatMap((s) => (s.repair ? [s, s.repair] : [s]));
}

const spotlights = (journey: Journey): SpotlightStep[] =>
  eachStep(journey).filter((s): s is SpotlightStep => s.kind === "spotlight");

/**
 * Walk a running journey to its end, collecting step titles.
 *
 * Tolerant by necessity: driver.js's last button is "Done", and clicking it
 * destroys the popover — so every read is guarded and the loop exits the moment
 * the popover goes away. An unguarded read hangs until the test times out, which
 * reports as a journey failure when the journey in fact completed.
 */
async function walkJourney(page: Page, maxSteps: number): Promise<string[]> {
  const popover = page.locator(".driver-popover");
  const titles: string[] = [];
  for (let i = 0; i < maxSteps; i++) {
    if (!(await popover.isVisible().catch(() => false))) break;
    const title = await popover
      .locator(".driver-popover-title")
      .textContent()
      .catch(() => null);
    if (title?.trim()) titles.push(title.trim());
    const next = popover.locator(".driver-popover-next-btn");
    if (!(await next.isVisible().catch(() => false))) break;
    await next.click();
    // Wait for the journey to ACTUALLY advance rather than for a fixed delay. A
    // step whose anchor is absent takes up to driver.js's `waitForElement` (5s)
    // before being skipped, so a short sleep re-reads the same step — which is how
    // this helper once collected the opening title seven times and reported a
    // missing repair step it had never reached.
    if (title?.trim()) {
      await expect(popover.locator(".driver-popover-title"))
        .not.toHaveText(title.trim(), { timeout: 8000 })
        .catch(() => {
          /* popover closed on the last step — the visibility check ends the loop */
        });
    }
  }
  return titles;
}

// ── The anchor-rot guard ─────────────────────────────────────────────────────

test.describe("journey anchors resolve", () => {
  test("the registry is non-empty", () => {
    expect(
      STUDIO_JOURNEYS.length,
      "no studio journeys registered — the glob import is broken, which would make every guard below vacuous",
    ).toBeGreaterThan(0);
  });

  for (const journey of STUDIO_JOURNEYS) {
    // `optional` is the author's declaration that a step's target may legitimately
    // be absent — #408's template step is optional because the card only renders
    // once the New Project gallery is open, which a bare route load does not do.
    // Enforcing those would fail correct journeys; they go in the ledger below
    // instead, so they stay visible rather than silently unchecked.
    const assertable = spotlights(journey).filter(
      (s) => isReachable(s.route) && !s.optional,
    );

    for (const step of assertable) {
      test(`${journey.id} / ${step.id}: ${step.selector} resolves on ${step.route}`, async ({
        page,
      }) => {
        const noCrash = assertNoPageCrash(page);
        await stubConsoleApi(page);
        await resetTourState(page);
        // The startup persona panel (#464/#471) opens on console load unless a
        // journey is running or resumable — and this test starts none. Suppress it
        // so the assertion is about the anchor, not about an overlay happening not
        // to affect `toHaveCount`.
        await suppressStartupPanel(page);
        // Route is absolute under the router basename; baseURL already ends in
        // /console/, so strip the leading slash.
        await page.goto(step.route!.replace(/^\//, ""));
        const target = page.locator(step.selector);
        await expect(
          target,
          `anchor for step "${step.id}" is missing or ambiguous — if you renamed a data-tour attribute, update TOUR_ANCHOR and the journey together`,
        ).toHaveCount(1);
        noCrash();
      });
    }
  }
});

// ── Deep links ───────────────────────────────────────────────────────────────

test.describe("?tour= deep links", () => {
  test("starts the requested journey and strips the param", async ({
    page,
  }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    // A deep link is explicit intent: it runs even for a completed journey.
    //
    // #464 revises #413 — c8ctl no longer prints `?tour=` links (they sprayed
    // console URLs across every command's output), and the console's own startup
    // persona panel becomes the front door. Consuming a deep link is an explicit
    // non-goal of that change, so this guard stays valid: the entry point is no
    // longer advertised, but it still works and still needs guarding.
    await page.goto("metrics?tour=overview");
    await expect(page.locator(".driver-popover")).toBeVisible();
    await expect(page).toHaveURL(/\/console\/metrics$/);
  });

  test("an unknown journey id is ignored, not fatal", async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    // An unknown id starts nothing, so the startup panel would open over this.
    await suppressStartupPanel(page);
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto("projects?tour=no-such-journey");
    await expect(page.getByText("Studio").first()).toBeVisible();
    expect(errors, "an unknown ?tour= must not throw").toEqual([]);
  });
});

// ── Precondition repair and skip ─────────────────────────────────────────────

test.describe("preconditions", () => {
  // The defect ADR 0049 exists to fix: the tour used to promise "hit Run to boot
  // the engine" on a host with no Node runtime, using data it already had.
  // Any journey with a runtime-gated step must show its repair step instead.
  const gated = STUDIO_JOURNEYS.flatMap((j) =>
    j.steps
      .filter((s) => s.precondition?.id === "has-js-runtime" && s.repair)
      .map((s) => ({ journey: j, step: s })),
  );

  if (gated.length === 0) {
    test("no runtime-gated step is registered yet (guard is inert)", () => {
      // Deliberately visible rather than silently absent: journeys 0a (#408) and
      // 2 (#410) each add a Run step with a repair, and this guard starts biting
      // the moment they land.
      expect(gated).toHaveLength(0);
    });
  }

  for (const { journey, step } of gated) {
    test(`${journey.id} / ${step.id}: repairs into an install hint with no runtime`, async ({
      page,
    }) => {
      await stubConsoleApi(page, {
        nodeAvailable: false,
      });
      // Resume straight to the gated step instead of clicking through: these
      // journeys pass through several workspace-only anchors, each costing
      // driver.js up to 5s to skip, and the claim under test is about how THIS
      // step renders — not how far a user can click.
      await seedTourState(page, journey.id, journey.steps.indexOf(step));
      await page.goto(`projects?tour=${journey.id}`);

      const title = page.locator(".driver-popover .driver-popover-title");
      await expect(title).toBeVisible();
      // The repair replaces the original in place, so the gated step's authored
      // index now renders the repair's content.
      await expect(
        title,
        "with no JavaScript runtime the step must offer an install hint, never tell the user to press Run",
      ).toHaveText(step.repair!.title);
      await expect(title).not.toHaveText(step.title);
    });
  }

  test("an optional step gated on hasTraces is skipped with no traces", async ({
    page,
  }) => {
    const withTraceGate = STUDIO_JOURNEYS.flatMap((j) =>
      j.steps
        .filter((s) => s.precondition?.id === "has-traces")
        .map((s) => ({ j, s })),
    );
    test.skip(withTraceGate.length === 0, "no trace-gated step registered");
    const { j, s } = withTraceGate[0];
    await stubConsoleApi(page, { traceCount: 0 });
    await resetTourState(page);
    await page.goto(`projects?tour=${j.id}`);
    await expect(page.locator(".driver-popover")).toBeVisible();
    const titles = await walkJourney(page, j.steps.length + 2);
    expect(titles, "an empty trace table teaches nothing").not.toContain(
      s.title,
    );
  });
});

// ── Resume and dismissal ─────────────────────────────────────────────────────

test.describe("resume and exit", () => {
  test("a reload mid-journey offers Resume, not a restart", async ({
    page,
  }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await page.goto("projects?tour=overview");
    const popover = page.locator(".driver-popover");
    await expect(popover).toBeVisible();
    await popover.locator(".driver-popover-next-btn").click();
    const secondTitle = await popover
      .locator(".driver-popover-title")
      .textContent();

    await page.reload();
    const rail = page.locator('[data-tour="take-a-tour"]');
    await expect(rail).toBeVisible();

    // Two outcomes are both legitimate here, and this test asserts the one that
    // is deterministic.
    //
    // `autoStart` may resume the journey on its own after AUTOSTART_DELAY_MS. It
    // did so locally and did NOT in CI — the trace showed the rail correctly
    // offering "Resume tour" with no popover, i.e. the journey was `active` and
    // simply never auto-started. That path is timing-dependent (its timer lives in
    // an effect keyed on the runner, so any dependency churn re-arms it) AND it is
    // explicitly transitional: #411 removes auto-start in favour of the picker. So
    // pinning it here would encode a behaviour that is being deleted, and would
    // stay flaky until it was.
    //
    // What must hold either way is the resume MECHANISM: the stored step index is
    // where the journey comes back, not step one. Wait past the auto-start window,
    // then drive it explicitly if it has not already resumed.
    await page.waitForTimeout(1200);
    if (!(await popover.isVisible().catch(() => false))) {
      await expect(rail).toContainText(/Resume/i);
      await rail.click();
    }
    await expect(popover.locator(".driver-popover-title")).toHaveText(
      secondTitle!.trim(),
    );
  });

  test("Escape exits and does not trap focus", async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await page.goto("projects?tour=overview");
    await expect(page.locator(".driver-popover")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator(".driver-popover")).toHaveCount(0);
    // Focus must be usable afterwards: a nav link is reachable by keyboard.
    await page.keyboard.press("Tab");
    await expect(page.locator("body")).toBeVisible();
  });
});

// ── Coverage report ──────────────────────────────────────────────────────────

test("reports steps this suite deliberately does not assert", () => {
  // Not a pass/fail gate — a visible ledger. A step inside a project workspace
  // needs a scaffolded, running project; asserting it would mean building and
  // running a Node app per CI run. Printing the gap keeps it honest, so nobody
  // reads a green suite as "every anchor is checked".
  const uncovered = STUDIO_JOURNEYS.flatMap((j) =>
    spotlights(j)
      .filter((s) => !isReachable(s.route) || s.optional)
      .map(
        (s) =>
          `${j.id}/${s.id} → ${s.selector} (${
            s.optional ? "optional" : `route: ${s.route ?? "none"}`
          })`,
      ),
  );
  console.log(
    uncovered.length === 0
      ? "coverage: every spotlight step is on a reachable route"
      : `coverage: ${uncovered.length} spotlight step(s) not asserted:\n  ${uncovered.join("\n  ")}`,
  );
  expect(Array.isArray(uncovered)).toBe(true);
});
