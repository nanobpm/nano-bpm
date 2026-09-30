// Worker-provisioning panel rendering guards (issue #1294).
//
// WHY THIS SUITE EXISTS: `src/lib/provisioning.test.ts` covers the pure
// presentation helpers, but nothing else proves the panel actually renders —
// the query wiring against `/console/api/provisioning`, the row/badge markup,
// the server-bound notice, or the empty state. A regression in any of those
// (a renamed field, a dropped testid, a broken filter) would ship silently.
// Only a browser run with a stubbed endpoint can see it. The states asserted
// here — starved, under-provisioned and server-bound — are the ones the
// issue's acceptance criteria name explicitly.

import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

/** One recommendation row, shaped exactly like the Rust `Recommendation`
 * (camelCase fields, kebab-case class/confidence on the wire). */
function rec(overrides: Record<string, unknown>) {
  return {
    jobType: "pay:invoices",
    class: "adequate",
    confidence: "high",
    backlog: 0,
    backlogSlopePerS: 0,
    drainPerS: 0,
    workers: 1,
    suggestWorkerDelta: 0,
    rationale: "backlog stable or shrinking — provisioning looks adequate.",
    ...overrides,
  };
}

/** Stub the provisioning endpoint on top of the base console-API stubs. */
async function stubProvisioning(
  page: Page,
  advice: Record<string, unknown>,
): Promise<void> {
  await page.route("**/console/api/provisioning", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(advice),
    }),
  );
}

const baseAdvice = {
  serverBound: false,
  writerBusyRatio: 0.2,
  ceilingThroughput: false,
  pendingCreateQueue: 0,
  shedDelta: 0,
  windowS: 1,
};

test.describe("worker provisioning panel", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("renders starved and under-provisioned rows with badges and suggestions", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubProvisioning(page, {
      ...baseAdvice,
      recommendations: [
        rec({
          jobType: "pay:invoices",
          class: "starved",
          backlog: 40,
          backlogSlopePerS: 10,
          workers: 0,
          suggestWorkerDelta: 1,
          rationale: "40 jobs waiting with 0 workers subscribed.",
        }),
        rec({
          jobType: "enrich:crm",
          class: "under-provisioned",
          backlog: 200,
          backlogSlopePerS: 100,
          drainPerS: 200,
          workers: 4,
          suggestWorkerDelta: 2,
          rationale: "backlog growing; add ~2 workers.",
        }),
      ],
    });
    await page.goto("workers");

    await expect(page.getByTestId("provisioning-panel")).toBeVisible();
    // Starved row: badge with the +1 suggestion, zero workers, growing trend.
    const starvedRow = page.getByTestId("provisioning-row-pay:invoices");
    await expect(starvedRow).toBeVisible();
    await expect(starvedRow).toHaveAttribute("data-class", "starved");
    await expect(
      page.getByTestId("provisioning-badge-pay:invoices"),
    ).toHaveText("Starved · +1 worker");
    // Under-provisioned row: badge with the Little's-Law suggestion.
    const underRow = page.getByTestId("provisioning-row-enrich:crm");
    await expect(underRow).toBeVisible();
    await expect(underRow).toHaveAttribute("data-class", "under-provisioned");
    await expect(page.getByTestId("provisioning-badge-enrich:crm")).toHaveText(
      "Under-provisioned · +2 workers",
    );
    // No server-bound notice when the server has headroom.
    await expect(
      page.getByTestId("provisioning-server-bound-note"),
    ).toHaveCount(0);
    noCrash();
  });

  test("renders the server-bound notice and a server-bound row with no worker suggestion", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubProvisioning(page, {
      ...baseAdvice,
      serverBound: true,
      writerBusyRatio: 0.93,
      ceilingThroughput: true,
      recommendations: [
        rec({
          jobType: "enrich:crm",
          class: "server-bound",
          backlog: 300,
          backlogSlopePerS: 200,
          drainPerS: 50,
          workers: 4,
          suggestWorkerDelta: 0,
          rationale: "server at its throughput ceiling.",
        }),
      ],
    });
    await page.goto("workers");

    // The panel-wide notice warns that scaling workers won't help.
    const note = page.getByTestId("provisioning-server-bound-note");
    await expect(note).toBeVisible();
    await expect(note).toContainText("throughput ceiling");
    await expect(note).toContainText("93% busy");
    // The row carries the Server-bound badge with NO worker suggestion.
    const row = page.getByTestId("provisioning-row-enrich:crm");
    await expect(row).toHaveAttribute("data-class", "server-bound");
    await expect(page.getByTestId("provisioning-badge-enrich:crm")).toHaveText(
      "Server-bound",
    );
    noCrash();
  });

  test("shows the empty state when no job type has a live signal", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubProvisioning(page, { ...baseAdvice, recommendations: [] });
    await page.goto("workers");

    await expect(page.getByTestId("provisioning-panel")).toBeVisible();
    await expect(
      page.getByText(
        "No job types with waiting work or connected workers right now.",
      ),
    ).toBeVisible();
    noCrash();
  });

  test("surfaces an endpoint failure instead of crashing the view", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await page.route("**/console/api/provisioning", (route) =>
      route.fulfill({ status: 500, body: "boom" }),
    );
    await page.goto("workers");

    await expect(page.getByText(/Couldn't load provisioning:/)).toBeVisible();
    noCrash();
  });
});
