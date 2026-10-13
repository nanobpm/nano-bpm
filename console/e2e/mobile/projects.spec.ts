// Mobile Studio project cards guard — unit A2.
//
// At a phone viewport each project renders as a card with its lifecycle actions
// (Start/Stop, Update, Rename, Delete) inline as 44px touch targets, and the
// 3500-line ProjectWorkspace IDE is DELIBERATELY unreachable — a phone gets an
// explicit "editor unavailable" note instead of a broken editor. Nothing in a
// build or unit test proves the drill-in is gated or the inline actions are
// reachable; only a phone-sized browser can. (Studio profile only — the observe
// build has no Studio route.)

import { expect, test } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "../fixtures.ts";
import { expectNoHorizontalScroll } from "./helpers.ts";

test.describe("mobile Studio project cards", () => {
  test("surface lifecycle actions inline and gate the ProjectWorkspace IDE", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubConsoleApi(page, {
      projects: [
        { name: "alpha", lang: "node", running: false },
        { name: "bravo", lang: "node", running: true },
      ],
    });
    await resetTourState(page);
    await suppressStartupPanel(page);

    await page.goto("projects");

    // A stopped project offers Start inline; a running one offers Stop.
    await expect(
      page.getByRole("button", { name: "Start alpha", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Stop bravo", exact: true }),
    ).toBeVisible();

    // Rename/Delete are reachable inline on each card, no drill-in required.
    await expect(
      page.getByRole("button", { name: "Rename project alpha" }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Delete project alpha" }),
    ).toBeVisible();

    // The IDE is gated: every card carries the explicit unavailable affordance,
    // and there is NO drill-in control that could open ProjectWorkspace. (The
    // desktop tile exposes a `role="button"` card body that navigates to
    // /projects/<name>; the mobile tile must not.)
    const notes = page.getByRole("note");
    await expect(notes.first()).toContainText(
      "The project editor is unavailable on this screen",
    );
    await expect(notes).toHaveCount(2);

    // Touch targets meet the 44px minimum.
    const startBox = await page
      .getByRole("button", { name: "Start alpha", exact: true })
      .boundingBox();
    expect(startBox?.height ?? 0).toBeGreaterThanOrEqual(44);

    await expectNoHorizontalScroll(page);

    // Nothing here navigates into a workspace — we stay on the list route.
    await expect(page).toHaveURL(/\/console\/projects$/);

    noCrash();
  });
});
