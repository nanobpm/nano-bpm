// Post-extension-update project-update dialog e2e guards (#1143).
//
// WHY THIS SUITE EXISTS: after a successful extension update the console must
// offer the projects scaffolded from that extension that can now be updated, and
// run each selected project through a running-state-preserving stop → update →
// restart lifecycle. None of that — the dialog appearing only after a *success*,
// scoping to the updated extension, the exact copy, per-project selection, and
// the stop/update/restart ORDERING and failure reporting — is observable without
// a browser driving the real Extensions view against a stubbed gateway. This is
// the compensating guard for that; the pure lifecycle/eligibility logic is unit
// tested in src/lib/projectUpdateRun.test.ts and src/lib/templateUpdate.test.ts.

import { expect, test, type Page, type Route } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";
import { POST_UPDATE_PROJECTS_MESSAGE } from "../src/lib/projectUpdateRun.ts";

const runState = (status: string) => ({
  status,
  pid: status === "running" ? 42 : null,
  startedAtMs: null,
  lastError: null,
  compiling: false,
});

const updatePlan = (overrides: Record<string, unknown> = {}) => ({
  pack: "@nanobpm/starter",
  fromVersion: "1.0.0",
  toVersion: "2.0.0",
  applied: true,
  versionBumped: true,
  create: [],
  overwrite: ["src/main.ts"],
  merged: [],
  preserved: [],
  conflicts: [],
  orphans: [],
  ...overrides,
});

// One scaffolded project. `running` and `updateAvailable`/`latestVersion` flip
// with the extension update: before the update nothing is eligible; after it the
// starter-scaffolded projects gain a v2 target.
type Proj = {
  name: string;
  running: boolean;
  pack?: string;
};

function projectsBody(list: Proj[], afterUpdate: boolean) {
  return {
    projects: list.map((p) => {
      const scaffolded = p.pack
        ? { pack: p.pack, version: "1.0.0" }
        : undefined;
      // Only starter-scaffolded projects become eligible after the update.
      const eligible = afterUpdate && p.pack === "@nanobpm/starter";
      return {
        name: p.name,
        description: "",
        deployTarget: "http://localhost:8080",
        updatedMs: 0,
        processes: 1,
        decisions: 0,
        forms: 0,
        workers: 0,
        running: p.running,
        source: "workspace",
        lang: "node",
        ...(scaffolded ? { scaffoldedFrom: scaffolded } : {}),
        updateAvailable: eligible,
        ...(eligible ? { latestVersion: "2.0.0" } : {}),
      };
    }),
    nodeAvailable: true,
    urbanAvailable: true,
    platforms: [],
    templates: [],
    extensions: { extensions: [], yolo: false },
  };
}

function marketBody(updateAvailable: boolean) {
  return {
    entries: [
      {
        name: "@nanobpm/starter",
        version: "2.0.0",
        description: "Starter app template",
        category: "app",
        official: true,
        installed: true,
        installedVersion: updateAvailable ? "1.0.0" : "2.0.0",
        updateAvailable,
      },
    ],
  };
}

type Stub = {
  /** Ordered log of lifecycle POSTs, e.g. "stop:orders", "update:orders". */
  calls: string[];
  /** Force update-from-template to fail for these project names. */
  failUpdateFor: Set<string>;
};

/**
 * Layer the Extensions-view API on top of the base stubs. Starts "before" the
 * update (nothing eligible); the install POST flips it "after" so the reloaded
 * project list reports the starter projects as newly updatable.
 */
async function stubExtensions(page: Page, list: Proj[]): Promise<Stub> {
  const stub: Stub = { calls: [], failUpdateFor: new Set() };
  let afterUpdate = false;

  await page.route("**/console/api/**", async (route: Route) => {
    const req = route.request();
    const url = new URL(req.url()).pathname;
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });

    if (url.endsWith("/console/api/extensions/marketplace")) {
      return json(marketBody(!afterUpdate));
    }
    if (url.endsWith("/console/api/extensions/install")) {
      afterUpdate = true;
      return json({ ok: true });
    }
    if (url.endsWith("/console/api/extensions")) {
      return json({ extensions: [], yolo: false });
    }
    if (url.endsWith("/console/api/projects")) {
      return json(projectsBody(list, afterUpdate));
    }
    const stopM = url.match(/\/console\/api\/projects\/([^/]+)\/stop$/);
    if (stopM) {
      stub.calls.push(`stop:${decodeURIComponent(stopM[1])}`);
      return json(runState("stopped"));
    }
    const runM = url.match(/\/console\/api\/projects\/([^/]+)\/run$/);
    if (runM) {
      stub.calls.push(`run:${decodeURIComponent(runM[1])}`);
      return json(runState("running"));
    }
    const updM = url.match(
      /\/console\/api\/projects\/([^/]+)\/update-from-template$/,
    );
    if (updM) {
      const name = decodeURIComponent(updM[1]);
      stub.calls.push(`update:${name}`);
      if (stub.failUpdateFor.has(name)) {
        return route.fulfill({ status: 400, body: "npm registry unreachable" });
      }
      return json(updatePlan());
    }
    return route.fallback();
  });

  return stub;
}

const dialog = (page: Page) => page.getByRole("dialog");

/** Click the marketplace "Update" button that triggers the extension update. */
async function updateExtension(page: Page): Promise<void> {
  await page.goto("extensions");
  await expect(
    page.getByRole("button", { name: "Update", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Update", exact: true }).click();
}

test.describe("post-extension-update project dialog", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("appears after a successful update, with the exact copy and only eligible projects", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubExtensions(page, [
      { name: "orders", running: true, pack: "@nanobpm/starter" },
      { name: "billing", running: false, pack: "@nanobpm/starter" },
      // Unrelated: not scaffolded from the updated pack — must be excluded.
      { name: "widgets", running: false },
    ]);
    await updateExtension(page);

    await expect(dialog(page)).toBeVisible();
    await expect(dialog(page)).toContainText(POST_UPDATE_PROJECTS_MESSAGE);
    await expect(
      dialog(page).getByRole("checkbox", { name: "Update orders" }),
    ).toBeVisible();
    await expect(
      dialog(page).getByRole("checkbox", { name: "Update billing" }),
    ).toBeVisible();
    await expect(
      dialog(page).getByRole("checkbox", { name: "Update widgets" }),
    ).toHaveCount(0);
    noCrash();
  });

  test("cancelling leaves every project unchanged", async ({ page }) => {
    const stub = await stubExtensions(page, [
      { name: "orders", running: true, pack: "@nanobpm/starter" },
    ]);
    await updateExtension(page);

    await expect(dialog(page)).toBeVisible();
    await dialog(page).getByRole("button", { name: "Cancel" }).click();
    await expect(dialog(page)).toHaveCount(0);
    // No lifecycle operation ran — the extension update stays installed, but no
    // project was stopped, updated or restarted.
    expect(stub.calls).toEqual([]);
  });

  test("runs stop→update→restart for a running project and update-only for a stopped one; unselected untouched", async ({
    page,
  }) => {
    const stub = await stubExtensions(page, [
      { name: "orders", running: true, pack: "@nanobpm/starter" },
      { name: "billing", running: false, pack: "@nanobpm/starter" },
      { name: "invoices", running: false, pack: "@nanobpm/starter" },
    ]);
    await updateExtension(page);
    await expect(dialog(page)).toBeVisible();

    // Deselect "invoices" — it must be neither stopped nor updated.
    await dialog(page)
      .getByRole("checkbox", { name: "Update invoices" })
      .uncheck();
    await dialog(page)
      .getByRole("button", { name: /Update 2 projects/ })
      .click();

    // The run completes: both selected projects report success, and the button
    // becomes Close.
    await expect(
      dialog(page).getByRole("button", { name: "Close" }),
    ).toBeVisible();

    // Running project: stop → update → restart, in that order. Stopped project:
    // update only, never started. Unselected project: nothing.
    expect(stub.calls).toEqual([
      "stop:orders",
      "update:orders",
      "run:orders",
      "update:billing",
    ]);
    expect(stub.calls).not.toContain("run:billing");
    expect(stub.calls.some((c) => c.endsWith(":invoices"))).toBe(false);

    await expect(dialog(page)).toContainText("Updated orders, billing.");
  });

  test("reports a failed update explicitly, not as a success", async ({
    page,
  }) => {
    const stub = await stubExtensions(page, [
      { name: "orders", running: false, pack: "@nanobpm/starter" },
    ]);
    stub.failUpdateFor.add("orders");
    await updateExtension(page);
    await expect(dialog(page)).toBeVisible();

    await dialog(page)
      .getByRole("button", { name: /Update 1 project/ })
      .click();
    await expect(
      dialog(page).getByRole("button", { name: "Close" }),
    ).toBeVisible();

    await expect(dialog(page)).toContainText("update failed");
    await expect(dialog(page)).toContainText("Failed: orders.");
  });
});
