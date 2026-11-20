// MCP processes view rendering guards (issue #1612).
//
// `src/lib/mcpProcesses.test.ts` covers the pure helpers; this suite proves the
// `/mcp` route renders the catalog served by `/console/api/mcp/processes`
// (whose shape is pinned server-side by `console_catalog`'s unit test): tool
// name, owning definition, description, input schema rows, and the
// enabled/disabled state.

import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

async function stubCatalog(page: Page, body: Record<string, unknown>) {
  await page.route("**/console/api/mcp/processes", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(body),
    }),
  );
}

const tool = {
  name: "mcp-process-with-inputs_2251799813685249",
  title: "mcp-process-with-inputs",
  description: "Tool for executing a process.\n\n## When to use\nAlways.",
  inputSchema: {
    type: "object",
    properties: {
      firstName: {
        type: "string",
        description: "The first name of the person.",
      },
      amount: { type: "integer", description: "The amount to process." },
      businessId: { type: "string", description: "Optional business id." },
    },
    required: ["amount"],
  },
  toolName: "mcp-process-with-inputs",
  messageName: "mcp-process-with-inputs",
  subscriptionKey: "2251799813685249",
  processDefinitionKey: "2251799813685250",
  processDefinitionId: "mcpProcessWithInputs",
  processDefinitionVersion: 2,
  processName: null,
};

test.describe("MCP processes view", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("lists exposed tools with definition, description and inputs", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubCatalog(page, {
      enabled: true,
      envVar: "NANOBPM_MCP_ENABLED",
      path: "/mcp/processes",
      staticTools: ["searchIncidents"],
      tools: [tool],
    });
    await page.goto("mcp");
    await expect(page.getByTestId("mcp-status")).toHaveAttribute(
      "data-enabled",
      "true",
    );
    const card = page.getByTestId("mcp-tool");
    await expect(card).toHaveCount(1);
    await expect(card.getByTestId("mcp-tool-name")).toHaveText(tool.name);
    await expect(card.getByTestId("mcp-tool-process")).toContainText(
      "mcpProcessWithInputs v2",
    );
    await expect(card.getByTestId("mcp-tool-description")).toContainText(
      "## When to use",
    );
    const rows = card.getByTestId("mcp-input-row");
    await expect(rows).toHaveCount(3);
    await expect(rows.nth(0)).toContainText("firstName");
    await expect(rows.nth(1)).toContainText("amount");
    await expect(rows.nth(1)).toContainText("yes");
    await expect(rows.nth(2)).toContainText("businessId");
    await expect(rows.nth(2)).toContainText("(reserved)");
    noCrash();
  });

  test("shows the disabled state and empty list", async ({ page }) => {
    const noCrash = assertNoPageCrash(page);
    await stubCatalog(page, {
      enabled: false,
      envVar: "NANOBPM_MCP_ENABLED",
      path: "/mcp/processes",
      staticTools: [],
      tools: [],
    });
    await page.goto("mcp");
    await expect(page.getByTestId("mcp-status")).toHaveAttribute(
      "data-enabled",
      "false",
    );
    await expect(page.getByTestId("mcp-status")).toContainText(
      "NANOBPM_MCP_ENABLED",
    );
    await expect(page.getByTestId("mcp-empty")).toBeVisible();
    noCrash();
  });
});
