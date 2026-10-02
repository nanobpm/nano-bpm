// Mobile agent session scrubber journey (nano-bpm#1314).
//
// WHY THIS SUITE EXISTS: on a phone the instance detail collapses into drill-in
// cards and the agent session scrubber opens as a FULL-SCREEN panel
// (InstanceDetail's `narrow` branch), not the desktop side rail. The desktop
// journey (e2e/agent-session-scrubber.spec.ts) runs only in the `chromium`
// project — its `testIgnore: /mobile\//` and this project's
// `testMatch: /mobile\/.*\.spec\.ts$/` are mutually exclusive — so the
// narrow-layout seam (badge inside the full-screen Model panel → full-screen
// scrubber, no horizontal overflow) is exercised nowhere else. This proves it
// in a real 375px viewport.
//
// Deterministic by construction (no retries, no sleeps): fixed keys and
// timestamps, and assertions wait on UI state, never on elapsed time.

import { expect, test, type Page } from "@playwright/test";
import type {
  AgentInstanceResult,
  AgentInstanceStatusEnum,
} from "../../src/gen-c8";
import {
  assertNoPageCrash,
  makeInstance,
  resetTourState,
  stubConsoleApi,
  stubInstances,
  suppressStartupPanel,
} from "../fixtures.ts";
import { expectNoHorizontalScroll } from "./helpers.ts";

const INSTANCE_KEY = "2251799813800001";
const AGENT_KEY = "2251799813900001";
// MINIMAL_BPMN's task: the element that ran the agent.
const ELEMENT_ID = "Task_1";

const T0 = Date.parse("2026-10-01T09:00:00Z");
const at = (s: number) => new Date(T0 + s * 1000).toISOString();

function historyItem(
  key: string,
  loopIteration: number,
  role: "USER" | "ASSISTANT" | "TOOL_RESULT",
  producedAtS: number,
  text: string,
  extra: Record<string, unknown> = {},
) {
  return {
    historyItemKey: key,
    historyItemId: `item-${key}`,
    agentInstanceKey: AGENT_KEY,
    elementInstanceKey: "2251799813850001",
    jobKey: "2251799813860001",
    jobLeaseToken: "lease-1",
    loopIteration,
    role,
    content: [{ contentType: "TEXT", text }],
    toolCalls: [],
    metrics: null,
    commitStatus: "COMMITTED",
    producedAt: at(producedAtS),
    tools: [],
    model: null,
    provider: null,
    limits: {},
    systemPrompt: [],
    ...extra,
  };
}

const FIRST_TURNS = [
  historyItem("1", 1, "USER", 0, "Summarise the order"),
  historyItem("2", 1, "ASSISTANT", 2, "Looking up the order", {
    model: "nano-model",
    toolCalls: [
      {
        toolCallId: "c1",
        toolName: "lookup_order",
        elementId: null,
        arguments: { id: 7 },
      },
    ],
  }),
  historyItem("3", 2, "TOOL_RESULT", 3, "order 7: 3 items", {
    toolCalls: [
      {
        toolCallId: "c1",
        toolName: "lookup_order",
        elementId: null,
        arguments: { id: 7 },
      },
    ],
  }),
];

function agentInstance(status: AgentInstanceStatusEnum): AgentInstanceResult {
  return {
    agentInstanceKey: AGENT_KEY,
    agentDefinitionKey: "2251799813910001",
    status,
    definition: {
      model: "nano-model",
      provider: "nano",
      systemPrompt: [{ contentType: "TEXT", text: "You summarise orders." }],
    },
    limits: { maxModelCalls: -1, maxToolCalls: -1, maxTokens: -1 },
    tools: [],
    elementId: ELEMENT_ID,
    elementInstanceKeys: ["2251799813850001"],
    processInstanceKey: INSTANCE_KEY,
    rootProcessInstanceKey: INSTANCE_KEY,
    processDefinitionKey: "2251799813600001",
    processDefinitionId: "demo",
    processDefinitionVersion: 1,
    processDefinitionVersionTag: null,
    tenantId: "<default>",
    creationDate: at(0),
    lastUpdatedDate: at(3),
    completionDate: null,
    metrics: {
      inputTokens: 100,
      outputTokens: 10,
      reasoningTokenCount: 0,
      cacheCreationTokenCount: 0,
      cacheReadTokenCount: 0,
      modelCalls: 1,
      toolCalls: 1,
    },
  };
}

/** Stubs the engine's agent endpoints, so the badge + session have data. */
async function stubAgents(page: Page) {
  const page1 = (items: unknown[]) => ({
    items,
    page: {
      totalItems: items.length,
      hasMoreTotalItems: false,
      startCursor: null,
      endCursor: null,
    },
  });
  await page.route("**/v2/agent-instances/search", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(page1([agentInstance("TOOL_CALLING")])),
    }),
  );
  await page.route(
    `**/v2/agent-instances/${AGENT_KEY}/history/search`,
    (route) =>
      route.fulfill({
        contentType: "application/json",
        body: JSON.stringify(page1(FIRST_TURNS)),
      }),
  );
}

test.describe("mobile agent session scrubber", () => {
  test("badge in the Model panel opens the full-screen scrubber", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await stubConsoleApi(page);
    await stubInstances(page, [
      makeInstance({ key: INSTANCE_KEY, process_id: "demo" }),
    ]);
    await stubAgents(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
    await page.goto(`explorer?instance=${INSTANCE_KEY}`);

    // Narrow layout: drill into the Model card to reach the BPMN diagram.
    const modelCard = page.getByRole("button", { name: /Model/ });
    await expect(modelCard).toBeVisible();
    await modelCard.click();
    const modelPanel = page.getByRole("dialog");
    await expect(modelPanel).toBeVisible();
    const badge = modelPanel.locator(
      `.nano-badge[data-element-id="${ELEMENT_ID}"]`,
    );
    await expect(badge).toHaveText("✦ calling tools");
    await expectNoHorizontalScroll(page);

    // Tapping the badge closes the Model panel and opens the session scrubber
    // full-screen (the narrow branch routes through setAgentElement).
    await badge.click();
    const session = page.getByRole("complementary", {
      name: `Agent session on ${ELEMENT_ID}`,
    });
    await expect(session).toBeVisible();
    // The scrubber sits inside its own full-screen dialog on mobile.
    await expect(
      page.getByRole("dialog", { name: `Agent session · ${ELEMENT_ID}` }),
    ).toBeVisible();
    const slider = session.getByRole("slider", {
      name: "Agent history playhead",
    });
    await expect(slider).toHaveAttribute("aria-valuemax", "3");
    await expect(session.getByText("order 7: 3 items")).toBeVisible();
    await expectNoHorizontalScroll(page);

    // Close via the panel's own close affordance, back to the drill-in cards.
    await session.getByRole("button", { name: "Close agent session" }).click();
    await expect(session).toHaveCount(0);
    await expect(modelCard).toBeVisible();
    noCrash();
  });
});
