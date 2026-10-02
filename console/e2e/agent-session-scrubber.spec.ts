// Agent session scrubber journey (nano-bpm#1314).
//
// WHY THIS SUITE EXISTS: the scrubber joins four seams that unit tests cover
// only in isolation: the generated Camunda REST subset client (src/gen-c8,
// `/v2/agent-instances/...`), the BpmnViewer badge overlay, InstanceDetail's
// element-click routing, and the live invalidation that makes it "live at
// turn". This proves the whole path in a real browser: a badge appears on the
// element that ran the agent, clicking it opens the session, the playhead
// steps through the engine's turn log, and a newly committed turn shows up
// (followed when the playhead is at the end, not when it is parked).
//
// Deterministic by construction (no retries, no sleeps): every fixture has
// fixed keys and timestamps, and the "new turn" is a fixture swap. The SSE
// stream is stubbed with a short `retry`, so the browser reconnects and every
// `open` invalidates the agent queries. Assertions wait on the resulting UI
// state, never on elapsed time.

import { expect, test, type Page } from "@playwright/test";
import type {
  AgentInstanceResult,
  AgentInstanceStatusEnum,
} from "../src/gen-c8";
import {
  assertNoPageCrash,
  makeInstance,
  resetTourState,
  stubConsoleApi,
  stubInstances,
  suppressStartupPanel,
} from "./fixtures.ts";

const INSTANCE_KEY = "2251799813800001";
const AGENT_KEY = "2251799813900001";
// A second run of the SAME element, for the run-picker journey.
const AGENT_KEY_2 = "2251799813900002";
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

const metrics = (durationMs: number, inputTokens: number) => ({
  inputTokens,
  outputTokens: 10,
  reasoningTokenCount: null,
  cacheCreationTokenCount: null,
  cacheReadTokenCount: null,
  durationMs,
});

const FIRST_TURNS = [
  historyItem("1", 1, "USER", 0, "Summarise the order"),
  historyItem("2", 1, "ASSISTANT", 2, "Looking up the order", {
    model: "nano-model",
    metrics: metrics(1500, 100),
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
    // A TOOL_RESULT carries a single `toolCalls` entry naming the originating
    // call (spec `AgentInstanceToolCall`), so the journey exercises the real
    // `toolCallId` correlation path, not the legacy "close all pending" fallback.
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
const NEXT_TURN = historyItem("4", 2, "ASSISTANT", 5, "Order 7 has 3 items.", {
  model: "nano-model",
  metrics: metrics(1800, 200),
});

function agentInstance(
  status: AgentInstanceStatusEnum,
  key: string = AGENT_KEY,
): AgentInstanceResult {
  return {
    agentInstanceKey: key,
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

/** Stubs the engine's agent endpoints from mutable state, so a test can
 *  "commit a turn" by swapping the fixture. */
async function stubAgents(page: Page) {
  const state: {
    history: ReturnType<typeof historyItem>[];
    status: AgentInstanceStatusEnum;
  } = { history: [...FIRST_TURNS], status: "TOOL_CALLING" };
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
      body: JSON.stringify(page1([agentInstance(state.status)])),
    }),
  );
  await page.route(
    `**/v2/agent-instances/${AGENT_KEY}/history/search`,
    (route) =>
      route.fulfill({
        contentType: "application/json",
        body: JSON.stringify(page1(state.history)),
      }),
  );
  // Each connection opens (=> invalidate), emits one edge, and closes; the
  // short retry makes the browser reconnect, so committed turns propagate.
  await page.route("**/console/api/stream", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body: 'retry: 200\nevent: instances\ndata: {"position":1,"active":1}\n\n',
    }),
  );
  return state;
}

async function setup(page: Page) {
  await stubConsoleApi(page);
  await stubInstances(page, [
    makeInstance({ key: INSTANCE_KEY, process_id: "demo" }),
  ]);
  const state = await stubAgents(page);
  await resetTourState(page);
  await suppressStartupPanel(page);
  await page.goto(`explorer?instance=${INSTANCE_KEY}`);
  await expect(
    page.getByText(new RegExp(`instance ${INSTANCE_KEY}\\b`)),
  ).toBeVisible();
  return state;
}

// A second run of the same element, with its own shorter transcript, so the
// run-picker journey can assert that selecting each option loads only that
// run's history (and resets the playhead).
const RUN2_TURNS = [
  historyItem("r2-1", 1, "USER", 0, "Draft the reply", {
    agentInstanceKey: AGENT_KEY_2,
  }),
  historyItem("r2-2", 1, "ASSISTANT", 1, "Here is the draft.", {
    agentInstanceKey: AGENT_KEY_2,
    model: "nano-model",
    metrics: metrics(900, 50),
  }),
];

/** Stubs two settled runs of the same element: AGENT_KEY (FIRST_TURNS, 3 items)
 *  and AGENT_KEY_2 (RUN2_TURNS, 2 items). */
async function stubTwoRuns(page: Page) {
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
      // Oldest first (agentsByElement order): Run 1 then Run 2.
      body: JSON.stringify(
        page1([
          agentInstance("COMPLETED", AGENT_KEY),
          agentInstance("COMPLETED", AGENT_KEY_2),
        ]),
      ),
    }),
  );
  await page.route(
    `**/v2/agent-instances/${AGENT_KEY}/history/search`,
    (route) =>
      route.fulfill({
        contentType: "application/json",
        body: JSON.stringify(page1([...FIRST_TURNS])),
      }),
  );
  await page.route(
    `**/v2/agent-instances/${AGENT_KEY_2}/history/search`,
    (route) =>
      route.fulfill({
        contentType: "application/json",
        body: JSON.stringify(page1([...RUN2_TURNS])),
      }),
  );
  await page.route("**/console/api/stream", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body: 'retry: 200\nevent: instances\ndata: {"position":1,"active":0}\n\n',
    }),
  );
}

async function setupTwoRuns(page: Page) {
  await stubConsoleApi(page);
  await stubInstances(page, [
    makeInstance({ key: INSTANCE_KEY, process_id: "demo" }),
  ]);
  await stubTwoRuns(page);
  await resetTourState(page);
  await suppressStartupPanel(page);
  await page.goto(`explorer?instance=${INSTANCE_KEY}`);
  await expect(
    page.getByText(new RegExp(`instance ${INSTANCE_KEY}\\b`)),
  ).toBeVisible();
}

const badge = (page: Page) =>
  page.locator(`.nano-badge[data-element-id="${ELEMENT_ID}"]`);
const panel = (page: Page) =>
  page.getByRole("complementary", { name: `Agent session on ${ELEMENT_ID}` });
const slider = (page: Page) =>
  panel(page).getByRole("slider", { name: "Agent history playhead" });

test.describe("agent session scrubber", () => {
  test("badge opens the session; the playhead steps through the turn log", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await setup(page);

    await expect(badge(page)).toHaveText("✦ calling tools");
    await badge(page).click();

    await expect(panel(page)).toBeVisible();
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "3");
    await expect(slider(page)).toHaveAttribute("aria-valuemax", "3");
    await expect(panel(page).getByText("order 7: 3 items")).toBeVisible();

    // Step back: the transcript ends at the ASSISTANT turn and the tool result
    // is no longer shown.
    await panel(page).getByRole("button", { name: "Previous step" }).click();
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "2");
    await expect(panel(page).getByText("order 7: 3 items")).toHaveCount(0);
    await expect(panel(page).getByText("Looking up the order")).toBeVisible();

    // Keyboard: Shift+Left rewinds to the chapter start, Home to the start.
    await slider(page).focus();
    await page.keyboard.press("Home");
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "1");
    await page.keyboard.press("Shift+ArrowRight");
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "3");

    await panel(page)
      .getByRole("button", { name: "Close agent session" })
      .click();
    await expect(panel(page)).toHaveCount(0);
    noCrash();
  });

  test("clicking the element itself opens its session", async ({ page }) => {
    const noCrash = assertNoPageCrash(page);
    await setup(page);
    await page
      .locator(`.djs-element[data-element-id="${ELEMENT_ID}"]`)
      .first()
      .click();
    await expect(panel(page)).toBeVisible();
    noCrash();
  });

  test("live at turn: a following playhead takes the new turn", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    const state = await setup(page);
    await badge(page).click();
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "3");
    await expect(
      panel(page).getByRole("button", { name: "● following" }),
    ).toBeVisible();

    state.history = [...FIRST_TURNS, NEXT_TURN];
    await expect(slider(page)).toHaveAttribute("aria-valuemax", "4");
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "4");
    await expect(panel(page).getByText("Order 7 has 3 items.")).toBeVisible();
    noCrash();
  });

  test("live at turn: a parked playhead stays put while turns arrive", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    const state = await setup(page);
    await badge(page).click();
    await panel(page).getByRole("button", { name: "Previous step" }).click();
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "2");

    state.history = [...FIRST_TURNS, NEXT_TURN];
    await expect(slider(page)).toHaveAttribute("aria-valuemax", "4");
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "2");
    await expect(
      panel(page).getByRole("button", { name: "Jump to live" }),
    ).toBeVisible();

    // Settling the agent stops the live affordances.
    state.status = "COMPLETED";
    await expect(badge(page)).toHaveText("✦ 1 call");
    await expect(
      panel(page).getByRole("button", { name: "Jump to live" }),
    ).toHaveCount(0);
    noCrash();
  });

  test("run picker loads only the selected run's transcript", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await setupTwoRuns(page);
    await badge(page).click();
    await expect(panel(page)).toBeVisible();

    const runPicker = panel(page).getByRole("combobox", { name: "Agent run" });
    await expect(runPicker).toBeVisible();

    // Default is the newest run (Run 2): only its transcript shows.
    await expect(runPicker).toHaveValue(AGENT_KEY_2);
    await expect(panel(page).getByText("Here is the draft.")).toBeVisible();
    await expect(slider(page)).toHaveAttribute("aria-valuemax", "2");
    await expect(panel(page).getByText("Summarise the order")).toHaveCount(0);

    // Selecting Run 1 swaps in only its transcript and resets the playhead to
    // that run's length (the key-change path that previously showed stale
    // history).
    await runPicker.selectOption(AGENT_KEY);
    await expect(panel(page).getByText("Summarise the order")).toBeVisible();
    await expect(slider(page)).toHaveAttribute("aria-valuemax", "3");
    await expect(slider(page)).toHaveAttribute("aria-valuenow", "3");
    await expect(panel(page).getByText("Here is the draft.")).toHaveCount(0);

    // And back to Run 2.
    await runPicker.selectOption(AGENT_KEY_2);
    await expect(panel(page).getByText("Here is the draft.")).toBeVisible();
    await expect(slider(page)).toHaveAttribute("aria-valuemax", "2");
    await expect(panel(page).getByText("Summarise the order")).toHaveCount(0);
    noCrash();
  });
});
