import { test } from "node:test";
import assert from "node:assert/strict";
import type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
} from "../gen-c8/types.gen";
import {
  agentBadge,
  agentsByElement,
  chapters,
  initialPlayhead,
  isAgentActive,
  onHistoryChanged,
  orderHistory,
  scrub,
  scrubActionForKey,
  seek,
  spans,
  timeWindow,
  pctIn,
  totals,
  type HistoryItem,
} from "./agentHistory.ts";

const T0 = Date.parse("2026-10-01T00:00:00Z");
const at = (s: number) => new Date(T0 + s * 1000).toISOString();

function item(
  key: string,
  loopIteration: number,
  role: HistoryItem["role"],
  producedAtS: number,
  extra: Partial<AgentInstanceHistoryItemResult> = {},
): HistoryItem {
  return {
    historyItemKey: key,
    historyItemId: `id-${key}`,
    agentInstanceKey: "9001",
    elementInstanceKey: "77",
    jobKey: "55",
    jobLeaseToken: "lease",
    loopIteration,
    role,
    content: [],
    toolCalls: [],
    metrics: null,
    commitStatus: "COMMITTED",
    producedAt: at(producedAtS),
    tools: [],
    model: null,
    provider: null,
    limits: {} as HistoryItem["limits"],
    systemPrompt: [],
    ...extra,
  };
}

const call = (id: string, name: string) => ({
  toolCallId: id,
  toolName: name,
  elementId: null,
  arguments: null,
});

const metrics = (durationMs: number, inputTokens = 0, outputTokens = 0) => ({
  inputTokens,
  outputTokens,
  reasoningTokenCount: null,
  cacheCreationTokenCount: null,
  cacheReadTokenCount: null,
  durationMs,
});

// A two-iteration session: user asks, model calls two tools, results come
// back (one TOOL_RESULT per call, each naming its `toolCallId`), model answers.
const session = (): HistoryItem[] => [
  item("10", 1, "USER", 0),
  item("11", 1, "ASSISTANT", 3, {
    model: "m1",
    metrics: metrics(2000, 100, 20),
    toolCalls: [call("a", "bash"), call("b", "read_file")],
  }),
  item("12", 2, "TOOL_RESULT", 5, { toolCalls: [call("a", "bash")] }),
  item("12b", 2, "TOOL_RESULT", 6, { toolCalls: [call("b", "read_file")] }),
  item("13", 2, "ASSISTANT", 9, {
    model: "m1",
    metrics: metrics(4000, 300, 50),
  }),
];

// --- ordering -------------------------------------------------------------------

test("orders by loopIteration, then producedAt, then numeric key", () => {
  const items = [
    item("100", 2, "ASSISTANT", 1),
    item("9", 1, "USER", 5),
    item("11", 1, "ASSISTANT", 5),
    item("10", 1, "ASSISTANT", 5),
  ];
  assert.deepEqual(
    orderHistory(items).map((i) => i.historyItemKey),
    ["9", "10", "11", "100"],
  );
});

test("de-duplicates by key, and the later copy wins", () => {
  const pending = item("10", 1, "USER", 0, { commitStatus: "PENDING" });
  const committed = item("10", 1, "USER", 0);
  const out = orderHistory([pending, committed]);
  assert.equal(out.length, 1);
  assert.equal(out[0].commitStatus, "COMMITTED");
});

// --- chapters -------------------------------------------------------------------

test("one chapter per loop iteration, sized by step count", () => {
  const c = chapters(session());
  assert.deepEqual(
    c.map((x) => [x.loopIteration, x.startIndex, x.endIndex]),
    [
      [1, 0, 1],
      [2, 2, 4],
    ],
  );
  assert.equal(c[0].leftPct, 0);
  assert.equal(c[0].widthPct, 40);
  assert.equal(c[1].leftPct, 40);
});

test("no chapters for an empty history", () => {
  assert.deepEqual(chapters([]), []);
});

// --- playhead -------------------------------------------------------------------

test("starts at the end, following", () => {
  assert.deepEqual(initialPlayhead(4), { index: 3, follow: true });
  assert.deepEqual(initialPlayhead(0), { index: -1, follow: true });
});

test("stepping clamps and toggles follow at the end", () => {
  const c = chapters(session());
  let h = initialPlayhead(5);
  h = scrub(h, "stepForward", c, 5);
  assert.deepEqual(h, { index: 4, follow: true });
  h = scrub(h, "stepBack", c, 5);
  assert.deepEqual(h, { index: 3, follow: false });
  h = scrub(h, "start", c, 5);
  h = scrub(h, "stepBack", c, 5);
  assert.deepEqual(h, { index: 0, follow: false });
  h = scrub(h, "end", c, 5);
  assert.deepEqual(h, { index: 4, follow: true });
});

test("chapter moves land on chapter starts; back rewinds within a chapter first", () => {
  const c = chapters(session());
  let h = seek(0, 5);
  h = scrub(h, "nextChapter", c, 5);
  assert.equal(h.index, 2);
  h = scrub(h, "nextChapter", c, 5); // no later chapter -> end
  assert.equal(h.index, 4);
  h = scrub(h, "previousChapter", c, 5); // inside chapter 2 -> its start
  assert.equal(h.index, 2);
  h = scrub(h, "previousChapter", c, 5); // at a start -> previous chapter
  assert.equal(h.index, 0);
  h = scrub(h, "previousChapter", c, 5);
  assert.equal(h.index, 0);
});

test("empty history keeps the playhead at -1 for every action", () => {
  for (const a of ["stepBack", "stepForward", "nextChapter", "end"] as const) {
    assert.equal(scrub(initialPlayhead(0), a, [], 0).index, -1);
  }
});

test("live: a following playhead tracks new items, a parked one stays", () => {
  assert.deepEqual(onHistoryChanged({ index: 3, follow: true }, 6), {
    index: 5,
    follow: true,
  });
  assert.deepEqual(onHistoryChanged({ index: 1, follow: false }, 6), {
    index: 1,
    follow: false,
  });
});

test("live: a parked playhead past a shrunken history clamps but does not start following", () => {
  assert.deepEqual(onHistoryChanged({ index: 5, follow: false }, 3), {
    index: 2,
    follow: false,
  });
});

// --- spans ----------------------------------------------------------------------

test("model spans end at producedAt and last durationMs", () => {
  const model = spans(session()).filter((s) => s.kind === "model");
  assert.deepEqual(
    model.map((s) => [s.startMs - T0, s.endMs! - T0, s.stepIndex, s.label]),
    [
      [1000, 3000, 1, "m1"],
      [5000, 9000, 4, "m1"],
    ],
  );
});

test("tool spans close at the TOOL_RESULT naming their toolCallId, on separate lanes", () => {
  const tools = spans(session()).filter((s) => s.kind === "tool");
  assert.deepEqual(
    tools.map((s) => [s.label, s.startMs - T0, s.endMs! - T0, s.lane]),
    [
      ["bash", 3000, 5000, 0],
      ["read_file", 3000, 6000, 1],
    ],
  );
});

test("a result closes only its own call; concurrent calls stay open until answered", () => {
  // One ASSISTANT dispatches two calls; the first result answers only `a`, so
  // `b` is still running until its own result arrives a step later.
  const s = spans(session()).filter((x) => x.kind === "tool");
  const bash = s.find((x) => x.label === "bash")!;
  const read = s.find((x) => x.label === "read_file")!;
  assert.equal(bash.endMs! - T0, 5000);
  assert.equal(read.endMs! - T0, 6000);

  // Right after the first result, `b` is still open.
  const mid = session().slice(0, 3);
  const midTools = spans(mid).filter((x) => x.kind === "tool");
  assert.equal(midTools.find((x) => x.label === "bash")!.endMs! - T0, 5000);
  assert.equal(midTools.find((x) => x.label === "read_file")!.endMs, null);
});

test("a TOOL_RESULT with no correlation entry closes every open call", () => {
  const legacy: HistoryItem[] = [
    item("1", 1, "ASSISTANT", 3, {
      toolCalls: [call("a", "bash"), call("b", "read_file")],
    }),
    item("2", 2, "TOOL_RESULT", 5),
  ];
  const tools = spans(legacy).filter((x) => x.kind === "tool");
  assert.ok(tools.every((x) => x.endMs! - T0 === 5000));
});

test("a tool call with no result yet is open", () => {
  const live = session().slice(0, 2);
  const tools = spans(live).filter((s) => s.kind === "tool");
  assert.ok(tools.every((s) => s.endMs === null));
});

test("an ASSISTANT item without metrics yields no model span", () => {
  assert.deepEqual(spans([item("1", 1, "ASSISTANT", 1)]), []);
});

// --- totals ---------------------------------------------------------------------

test("totals accumulate up to the playhead", () => {
  const s = session();
  assert.deepEqual(totals(s, 1), {
    inputTokens: 100,
    outputTokens: 20,
    reasoningTokens: 0,
    cacheReadTokens: 0,
    modelMs: 2000,
    modelCalls: 1,
    toolCalls: 2,
  });
  assert.equal(totals(s, 4).inputTokens, 400);
  assert.equal(totals(s, -1).modelCalls, 0);
});

// --- agent instances by element ----------------------------------------------------

function agent(
  key: string,
  elementId: string,
  status: AgentInstanceResult["status"],
  createdS: number,
): AgentInstanceResult {
  return {
    agentInstanceKey: key,
    elementId,
    status,
    creationDate: at(createdS),
  } as AgentInstanceResult;
}

test("groups runs per element, oldest first, newest status on the badge", () => {
  const map = agentsByElement([
    agent("3", "review", "THINKING", 20),
    agent("1", "review", "COMPLETED", 10),
    agent("2", "plan", "COMPLETED", 5),
  ]);
  const review = map.get("review")!;
  assert.deepEqual(
    review.instances.map((i) => i.agentInstanceKey),
    ["1", "3"],
  );
  assert.equal(review.status, "THINKING");
  assert.equal(review.active, true);
  assert.equal(map.get("plan")!.active, false);
});

test("only COMPLETED and UNKNOWN are settled", () => {
  assert.equal(isAgentActive("COMPLETED"), false);
  assert.equal(isAgentActive("UNKNOWN"), false);
  for (const s of [
    "IDLE",
    "INITIALIZING",
    "THINKING",
    "TOOL_CALLING",
    "TOOL_DISCOVERY",
  ] as const) {
    assert.equal(isAgentActive(s), true);
  }
});

test("badge: settled single run shows model calls; active shows status; runs counted", () => {
  const withCalls = (a: AgentInstanceResult, n: number) =>
    ({ ...a, metrics: { modelCalls: n } }) as AgentInstanceResult;
  const done = agentsByElement([withCalls(agent("1", "e", "COMPLETED", 1), 3)]);
  assert.deepEqual(agentBadge(done.get("e")!), {
    label: "✦ 3 calls",
    title: "Agent session on e: done, 3 model calls. Open the scrubber.",
    tone: "settled",
  });
  const live = agentsByElement([
    withCalls(agent("1", "e", "COMPLETED", 1), 1),
    withCalls(agent("2", "e", "TOOL_CALLING", 2), 1),
  ]);
  const b = agentBadge(live.get("e")!);
  assert.equal(b.label, "✦ calling tools · 2 runs");
  assert.equal(b.tone, "active");
  assert.match(b.title, /\(2 runs\).*2 model calls/);
});

test("hotkeys: arrows step, shift+arrows jump chapters, Home/End", () => {
  const k = (key: string, shiftKey = false) =>
    scrubActionForKey({ key, shiftKey });
  assert.equal(k("ArrowLeft"), "stepBack");
  assert.equal(k("ArrowRight"), "stepForward");
  assert.equal(k("ArrowLeft", true), "previousChapter");
  assert.equal(k("ArrowRight", true), "nextChapter");
  assert.equal(k("Home"), "start");
  assert.equal(k("End"), "end");
  assert.equal(k("a"), null);
});

test("time window spans model starts to last item; open spans reach now only while active", () => {
  const s = session();
  const w = timeWindow(s, spans(s), false, T0 + 60_000)!;
  assert.deepEqual([w.startMs - T0, w.endMs - T0], [0, 9000]);
  assert.equal(pctIn(w, T0 + 4500), 50);
  assert.equal(pctIn(w, T0 - 1), 0);

  const live = s.slice(0, 2); // tool calls still open
  const settled = timeWindow(live, spans(live), false, T0 + 60_000)!;
  assert.equal(settled.endMs - T0, 3000);
  const running = timeWindow(live, spans(live), true, T0 + 60_000)!;
  assert.equal(running.endMs - T0, 60_000);
  assert.equal(timeWindow([], [], true, 0), null);
});

test("a single instant still yields a non-zero window", () => {
  const w = timeWindow([item("1", 1, "USER", 0)], [], false, 0)!;
  assert.ok(w.endMs > w.startMs);
});
