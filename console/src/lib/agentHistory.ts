// Agent history scrubber: the pure state model behind the Process Explorer's
// agent-session panel (nano-bpm#1314).
//
// The source of truth is the engine's AgentHistory turn log (Camunda 8.10
// parity, `POST /agent-instances/{key}/history/search`). Every history item is
// one *step* on the scrubber. Items sharing a `loopIteration` (one pass through
// the agent loop: the model reasons, selects tools, evaluates results) form one
// *chapter*. The span timeline places model calls (ASSISTANT items, from their
// `metrics.durationMs`) and tool calls (from the ASSISTANT item that requested
// them to the TOOL_RESULT that answered them) on fixed lanes.
//
// "Live" is turn granularity: new committed items arrive with each refetch and
// the playhead follows the end when it was already there.
//
// Pure: no React, no fetch. Rendering lives in components/AgentHistoryPanel.tsx.
// Lane/chapter model after the Temporal Agent Harness console
// (ui/src/lib/state/{stepTimeline,scrubLane}.ts, MIT).

import type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
  AgentInstanceStatusEnum,
} from "../gen-c8/types.gen";

export type HistoryItem = AgentInstanceHistoryItemResult;

// --- Ordering ----------------------------------------------------------------

/** Engine order: `(loopIteration, producedAt)`, then the minted key as the
 *  final tiebreak (engine-core `agent.rs`). Keys are decimal strings of a
 *  64-bit counter, so compare by length before lexically. */
export function compareHistoryItems(a: HistoryItem, b: HistoryItem): number {
  if (a.loopIteration !== b.loopIteration) {
    return a.loopIteration - b.loopIteration;
  }
  const ta = Date.parse(a.producedAt);
  const tb = Date.parse(b.producedAt);
  if (ta !== tb) return ta - tb;
  return compareKeys(a.historyItemKey, b.historyItemKey);
}

export function compareKeys(a: string, b: string): number {
  if (a.length !== b.length) return a.length - b.length;
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Ordered, de-duplicated by `historyItemKey`. A page refetch can return an
 *  item already held, and the later copy wins (its commitStatus may have moved
 *  on from PENDING). */
export function orderHistory(items: readonly HistoryItem[]): HistoryItem[] {
  const byKey = new Map<string, HistoryItem>();
  for (const item of items) byKey.set(item.historyItemKey, item);
  return [...byKey.values()].sort(compareHistoryItems);
}

// --- Chapters (one per loop iteration) -----------------------------------------

export interface Chapter {
  loopIteration: number;
  /** Index of the chapter's first step. */
  startIndex: number;
  /** Index of the chapter's last step (inclusive). */
  endIndex: number;
  /** Position on the scrub bar, in percent of the whole bar. */
  leftPct: number;
  widthPct: number;
}

export function chapters(items: readonly HistoryItem[]): Chapter[] {
  const out: Chapter[] = [];
  const n = items.length;
  items.forEach((item, i) => {
    const last = out[out.length - 1];
    if (last && last.loopIteration === item.loopIteration) {
      last.endIndex = i;
    } else {
      out.push({
        loopIteration: item.loopIteration,
        startIndex: i,
        endIndex: i,
        leftPct: 0,
        widthPct: 0,
      });
    }
  });
  for (const c of out) {
    c.leftPct = (c.startIndex / n) * 100;
    c.widthPct = ((c.endIndex - c.startIndex + 1) / n) * 100;
  }
  return out;
}

/** The chapter holding step `index`, or undefined for an empty history. */
export function chapterAt(
  list: readonly Chapter[],
  index: number,
): Chapter | undefined {
  return list.find((c) => index >= c.startIndex && index <= c.endIndex);
}

// --- Playhead ------------------------------------------------------------------

export interface Playhead {
  /** The step shown, `-1` when there is no history yet. */
  index: number;
  /** Follow the end as new items arrive (live). Cleared by any manual move
   *  away from the end; restored by moving back to it. */
  follow: boolean;
}

export type ScrubAction =
  | "stepBack"
  | "stepForward"
  | "previousChapter"
  | "nextChapter"
  | "start"
  | "end";

export function initialPlayhead(count: number): Playhead {
  return { index: count - 1, follow: true };
}

function settle(index: number, count: number): Playhead {
  const clamped = Math.max(count === 0 ? -1 : 0, Math.min(index, count - 1));
  return { index: clamped, follow: clamped === count - 1 };
}

/** Moves the playhead. Chapter moves land on a chapter's first step; going
 *  back from inside a chapter first rewinds to its start (media-player
 *  convention). */
export function scrub(
  head: Playhead,
  action: ScrubAction,
  list: readonly Chapter[],
  count: number,
): Playhead {
  switch (action) {
    case "stepBack":
      return settle(head.index - 1, count);
    case "stepForward":
      return settle(head.index + 1, count);
    case "start":
      return settle(0, count);
    case "end":
      return settle(count - 1, count);
    case "nextChapter": {
      const next = list.find((c) => c.startIndex > head.index);
      return settle(next ? next.startIndex : count - 1, count);
    }
    case "previousChapter": {
      const here = chapterAt(list, head.index);
      if (here && head.index > here.startIndex) {
        return settle(here.startIndex, count);
      }
      const prev = [...list].reverse().find((c) => c.endIndex < head.index);
      return settle(prev ? prev.startIndex : 0, count);
    }
  }
}

/** Jump to an absolute step (a click or drag on the scrub bar). */
export function seek(index: number, count: number): Playhead {
  return settle(index, count);
}

/** Reconcile the playhead with a refetched history of `count` items: a
 *  following playhead tracks the new end; a parked one stays put (clamped, in
 *  case retention or a DISCARD shrank the list). */
export function onHistoryChanged(head: Playhead, count: number): Playhead {
  if (head.follow) return { index: count - 1, follow: true };
  return { ...settle(head.index, count), follow: false };
}

// --- Spans (the timeline) --------------------------------------------------------

export type SpanKind = "model" | "tool";

export interface Span {
  id: string;
  kind: SpanKind;
  label: string;
  loopIteration: number;
  startMs: number;
  /** `null` while still running (a tool call with no result yet). */
  endMs: number | null;
  /** The step that opened the span, so a click can seek to it. */
  stepIndex: number;
  /** 0-based lane within the kind; overlapping same-kind spans stack. */
  lane: number;
}

/**
 * Model spans come from ASSISTANT items with a `durationMs` (the span ends at
 * `producedAt`). Tool spans run from the ASSISTANT item that requested the call
 * to the first later TOOL_RESULT item; one TOOL_RESULT answers every call its
 * requesting item made (the AgentHistory record does not carry per-call
 * results). A tool call with no result yet is open (`endMs: null`).
 */
export function spans(items: readonly HistoryItem[]): Span[] {
  const out: Span[] = [];
  let pending: { calls: Span[] } | null = null;
  items.forEach((item, i) => {
    const at = Date.parse(item.producedAt);
    if (item.role === "TOOL_RESULT" && pending) {
      for (const s of pending.calls) s.endMs = at;
      pending = null;
    }
    if (item.role !== "ASSISTANT") return;
    const duration = item.metrics?.durationMs;
    if (duration != null && duration >= 0) {
      out.push({
        id: `model:${item.historyItemKey}`,
        kind: "model",
        label: item.model ?? "model",
        loopIteration: item.loopIteration,
        startMs: at - duration,
        endMs: at,
        stepIndex: i,
        lane: 0,
      });
    }
    if (item.toolCalls.length > 0) {
      const calls = item.toolCalls.map((call): Span => ({
        id: `tool:${item.historyItemKey}:${call.toolCallId}`,
        kind: "tool",
        label: call.toolName,
        loopIteration: item.loopIteration,
        startMs: at,
        endMs: null,
        stepIndex: i,
        lane: 0,
      }));
      out.push(...calls);
      pending = { calls };
    }
  });
  assignLanes(out);
  return out;
}

/** Greedy lane packing per kind: a span takes the lowest lane whose previous
 *  span has ended. An open span holds its lane for good. */
function assignLanes(list: Span[]): void {
  const laneEnds = new Map<SpanKind, number[]>();
  const ordered = [...list].sort((a, b) => a.startMs - b.startMs);
  for (const s of ordered) {
    const ends = laneEnds.get(s.kind) ?? [];
    let lane = ends.findIndex((end) => end <= s.startMs);
    if (lane === -1) lane = ends.length;
    ends[lane] = s.endMs ?? Number.POSITIVE_INFINITY;
    laneEnds.set(s.kind, ends);
    s.lane = lane;
  }
}

// --- Totals ------------------------------------------------------------------------

export interface Totals {
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  cacheReadTokens: number;
  modelMs: number;
  modelCalls: number;
  toolCalls: number;
}

/** Totals over steps `0..=upTo` (the playhead), so the readout scrubs too. */
export function totals(items: readonly HistoryItem[], upTo: number): Totals {
  const t: Totals = {
    inputTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    cacheReadTokens: 0,
    modelMs: 0,
    modelCalls: 0,
    toolCalls: 0,
  };
  for (const item of items.slice(0, upTo + 1)) {
    const m = item.metrics;
    t.inputTokens += m?.inputTokens ?? 0;
    t.outputTokens += m?.outputTokens ?? 0;
    t.reasoningTokens += m?.reasoningTokenCount ?? 0;
    t.cacheReadTokens += m?.cacheReadTokenCount ?? 0;
    t.modelMs += m?.durationMs ?? 0;
    if (item.role === "ASSISTANT") t.modelCalls += 1;
    t.toolCalls += item.toolCalls.length;
  }
  return t;
}

// --- Agent instances on the diagram -------------------------------------------------

/** Statuses after which no more turns will arrive. */
const SETTLED: ReadonlySet<AgentInstanceStatusEnum> = new Set([
  "COMPLETED",
  "UNKNOWN",
]);

export function isAgentActive(status: AgentInstanceStatusEnum): boolean {
  return !SETTLED.has(status);
}

export interface ElementAgents {
  elementId: string;
  /** Oldest first, so a loop's runs read in order. */
  instances: AgentInstanceResult[];
  active: boolean;
  /** Status of the newest instance, for the badge. */
  status: AgentInstanceStatusEnum;
}

/** Groups a process instance's agent instances by the BPMN element that ran
 *  them. An element that ran more than once (loop, multi-instance, retry) holds
 *  one AgentInstance per run. */
export function agentsByElement(
  instances: readonly AgentInstanceResult[],
): Map<string, ElementAgents> {
  const out = new Map<string, ElementAgents>();
  const sorted = [...instances].sort(
    (a, b) =>
      Date.parse(a.creationDate) - Date.parse(b.creationDate) ||
      compareKeys(a.agentInstanceKey, b.agentInstanceKey),
  );
  for (const inst of sorted) {
    const entry = out.get(inst.elementId) ?? {
      elementId: inst.elementId,
      instances: [],
      active: false,
      status: inst.status,
    };
    entry.instances.push(inst);
    entry.active ||= isAgentActive(inst.status);
    entry.status = inst.status;
    out.set(inst.elementId, entry);
  }
  return out;
}
