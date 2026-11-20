import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
// Import through the `file:` dep SYMLINK (`node_modules/@nanobpm/process-explorer`),
// never the package specifier (its barrel re-exports `BpmnViewer`, which drags the
// bundler-only `bpmn-js` specifier into the node test runner) — see
// TraceTimeline.test.tsx for the full rationale.
import AgentSessionPanel from "../../node_modules/@nanobpm/process-explorer/src/AgentSessionPanel.tsx";
import { ExplorerDataProvider } from "../../node_modules/@nanobpm/process-explorer/src/dataSource.tsx";
import type { ExplorerDataSource } from "../../node_modules/@nanobpm/process-explorer/src/dataSource.tsx";
import type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
} from "../../node_modules/@nanobpm/process-explorer/src/gen-c8/types.gen.ts";

// nano-bpm#1686 review: the PENDING-history panel's render-surface guards.
//
// 1. Accessible name: a transcript row's select button carries an explicit
//    `aria-label`, which overrides the row's descendant text — so the visible
//    "in progress" badge alone never reaches a screen reader. A PENDING turn's
//    button must say so; a COMMITTED turn's must not.
// 2. Shrunk-history clamp: when a refetch drops a discarded attempt's PENDING
//    turns, the playhead reconciliation effect runs only AFTER the render that
//    already holds the shrunken list — so the render itself must clamp the
//    playhead (`onHistoryChanged`), or `items[head.index]` is undefined and
//    `Date.parse(current.producedAt)` throws. The clamp's truth table is
//    exercised as a pure function in agentHistory.test.ts ("a following
//    playhead clamps to a shrunken history's new end"); here the source guard
//    pins that the render derives from the clamped playhead, not the raw one.

const T0 = Date.parse("2026-10-01T00:00:00Z");
const at = (s: number) => new Date(T0 + s * 1000).toISOString();

function item(
  key: string,
  loopIteration: number,
  role: AgentInstanceHistoryItemResult["role"],
  producedAtS: number,
  extra: Partial<AgentInstanceHistoryItemResult> = {},
): AgentInstanceHistoryItemResult {
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
    limits: {} as AgentInstanceHistoryItemResult["limits"],
    systemPrompt: [],
    ...extra,
  };
}

const instance = (
  over: Partial<AgentInstanceResult> = {},
): AgentInstanceResult =>
  ({
    agentInstanceKey: "9001",
    elementId: "review",
    status: "THINKING",
    creationDate: at(0),
    ...over,
  }) as AgentInstanceResult;

// SSR has no effect phase, so a useQuery stays pending on its own. Seed it
// through `initialData` in the default options — which ExplorerDataProvider's
// child query client inherits from the parent — so the transcript renders.
function renderPanel(history: AgentInstanceHistoryItemResult[]): string {
  const source = {
    agents: {
      listAgentInstances: () => Promise.resolve([instance()]),
      getAgentHistory: () => Promise.resolve(history),
    },
  } as unknown as ExplorerDataSource;
  const parent = new QueryClient({
    defaultOptions: {
      queries: { retry: false, initialData: (() => history) as never },
    },
  });
  return renderToStaticMarkup(
    createElement(
      QueryClientProvider,
      { client: parent },
      createElement(
        ExplorerDataProvider,
        // createElement passes children positionally; the ExplorerDataProvider
        // props type requires the key, so satisfy tsc with an explicit slot.
        { source, children: null as never },
        createElement(AgentSessionPanel, {
          elementId: "review",
          instances: [instance()],
          onClose: () => {},
        }),
      ),
    ),
  );
}

test("a pending turn's transcript button names its state; a committed one's does not", () => {
  const html = renderPanel([
    item("10", 1, "USER", 0),
    item("11", 1, "ASSISTANT", 3, { commitStatus: "PENDING" }),
  ]);
  // The PENDING row's accessible name carries the state (the explicit
  // aria-label overrides the visible "in progress" badge for screen readers);
  // the COMMITTED row's stays unadorned.
  assert.match(
    html,
    /aria-label="Show step: assistant, iteration 1, in progress"/,
  );
  assert.match(html, /aria-label="Show step: user, iteration 1"/);
  assert.doesNotMatch(html, /Show step: user, iteration 1, in progress/);
  // And the visible badge is still there for sighted users.
  assert.match(html, /in progress/);
});

test("a shrunk nonempty history still renders its surviving last step", () => {
  // The post-discard render: one surviving COMMITTED turn. The render-clamped
  // playhead keeps the view on the real last step even when the held playhead
  // state still points past the end of a previously longer list.
  const html = renderPanel([item("10", 1, "USER", 0)]);
  assert.match(html, /aria-label="Show step: user, iteration 1"/);
  assert.match(html, /step 1\/1/);
});

test("source guard: the render derives from the clamped playhead", () => {
  // Pin the mechanism, not just the outcome: `current` (and the aria slider
  // values) must come from the render-clamped `view`, never the raw `head`
  // state — a future refactor that drops the clamp reopens the crash.
  // The symlink path the imports above already use (the package's exports map
  // does not expose ./package.json, so resolve the file location directly).
  const src = readFileSync(
    join(
      dirname(fileURLToPath(import.meta.url)),
      "../../node_modules/@nanobpm/process-explorer/src/AgentSessionPanel.tsx",
    ),
    "utf8",
  );
  assert.ok(
    /const view = onHistoryChanged\(head, items\.length\)/.test(src),
    "render must clamp the playhead synchronously",
  );
  assert.ok(
    /const current = items\[view\.index\]/.test(src),
    "the current step must come from the clamped playhead",
  );
  assert.ok(
    !/const current = items\[head\.index\]/.test(src),
    "the raw playhead must not index the items",
  );
  assert.ok(
    /aria-label=\{`Show step:.*\$\{isPendingItem\(item\) \? ", in progress" : ""\}`\}/.test(
      src,
    ),
    "the transcript button's accessible name must carry the pending state",
  );
});
