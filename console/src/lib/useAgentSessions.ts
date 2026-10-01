import { useQuery } from "@tanstack/react-query";
import {
  searchAgentInstanceHistory,
  searchAgentInstances,
} from "../gen-c8/sdk.gen";
import type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
} from "../gen-c8/types.gen";
import { orderHistory } from "./agentHistory.ts";

/// React Query keys. Both are in InstanceDetail's `useLiveInvalidation` list, so
/// every SSE `instances` edge (the exported position advanced, which includes a
/// newly committed AgentHistory item) refetches them: "live at turn".
export const AGENT_INSTANCES_KEY = "agent-instances";
export const AGENT_HISTORY_KEY = "agent-history";

// The engine's page ceiling (spec/search-models.yaml LimitPagination.maximum).
const PAGE_LIMIT = 10_000;

/// Every AgentInstance run inside one process instance (any BPMN element).
export function useAgentInstances(processInstanceKey: string | undefined) {
  return useQuery({
    queryKey: [AGENT_INSTANCES_KEY, processInstanceKey],
    enabled: processInstanceKey !== undefined,
    queryFn: async (): Promise<AgentInstanceResult[]> => {
      const out: AgentInstanceResult[] = [];
      let after: string | undefined;
      for (;;) {
        const { data } = await searchAgentInstances({
          throwOnError: true,
          body: {
            filter: { processInstanceKey },
            page: after ? { after, limit: PAGE_LIMIT } : { limit: PAGE_LIMIT },
          },
        });
        out.push(...data.items);
        if (!data.page.endCursor || data.items.length < PAGE_LIMIT) return out;
        after = data.page.endCursor;
      }
    },
  });
}

/// The committed turn log of one AgentInstance, in engine order. PENDING items
/// (an in-flight job's uncommitted writes) and DISCARDED ones (a failed job's)
/// are not part of the agent's history and are left out.
export function useAgentHistory(agentInstanceKey: string | undefined) {
  return useQuery({
    queryKey: [AGENT_HISTORY_KEY, agentInstanceKey],
    enabled: agentInstanceKey !== undefined,
    // Keep showing the previous log while a live refetch is in flight, so the
    // scrubber doesn't blank between turns.
    placeholderData: (prev) => prev,
    queryFn: async (): Promise<AgentInstanceHistoryItemResult[]> => {
      const out: AgentInstanceHistoryItemResult[] = [];
      let after: string | undefined;
      for (;;) {
        const { data } = await searchAgentInstanceHistory({
          throwOnError: true,
          path: { agentInstanceKey: agentInstanceKey! },
          body: {
            filter: { commitStatus: "COMMITTED" },
            sort: [
              { field: "loopIteration", order: "ASC" },
              { field: "producedAt", order: "ASC" },
              { field: "historyItemKey", order: "ASC" },
            ],
            page: after ? { after, limit: PAGE_LIMIT } : { limit: PAGE_LIMIT },
          },
        });
        out.push(...data.items);
        if (!data.page.endCursor || data.items.length < PAGE_LIMIT) {
          return orderHistory(out);
        }
        after = data.page.endCursor;
      }
    },
  });
}
