// The console's Process Explorer data source: the generated `/console/api`
// client (src/gen) and the Camunda REST subset (src/gen-c8) behind the
// `@nanobpm/process-explorer` seam. Every operator action is available here.
import type { ExplorerDataSource } from "@nanobpm/process-explorer";
import {
  cancelInstance,
  getInstance,
  getTrace,
  listInstances,
  resolveIncident,
  resumeInstance,
  setInstanceVariables,
  suspendInstance,
} from "../gen";
import {
  searchAgentInstanceHistory,
  searchAgentInstances,
} from "../gen-c8/sdk.gen";
import type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
} from "../gen-c8/types.gen";
import { fetchProcessXml } from "./api.ts";
import { subscribeToReadModel } from "./liveStream.ts";

// The engine's page ceiling (spec/search-models.yaml LimitPagination.maximum).
const PAGE_LIMIT = 10_000;

export const consoleExplorerSource: ExplorerDataSource = {
  listInstances: async (query) =>
    (await listInstances({ query, throwOnError: true })).data,
  getInstance: async (key) =>
    (await getInstance({ path: { key }, throwOnError: true })).data,
  // The trace is folded from a bounded in-memory ring, so it may be absent
  // (never traced, or evicted). Only a 404 means "no trace"; any other failure
  // (network, 5xx) is a real error the Explorer shows as such.
  getTrace: async (key) => {
    const result = await getTrace({ path: { key }, throwOnError: false });
    if (result.response?.status === 404) return undefined;
    if (result.error) throw result.error;
    return result.data;
  },
  getProcessXml: fetchProcessXml,
  cancelInstance: (key) =>
    cancelInstance({ path: { key }, throwOnError: true }),
  suspendInstance: (key) =>
    suspendInstance({ path: { key }, throwOnError: true }),
  resumeInstance: (key) =>
    resumeInstance({ path: { key }, throwOnError: true }),
  resolveIncident: (key, incidentKey) =>
    resolveIncident({ path: { key, incidentKey }, throwOnError: true }),
  setVariables: (key, body) =>
    setInstanceVariables({ path: { key }, body, throwOnError: true }),
  subscribe: subscribeToReadModel,
  agents: {
    listAgentInstances: async (processInstanceKey) => {
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
    // Committed items only: PENDING (an in-flight job's uncommitted writes) and
    // DISCARDED (a failed job's) are not part of the agent's history.
    getAgentHistory: async (agentInstanceKey) => {
      const out: AgentInstanceHistoryItemResult[] = [];
      let after: string | undefined;
      for (;;) {
        const { data } = await searchAgentInstanceHistory({
          throwOnError: true,
          path: { agentInstanceKey },
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
        if (!data.page.endCursor || data.items.length < PAGE_LIMIT) return out;
        after = data.page.endCursor;
      }
    },
  },
};
