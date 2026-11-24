// Compile-time check of the public `readmodel-types` surface (`npm run check`):
// every name re-exported by index.d.ts must exist in the generated
// types.gen.ts, and the agent search request types must express the spec's
// full query (advanced operators, date ranges, sort, cursor paging) — the
// shape the WASM engine's searchAgentInstances / searchAgentInstanceHistory
// accept (server/WASM parity guard: engine-wasm/src/search_parity_tests.rs).
import type {
  AgentInstanceHistorySearchQuery,
  AgentInstanceSearchQuery,
  AgentInstanceSearchQueryResult,
  SearchQueryPageRequest,
} from "./index.js";

const page: SearchQueryPageRequest = { limit: 2, after: "AAAAAAAAAAAI" };

export const instances: AgentInstanceSearchQuery = {
  filter: {
    creationDate: { $gte: "2026-01-01T00:00:00Z", $lt: "2027-01-01T00:00:00Z" },
    status: { $in: ["IDLE", "THINKING"] },
    elementId: { $like: "agent*" },
    elementInstanceKeys: ["12", { $neq: "13" }],
  },
  sort: [{ field: "creationDate", order: "DESC" }],
  page,
};

export const history: AgentInstanceHistorySearchQuery = {
  filter: { commitStatus: "PENDING", loopIteration: { $gte: 1 }, role: "ASSISTANT" },
  sort: [{ field: "producedAt" }],
  page: { limit: 10, before: "AAAAAAAAAAAI" },
};

export const firstKey = (r: AgentInstanceSearchQueryResult): string | undefined =>
  r.items[0]?.agentInstanceKey;
