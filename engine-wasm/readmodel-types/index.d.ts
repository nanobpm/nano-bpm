// Public surface of `@nanobpm/engine-wasm/readmodel-types`: the derived DTO
// types for the read-model engine's query results, so browser consumers
// (@nanobpm/bojtos-kit → the web-demo-framework) can type
// `searchUserTasks` / `searchProcessInstances` / `searchVariables` /
// `getFormByKey` / `getResourceByKey` without hand-mirroring the shapes, and
// build agent search requests (`searchAgentInstances` /
// `searchAgentInstanceHistory`) against the spec's own filter/query/page types.
//
// These are re-exported from the generated `types.gen.d.ts`, which is derived
// from the single source of truth — the Camunda-parity REST OpenAPI in `spec/`
// — via `engine-wasm/readmodel-types` (`npm run gen`). Do not hand-edit; a
// stale artifact fails CI (`npm run check` = regen + `git diff --exit-code`).
export type {
  // `searchUserTasks(filter): UserTaskSearchQueryResult`
  UserTaskSearchQueryResult,
  UserTaskResult,
  // `searchProcessInstances(filter): ProcessInstanceSearchQueryResult`
  ProcessInstanceSearchQueryResult,
  ProcessInstanceResult,
  // `searchVariables(filter): VariableSearchQueryResult`
  VariableSearchQueryResult,
  VariableResult,
  // `getFormByKey(key): FormResult | null`
  FormResult,
  // `getResourceByKey(key): ResourceResult | null`
  ResourceResult,
  // Shared search envelope (`{ items, page }`) the *SearchQueryResult types extend.
  SearchQueryResponse,
  SearchQueryPageResponse,

  // ── Agent searches (#1883) ──────────────────────────────────────────────
  // `searchAgentInstances(query: AgentInstanceSearchQuery)` and
  // `searchAgentInstanceHistory(key, query: AgentInstanceHistorySearchQuery)`
  // run the SAME spec-derived search layer as the gateway (every advanced
  // filter operator, sort, limit/offset/cursor paging), so the request types
  // below are the contract the engine accepts, not just what it returns.
  AgentInstanceSearchQuery,
  AgentInstanceFilter,
  AgentInstanceSearchQuerySortRequest,
  AgentInstanceSearchQueryResult,
  AgentInstanceResult,
  AgentInstanceStatusEnum,
  AgentInstanceHistorySearchQuery,
  AgentInstanceHistoryFilter,
  AgentInstanceHistorySearchQuerySortRequest,
  AgentInstanceHistorySearchQueryResult,
  AgentInstanceHistoryItemResult,
  AgentInstanceHistoryRoleEnum,
  AgentInstanceHistoryCommitStatusEnum,
  // Shared search request envelope + paging (`page`) and sort order.
  SearchQueryRequest,
  SearchQueryPageRequest,
  LimitPagination,
  OffsetPagination,
  CursorForwardPagination,
  CursorBackwardPagination,
  SortOrderEnum,
  // Filter properties: a bare value (implicit `$eq`) or an advanced filter.
  StringFilterProperty,
  IntegerFilterProperty,
  DateTimeFilterProperty,
  ElementIdFilterProperty,
  AgentInstanceKeyFilterProperty,
  AgentDefinitionKeyFilterProperty,
  AgentHistoryItemKeyFilterProperty,
  ElementInstanceKeyFilterProperty,
  JobKeyFilterProperty,
  ProcessInstanceKeyFilterProperty,
  ProcessDefinitionKeyFilterProperty,
  AgentInstanceStatusFilterProperty,
  AgentInstanceHistoryRoleFilterProperty,
  AgentInstanceHistoryCommitStatusFilterProperty,
  // Advanced filter operator objects (`$eq`, `$neq`, `$exists`, `$in`,
  // `$notIn`, `$like`, `$gt`, `$gte`, `$lt`, `$lte`).
  BasicStringFilter,
  AdvancedStringFilter,
  LikeFilter,
  AdvancedIntegerFilter,
  AdvancedDateTimeFilter,
  AdvancedElementIdFilter,
  AdvancedAgentInstanceKeyFilter,
  AdvancedAgentDefinitionKeyFilter,
  AdvancedAgentHistoryItemKeyFilter,
  AdvancedElementInstanceKeyFilter,
  AdvancedJobKeyFilter,
  AdvancedProcessInstanceKeyFilter,
  AdvancedProcessDefinitionKeyFilter,
  AdvancedAgentInstanceStatusFilter,
  AdvancedAgentInstanceHistoryRoleFilter,
  AdvancedAgentInstanceHistoryCommitStatusFilter,
} from "./types.gen.js";
