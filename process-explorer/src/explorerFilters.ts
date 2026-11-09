// Pure filter logic for the Process Instances Explorer, kept out of the React
// component so it can be unit-tested with the Node test runner (no DOM).
//
// The Explorer filters the instance list by lifecycle **state** and by whether
// an instance carries an open **incident**. Both are applied server-side (so the
// pager total stays correct) and both are reflected in the URL query string so a
// filtered view is deep-linkable / reload-stable.
//
// URL contract (mirrors the existing `?instance=` deep-link pattern):
//   - `state=Active|Suspended|Completed|Terminated`  (absent = All)
//   - `incident=1`                          (absent = no incident constraint)

/**
 * The name of the deep-link query param that preselects — and, on a narrow
 * (mobile) viewport, navigates to — a single instance: `?instance=<key>`. This
 * is the live consumer end of the Urban → Console deep-link contract, so the
 * wire name lives in exactly one place that the Explorer and any standalone
 * `/console/explorer?instance=<key>` landing (unit A6) both read.
 */
export const INSTANCE_DEEP_LINK_PARAM = "instance";

/**
 * Read the `?instance=<key>` deep-link target from the URL, or `null` when it is
 * absent or blank. A whitespace-only value is treated as absent so a malformed
 * link degrades to the plain list rather than fetching a bogus key.
 */
export function readInstanceParam(params: URLSearchParams): string | null {
  const key = params.get(INSTANCE_DEEP_LINK_PARAM);
  if (key == null) return null;
  const trimmed = key.trim();
  return trimmed === "" ? null : trimmed;
}

/**
 * Which pane the *stacked* (mobile) Explorer shows: the instance **list** when
 * nothing is selected, or the **detail** once an instance is selected. This is
 * exactly what makes `?instance=<key>` land on the detail on mobile — the
 * deep-link sets the selection (see {@link readInstanceParam}), and a set
 * selection resolves to the detail view here, rather than merely preselecting a
 * row in an off-screen desktop pane. Unit A6's standalone
 * `/console/explorer?instance=<key>` landing relies on this behaviour.
 */
export function explorerStackView(selected: string | null): "list" | "detail" {
  return selected != null && selected !== "" ? "detail" : "list";
}

/** The lifecycle states the console can filter on (the console-api enum). */
export type InstanceStateFilter =
  "Active" | "Suspended" | "Completed" | "Terminated";

export const INSTANCE_STATE_FILTERS: readonly InstanceStateFilter[] = [
  "Active",
  "Suspended",
  "Completed",
  "Terminated",
];

/** The parsed, normalized filter selection driving the instance query. */
export type ExplorerFilters = {
  /** Selected state, or `undefined` for "All" (no state constraint). */
  state?: InstanceStateFilter;
  /** When true, restrict to instances with an open incident. */
  hasIncident: boolean;
};

/** The `state` query param the console API accepts (undefined = All). */
function isStateFilter(value: string | null): value is InstanceStateFilter {
  return INSTANCE_STATE_FILTERS.includes(value as InstanceStateFilter);
}

/**
 * Derive the active filters from URL search params. Unknown / absent values
 * fall back to the unconstrained defaults (All states, no incident constraint),
 * so a malformed deep-link degrades to the full list rather than erroring.
 */
export function parseExplorerFilters(params: URLSearchParams): ExplorerFilters {
  const stateParam = params.get("state");
  return {
    state: isStateFilter(stateParam) ? stateParam : undefined,
    hasIncident: params.get("incident") === "1",
  };
}

/** The `listInstances` query object shape for the filter dimensions. */
export type InstanceQueryFilters = {
  state?: InstanceStateFilter;
  hasIncident?: boolean;
};

/**
 * Build the `query` filter fields passed to `listInstances`. Omits a dimension
 * entirely when unconstrained so "no param = current behaviour" holds on the
 * wire (an omitted param means no constraint, byte-for-byte the old request).
 */
export function toInstanceQuery(
  filters: ExplorerFilters,
): InstanceQueryFilters {
  const query: InstanceQueryFilters = {};
  if (filters.state) query.state = filters.state;
  if (filters.hasIncident) query.hasIncident = true;
  return query;
}

/**
 * A stable, serializable representation of the filters for a React Query
 * `queryKey`, so each distinct filter combination caches (and refetches)
 * independently while sharing the `["instances"]` prefix that live SSE
 * invalidation targets.
 */
export function filtersQueryKey(
  filters: ExplorerFilters,
): [string | null, boolean] {
  return [filters.state ?? null, filters.hasIncident];
}

/** A single change to one filter dimension. */
export type FilterChange =
  | { kind: "state"; state?: InstanceStateFilter }
  | { kind: "hasIncident"; hasIncident: boolean };

/**
 * Apply a filter change to the current search params, returning the next params
 * to push. Callers pair this with a reset of the pager to page 0 (see
 * {@link shouldResetPageOnFilterChange}) so a narrowed set never leaves the user
 * stranded on an out-of-range page. Other params (e.g. `instance`) are
 * preserved untouched.
 */
export function applyFilterChange(
  current: URLSearchParams,
  change: FilterChange,
): URLSearchParams {
  const next = new URLSearchParams(current);
  if (change.kind === "state") {
    if (change.state) next.set("state", change.state);
    else next.delete("state");
  } else {
    if (change.hasIncident) next.set("incident", "1");
    else next.delete("incident");
  }
  return next;
}

/**
 * Whether a filter change should reset the pager to the first page. Always true
 * — the filtered set can be smaller than the current page offset — but exposed
 * as a named predicate so the intent is explicit and testable.
 */
export function shouldResetPageOnFilterChange(): boolean {
  return true;
}
