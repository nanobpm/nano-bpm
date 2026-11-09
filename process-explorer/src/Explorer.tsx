import { type ReactNode, useEffect, useRef, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import type { Instance } from "./gen";
import { useExplorerData, useLiveInvalidation } from "./dataSource.tsx";
import { usePaneResize } from "./usePaneResize";
import { ResizeHandle } from "./ResizeHandle";
import InstanceDetail from "./InstanceDetail";
import { stateTone } from "./stateTone";
import { Badge, Button, useIsNarrow } from "./ui";
import {
  applyFilterChange,
  explorerStackView,
  filtersQueryKey,
  INSTANCE_DEEP_LINK_PARAM,
  INSTANCE_STATE_FILTERS,
  parseExplorerFilters,
  readInstanceParam,
  toInstanceQuery,
  type InstanceStateFilter,
} from "./explorerFilters";

const PAGE_SIZE = 50;

/**
 * One instance row in the list. `rowRef` is forwarded only for the selected
 * row so the list can scroll it into view (e.g. after a deep-link preselects an
 * instance that would otherwise be below the fold). `pinnedLabel` renders the
 * small "Linked" tag on the pinned deep-link row that sits above the page when
 * the selected instance isn't on the current page. When `card` is set (narrow /
 * mobile viewports) the row renders as a standalone tappable card with a
 * comfortable touch target instead of a flush list row.
 */
function InstanceRow({
  inst,
  selected,
  onSelect,
  rowRef,
  pinnedLabel,
  card = false,
}: {
  inst: Instance;
  selected: boolean;
  onSelect: (key: string) => void;
  rowRef?: (node: HTMLButtonElement | null) => void;
  pinnedLabel?: boolean;
  card?: boolean;
}) {
  const className = card
    ? `nano-touch flex w-full flex-col gap-1 rounded-xl border p-4 text-left shadow-sm transition-colors ${
        selected
          ? "border-accent/60 bg-accent/10"
          : "border-edge bg-raised hover:border-edge-strong hover:bg-hover"
      }`
    : `flex w-full flex-col gap-1 border-b border-edge px-5 py-3 text-left hover:bg-hover ${
        selected ? "bg-accent/10" : ""
      }`;
  return (
    <button
      type="button"
      ref={rowRef}
      onClick={() => onSelect(inst.key)}
      className={className}
    >
      <div className="flex items-center justify-between">
        <span className="flex items-center gap-2 font-medium text-fg">
          {inst.process_id}
          {pinnedLabel && (
            <span className="rounded bg-accent/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-accent-strong">
              Linked
            </span>
          )}
        </span>
        <Badge tone={stateTone(inst.state, inst.has_incident)}>
          {inst.has_incident ? "Incident" : inst.state}
        </Badge>
      </div>
      <div className="font-mono text-xs text-fg-faint">
        {inst.key} · v{inst.version}
      </div>
    </button>
  );
}

/**
 * A pill filter chip for the mobile, horizontally-scrollable filter row. Toggle
 * semantics are exposed via `aria-pressed`; the chip carries a 44px touch
 * target (`.nano-touch`) so it is comfortably tappable on a phone.
 */
function FilterChip({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={`nano-touch inline-flex shrink-0 items-center justify-center whitespace-nowrap rounded-full border px-4 text-sm transition-colors ${
        active
          ? "border-accent bg-accent text-on-accent"
          : "border-edge bg-panel text-fg-muted hover:bg-hover"
      }`}
    >
      {children}
    </button>
  );
}

/**
 * The Explorer keeps its filters and the `?instance=` deep link in URL search
 * params, so a filtered view is linkable and survives a reload. The host owns
 * the URL: pass react-router's `useSearchParams()` pair, or any equivalent.
 */
export interface ExplorerProps {
  searchParams: URLSearchParams;
  setSearchParams: (
    update: (prev: URLSearchParams) => URLSearchParams,
    options?: { replace?: boolean },
  ) => void;
  /** Rendered under the list header's filters (e.g. the console's v2 base URL). */
  headerExtra?: ReactNode;
  /** A `data-tour` anchor for the list header (the console's guided tours). */
  tourAnchor?: string;
  /** The page heading (default "Process instances"). */
  title?: ReactNode;
}

export default function Explorer({
  searchParams,
  setSearchParams,
  headerExtra,
  tourAnchor,
  title = "Process instances",
}: ExplorerProps) {
  const source = useExplorerData();
  const [selected, setSelected] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const isNarrow = useIsNarrow();

  // Resizable, reload-persistent process list (the left column). The user can
  // drag it narrower to give the model/detail pane more room; the width is
  // stored under `nano.explorer.listWidth` and re-clamped so the right pane
  // always keeps a usable minimum.
  const listResize = usePaneResize({
    storageKey: "nano.explorer.listWidth",
    axis: "x",
    initial: 448, // matches the previous fixed w-[28rem]
    min: 256,
    max: () =>
      typeof window === "undefined"
        ? 640
        : Math.max(320, window.innerWidth - 480),
  });

  // Allow deep-linking to a specific instance (e.g. from the modeler's "Start
  // instance" success link, or Urban's `?instance=` landing): ?instance=<key>
  // selects it, then the param is cleared so it doesn't pin the selection on
  // later navigation. On a narrow (mobile) viewport a set selection resolves to
  // the *detail* view (see `explorerStackView`), so the deep link genuinely
  // navigates to the detail rather than preselecting a row in an off-screen
  // pane — the contract unit A6's standalone landing depends on.
  useEffect(() => {
    const key = readInstanceParam(searchParams);
    if (key) {
      setSelected(key);
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          next.delete(INSTANCE_DEEP_LINK_PARAM);
          return next;
        },
        { replace: true },
      );
    }
  }, [searchParams, setSearchParams]);

  // Live: the SSE feed invalidates the list whenever the read model advances.
  // The prefix `["instances"]` invalidates every page query.
  useLiveInvalidation(["instances"]);

  // Active filters are sourced from the URL so a filtered view is
  // deep-linkable / reload-stable (cf. the `?instance=` deep-link above).
  const filters = parseExplorerFilters(searchParams);

  // Changing a filter rewrites the URL params and resets to the first page —
  // the filtered set can be smaller than the current offset, so keeping the old
  // page could strand the user on an out-of-range (empty) page.
  const setStateFilter = (state?: InstanceStateFilter) => {
    setSearchParams(
      (prev) => applyFilterChange(prev, { kind: "state", state }),
      {
        replace: true,
      },
    );
    setPage(0);
  };
  const setHasIncident = (hasIncident: boolean) => {
    setSearchParams(
      (prev) => applyFilterChange(prev, { kind: "hasIncident", hasIncident }),
      { replace: true },
    );
    setPage(0);
  };

  const { data, isLoading, error, isPlaceholderData } = useQuery({
    queryKey: ["instances", page, ...filtersQueryKey(filters)],
    queryFn: () =>
      source.listInstances({
        page,
        pageSize: PAGE_SIZE,
        ...toInstanceQuery(filters),
      }),
    // Keep the current page visible while the next one loads, so paging and the
    // live SSE refetch don't flash an empty list.
    placeholderData: keepPreviousData,
  });

  const total = data?.total ?? 0;
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const rangeStart = total === 0 ? 0 : page * PAGE_SIZE + 1;
  const rangeEnd = Math.min(
    total,
    page * PAGE_SIZE + (data?.items.length ?? 0),
  );

  // Is the selected instance present on the page currently in view? A deep-link
  // (or a selection made before paging) can point at an instance that lives on
  // another page — the list is paged server-side with no key filter, so we can't
  // page to it. Instead we fetch just that instance and pin a highlighted row
  // above the list so the selection is always visible, not silently off-screen.
  const items = data?.items ?? [];
  const selectedOnPage =
    selected != null && items.some((i: Instance) => i.key === selected);
  const pinnedQuery = useQuery({
    // Same key + fetch as InstanceDetail's, so React Query dedupes to one request.
    queryKey: ["instance", selected],
    queryFn: () => source.getInstance(selected!),
    enabled: selected != null && !selectedOnPage,
  });
  const pinned =
    selected != null && !selectedOnPage
      ? pinnedQuery.data?.instance
      : undefined;

  // Scroll the selected row into view whenever the selection (or the loaded
  // page) changes, so a preselected instance below the fold is revealed rather
  // than merely highlighted off-screen. `block: "nearest"` avoids jumping the
  // whole page when the row is already visible.
  //
  // Deliberately NOT depending on the `data` object: this view uses
  // `useLiveInvalidation(["instances"])`, so every SSE-driven read-model advance
  // refetches and hands back a fresh `data` reference. Keying the effect on
  // identity (`selected`, `page`, `pinned?.key`) plus `isPlaceholderData` means
  // we scroll only when the selection/page/pinned row actually changes or when a
  // freshly-requested page's data settles (placeholder → real) — never on a
  // same-page background refetch, which would otherwise snap the list back to
  // the selected row while the user is scrolling. `firstPageLoaded` covers the
  // initial mount, where `isPlaceholderData` stays false and never toggles.
  const selectedRowRef = useRef<HTMLButtonElement | null>(null);
  const firstPageLoaded = data != null;
  useEffect(() => {
    if (isPlaceholderData) return;
    selectedRowRef.current?.scrollIntoView({ block: "nearest" });
  }, [selected, page, pinned?.key, isPlaceholderData, firstPageLoaded]);

  // Clamp the page if the dataset shrinks (e.g. retention prune) below it.
  useEffect(() => {
    if (page > 0 && page >= pageCount) setPage(pageCount - 1);
  }, [page, pageCount]);

  const countText = data
    ? total === 0
      ? "0 instances"
      : `${rangeStart}–${rangeEnd} of ${total} instance(s)`
    : "Live view";

  // Desktop filter controls: the segmented state group + the incident checkbox.
  const filterBar = (
    <div className="mt-3 flex flex-wrap items-center gap-2">
      <div
        role="group"
        aria-label="Filter by state"
        className="inline-flex overflow-hidden rounded-md border border-edge"
      >
        <button
          type="button"
          aria-pressed={filters.state === undefined}
          onClick={() => setStateFilter(undefined)}
          className={`px-2.5 py-1 text-xs ${
            filters.state === undefined
              ? "bg-accent text-on-accent"
              : "bg-panel text-fg-muted hover:bg-hover"
          }`}
        >
          All
        </button>
        {INSTANCE_STATE_FILTERS.map((s) => (
          <button
            key={s}
            type="button"
            aria-pressed={filters.state === s}
            onClick={() => setStateFilter(s)}
            className={`border-l border-edge px-2.5 py-1 text-xs ${
              filters.state === s
                ? "bg-accent text-on-accent"
                : "bg-panel text-fg-muted hover:bg-hover"
            }`}
          >
            {s}
          </button>
        ))}
      </div>
      <label className="inline-flex items-center gap-1.5 text-xs text-fg-muted">
        <input
          type="checkbox"
          checked={filters.hasIncident}
          onChange={(e) => setHasIncident(e.target.checked)}
        />
        Has incident
      </label>
    </div>
  );

  // Mobile filter controls: a horizontally-scrollable chip row. The row bleeds
  // to the header's padding edges (`-mx-4 px-4`) and scrolls on its own
  // (`overflow-x-auto`), so the filters stay reachable without ever forcing the
  // page itself to scroll horizontally at 375px.
  const filterChips = (
    <div
      role="group"
      aria-label="Filter instances"
      className="-mx-4 mt-3 flex gap-2 overflow-x-auto px-4 pb-1"
    >
      <FilterChip
        active={filters.state === undefined}
        onClick={() => setStateFilter(undefined)}
      >
        All
      </FilterChip>
      {INSTANCE_STATE_FILTERS.map((s) => (
        <FilterChip
          key={s}
          active={filters.state === s}
          onClick={() => setStateFilter(s)}
        >
          {s}
        </FilterChip>
      ))}
      <FilterChip
        active={filters.hasIncident}
        onClick={() => setHasIncident(!filters.hasIncident)}
      >
        Incident
      </FilterChip>
    </div>
  );

  // The instance list body — shared by the desktop left pane and the mobile
  // stacked list. On mobile each row renders as a spaced, tappable card.
  const listBody = (
    <div className="min-h-0 flex-1 overflow-auto">
      {isLoading && <p className="p-5 text-fg-muted">Loading…</p>}
      {error && (
        <p className="p-5 text-danger">Failed to load: {String(error)}</p>
      )}
      {data && data.items.length === 0 && (
        <p className="p-5 text-fg-faint">
          {filters.state !== undefined || filters.hasIncident
            ? "No instances match the current filters."
            : "No instances yet. Deploy a process and create one."}
        </p>
      )}
      <ul className={isNarrow ? "flex flex-col gap-2 p-3" : ""}>
        {pinned && (
          <li key={`pinned-${pinned.key}`}>
            <InstanceRow
              inst={pinned}
              selected
              onSelect={setSelected}
              rowRef={(node) => (selectedRowRef.current = node)}
              pinnedLabel
              card={isNarrow}
            />
          </li>
        )}
        {items.map((inst: Instance) => (
          <li key={inst.key}>
            <InstanceRow
              inst={inst}
              selected={selected === inst.key}
              onSelect={setSelected}
              rowRef={
                selected === inst.key
                  ? (node) => (selectedRowRef.current = node)
                  : undefined
              }
              card={isNarrow}
            />
          </li>
        ))}
      </ul>
    </div>
  );

  const pager =
    total > PAGE_SIZE ? (
      <footer className="flex shrink-0 items-center justify-between border-t border-edge px-5 py-3 text-xs">
        <Button
          size="sm"
          onClick={() => setPage((p) => Math.max(0, p - 1))}
          disabled={page === 0}
        >
          ← Prev
        </Button>
        <span className="text-fg-faint">
          Page {page + 1} of {pageCount}
        </span>
        <Button
          size="sm"
          onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
          disabled={page >= pageCount - 1}
        >
          Next →
        </Button>
      </footer>
    ) : null;

  // Mobile: a stacked list → detail navigation flow instead of the resizable
  // master/detail split. Selecting a card sets `selected`, which resolves to the
  // detail view (`explorerStackView`); "Instances" navigates back to the list.
  // A `?instance=<key>` deep link sets `selected` on mount, so it lands directly
  // on the detail here rather than in an off-screen pane.
  if (isNarrow) {
    if (explorerStackView(selected) === "detail" && selected) {
      return (
        <div className="flex h-full min-w-0 flex-col">
          <header className="flex shrink-0 items-center gap-2 border-b border-edge px-4 py-3">
            <Button size="sm" onClick={() => setSelected(null)}>
              ← Instances
            </Button>
          </header>
          <div className="min-h-0 flex-1 overflow-auto">
            <InstanceDetail
              instanceKey={selected}
              onNavigateInstance={setSelected}
            />
          </div>
        </div>
      );
    }
    return (
      <div className="flex h-full min-w-0 flex-col">
        <header
          data-tour={tourAnchor}
          className="shrink-0 border-b border-edge px-4 py-4"
        >
          <h1 className="text-xl font-semibold text-fg">{title}</h1>
          <p className="text-xs text-fg-faint">{countText}</p>
          {filterChips}
          {headerExtra}
        </header>
        {listBody}
        {pager}
      </div>
    );
  }

  // Desktop: the resizable master/detail split (the left list + the detail pane).
  return (
    <div className="flex h-full">
      <div
        style={{ width: listResize.size }}
        className="flex shrink-0 flex-col"
      >
        <header
          data-tour={tourAnchor}
          className="border-b border-edge px-5 py-4"
        >
          <h1 className="text-xl font-semibold text-fg">{title}</h1>
          <p className="text-xs text-fg-faint">{countText}</p>
          {filterBar}
          {headerExtra}
        </header>
        {listBody}
        {pager}
      </div>

      <ResizeHandle
        axis="x"
        label="Resize the process list"
        onPointerDown={listResize.onPointerDown}
        onKeyDown={listResize.onKeyDown}
        dragging={listResize.dragging}
        size={listResize.size}
        min={listResize.min}
        max={listResize.max}
      />

      <div className="min-w-0 flex-1 overflow-auto">
        {selected ? (
          <InstanceDetail
            instanceKey={selected}
            onNavigateInstance={setSelected}
          />
        ) : (
          <div className="flex h-full items-center justify-center text-sm text-fg-faint">
            Select an instance to inspect it.
          </div>
        )}
      </div>
    </div>
  );
}
