import type { Instance } from "./gen";
import { InstanceStartedAt } from "./InstanceTimes";
import { stateTone } from "./stateTone";
import { Badge } from "./ui";

/**
 * One instance row in the list. `rowRef` is forwarded only for the selected
 * row so the list can scroll it into view (e.g. after a deep-link preselects an
 * instance that would otherwise be below the fold). `pinnedLabel` renders the
 * small "Linked" tag on the pinned deep-link row that sits above the page when
 * the selected instance isn't on the current page. When `card` is set (narrow /
 * mobile viewports) the row renders as a standalone tappable card with a
 * comfortable touch target instead of a flush list row.
 */
export function InstanceRow({
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
      <div className="flex items-center justify-between gap-2 font-mono text-xs text-fg-faint">
        <span>
          {inst.key} · v{inst.version}
        </span>
        <InstanceStartedAt ms={inst.start_date_ms} />
      </div>
    </button>
  );
}
