import type { PaneResize } from "./usePaneResize";

/**
 * The draggable divider for a {@link usePaneResize} pane. Spread the hook's
 * result plus the resize `axis`; the handle renders a thin hit-target that
 * widens/brightens on hover and while dragging, shows the correct resize
 * cursor, and exposes an accessible `separator` role with keyboard support
 * (arrow keys nudge the pane).
 *
 * `label` names what the handle resizes (for screen readers). It is absolutely
 * positioned over the seam between the two panes, so its parent flex/column
 * child (the pane it follows) must be `relative` OR the handle can sit as its
 * own flex child — here it is a normal flex child with negative margin so it
 * overlaps the 1px border seam without shifting layout.
 */
export function ResizeHandle({
  axis,
  onPointerDown,
  onKeyDown,
  dragging,
  size,
  min,
  max,
  label,
}: Pick<
  PaneResize,
  "onPointerDown" | "onKeyDown" | "dragging" | "size" | "min" | "max"
> & {
  axis: "x" | "y";
  label: string;
}) {
  const horizontal = axis === "x";
  return (
    <div
      role="separator"
      aria-orientation={horizontal ? "vertical" : "horizontal"}
      aria-label={label}
      aria-valuenow={Math.round(size)}
      aria-valuemin={Math.round(min)}
      aria-valuemax={Math.round(max)}
      tabIndex={0}
      onPointerDown={onPointerDown}
      onKeyDown={onKeyDown}
      className={[
        "group relative z-10 shrink-0 touch-none",
        horizontal
          ? "-mx-1 w-2 cursor-col-resize"
          : "-my-1 h-2 cursor-row-resize",
        "flex items-center justify-center",
        "focus:outline-none",
      ].join(" ")}
    >
      {/* The visible seam: a hairline that thickens/brightens on hover, focus,
          and during a drag. */}
      <span
        className={[
          "pointer-events-none rounded-full transition-colors",
          horizontal ? "h-full w-px" : "h-px w-full",
          dragging
            ? "bg-accent"
            : "bg-edge group-hover:bg-accent/60 group-focus:bg-accent/60",
        ].join(" ")}
      />
    </div>
  );
}
