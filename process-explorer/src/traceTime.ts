// Pure timestamp/duration formatters for the trace timeline, Jobs list, and
// anywhere else the console labels an event. Kept in a `.ts` (no JSX) module so
// they are unit-testable under `node --test` (the strip-types runner can't load
// a `.tsx`). `TraceTimeline.tsx` re-exports these for existing importers.

/** Format a duration given in milliseconds. */
export function fmtDuration(ms: number | null): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.floor((ms % 60_000) / 1000);
  return `${m}m ${s}s`;
}

/**
 * Format an absolute wall-clock timestamp (ms since the Unix epoch) in the
 * viewer's LOCAL time zone as e.g. `11:23am Aug 25`. The `en-US` locale is fixed
 * so the 12-hour am/pm form and the short English month render consistently
 * regardless of the browser's locale; the time zone stays local (Intl uses the
 * host zone when none is supplied).
 */
export function fmtClock(ms: number): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    month: "short",
    day: "numeric",
  }).formatToParts(new Date(ms));
  const get = (type: Intl.DateTimeFormatPartTypes): string =>
    parts.find((p) => p.type === type)?.value ?? "";
  const period = get("dayPeriod")
    .toLowerCase()
    .replace(/[^a-z]/g, "");
  return `${get("hour")}:${get("minute")}${period} ${get("month")} ${get("day")}`;
}
