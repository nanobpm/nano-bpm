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

/** The job-timing fields the headline needs (structural subset of `TraceJob`). */
export interface HeadlineJob {
  createdAt: number;
  activatedAt: number | null;
  completedAt: number | null;
  waitMs: number | null;
  queueMs: number | null;
  serviceMs: number | null;
}

/** The structural subset of `TraceElement` the headline needs. */
export interface HeadlineElement {
  durationMs: number | null;
  /** When the element exited (completed or was terminated), if it has. */
  exitedAt?: number | null;
  job?: HeadlineJob | null;
}

export interface HeadlineDuration {
  /** How long the step took (or has been running), in ms. */
  ms: number | null;
  /** `true` when the job was activated but has not completed yet. */
  running: boolean;
  /** Secondary detail: queue time (created → activated), never the headline. */
  queueMs: number | null;
  /** The right-hand column label. */
  label: string;
  /** The row tooltip — derived from the same values as `label`. */
  title: string;
}

/**
 * The single canonical "how long did this step take" for a trace row (#1557).
 * Jobs: service time (activated → completed) when activation was observed, else
 * wait time (created → completed); an activated-but-incomplete job shows
 * elapsed time since activation, marked running. Queue time is secondary detail
 * only. Non-job elements: `durationMs`.
 *
 * `now` must come from the trace's own clock domain: wall-clock ms for a
 * production trace, the current event-sequence index for a simulation (whose
 * timestamps are logical steps — passing `Date.now()` there would headline
 * ~1.8 trillion "steps").
 *
 * `traceEndedAt` is the trace's end timestamp in that same domain (`null` while
 * the instance is active). A job left unfinished when its trace ENDED —
 * canceled work: the trace store has no `JobCanceled` arm, so `completedAt`
 * stays null — must not keep counting up to the viewer's clock: it measures to
 * the trace end. While the trace is still active (`traceEndedAt == null`) an
 * unfinished job is in flight and counts up to `now`, marked running.
 */
export function headlineDuration(
  el: HeadlineElement,
  now: number = Date.now(),
  fmt: (ms: number | null) => string = fmtDuration,
  traceEndedAt: number | null = null,
): HeadlineDuration {
  const job = el.job;
  if (!job) {
    const label = fmt(el.durationMs);
    return {
      ms: el.durationMs,
      running: false,
      queueMs: null,
      label,
      title: label,
    };
  }
  const queueMs =
    job.queueMs ??
    (job.activatedAt != null ? job.activatedAt - job.createdAt : null);
  const queueNote = queueMs != null ? ` · queue ${fmt(queueMs)}` : "";
  if (job.completedAt == null) {
    const since = job.activatedAt ?? job.createdAt;
    // A job left unfinished whose element EXITED (e.g. canceled by an
    // interrupting boundary while the instance continues) stops at the
    // element's exit; otherwise, if the trace ended, at the trace end. Only a
    // job whose element is still live counts up to `now`.
    const stopAt = el.exitedAt ?? traceEndedAt;
    if (stopAt != null) {
      const ms = Math.max(0, stopAt - since);
      const label = fmt(ms);
      const what =
        job.activatedAt != null ? "ended" : "ended (not yet activated)";
      return {
        ms,
        running: false,
        queueMs,
        label,
        title: `${what} ${label}${queueNote}`,
      };
    }
    const ms = Math.max(0, now - since);
    const label = `${fmt(ms)} …`;
    const what =
      job.activatedAt != null ? "running" : "waiting (not yet activated)";
    return {
      ms,
      running: true,
      queueMs,
      label,
      title: `${what} ${fmt(ms)}${queueNote}`,
    };
  }
  if (job.activatedAt != null) {
    const ms = job.serviceMs ?? job.completedAt - job.activatedAt;
    const label = fmt(ms);
    return {
      ms,
      running: false,
      queueMs,
      label,
      title: `service ${label}${queueNote}`,
    };
  }
  const ms = job.waitMs ?? job.completedAt - job.createdAt;
  const label = fmt(ms);
  return {
    ms,
    running: false,
    queueMs,
    label,
    title: `wait ${label} (activation not observed)`,
  };
}
