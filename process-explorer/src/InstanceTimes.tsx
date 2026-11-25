import { fmtClock, fmtDuration } from "./traceTime.ts";

/** The clock/duration formatters an absolute-time label needs. Structurally a
 * subset of `TraceTimeFmt`, so a trace's own formatter can be passed through. */
export interface ClockFmt {
  clock: (n: number) => string;
  duration: (n: number | null) => string;
}

const wallClock: ClockFmt = { clock: fmtClock, duration: fmtDuration };

/**
 * An instance's wall-clock start time (e.g. on an instance-list row), with its
 * relative age as the tooltip. `now` is injectable for deterministic tests.
 */
export function InstanceStartedAt({
  ms,
  now = Date.now(),
}: {
  ms: number;
  now?: number;
}) {
  return (
    <span
      className="shrink-0"
      title={`started ${fmtDuration(Math.max(0, now - ms))} ago`}
    >
      {fmtClock(ms)}
    </span>
  );
}

/**
 * The lifespan line for an instance or a trace: started at, ended at (if it
 * ended), total duration (if known) and — for a suspended instance — when it
 * was suspended. The single component every instance/trace header uses, so the
 * absolute times are formatted in one place (`traceTime.ts`).
 */
export function InstanceTimes({
  startedAt,
  endedAt = null,
  durationMs = null,
  suspendedDate = null,
  fmt = wallClock,
}: {
  startedAt: number;
  endedAt?: number | null;
  durationMs?: number | null;
  /** ISO-8601 datetime of the latest suspension (`Instance.suspendedDate`). */
  suspendedDate?: string | null;
  fmt?: ClockFmt;
}) {
  const duration =
    durationMs ?? (endedAt != null ? Math.max(0, endedAt - startedAt) : null);
  const suspendedAt = suspendedDate != null ? Date.parse(suspendedDate) : NaN;
  return (
    <span>
      started {fmt.clock(startedAt)}
      {endedAt != null && <> · ended {fmt.clock(endedAt)}</>}
      {duration != null && <> · took {fmt.duration(duration)}</>}
      {!Number.isNaN(suspendedAt) && <> · suspended {fmt.clock(suspendedAt)}</>}
    </span>
  );
}
