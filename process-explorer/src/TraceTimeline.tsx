import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { InstanceTrace, TraceElement } from "./gen";
import { SectionLabel } from "./ui";
import { IncidentReason } from "./IncidentReason";
import { fmtClock, fmtDuration, headlineDuration } from "./traceTime.ts";

// Re-exported so existing importers of `../components/TraceTimeline` keep
// working; the implementations live in the JSX-free `traceTime.ts` so they are
// unit-testable under `node --test`.
export { fmtClock, fmtDuration } from "./traceTime.ts";

/// How the timeline turns the trace's numeric timestamps into labels. Production
/// traces are wall-clock milliseconds (the default); the in-browser simulation
/// uses a logical step index instead (its virtual clock only advances on an
/// explicit "advance time"), so it supplies a step formatter.
export interface TraceTimeFmt {
  /** Label a span/duration between two timestamps. */
  duration: (n: number | null) => string;
  /** Label an absolute timestamp. */
  clock: (n: number) => string;
  /** Axis origin label (e.g. "0 ms" or "step 0"). */
  origin: string;
  /**
   * "Now" in the trace's clock domain, used for the elapsed time of an
   * unfinished job. Production traces pass `Date.now`; the simulation passes
   * its current event-sequence index — its timestamps are logical steps, so a
   * wall-clock `Date.now()` would headline ~1.8 trillion "steps". When
   * omitted the timeline falls back to `Date.now()` and ticks it (below).
   */
  now?: (trace: InstanceTrace) => number;
  /**
   * When true (and `now` is omitted), a shared 1 s clock tick re-renders the
   * rows while any job is still running, so its elapsed label advances even
   * when no new trace events arrive. Ignored for logical-clock traces (their
   * `now` only moves with the simulation's own events).
   */
  tickWhenRunning?: boolean;
}

export const msFmt: TraceTimeFmt = {
  duration: fmtDuration,
  clock: fmtClock,
  origin: "0 ms",
  tickWhenRunning: true,
};

/// The shared trace timeline: one lane per element instance with its lifetime
/// track, the job's queue (warn) / service (ok) segments, and an incident
/// table. Used by both the production Traces tab and the modeler's in-browser
/// test-run trace (which passes a step formatter).
export function TraceTimeline({
  trace,
  fmt = msFmt,
}: {
  trace: InstanceTrace;
  fmt?: TraceTimeFmt;
}) {
  const { t0, span } = useMemo(() => {
    const t0 = trace.startedAt;
    let max = trace.endedAt ?? trace.startedAt;
    for (const el of trace.elements) {
      max = Math.max(max, el.enteredAt, el.exitedAt ?? el.enteredAt);
      if (el.job) {
        max = Math.max(
          max,
          el.job.createdAt,
          el.job.activatedAt ?? el.job.createdAt,
          el.job.completedAt ?? el.job.createdAt,
        );
      }
    }
    return { t0, span: Math.max(max - t0, 1) };
  }, [trace]);

  const pct = (t: number) => ((t - t0) / span) * 100;

  // "Now" in the trace's own clock domain. A logical-clock trace (the
  // simulation) supplies `fmt.now`; a wall-clock trace defaults to Date.now()
  // and — while the trace is still active and a job is still running — a
  // single shared 1 s tick so the elapsed labels advance even when no new
  // trace events arrive (the trace queries refresh on read-model change
  // notifications, not a timer). An ended trace's rows are frozen, so no tick.
  const anyRunning =
    trace.endedAt == null &&
    trace.elements.some((el) => el.job != null && el.job.completedAt == null);
  const ticking = fmt.now == null && fmt.tickWhenRunning === true && anyRunning;
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!ticking) return;
    const t = setInterval(() => setTick((n) => n + 1), 1000);
    return () => clearInterval(t);
  }, [ticking]);
  void tick;
  const now = fmt.now ? fmt.now(trace) : Date.now();

  return (
    <div>
      <Legend />
      <div className="mt-4 space-y-1.5">
        {trace.elements.map((el) => (
          <ElementRow
            key={el.elementInstanceKey}
            el={el}
            pct={pct}
            fmt={fmt}
            now={now}
            traceEndedAt={trace.endedAt}
          />
        ))}
      </div>
      <div className="mt-2 flex justify-between border-t border-edge pt-1 font-mono text-[10px] text-fg-faint">
        <span>{fmt.origin}</span>
        <span>{fmt.duration(Math.round(span))}</span>
      </div>

      {trace.incidents.length > 0 && (
        <section className="mt-8">
          <SectionLabel>Incidents</SectionLabel>
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b border-edge text-left text-fg-faint">
                {["Element", "Kind", "Reason", "Raised", "Resolved"].map(
                  (h) => (
                    <th key={h} className="py-2 pr-4 font-medium">
                      {h}
                    </th>
                  ),
                )}
              </tr>
            </thead>
            <tbody>
              {trace.incidents.map((inc) => (
                <tr
                  key={`${inc.elementInstanceKey}:${inc.raisedAt}:${inc.kind}`}
                  className="border-b border-edge"
                >
                  <td className="py-2 pr-4 font-mono">{inc.elementId}</td>
                  <td className="py-2 pr-4">{inc.kind}</td>
                  <td className="py-2 pr-4 align-top" title={inc.reason}>
                    <IncidentReason reason={inc.reason} />
                  </td>
                  <td className="py-2 pr-4 font-mono text-fg-faint">
                    {fmt.clock(inc.raisedAt)}
                  </td>
                  <td className="py-2 pr-4 font-mono text-fg-faint">
                    {inc.resolvedAt ? fmt.clock(inc.resolvedAt) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}

function ElementRow({
  el,
  pct,
  fmt,
  now,
  traceEndedAt,
}: {
  el: TraceElement;
  pct: (t: number) => number;
  fmt: TraceTimeFmt;
  now: number;
  traceEndedAt: number | null;
}) {
  const job = el.job;
  const start = el.enteredAt;
  const end =
    el.exitedAt ?? Math.max(el.enteredAt, job?.completedAt ?? el.enteredAt);

  const headline = headlineDuration(el, now, fmt.duration, traceEndedAt);
  const left = pct(start);
  const width = Math.max(pct(end) - left, 0.6);

  return (
    <div className="flex items-center gap-3">
      <div
        className="w-44 shrink-0 truncate font-mono text-xs text-fg-muted"
        title={el.elementId}
      >
        {el.elementId}
        {el.incidents > 0 && <span className="ml-1 text-danger">⚠</span>}
      </div>
      <div className="relative h-6 flex-1 rounded bg-inset">
        <div
          className="absolute top-1/2 h-3 -translate-y-1/2 rounded-sm bg-edge-strong"
          style={{ left: `${left}%`, width: `${width}%` }}
          title={`${el.elementId} · ${headline.title}`}
        />
        {job && <JobBars job={job} pct={pct} fmt={fmt} />}
      </div>
      <div
        className="w-28 shrink-0 text-right font-mono text-[11px] text-fg-faint"
        title={headline.title}
      >
        {headline.label}
      </div>
    </div>
  );
}

function JobBars({
  job,
  pct,
  fmt,
}: {
  job: NonNullable<TraceElement["job"]>;
  pct: (t: number) => number;
  fmt: TraceTimeFmt;
}) {
  const segments: ReactNode[] = [];
  const created = job.createdAt;
  const activated = job.activatedAt;
  const completed = job.completedAt;

  if (activated != null) {
    const qLeft = pct(created);
    const qWidth = Math.max(pct(activated) - qLeft, 0.4);
    segments.push(
      <div
        key="q"
        className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded-l-sm bg-warn/80"
        style={{ left: `${qLeft}%`, width: `${qWidth}%` }}
        title={`queue ${fmt.duration(job.queueMs)} · worker ${job.worker ?? "—"}`}
      />,
    );
    const sEnd = completed ?? activated;
    const sLeft = pct(activated);
    const sWidth = Math.max(pct(sEnd) - sLeft, 0.4);
    segments.push(
      <div
        key="s"
        className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded-r-sm bg-ok/80"
        style={{ left: `${sLeft}%`, width: `${sWidth}%` }}
        title={`service ${fmt.duration(job.serviceMs)} · worker ${job.worker ?? "—"}`}
      />,
    );
  } else {
    const end = completed ?? created;
    const left = pct(created);
    const width = Math.max(pct(end) - left, 0.4);
    segments.push(
      <div
        key="w"
        className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded-sm bg-warn/50"
        style={{ left: `${left}%`, width: `${width}%` }}
        title={`wait ${fmt.duration(job.waitMs)} (activation not observed)`}
      />,
    );
  }
  return <>{segments}</>;
}

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-4 text-xs text-fg-muted">
      <Swatch className="bg-edge-strong">element</Swatch>
      <Swatch className="bg-warn/80">job queue</Swatch>
      <Swatch className="bg-ok/80">job service</Swatch>
      <Swatch className="bg-warn/50">job wait (activation unobserved)</Swatch>
    </div>
  );
}

function Swatch({
  className,
  children,
}: {
  className: string;
  children: ReactNode;
}) {
  return (
    <span className="flex items-center gap-1.5">
      <span className={`inline-block h-3 w-3 rounded-sm ${className}`} />
      {children}
    </span>
  );
}
