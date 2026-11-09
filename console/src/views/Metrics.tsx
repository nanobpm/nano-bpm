import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import { nodeConsoleUrl } from "../lib/api";
import { metricsStore } from "../lib/metricsStore";
import {
  Badge,
  Button,
  Card,
  ErrorText,
  PageHeader,
  SectionLabel,
} from "@nanobpm/process-explorer/ui";

export default function Metrics() {
  const state = useSyncExternalStore(
    metricsStore.subscribe,
    metricsStore.getSnapshot,
  );
  const { samples, latest: data, cluster, clusterRates, paused, error } = state;
  const isLoading = !data;
  const reset = () => metricsStore.clear();

  const isCluster = (cluster?.aggregate.totalNodes ?? 1) > 1;

  const latest = samples[samples.length - 1];

  return (
    <div className="p-8">
      <PageHeader
        title="Metrics"
        subtitle={
          <>
            Live throughput &amp; durability, sourced from the Prometheus
            surface
            {" · "}
            <a className="underline hover:text-fg" href="/metrics">
              /metrics
            </a>
          </>
        }
        actions={
          <>
            <Button size="sm" onClick={() => metricsStore.setPaused(!paused)}>
              {paused ? "Resume" : "Pause"}
            </Button>
            <Button size="sm" onClick={reset}>
              Clear
            </Button>
          </>
        }
      />

      {isLoading && !data && <p className="text-fg-muted">Loading…</p>}
      {error && <ErrorText>Failed to load metrics: {String(error)}</ErrorText>}

      {data && (
        <div className="space-y-8">
          {/* Headline live stats */}
          <section className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
            <Stat
              label="Process starts/s"
              value={fmt(latest?.startsPerSec ?? 0, 0)}
              accent="emerald"
            />
            <Stat
              label="Jobs completed/s"
              value={fmt(latest?.jobsPerSec ?? 0, 0)}
              accent="sky"
            />
            <Stat
              label="Active processes"
              value={data.activeInstances.toLocaleString()}
            />
            <Stat
              label="Connected clients"
              value={data.connectionsActive.toLocaleString()}
            />
          </section>

          {/* Capacity ceiling — the clipping LEDs (ADR 0013) */}
          <section>
            <div className="mb-2 flex items-center gap-2">
              <SectionLabel>Capacity ceiling</SectionLabel>
              <span className="-mt-1">
                <SlaBadge mode={data.slaMode} />
              </span>
            </div>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
              <CeilingLed
                label="Throughput"
                active={data.ceilingThroughput}
                help={CEILING_HELP.throughput}
                detail={
                  data.admissionBacklogLimit > 0
                    ? `backlog ${data.activeBacklog.toLocaleString()} / ${data.admissionBacklogLimit.toLocaleString()}`
                    : `backlog ${data.activeBacklog.toLocaleString()} · rail off`
                }
              />
              <CeilingLed
                label="Memory"
                active={data.ceilingMemory}
                help={CEILING_HELP.memory}
                detail={
                  data.admissionCreateQueueLimit > 0
                    ? `create queue ${data.pendingCreateQueue.toLocaleString()} / ${data.admissionCreateQueueLimit.toLocaleString()}`
                    : `create queue ${data.pendingCreateQueue.toLocaleString()} · rail off`
                }
              />
              <CeilingLed
                label="Exporter"
                active={data.ceilingExporter}
                help={CEILING_HELP.exporter}
                detail={`export queue fill ${(data.exporterFillPermille / 10).toFixed(1)}%${
                  data.ceilingExporter ? " · shedding intake" : ""
                }`}
              />
              <CeilingLed
                label="Flow control"
                active={data.ceilingFlowControl}
                help={CEILING_HELP.flowControl}
                detail={
                  data.ceilingFlowControl
                    ? "back-pressuring producers"
                    : "producer credit clear"
                }
              />
              <Stat
                label="Create queue"
                value={data.pendingCreateQueue.toLocaleString()}
                sub={
                  data.admissionCreateQueueLimit > 0
                    ? `shed at ${data.admissionCreateQueueLimit.toLocaleString()}`
                    : "unbounded"
                }
              />
              <Stat
                label="Admissions shed"
                value={data.admissionShedTotal.toLocaleString()}
                sub={
                  data.admissionShedTotal > 0 ? "load being shed" : undefined
                }
              />
            </div>
          </section>

          {/* Throughput charts */}
          <section className="grid gap-4 lg:grid-cols-2">
            <Chart
              title="Process starts / s"
              color="var(--nano-ok)"
              values={samples.map((s) => s.startsPerSec)}
            />
            <Chart
              title="Jobs completed / s"
              color="var(--nano-info)"
              values={samples.map((s) => s.jobsPerSec)}
            />
            <Chart
              title="Active processes"
              color="var(--nano-accent-strong)"
              values={samples.map((s) => s.active)}
            />
            <Chart
              title="Commit pipeline depth (in-flight)"
              color="var(--nano-warn)"
              values={samples.map((s) => s.inflight)}
            />
          </section>

          {/* Totals & durability */}
          <section>
            <SectionLabel>Totals</SectionLabel>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
              <Stat
                label="Instances created"
                value={data.createsTotal.toLocaleString()}
                sub={`rest ${data.createsRest.toLocaleString()} · stream ${data.createsStream.toLocaleString()}`}
              />
              <Stat
                label="Jobs completed"
                value={data.completionsTotal.toLocaleString()}
                sub={`rest ${data.completionsRest.toLocaleString()} · stream ${data.completionsStream.toLocaleString()}`}
              />
              <Stat
                label="Journal written"
                value={fmtBytes(data.bytesTotal)}
                sub={`${data.writesTotal.toLocaleString()} writes`}
              />
              <Stat
                label="Group commits"
                value={data.commitsTotal.toLocaleString()}
              />
              {data.residentBytes != null && (
                <Stat
                  label="Memory (resident)"
                  value={fmtBytes(data.residentBytes)}
                />
              )}
            </div>
          </section>

          {/* Cluster breakdown (only when this is a multi-node cluster) */}
          {isCluster && cluster && (
            <section>
              <div className="mb-1 flex items-baseline justify-between">
                <SectionLabel>Cluster</SectionLabel>
                <span className="text-xs text-fg-faint">
                  {cluster.aggregate.reachableNodes}/
                  {cluster.aggregate.totalNodes} nodes up
                  {(() => {
                    const recovering = cluster.nodes.filter(
                      (n) => n.metrics?.recovery?.recovering,
                    ).length;
                    return recovering > 0 ? (
                      <span className="text-warn">
                        {" "}
                        · {recovering} catching up
                      </span>
                    ) : null;
                  })()}
                </span>
              </div>
              <div className="mb-4 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
                <Stat
                  label="Cluster starts/s"
                  value={fmt(clusterRates?.starts ?? 0, 0)}
                  accent="emerald"
                />
                <Stat
                  label="Cluster jobs/s"
                  value={fmt(clusterRates?.jobs ?? 0, 0)}
                  accent="sky"
                />
                <Stat
                  label="Active (cluster)"
                  value={cluster.aggregate.activeInstances.toLocaleString()}
                />
                <Stat
                  label="Clients (cluster)"
                  value={cluster.aggregate.connectionsActive.toLocaleString()}
                />
                <Stat
                  label="Memory (cluster)"
                  value={fmtBytes(cluster.aggregate.residentBytes)}
                />
              </div>
              <div className="overflow-x-auto rounded-lg border border-edge">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-edge text-left text-fg-faint">
                      <th className="px-3 py-2 font-medium">Node</th>
                      <th className="px-3 py-2 font-medium">Status</th>
                      <th className="px-3 py-2 font-medium">SLA</th>
                      <th className="px-3 py-2 text-right font-medium">
                        Active
                      </th>
                      <th className="px-3 py-2 text-right font-medium">
                        Created
                      </th>
                      <th className="px-3 py-2 text-right font-medium">
                        Completed
                      </th>
                      <th className="px-3 py-2 text-right font-medium">
                        Clients
                      </th>
                      <th className="px-3 py-2 text-right font-medium">
                        In-flight
                      </th>
                      <th className="px-3 py-2 text-right font-medium">
                        Memory
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {cluster.nodes.map((n) => {
                      const href = n.isSelf
                        ? null
                        : nodeConsoleUrl(n.address, "/metrics");
                      return (
                        <tr
                          key={n.nodeId}
                          className="border-b border-edge last:border-0"
                        >
                          <td className="px-3 py-2">
                            <div className="flex items-center gap-2">
                              {href ? (
                                <a
                                  href={href}
                                  target="_blank"
                                  rel="noreferrer"
                                  title={`Open node ${n.nodeId} console (${n.address})`}
                                  className="font-medium text-info hover:underline"
                                >
                                  node {n.nodeId} ↗
                                </a>
                              ) : (
                                <span className="font-medium">
                                  node {n.nodeId}
                                </span>
                              )}
                              {n.isSelf && <Badge tone="ok">this</Badge>}
                            </div>
                            {n.address && (
                              <div className="mt-0.5 text-xs text-fg-faint">
                                {n.address}
                              </div>
                            )}
                          </td>
                          <td className="px-3 py-2">
                            {n.reachable ? (
                              n.metrics?.recovery?.recovering ? (
                                <span
                                  className="text-warn"
                                  title={
                                    n.metrics.recovery.detail ||
                                    "catching up after restart"
                                  }
                                >
                                  ● up · catching up
                                </span>
                              ) : (n.metrics?.recovery?.handingOff ?? 0) > 0 ? (
                                <span
                                  className="text-info"
                                  title={
                                    n.metrics?.recovery?.detail ||
                                    "handing leadership back to a recovering owner"
                                  }
                                >
                                  ● up · handing back
                                </span>
                              ) : (
                                <span className="text-ok">● up</span>
                              )
                            ) : (
                              <span
                                className="text-danger"
                                title={n.error ?? ""}
                              >
                                ● {n.error ?? "down"}
                              </span>
                            )}
                          </td>
                          <td className="px-3 py-2">
                            {n.metrics ? (
                              <SlaBadge mode={n.metrics.slaMode} />
                            ) : (
                              <span className="text-fg-faint">—</span>
                            )}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {n.metrics?.activeInstances.toLocaleString() ?? "—"}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {n.metrics?.createsTotal.toLocaleString() ?? "—"}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {n.metrics?.completionsTotal.toLocaleString() ??
                              "—"}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {n.metrics?.connectionsActive.toLocaleString() ??
                              "—"}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {n.metrics?.commitInflight.toLocaleString() ?? "—"}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {n.metrics?.residentBytes != null
                              ? fmtBytes(n.metrics.residentBytes)
                              : "—"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          <section>
            <SectionLabel>Durability &amp; latency (means)</SectionLabel>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
              <Stat
                label="fsync mean"
                value={`${fmt(data.fsyncMeanMs, 2)} ms`}
              />
              <Stat
                label="Commit wait mean"
                value={`${fmt(data.commitWaitMeanMs, 2)} ms`}
              />
              <Stat
                label="Commit batch mean"
                value={fmt(data.commitBatchMean, 1)}
              />
              <Stat
                label="Frame proc mean"
                value={`${fmt(data.frameProcessingMeanMs, 3)} ms`}
              />
              <Stat
                label="Writer busy"
                value={`${fmt(data.writerBusyRatio * 100, 1)} %`}
                sub={
                  data.writerBusyRatio > 0.9 ? "writer saturated" : undefined
                }
              />
              <Stat
                label="Credit stalls"
                value={data.creditStallsTotal.toLocaleString()}
              />
            </div>
          </section>
        </div>
      )}
    </div>
  );
}

/// A capacity-ceiling "clipping" LED, styled like a mixing-desk gain-reduction
/// indicator: a green dot while there is headroom, a pulsing red dot + "CLIPPING"
/// while the node is pressed against the limit. `detail` shows the live pressure
/// vs. its shed threshold; `help` (if given) surfaces a hover explanation of what
/// the ceiling measures and how it behaves across SLA modes.
function CeilingLed({
  label,
  active,
  detail,
  help,
}: {
  label: string;
  active: boolean;
  detail: string;
  help?: string;
}) {
  return (
    <Card className="px-4 py-3">
      <div className="flex items-center text-xs uppercase tracking-wide text-fg-faint">
        {label} ceiling
        {help && <InfoPopover text={help} />}
      </div>
      <div className="mt-1 flex items-center gap-2">
        <span
          className={`inline-block h-3 w-3 rounded-full ${
            active ? "bg-danger animate-pulse" : "bg-ok"
          }`}
        />
        <span
          className={`text-lg font-semibold ${active ? "text-danger" : "text-ok"}`}
        >
          {active ? "CLIPPING" : "clear"}
        </span>
      </div>
      <div className="mt-0.5 text-xs text-fg-faint tabular-nums">{detail}</div>
    </Card>
  );
}

/// A small "i" info affordance that toggles a click-triggered popover with
/// `text`. The popover closes on outside-click or Escape and supports multi-line
/// text via "\n". Click (not hover) so the explanation stays put while reading.
function InfoPopover({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDocClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node))
        setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <span ref={ref} className="relative ml-1 inline-flex">
      <button
        type="button"
        aria-label={text}
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        className={`inline-flex h-3.5 w-3.5 cursor-pointer items-center justify-center rounded-full border font-serif text-[9px] font-bold italic normal-case leading-none outline-none focus-visible:ring-2 focus-visible:ring-accent/60 ${
          open
            ? "border-fg-muted text-fg"
            : "border-edge text-fg-faint hover:border-fg-muted hover:text-fg"
        }`}
      >
        i
      </button>
      {open && (
        <div
          role="tooltip"
          className="absolute left-0 top-5 z-50 w-72 rounded-lg border border-edge bg-raised p-3 text-left text-[11px] font-normal normal-case leading-snug tracking-normal text-fg shadow-lg"
        >
          {text.split("\n").map((line, i) => (
            <p key={i} className={i === 0 ? "" : "mt-1.5"}>
              {line}
            </p>
          ))}
        </div>
      )}
    </span>
  );
}

/// The current per-node SLA mode as a coloured badge with a hover explanation of
/// both modes. `latency` (preserve latency) is the calm/info state; `admission`
/// (accept latency to keep admitting) is flagged amber since it lets the backlog
/// and end-to-end latency grow.
function SlaBadge({ mode }: { mode: "latency" | "admission" }) {
  const admission = mode === "admission";
  return (
    <Badge tone={admission ? "warn" : "info"} className="normal-case">
      SLA: {admission ? "admission" : "latency"}
      <InfoPopover text={SLA_HELP} />
    </Badge>
  );
}

const SLA_HELP =
  "SLA mode — configurable per node, switchable at runtime. Governs how the capacity ceilings above behave.\n" +
  "• Latency: preserve end-to-end latency. At the ceiling, shed new createProcessInstance calls (503) so accepted instances keep completing fast.\n" +
  "• Admission: preserve admission (accept latency). Drop the proactive active-backlog governor and run at the true drain ceiling, letting latency and backlog grow to the memory-safety rails.";

const CEILING_HELP = {
  throughput:
    "Throughput ceiling — create-processing concurrency (AIMD limiter) and/or the active-backlog governor are at their limit.\n" +
    "• Latency mode: both the AIMD limiter and the proactive backlog governor shed new creates (503) to hold end-to-end latency.\n" +
    "• Admission mode: the backlog governor is off, so this reflects only the AIMD engine-overload guard — latency and backlog are allowed to grow.",
  memory:
    "Memory ceiling — an always-on memory-safety rail is at its limit: submitted create-queue depth, in-flight pipeline bytes, or the resident-memory watermark.\n" +
    "Identical in both SLA modes — these survival rails prevent OOM regardless of policy, and are the backlog backstop in Admission mode.",
  exporter:
    "Exporter ceiling — the read-model export queue has crossed the Tier-1 knee, so a graded fraction of create intake is being shed to bound export lag: exporter backpressure is compressing throughput.\n" +
    "Same in both SLA modes (memory protection). In Admission mode it is typically the main create throttle, since the latency governor is disabled.",
  flowControl:
    "Flow control ceiling — create-submission credit is being back-pressured to the Falcon/REST producer clients right now.\n" +
    "• Both modes: the completion-paced drain servo meters credit grants, or the drain-stall valve hard-blocks creates (a liveness rail).\n" +
    "• Latency mode only: also engages on create-latency submission pressure from the AIMD limiter.",
};

function fmt(n: number, digits: number): string {
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

const ACCENTS: Record<string, string> = {
  emerald: "text-ok",
  sky: "text-info",
};

function Stat({
  label,
  value,
  sub,
  accent,
}: {
  label: string;
  value: React.ReactNode;
  sub?: string;
  accent?: string;
}) {
  return (
    <Card className="px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-fg-faint">
        {label}
      </div>
      <div
        className={`mt-1 text-2xl font-semibold tabular-nums ${accent ? ACCENTS[accent] : ""}`}
      >
        {value}
      </div>
      {sub && <div className="mt-0.5 text-xs text-fg-faint">{sub}</div>}
    </Card>
  );
}

/// A self-contained inline-SVG sparkline (no chart library — keeps the bundle
/// lean and the distribution fully offline). Scales the series to its own
/// min/max and draws a filled area + line. Hovering the chart shows a
/// crosshair + tooltip on the nearest sample (samples are 1s apart — see
/// `LOCAL_INTERVAL_MS` in metricsStore).
function Chart({
  title,
  values,
  color,
}: {
  title: string;
  values: number[];
  color: string;
}) {
  const W = 600;
  const H = 120;
  const pad = 4;
  const last = values[values.length - 1] ?? 0;
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const hasSeries = values.length >= 2;
  const max = hasSeries ? Math.max(...values, 1) : 1;
  const min = hasSeries ? Math.min(...values, 0) : 0;
  const span = max - min || 1;
  const n = values.length;
  const xOf = (i: number) => pad + (i / Math.max(n - 1, 1)) * (W - 2 * pad);
  const yOf = (v: number) => H - pad - ((v - min) / span) * (H - 2 * pad);

  let body: React.ReactNode = (
    <text
      x={W / 2}
      y={H / 2}
      fill="var(--nano-text-faint)"
      fontSize="13"
      textAnchor="middle"
    >
      collecting…
    </text>
  );

  if (hasSeries) {
    const line = values.map((v, i) => `${xOf(i)},${yOf(v)}`).join(" ");
    const area = `${pad},${H - pad} ${line} ${W - pad},${H - pad}`;
    body = (
      <>
        <polygon points={area} fill={color} opacity={0.12} />
        <polyline
          points={line}
          fill="none"
          stroke={color}
          strokeWidth={1.5}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
        {hoverIdx != null && hoverIdx >= 0 && hoverIdx < n && (
          <>
            <line
              x1={xOf(hoverIdx)}
              x2={xOf(hoverIdx)}
              y1={pad}
              y2={H - pad}
              stroke="var(--nano-text-faint)"
              strokeWidth={0.75}
              strokeDasharray="3 3"
              opacity={0.7}
            />
            <circle
              cx={xOf(hoverIdx)}
              cy={yOf(values[hoverIdx])}
              r={3}
              fill={color}
              stroke="var(--nano-app)"
              strokeWidth={1.5}
            />
          </>
        )}
      </>
    );
  }

  const onMove = (e: React.PointerEvent<SVGSVGElement>) => {
    if (!hasSeries) return;
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const px = e.clientX - rect.left;
    // Map from client pixels back to viewBox space (SVG is set to
    // preserveAspectRatio="none" so it stretches horizontally).
    const vx = (px / rect.width) * W;
    const t = ((vx - pad) / (W - 2 * pad)) * (n - 1);
    const idx = Math.max(0, Math.min(n - 1, Math.round(t)));
    setHoverIdx(idx);
  };

  const secondsAgo = hoverIdx != null && hasSeries ? n - 1 - hoverIdx : null;
  const hoverValue = hoverIdx != null ? values[hoverIdx] : null;

  // Tooltip left offset in %, clamped so it stays inside the plot at the edges.
  const tipLeftPct =
    hoverIdx != null && hasSeries
      ? Math.max(4, Math.min(96, (xOf(hoverIdx) / W) * 100))
      : 0;

  return (
    <Card className="p-4">
      <div className="mb-2 flex items-baseline justify-between">
        <span className="text-sm text-fg-muted">{title}</span>
        <span className="text-sm font-semibold tabular-nums" style={{ color }}>
          {fmt(hoverValue ?? last, (hoverValue ?? last) < 10 ? 1 : 0)}
        </span>
      </div>
      <div className="relative">
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          className="h-28 w-full"
          onPointerMove={onMove}
          onPointerLeave={() => setHoverIdx(null)}
        >
          {body}
        </svg>
        {hoverIdx != null && hoverValue != null && (
          <div
            className="pointer-events-none absolute -top-1 z-10 -translate-x-1/2 -translate-y-full rounded-md border border-edge bg-raised px-2 py-1 text-xs shadow-md"
            style={{ left: `${tipLeftPct}%` }}
          >
            <div className="font-semibold tabular-nums" style={{ color }}>
              {fmt(hoverValue, hoverValue < 10 ? 2 : 0)}
            </div>
            <div className="text-fg-faint tabular-nums">
              {secondsAgo === 0 ? "now" : `${secondsAgo}s ago`}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
