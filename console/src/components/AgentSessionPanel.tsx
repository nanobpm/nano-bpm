import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type {
  AgentInstanceMessageContent,
  AgentInstanceResult,
} from "../gen-c8/types.gen";
import {
  chapterAt,
  chapters as chaptersOf,
  initialPlayhead,
  isAgentActive,
  onHistoryChanged,
  pctIn,
  scrub,
  scrubActionForKey,
  seek,
  spans as spansOf,
  timeWindow,
  totals as totalsOf,
  type HistoryItem,
  type Playhead,
  type ScrubAction,
  type Span,
} from "../lib/agentHistory.ts";
import { useAgentHistory } from "../lib/useAgentSessions.ts";
import { Badge, Button, Spinner } from "./ui.tsx";

/// The agent-session scrubber (nano-bpm#1314): one AgentInstance's committed
/// turn log from the engine, with a scrub bar chaptered by loop iteration, a
/// model/tool span timeline, the transcript up to the playhead and running
/// totals. "Live at turn": the history query is refetched on every SSE edge
/// (InstanceDetail's useLiveInvalidation) and a playhead parked at the end
/// follows new turns. All state logic is in lib/agentHistory.ts.
export default function AgentSessionPanel({
  elementId,
  instances,
  onClose,
}: {
  elementId: string;
  /// The element's agent runs, oldest first (agentsByElement). Never empty.
  instances: AgentInstanceResult[];
  onClose: () => void;
}) {
  const [selectedKey, setSelectedKey] = useState(
    () => instances[instances.length - 1].agentInstanceKey,
  );
  const instance =
    instances.find((i) => i.agentInstanceKey === selectedKey) ??
    instances[instances.length - 1];
  const active = isAgentActive(instance.status);
  const {
    data: history,
    isLoading,
    error,
  } = useAgentHistory(instance.agentInstanceKey);
  const items = useMemo(() => history ?? [], [history]);

  return (
    <aside
      aria-label={`Agent session on ${elementId}`}
      className="flex h-full min-h-0 flex-col border-l border-edge-strong bg-raised"
    >
      <header className="flex shrink-0 items-center gap-2 border-b border-edge px-4 py-2">
        <h2 className="min-w-0 flex-1 truncate text-sm font-semibold text-fg">
          ✦ {elementId}
        </h2>
        {active ? (
          <Badge tone="accent">
            <span className="h-1.5 w-1.5 rounded-full bg-accent motion-safe:animate-pulse" />
            live · {instance.status.toLowerCase().replace("_", " ")}
          </Badge>
        ) : (
          <Badge>{instance.status.toLowerCase()}</Badge>
        )}
        {instances.length > 1 && (
          <select
            aria-label="Agent run"
            className="rounded border border-edge bg-surface px-1.5 py-0.5 text-xs text-fg"
            value={instance.agentInstanceKey}
            onChange={(e) => setSelectedKey(e.target.value)}
          >
            {instances.map((inst, n) => (
              <option key={inst.agentInstanceKey} value={inst.agentInstanceKey}>
                Run {n + 1} · {inst.status.toLowerCase()}
              </option>
            ))}
          </select>
        )}
        <button
          type="button"
          className="-mr-2 rounded p-1.5 text-fg-muted hover:bg-hover hover:text-fg"
          aria-label="Close agent session"
          onClick={onClose}
        >
          ✕
        </button>
      </header>
      {error ? (
        <p className="p-4 text-sm text-danger">
          Could not load the agent history: {String(error)}
        </p>
      ) : isLoading && items.length === 0 ? (
        <p className="flex items-center gap-2 p-4 text-sm text-fg-muted">
          <Spinner /> Loading the agent history…
        </p>
      ) : items.length === 0 ? (
        <p className="p-4 text-sm text-fg-muted">
          {active
            ? "No committed turns yet. They appear here as the agent commits them."
            : "This agent run recorded no history."}
        </p>
      ) : (
        <Scrubber
          // A different run is a different log: reset the playhead.
          key={instance.agentInstanceKey}
          items={items}
          active={active}
        />
      )}
    </aside>
  );
}

function Scrubber({
  items,
  active,
}: {
  items: HistoryItem[];
  active: boolean;
}) {
  const chapters = useMemo(() => chaptersOf(items), [items]);
  const spans = useMemo(() => spansOf(items), [items]);
  const [head, setHead] = useState<Playhead>(() =>
    initialPlayhead(items.length),
  );

  // Live: reconcile with each refetched log (follow the end, or stay parked).
  const seenCount = useRef(items.length);
  useEffect(() => {
    if (seenCount.current === items.length) return;
    seenCount.current = items.length;
    setHead((h) => onHistoryChanged(h, items.length));
  }, [items.length]);

  // Open tool spans grow with the clock while the agent is active.
  const [now, setNow] = useState(() => Date.now());
  const hasOpen = active && spans.some((s) => s.endMs === null);
  useEffect(() => {
    if (!hasOpen) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [hasOpen]);

  const act = (a: ScrubAction) =>
    setHead((h) => scrub(h, a, chapters, items.length));
  const onKeyDown = (e: React.KeyboardEvent) => {
    const a = scrubActionForKey(e);
    if (!a) return;
    e.preventDefault();
    act(a);
  };

  const current = items[head.index];
  const chapter = chapterAt(chapters, head.index);
  const totals = totalsOf(items, head.index);
  const window = timeWindow(items, spans, active, now);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {/* Scrub bar: chapter segments + playhead, keyboard driven. */}
      <div className="shrink-0 space-y-2 border-b border-edge px-4 py-3">
        <div
          role="slider"
          tabIndex={0}
          aria-label="Agent history playhead"
          aria-valuemin={1}
          aria-valuemax={items.length}
          aria-valuenow={head.index + 1}
          aria-valuetext={`Step ${head.index + 1} of ${items.length}, iteration ${chapter?.loopIteration ?? "-"}`}
          onKeyDown={onKeyDown}
          onPointerDown={(e) => {
            // Capture the element now: React clears `currentTarget` after the
            // handler returns, so the async `pointermove` below cannot read it.
            const el = e.currentTarget;
            const seekTo = (clientX: number) => {
              const r = el.getBoundingClientRect();
              const f = (clientX - r.left) / r.width;
              setHead(seek(Math.floor(f * items.length), items.length));
            };
            el.setPointerCapture(e.pointerId);
            seekTo(e.clientX);
            const move = (ev: PointerEvent) => seekTo(ev.clientX);
            const up = () => {
              el.removeEventListener("pointermove", move);
              el.removeEventListener("pointerup", up);
            };
            el.addEventListener("pointermove", move);
            el.addEventListener("pointerup", up);
          }}
          className="relative h-6 cursor-pointer rounded outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
        >
          {chapters.map((c) => (
            <div
              key={c.loopIteration}
              title={`Iteration ${c.loopIteration}`}
              className={`absolute inset-y-1 rounded-sm border-r-2 border-raised ${
                c === chapter ? "bg-accent/40" : "bg-hover"
              }`}
              style={{ left: `${c.leftPct}%`, width: `${c.widthPct}%` }}
            />
          ))}
          <div
            aria-hidden
            className="absolute inset-y-0 w-0.5 bg-accent"
            style={{
              left: `${((head.index + 0.5) / items.length) * 100}%`,
            }}
          />
        </div>
        <div className="flex items-center gap-1">
          <Button
            size="sm"
            variant="ghost"
            aria-label="Previous iteration"
            title="Previous iteration (Shift+←)"
            onClick={() => act("previousChapter")}
          >
            ⏮
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label="Previous step"
            title="Previous step (←)"
            onClick={() => act("stepBack")}
          >
            ◀
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label="Next step"
            title="Next step (→)"
            onClick={() => act("stepForward")}
          >
            ▶
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label="Next iteration"
            title="Next iteration (Shift+→)"
            onClick={() => act("nextChapter")}
          >
            ⏭
          </Button>
          <span className="ml-2 text-xs text-fg-muted tabular-nums">
            step {head.index + 1}/{items.length} · iteration{" "}
            {chapter?.loopIteration}
          </span>
          {active && (
            <Button
              size="sm"
              variant={head.follow ? "secondary" : "ghost"}
              className="ml-auto"
              aria-pressed={head.follow}
              onClick={() => act("end")}
              title="Follow new turns (End)"
            >
              {head.follow ? "● following" : "Jump to live"}
            </Button>
          )}
        </div>
      </div>

      {/* Span timeline. */}
      {window && spans.length > 0 && (
        <Timeline
          spans={spans}
          window={window}
          now={now}
          playheadMs={Date.parse(current.producedAt)}
          currentStep={head.index}
          onSeek={(i) => setHead(seek(i, items.length))}
        />
      )}

      <Totals totals={totals} />

      {/* Transcript up to the playhead; the current step is highlighted. */}
      <ol className="min-h-0 flex-1 space-y-2 overflow-auto px-4 py-3">
        {items.slice(0, head.index + 1).map((item, i) => (
          <TranscriptItem
            key={item.historyItemKey}
            item={item}
            current={i === head.index}
            onSelect={() => setHead(seek(i, items.length))}
          />
        ))}
      </ol>
    </div>
  );
}

const LANE_PX = 14;

function Timeline({
  spans,
  window,
  now,
  playheadMs,
  currentStep,
  onSeek,
}: {
  spans: Span[];
  window: { startMs: number; endMs: number };
  now: number;
  playheadMs: number;
  currentStep: number;
  onSeek: (step: number) => void;
}) {
  const kinds = (["model", "tool"] as const).map((kind) => {
    const of = spans.filter((s) => s.kind === kind);
    return {
      kind,
      spans: of,
      lanes: Math.max(1, ...of.map((s) => s.lane + 1)),
    };
  });
  return (
    <div className="shrink-0 space-y-1 border-b border-edge px-4 py-2">
      {kinds.map(({ kind, spans: of, lanes }) => (
        <div key={kind} className="flex items-start gap-2">
          <span className="w-10 shrink-0 pt-px text-[10px] uppercase text-fg-faint">
            {kind}
          </span>
          <div className="relative flex-1" style={{ height: lanes * LANE_PX }}>
            {of.map((s) => {
              const left = pctIn(window, s.startMs);
              const right = pctIn(window, s.endMs ?? now);
              return (
                <button
                  key={s.id}
                  type="button"
                  title={`${s.label} · iteration ${s.loopIteration}${
                    s.endMs === null
                      ? " · running"
                      : ` · ${fmtMs(s.endMs - s.startMs)}`
                  }`}
                  onClick={() => onSeek(s.stepIndex)}
                  className={`absolute h-[10px] min-w-[3px] rounded-sm ${
                    kind === "model" ? "bg-accent/70" : "bg-info/70"
                  } ${s.stepIndex === currentStep ? "ring-2 ring-fg/40" : ""} ${
                    s.endMs === null ? "motion-safe:animate-pulse" : ""
                  }`}
                  style={{
                    left: `${left}%`,
                    width: `${Math.max(0, right - left)}%`,
                    top: s.lane * LANE_PX,
                  }}
                />
              );
            })}
            <div
              aria-hidden
              className="absolute inset-y-0 w-px bg-fg/50"
              style={{ left: `${pctIn(window, playheadMs)}%` }}
            />
          </div>
        </div>
      ))}
      <div className="flex justify-between pl-12 text-[10px] text-fg-faint tabular-nums">
        <span>0s</span>
        <span>{fmtMs(window.endMs - window.startMs)}</span>
      </div>
    </div>
  );
}

function Totals({ totals }: { totals: ReturnType<typeof totalsOf> }) {
  const cells: [string, string][] = [
    ["model calls", String(totals.modelCalls)],
    ["tool calls", String(totals.toolCalls)],
    ["tokens in", totals.inputTokens.toLocaleString()],
    ["tokens out", totals.outputTokens.toLocaleString()],
    ["model time", fmtMs(totals.modelMs)],
  ];
  return (
    <dl className="flex shrink-0 flex-wrap gap-x-4 gap-y-1 border-b border-edge px-4 py-2 text-xs">
      {cells.map(([k, v]) => (
        <div key={k} className="flex gap-1">
          <dt className="text-fg-faint">{k}</dt>
          <dd className="text-fg tabular-nums">{v}</dd>
        </div>
      ))}
    </dl>
  );
}

const ROLE_STYLE: Record<HistoryItem["role"], string> = {
  USER: "border-edge-strong",
  ASSISTANT: "border-accent/60",
  TOOL_RESULT: "border-info/60",
  CONFIGURATION: "border-edge",
};

function TranscriptItem({
  item,
  current,
  onSelect,
}: {
  item: HistoryItem;
  current: boolean;
  onSelect: () => void;
}) {
  const ref = useRef<HTMLLIElement>(null);
  useEffect(() => {
    if (current) ref.current?.scrollIntoView({ block: "nearest" });
  }, [current]);
  return (
    <li
      ref={ref}
      aria-current={current ? "step" : undefined}
      className={`rounded border-l-4 bg-surface text-sm ${ROLE_STYLE[item.role] ?? "border-edge"} ${
        current ? "ring-2 ring-accent/50" : "opacity-80 hover:opacity-100"
      }`}
    >
      {/* Semantic select affordance: keyboard users get the same "jump to this
          step" interaction pointer users have. The <details> blocks below stay
          outside the button so no interactive controls are nested. */}
      <button
        type="button"
        onClick={onSelect}
        aria-label={`Show step: ${item.role.toLowerCase().replace("_", " ")}, iteration ${item.loopIteration}`}
        className="block w-full cursor-pointer rounded px-3 pt-2 text-left outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
      >
        <span className="mb-1 flex items-center gap-2 text-[11px] text-fg-faint">
          <span className="font-semibold uppercase">
            {item.role.toLowerCase().replace("_", " ")}
          </span>
          <span>iteration {item.loopIteration}</span>
          {item.model && <span>{item.model}</span>}
          {item.metrics?.durationMs != null && (
            <span>{fmtMs(item.metrics.durationMs)}</span>
          )}
          <time className="ml-auto tabular-nums" dateTime={item.producedAt}>
            {new Date(item.producedAt).toLocaleTimeString()}
          </time>
        </span>
      </button>
      <div className="px-3 pb-2">
        {item.role === "CONFIGURATION" && (
          <details className="text-xs text-fg-muted">
            <summary className="cursor-pointer">
              {[item.provider, item.model].filter(Boolean).join(" / ") ||
                "configuration"}{" "}
              · {item.tools.length} {item.tools.length === 1 ? "tool" : "tools"}
            </summary>
            {item.systemPrompt.map((c, i) => (
              <Content key={i} content={c} />
            ))}
            {item.tools.length > 0 && (
              <p className="mt-1">{item.tools.map((t) => t.name).join(", ")}</p>
            )}
          </details>
        )}
        {item.content.map((c, i) => (
          <Content key={i} content={c} />
        ))}
        {item.toolCalls.map((call) => (
          <details key={call.toolCallId} className="mt-1 text-xs">
            <summary className="cursor-pointer text-info">
              → {call.toolName}
            </summary>
            <pre className="mt-1 overflow-auto rounded bg-hover p-2 text-[11px]">
              {JSON.stringify(call.arguments, null, 2)}
            </pre>
          </details>
        ))}
      </div>
    </li>
  );
}

function Content({
  content,
}: {
  content: AgentInstanceMessageContent;
}): ReactNode {
  switch (content.contentType) {
    case "TEXT":
      return (
        <p className="whitespace-pre-wrap break-words text-fg">
          {content.text}
        </p>
      );
    case "OBJECT":
      return (
        <pre className="overflow-auto rounded bg-hover p-2 text-[11px]">
          {JSON.stringify(content.object, null, 2)}
        </pre>
      );
    case "DOCUMENT":
      return (
        <p className="text-xs text-fg-muted">
          📄{" "}
          {content.documentReference.metadata?.fileName ??
            content.documentReference.documentId}
        </p>
      );
    default:
      return null;
  }
}

function fmtMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  // Round to whole seconds first so a sub-minute remainder can't render an
  // impossible value like `1m60s` (e.g. 119.9s -> `2m0s`).
  const totalSeconds = Math.round(ms / 1000);
  return `${Math.floor(totalSeconds / 60)}m${totalSeconds % 60}s`;
}
