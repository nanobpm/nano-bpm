import {
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type ReactNode,
  type RefObject,
} from "react";
import { createPortal } from "react-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelInstance,
  getInstance,
  getTrace,
  resolveIncident,
  resumeInstance,
  setInstanceVariables,
  suspendInstance,
  type InstanceTrace,
  type Variable,
} from "../gen";
import { fetchProcessXml } from "../lib/api";
import {
  isCancellable,
  isResumable,
  isSuspendable,
  cancelConfirmMessage,
} from "../lib/instanceActions";
import { useLiveInvalidation } from "../lib/useLiveInvalidation";
import { usePaneResize } from "../lib/usePaneResize";
import { ResizeHandle } from "../components/ResizeHandle";
import BpmnViewer from "../components/BpmnViewer";
import AgentSessionPanel from "../components/AgentSessionPanel";
import { agentBadge, agentsByElement } from "../lib/agentHistory";
import {
  AGENT_HISTORY_KEY,
  AGENT_INSTANCES_KEY,
  useAgentInstances,
} from "../lib/useAgentSessions";
import { IncidentReason } from "../components/IncidentReason";
import {
  TraceTimeline,
  fmtClock,
  fmtDuration,
} from "../components/TraceTimeline";
import {
  Badge,
  Button,
  CardGrid,
  Input,
  NavCard,
  SectionLabel,
  inputClass,
  useIsNarrow,
} from "../components/ui";
import {
  VALUE_JSON_ERROR,
  scopeKeyOptions,
  validateNewVariable,
} from "./newVariableForm";
import {
  buildAncestorChain,
  breadcrumbHops,
  activateHop,
} from "./instanceBreadcrumb";
import {
  callActivityElementIds,
  groupCalledInstances,
  resolveCallActivitySelection,
  type CalledInstanceGroup,
} from "./calledInstances";
import { stateTone } from "./stateTone";

export default function InstanceDetail({
  instanceKey,
  onNavigateInstance,
}: {
  instanceKey: string;
  /// Navigate the Explorer's detail pane to another instance. Optional so a
  /// caller that doesn't wire it still compiles and renders (no navigation
  /// offered — e.g. the call-activity breadcrumb hops become inert). Owned by
  /// this slice (#1116); the parent->child "Called Process Instances" slice
  /// (#1117) consumes the same seam.
  onNavigateInstance?: (instanceKey: string) => void;
}) {
  // Detail refetches on the same live signal as the list; the trace is folded
  // from the same event stream, so refresh it on the same signal too. Agent
  // sessions ride the same edge: a committed AgentHistory item advances the
  // exported position, so the scrubber is live at turn granularity (#1314).
  useLiveInvalidation([
    "instance",
    "trace",
    AGENT_INSTANCES_KEY,
    AGENT_HISTORY_KEY,
  ]);
  const qc = useQueryClient();
  const narrow = useIsNarrow();
  // Which drill-down is open full-screen on mobile (Model / Variables / Trace).
  const [panel, setPanel] = useState<null | "model" | "variables" | "trace">(
    null,
  );
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  // Parent->child "Called Process Instances" navigation state. Selecting a
  // multi-instance call-activity cell filters the section to that cell's
  // children ("View all" parity); `calledFilter` holds the calling element id
  // (null = show every called instance). Selecting a call activity that has not
  // spawned a child yet surfaces `noCalledNotice` (the cell's id) as a clear
  // "no called instance" affordance. The section is scrolled into view via
  // `calledSectionRef`.
  const [calledFilter, setCalledFilter] = useState<string | null>(null);
  const [noCalledNotice, setNoCalledNotice] = useState<string | null>(null);
  const calledSectionRef = useRef<HTMLElement>(null);

  // Agent sessions on the diagram (#1314): the engine's AgentInstances for this
  // process instance, grouped by BPMN element, drawn as badges; `agentElement`
  // is the element whose session scrubber is open.
  const { data: agentInstances } = useAgentInstances(instanceKey);
  const agentMap = useMemo(
    () => agentsByElement(agentInstances ?? []),
    [agentInstances],
  );
  const agentBadges = useMemo(
    () =>
      [...agentMap.values()].map((entry) => ({
        elementId: entry.elementId,
        ...agentBadge(entry),
      })),
    [agentMap],
  );
  const [agentElement, setAgentElement] = useState<string | null>(null);

  // Explorer keeps this component mounted (no `key` prop) and simply swaps the
  // `instanceKey`, so the parent->child navigation slice above would otherwise
  // carry over: a filter or "has not called" notice selected on one instance
  // would stay applied to the next. Reset it whenever the inspected instance
  // changes so each instance starts from the unfiltered, notice-free view.
  useEffect(() => {
    setCalledFilter(null);
    setNoCalledNotice(null);
    setAgentElement(null);
  }, [instanceKey]);

  // Resizable, reload-persistent model space. Dragging the divider below the
  // BPMN diagram taller gives the model more room and shrinks the variables/
  // detail area beneath it; the height is stored under `nano.explorer.modelHeight`.
  const modelResize = usePaneResize({
    storageKey: "nano.explorer.modelHeight",
    axis: "y",
    initial: 288, // matches the previous fixed h-72
    min: 144,
    max: () =>
      typeof window === "undefined"
        ? 640
        : Math.max(200, window.innerHeight - 280),
  });

  const { data, isLoading, error } = useQuery({
    queryKey: ["instance", instanceKey],
    queryFn: async () =>
      (await getInstance({ path: { key: instanceKey }, throwOnError: true }))
        .data,
  });

  const defKey = data?.instance.process_definition_key;
  const { data: xml } = useQuery({
    queryKey: ["process-xml", defKey],
    queryFn: () => fetchProcessXml(defKey!),
    enabled: !!defKey,
    staleTime: Infinity,
  });

  // Call-activity child->parent breadcrumb: when this instance was spawned by a
  // call activity (`parent_process_instance_key` is non-null — the snake_case
  // wire field from #1113), climb the parent linkage hop-by-hop to the root and
  // render a clickable path. Each parent's summary comes from the same
  // `["instance", key]` query the detail pane uses, so the climb reuses cache
  // and dedupes. Gated on the current instance having a parent so a top-level
  // instance issues no extra request.
  const hasParent = data?.instance.parent_process_instance_key != null;
  const { data: ancestors } = useQuery({
    queryKey: ["ancestor-chain", instanceKey],
    queryFn: () =>
      buildAncestorChain(instanceKey, async (key) => {
        // Reuse the detail-pane cache: the base `["instance", key]` query has
        // no `staleTime`, so `fetchQuery` would treat every cached entry as
        // stale and refetch each hop (including the currently viewed
        // instance). Read the cache first and only hit the network for hops we
        // haven't loaded yet, so the climb dedupes as intended.
        const cached = qc.getQueryData<
          Awaited<ReturnType<typeof getInstance>>["data"]
        >(["instance", key]);
        const detail =
          cached ??
          (await qc.fetchQuery({
            queryKey: ["instance", key],
            queryFn: async () =>
              (await getInstance({ path: { key }, throwOnError: true })).data,
          }));
        return detail?.instance ?? null;
      }),
    enabled: !!instanceKey && hasParent,
  });
  const crumbs = breadcrumbHops(ancestors ?? []);

  // The BPMN ids of the call-activity cells in this model. `onElementSelect`
  // (from #1114) only reports an element id, not its type, so this set lets us
  // tell a call activity that simply hasn't spawned a child yet apart from an
  // ordinary element (which must never attempt navigation). Parsed once per XML.
  const callActivityIds = useMemo(() => callActivityElementIds(xml), [xml]);

  // The execution trace is folded from a bounded in-memory ring, so it may be
  // absent (never traced, or evicted). Only a 404 means "no trace" — surfaced as
  // an explicit empty state. Any other failure (network, 5xx) is a real error we
  // throw so the section can render an error state instead of hiding it.
  const {
    data: trace,
    isLoading: traceLoading,
    error: traceError,
  } = useQuery({
    queryKey: ["trace", instanceKey],
    queryFn: async () => {
      const result = await getTrace({ path: { key: instanceKey } });
      if (result.response?.status === 404) return undefined;
      if (result.error) throw result.error;
      return result.data;
    },
    enabled: !!instanceKey,
    retry: false,
  });

  // Operator actions refresh both detail queries so the diagram overlay, the
  // incident list, the variables and the Process Trace section all reflect the
  // new engine state right away rather than waiting for the next SSE signal.
  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["instance", instanceKey] });
    qc.invalidateQueries({ queryKey: ["trace", instanceKey] });
  };

  const onResolve = (incidentKey: string) => {
    setBusy(true);
    setActionError(null);
    resolveIncident({
      path: { key: instanceKey, incidentKey },
      throwOnError: true,
    })
      .then(refresh)
      .catch((e) => setActionError(String(e)))
      .finally(() => setBusy(false));
  };

  const onSetVariable = (scopeKey: string, name: string, value: unknown) => {
    setBusy(true);
    setActionError(null);
    return setInstanceVariables({
      path: { key: instanceKey },
      body: { scopeKey, variables: { [name]: value } },
      throwOnError: true,
    })
      .then(refresh)
      .catch((e) => {
        setActionError(String(e));
        throw e;
      })
      .finally(() => setBusy(false));
  };

  // Creating a NEW variable is the same PUT as inline edit, but with
  // `local: true` so the value lands on EXACTLY the chosen scope. Without it the
  // engine's default merge would push a name matching an ancestor upward instead
  // of creating it here (Zeebe local semantics). Mirrors `onSetVariable`'s
  // busy/error/refresh flow so the new row appears via `refresh()`.
  const onCreateVariable = (scopeKey: string, name: string, value: unknown) => {
    setBusy(true);
    setActionError(null);
    return setInstanceVariables({
      path: { key: instanceKey },
      body: { scopeKey, variables: { [name]: value }, local: true },
      throwOnError: true,
    })
      .then(refresh)
      .catch((e) => {
        setActionError(String(e));
        throw e;
      })
      .finally(() => setBusy(false));
  };
  // first because it cannot be undone; refreshes the detail so the state badge
  // flips to Terminated and the overlay clears.
  const onCancelInstance = (processId: string) => {
    if (!window.confirm(cancelConfirmMessage(processId, instanceKey))) return;
    setBusy(true);
    setActionError(null);
    cancelInstance({ path: { key: instanceKey }, throwOnError: true })
      .then(refresh)
      .catch((e) => setActionError(String(e)))
      .finally(() => setBusy(false));
  };

  // Suspend/resume are reversible (unlike cancel), so no confirm prompt: the
  // operator can undo either instantly. Refresh so the state badge and the
  // Suspend/Resume affordance flip to match the new engine state.
  const onSuspendInstance = () => {
    setBusy(true);
    setActionError(null);
    suspendInstance({ path: { key: instanceKey }, throwOnError: true })
      .then(refresh)
      .catch((e) => setActionError(String(e)))
      .finally(() => setBusy(false));
  };

  const onResumeInstance = () => {
    setBusy(true);
    setActionError(null);
    resumeInstance({ path: { key: instanceKey }, throwOnError: true })
      .then(refresh)
      .catch((e) => setActionError(String(e)))
      .finally(() => setBusy(false));
  };

  if (isLoading) return <p className="p-8 text-fg-muted">Loading…</p>;
  if (error)
    return <p className="p-8 text-danger">Failed to load: {String(error)}</p>;
  if (!data) return null;

  const { instance, variables, jobs, incidents, active_elements } = data;
  // The child instances this parent spawned through its call activities (#1115,
  // snake_case wire field). Default to an empty list so an older server payload
  // that predates the field renders as "no called instances" rather than
  // crashing. Grouped by calling cell for the section and the "View all" filter.
  const calledInstances = data.called_instances ?? [];
  const calledGroups = groupCalledInstances(calledInstances);

  // Selecting an element on the diagram (BpmnViewer `onElementSelect`, #1114):
  // resolve it against this parent's called instances, matching Camunda Operate.
  // A single child navigates straight to it; a multi-instance call activity
  // reveals its rows; an un-spawned call activity shows a "no called instance"
  // affordance; a non-call-activity element is ignored (never navigates).
  const onDiagramElementSelect = (elementId: string) => {
    // An element that ran an agent opens its session scrubber.
    if (agentMap.has(elementId)) {
      if (narrow) setPanel(null);
      setAgentElement(elementId);
      return;
    }
    const sel = resolveCallActivitySelection(
      elementId,
      calledInstances,
      callActivityIds,
    );
    if (sel.kind === "ignore") return;
    // On mobile the diagram opens full-screen; close it so the navigation /
    // revealed section / notice is visible underneath.
    if (narrow) setPanel(null);
    switch (sel.kind) {
      case "navigate":
        onNavigateInstance?.(sel.instanceKey);
        break;
      case "reveal":
        setNoCalledNotice(null);
        setCalledFilter(sel.elementId);
        // The section renders this same tick, but React commits the DOM after
        // this handler returns. `queueMicrotask` would run before that commit,
        // so the ref could still be null; `requestAnimationFrame` defers the
        // scroll to the next frame, once the section is in the DOM.
        requestAnimationFrame(() =>
          calledSectionRef.current?.scrollIntoView({
            behavior: "smooth",
            block: "start",
          }),
        );
        break;
      case "none":
        setCalledFilter(null);
        setNoCalledNotice(sel.elementId);
        break;
    }
  };
  // Live token positions drive the overlay. Active element instances cover every
  // wait state — including catch events, timers, receive tasks and event-based
  // gateways that have no job — so an instance parked on one still shows a token.
  // Union with pending-job element ids (belt and suspenders) and dedupe.
  const jobEls = jobs
    .filter((j) => j.state === "Created" || j.state === "Activated")
    .map((j) => j.element_id);
  const activeEls = Array.from(
    new Set([...jobEls, ...active_elements.map((e) => e.element_id)]),
  );
  const incidentEls = incidents
    .filter((i) => i.state === "Active")
    .map((i) => i.element_id);

  const header = (
    <header className="border-b border-edge px-4 py-4 sm:px-8">
      <div className="flex items-center gap-3">
        <h1 className="min-w-0 truncate text-xl font-semibold text-fg">
          {instance.process_id}
        </h1>
        <Badge tone="neutral">{instance.state}</Badge>
        {instance.has_incident && <Badge tone="danger">Incident</Badge>}
        <div className="ml-auto flex shrink-0 items-center gap-2">
          {isSuspendable(instance.state) && (
            <Button
              size="sm"
              variant="secondary"
              disabled={busy}
              onClick={onSuspendInstance}
            >
              Suspend
            </Button>
          )}
          {isResumable(instance.state) && (
            <Button
              size="sm"
              variant="primary"
              disabled={busy}
              onClick={onResumeInstance}
            >
              Resume
            </Button>
          )}
          {isCancellable(instance.state) && (
            <Button
              size="sm"
              variant="danger"
              disabled={busy}
              onClick={() => onCancelInstance(instance.process_id)}
            >
              Cancel instance
            </Button>
          )}
        </div>
      </div>
      <div className="mt-1 font-mono text-xs break-all text-fg-faint">
        instance {instance.key} · definition {instance.process_definition_key} ·
        v{instance.version}
      </div>
    </header>
  );

  // Operate-style call-activity breadcrumb: root → … → current. Rendered only
  // for a child instance (a resolved chain of more than one hop); a top-level
  // instance shows nothing. When navigation is wired, every ancestor hop is a
  // button that navigates the detail pane to that instance via
  // `onNavigateInstance`; without a handler the hops render as plain text (no
  // focusable no-op controls). The current instance is inert (`aria-current`).
  const canNavigate = typeof onNavigateInstance === "function";
  const breadcrumbBar = crumbs.length > 0 && (
    <nav
      aria-label="Call chain"
      className="border-b border-edge px-4 py-2 sm:px-8"
    >
      <ol className="flex flex-wrap items-center gap-x-1 gap-y-1 text-xs">
        {crumbs.map((hop, i) => (
          <li key={hop.key} className="flex items-center gap-x-1">
            {i > 0 && (
              <span aria-hidden className="text-fg-faint">
                ›
              </span>
            )}
            {hop.isCurrent ? (
              <span aria-current="page" className="font-medium text-fg">
                {hop.label}
              </span>
            ) : canNavigate ? (
              <button
                type="button"
                title={`Instance ${hop.key}`}
                onClick={() => activateHop(hop, onNavigateInstance)}
                className="rounded text-accent-strong hover:underline focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
              >
                {hop.label}
              </button>
            ) : (
              <span title={`Instance ${hop.key}`} className="text-fg-muted">
                {hop.label}
              </span>
            )}
          </li>
        ))}
      </ol>
    </nav>
  );

  const actionBanner = actionError && (
    <p className="mb-4 rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-sm text-danger">
      {actionError}
    </p>
  );

  // bg-white is intentional: the BPMN diagram canvas is a physical white
  // "sheet" regardless of theme — and keeping it white is what keeps the
  // fixed-light `.nano-active` / `.nano-incident` overlay colours legible on a
  // phone (they are deliberately not theme-driven).
  const modelBody = (
    <div className="h-full w-full bg-white">
      <BpmnViewer
        xml={xml ?? null}
        activeElementIds={activeEls}
        incidentElementIds={incidentEls}
        onElementSelect={onDiagramElementSelect}
        badges={agentBadges}
        onBadgeClick={onDiagramElementSelect}
        fitOnResize
      />
    </div>
  );

  const variablesBody = (
    <VariablesPanel
      instanceKey={instance.key}
      variables={variables}
      busy={busy}
      onCreate={onCreateVariable}
      onSetVariable={onSetVariable}
    />
  );

  const traceBody = (
    <TraceContent trace={trace} isLoading={traceLoading} error={traceError} />
  );

  const incidentsSection = incidents.length > 0 && (
    <Section title="Incidents">
      <ScrollX>
        <Table head={["Element", "Kind", "State", "Reason", ""]}>
          {incidents.map((i) => (
            <tr key={i.key} className="border-b border-edge">
              <Td>{i.element_id}</Td>
              <Td>{i.kind}</Td>
              <Td>{i.state}</Td>
              <Td className="align-top" title={i.reason}>
                <IncidentReason reason={i.reason} />
              </Td>
              <Td className="text-right">
                {i.state === "Active" && (
                  <Button
                    size="sm"
                    disabled={busy}
                    onClick={() => onResolve(i.key)}
                  >
                    Resolve
                  </Button>
                )}
              </Td>
            </tr>
          ))}
        </Table>
      </ScrollX>
    </Section>
  );

  const jobsSection = (
    <Section title="Jobs">
      {jobs.length === 0 ? (
        <Empty>No jobs.</Empty>
      ) : (
        <ScrollX>
          <Table
            head={[
              "Element",
              "Type",
              "State",
              "Retries",
              "Worker",
              "Job key",
              "Activated",
              "Timeout",
            ]}
          >
            {jobs.map((j) => (
              <tr key={j.key} className="border-b border-edge">
                <Td>{j.element_id}</Td>
                <Td className="font-mono">{j.job_type}</Td>
                <Td>{j.state}</Td>
                <Td>{j.retries}</Td>
                <Td className="text-fg-faint">{j.worker ?? "—"}</Td>
                <Td className="font-mono text-fg-faint">{j.key}</Td>
                <Td className="text-fg-faint">
                  {j.activated_at_ms != null
                    ? fmtClock(j.activated_at_ms)
                    : "—"}
                </Td>
                <Td className="text-fg-faint">
                  {j.timeout_ms != null ? fmtDuration(j.timeout_ms) : "—"}
                </Td>
              </tr>
            ))}
          </Table>
        </ScrollX>
      )}
    </Section>
  );

  // "Called Process Instances": the child instances this parent spawned through
  // its call activities, each row navigating to the child (Operate's Details-tab
  // "Called Process Instance"). Rendered only when there is at least one. The
  // optional `noCalledNotice` banner is shown when the operator selected a call
  // activity that has not spawned a child yet.
  const calledNotice = noCalledNotice != null && (
    <div
      role="status"
      className="mb-6 flex items-center gap-2 rounded-md border border-edge bg-panel px-3 py-2 text-sm text-fg-muted"
    >
      <span>
        The call activity{" "}
        <span className="font-mono text-fg">{noCalledNotice}</span> has not
        called a process instance yet.
      </span>
      <button
        type="button"
        className="ml-auto rounded text-xs text-accent-strong hover:underline focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
        onClick={() => setNoCalledNotice(null)}
      >
        Dismiss
      </button>
    </div>
  );

  const calledInstancesSection = calledInstances.length > 0 && (
    <CalledInstancesSection
      sectionRef={calledSectionRef}
      groups={calledGroups}
      filterElementId={calledFilter}
      onClearFilter={() => setCalledFilter(null)}
      onNavigate={onNavigateInstance}
    />
  );
  // The open agent session (null if none, or if its element no longer has one).
  const agentSession = agentElement ? agentMap.get(agentElement) : undefined;
  const agentSessionPanel = agentSession && (
    <AgentSessionPanel
      key={agentSession.elementId}
      elementId={agentSession.elementId}
      instances={agentSession.instances}
      onClose={() => setAgentElement(null)}
    />
  );

  if (narrow) {
    const panels = {
      model: { title: `${instance.process_id} · Model`, body: modelBody },
      variables: { title: "Variables", body: variablesBody },
      trace: { title: "Process Trace", body: traceBody },
    } as const;
    const openPanel = panel ? panels[panel] : null;

    return (
      <div className="flex h-full flex-col">
        {header}
        {breadcrumbBar}
        <div className="nano-safe-x min-h-0 flex-1 overflow-auto p-4">
          {actionBanner}
          <CardGrid className="mb-6">
            <NavCard
              label="Model"
              description="BPMN diagram"
              onClick={() => setPanel("model")}
            />
            <NavCard
              label="Variables"
              description={`${variables.length} variable${
                variables.length === 1 ? "" : "s"
              }`}
              onClick={() => setPanel("variables")}
            />
            <NavCard
              label="Process Trace"
              description="Execution timeline"
              onClick={() => setPanel("trace")}
            />
          </CardGrid>
          {incidentsSection}
          {calledNotice}
          {calledInstancesSection}
          {jobsSection}
        </div>
        {openPanel && (
          <FullScreenPanel
            title={openPanel.title}
            onClose={() => setPanel(null)}
            bodyClassName={
              panel === "model"
                ? "min-h-0 flex-1 overflow-hidden bg-white"
                : "min-h-0 flex-1 overflow-auto p-4"
            }
          >
            {openPanel.body}
          </FullScreenPanel>
        )}
        {agentSession && (
          <FullScreenPanel
            title={`Agent session · ${agentSession.elementId}`}
            onClose={() => setAgentElement(null)}
            bodyClassName="min-h-0 flex-1 overflow-hidden"
          >
            {agentSessionPanel}
          </FullScreenPanel>
        )}
      </div>
    );
  }

  return (
    <div className="flex h-full">
      <div className="flex min-w-0 flex-1 flex-col">
        {header}
        {breadcrumbBar}

        <div style={{ height: modelResize.size }} className="shrink-0 bg-white">
          <BpmnViewer
            xml={xml ?? null}
            activeElementIds={activeEls}
            incidentElementIds={incidentEls}
            onElementSelect={onDiagramElementSelect}
            badges={agentBadges}
            onBadgeClick={onDiagramElementSelect}
          />
        </div>

        <ResizeHandle
          axis="y"
          label="Resize the model space"
          onPointerDown={modelResize.onPointerDown}
          onKeyDown={modelResize.onKeyDown}
          dragging={modelResize.dragging}
          size={modelResize.size}
          min={modelResize.min}
          max={modelResize.max}
        />

        <div className="min-h-0 flex-1 overflow-auto p-8">
          {actionBanner}
          {incidentsSection}
          {calledNotice}
          {calledInstancesSection}
          <Section title="Variables">{variablesBody}</Section>
          {jobsSection}
          <Section title="Process Trace">{traceBody}</Section>
        </div>
      </div>
      {agentSession && (
        <div className="w-[min(480px,45vw)] shrink-0">{agentSessionPanel}</div>
      )}
    </div>
  );
}

/** A horizontally-scrollable wrapper so a wide table (jobs, variables) scrolls
 * within its own box instead of forcing the whole page to scroll sideways on a
 * phone. Inert on desktop, where the table already fits. */
function ScrollX({ children }: { children: ReactNode }) {
  return <div className="overflow-x-auto">{children}</div>;
}

/** Renders the shared trace timeline for an instance, an explicit empty state
 * when no trace was captured (the trace ring is bounded and evicts), or an error
 * state when the trace fetch failed for a non-404 reason. */
function TraceContent({
  trace,
  isLoading,
  error,
}: {
  trace: InstanceTrace | undefined;
  isLoading: boolean;
  error?: Error | null;
}) {
  if (isLoading) return <p className="text-sm text-fg-muted">Loading…</p>;
  if (error)
    return (
      <Empty>
        Failed to load the trace for this instance. Please try again.
      </Empty>
    );
  if (!trace)
    return (
      <Empty>
        No trace captured for this instance. Traces are held in a bounded
        in-memory ring and are not persisted.
      </Empty>
    );
  return <TraceTimeline trace={trace} />;
}

/** A full-screen overlay for a mobile drill-in (Model / Variables / Trace).
 * Portals to the document body, closes on Escape or the ✕, and clears the notch
 * with the shared safe-area helper. */
function FullScreenPanel({
  title,
  onClose,
  bodyClassName = "min-h-0 flex-1 overflow-auto p-4",
  children,
}: {
  title: ReactNode;
  onClose: () => void;
  bodyClassName?: string;
  children: ReactNode;
}) {
  const titleId = useId();
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  if (typeof document === "undefined") return null;

  return createPortal(
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleId}
      className="fixed inset-0 z-50 flex flex-col bg-app"
    >
      <header className="nano-safe-top nano-safe-x flex shrink-0 items-center gap-2 border-b border-edge px-4 py-3">
        <h2
          id={titleId}
          className="min-w-0 flex-1 truncate text-sm font-semibold text-fg"
        >
          {title}
        </h2>
        <button
          type="button"
          className="nano-touch -mr-2 rounded p-2 text-fg-muted hover:bg-hover hover:text-fg"
          aria-label="Close"
          onClick={onClose}
        >
          ✕
        </button>
      </header>
      <div className={`nano-safe-bottom nano-safe-x ${bodyClassName}`}>
        {children}
      </div>
    </div>,
    document.body,
  );
}

/** The Variables section: an "Add variable" affordance, an optional inline
 * create form, and the existing-variables table (or an empty state that itself
 * offers the add affordance, so a scope with zero variables can still get one).
 * Creating merges through `onCreate` (a `local: true` PUT); editing an existing
 * row goes through `onSetVariable`, exactly as before. */
function VariablesPanel({
  instanceKey,
  variables,
  busy,
  onCreate,
  onSetVariable,
}: {
  instanceKey: string;
  variables: Variable[];
  busy: boolean;
  onCreate: (
    scopeKey: string,
    name: string,
    value: unknown,
  ) => Promise<unknown>;
  onSetVariable: (
    scopeKey: string,
    name: string,
    value: unknown,
  ) => Promise<unknown>;
}) {
  const [adding, setAdding] = useState(false);
  const scopes = scopeKeyOptions(instanceKey, variables);

  const addButton = (
    <Button
      size="sm"
      variant="secondary"
      disabled={busy}
      onClick={() => setAdding(true)}
    >
      Add variable
    </Button>
  );

  const form = adding && (
    <NewVariableForm
      instanceKey={instanceKey}
      scopes={scopes}
      variables={variables}
      busy={busy}
      onCreate={onCreate}
      onDone={() => setAdding(false)}
    />
  );

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-end">
        {!adding && variables.length > 0 && addButton}
      </div>
      {form}
      {variables.length === 0 ? (
        !adding && (
          <div className="flex flex-col items-start gap-2">
            <Empty>No variables yet.</Empty>
            {addButton}
          </div>
        )
      ) : (
        <ScrollX>
          <Table head={["Name", "Value", "Scope", ""]}>
            {variables.map((v) => (
              <VariableRow
                key={`${v.scope_key}:${v.name}`}
                name={v.name}
                value={v.value}
                scopeKey={v.scope_key}
                busy={busy}
                onSave={(parsed) => onSetVariable(v.scope_key, v.name, parsed)}
              />
            ))}
          </Table>
        </ScrollX>
      )}
    </div>
  );
}

/** Inline form to create a NEW variable: name (text), value (JSON, parsed and
 * validated exactly like inline edit) and a scope selector (defaulting to the
 * process-instance scope). Validation — blank name, duplicate-on-scope, invalid
 * JSON — runs client-side before the request; on success the parent's
 * `refresh()` surfaces the new row and the form closes. */
function NewVariableForm({
  instanceKey,
  scopes,
  variables,
  busy,
  onCreate,
  onDone,
}: {
  instanceKey: string;
  scopes: string[];
  variables: Variable[];
  busy: boolean;
  onCreate: (
    scopeKey: string,
    name: string,
    value: unknown,
  ) => Promise<unknown>;
  onDone: () => void;
}) {
  const [name, setName] = useState("");
  const [valueDraft, setValueDraft] = useState("");
  const [scopeKey, setScopeKey] = useState(instanceKey);
  const [error, setError] = useState<string | null>(null);
  const nameId = useId();
  const valueId = useId();
  const scopeId = useId();

  const submit = () => {
    if (busy) return;
    const result = validateNewVariable({
      name,
      valueDraft,
      scopeKey,
      variables,
    });
    if (!result.ok) {
      setError(result.error);
      return;
    }
    setError(null);
    onCreate(result.scopeKey, result.name, result.value)
      .then(onDone)
      .catch(() => {
        /* surfaced by the parent's action error banner */
      });
  };

  return (
    <div className="rounded-md border border-edge bg-inset/40 p-3">
      <div className="grid gap-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-fg-faint" id={nameId}>
            Name
          </span>
          <Input
            aria-labelledby={nameId}
            value={name}
            autoFocus
            placeholder="myVariable"
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") submit();
              if (e.key === "Escape") onDone();
            }}
          />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-fg-faint" id={valueId}>
            Value (JSON)
          </span>
          <Input
            aria-labelledby={valueId}
            className="font-mono text-xs"
            value={valueDraft}
            placeholder='42, true, "text"'
            onChange={(e) => setValueDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") submit();
              if (e.key === "Escape") onDone();
            }}
          />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-fg-faint" id={scopeId}>
            Scope
          </span>
          <select
            aria-labelledby={scopeId}
            className={`${inputClass} font-mono text-xs`}
            value={scopeKey}
            onChange={(e) => setScopeKey(e.target.value)}
          >
            {scopes.map((s) => (
              <option key={s} value={s}>
                {s === instanceKey ? `${s} (instance)` : s}
              </option>
            ))}
          </select>
        </label>
      </div>
      {error && <span className="mt-2 block text-xs text-danger">{error}</span>}
      <p className="mt-2 text-xs text-fg-faint">
        Created on exactly the chosen scope (<code>local: true</code>), so a
        name matching an ancestor is not merged upward.
      </p>
      <div className="mt-3 flex justify-end gap-2">
        <Button size="sm" disabled={busy} onClick={submit}>
          Add
        </Button>
        <Button size="sm" variant="ghost" disabled={busy} onClick={onDone}>
          Cancel
        </Button>
      </div>
    </div>
  );
}

/** One variable row with inline edit. The stored `value` is a serialized-JSON
 * string (e.g. `"text"`, `42`, `true`); the editor is seeded with it and the
 * input is parsed as JSON on save so the engine receives a typed value. */
function VariableRow({
  name,
  value,
  scopeKey,
  busy,
  onSave,
}: {
  name: string;
  value: string;
  scopeKey: string;
  busy: boolean;
  onSave: (parsed: unknown) => Promise<unknown>;
}) {
  const [editing, setEditing] = useState(false);
  // Collapse the row whenever the underlying value changes (switching instances
  // or after a refresh). InstanceDetail stays mounted and VariableRow keys are
  // stable (`scope_key:name`), so a bare boolean would persist across value
  // changes — and deriving `expanded` from the value itself breaks when a value
  // recurs (expand A → switch to B → back to A would auto-expand A). Instead we
  // remember the value we last rendered and reset `expanded` during render when
  // it changes: React's recommended "adjust state when a prop changes" pattern,
  // no effect flash, and correct even when a value recurs.
  const [expanded, setExpanded] = useState(false);
  const [renderedValue, setRenderedValue] = useState(value);
  if (renderedValue !== value) {
    setRenderedValue(value);
    setExpanded(false);
  }
  const [draft, setDraft] = useState(value);
  const [parseError, setParseError] = useState<string | null>(null);

  // A value worth collapsing: multi-line, or too long to sit on one row without
  // blowing out the panel (e.g. an LLM prompt). Short scalars render as-is with
  // no toggle so the common case stays quiet.
  const isLong = value.length > 80 || value.includes("\n");

  const start = () => {
    setDraft(value);
    setParseError(null);
    setEditing(true);
  };
  const cancel = () => {
    setEditing(false);
    setParseError(null);
  };
  const save = () => {
    if (busy) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(draft);
    } catch {
      setParseError(VALUE_JSON_ERROR);
      return;
    }
    onSave(parsed)
      .then(() => setEditing(false))
      .catch(() => {
        /* surfaced by the parent's action error banner */
      });
  };

  if (!editing) {
    return (
      <tr className="border-b border-edge align-top">
        <Td className="font-medium">{name}</Td>
        <Td className="font-mono text-fg-muted">
          <div className="flex max-w-[36rem] items-start gap-1.5">
            {isLong && (
              <button
                type="button"
                aria-label={expanded ? "Collapse value" : "Expand value"}
                aria-expanded={expanded}
                onClick={() => setExpanded((v) => !v)}
                className="mt-px shrink-0 select-none text-fg-faint hover:text-fg"
              >
                {expanded ? "▼" : "▶"}
              </button>
            )}
            {expanded ? (
              <pre className="min-w-0 flex-1 whitespace-pre-wrap break-words">
                {value}
              </pre>
            ) : (
              <span
                className={`min-w-0 flex-1 ${
                  isLong ? "truncate" : "break-words"
                }`}
              >
                {value}
              </span>
            )}
          </div>
        </Td>
        <Td className="font-mono text-fg-faint">{scopeKey}</Td>
        <Td className="text-right">
          <Button size="sm" variant="ghost" disabled={busy} onClick={start}>
            Edit
          </Button>
        </Td>
      </tr>
    );
  }

  return (
    <tr className="border-b border-edge">
      <Td className="font-medium">{name}</Td>
      <Td colSpan={2}>
        <Input
          className="w-full font-mono text-xs"
          value={draft}
          autoFocus
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") save();
            if (e.key === "Escape") cancel();
          }}
        />
        {parseError && (
          <span className="mt-1 block text-xs text-danger">{parseError}</span>
        )}
      </Td>
      <Td className="text-right whitespace-nowrap">
        <Button size="sm" disabled={busy} onClick={save}>
          Save
        </Button>{" "}
        <Button size="sm" variant="ghost" disabled={busy} onClick={cancel}>
          Cancel
        </Button>
      </Td>
    </tr>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="mb-8">
      <SectionLabel>{title}</SectionLabel>
      {children}
    </section>
  );
}

function Table({ head, children }: { head: string[]; children: ReactNode }) {
  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-edge text-left text-fg-faint">
          {head.map((h, idx) => (
            <th key={h || `col-${idx}`} className="py-2 pr-4 font-medium">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}

function Td({
  children,
  className = "",
  colSpan,
  title,
}: {
  children: ReactNode;
  className?: string;
  colSpan?: number;
  title?: string;
}) {
  return (
    <td className={`py-2 pr-4 ${className}`} colSpan={colSpan} title={title}>
      {children}
    </td>
  );
}

function Empty({ children }: { children: ReactNode }) {
  return <p className="text-sm text-fg-faint">{children}</p>;
}

/**
 * The "Called Process Instances" section: the child instances this parent
 * spawned through its call activities, grouped by the calling cell. Each row is
 * keyboard-activatable and navigates to the child via `onNavigate`. When
 * `filterElementId` is set (the operator selected a multi-instance call-activity
 * cell — "View all"), only that cell's rows are shown, with a "Show all" control
 * to clear the filter.
 */
function CalledInstancesSection({
  sectionRef,
  groups,
  filterElementId,
  onClearFilter,
  onNavigate,
}: {
  sectionRef: RefObject<HTMLElement>;
  groups: CalledInstanceGroup[];
  filterElementId: string | null;
  onClearFilter: () => void;
  onNavigate?: (instanceKey: string) => void;
}) {
  const shown =
    filterElementId != null
      ? groups.filter((g) => g.elementId === filterElementId)
      : groups;
  const filterLabel =
    filterElementId != null
      ? (groups.find((g) => g.elementId === filterElementId)?.elementName ??
        filterElementId)
      : null;
  const canNavigate = typeof onNavigate === "function";
  const rows = shown.flatMap((g) => g.instances.map((c) => ({ group: g, c })));

  return (
    <section ref={sectionRef} className="mb-8">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <SectionLabel>Called Process Instances</SectionLabel>
        {filterElementId != null && (
          <div className="flex items-center gap-2 text-xs text-fg-muted">
            <span>
              Showing <span className="font-mono text-fg">{filterLabel}</span>
            </span>
            <Button size="sm" variant="ghost" onClick={onClearFilter}>
              Show all
            </Button>
          </div>
        )}
      </div>
      {rows.length === 0 ? (
        <Empty>No called process instances.</Empty>
      ) : (
        <ScrollX>
          <Table head={["Called from", "Process", "Instance", "State", ""]}>
            {rows.map(({ group, c }) => {
              return (
                <tr
                  key={c.key}
                  className={`border-b border-edge ${
                    canNavigate ? "hover:bg-hover" : ""
                  }`}
                >
                  <Td title={group.elementId ?? undefined}>
                    {group.elementName ?? group.elementId ?? "—"}
                  </Td>
                  <Td>
                    {c.process_id}
                    <span className="text-fg-faint"> · v{c.version}</span>
                  </Td>
                  <Td className="font-mono text-fg-faint">
                    {canNavigate ? (
                      <button
                        type="button"
                        onClick={() => onNavigate?.(c.key)}
                        aria-label={`Open called instance ${c.process_id} ${c.key}`}
                        className="rounded font-mono text-accent-strong hover:underline focus:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent"
                      >
                        {c.key}
                      </button>
                    ) : (
                      c.key
                    )}
                  </Td>
                  <Td>
                    <Badge tone={stateTone(c.state, c.has_incident)}>
                      {c.state}
                    </Badge>
                  </Td>
                  <Td className="text-right">
                    {c.has_incident && <Badge tone="danger">Incident</Badge>}
                  </Td>
                </tr>
              );
            })}
          </Table>
        </ScrollX>
      )}
    </section>
  );
}
