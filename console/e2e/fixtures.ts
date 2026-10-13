// Deterministic console-API stubs for the journey guards (#417).
//
// The suite never talks to a real gateway. That is not just convenience: the
// tour's preconditions are computed from `listProjects()`, so stubbing it is the
// only way to drive a *repair* path on purpose (flip both runtime flags to false
// and assert the journey offers an install hint instead of telling the user to
// press Run). It also means a run needs no engine, no Node toolchain and no
// scaffolded project, which is what keeps this suite fast and non-flaky.
//
// Every stub returns a SHAPED payload, not `{}`. An earlier version of this file
// fulfilled unknown routes with an empty object and the Explorer view crashed on
// `.length` of an undefined array — and because the console has no error boundary,
// one view's crash unmounts the whole SPA, rail and all. The anchor assertions
// then failed with "anchor missing" when the real cause was a bad fixture. So:
// shapes are taken from `src/gen/types.gen.ts`, and `assertNoPageCrash` (below)
// makes a crash report itself instead of masquerading as anchor rot.

import { expect, type Page } from "@playwright/test";
import type {
  AppUi,
  Instance,
  InstanceDetail,
  InstanceTrace,
  ProjectConfig,
  ProjectDetail,
  RunState,
} from "../src/gen";

export interface StubOptions {
  /** Both false ⇒ `hasJsRuntime` resolves to "repair". */
  nodeAvailable?: boolean;
  urbanAvailable?: boolean;
  /** 0 ⇒ `hasTraces` resolves to "skip". */
  traceCount?: number;
  /** Empty ⇒ `hasProject` resolves to "skip", and the picker's empty state shows. */
  projects?: { name: string; lang?: string; running?: boolean }[];
  /** >1 ⇒ `hasCluster` resolves to "ok". */
  nodeCount?: number;
}

const template = (id: string, label: string) => ({
  id,
  label,
  description: `${label} description`,
  lang: "node",
  source: "builtin",
});

/** A zero-valued MetricsSnapshot — every numeric field the Metrics view reads. */
const metricsSnapshot = () => ({
  timestampMs: 0,
  activeInstances: 0,
  createsRest: 0,
  createsStream: 0,
  createsTotal: 0,
  completionsRest: 0,
  completionsStream: 0,
  completionsTotal: 0,
  connectionsActive: 0,
  commitInflight: 0,
  commitsTotal: 0,
  writesTotal: 0,
  bytesTotal: 0,
  creditStallsTotal: 0,
  fsyncMeanMs: 0,
  commitWaitMeanMs: 0,
  commitBatchMean: 0,
  frameProcessingMeanMs: 0,
  writerBusyRatio: 0,
  residentBytes: null,
  ceilingThroughput: false,
  ceilingMemory: false,
  ceilingExporter: false,
  ceilingFlowControl: false,
  exporterFillPermille: 0,
  slaMode: "latency",
  pendingCreateQueue: 0,
  activeBacklog: 0,
  admissionBacklogLimit: 0,
  admissionCreateQueueLimit: 0,
  admissionShedTotal: 0,
  recovery: { state: "idle", startedAtMs: null, completedAtMs: null },
});

/**
 * Route-intercept every console API call with a shaped response.
 *
 * Ordering matters: the most specific patterns are tested first, and the
 * catch-all is last. Anything not listed returns `{}` — acceptable only because
 * `assertNoPageCrash` turns the resulting render failure into a clear error.
 */
export async function stubConsoleApi(
  page: Page,
  options: StubOptions = {},
): Promise<void> {
  const {
    nodeAvailable = true,
    urbanAvailable = true,
    traceCount = 0,
    nodeCount = 1,
    projects = [{ name: "demo", lang: "node" }],
  } = options;

  const projectsBody = {
    projects: projects.map((p) => ({
      name: p.name,
      description: "",
      deployTarget: "http://localhost:8080",
      updatedMs: 0,
      processes: 1,
      decisions: 0,
      forms: 0,
      workers: 0,
      running: p.running ?? false,
      source: "workspace",
      lang: p.lang ?? "node",
    })),
    nodeAvailable,
    urbanAvailable,
    platforms: [],
    templates: [
      template("starter", "Starter app"),
      template("workflow-starter", "Code-first workflow"),
      template("urban-starter", "Urban App"),
    ],
    extensions: { extensions: [], yolo: false },
  };

  // `/traces` returns a bare ARRAY of summaries, not an object — getting this
  // wrong crashed the Traces view rather than showing an empty table.
  const traces = Array.from({ length: traceCount }, (_, i) => ({
    traceId: `t${i}`,
    processDefinitionId: "demo",
    processInstanceKey: `${1000 + i}`,
    startedAtMs: 0,
    durationMs: 1,
    spanCount: 1,
  }));

  await page.route("**/console/api/**", async (route) => {
    const url = new URL(route.request().url()).pathname;
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });

    if (url.endsWith("/console/api/projects")) return json(projectsBody);
    if (url.endsWith("/console/api/instances")) {
      return json({ items: [], total: 0, page: 0, pageSize: 25 });
    }
    if (url.endsWith("/console/api/traces")) return json(traces);
    if (url.endsWith("/console/api/topology")) {
      return json({
        node_id: 0,
        num_nodes: nodeCount,
        num_partitions: 1,
        replication_factor: 1,
        raft_enabled: nodeCount > 1,
        gateway_version: "e2e",
        // The Topology view maps over these; the real endpoint always includes
        // them, so an empty-cluster stub must too or the view crashes on
        // `nodes.map`. (The observe build lands here as its home route.)
        nodes: [],
        partitions: [],
      });
    }
    if (url.endsWith("/console/api/cluster/health")) {
      // Match the generated `ClusterHealth` contract (types.gen.ts): the real
      // endpoint always includes `checkedAtMs`, so the stub must too or a view
      // reading the timestamp diverges from production shape.
      return json({ checkedAtMs: 0, nodes: [] });
    }
    if (url.endsWith("/console/api/metrics")) return json(metricsSnapshot());
    if (url.endsWith("/console/api/cluster/metrics")) {
      return json({
        checkedAtMs: 0,
        nodes: [],
        aggregate: {
          reachableNodes: nodeCount,
          totalNodes: nodeCount,
          activeInstances: 0,
          createsTotal: 0,
          completionsTotal: 0,
          connectionsActive: 0,
        },
      });
    }
    if (url.endsWith("/console/api/workers")) {
      return json({ workers: [], nodeAvailable });
    }
    if (url.endsWith("/console/api/extensions")) {
      return json({ extensions: [], yolo: false });
    }
    return json({});
  });
}

/**
 * Clear persisted journey state, ONCE per test.
 *
 * `addInitScript` runs on every navigation, including `reload()` — clearing
 * unconditionally wiped the state the resume guard exists to verify, so the
 * journey restarted at step one and the test failed for a reason that had nothing
 * to do with the app. A sessionStorage sentinel survives a reload within the same
 * tab, so the reset happens on first load only.
 */
export async function resetTourState(page: Page): Promise<void> {
  await page.addInitScript(() => {
    try {
      if (window.sessionStorage.getItem("e2e.tour.reset")) return;
      window.sessionStorage.setItem("e2e.tour.reset", "1");
      window.localStorage.removeItem("nano.tour.v2");
      window.localStorage.removeItem("nano.tour.v1.seen");
    } catch {
      /* storage disabled — the app tolerates it, so must the test */
    }
  });
}

/**
 * Suppress the startup persona panel (#464/#471).
 *
 * That panel is a `role="dialog" aria-modal="true"` overlay shown on console open,
 * and its "Show at startup" preference lives INSIDE tour state — so
 * `resetTourState` (which deletes the key) restores the default and the panel
 * opens. Every test here that does not start a journey therefore gets a modal on
 * top of the page.
 *
 * The anchor assertions are DOM-presence (`toHaveCount`), so they pass either way
 * — but passing because an overlay happens not to affect `toHaveCount` is luck, not
 * design. Suppressing it explicitly keeps each test about one thing, and keeps this
 * suite from silently becoming a test of the panel's z-index.
 *
 * Safe to call whether or not the panel exists: an unknown field in tour state is
 * ignored, so this is a no-op on a console predating #471.
 */
export async function suppressStartupPanel(page: Page): Promise<void> {
  await page.addInitScript(() => {
    try {
      const raw = window.localStorage.getItem("nano.tour.v2");
      const state = raw ? JSON.parse(raw) : { version: 2, journeys: {} };
      state.showStartupPanel = false;
      window.localStorage.setItem("nano.tour.v2", JSON.stringify(state));
    } catch {
      /* storage disabled — the app tolerates it, so must the test */
    }
  });
}

/**
 * Seed journey state so a journey resumes at a chosen authored step.
 *
 * Walking a journey click-by-click is not viable for the workspace-heavy ones: a
 * step whose anchor is absent costs driver.js up to `waitForElement` (5s) before it
 * is skipped, so reaching step five through three missing anchors takes ~15s and
 * made the repair guard look like a failure to advance. Resuming lands on the step
 * under test directly, which is both faster and a sharper assertion — the claim is
 * "this step renders as its repair", not "a user can click that far".
 */
export async function seedTourState(
  page: Page,
  journeyId: string,
  stepIndex: number,
): Promise<void> {
  await page.addInitScript(
    ({ journeyId, stepIndex }) => {
      try {
        window.localStorage.setItem(
          "nano.tour.v2",
          JSON.stringify({
            version: 2,
            journeys: { [journeyId]: { status: "active", stepIndex } },
          }),
        );
      } catch {
        /* storage disabled */
      }
    },
    { journeyId, stepIndex },
  );
}

/**
 * Fail with the actual error if a view crashed.
 *
 * The console has no error boundary, so an uncaught render error unmounts the
 * whole SPA and every `data-tour` anchor disappears at once. Without this, that
 * presents identically to the breakage this suite is meant to detect.
 */
export function assertNoPageCrash(page: Page): () => void {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  return () =>
    expect(
      errors,
      "a view threw during render, which unmounts the whole SPA — the missing anchor below is a symptom, not the cause",
    ).toEqual([]);
}

// ─────────────────────────────────────────────────────────────────────────
// Mobile-first fixtures (issue #1005, unit A7)
//
// The desktop journey stubs above drive the tour; the mobile guards need a few
// more shaped payloads so a phone-sized run can reach real content — an instance
// list to tap into, an instance detail with a trace and a diagram, and a running
// app to embed. These are LAYERED ON TOP of `stubConsoleApi`: each registers a
// `**/console/api/**` route that handles only its own paths and `route.fallback()`s
// the rest, so the base stubs keep answering metrics/topology/projects. Register
// them AFTER `stubConsoleApi` (Playwright tries the most-recently-added first).
// ─────────────────────────────────────────────────────────────────────────

/**
 * The smallest valid, laid-out BPMN 2.0 document — a start event wired to one
 * task — with the diagram interchange (DI) bpmn-js needs to render shapes. The
 * instance-detail Model card feeds this to `BpmnViewer`; without the `BPMNShape`
 * DI, bpmn-js imports the semantics but paints nothing, so the "diagram rendered"
 * assertion would pass on an empty canvas. The `Task_1` id matches the active /
 * incident element ids the instance factory emits so the token overlay lands.
 */
export const MINIMAL_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
  id="Definitions_1" targetNamespace="http://nano/e2e">
  <bpmn:process id="demo" isExecutable="true">
    <bpmn:startEvent id="Start_1">
      <bpmn:outgoing>Flow_1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:task id="Task_1" name="Do the thing">
      <bpmn:incoming>Flow_1</bpmn:incoming>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_1" sourceRef="Start_1" targetRef="Task_1" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="Diagram_1">
    <bpmndi:BPMNPlane id="Plane_1" bpmnElement="demo">
      <bpmndi:BPMNShape id="Start_1_di" bpmnElement="Start_1">
        <dc:Bounds x="150" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Task_1_di" bpmnElement="Task_1">
        <dc:Bounds x="240" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_1_di" bpmnElement="Flow_1">
        <di:waypoint x="186" y="118" />
        <di:waypoint x="240" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

/** Build a list-shaped `Instance` with sensible defaults; override per test. */
export function makeInstance(overrides: Partial<Instance> = {}): Instance {
  return {
    key: "2251799813685250",
    process_id: "order-fulfilment",
    process_definition_key: "2251799813685249",
    version: 1,
    state: "Active",
    start_date_ms: 0,
    has_incident: false,
    business_id: null,
    tags: [],
    suspendedDate: null,
    parent_process_instance_key: null,
    parent_element_instance_key: null,
    ...overrides,
  };
}

/**
 * A full `InstanceDetail` around a list instance, with one trace-linked task.
 *
 * `overrides` shallow-merges over the derived detail so a scenario can supply its
 * own `called_instances` (the parent->child surface, #1115) or `active_elements`
 * (e.g. a call-activity token) without re-deriving the common shape — the
 * call-activity navigation guard (#1118) uses this to describe a parent whose
 * call activity spawned a child.
 */
export function makeInstanceDetail(
  inst: Instance,
  overrides: Partial<Omit<InstanceDetail, "instance">> = {},
): InstanceDetail {
  return {
    instance: inst,
    variables: [
      { name: "orderId", value: '"A-1001"', scope_key: inst.key },
      { name: "amount", value: "42", scope_key: inst.key },
    ],
    jobs: [],
    incidents: inst.has_incident
      ? [
          {
            key: `${inst.key}-inc`,
            element_id: "Task_1",
            kind: "JOB_NO_RETRIES",
            state: "CREATED",
            reason: "worker failed",
            created_at_ms: 0,
          },
        ]
      : [],
    active_elements: [
      {
        element_id: "Task_1",
        element_type: "TASK",
        element_name: "Do the thing",
      },
    ],
    // No call activities in this fixture's model, so no children were spawned.
    called_instances: [],
    ...overrides,
  };
}

/** A captured `InstanceTrace` (start → Task_1) so the Process Trace card renders
 * the shared `TraceTimeline` instead of the empty state. */
function makeInstanceTrace(inst: Instance): InstanceTrace {
  return {
    instanceKey: inst.key,
    processId: inst.process_id,
    version: inst.version,
    businessId: null,
    tags: [],
    startedAt: 1000,
    endedAt: null,
    durationMs: null,
    outcome: "active",
    elements: [
      {
        elementId: "Start_1",
        elementInstanceKey: `${inst.key}-s`,
        scope: inst.key,
        enteredAt: 1000,
        exitedAt: 1005,
        durationMs: 5,
        incidents: 0,
        job: null,
      },
      {
        elementId: "Task_1",
        elementInstanceKey: `${inst.key}-t`,
        scope: inst.key,
        enteredAt: 1005,
        exitedAt: null,
        durationMs: null,
        incidents: 0,
        job: null,
      },
    ],
    incidents: [],
    path: ["Start_1", "Task_1"],
  };
}

/**
 * Layer instance content over `stubConsoleApi`: the list endpoint returns an
 * `InstancePage` (`items`, `total`, `page`, `pageSize`, matching
 * src/gen/types.gen.ts), `/instances/{key}` returns a shaped detail, `/traces/{key}`
 * returns that instance's captured trace (404 for unknown keys, exactly as the
 * real bounded ring would), and the process-definition XML endpoint (which lives
 * OUTSIDE `/console/api`, at `/v2/…`, so the base stub never sees it) returns a
 * laid-out diagram. Everything else falls through to the base stubs.
 */
export async function stubInstances(
  page: Page,
  instances: Instance[],
): Promise<void> {
  const byKey = new Map(instances.map((i) => [i.key, i]));

  await page.route("**/console/api/**", async (route) => {
    const url = new URL(route.request().url()).pathname;
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });

    const detailMatch = url.match(/\/console\/api\/instances\/([^/]+)$/);
    if (detailMatch) {
      const inst = byKey.get(decodeURIComponent(detailMatch[1]));
      if (!inst)
        return route.fulfill({ status: 404, body: "no such instance" });
      return json(makeInstanceDetail(inst));
    }
    if (url.endsWith("/console/api/instances")) {
      return json({
        items: instances,
        total: instances.length,
        page: 0,
        pageSize: 50,
      });
    }
    const traceMatch = url.match(/\/console\/api\/traces\/([^/]+)$/);
    if (traceMatch) {
      const inst = byKey.get(decodeURIComponent(traceMatch[1]));
      if (!inst) return route.fulfill({ status: 404, body: "no trace" });
      return json(makeInstanceTrace(inst));
    }
    return route.fallback();
  });

  // The BPMN XML is fetched from the gateway's v2 endpoint, not `/console/api`.
  await page.route("**/v2/process-definitions/*/xml", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/xml",
      body: MINIMAL_BPMN,
    }),
  );
}

/** A minimal running-app `ProjectDetail`, UI or headless, for the AppView guard. */
function makeProjectDetail(
  name: string,
  opts: { headless?: boolean; label?: string } = {},
): ProjectDetail {
  const config: ProjectConfig = {
    name,
    displayName: opts.label ?? name,
    description: "",
    deployTarget: "http://localhost:8080",
    main: "main.ts",
    platforms: [],
    lang: "node",
    app: "urban",
    createdMs: 0,
    updatedMs: 0,
  };
  const runState: RunState = {
    status: "running",
    pid: 1234,
    startedAtMs: 0,
    lastError: null,
    compiling: false,
  };
  const appUi: AppUi = opts.headless
    ? { enabled: false, port: null, label: opts.label ?? name }
    : { enabled: true, port: 4321, path: "/", label: opts.label ?? name };
  return {
    config,
    files: [],
    runState,
    appUi,
    rootPath: `/tmp/${name}`,
    nodeAvailable: true,
    urbanAvailable: true,
    runnable: true,
    platforms: [],
  };
}

/** Default HTML served for an embedded app iframe: enough to load cleanly so the
 * theme bridge (`onLoad`) fires. A test may pass its own body (e.g. one that
 * posts `nano-navigate`) to drive the embedded deep-link bridge. */
const APP_VIEW_HTML = `<!doctype html><html><head><meta charset="utf-8"><title>app</title></head><body><main id="app">embedded app</main></body></html>`;

/**
 * Stub a single running app for the AppView guard: `/console/api/projects/{name}`
 * returns a running detail (UI or headless), the per-project log SSE returns an
 * empty (but well-formed) event stream so the live-log effect connects without a
 * real gateway, and the same-origin app-view iframe route returns a small HTML
 * document. Layered over `stubConsoleApi` — the projects LIST and everything else
 * still come from the base stub.
 */
export async function stubApp(
  page: Page,
  opts: {
    name: string;
    headless?: boolean;
    label?: string;
    appViewHtml?: string;
  },
): Promise<void> {
  const { name, headless, label } = opts;
  const detail = makeProjectDetail(name, { headless, label });

  await page.route("**/console/api/**", async (route) => {
    const url = new URL(route.request().url()).pathname;
    if (
      url.endsWith(`/console/api/projects/${encodeURIComponent(name)}/logs`)
    ) {
      return route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        headers: { "cache-control": "no-cache" },
        // Emit a long `retry:` directive so the browser EventSource stays quiet
        // for the life of the test instead of hitting EOF and auto-reconnecting
        // every ~3s (each reconnect would trigger a needless getProject refresh).
        body: "retry: 86400000\n\n",
      });
    }
    if (url.endsWith(`/console/api/projects/${encodeURIComponent(name)}`)) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(detail),
      });
    }
    return route.fallback();
  });

  await page.route(
    `**/console/app-view/${encodeURIComponent(name)}/**`,
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "text/html",
        body: opts.appViewHtml ?? APP_VIEW_HTML,
      }),
  );
}

// ─────────────────────────────────────────────────────────────────────────
// Call-activity parent↔child navigation fixtures (issue #1118)
//
// The parent/child navigation guard needs a deterministic two-process graph: a
// PARENT model that carries a `callActivity` cell (so the diagram renders a
// clickable cell the BpmnViewer's `onElementSelect` fires on), and a CHILD whose
// `parent_process_instance_key` climbs back to the parent (so the breadcrumb
// resolves) and which the parent lists in its `called_instances`. These are the
// stub equivalents of "deploy parent+child, run an instance so the child spawns":
// the console API never talks to a real gateway here, so the spawned graph is
// expressed as shaped fixtures rather than a live deployment — which is what
// keeps the guard deterministic (fixed keys, no timing) under the no-retries rule.
// ─────────────────────────────────────────────────────────────────────────

/**
 * A laid-out PARENT model whose flow runs start → `callActivity` (`Call_Child`,
 * named "Fulfil order") → end. The `BPMNShape` DI is mandatory: without it
 * bpmn-js imports the semantics but paints no shape, so the call-activity cell
 * would not be clickable and the `onElementSelect` navigation assertion could
 * never fire. The `Call_Child` id is what the child's `calling_element_id`
 * points back at, so selecting the cell resolves to the one spawned child.
 */
export const CALL_ACTIVITY_PARENT_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
  id="Definitions_parent" targetNamespace="http://nano/e2e">
  <bpmn:process id="order-orchestrator" isExecutable="true">
    <bpmn:startEvent id="Start_1">
      <bpmn:outgoing>Flow_1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:callActivity id="Call_Child" name="Fulfil order" calledElement="fulfilment">
      <bpmn:incoming>Flow_1</bpmn:incoming>
      <bpmn:outgoing>Flow_2</bpmn:outgoing>
    </bpmn:callActivity>
    <bpmn:endEvent id="End_1">
      <bpmn:incoming>Flow_2</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="Flow_1" sourceRef="Start_1" targetRef="Call_Child" />
    <bpmn:sequenceFlow id="Flow_2" sourceRef="Call_Child" targetRef="End_1" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="Diagram_1">
    <bpmndi:BPMNPlane id="Plane_1" bpmnElement="order-orchestrator">
      <bpmndi:BPMNShape id="Start_1_di" bpmnElement="Start_1">
        <dc:Bounds x="150" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Call_Child_di" bpmnElement="Call_Child">
        <dc:Bounds x="240" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_1_di" bpmnElement="End_1">
        <dc:Bounds x="400" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_1_di" bpmnElement="Flow_1">
        <di:waypoint x="186" y="118" />
        <di:waypoint x="240" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_2_di" bpmnElement="Flow_2">
        <di:waypoint x="340" y="118" />
        <di:waypoint x="400" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

/**
 * Layer an explicit instance GRAPH over `stubConsoleApi`: unlike `stubInstances`
 * (which derives every detail from a list `Instance` and serves one shared XML),
 * this serves caller-supplied `InstanceDetail`s verbatim — so a scenario controls
 * each instance's `called_instances` and `parent_process_instance_key` — and
 * resolves the diagram XML per `process_definition_key` from `xmlByDefKey`
 * (falling back to `MINIMAL_BPMN`). The list endpoint returns each detail's
 * `instance`; `/traces/{key}` 404s (the component treats that as "no trace",
 * exactly as the real bounded ring does for an untraced instance). Everything
 * else falls through to the base stubs. Register AFTER `stubConsoleApi`.
 */
export async function stubInstanceGraph(
  page: Page,
  details: InstanceDetail[],
  xmlByDefKey: Record<string, string> = {},
): Promise<void> {
  const byKey = new Map(details.map((d) => [d.instance.key, d]));
  const items = details.map((d) => d.instance);

  await page.route("**/console/api/**", async (route) => {
    const url = new URL(route.request().url()).pathname;
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });

    const detailMatch = url.match(/\/console\/api\/instances\/([^/]+)$/);
    if (detailMatch) {
      const detail = byKey.get(decodeURIComponent(detailMatch[1]));
      if (!detail)
        return route.fulfill({ status: 404, body: "no such instance" });
      return json(detail);
    }
    if (url.endsWith("/console/api/instances")) {
      return json({ items, total: items.length, page: 0, pageSize: 50 });
    }
    // An untraced instance: 404 is the only "no trace" signal the component
    // accepts (a shaped-but-empty trace would render a misleading timeline).
    if (/\/console\/api\/traces\/[^/]+$/.test(url)) {
      return route.fulfill({ status: 404, body: "no trace" });
    }
    return route.fallback();
  });

  // The BPMN XML is fetched from the gateway's v2 endpoint (outside /console/api),
  // keyed by process-definition key — resolve each parent/child model by its key.
  await page.route("**/v2/process-definitions/*/xml", (route) => {
    const path = new URL(route.request().url()).pathname;
    const m = path.match(/\/v2\/process-definitions\/([^/]+)\/xml$/);
    const key = m ? decodeURIComponent(m[1]) : "";
    route.fulfill({
      status: 200,
      contentType: "application/xml",
      body: xmlByDefKey[key] ?? MINIMAL_BPMN,
    });
  });
}
