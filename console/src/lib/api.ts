// Hand-written helpers for console surfaces that are intentionally NOT part of
// the spec-first console API (spec-console/console-api.yaml → src/gen). These
// cover streaming (SSE), binary/zip downloads, and the cross-origin Camunda
// gateway proxy — shapes the generated fetch client can't model. Everything
// else now goes through the generated SDK in `../gen`.

import { debug } from "./debugBus";

/**
 * Build a link to the equivalent console page on another cluster node.
 * `address` is that node's advertised base URL (`http://host:port`, from the
 * topology); `path` is the in-app route (e.g. "/topology"). Returns null when
 * there's no usable address — e.g. the self/local node, whose address is empty.
 */
export function nodeConsoleUrl(
  address: string | undefined | null,
  path: string,
): string | null {
  if (!address) return null;
  const base = address.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  return `${base}/console${p}`;
}

// --- Binary project file read ----------------------------------------------

/// A project file's contents plus metadata. For binary files `text` is empty
/// and the UI shows a placeholder built from `absPath`/`size` instead.
export interface ProjectFile {
  binary: boolean;
  text: string;
  absPath: string;
  size: number;
}

/// Fetches a project file, distinguishing binary files (which the server
/// reports via `X-File-Binary` and a `{ absPath, size }` JSON descriptor).
/// Kept hand-written because the generated JSON/text client can't model the
/// header-driven binary/text branch.
async function getProjectFile(path: string): Promise<ProjectFile> {
  const res = await fetch(`/console/api${path}`);
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `${path} → HTTP ${res.status}`);
  }
  const size = Number(res.headers.get("X-File-Size") ?? "0");
  if (res.headers.get("X-File-Binary") === "true") {
    const meta = (await res.json().catch(() => ({}))) as {
      absPath?: string;
      size?: number;
    };
    return {
      binary: true,
      text: "",
      absPath: meta.absPath ?? "",
      size: meta.size ?? size,
    };
  }
  return { binary: false, text: await res.text(), absPath: "", size };
}

/// Reads a project file, surfacing the binary/text distinction (see
/// `ProjectFile`). Used by the project workspace editor.
export function projectFileEx(
  name: string,
  path: string,
): Promise<ProjectFile> {
  return getProjectFile(
    `/projects/${encodeURIComponent(name)}/file?path=${encodeURIComponent(path)}`,
  );
}

// --- Import-by-reference filesystem browser (loopback only) ----------------

/// One sub-directory returned by the host filesystem browser.
export interface BrowseEntry {
  name: string;
  path: string;
  isNanoApp: boolean;
}

/// A single level of the host filesystem, for the Import-by-reference picker.
export interface BrowseResult {
  path: string;
  parent: string | null;
  isNanoApp: boolean;
  entries: BrowseEntry[];
}

/// True when the console is being viewed over a loopback origin, in which case
/// the server's filesystem browser (`browseFilesystem`) is reachable. Off
/// localhost the Import panel keeps the plain typed-path field only.
export function isLocalhost(): boolean {
  const h = window.location.hostname;
  return h === "localhost" || h === "127.0.0.1" || h === "::1" || h === "[::1]";
}

/// Lists the immediate sub-directories of an absolute host path so the
/// Import-by-reference dialog can browse to a checked-out app. Omit `path` to
/// open on the server's home directory. Loopback-only server-side; throws with
/// the server's error text (e.g. a bad path, or 403 off localhost).
export async function browseFilesystem(path?: string): Promise<BrowseResult> {
  const qs = path ? `?path=${encodeURIComponent(path)}` : "";
  const res = await fetch(`/console/api/fs/browse${qs}`);
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `browse → HTTP ${res.status}`);
  }
  return (await res.json()) as BrowseResult;
}

// --- Integrated terminal enablement (issue #500) ---------------------------

/// The integrated terminal's current enablement, as reported by the server.
export interface TerminalConfig {
  /// Effective: whether the terminal is on right now.
  enabled: boolean;
  /// `NANO_CONSOLE_TERMINAL` has hard-disabled it; the console cannot enable it.
  locked: boolean;
  /// Whether *this* client is on the local machine and could actually use it.
  local: boolean;
  /// How the value was decided: env-locked | env-default | console | default.
  source: string;
}

/// Reads the integrated terminal's enablement. Informational (ungated); used by
/// the Config toggle and the terminal pane to render the right state.
export async function getTerminalConfig(): Promise<TerminalConfig> {
  const res = await fetch("/console/api/config/terminal");
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `terminal config → HTTP ${res.status}`);
  }
  return (await res.json()) as TerminalConfig;
}

/// Enables or disables the integrated terminal and persists it server-side.
/// Loopback-only server-side; throws with the server's text on 403 (off
/// localhost) or 409 (locked off by `NANO_CONSOLE_TERMINAL`).
export async function setTerminalConfig(
  enabled: boolean,
): Promise<TerminalConfig> {
  const res = await fetch("/console/api/config/terminal", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ enabled }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `terminal config → HTTP ${res.status}`);
  }
  return (await res.json()) as TerminalConfig;
}

/// A single line of a worker's run log stream, delivered over SSE.
export interface WorkerLogLine {
  tsMs: number;
  stream: "out" | "err" | "sys";
  text: string;
}

// --- Standalone worker app export (zip) ------------------------------------

/// Bundles the named workers into a standalone, runnable Deno application and
/// triggers a browser download of the returned `.zip`. The zip ships every
/// worker's source, the embedded worker SDK, a `main.ts` that deploys all
/// `resources/*.bpmn` models on startup before running the workers, a
/// `deno.json` start task, and a README. Throws with the server's error text on
/// failure (e.g. no workers selected).
export async function exportWorkersApp(workers: string[]): Promise<void> {
  const res = await fetch("/console/api/export-workers-app", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workers }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `export → HTTP ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("content-disposition") || "";
  const match = /filename="?([^"]+)"?/.exec(disposition);
  const filename = match ? match[1] : "nano-workers-app.zip";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// --- Camunda gateway helpers (deploy/start against a possibly-foreign gateway)

/// Deploys BPMN XML to the engine through the standard Camunda deployment
/// endpoint (not a console API). Resolves on success; throws with the server's
/// problem detail otherwise. Deployment is idempotent, so deploying an unchanged
/// model is a safe no-op.
export async function deployXml(
  name: string,
  xml: string,
  baseUrl?: string,
): Promise<void> {
  const form = new FormData();
  form.append(
    "resources",
    new Blob([xml], { type: "text/xml" }),
    `${name}.bpmn`,
  );
  const url = joinBase(baseUrl, "/v2/deployments");
  const started = performance.now();
  debug("deploy", "info", `POST ${url}`, {
    baseUrl: baseUrl ?? "(relative)",
    resource: `${name}.bpmn`,
    bytes: xml.length,
    sameOrigin: url.startsWith("/"),
  });
  let res: Response;
  try {
    res = await gatewayFetch(baseUrl, "/v2/deployments", {
      method: "POST",
      body: form,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    debug("deploy", "error", `fetch failed: ${msg}`, {
      url,
      hint:
        url.startsWith("http") && !url.startsWith(window.location.origin)
          ? "Cross-origin request routed through /console/api/gateway-proxy — the Nano server could not reach the upstream gateway."
          : "Is the gateway running on this port? Check `curl " + url + "`.",
    });
    throw new Error(`fetch failed: ${msg}`);
  }
  const ms = Math.round(performance.now() - started);
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    debug("deploy", "error", `HTTP ${res.status} in ${ms}ms`, {
      body: detail.slice(0, 500),
    });
    throw new Error(detail || `deploy → HTTP ${res.status}`);
  }
  debug("deploy", "ok", `HTTP ${res.status} in ${ms}ms`);
}

/// The result of starting a process instance, as returned by the Camunda
/// `createProcessInstance` endpoint. Only the fields the console needs are typed.
export interface CreateInstanceResult {
  processInstanceKey: string;
  processDefinitionId: string;
  processDefinitionVersion: number;
  processCompleted: boolean;
}

/// Starts a process instance on the connected cluster through the standard
/// Camunda endpoint `POST /v2/process-instances` (not a console API). The process
/// must already be deployed; the gateway picks the latest deployed version of
/// `processId`. When `awaitCompletion` is set the request blocks until the
/// instance reaches a terminal state (or the gateway's request timeout elapses),
/// reflected in `processInstanceCompleted`. Throws with the server's problem
/// detail on failure (e.g. 404 not-deployed, 503 RESOURCE_EXHAUSTED).
export async function createProcessInstance(opts: {
  processId: string;
  variables?: Record<string, unknown>;
  awaitCompletion?: boolean;
  baseUrl?: string;
}): Promise<CreateInstanceResult> {
  const body: Record<string, unknown> = {
    processDefinitionId: opts.processId,
    variables: opts.variables ?? {},
  };
  if (opts.awaitCompletion) body.awaitCompletion = true;
  const url = joinBase(opts.baseUrl, "/v2/process-instances");
  const started = performance.now();
  debug("startInstance", "info", `POST ${url}`, {
    processId: opts.processId,
    variables: opts.variables ?? {},
  });
  let res: Response;
  try {
    res = await gatewayFetch(opts.baseUrl, "/v2/process-instances", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    debug("startInstance", "error", `fetch failed: ${msg}`, { url });
    throw new Error(`fetch failed: ${msg}`);
  }
  const ms = Math.round(performance.now() - started);
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    debug("startInstance", "error", `HTTP ${res.status} in ${ms}ms`, {
      body: detail.slice(0, 500),
    });
    throw new Error(detail || `start instance → HTTP ${res.status}`);
  }
  const parsed = (await res.json()) as CreateInstanceResult;
  debug("startInstance", "ok", `HTTP ${res.status} in ${ms}ms`, {
    processInstanceKey: parsed.processInstanceKey,
    processDefinitionVersion: parsed.processDefinitionVersion,
  });
  return parsed;
}

/// Concats a Camunda-relative path with an optional base URL. When `base` is
/// missing or empty, or its origin matches the console's own, the path is
/// returned verbatim so fetch() stays same-origin (no CORS preflight,
/// cookies pass through). Trailing/leading slashes are normalised.
function joinBase(base: string | undefined, path: string): string {
  if (!base) return path;
  try {
    const u = new URL(base, window.location.href);
    if (u.origin === window.location.origin) {
      return `${u.pathname.replace(/\/+$/, "")}${path.startsWith("/") ? path : `/${path}`}`;
    }
  } catch {
    // Malformed base — fall through and let the caller see the fetch error.
  }
  return `${base.replace(/\/+$/, "")}${path.startsWith("/") ? path : `/${path}`}`;
}

/// Decides how to reach a Camunda REST endpoint on a possibly-foreign gateway.
///
/// - Same-origin (or no base): a plain relative fetch, no proxy needed.
/// - Cross-origin: routes through the console's `/console/api/gateway-proxy/*`
///   endpoint, packing the upstream base URL into `X-Gateway-Target`. This
///   sidesteps browser CORS entirely because the browser only ever talks to
///   the Nano server that shipped this SPA. Stock `c8run` doesn't expose CORS
///   headers, so this is the only way the Console can deploy/start against a
///   Camunda 8 cluster running on a different port.
///
/// Callers should NOT set `X-Gateway-Target` themselves; the helper merges it
/// into `init.headers` when routing through the proxy.
function gatewayFetch(
  baseUrl: string | undefined,
  path: string,
  init?: RequestInit,
): Promise<Response> {
  const normalisedPath = path.startsWith("/") ? path : `/${path}`;
  if (!baseUrl) return fetch(normalisedPath, init);
  let target: URL;
  try {
    target = new URL(baseUrl, window.location.href);
  } catch {
    return fetch(joinBase(baseUrl, normalisedPath), init);
  }
  if (target.origin === window.location.origin) {
    return fetch(joinBase(baseUrl, normalisedPath), init);
  }
  const proxyPath = `/console/api/gateway-proxy${normalisedPath}`;
  const upstreamBase = `${target.origin}${target.pathname.replace(/\/+$/, "")}`;
  const headers = new Headers(init?.headers);
  headers.set("X-Gateway-Target", upstreamBase);
  return fetch(proxyPath, { ...init, headers });
}

/// Fetches the deployed BPMN XML for a process id from a specific gateway.
/// Used by the modeler to decide whether the on-disk file matches the deployed
/// definition. Camunda's REST API needs the deployment *key* to serve XML; we
/// look up the latest deployed definition by id first, then pull its XML.
/// Returns `null` for any non-success outcome — the process has never been
/// deployed, the gateway is unreachable, the request fails auth/validation,
/// or the search returns no matches. Callers treat "unknown" the same as
/// "not deployed": Start stays disabled until the user clicks Deploy.
export async function fetchDeployedXmlByProcessId(
  processId: string,
  baseUrl?: string,
): Promise<string | null> {
  const searchUrl = joinBase(baseUrl, "/v2/process-definitions/search");
  const started = performance.now();
  debug("probe", "info", `POST ${searchUrl}`, {
    processId,
    baseUrl: baseUrl ?? "(relative)",
  });
  try {
    const res = await gatewayFetch(baseUrl, "/v2/process-definitions/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filter: { processDefinitionId: processId },
        sort: [{ field: "version", order: "DESC" }],
        page: { from: 0, limit: 1 },
      }),
    });
    if (!res.ok) {
      debug("probe", "warn", `search → HTTP ${res.status}`, {
        processId,
        hint: "Start Instance will stay disabled; deploy first.",
      });
      return null;
    }
    const body = (await res.json()) as {
      items?: Array<{ processDefinitionKey?: string }>;
    };
    const key = body.items?.[0]?.processDefinitionKey;
    if (!key) {
      debug("probe", "info", "no prior deployment found", { processId });
      return null;
    }
    const xmlRes = await gatewayFetch(
      baseUrl,
      `/v2/process-definitions/${key}/xml`,
    );
    if (xmlRes.status !== 200) {
      debug("probe", "warn", `xml → HTTP ${xmlRes.status}`, {
        processDefinitionKey: key,
      });
      return null;
    }
    const xml = await xmlRes.text();
    const ms = Math.round(performance.now() - started);
    debug("probe", "ok", `deployed XML loaded (${xml.length} bytes, ${ms}ms)`, {
      processId,
      processDefinitionKey: key,
    });
    return xml;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    debug("probe", "error", `fetch failed: ${msg}`, {
      url: searchUrl,
      hint:
        searchUrl.startsWith("http") &&
        !searchUrl.startsWith(window.location.origin)
          ? "Cross-origin request — the gateway may lack CORS headers, or be unreachable."
          : "Is the gateway running on this port?",
    });
    return null;
  }
}

/// The verbatim BPMN XML for a process definition, served by the gateway's
/// generated Camunda endpoint (getProcessDefinitionXML) — not a console API.
export async function fetchProcessXml(
  processDefinitionKey: string,
): Promise<string | null> {
  const res = await fetch(
    `/v2/process-definitions/${processDefinitionKey}/xml`,
  );
  if (res.status === 200) {
    return res.text();
  }
  // 204 (no XML) or 404 (unknown / non-latest version).
  return null;
}

// --- Project export (zip) + run/compile log stream (SSE) -------------------

/// A BPMN model derived from a code-first workflow definition (ADR 0045).
export interface DerivedModel {
  id: string;
  /** `imperative` (replayed) or `declarative` (graph) workflow. */
  kind: string;
  /** The executable BPMN XML derived from the code by `@nanobpm/workflow`. */
  xml: string;
}

/// Derives the executable BPMN for a code-first workflow project. The server
/// runs the project's `workflows/*.ts` through `@nanobpm/workflow`'s `toBpmn`
/// (read-only). Throws with the server's detail on failure (e.g. Deno missing).
export async function projectDerivedModels(
  name: string,
): Promise<DerivedModel[]> {
  const res = await fetch(
    `/console/api/projects/${encodeURIComponent(name)}/derived-models`,
  );
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `derive → HTTP ${res.status}`);
  }
  return (await res.json()) as DerivedModel[];
}

/// The download URL for a project export zip. When `dist` is set the compiled
/// `dist/` binaries are bundled too (large + platform-specific).
export function projectExportUrl(name: string, dist = false): string {
  const q = dist ? "?dist=true" : "";
  return `/console/api/projects/${encodeURIComponent(name)}/export${q}`;
}

/// Triggers a browser download of a project's export zip.
export async function exportProject(name: string, dist = false): Promise<void> {
  const res = await fetch(projectExportUrl(name, dist));
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `export → HTTP ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("content-disposition") || "";
  const match = /filename="?([^"]+)"?/.exec(disposition);
  const filename = match ? match[1] : `${name}.zip`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/// A single line of a project's run/compile log stream.
export interface ProjectLogLine {
  tsMs: number;
  /** `out` (stdout), `err` (stderr), or `sys` (supervisor notes). */
  stream: "out" | "err" | "sys";
  text: string;
}

/// Opens an SSE stream of a project's run/compile log lines. `onLine` is called
/// for each line (history is replayed first, then live). Returns the
/// `EventSource`; the caller is responsible for `.close()`.
export function projectLogs(
  name: string,
  onLine: (line: ProjectLogLine) => void,
): EventSource {
  const src = new EventSource(
    `/console/api/projects/${encodeURIComponent(name)}/logs`,
  );
  src.addEventListener("log", (ev) => {
    try {
      onLine(JSON.parse((ev as MessageEvent).data) as ProjectLogLine);
    } catch {
      /* ignore malformed line */
    }
  });
  return src;
}

// --- Live consumers panel (issue #404) --------------------------------------

/// Transport a live job consumer arrived on. `rest` = `activateJobs` long-poll;
/// `falcon` = the `/falcon` command-stream WebSocket.
export type ConsumerTransport = "rest" | "falcon";

/// One live consumer (a "hired agent" polling a job type), as reported by the
/// engine's `/console/api/consumers` endpoint — distinct from a console-authored
/// worker directory.
export interface Consumer {
  jobType: string;
  worker: string;
  transport: ConsumerTransport;
  /** Wall-clock (epoch millis) of the consumer's most recent activity. */
  lastSeenMs: number;
  /** Age of `lastSeenMs` relative to the response's `nowMs`. */
  ageMs: number;
  /** `live` while within its transport's liveness window, else `idle`. */
  status: "live" | "idle";
}

/// The engine's consumers snapshot: the live rows plus the liveness windows used
/// to compute their status (so the UI can label the thresholds it shows).
export interface ConsumersResponse {
  nowMs: number;
  restStaleMs: number;
  falconLivenessMs: number;
  consumers: Consumer[];
}

/// Fetches the live "who is polling what" consumers snapshot. Hand-written (not
/// spec-first) because it reads a gateway-level engine endpoint, not the
/// console's own CRUD API.
export async function getConsumers(): Promise<ConsumersResponse> {
  const res = await fetch("/console/api/consumers");
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `consumers → HTTP ${res.status}`);
  }
  return (await res.json()) as ConsumersResponse;
}

// --- Worker-provisioning advice panel (issue #1294) -------------------------

/// Per-job-type provisioning classification from the shared worker-scaling
/// advisor. Kebab-case on the wire (Rust `serde(rename_all = "kebab-case")`):
/// - `starved`: jobs waiting, zero workers subscribed — no drain capacity.
/// - `under-provisioned`: workers present but backlog growing with server headroom.
/// - `server-bound`: backlog growing but the server is at its throughput ceiling —
///   more workers won't help.
/// - `adequate`: backlog small or shrinking.
/// - `warming`: not enough history yet (first scrape) to judge rates.
export type ProvisioningClass =
  | "starved"
  | "under-provisioned"
  | "server-bound"
  | "adequate"
  | "warming";

/// Confidence in a recommendation.
export type ProvisioningConfidence = "high" | "medium" | "low";

/// One job type's provisioning recommendation, as reported by the engine's
/// `/console/api/provisioning` endpoint.
export interface ProvisioningRecommendation {
  jobType: string;
  class: ProvisioningClass;
  confidence: ProvisioningConfidence;
  /** Jobs waiting for pickup right now. */
  backlog: number;
  /** Backlog growth over the window (jobs/s; negative = draining). */
  backlogSlopePerS: number;
  /** Jobs handed to workers over the window (jobs/s) — the drain rate. */
  drainPerS: number;
  /** Subscribed workers (both stream and REST long-poll transports). */
  workers: number;
  /** Suggested change to the worker count (0 = leave alone). */
  suggestWorkerDelta: number;
  /** Human-readable explanation of the classification. */
  rationale: string;
}

/// The advisor's whole-instance provisioning verdict for the latest window.
export interface ProvisioningAdvice {
  /** True when the server itself is the bottleneck — scaling workers won't help. */
  serverBound: boolean;
  /** Journal-writer duty cycle over the window (busy fraction, 0..1). */
  writerBusyRatio: number;
  /** The throughput-ceiling clipping LED. */
  ceilingThroughput: boolean;
  pendingCreateQueue: number;
  /** Admissions shed over the window (delta). */
  shedDelta: number;
  /** Seconds between the two samples this advice was computed over. */
  windowS: number;
  recommendations: ProvisioningRecommendation[];
}

/// Fetches the latest per-job-type worker-provisioning advice. Hand-written (not
/// spec-first) for the same reason as `getConsumers`: it reads a gateway-level
/// engine endpoint, not the console's own CRUD API.
export async function getProvisioning(): Promise<ProvisioningAdvice> {
  const res = await fetch("/console/api/provisioning");
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `provisioning → HTTP ${res.status}`);
  }
  return (await res.json()) as ProvisioningAdvice;
}
