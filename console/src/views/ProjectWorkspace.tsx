import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  Suspense,
} from "react";
import { Link, useParams } from "react-router-dom";
import CodeEditor, { languageForFile } from "../components/CodeEditor";
import TerminalPane from "../components/TerminalPane";
import MarkdownPreview from "../components/MarkdownPreview";
import type {
  BpmnModelerHandle,
  DomainTypeBinding,
} from "../components/BpmnModeler";
import type { DmnModelerHandle } from "../components/DmnModeler";
import type { FormEditorHandle } from "../components/FormEditor";
import ShapeComposer, { type ShapePreview } from "../components/ShapeComposer";
import PageComposer, {
  type PageComposerHandle,
} from "../components/PageComposer";
import type { MetaEntry, ShapeDecl, ShapeOp } from "../lib/shapeCarrier";
import { lazyImport } from "../lib/lazyWithReload";
import { envelopeEditableFields } from "../lib/shapeCarrier";
import type { ComposerEntity } from "../lib/shapeComposer";
import { SCALAR_KEYWORDS } from "../lib/shapeComposer";
import AppManifestEditor, {
  isAppManifestPath,
} from "../components/AppManifestEditor";
import {
  compileProject,
  createProjectPath,
  deleteProjectPath,
  getProject,
  getConnectors,
  getDataSchema,
  getDataSources,
  listProjectFiles,
  listProjects,
  previewDomainTypes,
  runProject,
  saveProjectConfig,
  saveProjectFile,
  setActiveRunConfig,
  stopProject,
  type FileNode,
  type ProjectDetail,
  type ProjectConfig,
  type ProjectSummary,
  type RunState,
} from "../gen";
import {
  projectLogs,
  exportProject,
  deployXml,
  createProcessInstance,
  fetchDeployedXmlByProcessId,
  projectFileEx,
  type ProjectLogLine,
  type ProjectFile,
} from "../lib/api";
import { Button, inputClass } from "../components/ui";
import { useTemplateUpdate } from "../components/TemplateUpdate";
import { toUpdateTarget } from "../lib/templateUpdate";
import { appIsRunning } from "../lib/appRunning";
import { decisionFeelVariables } from "../lib/dmnDomainVariables";
import { TOUR_ANCHOR } from "../lib/tour/tourAnchors";
import {
  type ActivePanel,
  togglePanel,
  selectFile,
} from "../lib/workspacePanel";
import {
  forgetServedAppUrl,
  rememberServedAppUrl,
  servedAppPort,
  servedAppUrl,
} from "../lib/servedApp";
import {
  processFeelVariables,
  componentOutputFeelVariables,
  type ComponentOutput,
} from "../lib/bpmnDomainVariables";
import { formTypeId } from "@nanobpm/nano-app-schema";
import {
  loadProjectComponents,
  loadPackComponents,
  combineComponents,
  filterEnabledPackComponents,
  type ElementTemplate,
} from "../lib/projectComponents";
import {
  subscribe as subscribeDebug,
  snapshot as debugSnapshot,
  clear as clearDebug,
  debug,
  type DebugEntry,
} from "../lib/debugBus";
import {
  type DirState,
  collectDirPaths,
  isDirOpen,
  loadDirState,
  pruneDirState,
  serializeDirState,
  toggleDir,
} from "./explorerDirState";

// All route-level lazy chunks + their shared fallback. Declared here, below the
// full import block, so every `import` stays contiguous at the top of the module
// (ESM hoists imports, but keeping them together is clearer and lint-friendly).
// The BPMN/DMN/form modelers each drag in a heavy editor stack (bpmn-js, dmn-js,
// @bpmn-io/form-js-editor); they are used ONLY here and only when a file of that
// kind is open, so lazy-loading them splits ~megabytes out of the
// ProjectWorkspace chunk and defers the cost until you actually edit that file
// type. Each modeler usage is wrapped in <Suspense fallback={modelerFallback}>.
const TestRunPanel = lazyImport(() => import("../components/TestRunPanel"));
const DataPanel = lazyImport(() => import("../components/DataPanel"));
const TriggersPanel = lazyImport(() => import("../components/TriggersPanel"));
const ConnectorsPanel = lazyImport(
  () => import("../components/ConnectorsPanel"),
);
const DerivedModelPanel = lazyImport(
  () => import("../components/DerivedModelPanel"),
);
const BpmnModeler = lazyImport(() => import("../components/BpmnModeler"));
const DmnModeler = lazyImport(() => import("../components/DmnModeler"));
const FormEditor = lazyImport(() => import("../components/FormEditor"));
const FormPreview = lazyImport(() => import("../components/FormPreview"));

const modelerFallback = (
  <div className="flex h-full items-center justify-center text-sm text-fg-faint">
    Loading editor…
  </div>
);

/// One project's workspace: file browser, graphical/code editors, the run
/// console, and the Run/Stop/Compile/Configure/Export toolbar.
export default function ProjectWorkspace() {
  const { name = "" } = useParams();
  const [detail, setDetail] = useState<ProjectDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Remember the open file per project so leaving the workspace (e.g. to peek
  // at Metrics) and coming back restores the same editor tab.
  const selectedKey = `nano.project.${name}.openFile`;
  const [selected, setSelectedState] = useState<string | null>(() =>
    localStorage.getItem(selectedKey),
  );
  const setSelected = useCallback(
    (path: string | null) => {
      setSelectedState(path);
      if (path) localStorage.setItem(selectedKey, path);
      else localStorage.removeItem(selectedKey);
    },
    [selectedKey],
  );
  const [runState, setRunState] = useState<RunState | null>(null);
  const [logs, setLogs] = useState<ProjectLogLine[]>([]);
  const [showConfig, setShowConfig] = useState(false);
  // Single source of truth for the mutually-exclusive auxiliary panels; `null`
  // means the file-content editor is showing. Opening a file clears it (#485).
  const [activePanel, setActivePanel] = useState<ActivePanel>(null);
  const openFile = useCallback(
    (path: string) => {
      setSelected(path);
      setActivePanel(selectFile);
    },
    [setSelected],
  );
  const logRef = useRef<HTMLDivElement>(null);
  const [consoleHeight, setConsoleHeight] = useState(() => {
    const saved = Number(localStorage.getItem("nano.consoleHeight"));
    return saved >= 80 && saved <= 1200 ? saved : 224;
  });
  const dragging = useRef(false);
  const startDrag = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      const next = Math.min(
        Math.max(window.innerHeight - ev.clientY, 80),
        1200,
      );
      setConsoleHeight(next);
    };
    const onUp = () => {
      dragging.current = false;
      localStorage.setItem(
        "nano.consoleHeight",
        String(consoleHeightRef.current),
      );
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);
  const consoleHeightRef = useRef(consoleHeight);
  consoleHeightRef.current = consoleHeight;

  const reloadFiles = useCallback(async () => {
    try {
      const res = (
        await listProjectFiles({ path: { name }, throwOnError: true })
      ).data;
      setDetail((d) => (d ? { ...d, files: res.files } : d));
    } catch {
      /* ignore */
    }
  }, [name]);

  const load = useCallback(async () => {
    try {
      const d = (await getProject({ path: { name }, throwOnError: true })).data;
      setDetail(d);
      setRunState(d.runState);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [name]);

  // The open project's listing summary — the single source of the computed
  // `updateAvailable` / `latestVersion` signal (the detail endpoint returns the
  // stored config only). Fetched alongside the detail so the toolbar can offer
  // the same "Update possible" affordance as the projects list, wired to the
  // shared review-and-apply flow. Refreshed after an apply so the badge clears.
  const [summary, setSummary] = useState<ProjectSummary | null>(null);
  const loadSummary = useCallback(async () => {
    try {
      const list = (await listProjects({ throwOnError: true })).data.projects;
      setSummary(list.find((p) => p.name === name) ?? null);
    } catch {
      /* non-fatal: the affordance just stays hidden */
    }
  }, [name]);
  const {
    startUpdate,
    previewName,
    busy: updateBusy,
    error: updateError,
    modals: updateModals,
  } = useTemplateUpdate({
    onApplied: async () => {
      await load();
      await loadSummary();
    },
  });
  const canUpdate = !!summary?.updateAvailable && summary.source !== "path";

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    void loadSummary();
  }, [loadSummary]);

  // Drop a remembered open-file selection if that file no longer exists (e.g.
  // it was deleted since the last visit), so we don't render a 404 editor pane.
  useEffect(() => {
    if (!detail || !selected) return;
    const exists = (nodes: FileNode[]): boolean =>
      nodes.some(
        (n) => n.path === selected || (n.children ? exists(n.children) : false),
      );
    if (!exists(detail.files)) setSelected(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [detail]);

  // Stream run/compile logs for the life of the workspace.
  useEffect(() => {
    if (!name) return;
    const src = projectLogs(name, (line) => {
      setLogs((prev) => {
        const next =
          prev.length > 2000 ? prev.slice(prev.length - 2000) : prev.slice();
        next.push(line);
        return next;
      });
    });
    return () => src.close();
  }, [name]);

  // Poll run state while the app is active (run/stop/compile flip it async).
  useEffect(() => {
    const active =
      runState && (runState.status !== "stopped" || runState.compiling);
    if (!active) return;
    const t = setInterval(async () => {
      try {
        const d = (await getProject({ path: { name }, throwOnError: true }))
          .data;
        setRunState(d.runState);
        // Refresh the app-view descriptor too: the ADR 0057 boot handshake
        // reports the app's real bound port a moment AFTER it starts, so a
        // stale `detail.appUi` (captured before start) would leave "Open app"
        // and the served-URL probe pointing at the old/guessed port.
        setDetail((prev) => (prev ? { ...prev, appUi: d.appUi } : prev));
      } catch {
        /* ignore */
      }
    }, 1500);
    return () => clearInterval(t);
  }, [name, runState]);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [logs]);

  const runnable = detail?.runnable ?? false;
  const lang = detail?.config.lang ?? "node";
  // Categorical edit gate (issue #889): mirror the server's "running" set so
  // the banner + disabled affordances match exactly what it refuses with 409.
  const running = appIsRunning(runState?.status);
  const compiling = runState?.compiling ?? false;

  // Urban App projects carry a root `nano.app.json`; only those have
  // datasources, so the DB Manager (Data) toolbar entry is shown for them.
  const isUrbanApp =
    detail?.files.some(
      (f) => f.kind === "file" && f.name === "nano.app.json",
    ) ?? false;

  // While an Urban App runs, publish its served URL (on its real configured
  // port) so the RAD guided journey's success probe hits the actual app, not a
  // guessed default — and clear it when it stops or the workspace unmounts. The
  // "Open app" link below computes the same URL. This must sit ABOVE the early
  // returns below so the hook runs on every render — otherwise hook order
  // differs between the loading render (no `detail`) and the loaded one, which
  // is React error #310 ("Rendered more hooks than during the previous render").
  const servedUrl =
    running && isUrbanApp && detail
      ? servedAppUrl(
          window.location.origin,
          // The ADR 0057 boot handshake reports the port the app actually bound
          // (surfaced as `appUi.port`); prefer it over the legacy manifest/env
          // guess so "Open app" opens the real port even when the app picks it
          // at runtime behind a custom env var.
          detail.appUi?.port ?? servedAppPort(detail.config),
        )
      : null;
  useEffect(() => {
    if (servedUrl) {
      rememberServedAppUrl(servedUrl);
      return () => forgetServedAppUrl();
    }
    forgetServedAppUrl();
    return undefined;
  }, [servedUrl]);

  const runConfigs = detail?.config.toolchain?.runConfigs ?? [];
  // The server resolves the same fallback (pinned → default:true → first) at
  // resolve_run_argv time, but we mirror it here so the picker's initial
  // value matches what Run would actually spawn. Also validate the pinned
  // id against the current runConfigs — a hand-edited nanobpm.project.json
  // could name an unknown id, in which case the server falls back but the
  // controlled <select> would render blank/out-of-sync without this check.
  const pinnedId = detail?.config.toolchain?.activeRunConfig ?? null;
  const activeRunConfigId =
    (pinnedId && runConfigs.some((c) => c.id === pinnedId) ? pinnedId : null) ??
    runConfigs.find((c) => c.default)?.id ??
    runConfigs[0]?.id ??
    null;

  const changeRunConfig = async (id: string | null) => {
    try {
      await setActiveRunConfig({
        path: { name },
        body: { id },
        throwOnError: true,
      });
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const run = async () => {
    setLogs([]);
    try {
      setRunState(
        (await runProject({ path: { name }, throwOnError: true })).data,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  const stop = async () => {
    try {
      setRunState(
        (await stopProject({ path: { name }, throwOnError: true })).data,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  // Compile runs the project's one canonical compile invocation: a snapshotted
  // toolchain (flat `compile` or a runConfig with compile argv) or the lang
  // pack's. The old Deno `compile` — with its per-target cross-compile modal —
  // was the only built-in cross-compiler and was removed with Deno (ADR 0065);
  // a project with no compile command surfaces the server's "no compile
  // command" error.
  const handleCompile = async () => {
    try {
      await compileProject({
        path: { name },
        body: { targets: [] },
        throwOnError: true,
      });
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  if (error) {
    return (
      <div className="p-8">
        <Link
          to="/projects"
          className="text-sm text-accent-strong hover:underline"
        >
          ← Projects
        </Link>
        <div className="mt-4 rounded-md border border-danger/40 bg-danger/10 px-4 py-3 text-sm text-danger">
          {error}
        </div>
      </div>
    );
  }

  if (!detail) {
    return <div className="p-8 text-sm text-fg-faint">Loading…</div>;
  }

  // Code-first workflow projects (ADR 0045) have no authored `.bpmn`; their model
  // is DERIVED from `workflows/*.ts`. Offer a read-only "Model" view for them.
  const isWorkflowProject = hasTopLevelDir(detail.files, "workflows");

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b border-edge bg-panel px-4 py-2">
        <nav
          className="flex items-center gap-1.5 text-sm"
          aria-label="Breadcrumb"
        >
          <Link to="/projects" className="text-fg-faint hover:text-fg-muted">
            Projects
          </Link>
          <span className="text-fg-faint">/</span>
          <span className="font-semibold text-fg" title={detail.config.name}>
            {detail.config.displayName ?? detail.config.name}
          </span>
        </nav>
        {running && (
          <span className="inline-flex items-center gap-1 rounded-full bg-ok/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-ok">
            <span className="h-1.5 w-1.5 rounded-full bg-ok" /> running
          </span>
        )}
        {running && isUrbanApp && servedUrl && (
          <a
            data-tour={TOUR_ANCHOR.servedApp}
            href={servedUrl}
            target="_blank"
            rel="noreferrer noopener"
            className="inline-flex items-center gap-1 rounded-md border border-edge-strong bg-bg-subtle px-2 py-0.5 text-xs font-medium text-fg hover:bg-hover"
            title="Open the running app on its own port"
          >
            Open app ↗
          </a>
        )}
        {compiling && (
          <span className="rounded-full bg-info/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-info">
            compiling…
          </span>
        )}
        {canUpdate && summary && (
          <button
            type="button"
            onClick={() => void startUpdate(toUpdateTarget(summary))}
            disabled={updateBusy}
            aria-busy={previewName === name}
            title={`A newer template is available${summary.latestVersion ? ` (v${summary.latestVersion})` : ""} — click to review and apply the update`}
            aria-label={`Update ${detail.config.displayName ?? detail.config.name} from template`}
            className="inline-flex items-center gap-1 rounded-full bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent hover:bg-accent/20 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {previewName === name ? "checking…" : "↑ update possible"}
          </button>
        )}
        <div className="flex-1" />
        {runConfigs.length > 0 && (
          <label
            className="flex items-center gap-1.5 text-xs text-fg-faint"
            title="Named run configuration from the pack — drives Run and Compile. Persists in nanobpm.project.json."
          >
            <span className="hidden sm:inline">Config:</span>
            <select
              value={activeRunConfigId ?? ""}
              onChange={(e) => void changeRunConfig(e.target.value || null)}
              disabled={running || compiling}
              className="rounded-md border border-edge-strong bg-bg-subtle px-2 py-1 text-sm text-fg hover:bg-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              {runConfigs.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.label}
                </option>
              ))}
            </select>
          </label>
        )}
        {running ? (
          <ToolbarButton onClick={() => void stop()} kind="danger">
            ■ Stop
          </ToolbarButton>
        ) : (
          <ToolbarButton
            onClick={() => void run()}
            kind="primary"
            disabled={!runnable}
            dataTour={TOUR_ANCHOR.run}
          >
            ▶ Run
          </ToolbarButton>
        )}
        <ToolbarButton
          onClick={() => void handleCompile()}
          disabled={!runnable || compiling}
        >
          Compile
        </ToolbarButton>
        {isUrbanApp && (
          <ToolbarButton
            onClick={() => {
              setActivePanel((p) => togglePanel(p, "data"));
            }}
            kind={activePanel === "data" ? "primary" : undefined}
          >
            Data
          </ToolbarButton>
        )}
        {isUrbanApp && (
          <ToolbarButton
            onClick={() => {
              setActivePanel((p) => togglePanel(p, "triggers"));
            }}
            kind={activePanel === "triggers" ? "primary" : undefined}
          >
            Triggers
          </ToolbarButton>
        )}
        {isUrbanApp && (
          <ToolbarButton
            onClick={() => {
              setActivePanel((p) => togglePanel(p, "connectors"));
            }}
            kind={activePanel === "connectors" ? "primary" : undefined}
          >
            Connectors
          </ToolbarButton>
        )}
        {isWorkflowProject && (
          <ToolbarButton
            onClick={() => {
              setActivePanel((p) => togglePanel(p, "model"));
            }}
            kind={activePanel === "model" ? "primary" : undefined}
            dataTour={TOUR_ANCHOR.modelView}
          >
            Model
          </ToolbarButton>
        )}
        <ToolbarButton onClick={() => setShowConfig(true)}>
          Configure
        </ToolbarButton>
        <ToolbarButton onClick={() => void exportProject(name, false)}>
          Export
        </ToolbarButton>
      </div>

      {/*
        "Stop the app to edit it" banner (issue #889). While the app is running,
        the server refuses every project-mutating request with 409 `app_running`
        (a single categorical chokepoint). Rather than let those calls fail, the
        studio surfaces the gate up front: a persistent banner plus disabled
        apply/deploy/migrate affordances (see the `running` prop threaded into
        the editor and the Data/Triggers/Connectors panels). One-click Stop
        re-enables editing; the user starts the app again to pick up the changes.
      */}
      {running && (
        <div className="flex items-center gap-3 border-b border-warn/30 bg-warn/10 px-4 py-1.5 text-xs text-warn">
          <span className="flex-1">
            This app is running — stop it to make changes. Edits, deploys and
            migrations are disabled until it stops.
          </span>
          <button
            type="button"
            onClick={() => void stop()}
            className="shrink-0 rounded-md border border-warn/50 px-2 py-0.5 text-xs font-medium text-warn transition-colors hover:bg-warn/20"
          >
            ■ Stop app
          </button>
        </div>
      )}

      {updateError && (
        <div className="border-b border-danger/30 bg-danger/10 px-4 py-1.5 text-xs text-danger">
          {updateError}
        </div>
      )}
      {updateModals}

      {!runnable && (
        <div className="border-b border-warn/30 bg-warn/10 px-4 py-1.5 text-xs text-warn">
          {(() => {
            const mt = detail.missingToolchain;
            const probes = mt?.probes ?? [];
            const looked =
              probes.length > 0
                ? ` Looked for ${probes
                    .map((p) => `\u201C${p}\u201D`)
                    .join(" or ")} on your PATH.`
                : "";
            const who = mt?.displayName ?? lang;
            return (
              <>
                No {who} toolchain detected — Run and Compile for this project
                need it.
                {looked}
                {mt?.installHint ? ` ${mt.installHint}` : ""} Authoring and
                Export still work.
                {mt?.installUrl && (
                  <>
                    {" "}
                    <a
                      href={mt.installUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="font-medium underline"
                    >
                      Install →
                    </a>
                  </>
                )}
              </>
            );
          })()}
        </div>
      )}

      <div className="flex min-h-0 flex-1">
        {/* File tree */}
        <FileBrowser
          name={name}
          config={detail.config}
          files={detail.files}
          rootPath={detail.rootPath}
          selected={selected}
          onSelect={openFile}
          onChanged={reloadFiles}
        />

        {/* Editor + console — or the DB Manager (Data) / Triggers panel when toggled */}
        {activePanel === "data" ? (
          <div className="flex min-w-0 flex-1 flex-col">
            <Suspense
              fallback={
                <div className="p-8 text-sm text-fg-faint">
                  Loading Data panel…
                </div>
              }
            >
              <DataPanel name={name} appRunning={running} />
            </Suspense>
          </div>
        ) : activePanel === "triggers" ? (
          <div className="flex min-w-0 flex-1 flex-col">
            <Suspense
              fallback={
                <div className="p-8 text-sm text-fg-faint">
                  Loading Triggers panel…
                </div>
              }
            >
              <TriggersPanel name={name} appRunning={running} />
            </Suspense>
          </div>
        ) : activePanel === "connectors" ? (
          <div className="flex min-w-0 flex-1 flex-col">
            <Suspense
              fallback={
                <div className="p-8 text-sm text-fg-faint">
                  Loading Connectors panel…
                </div>
              }
            >
              <ConnectorsPanel name={name} appRunning={running} />
            </Suspense>
          </div>
        ) : activePanel === "model" ? (
          <div className="flex min-w-0 flex-1 flex-col">
            <Suspense
              fallback={
                <div className="p-8 text-sm text-fg-faint">
                  Loading Model view…
                </div>
              }
            >
              <DerivedModelPanel name={name} />
            </Suspense>
          </div>
        ) : (
          <div className="flex min-w-0 flex-1 flex-col">
            <div
              data-tour={TOUR_ANCHOR.flowEditor}
              className="min-h-0 flex-1 overflow-hidden border-b border-edge"
            >
              {selected ? (
                isAppManifestPath(selected) ? (
                  <AppManifestEditor
                    key={selected}
                    name={name}
                    path={selected}
                    files={detail.files}
                  />
                ) : (
                  <EditorPane
                    key={selected}
                    name={name}
                    path={selected}
                    deployTarget={detail.config.deployTarget}
                    appRunning={running}
                  />
                )
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                  Select a file to edit
                </div>
              )}
            </div>
            <div
              onMouseDown={startDrag}
              className="h-1.5 shrink-0 cursor-row-resize bg-hover transition-colors hover:bg-accent"
              title="Drag to resize console"
            />
            <RunConsole
              name={name}
              logs={logs}
              forwardRef={logRef}
              onClear={() => setLogs([])}
              height={consoleHeight}
              dataTour={TOUR_ANCHOR.runOutput}
            />
          </div>
        )}
      </div>

      {showConfig && (
        <ConfigModal
          name={name}
          config={detail.config}
          appRunning={running}
          onClose={() => setShowConfig(false)}
          onSaved={(cfg) => {
            setDetail((d) => (d ? { ...d, config: cfg } : d));
            setShowConfig(false);
          }}
        />
      )}
    </div>
  );
}

function ToolbarButton({
  children,
  onClick,
  disabled,
  kind = "default",
  dataTour,
}: {
  children: React.ReactNode;
  onClick: () => void;
  disabled?: boolean;
  kind?: "default" | "primary" | "danger";
  dataTour?: string;
}) {
  const styles =
    kind === "primary"
      ? "bg-ok text-on-accent hover:bg-ok/85"
      : kind === "danger"
        ? "bg-danger text-on-accent hover:bg-danger/85"
        : "border border-edge-strong text-fg hover:bg-hover";
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      data-tour={dataTour}
      className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${styles}`}
    >
      {children}
    </button>
  );
}

// --- File browser ----------------------------------------------------------

function FileBrowser({
  name,
  config,
  files,
  rootPath,
  selected,
  onSelect,
  onChanged,
}: {
  name: string;
  config: ProjectConfig;
  files: FileNode[];
  rootPath: string;
  selected: string | null;
  onSelect: (path: string) => void;
  onChanged: () => void;
}) {
  const [newFileOpen, setNewFileOpen] = useState(false);
  // Persist which directories are collapsed/expanded, per project, so the tree
  // reopens as the user left it across navigation, live refetches, and reloads.
  // Divergences from the depth-based default are stored under this key; folders
  // absent from the map use the default (all folders collapsed on first open).
  // See explorerDirState.ts.
  const dirStateKey = `nano.project.${name}.explorerDirs`;
  const [dirState, setDirState] = useState<DirState>(() => {
    try {
      return loadDirState(localStorage.getItem(dirStateKey));
    } catch {
      // Reading localStorage itself can throw (blocked storage, sandboxed
      // frame); fall back to the in-memory default rather than crash the
      // workspace.
      return loadDirState(null);
    }
  });
  // Mirror the latest state in a ref so the toggle/prune handlers can compute
  // the next value and persist it *outside* the state updater — React updaters
  // must stay pure (StrictMode/concurrent may re-invoke or discard them), so
  // the localStorage write can't live inside `setDirState(...)`. Mirrors the
  // existing `consoleHeightRef` pattern in this component.
  const dirStateRef = useRef(dirState);
  dirStateRef.current = dirState;
  const persistDirState = useCallback(
    (next: DirState) => {
      try {
        localStorage.setItem(dirStateKey, serializeDirState(next));
      } catch {
        // localStorage may be unavailable/full; the in-memory state still works
        // for this session, so a write failure must not break the explorer.
      }
    },
    [dirStateKey],
  );
  const commitDirState = useCallback(
    (next: DirState) => {
      dirStateRef.current = next;
      setDirState(next);
      persistDirState(next);
    },
    [persistDirState],
  );
  const onToggleDir = useCallback(
    (path: string, depth: number) => {
      commitDirState(toggleDir(dirStateRef.current, path, depth));
    },
    [commitDirState],
  );
  // Drop overrides for directories that no longer exist (renamed/deleted), so
  // stale entries don't accumulate in localStorage. FileBrowser only renders
  // once `detail` has loaded, so an empty `files` means a genuinely empty
  // project (not "still loading") — pruning against an empty set then correctly
  // clears any leftover overrides.
  useEffect(() => {
    const pruned = pruneDirState(dirStateRef.current, collectDirPaths(files));
    if (pruned !== dirStateRef.current) commitDirState(pruned);
  }, [files, commitDirState]);
  const newFolder = async () => {
    const base = prompt("New folder path (project-relative):", "resources/");
    if (!base) return;
    try {
      await createProjectPath({
        path: { name },
        body: { path: base, dir: true },
        throwOnError: true,
      });
      onChanged();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    }
  };
  const del = async (path: string) => {
    if (!confirm(`Delete ${path}?`)) return;
    try {
      await deleteProjectPath({
        path: { name },
        query: { path },
        throwOnError: true,
      });
      onChanged();
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    }
  };

  // Right-click "copy path" menu. rootPath is the project dir on the host; the
  // path separator is inferred from it so absolute paths look native on Windows
  // (backslashes) as well as POSIX hosts.
  const [menu, setMenu] = useState<{
    x: number;
    y: number;
    node: FileNode;
  } | null>(null);
  const sep = rootPath.includes("\\") && !rootPath.includes("/") ? "\\" : "/";
  const absPathOf = (rel: string) => rootPath + sep + rel.split("/").join(sep);
  const openMenu = (e: React.MouseEvent, node: FileNode) => {
    e.preventDefault();
    setMenu({ x: e.clientX, y: e.clientY, node });
  };

  return (
    <aside
      data-tour={TOUR_ANCHOR.fileTree}
      className="flex w-64 shrink-0 flex-col border-r border-edge bg-panel"
    >
      <div className="flex items-center justify-between px-3 py-2 text-xs uppercase tracking-wider text-fg-faint">
        <span>Files</span>
        <div className="flex gap-1">
          <button
            title="New file"
            onClick={() => setNewFileOpen(true)}
            className="rounded px-1.5 py-0.5 hover:bg-hover hover:text-fg"
          >
            ＋
          </button>
          <button
            title="New folder"
            onClick={() => void newFolder()}
            className="rounded px-1.5 py-0.5 hover:bg-hover hover:text-fg"
          >
            ⊞
          </button>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto px-1 pb-2">
        <FileTree
          nodes={files}
          depth={0}
          selected={selected}
          dirState={dirState}
          onToggleDir={onToggleDir}
          onSelect={onSelect}
          onDelete={del}
          onContextMenu={openMenu}
        />
      </div>
      {/* On-disk location of the project. Lets users find their files outside
          the IDE; the copy button grabs the absolute path. */}
      <ProjectPathFooter rootPath={rootPath} />
      {menu && (
        <PathContextMenu
          x={menu.x}
          y={menu.y}
          node={menu.node}
          absPath={absPathOf(menu.node.path)}
          onClose={() => setMenu(null)}
        />
      )}
      {newFileOpen && (
        <NewFileModal
          name={name}
          config={config}
          files={files}
          onClose={() => setNewFileOpen(false)}
          onCreated={(path) => {
            onChanged();
            onSelect(path);
            setNewFileOpen(false);
          }}
        />
      )}
    </aside>
  );
}

/// Clipboard helper — resolves to true on success. Falls back to a temporary
/// textarea + execCommand for the rare browser/context without the async
/// Clipboard API (the console is served from localhost, a secure context, so
/// navigator.clipboard is normally available).
async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return ok;
    } catch {
      return false;
    }
  }
}

/// Footer of the file tree showing the project's absolute directory on the host,
/// with a one-click copy. This is the primary "where is this on disk?" affordance.
function ProjectPathFooter({ rootPath }: { rootPath: string }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    if (await copyToClipboard(rootPath)) {
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    }
  };
  return (
    <div className="flex items-center gap-1 border-t border-edge px-2 py-1.5 text-[11px] text-fg-faint">
      <span className="shrink-0 text-fg-faint" aria-hidden>
        📁
      </span>
      <span className="truncate font-mono" title={rootPath} dir="rtl">
        {rootPath}
      </span>
      <span className="flex-1" />
      <button
        onClick={() => void copy()}
        title="Copy the project's path on disk"
        className="shrink-0 rounded px-1 py-0.5 hover:bg-hover hover:text-fg"
      >
        {copied ? "✓" : "⧉"}
      </button>
    </div>
  );
}

/// Right-click menu on a file/dir node: copy its absolute path (host path) or
/// its project-relative path. A full-screen transparent backdrop closes it on
/// any outside click or right-click.
function PathContextMenu({
  x,
  y,
  node,
  absPath,
  onClose,
}: {
  x: number;
  y: number;
  node: FileNode;
  absPath: string;
  onClose: () => void;
}) {
  const act = async (text: string) => {
    await copyToClipboard(text);
    onClose();
  };
  return (
    <div
      className="fixed inset-0 z-50"
      onClick={onClose}
      onContextMenu={(e) => {
        e.preventDefault();
        onClose();
      }}
    >
      <ul
        className="absolute min-w-44 rounded-md border border-edge bg-panel py-1 text-sm shadow-lg"
        style={{ left: x, top: y }}
        onClick={(e) => e.stopPropagation()}
      >
        <li
          className="truncate px-3 py-1 text-[11px] uppercase tracking-wider text-fg-faint"
          title={node.path}
        >
          {node.name}
        </li>
        <li>
          <button
            className="block w-full px-3 py-1.5 text-left text-fg-muted hover:bg-hover hover:text-fg"
            onClick={() => void act(absPath)}
          >
            Copy path
          </button>
        </li>
        <li>
          <button
            className="block w-full px-3 py-1.5 text-left text-fg-muted hover:bg-hover hover:text-fg"
            onClick={() => void act(node.path)}
          >
            Copy relative path
          </button>
        </li>
      </ul>
    </div>
  );
}

// Fixed file kinds every project understands. The `source` kind is derived from
// the project language at render time (extension + default dir), so it isn't
// listed here.
const NEW_FILE_KINDS = [
  {
    id: "model" as const,
    label: "Model",
    hint: "BPMN process diagram",
    dir: "resources/processes",
    ext: ".bpmn",
  },
  {
    id: "decision" as const,
    label: "Decision",
    hint: "DMN decision table",
    dir: "resources/decisions",
    ext: ".dmn",
  },
  {
    id: "form" as const,
    label: "Form",
    hint: "User task form",
    dir: "resources/forms",
    ext: ".form",
  },
  {
    id: "page" as const,
    label: "Page",
    hint: "App screen (Page Composer)",
    dir: "pages",
    ext: ".page.json",
  },
];

type NewFileKindId = "model" | "decision" | "form" | "page" | "source";

const extnameOf = (p: string): string => {
  const base = p.split("/").pop() ?? "";
  const dot = base.lastIndexOf(".");
  return dot > 0 ? base.slice(dot) : "";
};

const dirnameOf = (p: string): string => {
  const slash = p.lastIndexOf("/");
  return slash >= 0 ? p.slice(0, slash) : "";
};

const hasTopLevelDir = (files: FileNode[], dir: string): boolean =>
  files.some((f) => f.kind === "dir" && f.name === dir);

/// Language-aware "New file" dialog. Offers the fixed BPMN/DMN/Form kinds plus a
/// project-language "Source" kind, each pre-filling the conventional directory
/// (`resources/processes|decisions|forms`) and extension so files land where the
/// Urban manifest and editors expect them. The location stays editable for the
/// escape-hatch cases.
function NewFileModal({
  name,
  config,
  files,
  onClose,
  onCreated,
}: {
  name: string;
  config: ProjectConfig;
  files: FileNode[];
  onClose: () => void;
  onCreated: (path: string) => void;
}) {
  // Derive the "Source" kind from the project entrypoint: its extension mirrors
  // the language (main.ts -> .ts, main.rs -> .rs) and its directory is the most
  // natural home for new source (the entrypoint's own dir, else a top-level
  // `lib`/`src`, else the project root).
  const sourceExt = extnameOf(config.main) || ".txt";
  const sourceDir = useMemo(() => {
    const d = dirnameOf(config.main);
    if (d) return d;
    for (const cand of ["lib", "src"]) {
      if (hasTopLevelDir(files, cand)) return cand;
    }
    return "";
  }, [config.main, files]);

  const kindOf = useCallback(
    (id: NewFileKindId) =>
      id === "source"
        ? {
            id,
            label: "Source",
            hint: `${config.lang} source (${sourceExt})`,
            dir: sourceDir,
            ext: sourceExt,
          }
        : NEW_FILE_KINDS.find((k) => k.id === id)!,
    [config.lang, sourceDir, sourceExt],
  );

  const kinds = useMemo(() => [...NEW_FILE_KINDS, kindOf("source")], [kindOf]);

  const [kindId, setKindId] = useState<NewFileKindId>("model");
  const [baseName, setBaseName] = useState("new");
  const [dir, setDir] = useState(NEW_FILE_KINDS[0].dir);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const active = kindOf(kindId);

  const selectKind = (id: NewFileKindId) => {
    setKindId(id);
    setDir(kindOf(id).dir);
    setError(null);
  };

  const finalPath = useMemo(() => {
    let bn = baseName.trim().replace(/^\/+|\/+$/g, "");
    if (bn.endsWith(active.ext)) bn = bn.slice(0, -active.ext.length);
    const cleanDir = dir.trim().replace(/^\/+|\/+$/g, "");
    const file = `${bn}${active.ext}`;
    return cleanDir ? `${cleanDir}/${file}` : file;
  }, [baseName, dir, active.ext]);

  const create = async () => {
    if (!baseName.trim()) {
      setError("Enter a file name.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await createProjectPath({
        path: { name },
        body: { path: finalPath, dir: false },
        throwOnError: true,
      });
      onCreated(finalPath);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="New file" onClose={onClose}>
      <div className="space-y-4">
        <div>
          <label className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
            Type
          </label>
          <div className="grid grid-cols-2 gap-2">
            {kinds.map((k) => (
              <label
                key={k.id}
                className={`flex cursor-pointer items-start gap-2 rounded-md border p-2.5 text-sm transition-colors ${
                  kindId === k.id
                    ? "border-accent bg-accent/10"
                    : "border-edge hover:bg-hover"
                }`}
              >
                <input
                  type="radio"
                  name="new-file-kind"
                  checked={kindId === k.id}
                  onChange={() => selectKind(k.id)}
                  className="mt-0.5 accent-accent"
                />
                <span className="min-w-0">
                  <span className="block font-medium text-fg">{k.label}</span>
                  <span className="block truncate text-xs text-fg-faint">
                    {k.hint}
                  </span>
                </span>
              </label>
            ))}
          </div>
        </div>
        <div className="grid grid-cols-[1fr_auto] items-end gap-2">
          <label className="block">
            <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
              Name
            </span>
            <input
              autoFocus
              value={baseName}
              onChange={(e) => setBaseName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void create();
              }}
              className={`${inputClass} w-full`}
              placeholder="new"
            />
          </label>
          <div className="rounded-md border border-edge bg-inset px-3 py-2 font-mono text-sm text-fg-muted">
            {active.ext}
          </div>
        </div>
        <label className="block">
          <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
            Location
          </span>
          <input
            value={dir}
            onChange={(e) => setDir(e.target.value)}
            className={`${inputClass} w-full`}
            placeholder="resources/processes"
          />
        </label>
        <p className="text-xs text-fg-muted">
          Creates <code className="text-fg">{finalPath}</code>
        </p>
        {error && <p className="text-sm text-danger">{error}</p>}
        <div className="flex justify-end gap-2">
          <Button variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={() => void create()}
            disabled={busy || !baseName.trim()}
          >
            {busy ? "Creating…" : "Create"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function FileTree({
  nodes,
  depth,
  selected,
  dirState,
  onToggleDir,
  onSelect,
  onDelete,
  onContextMenu,
}: {
  nodes: FileNode[];
  depth: number;
  selected: string | null;
  dirState: DirState;
  onToggleDir: (path: string, depth: number) => void;
  onSelect: (p: string) => void;
  onDelete: (p: string) => void;
  onContextMenu: (e: React.MouseEvent, node: FileNode) => void;
}) {
  return (
    <ul>
      {nodes.map((node) => (
        <TreeNode
          key={node.path}
          node={node}
          depth={depth}
          selected={selected}
          dirState={dirState}
          onToggleDir={onToggleDir}
          onSelect={onSelect}
          onDelete={onDelete}
          onContextMenu={onContextMenu}
        />
      ))}
    </ul>
  );
}

function TreeNode({
  node,
  depth,
  selected,
  dirState,
  onToggleDir,
  onSelect,
  onDelete,
  onContextMenu,
}: {
  node: FileNode;
  depth: number;
  selected: string | null;
  dirState: DirState;
  onToggleDir: (path: string, depth: number) => void;
  onSelect: (p: string) => void;
  onDelete: (p: string) => void;
  onContextMenu: (e: React.MouseEvent, node: FileNode) => void;
}) {
  const pad = { paddingLeft: `${depth * 12 + 8}px` };
  if (node.kind === "dir") {
    const open = isDirOpen(dirState, node.path, depth);
    return (
      <li>
        <div
          style={pad}
          onClick={() => onToggleDir(node.path, depth)}
          onContextMenu={(e) => onContextMenu(e, node)}
          className="group flex cursor-pointer items-center gap-1 rounded py-1 pr-2 text-sm text-fg-muted hover:bg-hover"
        >
          <span className="text-fg-faint">{open ? "▾" : "▸"}</span>
          <span className="truncate">{node.name}</span>
        </div>
        {open && node.children && (
          <FileTree
            nodes={node.children}
            depth={depth + 1}
            selected={selected}
            dirState={dirState}
            onToggleDir={onToggleDir}
            onSelect={onSelect}
            onDelete={onDelete}
            onContextMenu={onContextMenu}
          />
        )}
      </li>
    );
  }
  const active = selected === node.path;
  return (
    <li>
      <div
        style={pad}
        className={`group flex cursor-pointer items-center gap-1 rounded py-1 pr-2 text-sm ${
          active
            ? "bg-accent/10 font-medium text-accent-strong"
            : "text-fg-muted hover:bg-hover hover:text-fg"
        }`}
        onClick={() => onSelect(node.path)}
        onContextMenu={(e) => onContextMenu(e, node)}
      >
        <span className="opacity-0">▸</span>
        <span className="truncate">{node.name}</span>
        <span className="flex-1" />
        <button
          type="button"
          title={`Delete ${node.name}`}
          aria-label={`Delete ${node.name}`}
          onClick={(e) => {
            e.stopPropagation();
            onDelete(node.path);
          }}
          className="hidden rounded px-1 text-xs text-fg-faint hover:text-danger group-hover:block [@media(hover:none)]:block"
        >
          ✕
        </button>
      </div>
    </li>
  );
}

// --- Editor dispatch -------------------------------------------------------

function EditorPane({
  name,
  path,
  deployTarget,
  appRunning,
}: {
  name: string;
  path: string;
  deployTarget: string;
  appRunning: boolean;
}) {
  const [content, setContent] = useState<string | null>(null);
  const [meta, setMeta] = useState<ProjectFile | null>(null);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const bpmnRef = useRef<BpmnModelerHandle>(null);
  const dmnRef = useRef<DmnModelerHandle>(null);
  const formRef = useRef<FormEditorHandle>(null);
  // The graphical editors (BPMN/DMN/Form) are lazy-loaded, so the imperative
  // ref they expose may still be null when the fetched `content` first arrives.
  // This flag flips once the active editor has mounted and finished its initial
  // load (its `onReady`), and is a dependency of the content-import effect below
  // so the import RETRIES the moment the editor is ready. Without it, a `content`
  // that lands before the lazy chunk mounts is dropped and the canvas stays on
  // bpmn-js's blank default — the "model looks wiped on first open" bug.
  const [editorReady, setEditorReady] = useState(false);
  const pageRef = useRef<PageComposerHandle>(null);
  const [testXml, setTestXml] = useState<string | null>(null);
  // Markdown files open in a rendered Preview tab; the user can switch to Edit.
  const [mdView, setMdView] = useState<"preview" | "edit">("preview");
  // BPMN files open in the graphical modeler; the user can switch to a raw XML
  // editor to inspect/tweak the underlying document. The modeler stays mounted
  // when the XML tab is active so canvas state is preserved across toggles.
  const [bpmnView, setBpmnView] = useState<"visual" | "xml">("visual");
  const [bpmnXml, setBpmnXml] = useState<string>("");
  // Tracks whether `bpmnXml` was edited in the XML tab since it was pulled from
  // the modeler. On switch back to Visual we import it so both surfaces stay in
  // sync; on Save from the XML tab we persist `bpmnXml` directly.
  const bpmnXmlDirtyRef = useRef(false);
  // Form files open in the graphical form editor; the user can switch to a raw
  // JSON editor to inspect/tweak the underlying schema. The editor stays mounted
  // when the JSON tab is active so form state is preserved across toggles.
  const [formView, setFormView] = useState<"visual" | "json" | "preview">(
    "visual",
  );
  const [formJson, setFormJson] = useState<string>("");
  // Mirrors bpmnXmlDirtyRef for the form JSON tab.
  const formJsonDirtyRef = useRef(false);
  // Track what we most recently deployed to <deployTarget> so we can enable
  // Start Instance only when the saved XML matches deployment. Primed once
  // on load by pulling the deployed XML for the file's primary process id;
  // updated in-place after a successful Deploy.
  const [lastDeployedXml, setLastDeployedXml] = useState<string | null>(null);
  const [deploying, setDeploying] = useState(false);
  const [startModalOpen, setStartModalOpen] = useState(false);

  const ext = path.split(".").pop()?.toLowerCase() ?? "";
  const kind: "bpmn" | "dmn" | "form" | "page" | "md" | "code" = path
    .toLowerCase()
    .endsWith(".page.json")
    ? "page"
    : ext === "bpmn"
      ? "bpmn"
      : ext === "dmn"
        ? "dmn"
        : ext === "form"
          ? "form"
          : ext === "md" || ext === "markdown"
            ? "md"
            : "code";

  // Parse process ids client-side. Multi-process files are rare and use the
  // first as the primary (matches the server's deploy_status_of contract).
  const processIds = useMemo<string[]>(() => {
    if (kind !== "bpmn" || content == null) return [];
    return Array.from(
      content.matchAll(/<(?:bpmn2?:)?process\b[^>]*\bid=["']([^"']+)["']/g),
    ).map((m) => m[1]);
  }, [kind, content]);
  const primaryProcessId = processIds[0] ?? null;
  const deployedInSync =
    lastDeployedXml != null && lastDeployedXml === content && !dirty;

  // The App manifest supplies the domain type bound to each decision (ADR 0029
  // §5) and each process (ADR 0030), which scopes the DMN input-expression and
  // BPMN component-input FEEL autocomplete. Load it (root nano.app.json) while a
  // decision or process model is open; reload on file switch so recent binding
  // edits are reflected. Absent/invalid manifest → no bound variables.
  const [manifestText, setManifestText] = useState<string | null>(null);
  useEffect(() => {
    if (kind !== "dmn" && kind !== "bpmn" && kind !== "form") {
      setManifestText(null);
      return;
    }
    let alive = true;
    projectFileEx(name, "nano.app.json")
      .then((f) => alive && setManifestText(f.binary ? null : f.text))
      .catch(() => alive && setManifestText(null));
    return () => {
      alive = false;
    };
  }, [name, kind, path]);
  const manifest = useMemo<unknown>(() => {
    if (!manifestText) return undefined;
    try {
      return JSON.parse(manifestText);
    } catch {
      return undefined;
    }
  }, [manifestText]);
  // Latest manifest text, read by the domain-type binding's `set` so a service-
  // task type edit patches the current `nano.app.json` (avoids a stale closure).
  const manifestTextRef = useRef(manifestText);
  manifestTextRef.current = manifestText;
  // The components (element templates) installed for this project drive the BPMN
  // palette + template chooser (ADR 0033 increment 2). Load them from the
  // project's component dirs while a BPMN model is open; reload when the file
  // tree changes so a newly-added component file appears. Absent → empty palette.
  // The components (element templates) available for this BPMN model: the
  // project's own (`components/` + `.camunda/element-templates/`, increment 2)
  // layered over the set contributed by installed packs (ADR 0033 §4, increment
  // 6). Load both when a BPMN model is open; reload on file switch. Project
  // components win on an id collision. Absent → empty palette.
  const [components, setComponents] = useState<ElementTemplate[]>([]);
  useEffect(() => {
    if (kind !== "bpmn") {
      setComponents([]);
      return;
    }
    let alive = true;
    Promise.all([
      loadPackComponents(),
      listProjectFiles({ path: { name }, throwOnError: true }).then((res) =>
        loadProjectComponents(name, res.data.files),
      ),
      // The project's enabled connectors gate which pack connector components
      // belong in the palette (ADR 0050, amending ADR 0033 §2). A project with
      // no manifest / no connectors yields an empty enabled set, so only design-
      // only pack components (and the project's own) show.
      getConnectors({ path: { name }, throwOnError: true })
        .then((res) => res.data.connectors.map((c) => c.taskType))
        .catch(() => [] as string[]),
    ])
      .then(([pack, project, enabledTaskTypes]) => {
        if (!alive) return;
        const gatedPack = filterEnabledPackComponents(pack, enabledTaskTypes);
        setComponents(combineComponents(gatedPack, project));
      })
      .catch(() => alive && setComponents([]));
    return () => {
      alive = false;
    };
  }, [name, kind, path]);
  const dmnGetVariables = useCallback(
    (decisionId: string | undefined) =>
      decisionFeelVariables(manifest, decisionId),
    [manifest],
  );
  const bpmnGetVariables = useCallback(
    ({ taskOutputs }: { taskOutputs: ComponentOutput[] }) => [
      ...processFeelVariables(manifest, primaryProcessId ?? undefined),
      ...componentOutputFeelVariables(manifest, taskOutputs),
    ],
    [manifest, primaryProcessId],
  );
  // The declared registry type ids (manifest `types`), the options a Data
  // envelope picker offers (ADR 0033 §6).
  const domainTypeIds = useMemo<string[]>(() => {
    const types = (manifest as { types?: unknown } | undefined)?.types;
    if (!types || typeof types !== "object") return [];
    return Object.keys(types as Record<string, unknown>);
  }, [manifest]);
  // Bridges the modeler's "Create new envelope…" action (bpmn-js land) to a React
  // modal: `createDomainType()` opens the field editor and returns a promise that
  // resolves with the new type id once it is authored + persisted, or `undefined`
  // if the maker cancelled. The resolver is stashed so the modal's save/close
  // handlers can settle the awaiting modeler.
  const [envelopeEditorOpen, setEnvelopeEditorOpen] = useState(false);
  const envelopeResolverRef = useRef<((id: string | undefined) => void) | null>(
    null,
  );
  // The single in-flight open request. Re-entrant calls (double-click, a second
  // envelope picker) return this same promise instead of overwriting the
  // resolver — otherwise the first awaiter would hang forever.
  const envelopePromiseRef = useRef<Promise<string | undefined> | null>(null);
  // Composed-shape ids (`nano:shape`) share the fused-entity namespace with
  // manifest `types` server-side (ADR 0040 §10 / `resolveShapes` same-id
  // collision), and the modeler also surfaces them as selectable envelope
  // types. A new manifest type whose id collides with a shape id would drop
  // that shape from the picker (de-dupe) and break shape resolution, so the
  // editor must reject those ids up front too. `composerShapes` is only synced
  // while the shapes drawer is open, so read the ids fresh from the model at
  // open time — independent of the drawer.
  const [envelopeShapeIds, setEnvelopeShapeIds] = useState<string[]>([]);
  // When set, the envelope editor is in *edit* mode for this existing model-shape
  // type (id locked, fields pre-filled). Null → create mode. `editDomainType`
  // populates it; `closeEnvelopeEditor` clears it.
  const [envelopeEditInitial, setEnvelopeEditInitial] = useState<{
    id: string;
    fields: { name: string; type: string; optional: boolean }[];
  } | null>(null);
  const createDomainType = useCallback((): Promise<string | undefined> => {
    if (envelopePromiseRef.current) return envelopePromiseRef.current;
    setEnvelopeShapeIds((bpmnRef.current?.getShapes() ?? []).map((s) => s.id));
    const p = new Promise<string | undefined>((resolve) => {
      envelopeResolverRef.current = resolve;
      setEnvelopeEditorOpen(true);
    });
    envelopePromiseRef.current = p;
    return p;
  }, []);
  const closeEnvelopeEditor = useCallback((id: string | undefined) => {
    setEnvelopeEditorOpen(false);
    setEnvelopeEditInitial(null);
    const resolve = envelopeResolverRef.current;
    envelopeResolverRef.current = null;
    envelopePromiseRef.current = null;
    resolve?.(id);
  }, []);
  // Opens the envelope editor pre-filled for an existing model-shape type so its
  // fields can be edited in place (the picker's "Edit fields…" affordance). Only a
  // pure `extend` shape (a flat scalar field list, no composition/list ops) round-
  // trips losslessly through the flat editor; anything richer resolves `undefined`
  // (no-op) so the maker keeps using the shape composer for it.
  const editDomainType = useCallback(
    (typeId: string): Promise<string | undefined> => {
      if (envelopePromiseRef.current) return envelopePromiseRef.current;
      const shapes = bpmnRef.current?.getShapes() ?? [];
      const shape = shapes.find((s) => s.id === typeId);
      const fields = envelopeEditableFields(shape);
      // Bail if the shape isn't a flat scalar field list the editor can
      // round-trip (composition/list shapes stay in the shape composer).
      if (!fields) return Promise.resolve(undefined);
      setEnvelopeShapeIds(shapes.map((s) => s.id));
      setEnvelopeEditInitial({ id: typeId, fields });
      const p = new Promise<string | undefined>((resolve) => {
        envelopeResolverRef.current = resolve;
        setEnvelopeEditorOpen(true);
      });
      envelopePromiseRef.current = p;
      return p;
    },
    [],
  );
  // Projects a service task's chosen envelope onto its `workers[]` entry (creating
  // it if absent, clearing on ""), so the reifier keeps `defineWorker` typed while
  // the model stays the source of truth (ADR 0033 §6). Optimistic — updates the
  // in-memory manifest immediately so the modeler's FEEL scopes + the panel reflect
  // the change before the write lands.
  const setWorkerType = useCallback(
    (
      taskType: string,
      field: "inputType" | "outputType",
      value: string,
    ): void => {
      const prev = manifestTextRef.current;
      if (prev == null) return;
      let obj: Record<string, unknown>;
      try {
        const parsed = JSON.parse(prev);
        if (!parsed || typeof parsed !== "object") return;
        obj = parsed as Record<string, unknown>;
      } catch {
        return;
      }
      const workers = Array.isArray(obj.workers)
        ? (obj.workers as Record<string, unknown>[])
        : ((obj.workers = []) as Record<string, unknown>[]);
      let entry = workers.find((w) => w && w.taskType === taskType);
      if (!entry) {
        entry = { taskType };
        workers.push(entry);
      }
      if (value) entry[field] = value;
      else delete entry[field];
      const next = `${JSON.stringify(obj, null, 2)}\n`;
      setManifestText(next);
      void saveProjectFile({
        path: { name },
        query: { path: "nano.app.json" },
        body: next,
        throwOnError: true,
      }).catch(() => {
        // Best-effort persist; the optimistic in-memory manifest still reflects
        // the edit for this session.
      });
    },
    [name],
  );
  const bpmnDomainTypeBinding = useMemo<DomainTypeBinding>(
    () => ({
      enabled: manifest != null,
      typeIds: domainTypeIds,
      set: setWorkerType,
      createType: createDomainType,
      editType: editDomainType,
      // A user task's envelope defaults to the domain type bound to its linked
      // form in `bindings[]` (ADR 0033 §6 / 0029 §5).
      formType: (formId: string) => formTypeId(manifest, formId),
    }),
    [manifest, domainTypeIds, setWorkerType, createDomainType, editDomainType],
  );
  // Datasource aliases declared in the App manifest (`data.sources`), for the
  // form editor's "Data source" binding inspector (ADR 0024 §5). Recomputed when
  // the manifest reloads; the FormEditor reads it lazily via getDataSources.
  const dataSourceNames = useMemo<string[]>(() => {
    const sources = (manifest as { data?: { sources?: unknown } } | undefined)
      ?.data?.sources;
    if (!sources || typeof sources !== "object") return [];
    return Object.keys(sources as Record<string, unknown>);
  }, [manifest]);
  const formGetDataSources = useCallback(
    () => dataSourceNames,
    [dataSourceNames],
  );
  // The manifest's default datasource (`data.default`), used to resolve a
  // `data.query(sql)` call's default-source form in the form preview (ADR 0024 §5).
  const defaultDataSource = useMemo<string | undefined>(() => {
    const d = (manifest as { data?: { default?: unknown } } | undefined)?.data
      ?.default;
    return typeof d === "string" ? d : undefined;
  }, [manifest]);

  // --- Composed-shape composer (ADR 0040 §9/§10) -----------------------------
  // The composer is a right-side drawer over the BPMN canvas that edits the
  // primary process's `nano:shape` set via the modeler handle (undoable). Its
  // `shapes` are mirrored into local state so React re-renders as the maker
  // edits, and re-synced whenever the model changes (undo/redo, XML round-trip).
  const [shapesOpen, setShapesOpen] = useState(false);
  const [composerShapes, setComposerShapes] = useState<ShapeDecl[]>([]);
  const [composerMeta, setComposerMeta] = useState<MetaEntry[]>([]);
  const [composerEntities, setComposerEntities] = useState<ComposerEntity[]>(
    [],
  );
  // Process ids the `actionForm` picker offers — enumerated from the app's BPMN
  // files (ADR 0042 fuse-typed action binding). Loaded when a page is edited.
  const [pageProcessIds, setPageProcessIds] = useState<string[]>([]);

  // The fuse leaf entities the composer's pickers offer: every DB table (across
  // datasources) as a `carry`/`project` source with its columns + FK paths, and
  // every manifest `type` as a nominal `extend`/`reference` target. Loaded when
  // the drawer opens; sibling composed shapes are merged in client-side from the
  // live preview (ShapeComposer).
  const loadComposerEntities = useCallback(async () => {
    const tableEntities: ComposerEntity[] = [];
    try {
      const srcRes = await getDataSources({
        path: { name },
        throwOnError: true,
      });
      const sources = (srcRes.data.sources ?? []).map((s) => s.name);
      for (const source of sources) {
        try {
          const schema = await getDataSchema({
            path: { name, source },
            throwOnError: true,
          });
          for (const t of schema.data.tables ?? []) {
            // Qualify table ids as `source.table` — the server always indexes a
            // table under that unambiguous alias (and FK targets are stored
            // qualified), so this disambiguates same-named tables across sources
            // and lets the composer author a qualified ref. Bare names would
            // collide silently across datasources.
            tableEntities.push({
              id: `${source}.${t.name}`,
              kind: "table",
              fields: t.columns.map((c) => c.name),
              fks: t.foreignKeys.map((fk) => ({
                column: fk.column,
                refId: `${source}.${fk.refTable}`,
              })),
            });
          }
        } catch {
          // A datasource that fails to introspect just contributes no tables.
        }
      }
    } catch {
      // No datasources configured — tables simply aren't offered.
    }
    const typeEntities: ComposerEntity[] = domainTypeIds.map((id) => ({
      id,
      kind: "type",
      fields: [],
    }));
    setComposerEntities([...tableEntities, ...typeEntities]);
  }, [name, domainTypeIds]);

  // Editing a page: the composer's pickers are fed by the fuse, not by the
  // currently-open file. Load the datasource table entities (for the dataGrid
  // picker) and enumerate the app's process ids (for the actionForm picker) by
  // scanning every BPMN file — once, when a page opens.
  useEffect(() => {
    if (kind !== "page") return;
    let alive = true;
    void loadComposerEntities();
    void (async () => {
      try {
        const proj = (await getProject({ path: { name }, throwOnError: true }))
          .data;
        const bpmnPaths: string[] = [];
        const walk = (nodes: FileNode[] | undefined) => {
          for (const n of nodes ?? []) {
            if (n.kind === "dir") walk(n.children);
            else if (n.path.endsWith(".bpmn")) bpmnPaths.push(n.path);
          }
        };
        walk(proj.files);
        const ids = new Set<string>();
        for (const p of bpmnPaths) {
          try {
            const f = await projectFileEx(name, p);
            if (f.binary || !f.text) continue;
            for (const m of f.text.matchAll(
              /<(?:bpmn2?:)?process\b[^>]*\bid=["']([^"']+)["']/g,
            )) {
              ids.add(m[1]);
            }
          } catch {
            // A file that fails to read just contributes no process ids.
          }
        }
        if (alive) setPageProcessIds([...ids]);
      } catch {
        // No project/files — the picker degrades to a free-text field.
      }
    })();
    return () => {
      alive = false;
    };
  }, [kind, name, loadComposerEntities]);

  // Resolve the in-editor shapes server-side (the preview endpoint bypasses the
  // saved-model scan, so unsaved edits are reflected). `source` is nominal — the
  // CLI unions every datasource regardless of the path param.
  const previewShapes = useCallback(
    async (shapes: ShapeDecl[], meta: MetaEntry[]): Promise<ShapePreview> => {
      const source = defaultDataSource ?? dataSourceNames[0] ?? "default";
      const r = await previewDomainTypes({
        path: { name, source },
        body: { shapes, meta },
        throwOnError: true,
      });
      return { text: r.data.text, diagnostics: r.data.shapeDiagnostics ?? [] };
    },
    [name, defaultDataSource, dataSourceNames],
  );

  // Open the drawer: seed the shapes + metadata from the model and (re)load the
  // picker entities. Only meaningful for a BPMN model in an App project.
  const openShapes = useCallback(() => {
    setComposerShapes(bpmnRef.current?.getShapes() ?? []);
    setComposerMeta(bpmnRef.current?.getMeta() ?? []);
    void loadComposerEntities();
    setShapesOpen(true);
  }, [loadComposerEntities]);

  // Persist an edited shape set to the model (one undoable command) and mirror it
  // into local state so the drawer reflects the edit immediately. The write fires
  // `commandStack.changed`, so `composerWriteRef` marks it as self-inflicted for
  // `onBpmnChange` to skip the resync (the controlled React state is already
  // authoritative — a resync would `readShapes` and drop a mid-rename blank id).
  const composerWriteRef = useRef(false);
  const onShapesChange = useCallback((next: ShapeDecl[]) => {
    composerWriteRef.current = true;
    bpmnRef.current?.setShapes(next);
    setComposerShapes(next);
    setDirty(true);
  }, []);

  // Persists a new pure-motion payload type *into the model* (the "Create new
  // envelope…" affordance, ADR 0033 §6) as a composed `nano:shape` built entirely
  // from `extend` ops — the degenerate "flat record" case of the shape algebra
  // (ADR 0040 §9). Carrying the definition in the `.bpmn` (not the manifest) keeps
  // it portable: move the model to another system and the payload schema travels
  // with it, referenced nominally by the envelope. The fuse (`resolveShapes`)
  // reifies it into `DomainTypes`, so codegen and the picker see it like any other
  // registry type; `manifest.types` is a derived cache, not an authoring home.
  //
  // The write is one undoable model command (via `setShapes`) and marks the buffer
  // dirty — the new type and the envelope selection that follows it persist
  // together on the next Save, exactly like the composer. Existing shape ids are
  // left untouched: a collision is rejected by the editor up front and, defensively,
  // re-checked here so a stale id set can't clobber an existing shape.
  const writeModelEnvelopeType = useCallback(
    async (
      id: string,
      fields: { name: string; type: string; optional?: boolean }[],
      replaceId?: string,
    ): Promise<void> => {
      // Fail fast rather than silently no-op: the modal closes and the envelope
      // selection references this id the moment this resolves, so a swallowed
      // write would leave the UI claiming a type the model never gained. The
      // modal's onSave catches a throw, surfaces it, and keeps itself open.
      const bpmn = bpmnRef.current;
      if (!bpmn) {
        throw new Error(
          "The BPMN modeler isn't ready yet — reopen the diagram and try again.",
        );
      }
      const existing = bpmn.getShapes();
      // In edit mode (`replaceId`) the id is expected to already exist — it's the
      // shape being rewritten. Only a *different* shape sharing the id collides.
      if (existing.some((s) => s.id === id && s.id !== replaceId)) {
        throw new Error(`Type "${id}" already exists in the model.`);
      }
      const ops: ShapeOp[] = fields
        .filter((f) => f.name.trim())
        .map((f) => {
          const op: ShapeOp = {
            op: "extend",
            name: f.name.trim(),
            type: f.type,
          };
          if (f.optional) op.optional = true;
          return op;
        });
      // Edit mode rewrites the target shape's ops in place (preserving author
      // order); create mode appends a new shape.
      const next: ShapeDecl[] = replaceId
        ? existing.map((s) => (s.id === replaceId ? { id, ops } : s))
        : [...existing, { id, ops }];
      // Server-fuse pre-check: the up-front editor guard only knows model shapes
      // and manifest `types`, but a new id can also collide with a *fused leaf
      // entity* (e.g. a datasource table). `resolveShapes` reports that as an
      // `error`-severity `same-id-collision` and omits the shape, which would
      // leave the model referencing a payload type that never reifies into
      // `DomainTypes`. Reject with the server's diagnostic before committing.
      // A preview transport failure is non-fatal — fall through to the write +
      // verify below (no worse than the prior behaviour).
      const preview = await previewShapes(next, bpmn.getMeta()).catch(
        () => null,
      );
      const blocking = preview?.diagnostics.find(
        (d) => d.shape === id && d.severity === "error",
      );
      if (blocking) {
        throw new Error(blocking.message);
      }
      composerWriteRef.current = true;
      bpmn.setShapes(next);
      // Verify the undoable write actually landed (setShapes swallows a
      // not-ready/disposed modeler and a moddle failure). Re-clear the
      // self-inflicted-change guard on the failure path so a later genuine
      // external change isn't wrongly skipped.
      if (!bpmn.getShapes().some((s) => s.id === id)) {
        composerWriteRef.current = false;
        throw new Error(
          `Couldn't write type "${id}" to the model — the diagram may still be loading.`,
        );
      }
      if (shapesOpen) setComposerShapes(next);
      setDirty(true);
    },
    [shapesOpen, previewShapes],
  );

  // Persist edited model-level metadata to the model (one undoable command),
  // mirrored into local state. Same self-inflicted-change guard as the shapes.
  const onMetaChange = useCallback((next: MetaEntry[]) => {
    composerWriteRef.current = true;
    bpmnRef.current?.setMeta(next);
    setComposerMeta(next);
    setDirty(true);
  }, []);

  // Re-read shapes + metadata from the model on any *external* diagram change while
  // the drawer is open, so undo/redo and XML-tab round-trips keep the composer in
  // sync. A change from our own `setShapes`/`setMeta` is skipped (controlled React
  // state already holds it), so clearing an input mid-edit can't momentarily drop it.
  const onBpmnChange = useCallback(() => {
    setDirty(true);
    if (composerWriteRef.current) {
      composerWriteRef.current = false;
      return;
    }
    if (shapesOpen) {
      setComposerShapes(bpmnRef.current?.getShapes() ?? []);
      setComposerMeta(bpmnRef.current?.getMeta() ?? []);
    }
  }, [shapesOpen]);

  // Close the drawer when leaving the BPMN model (file switch / not a manifest).
  useEffect(() => {
    if (kind !== "bpmn" || manifest == null) setShapesOpen(false);
  }, [kind, manifest, path]);

  useEffect(() => {
    let alive = true;
    setContent(null);
    setMeta(null);
    setDirty(false);
    setLoadError(null);
    setMdView("preview");
    setBpmnView("visual");
    setBpmnXml("");
    bpmnXmlDirtyRef.current = false;
    setFormView("visual");
    setFormJson("");
    formJsonDirtyRef.current = false;
    projectFileEx(name, path)
      .then((f) => {
        if (!alive) return;
        setMeta(f);
        setContent(f.binary ? null : f.text);
      })
      .catch(
        (e) =>
          alive && setLoadError(e instanceof Error ? e.message : String(e)),
      );
    return () => {
      alive = false;
    };
  }, [name, path]);

  // Load the fetched document into the graphical editor once mounted. A newly
  // created (empty) model file is seeded with a valid blank document via the
  // editor's createBlank() — importing empty/`{}` would fail to load — and
  // marked dirty so a Save persists the seeded document.
  useEffect(() => {
    if (content == null) return;
    const isEmpty = content.trim() === "";
    const seeded = () => setDirty(true);
    if (kind === "bpmn" && bpmnRef.current) {
      const ed = bpmnRef.current;
      void (
        isEmpty ? ed.createBlank().then(seeded) : ed.importXml(content)
      ).catch(() => void 0);
    } else if (kind === "dmn" && dmnRef.current) {
      const ed = dmnRef.current;
      void (
        isEmpty ? ed.createBlank().then(seeded) : ed.importXml(content)
      ).catch(() => void 0);
    } else if (kind === "form" && formRef.current) {
      const ed = formRef.current;
      void (
        isEmpty ? ed.createBlank().then(seeded) : ed.importSchema(content)
      ).catch(() => void 0);
    } else if (kind === "page" && pageRef.current) {
      const res = pageRef.current.setPageJson(content);
      if (!res.ok) {
        // The page uses a shape this Console build can't parse (e.g. a newer node
        // type). Surface it read-only instead of loading a blank editable canvas —
        // a blank + Save would overwrite the real file. The file on disk is intact.
        setLoadError(
          `This page can't be opened in the Page Composer:\n\n${res.errors.join("\n")}\n\n` +
            `Your file on disk is unchanged. This usually means the page uses a newer ` +
            `component than this Console build supports — update the Console, or edit the ` +
            `page.json directly.`,
        );
      }
    }
    // Only when the document first arrives for this path — or once the lazy
    // editor becomes ready, whichever is later (`editorReady` guarantees the
    // ref above is populated so the import isn't silently dropped).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [content, kind, editorReady]);

  const save = useCallback(async () => {
    // Belt-and-braces with the disabled Save button: while the app is running
    // the server refuses the write (409 `app_running`), so short-circuit every
    // save path (button, Cmd/Ctrl+S, graphical-editor onSave) up front.
    if (appRunning) return;
    setSaving(true);
    try {
      let body = content ?? "";
      if (kind === "bpmn" && bpmnView === "xml") body = bpmnXml;
      else if (kind === "bpmn" && bpmnRef.current)
        body = await bpmnRef.current.getXml();
      else if (kind === "dmn" && dmnRef.current)
        body = await dmnRef.current.getXml();
      else if (
        kind === "form" &&
        (formView === "json" || formView === "preview")
      )
        body = formJson;
      else if (kind === "form" && formRef.current)
        body = await formRef.current.getSchema();
      else if (kind === "page" && pageRef.current)
        body = pageRef.current.getPageJson();
      await saveProjectFile({
        path: { name },
        query: { path },
        body,
        throwOnError: true,
      });
      setContent(body);
      setDirty(false);
      if (kind === "bpmn") {
        // Persisted body is now authoritative in both surfaces.
        setBpmnXml(body);
        bpmnXmlDirtyRef.current = false;
        // If the save came from the XML tab, the canvas still holds the
        // pre-save document. Push the saved XML into the modeler so a
        // subsequent switch to Visual shows the up-to-date diagram
        // (the switch guards on bpmnXmlDirtyRef and would otherwise skip
        // importXml, leaving the canvas stale).
        if (bpmnView === "xml" && bpmnRef.current) {
          try {
            await bpmnRef.current.importXml(body);
          } catch {
            // Invalid XML shouldn't normally reach here (Save with bad
            // XML is on the user), but don't turn a successful persist
            // into a hard error.
          }
        }
      }
      if (kind === "form") {
        // Persisted body is now authoritative in both surfaces (mirrors bpmn).
        setFormJson(body);
        formJsonDirtyRef.current = false;
        if (
          (formView === "json" || formView === "preview") &&
          formRef.current
        ) {
          try {
            await formRef.current.importSchema(body);
          } catch {
            // Invalid JSON shouldn't normally reach here (Save with bad
            // JSON is on the user), but don't turn a successful persist
            // into a hard error.
          }
        }
      }
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [
    content,
    kind,
    name,
    path,
    bpmnView,
    bpmnXml,
    formView,
    formJson,
    appRunning,
  ]);

  // Prime lastDeployedXml on load — probes <deployTarget> for the currently
  // deployed BPMN of primaryProcessId. Silent on failure (network down, no
  // deployment yet) — the Start Instance button just stays disabled until
  // the user clicks Deploy.
  useEffect(() => {
    if (kind !== "bpmn" || !primaryProcessId) {
      setLastDeployedXml(null);
      return;
    }
    let alive = true;
    debug("modeler", "info", `priming deployed XML for '${primaryProcessId}'`, {
      file: path,
      deployTarget,
    });
    void fetchDeployedXmlByProcessId(primaryProcessId, deployTarget).then(
      (xml) => {
        if (!alive) return;
        setLastDeployedXml(xml);
        if (xml != null) {
          debug(
            "modeler",
            "ok",
            `primed lastDeployedXml (${xml.length} bytes); Start Instance ${xml === content ? "enabled" : "stays disabled — saved XML differs from deployed"}`,
          );
        }
      },
    );
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind, primaryProcessId, deployTarget]);

  const deploy = useCallback(async () => {
    if (content == null) return;
    setDeploying(true);
    const modelName =
      path
        .split("/")
        .pop()
        ?.replace(/\.bpmn$/i, "") || name;
    debug("modeler", "info", `deploy '${modelName}' clicked`, {
      file: path,
      bytes: content.length,
      primaryProcessId,
    });
    try {
      await deployXml(modelName, content, deployTarget);
      setLastDeployedXml(content);
      debug(
        "modeler",
        "ok",
        `deployed; Start Instance is now enabled for '${primaryProcessId ?? "(unknown)"}'`,
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      debug("modeler", "error", `deploy failed: ${msg}`);
      alert(
        `Deploy failed: ${msg}\n\nSee the Debug tab for the request URL and diagnostic hints.`,
      );
    } finally {
      setDeploying(false);
    }
  }, [content, deployTarget, name, path, primaryProcessId]);

  // Switches the BPMN editor between the graphical canvas and the raw XML tab.
  // Visual → XML: pull the current serialized document from the modeler.
  // XML → Visual: if the XML was edited in the textarea, import it back so the
  // canvas reflects the edits (any import error surfaces via alert so the user
  // can fix the XML rather than silently losing changes).
  const switchBpmnView = useCallback(
    async (next: "visual" | "xml") => {
      if (next === bpmnView) return;
      if (next === "xml") {
        const xml = (await bpmnRef.current?.getXml()) ?? "";
        setBpmnXml(xml);
        bpmnXmlDirtyRef.current = false;
        setBpmnView("xml");
      } else {
        if (bpmnXmlDirtyRef.current && bpmnRef.current) {
          try {
            await bpmnRef.current.importXml(bpmnXml);
            bpmnXmlDirtyRef.current = false;
          } catch (e) {
            alert(
              `Could not import XML: ${e instanceof Error ? e.message : String(e)}`,
            );
            return;
          }
        }
        setBpmnView("visual");
      }
    },
    [bpmnView, bpmnXml],
  );

  // Switches the form editor between the graphical canvas, the raw JSON tab, and
  // the live Preview (form-js viewer with datasource-resolved options, ADR 0024
  // §5). Leaving the canvas snapshots its schema into `formJson` so JSON and
  // Preview reflect unsaved canvas edits; returning to the canvas re-imports any
  // JSON edits (errors surface via alert).
  const switchFormView = useCallback(
    async (next: "visual" | "json" | "preview") => {
      if (next === formView) return;
      if (formView === "visual") {
        const json = (await formRef.current?.getSchema()) ?? "";
        setFormJson(json);
        formJsonDirtyRef.current = false;
      }
      if (next === "visual") {
        if (formJsonDirtyRef.current && formRef.current) {
          try {
            await formRef.current.importSchema(formJson);
            formJsonDirtyRef.current = false;
          } catch (e) {
            alert(
              `Could not import form JSON: ${e instanceof Error ? e.message : String(e)}`,
            );
            return;
          }
        }
      }
      setFormView(next);
    },
    [formView, formJson],
  );

  // Cmd/Ctrl+S saves graphical editors too (CodeEditor has its own binding).
  useEffect(() => {
    if (kind === "code") return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        void save();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [kind, save]);

  if (loadError) {
    return (
      <div className="p-6 text-sm text-danger whitespace-pre-wrap">
        {loadError}
      </div>
    );
  }
  if (meta == null) {
    return <div className="p-6 text-sm text-fg-faint">Loading {path}…</div>;
  }
  if (meta.binary) {
    const mb = (meta.size / (1024 * 1024)).toFixed(2);
    return (
      <div className="flex h-full flex-col">
        <div className="flex items-center gap-3 border-b border-edge bg-panel px-3 py-1.5">
          <span className="truncate font-mono text-xs text-fg-muted">
            {path}
          </span>
        </div>
        <div className="flex min-h-0 flex-1 items-center justify-center p-6">
          <p className="max-w-2xl break-words text-center font-mono text-sm leading-relaxed text-fg-muted">
            Binary file. Path on disk: {meta.absPath}. Size: {meta.size} ({mb}
            MB)
          </p>
        </div>
      </div>
    );
  }
  if (content == null) {
    return <div className="p-6 text-sm text-fg-faint">Loading {path}…</div>;
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-3 py-1.5">
        <span className="truncate font-mono text-xs text-fg-muted">{path}</span>
        {dirty && <span className="text-[10px] text-warn">● unsaved</span>}
        <div className="flex-1" />
        {kind === "md" && (
          <div className="flex overflow-hidden rounded-md border border-edge-strong">
            {(["preview", "edit"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => setMdView(mode)}
                className={`px-3 py-1 text-xs font-medium capitalize transition-colors ${
                  mdView === mode
                    ? "bg-accent text-on-accent"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        )}
        {kind === "bpmn" && (
          <div className="flex overflow-hidden rounded-md border border-edge-strong">
            {(["visual", "xml"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => void switchBpmnView(mode)}
                className={`px-3 py-1 text-xs font-medium uppercase transition-colors ${
                  bpmnView === mode
                    ? "bg-accent text-on-accent"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        )}
        {kind === "form" && (
          <div className="flex overflow-hidden rounded-md border border-edge-strong">
            {(["visual", "json", "preview"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => void switchFormView(mode)}
                className={`px-3 py-1 text-xs font-medium uppercase transition-colors ${
                  formView === mode
                    ? "bg-accent text-on-accent"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        )}
        {kind === "bpmn" && (
          <>
            <button
              onClick={() => void deploy()}
              disabled={deploying || dirty || !primaryProcessId || appRunning}
              title={
                appRunning
                  ? "Stop the app before deploying"
                  : !primaryProcessId
                    ? "The BPMN file has no <process id=...>"
                    : dirty
                      ? "Save first"
                      : `Deploy to ${deployTarget}`
              }
              className="rounded-md border border-edge-strong px-3 py-1 text-xs font-medium text-fg transition-colors hover:border-accent hover:text-accent-strong disabled:opacity-40"
            >
              {deploying
                ? "Deploying…"
                : deployedInSync
                  ? "Deploy ✓"
                  : "Deploy"}
            </button>
            <button
              onClick={() => setStartModalOpen(true)}
              disabled={!deployedInSync || !primaryProcessId}
              title={
                !primaryProcessId
                  ? "The BPMN file has no <process id=...>"
                  : !deployedInSync
                    ? "Deploy the current model first"
                    : `Start an instance of ${primaryProcessId}`
              }
              className="rounded-md border border-edge-strong px-3 py-1 text-xs font-medium text-fg transition-colors hover:border-accent hover:text-accent-strong disabled:opacity-40"
            >
              Start instance
            </button>
          </>
        )}
        {kind === "bpmn" && (
          <button
            onClick={async () => {
              // Pull from whichever surface currently holds the latest doc.
              const xml =
                bpmnView === "xml"
                  ? bpmnXml
                  : ((await bpmnRef.current?.getXml()) ?? "");
              if (xml) setTestXml(xml);
            }}
            className="rounded-md border border-edge-strong px-3 py-1 text-xs font-medium text-fg transition-colors hover:border-ok hover:text-ok"
          >
            Test
          </button>
        )}
        {kind === "bpmn" && manifest != null && (
          <button
            onClick={() => (shapesOpen ? setShapesOpen(false) : openShapes())}
            title="Author composed motion shapes (ADR 0040)"
            className={`rounded-md border px-3 py-1 text-xs font-medium transition-colors ${
              shapesOpen
                ? "border-accent text-accent-strong"
                : "border-edge-strong text-fg hover:border-accent hover:text-accent-strong"
            }`}
          >
            Shapes
          </button>
        )}
        <button
          onClick={() => void save()}
          disabled={saving || !dirty || appRunning}
          title={appRunning ? "Stop the app before saving changes" : undefined}
          className="rounded-md bg-accent px-3 py-1 text-xs font-medium text-on-accent transition-colors hover:bg-accent-strong disabled:opacity-40"
        >
          {saving ? "Saving…" : "Save"}
        </button>
      </div>
      <div className="min-h-0 flex-1">
        {kind === "bpmn" && (
          <div className="relative h-full">
            {/*
              Keep the modeler mounted while the XML tab is active so canvas
              state (selection, viewport, in-flight edits) survives toggling.
              The XML editor is layered above via absolute positioning.
            */}
            <div
              className={bpmnView === "visual" ? "h-full" : "h-full invisible"}
            >
              <Suspense fallback={modelerFallback}>
                <BpmnModeler
                  ref={bpmnRef}
                  onReady={() => setEditorReady(true)}
                  onChange={onBpmnChange}
                  getVariables={bpmnGetVariables}
                  components={components}
                  domainTypeBinding={bpmnDomainTypeBinding}
                />
              </Suspense>
            </div>
            {bpmnView === "xml" && (
              <div className="absolute inset-0 bg-app">
                <CodeEditor
                  value={bpmnXml}
                  language="xml"
                  path={`file:///${name}/${path}`}
                  onChange={(v) => {
                    setBpmnXml(v);
                    bpmnXmlDirtyRef.current = true;
                    setDirty(true);
                  }}
                  onSave={() => void save()}
                />
              </div>
            )}
            {testXml && (
              <div className="absolute inset-0 z-10 bg-app">
                <Suspense
                  fallback={
                    <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                      Loading the in-browser engine…
                    </div>
                  }
                >
                  <TestRunPanel
                    xml={testXml}
                    onClose={() => setTestXml(null)}
                  />
                </Suspense>
              </div>
            )}
            {shapesOpen && (
              <div className="absolute inset-y-0 right-0 z-20 w-[36rem] max-w-full border-l border-edge shadow-xl">
                <ShapeComposer
                  shapes={composerShapes}
                  meta={composerMeta}
                  entities={composerEntities}
                  onShapesChange={onShapesChange}
                  onMetaChange={onMetaChange}
                  preview={previewShapes}
                  onClose={() => setShapesOpen(false)}
                />
              </div>
            )}
            {startModalOpen && primaryProcessId && (
              <StartInstanceModal
                processId={primaryProcessId}
                deployTarget={deployTarget}
                onClose={() => setStartModalOpen(false)}
              />
            )}
            {envelopeEditorOpen && (
              <EnvelopeEditorModal
                existingIds={[
                  ...new Set([...domainTypeIds, ...envelopeShapeIds]),
                ]}
                initial={envelopeEditInitial ?? undefined}
                onCancel={() => closeEnvelopeEditor(undefined)}
                onSave={async (id, fields) => {
                  await writeModelEnvelopeType(
                    id,
                    fields,
                    envelopeEditInitial?.id,
                  );
                  closeEnvelopeEditor(id);
                }}
              />
            )}
          </div>
        )}
        {kind === "dmn" && (
          <Suspense fallback={modelerFallback}>
            <DmnModeler
              ref={dmnRef}
              onReady={() => setEditorReady(true)}
              onChange={() => setDirty(true)}
              getVariables={dmnGetVariables}
            />
          </Suspense>
        )}
        {kind === "page" && (
          <div data-tour={TOUR_ANCHOR.pageEditor} className="h-full">
            <PageComposer
              ref={pageRef}
              onChange={() => setDirty(true)}
              entities={composerEntities}
              processes={pageProcessIds}
            />
          </div>
        )}
        {kind === "form" && (
          <div data-tour={TOUR_ANCHOR.pageEditor} className="relative h-full">
            {/*
              Keep the form editor mounted while the JSON tab is active so schema
              state survives toggling. The JSON editor is layered above via
              absolute positioning (mirrors the BPMN visual/xml split).
            */}
            <div
              className={formView === "visual" ? "h-full" : "h-full invisible"}
            >
              <Suspense fallback={modelerFallback}>
                <FormEditor
                  ref={formRef}
                  onReady={() => setEditorReady(true)}
                  onChange={() => setDirty(true)}
                  getDataSources={formGetDataSources}
                />
              </Suspense>
            </div>
            {formView === "json" && (
              <div className="absolute inset-0 bg-app">
                <CodeEditor
                  value={formJson}
                  language="json"
                  path={`file:///${name}/${path}`}
                  onChange={(v) => {
                    setFormJson(v);
                    formJsonDirtyRef.current = true;
                    setDirty(true);
                  }}
                  onSave={() => void save()}
                />
              </div>
            )}
            {formView === "preview" && (
              <div className="absolute inset-0 bg-app">
                <Suspense fallback={modelerFallback}>
                  <FormPreview
                    schema={formJson}
                    name={name}
                    defaultSource={defaultDataSource}
                  />
                </Suspense>
              </div>
            )}
          </div>
        )}
        {kind === "md" &&
          (mdView === "preview" ? (
            <MarkdownPreview source={content} />
          ) : (
            <CodeEditor
              value={content}
              language="markdown"
              path={`file:///${name}/${path}`}
              onChange={(v) => {
                setContent(v);
                setDirty(true);
              }}
              onSave={() => void save()}
            />
          ))}
        {kind === "code" && (
          <CodeEditor
            value={content}
            language={languageForFile(path)}
            path={`file:///${name}/${path}`}
            sdkProject={name}
            onChange={(v) => {
              setContent(v);
              setDirty(true);
            }}
            onSave={() => void save()}
          />
        )}
      </div>
    </div>
  );
}

// --- Run console -----------------------------------------------------------

function RunConsole({
  name,
  logs,
  forwardRef,
  onClear,
  height,
  dataTour,
}: {
  name: string;
  logs: ProjectLogLine[];
  forwardRef: React.RefObject<HTMLDivElement>;
  onClear: () => void;
  height: number;
  dataTour?: string;
}) {
  const [tab, setTab] = useState<"output" | "debug" | "terminal">("output");
  const [debugLog, setDebugLog] = useState<DebugEntry[]>(() => debugSnapshot());
  const debugRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const unsub = subscribeDebug((entry) => {
      setDebugLog((prev) => {
        const next = prev.length >= 500 ? prev.slice(-499) : prev.slice();
        next.push(entry);
        return next;
      });
    });
    return unsub;
  }, []);

  useEffect(() => {
    if (tab === "debug") debugRef.current?.scrollTo({ top: 1e9 });
  }, [debugLog, tab]);

  const tabCls = (active: boolean) =>
    `border-b-2 px-3 py-1 text-xs uppercase tracking-wider transition-colors ${
      active
        ? "border-accent text-fg"
        : "border-transparent text-fg-faint hover:text-fg-muted"
    }`;

  return (
    <div
      data-tour={dataTour}
      className="flex shrink-0 flex-col bg-inset"
      style={{ height }}
    >
      <div className="flex items-center justify-between border-b border-edge pr-3">
        <div className="flex">
          <button
            className={tabCls(tab === "output")}
            onClick={() => setTab("output")}
          >
            Output
          </button>
          <button
            className={tabCls(tab === "debug")}
            onClick={() => setTab("debug")}
          >
            Debug{debugLog.length ? ` · ${debugLog.length}` : ""}
          </button>
          <button
            className={tabCls(tab === "terminal")}
            onClick={() => setTab("terminal")}
          >
            Terminal
          </button>
        </div>
        {tab !== "terminal" && (
          <button
            onClick={() => {
              if (tab === "output") onClear();
              else {
                clearDebug();
                setDebugLog([]);
              }
            }}
            className="rounded px-1.5 py-0.5 text-xs uppercase tracking-wider text-fg-faint hover:bg-hover hover:text-fg-muted"
          >
            Clear
          </button>
        )}
      </div>
      {tab === "output" ? (
        <div
          ref={forwardRef}
          className="min-h-0 flex-1 overflow-auto px-3 py-2 font-mono text-xs leading-relaxed"
        >
          {logs.length === 0 ? (
            <div className="text-fg-faint">
              No output yet. Run the application to see logs.
            </div>
          ) : (
            logs.map((l, i) => (
              <div
                key={i}
                className={
                  l.stream === "err"
                    ? "whitespace-pre-wrap text-danger"
                    : l.stream === "sys"
                      ? "whitespace-pre-wrap text-info"
                      : "whitespace-pre-wrap text-fg-muted"
                }
              >
                {l.text}
              </div>
            ))
          )}
        </div>
      ) : tab === "debug" ? (
        <div
          ref={debugRef}
          className="min-h-0 flex-1 overflow-auto px-3 py-2 font-mono text-xs leading-relaxed"
        >
          {debugLog.length === 0 ? (
            <div className="text-fg-faint">
              No debug traces yet. Deploy, Start Instance and probe requests log
              their URL, response, and timing here.
            </div>
          ) : (
            debugLog.map((e) => <DebugRow key={e.id} entry={e} />)
          )}
        </div>
      ) : null}
      {/* Terminal stays mounted across every bottom-panel tab so its shell
          session survives a tab switch (#504); it renders zero-footprint
          (display:none) when the terminal tab isn't selected. */}
      <TerminalPane name={name} active={tab === "terminal"} />
    </div>
  );
}

function DebugRow({ entry }: { entry: DebugEntry }) {
  const [open, setOpen] = useState(false);
  const hasDetail = entry.detail && Object.keys(entry.detail).length > 0;
  const color =
    entry.level === "error"
      ? "text-danger"
      : entry.level === "warn"
        ? "text-warn"
        : entry.level === "ok"
          ? "text-ok"
          : "text-fg-muted";
  const time = new Date(entry.ts).toISOString().slice(11, 23);
  return (
    <div className="whitespace-pre-wrap">
      <button
        onClick={() => hasDetail && setOpen((o) => !o)}
        className={`w-full text-left ${color} ${hasDetail ? "cursor-pointer hover:bg-hover" : "cursor-default"}`}
      >
        <span className="text-fg-faint">{time}</span>{" "}
        <span className="text-fg-faint">[{entry.scope}]</span>{" "}
        {hasDetail ? (open ? "▾ " : "▸ ") : "  "}
        {entry.message}
      </button>
      {open && hasDetail && (
        <pre className="ml-8 border-l-2 border-edge px-2 text-fg-muted">
          {JSON.stringify(entry.detail, null, 2)}
        </pre>
      )}
    </div>
  );
}

// --- Configure modal -------------------------------------------------------

function ConfigModal({
  name,
  config,
  appRunning,
  onClose,
  onSaved,
}: {
  name: string;
  config: ProjectConfig;
  /** Mirrors the server's 409 `app_running` guard: config is read-only while
   * the app runs (issue #1350, same gate as the file editor's Save). */
  appRunning: boolean;
  onClose: () => void;
  onSaved: (cfg: ProjectConfig) => void;
}) {
  const [deployTarget, setDeployTarget] = useState(config.deployTarget);
  const [main, setMain] = useState(config.main);
  const [desc, setDesc] = useState(config.description);
  // Env editor state: array of {key,value} rows, kept ordered so that adding
  // a fresh empty row doesn't reshuffle the user's typing. We serialise back
  // to an object on save, dropping rows with an empty key.
  const [envRows, setEnvRows] = useState<Array<{ key: string; value: string }>>(
    () =>
      Object.entries(config.env ?? {})
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, value]) => ({ key, value })),
  );
  const [busy, setBusy] = useState(false);

  const addEnvRow = () =>
    setEnvRows((rows) => [...rows, { key: "", value: "" }]);
  const removeEnvRow = (idx: number) =>
    setEnvRows((rows) => rows.filter((_, i) => i !== idx));
  const setEnvRow = (
    idx: number,
    patch: Partial<{ key: string; value: string }>,
  ) =>
    setEnvRows((rows) =>
      rows.map((r, i) => (i === idx ? { ...r, ...patch } : r)),
    );

  const save = async () => {
    if (appRunning) return;
    setBusy(true);
    try {
      const env: Record<string, string> = {};
      for (const { key, value } of envRows) {
        const k = key.trim();
        if (k) env[k] = value;
      }
      const cfg = (
        await saveProjectConfig({
          path: { name },
          body: {
            ...config,
            description: desc,
            deployTarget,
            main,
            env,
          },
          throwOnError: true,
        })
      ).data;
      onSaved(cfg);
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="Configure project" onClose={onClose}>
      <label className="block text-xs uppercase tracking-wider text-fg-faint">
        Description
      </label>
      <input
        value={desc}
        onChange={(e) => setDesc(e.target.value)}
        className={inputCls}
      />
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">
        Deploy target
      </label>
      <input
        value={deployTarget}
        onChange={(e) => setDeployTarget(e.target.value)}
        className={inputCls}
        placeholder="http://localhost:8080"
      />
      <p className="mt-1 text-[11px] text-fg-faint">
        REST API at &lt;target&gt;/v2; the Falcon protocol is dialled here too.
      </p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">
        Entry point
      </label>
      <input
        value={main}
        onChange={(e) => setMain(e.target.value)}
        className={inputCls}
        placeholder="main.ts"
      />
      <div className="mt-3 flex items-center justify-between">
        <label className="block text-xs uppercase tracking-wider text-fg-faint">
          Environment variables
        </label>
        <button
          type="button"
          onClick={addEnvRow}
          className="text-[11px] text-accent hover:underline"
        >
          + Add
        </button>
      </div>
      <p className="mt-1 text-[11px] text-fg-faint">
        Passed to every Run/Compile spawn. Run-config env overrides these.
        Example:{" "}
        <span className="font-mono">
          CAMUNDA_REST_ADDRESS=http://localhost:8081
        </span>
        .
      </p>
      {envRows.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg-faint italic">
          No env vars set.
        </p>
      ) : (
        <div className="mt-2 space-y-1">
          {envRows.map((row, i) => (
            <div key={i} className="flex gap-2">
              <input
                value={row.key}
                onChange={(e) => setEnvRow(i, { key: e.target.value })}
                placeholder="KEY"
                className={`${inputClass} font-mono w-2/5`}
              />
              <input
                value={row.value}
                onChange={(e) => setEnvRow(i, { value: e.target.value })}
                placeholder="value"
                className={`${inputClass} font-mono min-w-0 flex-1`}
              />
              <button
                type="button"
                onClick={() => removeEnvRow(i)}
                aria-label="Remove"
                className="px-2 text-fg-faint hover:text-fg"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}
      {appRunning && (
        <p className="mt-4 text-xs text-fg-faint" role="status">
          Stop the app before changing its configuration. The app reads its
          environment when it starts.
        </p>
      )}
      <div className="mt-5 flex justify-end gap-2">
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="primary"
          onClick={() => void save()}
          disabled={busy || appRunning}
          title={appRunning ? "Stop the app before saving changes" : undefined}
        >
          {busy ? "Saving…" : "Save"}
        </Button>
      </div>
    </Modal>
  );
}

const IDENT_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

/// Field-authoring surface for a transient domain type (an "envelope"): the maker
/// names the type and declares its fields (name + scalar type + optional), which
/// are written *into the model* as a composed `nano:shape` (all-`extend` ops — the
/// flat-record case of the shape algebra, ADR 0040 §9). Opened from the modeler's
/// "Create new envelope…" affordance; on save the parent writes the shape to the
/// `.bpmn` and the modeler picks the new id in one undoable gesture, so the payload
/// schema travels with the model. Field `type` is restricted to `SCALAR_KEYWORDS`
/// here — nominal references to other registry types stay an escape hatch for the
/// shape composer / raw JSON editor.
function EnvelopeEditorModal({
  existingIds,
  initial,
  onSave,
  onCancel,
}: {
  existingIds: string[];
  // When provided, the modal edits this existing model-shape type in place: the
  // id is locked and the fields are pre-filled. Absent → create a new type.
  initial?: {
    id: string;
    fields: { name: string; type: string; optional: boolean }[];
  };
  onSave: (
    id: string,
    fields: { name: string; type: string; optional?: boolean }[],
  ) => Promise<void>;
  onCancel: () => void;
}) {
  const isEdit = initial != null;
  const [id, setId] = useState(initial?.id ?? "");
  const [fields, setFields] = useState<
    { key: number; name: string; type: string; optional: boolean }[]
  >(
    initial && initial.fields.length > 0
      ? initial.fields.map((f, i) => ({ key: i, ...f }))
      : [{ key: 0, name: "", type: "string", optional: false }],
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Monotonic id source so each row keeps a stable React key across
  // removals/reorders (index keys would make edited values appear to jump rows).
  const nextFieldKey = useRef(initial ? initial.fields.length : 1);

  const setField = (
    i: number,
    patch: Partial<{ name: string; type: string; optional: boolean }>,
  ) =>
    setFields((prev) => prev.map((f, j) => (j === i ? { ...f, ...patch } : f)));
  const addField = () =>
    setFields((prev) => [
      ...prev,
      {
        key: nextFieldKey.current++,
        name: "",
        type: "string",
        optional: false,
      },
    ]);
  const removeField = (i: number) =>
    setFields((prev) =>
      prev.length > 1 ? prev.filter((_, j) => j !== i) : prev,
    );

  const validate = (): string | null => {
    const tid = id.trim();
    if (!tid) return "Enter a type id.";
    if (!IDENT_RE.test(tid))
      return "Type id must be a valid identifier (letters, digits, _ or $; not starting with a digit).";
    // In edit mode the id is locked to an existing type, so a self-match is fine;
    // only guard collisions when creating a new id.
    if (!isEdit && existingIds.includes(tid))
      return `Type "${tid}" already exists.`;
    const named = fields
      .map((f) => ({ ...f, name: f.name.trim() }))
      .filter((f) => f.name);
    if (named.length === 0) return "Add at least one field.";
    for (const f of named) {
      if (!IDENT_RE.test(f.name))
        return `Field "${f.name}" is not a valid identifier.`;
    }
    const seen = new Set<string>();
    for (const f of named) {
      if (seen.has(f.name)) return `Duplicate field name "${f.name}".`;
      seen.add(f.name);
    }
    return null;
  };

  const save = async () => {
    const problem = validate();
    if (problem) {
      setError(problem);
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const named = fields
        .map((f) => ({
          name: f.name.trim(),
          type: f.type,
          optional: f.optional,
        }))
        .filter((f) => f.name);
      await onSave(id.trim(), named);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal
      title={isEdit ? `Edit envelope — ${initial.id}` : "New envelope"}
      onClose={busy ? () => {} : onCancel}
    >
      <div className="space-y-4">
        <label className="block">
          <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
            Type id
          </span>
          <input
            autoFocus
            value={id}
            onChange={(e) => setId(e.target.value)}
            readOnly={isEdit}
            aria-readonly={isEdit}
            className={`${inputClass} w-full${isEdit ? " opacity-60" : ""}`}
            placeholder="orderPlaced"
          />
        </label>
        <div>
          <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-fg-faint">
            Fields
          </span>
          <div className="space-y-2">
            {fields.map((f, i) => (
              <div
                key={f.key}
                className="grid grid-cols-[1fr_auto_auto_auto] items-center gap-2"
              >
                <input
                  value={f.name}
                  onChange={(e) => setField(i, { name: e.target.value })}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") void save();
                  }}
                  className={`${inputClass} w-full`}
                  placeholder="fieldName"
                />
                <select
                  value={f.type}
                  onChange={(e) => setField(i, { type: e.target.value })}
                  className={`${inputClass} w-32`}
                >
                  {SCALAR_KEYWORDS.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                </select>
                <label
                  className="flex items-center gap-1 text-xs text-fg-muted"
                  title="Optional field"
                >
                  <input
                    type="checkbox"
                    checked={f.optional}
                    onChange={(e) =>
                      setField(i, { optional: e.target.checked })
                    }
                  />
                  opt
                </label>
                <Button
                  variant="secondary"
                  onClick={() => removeField(i)}
                  disabled={fields.length <= 1}
                  aria-label="Remove field"
                  title="Remove field"
                >
                  ✕
                </Button>
              </div>
            ))}
          </div>
          <button
            type="button"
            onClick={addField}
            className="mt-2 text-sm text-accent hover:underline"
          >
            + Add field
          </button>
        </div>
        <p className="text-xs text-fg-muted">
          Declares a payload type <em>in the model</em> as a{" "}
          <code className="text-fg">nano:shape</code> — a data contract carried
          by an envelope, with no backing table. It travels with the{" "}
          <code className="text-fg">.bpmn</code> and is saved on the next Save.
        </p>
        {error && <p className="text-sm text-danger">{error}</p>}
        <div className="flex justify-end gap-2">
          <Button variant="secondary" onClick={onCancel} disabled={busy}>
            Cancel
          </Button>
          <Button variant="primary" onClick={() => void save()} disabled={busy}>
            {busy
              ? isEdit
                ? "Saving…"
                : "Creating…"
              : isEdit
                ? "Save changes"
                : "Create"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function Modal({
  title,
  children,
  onClose,
}: {
  title: string;
  children: React.ReactNode;
  onClose: () => void;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-lg rounded-lg border border-edge bg-raised p-5 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="mb-4 text-lg font-semibold text-fg">{title}</h2>
        {children}
      </div>
    </div>
  );
}

/// Modal for starting a process instance. User pastes/edits a JSON variables
/// payload (defaults to `{}`); the POST goes to <deployTarget>/v2/process-instances.
/// Shows the returned processInstanceKey on success, or the server's problem
/// detail on failure — no navigation, so the user can start another instance
/// right away with tweaked variables.
function StartInstanceModal({
  processId,
  deployTarget,
  onClose,
}: {
  processId: string;
  deployTarget: string;
  onClose: () => void;
}) {
  const [json, setJson] = useState("{}");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const start = async () => {
    setError(null);
    setResult(null);
    let variables: Record<string, unknown>;
    try {
      const parsed = json.trim() === "" ? {} : JSON.parse(json);
      if (
        parsed == null ||
        typeof parsed !== "object" ||
        Array.isArray(parsed)
      ) {
        throw new Error("Variables must be a JSON object.");
      }
      variables = parsed as Record<string, unknown>;
    } catch (e) {
      setError(`Invalid JSON: ${e instanceof Error ? e.message : String(e)}`);
      return;
    }
    setBusy(true);
    try {
      const r = await createProcessInstance({
        processId,
        variables,
        baseUrl: deployTarget,
      });
      setResult(r.processInstanceKey);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal title={`Start instance — ${processId}`} onClose={onClose}>
      <p className="text-sm text-fg-muted">
        Posted to{" "}
        <span className="font-mono text-fg">{`${deployTarget.replace(/\/+$/, "")}/v2/process-instances`}</span>
        . Variables must be a JSON object; leave{" "}
        <span className="font-mono">{"{}"}</span> for no vars.
      </p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">
        Variables (JSON)
      </label>
      <textarea
        value={json}
        onChange={(e) => setJson(e.target.value)}
        spellCheck={false}
        rows={8}
        className={`${inputCls} font-mono text-xs`}
      />
      {error && (
        <p className="mt-2 rounded-md border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
          {error}
        </p>
      )}
      {result && (
        <p className="mt-2 rounded-md border border-ok/40 bg-ok/10 px-3 py-2 text-xs text-ok">
          Started — processInstanceKey{" "}
          <span className="font-mono">{result}</span>
        </p>
      )}
      <div className="mt-5 flex justify-end gap-2">
        <Button onClick={onClose}>Close</Button>
        <Button variant="primary" onClick={() => void start()} disabled={busy}>
          {busy ? "Starting…" : "Start"}
        </Button>
      </div>
    </Modal>
  );
}

const inputCls = `mt-1 w-full ${inputClass}`;
