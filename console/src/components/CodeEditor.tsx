import { useEffect, useRef } from "react";
import * as monaco from "monaco-editor";
import { Editor, loader } from "@monaco-editor/react";
import { useTheme } from "../theme/ThemeProvider";

// Bundle Monaco's web workers through Vite (the `?worker` suffix emits each as a
// separate chunk) instead of fetching them from a CDN. This keeps the console
// fully self-contained: everything is embedded in the gateway binary and works
// offline, with no network round-trips when the editor mounts.
//
// The subpaths go through monaco-editor's `exports` map (`./*` → `esm/vs/*.js`);
// monaco-editor 0.57 made the package exports-only, so the old
// `monaco-editor/esm/vs/…` deep paths no longer resolve.
import editorWorker from "monaco-editor/editor/editor.worker.js?worker";
import jsonWorker from "monaco-editor/language/json/json.worker.js?worker";
import tsWorker from "monaco-editor/language/typescript/ts.worker.js?worker";

(
  self as unknown as { MonacoEnvironment: monaco.Environment }
).MonacoEnvironment = {
  getWorker(_workerId, label) {
    if (label === "json") return new jsonWorker();
    if (label === "typescript" || label === "javascript") return new tsWorker();
    return new editorWorker();
  },
};

// Point @monaco-editor/react at our locally bundled monaco instance rather than
// letting it lazy-load the AMD build from jsdelivr (the default).
loader.config({ monaco });

// Worker code targets Node: ESM modules and top-level await. Configure the TS
// language service to match and silence the "cannot find module" diagnostics
// that non-resolvable specifiers (e.g. a not-yet-acquired npm package) produce.
monaco.typescript.typescriptDefaults.setCompilerOptions({
  target: monaco.typescript.ScriptTarget.ESNext,
  module: monaco.typescript.ModuleKind.ESNext,
  moduleResolution: monaco.typescript.ModuleResolutionKind.NodeJs,
  allowNonTsExtensions: true,
  allowJs: true,
  // Match Node/bundler semantics so default imports of CommonJS-typed npm
  // packages (e.g. `import _ from "lodash"`) resolve to their members.
  esModuleInterop: true,
  allowSyntheticDefaultImports: true,
  noEmit: true,
  lib: ["esnext", "dom"],
  // Mirror the worker `node.json` import map's `@lib/` alias so that
  // `import { fmt } from "@lib/money.ts"` resolves to the shared-library models
  // registered at `file:///lib/*` (see `registerModels`).
  baseUrl: "file:///",
  paths: { "@lib/*": ["lib/*"] },
});
monaco.typescript.typescriptDefaults.setDiagnosticsOptions({
  diagnosticCodesToIgnore: [
    2307, // Cannot find module '...' (an npm package not yet acquired via ATA)
    2792, // Cannot find module — consider moduleResolution
    1375, // Top-level await "needs imports/exports": every worker file is an ES
    //       module, so top-level await is always valid here.
  ],
});

// --- IntelliSense: ambient Node.js types ------------------------------------
// Worker and `main.ts` code runs on Node, so `process`, `Buffer`, and the
// `node:*` builtin modules must resolve with real types. Historically this came
// from a vendored Deno namespace `.d.ts`; Node has no single
// ambient-namespace equivalent, so the ambient types are derived from the
// console's own pinned `@types/node` devDependency and registered as Monaco
// extra-libs — a single source of truth, with nothing vendored to drift.
//
// Vite inlines each `.d.ts` as a string at build time via a *lazy* `?raw` glob
// (no `eager`): the definitions split into per-file chunks that load only when
// the editor first registers them, so the console's initial bundle is
// untouched. `@types/node`'s root `index.d.ts` targets the TypeScript the editor
// bundles (Monaco 0.57 ships TS 5.9); its versioned `ts5.6/` / `ts5.7/` trees
// target older compilers and are excluded to avoid duplicate global
// declarations.
const nodeTypeModules = import.meta.glob(
  [
    "../../node_modules/@types/node/**/*.d.ts",
    "../../node_modules/undici-types/**/*.d.ts",
    "!../../node_modules/@types/node/ts5.6/**",
    "!../../node_modules/@types/node/ts5.7/**",
  ],
  { query: "?raw", import: "default" },
);
let nodeLibRegistered = false;
async function ensureNodeTypes(): Promise<void> {
  if (nodeLibRegistered) return;
  nodeLibRegistered = true;
  try {
    const paths = Object.keys(nodeTypeModules);
    const sources = await Promise.all(paths.map((p) => nodeTypeModules[p]()));
    paths.forEach((p, i) => {
      const src = sources[i];
      if (typeof src !== "string") return;
      // `../../node_modules/@types/node/http.d.ts` →
      // `file:///node_modules/@types/node/http.d.ts`, so a bare
      // `import ... from "undici-types"` and the relative references inside the
      // definitions resolve exactly as they do on disk.
      monaco.typescript.typescriptDefaults.addExtraLib(
        src,
        "file:///" + p.replace(/^\.\.\/\.\.\//, ""),
      );
    });
  } catch {
    nodeLibRegistered = false; // let a later mount retry
  }
}

// --- IntelliSense: the @nanobpm/worker SDK ---------------------------------
// Fetch the embedded SDK source once and register it under a node_modules path
// so a bare `import { defineWorker } from "@nanobpm/worker"` resolves with full
// types, signatures, and JSDoc. Fully offline (served by the gateway).
let sdkLibRegistered = false;
async function ensureSdkLib(): Promise<void> {
  if (sdkLibRegistered) return;
  sdkLibRegistered = true;
  try {
    const res = await fetch("/console/api/worker-sdk");
    if (!res.ok) {
      sdkLibRegistered = false;
      return;
    }
    const src = await res.text();
    monaco.typescript.typescriptDefaults.addExtraLib(
      src,
      "file:///node_modules/@nanobpm/worker/index.ts",
    );
  } catch {
    sdkLibRegistered = false; // let a later mount retry
  }
}

// --- IntelliSense: the @nanobpm/urban runtime SDK --------------------------
// A modern Urban app (ADR 0055/0059) imports its APIs from `@nanobpm/urban`:
// `import type { AppJobHandler } from "@nanobpm/urban"` in workers,
// `import { runFromEnv, selectHost } from "@nanobpm/urban"` in main.ts, and the
// generated `controller.ts`/`api-io.d.ts` import `OperationHandler` from it too.
// Fetch the gateway's embedded, self-contained rollup of that type surface once
// and register it under a node_modules path so a bare `@nanobpm/urban` import
// resolves with full types, signatures and JSDoc. Fully offline (served by the
// gateway from its baked-in copy — see server `urban_types.d.ts`).
let urbanLibRegistered = false;
async function ensureUrbanLib(): Promise<void> {
  if (urbanLibRegistered) return;
  urbanLibRegistered = true;
  try {
    const res = await fetch("/console/api/urban-types");
    if (!res.ok) {
      urbanLibRegistered = false;
      return;
    }
    const src = await res.text();
    monaco.typescript.typescriptDefaults.addExtraLib(
      src,
      "file:///node_modules/@nanobpm/urban/index.d.ts",
    );
    // Node module resolution of a bare `@nanobpm/urban` specifier only finds the
    // declaration file above when a `package.json` names it via `types` — a bare
    // directory with just `index.d.ts` (and no manifest) fails with TS2307, so
    // every symbol would silently fall back to `any`. Register a minimal
    // manifest so the specifier resolves with full types.
    monaco.typescript.typescriptDefaults.addExtraLib(
      JSON.stringify({
        name: "@nanobpm/urban",
        types: "index.d.ts",
      }),
      "file:///node_modules/@nanobpm/urban/package.json",
    );
  } catch {
    urbanLibRegistered = false; // let a later mount retry
  }
}

// --- IntelliSense: the project's *typed* @nanobpm SDK surface --------------
// `ensureSdkLib()` above wires up the generic, untyped worker SDK so a bare
// `@nanobpm/worker` import at least resolves. But a real Urban app reifies a
// *typed* SDK into `nano-generated/` (ADR 0029/0033): `workers.ts` overrides
// `defineWorker` with a taskType-keyed signature (so `job.variables` is typed
// from the worker's declared domain type), `domain.ts` exposes `openDomain()`
// with typed tables, and `data-sdk.ts` the raw data gateway. Register those
// per-project files under `node_modules/@nanobpm/{worker,domain,data,llm}/` so
// the bare specifiers resolve to the typed surface — regardless of the editing
// model's URI scheme (Monaco resolves bare specifiers via node_modules, so this
// works for both the standalone Workers editor and the project workspace, whose
// files live under different `file:///…` roots). Missing files (a non-Urban
// project, or one not yet reified) are tolerated: the generic SDK stays.
let projectSdkLoaded = "";
/** Per-project generated-SDK directory (mirror of `domain_types.ts::GEN_DIR`). */
const GEN_DIR = "nano-generated";
export async function ensureProjectSdkLibs(
  project: string | undefined,
): Promise<void> {
  if (!project || projectSdkLoaded === project) return;
  const read = async (p: string): Promise<string | null> => {
    try {
      const { projectFileEx } = await import("../lib/api");
      const f = await projectFileEx(project, p);
      return f.binary ? null : f.text;
    } catch {
      return null;
    }
  };
  const [workersTs, workerSdk, workersDts, domainTs, domainDts, dataSdk, llm] =
    await Promise.all([
      read(`${GEN_DIR}/workers.ts`),
      read(`${GEN_DIR}/worker-sdk.ts`),
      read(`${GEN_DIR}/worker-io.d.ts`),
      read(`${GEN_DIR}/domain.ts`),
      read(`${GEN_DIR}/domain-rows.d.ts`),
      read(`${GEN_DIR}/data-sdk.ts`),
      read(`${GEN_DIR}/llm-worker.ts`),
    ]);
  const add = (content: string | null, path: string) => {
    if (content == null) return;
    monaco.typescript.typescriptDefaults.addExtraLib(content, path);
  };
  // @nanobpm/worker → the typed `defineWorker` wrapper + the relative modules it
  // (and its `worker-io.d.ts`) import, co-located so `./worker-sdk.ts`,
  // `./worker-io.d.ts` and `./domain-rows.d.ts` all resolve within the package dir.
  add(workersTs, "file:///node_modules/@nanobpm/worker/index.ts");
  add(workerSdk, "file:///node_modules/@nanobpm/worker/worker-sdk.ts");
  add(workersDts, "file:///node_modules/@nanobpm/worker/worker-io.d.ts");
  add(domainDts, "file:///node_modules/@nanobpm/worker/domain-rows.d.ts");
  add(dataSdk, "file:///node_modules/@nanobpm/worker/data-sdk.ts");
  // @nanobpm/domain → `openDomain()` + its relative deps (`data-sdk.ts` for the
  // Table gateway, `domain-rows.d.ts` for the row interfaces).
  add(domainTs, "file:///node_modules/@nanobpm/domain/index.ts");
  add(dataSdk, "file:///node_modules/@nanobpm/domain/data-sdk.ts");
  add(domainDts, "file:///node_modules/@nanobpm/domain/domain-rows.d.ts");
  // @nanobpm/data → the raw typed data SDK.
  add(dataSdk, "file:///node_modules/@nanobpm/data/index.ts");
  // @nanobpm/llm → the LLM worker helper, when the app declares LLM workers.
  add(llm, "file:///node_modules/@nanobpm/llm/index.ts");
  if (workersTs) projectSdkLoaded = project;
}

/** Drop the cached project so the next `ensureProjectSdkLibs` refetches — call
 * after a reify / manifest change so the editor picks up the new typings. */
export function invalidateProjectSdkLibs(): void {
  projectSdkLoaded = "";
}

// --- IntelliSense: Automatic Type Acquisition (npm packages) ---------------
// Scan the edited source for imported packages and fetch their .d.ts from the
// jsdelivr CDN, registering each as a Monaco extra-lib — so importing e.g.
// `@camunda8/orchestration-cluster-api` (or any npm package) lights up with
// full IntelliSense. This is a *progressive enhancement*: it needs network
// access from the browser. The editor and the SDK types above stay offline.
type AtaRun = (code: string) => void;
let ataPromise: Promise<AtaRun | null> | null = null;
function ensureAta(): Promise<AtaRun | null> {
  if (!ataPromise) {
    ataPromise = (async () => {
      try {
        // typescript is multi-MB; load it (and ATA) in their own async chunk,
        // only the first time a TS/JS file is edited.
        const [ata, tsmod] = await Promise.all([
          import("@typescript/ata"),
          import("typescript"),
        ]);
        const ts = (tsmod as unknown as { default?: unknown }).default ?? tsmod;
        return ata.setupTypeAcquisition({
          projectName: "nano-workers",
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          typescript: ts as any,
          delegate: {
            receivedFile: (code: string, path: string) => {
              monaco.typescript.typescriptDefaults.addExtraLib(
                code,
                `file://${path}`,
              );
            },
          },
        });
      } catch {
        return null; // offline or load failure — editor still works
      }
    })();
  }
  return ataPromise;
}

function acquireTypes(code: string): void {
  void ensureAta().then((run) => run?.(code));
}

// Re-export from the framework-free module so the sibling registerModels()
// below and consumers importing `languageForFile` from CodeEditor share a
// single ext→language table — including any file types registered from
// installed lang packs at runtime.
import { languageForFile } from "../lib/editorLang";
export { languageForFile };
import { initMonacoIntellisense } from "../lib/monacoIntellisense";
import { ensureBundledJsonSchemas } from "../lib/jsonSchemas";

// --- Cross-file IntelliSense: sibling + shared-library models --------------
// A worker is a folder of files, and shared logic lives under `@lib/…`. For the
// TS service to resolve `import "./helper.ts"` or `import "@lib/money.ts"`, the
// *imported* files must exist as Monaco models — not just the one in the active
// tab. We register each as a background model at its `file:///…` URI (creating
// it, or updating its text if it already exists). The active editing model is
// owned by <Editor> via its `path`; we never touch or dispose that one.
export type ExtraModel = { path: string; content: string };

function registerModels(models: ExtraModel[], activePath?: string): void {
  for (const m of models) {
    if (m.path === activePath) continue;
    const uri = monaco.Uri.parse(m.path);
    const existing = monaco.editor.getModel(uri);
    if (existing) {
      if (existing.getValue() !== m.content) existing.setValue(m.content);
    } else {
      const file = m.path.split("/").pop() ?? m.path;
      monaco.editor.createModel(m.content, languageForFile(file), uri);
    }
  }
}

// --- Pack-fed IntelliSense providers ---------------------------------------
// TS/JS get Monaco's real language service; every other language relies on the
// curated, SDK-derived data a lang pack ships (see lib/langIntellisense.ts,
// populated from the extensions overview). The provider layer lives in
// lib/monacoIntellisense.ts — imported only here, inside the lazily-loaded
// editor chunk — and reads the shared store live so later pack installs take
// effect without a reload.
initMonacoIntellisense();

// Register the bundled JSON schemas (Urban App manifest + Camunda element
// template) with Monaco eagerly, as soon as this lazily-loaded editor chunk
// loads. Unlike manifest completion (which the manifest editor opts a model into
// via `setManifestSource`), an element template is a plain `components/*.json`
// edited through this generic editor with no manifest necessarily open — so its
// `$schema` must resolve offline regardless. Idempotent; see `jsonSchemas.ts`.
ensureBundledJsonSchemas();

export default function CodeEditor({
  value,
  language,
  path,
  readOnly,
  extraModels,
  sdkProject,
  onChange,
  onSave,
}: {
  value: string;
  language: string;
  /** Virtual file path (a `file:///…` URI) so module resolution + the SDK and
   * acquired npm types resolve from the editing model. */
  path?: string;
  readOnly?: boolean;
  /** Other files visible to module resolution (sibling worker files + shared
   * `@lib/` modules), registered as background Monaco models so cross-file and
   * `@lib/…` imports type-check and complete. */
  extraModels?: ExtraModel[];
  /** The project whose *typed* `.nanobpm` SDK should back the `@nanobpm/*` bare
   * specifiers (so `job.variables` is typed from the worker's declared domain
   * type). Pass this when editing a project's worker/`main.ts` code. */
  sdkProject?: string;
  onChange: (value: string) => void;
  onSave?: () => void;
}) {
  // Monaco is theme-managed per instance; follow the console's resolved
  // appearance (light/dark/system/theme packs all reduce to one of the two).
  const { appearance } = useTheme();

  // Keep the latest onSave in a ref so the Cmd/Ctrl+S keybinding registered on
  // mount always calls the current handler (avoids a stale closure).
  const saveRef = useRef(onSave);
  saveRef.current = onSave;

  // Debounce type acquisition so we don't refetch on every keystroke.
  const ataTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const isCode = language === "typescript" || language === "javascript";
  const scheduleAta = (code: string) => {
    if (!isCode) return;
    clearTimeout(ataTimer.current);
    ataTimer.current = setTimeout(() => acquireTypes(code), 600);
  };

  // Keep sibling/library models in sync so cross-file resolution sees current
  // text. Serialized so the effect re-runs whenever any imported file changes.
  const extraKey = JSON.stringify(extraModels ?? []);
  useEffect(() => {
    if (extraModels?.length) registerModels(extraModels, path);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [extraKey, path]);

  useEffect(() => {
    void ensureNodeTypes();
    void ensureSdkLib();
    void ensureUrbanLib();
    if (isCode) {
      void ensureProjectSdkLibs(sdkProject);
      acquireTypes(value);
    }
    return () => clearTimeout(ataTimer.current);
    // Re-run when switching to a different file/value.
  }, [path, isCode, value, sdkProject]);

  return (
    <Editor
      value={value}
      language={language}
      path={path}
      theme={appearance === "light" ? "light" : "vs-dark"}
      onChange={(v) => {
        const next = v ?? "";
        onChange(next);
        scheduleAta(next);
      }}
      onMount={(editor) => {
        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () =>
          saveRef.current?.(),
        );
        // Monaco measures monospace glyph widths at init; if the JetBrains Mono
        // webfont is still loading it measures the fallback and the caret/columns
        // misalign. Remeasure once the font is ready so glyphs line up.
        void document.fonts.ready.then(() => monaco.editor.remeasureFonts());
      }}
      loading={<div className="p-4 text-sm text-fg-faint">Loading editor…</div>}
      options={{
        readOnly,
        fontFamily:
          '"JetBrains Mono Variable", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
        fontSize: 13,
        tabSize: 2,
        insertSpaces: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
        smoothScrolling: true,
        renderWhitespace: "none",
        fixedOverflowWidgets: true,
      }}
    />
  );
}
