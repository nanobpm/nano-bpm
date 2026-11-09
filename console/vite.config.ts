import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

/**
 * The shared Process Explorer package (`../process-explorer`) is console source
 * kept outside `console/` so other hosts can embed it. Serve it from its real
 * path, like `src/`, not through its `node_modules` symlink: Vite treats files
 * under `node_modules` as third-party and versions their URLs inconsistently
 * (`?v=` on some imports, not others), which loads a module twice — and a second
 * copy of its data-source React context leaves the Explorer without the
 * console's provider. Each `exports` entry becomes an exact-match alias.
 */
const explorerDir = fileURLToPath(
  new URL("../process-explorer/", import.meta.url),
);
const explorerExports: Record<string, string> = JSON.parse(
  readFileSync(explorerDir + "package.json", "utf8"),
).exports;
const explorerAliases = Object.entries(explorerExports).map(([sub, file]) => ({
  find: new RegExp(`^@nanobpm/process-explorer${sub.slice(1)}$`),
  replacement: explorerDir + file.slice(2),
}));

// The gateway the dev server proxies API calls to. Defaults to the local
// gateway `c8ctl` starts on :8080; override with NANO_BACKEND to point at a
// gateway running elsewhere (another port or host).
const backend = process.env.NANO_BACKEND ?? "http://127.0.0.1:8080";

// HMR dev-server port. Kept distinct from the gateway's :8080 so both run side
// by side; override with CONSOLE_PORT if 5173 is taken.
const port = Number(process.env.CONSOLE_PORT ?? 5173);

// The console ships two build profiles (ADR 0034): the full "studio" RAD IDE and
// the lean "observe" operator surface. The profile is chosen at build time via
// `VITE_CONSOLE_PROFILE` (default "studio"). We surface it to the app two ways:
//   - `import.meta.env.VITE_CONSOLE_PROFILE` — for ordinary runtime gates.
//   - a `define`d `__STUDIO__` boolean literal — for the `lazy(() => import())`
//     anchors. `define` is applied by esbuild during *transform*, so the guarded
//     `import()` folds to dead code before Rollup ever walks it. That matters: an
//     imported `IS_STUDIO` const only tree-shakes *after* transform, by which
//     point Vite's `?worker` plugin has already emitted Monaco's (6MB) worker
//     chunks as orphans. A `define`d literal drops the import target outright.
const profile =
  process.env.VITE_CONSOLE_PROFILE === "observe" ? "observe" : "studio";
const isStudio = profile === "studio";

// The console is served by the gateway under the `/console/` path prefix
// (see server/src/console/mod.rs), so the built asset URLs must be prefixed too.
export default defineConfig({
  base: "/console/",
  define: {
    __STUDIO__: JSON.stringify(isStudio),
  },
  plugins: [react(), tailwindcss()],
  // `@bpmn-io/form-js-editor` pins its own nested `preact` (10.15.x) while the
  // form *viewer* it renders through — plus `@bpmn-io/properties-panel` and
  // diagram-js — resolve the hoisted `preact` (10.29.x). Two preact copies means
  // two independent "current component" globals, so the editor's `useService`
  // hooks read an undefined render context and the whole form editor crashes
  // ("Cannot read properties of undefined (reading 'context')"), rendering no
  // palette or properties panel. Collapsing preact to a single instance fixes it
  // in both dev (optimizeDeps) and the production build (Rollup).
  resolve: {
    dedupe: [
      "preact",
      // The Process Explorer's bare imports, resolved from here: the package has
      // no install of its own (the console provides its peer dependencies).
      "react",
      "react-dom",
      "@tanstack/react-query",
      "bpmn-js",
      "@nanobpm/nano-app-schema",
    ],
    alias: explorerAliases,
    // Match the console tsconfig's `preserveSymlinks`: `@nanobpm/nano-app-schema`
    // is a `file:` dep (`../spec-app`) symlinked into `node_modules`. Resolving
    // via its realpath (Rollup's default) can miss the hoisted copy, so follow
    // the symlink path rooted here instead. `@nanobpm/engine-wasm` now comes from
    // npm transitively (via the published `@nanobpm/bojtos-*` packages), so it no
    // longer needs the symlink treatment — but the setting is retained for the
    // remaining `file:` dep.
    preserveSymlinks: true,
  },
  // The published `@nanobpm/engine-wasm` (wasm-pack `--target web` output) resolves
  // its binary via `new URL('nanobpmn_engine_bg.wasm', import.meta.url)`. Excluding
  // it from esbuild's dependency pre-bundling keeps that asset reference intact so
  // Vite emits the `.wasm` as a hashed asset instead of esbuild rewriting the
  // `import.meta.url` and losing the binary. The Bojtos packages
  // (`@nanobpm/bojtos-kit` / `-react`) load the engine through that same loader and
  // pull engine-wasm in transitively, so all three are excluded to keep the wasm
  // asset reference intact end to end.
  optimizeDeps: {
    exclude: [
      "@nanobpm/engine-wasm",
      "@nanobpm/bojtos-kit",
      "@nanobpm/bojtos-react",
    ],
    // The whole BPMN/DMN/form modeler stack is reached only through the lazy
    // `lazy(() => import())` modeler routes, so esbuild's cold-start dependency
    // scan does not always prebundle it up front. When a *later* navigation then
    // pulls one of these packages in for the first time, Vite runs a mid-session
    // re-optimize + full reload — and that second esbuild pass can split
    // `@bpmn-io/properties-panel`'s vendored preact (its `../preact`, shared with
    // `bpmn-js-properties-panel` and `bpmn-js-element-templates` via the
    // `@bpmn-io/properties-panel/preact` subpath) into a *second* copy. Two
    // preact instances means the properties-panel `Group` renders under one
    // preact while its hooks read the other's "current component" — the exact
    // `Cannot read '__H'` + `debounce is not a function` crash that unmounts the
    // Agent-task properties group mid-render (issue #1127). Prebundling the whole
    // stack — plus preact's own entry points — up front makes the optimize pass
    // complete and stable at server start, so no runtime re-optimize reshuffles
    // preact and the panel renders deterministically.
    include: [
      "bpmn-js/lib/Modeler",
      "bpmn-js/lib/NavigatedViewer",
      "bpmn-js-properties-panel",
      "bpmn-js-element-templates",
      "@bpmn-io/properties-panel",
      "@bpmn-io/extract-process-variables/zeebe",
      "camunda-bpmn-js-behaviors/lib/camunda-cloud",
      "diagram-js/lib/draw/BaseRenderer",
      "@bpmn-io/form-js-editor",
      "@bpmn-io/form-js-viewer",
      "dmn-js/lib/Modeler",
      "preact",
      "preact/hooks",
      "preact/jsx-runtime",
    ],
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Every view is a lazy route chunk and the heaviest editors (Monaco and its
    // ~3.6MB TypeScript language service, the bpmn/dmn/form modeler stacks) are
    // further lazy-loaded on demand — so a large chunk here is an inherently big
    // third-party editor fetched only when used, not eager startup weight. Raise
    // the warning threshold above those known-large on-demand chunks so the
    // signal flags genuinely new regressions instead of firing on every build.
    chunkSizeWarningLimit: 4000,
    rollupOptions: {
      // The agentic tour journeys (src/lib/tour/journeys/agentic.ts) reach the
      // generated fetch API client (`src/gen`) and the SSE/EventSource client
      // (`src/lib/api.ts`) through dynamic `import()` on purpose: it keeps those
      // browser-only clients out of module scope so the Node tour guard tests can
      // import the journey module without dragging fetch/EventSource into Node
      // (see the header comment in agentic.ts). Both modules are also statically
      // imported across the app, so Rollup emits a benign reporter notice that the
      // dynamic import cannot move them into their own chunk. Silence only those
      // two intentional cases; every other warning (including a NEW accidental
      // static+dynamic split elsewhere) still surfaces.
      onwarn(warning, defaultHandler) {
        const msg = warning.message ?? "";
        const intentionalMixedImport =
          msg.includes(
            "dynamic import will not move module into another chunk",
          ) &&
          (msg.includes("src/gen/index.ts is dynamically imported by") ||
            msg.includes("src/lib/api.ts is dynamically imported by"));
        if (intentionalMixedImport) return;
        defaultHandler(warning);
      },
    },
  },
  server: {
    port,
    // `npm run dev` serves the SPA with hot-module reload and proxies the two
    // gateway surfaces the console talks to, so the frontend develops against a
    // live backend without rebuilding/embedding it into the binary:
    //   - `/console/api` — the console's own JSON API plus the SSE live-update
    //     stream (`/console/api/stream`) that drives auto-refresh.
    //   - `/v2`          — the public REST API: deployments, instance creation,
    //     and the BPMN definition XML the Process Explorer's diagram viewer
    //     fetches (`/v2/process-definitions/{key}/xml`).
    // Start a backing gateway first (e.g. with `c8ctl` on :8080), then run
    // `npm run dev` and open the printed http://localhost:<port>/console/ URL.
    proxy: {
      // `ws: true` so the integrated terminal's PTY WebSocket
      // (`/console/api/projects/{name}/pty`, issue #496) is proxied to the
      // gateway too, not just the plain-HTTP JSON/SSE surface.
      "/console/api": { target: backend, changeOrigin: true, ws: true },
      "/v2": { target: backend, changeOrigin: true },
    },
  },
});
