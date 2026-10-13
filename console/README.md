# nanobpmn console

The embedded web console (React + Vite + Tailwind). In a release build it is
compiled to `console/dist` and baked into the gateway binary via RustEmbed
(`server/src/console/mod.rs`), served under `/console/`.

## Local development (HMR)

For fast iteration you don't want to rebuild `console/dist` and recompile the
gateway on every change. Instead run the Vite dev server with hot-module reload
against a separately-running gateway:

1. Start a backing gateway on `:8080` (e.g. with `c8ctl`).
2. In this directory, run the single dev command:

   ```sh
   npm install   # first time only
   npm run dev
   ```

3. Open the printed URL, e.g. <http://localhost:5173/console/>.

The dev server serves the SPA with HMR on a separate port (`5173`) and proxies
the two gateway surfaces the console calls to the backend, so everything works
against live data:

- `/console/api` — the console's JSON API and the SSE live-update stream
  (`/console/api/stream`) that drives auto-refresh.
- `/v2` — the public REST API: deployments, instance creation, and the BPMN
  definition XML the Process Explorer diagram viewer fetches.

### Configuration

Both are environment variables (all optional):

| Variable        | Default                  | Purpose                                   |
| --------------- | ------------------------ | ----------------------------------------- |
| `NANO_BACKEND`  | `http://127.0.0.1:8080`  | Gateway the dev server proxies API calls to. |
| `CONSOLE_PORT`  | `5173`                   | Port the HMR dev server listens on.       |

Example pointing at a gateway on another port:

```sh
NANO_BACKEND=http://127.0.0.1:9000 npm run dev
```

## Production build

```sh
npm run build   # → console/dist, embedded by the gateway's `console` feature
```

## Projects (RAD environment)

The **Projects** view (the console's home) is a Rapid Application Development
environment. A project is a self-contained
[Node](https://nodejs.org) application directory holding everything an automation
needs: process models, the workers that service their jobs, shared libraries,
and a generated entry point that ties them together.

### Layout

```
<projectsRoot>/<project>/
  nanobpm.project.json   # name, description, deployTarget, main, export platforms
  node.json              # import map (@nanobpm/worker, @lib/); package.json runs it
  package.json           # npm identity (engines: node >= 22.6) + `npm start`
  main.ts                # deploys resources/processes/*.bpmn, then starts workers
  resources/
    processes/  *.bpmn   # deployed to the engine on Run
    decisions/  *.dmn    # authored + exported (see note below)
    forms/      *.form    # authored + exported (see note below)
  workers/<name>/        # one folder per worker (worker.ts + node.json)
  lib/                   # shared @lib/… modules usable from every worker
  nano-generated/worker-sdk.ts # embedded worker SDK
```

`<projectsRoot>` defaults to `<workspace>/projects` and can be overridden with
the `NANOBPMN_PROJECTS_DIR` environment variable. Each immediate subdirectory is
one project, surfaced as a tile on the Projects home.

### Workflow

Open a project to get a file browser, graphical/code editors, the run console,
and a toolbar:

- **Run** — `node --experimental-strip-types main.ts`: deploys every
  `resources/processes/*.bpmn` to the configured deploy target, then imports and
  starts each worker. Stdout/stderr stream live into the Output panel. **Stop**
  terminates the application.
- **Compile** — runs the project's configured compile command (lang pack or
  toolchain argv), producing standalone binaries under `dist/`.
- **Configure** — edits the deploy target (`<target>/v2` is the REST API the app
  deploys to and dials the Falcon protocol on), the entry point, and the set of
  export platforms.
- **Export** — downloads the whole project as a `.zip` (add the compiled `dist/`
  binaries with the `?dist=true` option).

### Editors

The center pane dispatches on file extension:

| Extension | Editor |
| --------- | ------ |
| `.bpmn`   | bpmn-js modeler (with properties panel) |
| `.dmn`    | dmn-js (DRD + decision table) |
| `.form`   | @bpmn-io/form-js builder |
| other     | Monaco code editor (with worker-SDK IntelliSense) |

> **Note — decisions and forms are authoring + export artifacts.** The nanobpmn
> engine executes BPMN only; it has no DMN or form runtime. You can author
> `.dmn`/`.form` resources graphically and they travel with the project (and its
> export zip), but only `resources/processes/*.bpmn` are deployed and executed.

Running and compiling require a Node (≥ 22.6) runtime on the gateway host; when
none is detected the editors and Export still work, but Run and Compile are
disabled. The `NANOBPMN_NODE_BIN` environment variable names an explicit binary,
but a version older than 22.6 is skipped in favour of a supported Node on
`PATH`/`~/.local/bin`.

