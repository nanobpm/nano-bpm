# Debugging BPMN processes — getting started

A hands-on runbook for the **in-engine BPMN process debugger**: set a breakpoint
on a flow node, pause a running process instance *inside the engine*, inspect its
variables, and step or resume — with the paused element highlighted on the
diagram in VS Code.

If you just want to **see it working in 30 seconds without VS Code**, jump to
[Path A](#path-a--30-second-headless-proof). For the full click-the-diagram
experience, see [Path B](#path-b--interactive-debugging-in-vs-code).

## The stack (what you are running)

The debugger is four pieces, each driving the next:

```
engine-core stepping executor (#647)      real breakpoints in the fixpoint loop
        │  debug_start / debug_step / debug_resume
        ▼
engine-wasm debug control surface (#650)  TestEngine.debug* over wasm-bindgen
        │  file:../engine-wasm/pkg
        ▼
dap-adapter (#651)                        Debug Adapter Protocol ⇄ engine
        │  stdio DAP  +  nanobpmn/activeElements event
        ▼
vscode-bpmn-debug (#652)                   bpmn-js webview: highlights the pause
```

- **`engine-core`** gained a stepping executor: the shared fixpoint loop can pause
  at a breakpoint mid-command and resume with run-to-completion parity.
- **`engine-wasm`** exposes that surface to JavaScript as `TestEngine.debug*`. The
  built package lives under `engine-wasm/pkg/`, a build output that is **not
  committed**: build it once with `make console-wasm` (see Prerequisites).
- **`dap-adapter`** is a standalone [Debug Adapter
  Protocol](https://microsoft.github.io/debug-adapter-protocol/) server that any
  DAP client can drive. See [`dap-adapter/README.md`](../dap-adapter/README.md).
- **`vscode-bpmn-debug`** is the VS Code extension that renders the diagram and
  highlights the paused node. See
  [`vscode-bpmn-debug/README.md`](../vscode-bpmn-debug/README.md).

> **Proof-of-concept.** This track is a working prototype. The debugger drives the
> **wasm** engine (an in-process `TestEngine`), not a running Nano server.

## Prerequisites

- **Node.js ≥ 22** (`node --version`).
- **The wasm engine package, built once:** a Rust toolchain plus wasm-pack
  (`make install-wasm-pack`), then `make console-wasm` from the repo root. It
  writes `engine-wasm/pkg/`, which the adapter links via
  `file:../engine-wasm/pkg`. The package is a gitignored build output, so a
  fresh clone does not have it. Re-run `make console-wasm` after changing the
  engine.
- **VS Code** — only for Path B.

## Path A — 30-second headless proof

This runs the adapter's integration test, which performs a **real DAP handshake**
against the wasm engine — `launch → setBreakpoints → configurationDone → stopped
→ stackTrace → variables → continue → terminated` — with **no VS Code involved**.

```bash
cd dap-adapter
npm install        # links @nanobpm/engine-wasm from ../engine-wasm/pkg (built by make console-wasm)
npm run build      # tsc → dist/ (the adapter binary dist/index.js)
npm test           # vitest: source-map units + the headless DAP integration test
```

You should see **7 tests pass** across two files. The one that proves the
debugger end-to-end is in
[`dap-adapter/test/adapter.test.ts`](../dap-adapter/test/adapter.test.ts): it
launches the built `dist/index.js`, sets a breakpoint on the **start event** of
[`test/fixtures/two-tasks.bpmn`](../dap-adapter/test/fixtures/two-tasks.bpmn)
(process `p`: `start(s) → serviceTask(first) → serviceTask(second) → end(e)`),
and asserts that:

- the engine emits a `stopped` event with `reason: "breakpoint"`,
- `stackTrace` reports the paused frame as element **`s`** on the start-event
  source line,
- `variables` is readable at the pause,
- `continue` runs the instance to `terminated`.

That is the debugger pausing a real process instance mid-run. To watch it happen
step by step, run the adapter against a DAP client of your own, or use Path B.

## Path B — interactive debugging in VS Code

Here you click a node in the BPMN diagram to set a breakpoint, launch a process
instance, and watch the engine pause with that node highlighted.

### 1. Build both packages

```bash
# the adapter binary the extension launches
cd dap-adapter && npm install && npm run build

# the extension (esbuild → dist/extension.js + dist/webview.js)
cd ../vscode-bpmn-debug && npm install && npm run build
```

> The extension launches the adapter at `../dap-adapter/dist/index.js`, so the
> adapter must be built first. (`@nanobpm/dap-adapter` also has a `prepare` hook
> that builds `dist/` on install, but building explicitly is clearest.)

### 2. Launch the Extension Development Host

Open the `vscode-bpmn-debug/` folder in VS Code and press **F5**. This starts a
second VS Code window (the *Extension Development Host*) with the `nanobpmn` debug
type registered.

### 3. Open a workspace with a **laid-out** `.bpmn`

In the dev-host window, open a folder containing a BPMN file **that carries
diagram interchange** (a `bpmndi:BPMNDiagram` with shapes/edges). The webview
renders with `bpmn-js`, which draws the *saved layout* — it does **not**
auto-layout.

> ⚠️ The repo's `dap-adapter/test/fixtures/two-tasks.bpmn` is intentionally
> **DI-free** (it exists only to test the source-line map), so it renders blank in
> the webview. Use a diagram authored in **Camunda Modeler** or exported from the
> **nano console** — both emit DI. Any executable process with a start event works;
> note its `process id` and the element ids for step 5.

### 4. Create a launch configuration

Add `.vscode/launch.json` in that workspace (template:
[`dap-adapter/examples/launch.json`](../dap-adapter/examples/launch.json)). The
`nanobpmn` type takes `bpmn` + `processId` (required) and optional `variables`:

```jsonc
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "nanobpmn",
      "request": "launch",
      "name": "Debug BPMN process",
      "bpmn": "${workspaceFolder}/your-process.bpmn",
      "processId": "your-process-id",
      "variables": {}
    }
  ]
}
```

### 5. Set a breakpoint, launch, and watch it pause

1. The diagram panel **auto-opens** beside the editor when the session starts.
2. **Click the start event** in the diagram (or set a gutter breakpoint on its
   source line) — this toggles a breakpoint at that element.
3. Start the `Debug BPMN process` configuration (**F5** / the Run panel).
4. The engine runs the instance and **pauses at the start event**, and the webview
   **highlights that node**. `stackTrace` shows the active element; **Variables**
   shows the instance state.
5. **Continue** (`F5`) to run to the next breakpoint or completion, or **Step
   Over** (`F10`) to advance exactly one engine step. The highlight clears on
   termination.

### Breakpoint semantics (read this)

A service task's `ElementActivated` and its `JobCreated` are emitted in the **same**
engine step, so a breakpoint on a service task pauses at the point the run would
*naturally* park anyway (waiting for the job). To observe a genuinely *mid-command*
pause, **break on an upstream node** — e.g. the **start event** — which stops the
run before any job parks. That is why both the bundled example and the headless
test break on the start event.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `npm install` can't resolve `@nanobpm/engine-wasm` | Run it from **inside `dap-adapter/`**; the dep is `file:../engine-wasm/pkg`. That directory is a build output: run `make console-wasm` from the repo root first. |
| The webview panel is **blank** | Your `.bpmn` has no diagram interchange. Use a Modeler/console-authored file with a `bpmndi:BPMNDiagram` (the test fixture is DI-free by design). |
| Breakpoint on a **service task** doesn't feel "mid-run" | Expected — it parks where the job would anyway. Break on the start event or another upstream node. |
| The extension launches but the adapter errors on start | Build the adapter first: `cd dap-adapter && npm run build` (the extension runs `../dap-adapter/dist/index.js`). |
| A gutter breakpoint shows **unverified** | It's on a line with no BPMN element (e.g. the XML prolog). Put it on the line carrying a flow node's `id`. |

## Where to go next

- Per-component detail and the DAP⇄engine mapping table:
  [`dap-adapter/README.md`](../dap-adapter/README.md).
- The webview wiring (custom `nanobpmn/activeElements` event, click-to-breakpoint):
  [`vscode-bpmn-debug/README.md`](../vscode-bpmn-debug/README.md).
- The engine-core stepping executor and breakpoint conditions live in
  `engine-core/src/engine/debug.rs`; the wasm surface in `engine-wasm/src/lib.rs`.
