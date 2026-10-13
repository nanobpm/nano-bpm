# ADR 0052 — The Urban runtime: a decoupled manifest interpreter (`@nanobpm/urban-runtime`), a scaffolder, and interchangeable hosts

Status: **Proposed.**
Date: 2026-07-31.
Relates to:
ADR 0027 (`0027-urban-app-manifest-spec.md`, the `nano.app.json` manifest — this ADR names the
component that *reads* it and turns declaration into a running app),
ADR 0026 (`0026-urban-human-surfaces-and-run-model.md`, the App run model & dev loop — this ADR
**revises** where that loop lives: the console supervisor becomes one *host* of a shared runtime, not
the runtime itself),
ADR 0022 (`0022-nano-rad-application.md`, the Urban App bundle — the thing this runtime executes),
ADR 0024 / 0025 / 0026 (the `data` / `triggers` / `surfaces` blocks the runtime materializes as
modules),
ADR 0041 (`0041-urban-app-import-registry.md`, headless run of an imported app — this ADR
generalizes "headless run" into a first-class, console-independent entrypoint),
ADR 0038 (`0038-node-first-runtime.md`, **Node-first, Deno optional**) and ADR 0036
(`0036-dual-runtime-workers-deno-node-fallback.md`, dual-runtime workers) — the runtime honours both:
a runtime-agnostic core with thin Node and Deno adapters,
ADR 0007 (`0007-rad-extension-system.md`, **declared data, no `eval`** — the runtime drives data, it
does not evaluate the manifest as code),
ADR 0009 (`0009-gui-application-projects.md`, the served-UI `app` kind the runtime mounts),
`jwulf/nano-workforce` (the probe app whose gaps A/C/D/E this runtime answers; `probe/NANO-SURFACE.md`).

## Context

Building the **Nano Workforce** probe app (ADR 0051, `jwulf/nano-workforce`) surfaced a structural
gap. The manifest `nano-workforce.nano.app.json` declares everything an Urban app needs — `models`,
`data`, `types`, `triggers`, `bindings`, `surfaces`, `workers`, `llm`, `security` — but **nothing
reads it**. To run the app you must, by hand: deploy the BPMN/DMN, run the SQLite migrations, start
five workers, serve a console, and wire the webhook. The manifest is inert documentation; the
knowledge of how to *materialize* it lives nowhere reusable.

Where that knowledge *does* live today is the **console** — the supervisor in
`server/src/console/projects.rs` spawns a project, points it at a `deployTarget`, runs
`deno run`/`deno compile`, and the console's own code deploys models, renders forms, and hosts
surfaces. This couples "running an Urban app" to "running the console." Two constituencies are shut
out by that coupling:

- **Agents** (ten of them ran the User Journey epic; Nano Workforce automates exactly this) need to
  `clone && run` an app in CI or a worktree with no console and no IDE.
- **Users** building a Delphi-style local app want to `create` an app, `run` it standalone on their
  machine, and *optionally* open the same unchanged repo in the console/IDE later.

The pattern the frontend world settled on is instructive. **Create React App** hid the runtime as an
opaque `react-scripts` and made "ejecting" a one-way cliff — fatal for us, because agents and power
users *must* see and override the wiring. **Vite / Astro** went the other way: an explicit,
versioned library dependency the app imports and configures, with a thin `bin`. Legibility over
magic. Nano's own `0007` no-`eval` discipline already points here — the manifest is *data a host
drives*, so the host is swappable by construction.

The missing piece is not more console features. It is a **standalone interpreter of the manifest**,
depended on as a library, that every host — CLI, IDE, console, or nothing at all — runs the same way.

## Decision (proposed)

Extract the manifest interpreter into a decoupled toolchain, published from the **nano-ide** monorepo
(the Urban tooling home) as `@nanobpm/*` packages, consumable by apps via `npm:`/npm and by the
console as an ordinary library dependency.

### 1. `@nanobpm/urban-runtime` — the interpreter (the headline)

A **library** that takes a parsed, validated `nano.app.json` (ADR 0027) plus a host context and
*materializes* it against a Nano engine:

- **deploy** the referenced `models` (BPMN/DMN/forms) via the SDK,
- **provision** each `data` source and run its `migrations`, exposing typed accessors for `types`
  (ADR 0024/0040 — this answers Nano Workforce gap **C**),
- **start** the declared `workers`, mapping `workers[].handler` → code modules and draining via the
  Falcon push path (this answers gap **A**'s worker-start),
- **mount** `surfaces` — the `taskInbox` forms host and `chat` (ADR 0026; gap **E**),
- **install** `triggers` — the inbound webhook/connector host that publishes the declared message
  (ADR 0025; gap **D**),
- **apply** `security` (roles/providers, ADR 0028).

The runtime is a **dependency the app imports**, not a black box: `import { createUrbanApp } from
"@nanobpm/urban-runtime"` returns a handle the app (or a host) starts, inspects, and stops. Each
block above is a **module** with a stable seam, so a host can mount a subset (e.g. the console mounts
surfaces itself but reuses deploy + workers + datasource).

**Runtime-agnostic core, thin adapters (ADR 0038/0036).** The core is plain TypeScript with no
runtime-specific imports. Two small adapters supply the host primitives that differ — HTTP serving,
filesystem, SQLite, process spawn, env — one for **Node** and one for **Deno**. Node-first per ADR
0038 (the default for CI, the console, and embedded use); first-class Deno per ADR 0036/0009 (the
`app-deno-gui` template and `deno compile` single-binary path). The *same* app runs unmodified on
either — the adapter is selected at load, not baked into app code.

### 2. `create-urban-app` — the scaffolder

`npm create urban-app@latest my-app` (Node) / `deno run -A npm:create-urban-app my-app` (Deno).
Pure code-generation, **zero runtime logic**: it materializes a repo — a `nano.app.json`, one
process, one form, a datasource migration, a worker module, and the `deno.json`/`package.json` that
wires `@nanobpm/urban-runtime` and an `urban` task. The output is a repo that **already knows how to
run itself** (`urban run`). It seeds from the existing `packages/app-deno-gui` template and the
Nano Workforce layout, and offers a couple of presets (headless workers-only vs. workers+surfaces).

### 3. `urban` — the CLI (a thin front-end)

A small `bin` over the runtime library: `urban scaffold`, `urban dev` (watch/reload), `urban run`
(materialize + serve), `urban deploy` (models only), `urban check` (validate the manifest against the
ADR 0027 schema). It holds no logic the library doesn't — it is argument-parsing + the adapter
selection. This is the Delphi command-line compiler to the runtime's VCL.

### 4. Hosts are interchangeable

The invariant this ADR establishes:

> **The manifest is the contract. `@nanobpm/urban-runtime` is the interpreter. Hosts — CLI, IDE,
> console, or a bare process — are interchangeable front-ends over the same library.**

The **console** stops being the interpreter and becomes *one host*: it keeps only genuinely
host-level concerns — multi-tenancy, edge auth, the visual editors, the marketplace — and calls the
same `@nanobpm/urban-runtime` everything else calls (this **revises** ADR 0026's run loop and
generalizes ADR 0041's headless run into the primary entrypoint). An **agent** clones a repo and
`urban run`s it with no console; a **user** opens the identical repo in the console unchanged. No
per-host reimplementation of deploy/migrate/worker-start/surface-mount.

## Consequences

- **Nano Workforce's gap epic is reframed.** Gap **A** ("manifest → runtime") *is* building
  `@nanobpm/urban-runtime` + the scaffolder/CLI, and it now lands in **nano-ide** (published), not as
  app-local glue. Gaps **C** (datasource/domain), **D** (triggers host), **E** (forms surface)
  become **modules of the runtime**, not app-local or console-only features. Nano Workforce becomes
  the runtime's first consumer and conformance probe.
- **The console shrinks.** "Magical" console behaviour moves down into a versioned library both the
  console and standalone apps share, killing a whole class of console/standalone drift.
- **Two runtimes, one core.** Honouring ADR 0038 + 0036 costs an adapter boundary, but buys the
  Deno single-binary (`deno compile`) *and* the Node/embedded/CI path from one codebase.
- **Vite-not-CRA is a standing constraint.** The runtime dependency stays explicit and legible; there
  is no hidden `react-scripts` and no eject cliff. Agents and users can read and override the wiring.
- **A new spec-first surface.** The runtime's manifest→action contract should be codegen'd from the
  ADR 0027 schema the same way `spec-console` is, so the Rust console host and the TS runtime never
  drift.

## Open questions

1. **Package boundaries.** One `@nanobpm/urban-runtime` with sub-path exports (`/datasource`,
   `/triggers`, `/surfaces`), or a small family (`@nanobpm/urban-runtime-core` + `-node` + `-deno`)?
   Lean core + adapters is likely, but the surfaces module drags in a UI stack that headless hosts
   shouldn't pay for.
2. **Console consumption.** Does the Rust console call the TS runtime out-of-process (spawn `urban`)
   or does it keep its Rust deploy path and reuse only the TS surface/trigger modules? The spawn path
   is cleaner but crosses a language boundary on the hot dev loop.
3. **Embedded engine.** How does the runtime compose with Bernd (ADR 0005) so `urban run` can offer
   an all-in-one local engine + app for the pure-desktop Delphi use case, not only remote-connect.
4. **Where `urban` and `create-urban-app` publish.** Inside nano-ide's workspace + release flow, or
   their own repo? nano-ide is the natural home (templates + packs already live there).
