# Nano repositories — how the pieces fit

Nano is developed across **five repositories**. This is the map: what each one
is, where each piece lives, and how they depend on one another.

| Repo | Product name | Role |
| --- | --- | --- |
| [`nanobpm/nano-bpm`](https://github.com/nanobpm/nano-bpm) | **Nano BPM** | The core product: engine, gateway server, web console, embeddable engine libraries (`nano-bernd`, `engine-wasm`), and schemas. The source of truth. |
| [`nanobpm/nano-ide`](https://github.com/nanobpm/nano-ide) | **Nano IDE** | Console extension packs (languages, app templates, examples, themes, triggers/connectors) **and** the published code-first stack: the Urban app runtime (`@nanobpm/urban`), the `create-urban-app` scaffolder, and the code-first workflow SDK (`@nanobpm/workflow`). Published to npm. |
| [`jwulf/nano-sdk-js`](https://github.com/jwulf/nano-sdk-js) | **Nano SDK (JS)** | `@nanobpm/nano-sdk` — the **engine-transport spine**: a drop-in replacement for `@camunda8/orchestration-cluster-api` that transparently upgrades the hot paths to Nano's **Falcon** protocol (and offers an in-process `embedded` transport). Cross-runtime (Node, Bun). Published to npm. |
| [`jwulf/c8ctl-plugin-nano`](https://github.com/jwulf/c8ctl-plugin-nano) | **c8ctl Nano plugin** | A `c8ctl` CLI plugin that installs, runs, and manages a local Nano BPM cluster; ships the prebuilt gateway binary; and turns CLI agents into job workers. |
| [`nanobpm/bojtos`](https://github.com/nanobpm/bojtos) | **Bojtos** | The publishable in-browser BPMN demo framework: `@nanobpm/bojtos-kit` (framework-agnostic) and `@nanobpm/bojtos-react` (React bindings), built on `@nanobpm/engine-wasm`. Extracted from nano-bpm (ADR 0043); public, published to npm. |

At a glance, the dependency direction is one-way into Nano BPM:

```
   nano-ide packs ────────(npm, "nano-ide-ext" keyword)────────▶ Nano BPM console
                                                                     │
   nano-ide code-first stack                                         │
   (@nanobpm/urban, create-urban-app, @nanobpm/workflow)            │
        │ depends on                                                 │
        ▼                                                            │
   nano-sdk-js (@nanobpm/nano-sdk) ── the transport spine ──▶  Nano BPM  ◀── source of truth
                    (REST v2 · Falcon · embedded)       (engine + server + console
                                                         + libs + schemas + Falcon)
                                                             │  gateway binary (release)  ▲
                                                             ▼                            │
                          c8ctl-plugin-nano ──(prebuilt binary + `c8ctl nano`)────────────┘

   bojtos (@nanobpm/bojtos-kit, -react) ──(consumes @nanobpm/engine-wasm, npm)──▶ Nano BPM
```

---

## 1. Nano BPM — the core (`nanobpm/nano-bpm`)

A Rust + TypeScript monorepo that produces the `nanobpmn` gateway binary and the
embeddable engine. It owns the REST contract, the Falcon protocol, the console,
the extension-manifest schema, and the engine release pipeline. **Everything else
depends on it; it depends on nothing in the other four repos.**

Where each piece lives:

| Path | What |
| --- | --- |
| `engine-core/` | The deterministic, event-sourced BPMN engine (pure Rust crate, no I/O). |
| `server/` | The `nanobpmn` gateway binary: Camunda 8-compatible **v2 REST API**, the **Falcon** streaming protocol, append-only journal, SQLite read model, optional multi-node Raft replication, and the built-in **web console** host. |
| `server/src/console/extensions.rs` | The **authoritative extension-manifest schema** that Nano IDE packs target (see repo 2). |
| `server/src/console/agent_brief.rs` | The **agent authoring surface**: `GET /agent` (+ `/agent.md`, `/llms.txt`) serves a live per-node brief that teaches an external agent how Nano works and how to author + link in an Urban app (ADR 0051). "Point your agent here." |
| `console/` | The built-in web console SPA — the **RAD IDE** (Projects: BPMN, DMN, forms, and Page Composer authoring), Extensions, Explorer, **Traces** (per-instance execution timelines), Workers, guided journeys, and Topology/Metrics. See the console tour in [`README.md`](../README.md#web-console) and [`USERGUIDE.md`](../USERGUIDE.md#tour-the-web-console). |
| `engine-wasm/` + `clients/nano-bernd/` | The engine compiled to WASM and wrapped as an **embeddable library** with two hosts: `@nanobpm/nano-bernd` (npm) and `io.github.jwulf:nano-bernd` (JVM / Maven Central). `@nanobpm/engine-wasm` (the wasm-pack build) is published to npm and consumed by the **Bojtos** framework (repo `nanobpm/bojtos`). |
| `clients/` | Client transports (e.g. the `node-stream` command-stream client). |
| `processos/` | ProcessOS — a companion binary built from the same engine; it ships in the same `v*` release train as the gateway (see below). |
| `spec/`, `spec-app/`, `spec-console/` | JSON Schemas / OpenAPI specs, published to `nanobpm.io`. |

> **Note.** The code-first workflow SDK (`@nanobpm/workflow`) originated here under
> `workflow/` but has been **relocated to `nanobpm/nano-ide`** (`packages/workflow`)
> — it is a self-contained engine client with no coupling to the engine, server, or
> console, so it now lives alongside the rest of the code-first stack and publishes
> from there (ADR 0044 relocation note; nano-ide ADR 0054/0055).

**Release trains** (see [`RELEASE.md`](../RELEASE.md)). The three coordinated
engine trains are cut from the same `main` commit:

1. **Gateway binary** (`v*`) → published to the **`c8ctl-plugin-nano` repo
   releases** + an S3 mirror. This is the binary the CLI plugin ships (repo 4).
   ProcessOS ships alongside it.
2. **`@nanobpm/nano-bernd`** (embedded engine, npm).
3. **`io.github.jwulf:nano-bernd`** (embedded engine, JVM / Maven Central).

A further npm train publishes on its own cadence from this repo:

- **`@nanobpm/engine-wasm`** — the wasm engine build
  (`release-bojtos-npm.yml`, OIDC; see [`docs/releasing-bojtos-npm.md`](releasing-bojtos-npm.md)).
  The Bojtos framework packages (`@nanobpm/bojtos-kit` + `@nanobpm/bojtos-react`)
  that consume it are published from the separate
  [`nanobpm/bojtos`](https://github.com/nanobpm/bojtos) repo.

---

## 2. Nano IDE — console packs + the code-first stack (`nanobpm/nano-ide`)

A monorepo of npm packages under `packages/*`. It began as **extension packs**
for the Nano BPM console's RAD IDE, and has grown to also host the **published
code-first stack** (the Urban app runtime + the workflow SDK).

### Extension packs — *content for* the console

Each pack is a self-contained extension; it is not a running service.

- **Contract.** Every pack ships a `nano-ide.ext.json` manifest. That manifest is
  a **mirror of the authoritative schema in Nano BPM**
  (`server/src/console/extensions.rs`) — Nano BPM is the single source of truth;
  Nano IDE targets it.
- **Discovery.** The console finds packs by the `nano-ide-ext` npm keyword,
  installs the tarball into the workspace, reads the manifest, and wires the pack
  in. No `preinstall`/`postinstall` scripts run.
- **Baseline.** The `node` runtime and the `node-gui` app template ship built
  into the server so the console works offline with zero installs; every other
  pack (including `rust`) is installed from here.
- **Pack kinds:** `lang` (file types + toolchain + templates), `app` (a runnable
  project template — including the code-first `app-workflow` scaffold),
  `example` (a complete app copied into a new project), `theme` (console colour
  themes as pure data), and `trigger`/**connector** (event sources that start
  processes and the outbound-I/O edge, ADR 0050 — e.g. `trigger-mqtt`,
  `connector-slack`).

### The code-first stack — *published SDKs*

Unlike packs, these are ordinary npm libraries a developer depends on directly to
build and run apps outside the IDE. They all talk to the engine through the one
transport spine, `@nanobpm/nano-sdk` (repo 3; ADR 0055):

- **`@nanobpm/urban`** — the Urban app runtime, derivation toolkit, and CLI in
  one: a decoupled manifest interpreter that runs a `nano.app.json` app (durable
  processes, typed datasource, forms, triggers, surfaces) on Node, with
  interchangeable hosts (CLI / IDE / standalone) (ADR 0052).
- **`create-urban-app`** — the scaffolder: `npm create urban-app@latest` /
  `npm create urban-app@latest` produces a repo that already knows how to run
  itself against a Nano gateway (ADR 0052).
- **`@nanobpm/workflow`** — the code-first durable-orchestration SDK
  (`defineFlow`): derives an executable BPMN model, job types, and correlation
  wiring, then hosts a generic worker (ADR 0044/0045/0047/0048). The
  `app-workflow` pack scaffolds a project that depends on it.

**Direction of dependency:** `nano-ide` → `nano-bpm` (packs target its manifest
contract; the runtime libraries target its REST v2 + Falcon contract, via
`@nanobpm/nano-sdk`). Nano BPM's console consumes the packs **at runtime, from
npm**.

> **Authoring packs.** The [**Extensions authoring & publishing
> guide**](extensions.md) documents the manifest, every pack kind with a minimal
> example, the install/trust model, and how to publish.

---

## 3. Nano SDK (JS) — the transport spine (`jwulf/nano-sdk-js`)

`@nanobpm/nano-sdk` is a client library, published to npm, and the **single
engine-transport spine** for the code-first stack (ADR 0055). It is a **drop-in
replacement for `@camunda8/orchestration-cluster-api`**: existing Camunda 8 code
keeps working, and when the SDK detects a Nano server it transparently upgrades
the two throughput-critical paths — `createProcessInstance` and `createJobWorker`
— to Nano's **Falcon** streaming protocol (crash-resilient reconnect with
backoff), with REST for everything else. It also offers an **`embedded`**
transport (an in-process engine host, no separate gateway). It is **cross-runtime**
— verified on Node and Bun (Web `fetch` transport + a global-`WebSocket`-first
Falcon client).

Instead of each code-first library hand-rolling its own deploy / create / job /
message / signal / user-task / decision calls, they all go through this one client
— and the client is **exposed to app authors**, so an Urban app can reach any
engine capability directly (ADR 0055).

**Direction of dependency:** `nano-sdk-js` → `nano-bpm` (targets its REST v2 +
Falcon contract, and embeds its engine). `nano-ide`'s code-first stack →
`nano-sdk-js`. Nothing in Nano BPM depends on it.

---

## 4. c8ctl Nano plugin — run + manage + agentic workers (`jwulf/c8ctl-plugin-nano`)

A plugin for [`c8ctl`](https://github.com/camunda/c8ctl) (the Camunda 8 CLI). It
adds the `c8ctl nano` command and is the easiest way to run and operate Nano BPM
locally.

- **Ships the prebuilt gateway binary.** Per-platform npm `optionalDependencies`
  (under the `@nanobpm/` scope) carry the compiled `nanobpmn` binary that Nano
  BPM's release workflow produced — so there is nothing to compile.
- **Cluster ops:** `start`, `stop`, `status`, `restart`, `logs`, `pause`,
  `resume`, `clean`, `set`, `config`, `update`.
- **Agentic job workers:** `hire` / `work` turn an interactive CLI agent harness
  (Copilot CLI, Claude CLI, …) into a Nano BPM job worker.

**Direction of dependency:** `c8ctl-plugin-nano` → `nano-bpm` (consumes its
compiled binary). Nano BPM's README points users here for the quick start.

---

## How they fit together

**Nano BPM is the single source of truth.** It compiles the gateway binary and
the embedded engine, and owns the REST contract, the Falcon protocol, the
console, and the extension-manifest schema. The other three repos consume it —
none is a dependency of Nano BPM.

Publish/consume flows connecting them:

- **Binary distribution.** Nano BPM CI builds the gateway binary → publishes it
  to `c8ctl-plugin-nano` releases (+ S3) → the plugin packages it per platform on
  npm → end users get it via `c8ctl nano`.
- **Extensions.** Nano IDE publishes packs → npm → Nano BPM's console discovers
  and installs them at runtime by the `nano-ide-ext` keyword.
- **Code-first apps.** Nano IDE publishes the code-first stack (`@nanobpm/urban`,
  `create-urban-app`, `@nanobpm/workflow`) → npm → developers scaffold and run
  Urban / code-first apps against a running gateway, all talking to it through
  `@nanobpm/nano-sdk`.
- **Transport spine.** `nano-sdk-js` publishes `@nanobpm/nano-sdk` → npm → the
  code-first stack (and any application) talks to a Nano (or Camunda 8) gateway,
  auto-upgrading to Falcon on Nano, or embedding the engine in-process.

**End-user paths:**

- **Run a cluster:** install `c8ctl` → `c8ctl load plugin c8ctl-plugin-nano` →
  `c8ctl nano start`.
- **Extend the IDE:** from the console UI, install a pack by its `nano-ide-ext`
  keyword.
- **Build a code-first app:** `npm create urban-app@latest`, or author a flow with
  `@nanobpm/workflow`'s `defineFlow`.
- **Author with an agent:** point an agent at a node's `/agent` endpoint (ADR
  0051) — it learns to build an Urban app and link it in.
- **Talk to a gateway from code:** depend on `@nanobpm/nano-sdk` (drop-in Camunda
  client, upgrades to Falcon on Nano, or runs the engine embedded).
- **Embed the engine:** depend on `@nanobpm/nano-bernd` (npm) or
  `io.github.jwulf:nano-bernd` (JVM); run it **in the browser** with
  `@nanobpm/bojtos-kit` / `@nanobpm/bojtos-react`.
