# Nano BPM — User Guide

**Nano BPM is a fast, single-binary process engine that speaks the Camunda 8 v2
REST API.** You run BPMN processes on it, drive them with any Camunda 8 client,
and operate the whole thing — single node or a replicated cluster — from one
executable with no runtime dependencies and a built-in web console.

This guide is task-focused. It walks you through running Nano, modelling and
testing a process, writing the workers that do the work, connecting your own
application, and operating a cluster in production. You do **not** need source
code or a build toolchain to follow it — the binary is distributed prebuilt.

> **Just want to try it?** Jump to [Get started with c8ctl](#get-started-with-c8ctl),
> then [Tour the web console](#tour-the-web-console). Everything else is here when
> you need it.

## Get started with c8ctl

The easiest way to run and manage Nano BPM — a single node or a whole cluster — is
the **[c8ctl](https://github.com/camunda/c8ctl)** CLI with the
**[c8ctl-plugin-nano](https://github.com/jwulf/c8ctl-plugin-nano)** plugin. The
plugin ships a prebuilt Nano binary for your platform and installs it for you, so
there is nothing to compile.

### The one-liner (recommended)

The fastest way to go from zero to a running **Nano Workforce** — the engine, a
supervised workforce of hired coding agents, and the Workforce app — is the
one-command installer:

```bash
curl -fsSL https://nanobpm.io/install.sh | sh
```

It installs the Camunda 8 CLI and the `c8ctl-plugin-nano` plugin, lets you pick
which of your installed coding harnesses to hire (and with which model and how
many instances), composes a workforce manifest from those choices, brings the
engine and workforce up, then installs and runs the Workforce app. It never
runs `sudo` on your behalf — it installs the CLI with `npm i -g`, so where the
global package lands depends on your Node/npm setup (a user-owned prefix such as
`nvm` needs no elevation; a system-owned prefix may require you to fix your npm
prefix first).

Prefer to read the script before running it? Download, inspect, then run:

```bash
curl -fsSL https://nanobpm.io/install.sh -o install.sh
less install.sh
sh install.sh
```

`https://nanobpm.io/install.sh` always mirrors the script on `main` in the
[nano-workforce](https://github.com/nanobpm/nano-workforce/blob/main/install.sh)
repository — the single source of truth.

The rest of this section sets up **just the engine**, by hand. Use it when you
want a single node or a cluster without the workforce, or to understand exactly
what the installer automates for you.

### Prerequisites

Install these tools before you start.

- **Node.js 22.18 or later.** The Camunda 8 CLI runs on Node.js. Download Node.js
  from [nodejs.org](https://nodejs.org).
- **The Camunda 8 CLI.** Install the CLI with `npm i -g @camunda8/cli`. This
  package provides the `c8ctl` command.

Verify each tool:

```bash
node --version
c8ctl --version
```

Then manage Nano with `c8ctl`:

```bash
# Load the plugin (installs the matching prebuilt binary for your OS/arch)
c8ctl load plugin c8ctl-plugin-nano

# Start a single-node cluster on port 8080
c8ctl nano start

# Start a 3-node cluster (ports 8080, 8081, 8082)
c8ctl nano start 3

# Start a 3-node Raft-replicated cluster (RF=3 enables replication automatically)
c8ctl nano start 3 --rf 3

# Show cluster status and per-node health
c8ctl nano status

# Tail a node's log (node ids are 0-indexed, so a single node is node 0)
c8ctl nano logs 0 --follow

# Simulate a node failing and recovering
c8ctl nano pause 0
c8ctl nano resume 0

# Stop the cluster (engine data retained); add --purge to delete engine data
c8ctl nano stop
```

`c8ctl nano` keeps your **authoring assets** (BPMN models and worker code) in a
shared workspace that survives `stop` and `clean`, separate from the throwaway
per-node engine data. It also wires every node's ports, ids, partitions,
replication, and data directories for you.

Once a cluster is up:

- Point any **Camunda 8 v2 REST client** (or the
  [Nano SDK](#connect-your-application)) at `http://127.0.0.1:8080/v2`.
- Open the **web console** at `http://127.0.0.1:8080/console`.

A demo process (`processDefinitionId: "demo"`) is pre-deployed at startup, so you
have something to run immediately.

### Compose and run a workforce by hand

The [one-liner installer](#the-one-liner-recommended) composes a **workforce
manifest** for you. To build one by hand — or to curate a fleet you can diff,
edit, and copy between machines — use the `hire` → `workforce` commands. A
*workforce* is a fleet of supervised **coding-agent** workers (each running a
harness such as `copilot`, `claude`, or `qwen` on your `PATH`) described by a
single manifest file. This needs a running engine (above) and at least one agent
harness installed.

**1. Hire agent profiles.** A profile is a reusable definition — a rank, the
harness command, an optional model, and any launch args (the examples below set
no model, so the harness uses its own default):

```bash
# Create a "copilot" senior profile that runs `copilot --allow-all`
c8ctl nano hire --name copilot --rank senior --command copilot --arg --allow-all
# Create a "qwen" profile that runs the `qwen` harness
c8ctl nano hire --name qwen --rank senior --command qwen
c8ctl nano hire --list
```

**2. Compose the manifest** with `workforce add`. `--instances N` spawns N copies
of a profile; `--auto` serves every deployed agent job type, or `--roles a,b`
scopes it to specific roles:

```bash
c8ctl nano workforce add copilot --instances 5 --auto
c8ctl nano workforce add qwen --instances 2 --roles pr-review,feature
```

The manifest is a portable JSON document at
`<stateHome>/workforce/<name>.json` (`default.json` unless you pass
`--manifest <name>`), where `<stateHome>` is c8ctl's OS-dependent state
directory (e.g. `~/.local/share/c8ctl-nano` on Linux). It is meant to be read,
hand-edited, diffed, and copied between machines:

```json
{
  "version": 1,
  "name": "default",
  "workers": [
    { "profile": "copilot", "instances": 5, "roles": "auto" },
    { "profile": "qwen", "instances": 2, "roles": ["pr-review", "feature"] }
  ]
}
```

Each entry references a hired `profile` and sets `instances` (how many copies)
and `roles` — either `"auto"` (serve every agent job type, no capability gate) or
a list, each mapped to a `<rank>:<role>` job type. Optional fields: `autoScope`
(narrow `--auto` to a single `bpmn:process` id prefix — one app/network) and
`args` (verbatim flags appended to each worker).

**3. Bring the whole fleet up** under a supervisor with one command:

```bash
c8ctl nano workforce start                      # the default manifest
c8ctl nano workforce start --manifest review-only
```

`start` reconciles the running supervisor to the manifest using deterministic
`wf-<manifest>-<profile>-<index>` worker names, so re-running an unchanged
manifest starts, stops, and restarts nothing (it is idempotent).

**4. Inspect and manage** the fleet:

```bash
c8ctl nano workforce status --json   # desired (manifest) joined with actual (live supervisor)
c8ctl nano workforce list
c8ctl nano workforce stop            # bring the fleet down
```

The supervisor is a fleet runner, so you can also adjust individual workers
without editing the manifest:

```bash
c8ctl nano supervisor add copilot --instances 3 --auto   # or --roles pr-review,feature
c8ctl nano supervisor restart copilot
c8ctl nano supervisor status
c8ctl nano supervisor stop            # stop the daemon and all its workers
```

> **GitHub authentication.** Agent workers clone each task's repository over
> HTTPS, so every worker needs *non-interactive* GitHub credentials. Without
> them a job fails to provision with
> `fatal: could not read Username for 'https://github.com': terminal prompts disabled`.
> Two ways to provide them:
>
> - **Recommended — `gh` credential helper.** Run `gh auth login` and enable the
>   git integration (`gh auth setup-git`). This writes a credential helper into
>   your global `~/.gitconfig`, which every worker's `git clone` picks up
>   automatically — nothing to export. Verify with `gh auth status`.
> - **Or a token in the environment.** Export a token in the shell that runs
>   `workforce start`; the supervisor and all its workers inherit it:
>   `export GITHUB_TOKEN="$(gh auth token)"` (or any PAT with `repo` scope).

> **macOS + a remote engine over SSH (Local Network Privacy).** If you run the
> fleet on a Mac over SSH against an engine on your **LAN** (e.g.
> `merlin.local:8080`, `192.168.x.x`), workers may show `running` but sit idle —
> `listening on 0 job type(s)`, agentic `disconnected`, logs spinning on
> `activateJobs failed: fetch failed` / `reconcile skipped — fetch failed`. On
> **macOS 15 Sequoia / 26 Tahoe** the supervisor's session-independent
> **LaunchAgent** (installed so the fleet survives SSH logout) is a separate
> **Local Network Privacy** identity that is **not** granted LAN access — so it
> cannot reach a LAN engine, even though internet hosts and an interactive SSH
> shell work. The signature is `EHOSTUNREACH` to the engine's LAN IP from the
> service. Fix it with one of:
>
> - **Grant Node.js Local Network access** on the Mac's GUI: **System Settings →
>   Privacy & Security → Local Network**, enable the **Node.js** runtime entry
>   (it may appear as *“Node.js Foundation”* / *“App Background Activity”*), then
>   `c8ctl nano supervisor stop && c8ctl nano supervisor start`.
> - **Route over Tailscale** — traffic over the `utun` interface is not treated
>   as “local network”, so point the engine at the tailnet address (e.g.
>   `NANO_REST_URL=http://<host>.<tailnet>.ts.net:8080`).
> - **Run in the SSH session** (`c8ctl nano supervisor uninstall`) — inherits the
>   Terminal grant, but then pin a `tmux`/SSH session so it does not die on
>   logout.
>
> See the [c8ctl Nano plugin README](https://github.com/jwulf/c8ctl-plugin-nano#surviving-ssh-logout-supervisor-install--uninstall)
> (“Surviving SSH logout”) for the full explanation.

## Run the binary directly

You can also run the binary yourself (the same one the plugin installs),
configuring it entirely through environment variables:

```bash
NANOBPMN_DATA_DIR=/var/lib/nanobpmn PORT=8080 ./nanobpm-gateway-rest-server
```

On startup it prints the URLs it serves:

```text
Nano BPM is up:
  Landing page   http://127.0.0.1:8080/
  Web console    http://127.0.0.1:8080/console
  API reference  http://127.0.0.1:8080/swagger
  REST API       http://127.0.0.1:8080/v2
  Metrics        http://127.0.0.1:8080/metrics
```

The most common settings:

| Variable | Effect |
|---|---|
| `PORT=<n>` | HTTP listen port (default `8080`). |
| `NANOBPMN_DATA_DIR=<dir>` | Durable data directory (event log + read model). **Without it, the server runs fully in-memory and loses everything on exit.** Set it for anything you want to keep. |
| `NANOBPMN_WORKSPACE_DIR=<dir>` | Where the console stores your projects and workers (default `./nanobpm-workspace`; projects live under `<workspace>/projects/` by default, unless `NANOBPMN_PROJECTS_DIR` moves them). Survives deletion of the engine data dir. |
| `NANOBPMN_PROJECTS_DIR=<dir>` | Advanced: override the projects root directly (default `<workspace>/projects`). |
| `DEBUG_REST=1` | Log every REST request/response (method, URI, status, latency). Leave off in production. |

The clustering and tuning variables are covered in
[Run a cluster](#run-a-cluster) and
[Tune durability and performance](#tune-durability-and-performance).

## Tour the web console

Open `http://127.0.0.1:8080/console`. The console is a single-page app whose
navigation depends on the build **profile**: the **Studio** (authoring) profile
adds the **Studio** and **Extensions** tabs, while the **Observe** (operator)
profile shows only the runtime tabs below. The root `/` is a small landing page,
and `/swagger` is an **offline** Swagger UI for the REST API (nothing is fetched
from the internet).

| Tab | Profile | What it's for |
|---|---|---|
| **Studio** | Studio | **Nano Studio** — the Rapid Application Development IDE. Open a project and author BPMN models, DMN decisions, forms, and pages; **deploy**, **start** instances, and **test** a model in your browser. See [Model and test a process](#model-and-test-a-process). |
| **Extensions** | Studio | Install extension packs — agentic SDLC, languages, app templates, example apps, triggers, and themes. |
| **Topology** | both | Cluster, partition, and Raft overview with **live per-node health** — each node is probed every few seconds for reachability, version, and round-trip latency. |
| **Metrics** | both | A live performance dashboard — process starts/s, jobs/s, active processes, connected clients, commit-pipeline depth, journal/fsync timings, memory — with sparklines, and a per-node breakdown in a cluster. |
| **Explorer** | both | A live process-instance explorer — variables, jobs, incidents, and the BPMN XML for each running or completed instance. |
| **Traces** | both | **Execution traces** folded from the engine event stream — a per-instance timeline of elements visited, jobs created/completed, and variable snapshots. |
| **Workers** | both | Author TypeScript job workers in the browser, run them as sandboxed Deno processes, watch a live fleet view, and export them as a standalone app. See [Write and run job workers](#write-and-run-job-workers). |

(Two more views, **Config** and **Credits**, are reachable by route but are not on
the navigation rail.)

## Model and test a process

Models are authored inside a **project** in the **Studio** tab. To get started:

1. Open the **Studio** tab and **create or open a project** (use **New project**
   to scaffold one from a template).
2. In the project workspace, use **New file → Model** to create a `.bpmn` file (or
   open an existing one under `resources/processes/`). DMN decisions, forms, and
   Page Composer pages are authored the same way, alongside the model.

- **Create / edit** a `.bpmn` model, **Deploy** it to the engine (deployment is
  idempotent — redeploying an unchanged model is a no-op; a changed model becomes
  the next version), then **Start instance** once the saved model matches what is
  deployed.

### Test a model in the browser (μ-nano)

Click **Test** to execute a model **entirely in your browser** — no cluster
round-trip, fully offline. Test mode is powered by **μ-nano** ("micro-nano"), a
compact (~0.5 MB) WebAssembly build of the *exact same* Rust engine the cluster
runs. Because it is the real engine and not a re-implementation, token flow,
gateways, timers, DMN, and FEEL expressions behave precisely as they will on the
server.

To run a test and inspect what happened:

1. Open a model and click **Test**.
2. **Start an instance**, optionally supplying initial variables as JSON.
3. As each job activates, **complete** it with mock result variables, **fail** it,
   or throw a BPMN **error** — μ-nano advances the tokens accordingly.
4. **Fast-forward** the virtual clock to fire timers without waiting in real time.
5. Watch tokens move on the diagram and read the **execution trace** — the ordered
   list of elements visited, jobs created/completed, and variable snapshots — to
   confirm the model behaves as intended before you deploy anything.

## Write and run job workers

A *worker* is the code that performs a process's service tasks. The **Workers**
tab lets you author them in TypeScript right in the browser and run them as
sandboxed **Deno** subprocesses, with a live fleet view (status, throughput,
completed/failed, uptime, restarts) and streamed logs.

> **Running workers needs [Deno](https://deno.com) installed** on the host (the
> `deno` binary on `PATH`, or pointed to by `NANOBPMN_DENO_BIN`). Without it you
> can still author and export worker code; only *starting* a worker is
> unavailable. Install Deno from <https://deno.com>.

A worker is a directory with a `worker.ts` that declares a handler:

```ts
import { defineWorker } from "@nanobpm/worker";

defineWorker({
  type: "my-job",
  maxParallelJobs: 10,
  async handle(job) {
    // job.variables holds the activated job's variables.
    return { result: 42 };        // resolves -> completes the job with { result: 42 }
    // or: job.fail("boom") / job.error("CODE", "msg")
  },
});
```

### A 90s IDE, on purpose

The worker editor is deliberately styled after the **integrated development
environments of the early-to-mid 1990s** — Borland Delphi, Microsoft Visual Basic
— where the wiring was invisible and you focused only on what *can't* be
automatically connected. The BPMN model is the form; the engine binds tasks to
workers by job type; the transport, retries, timeouts, and dependency resolution
are handled for you. You write only the handler body. That ethos drives the
editor's code intelligence:

- **Full IntelliSense for the worker SDK, offline.** `defineWorker`, the `job`
  object (`job.variables`, `job.complete`, `job.fail`, `job.error`, `job.jobKey`,
  `job.processInstanceKey`, …) and every option auto-complete and type-check with
  no network access.
- **Types for any npm package.** Import *any* package — `import _ from "lodash"`,
  `import { ... } from "@camunda8/orchestration-cluster-api"` — and the editor
  fetches its type definitions on the fly so completion, hovers, and signatures
  light up. This needs network access and degrades silently to SDK-only
  IntelliSense when offline.
- **Reusable logic across files.** Split a worker into multiple files and
  `import { helper } from "./helper.ts"` — siblings resolve with full types. For
  logic shared across *every* worker, drop a module into the workspace **Shared
  library** (the `📚 Shared library` entry below the worker list) and import it
  anywhere with the `@lib/` alias — `import { fmtMoney } from "@lib/money.ts"`.
  There is nothing to configure: the alias resolves in the editor, at runtime, and
  in an exported app.

## Export workers as an application

Workers don't have to run inside the console. From the **Workers** tab, click
**Export app**, tick the workers you want (all selected by default), and
**Download zip** to get a self-contained, runnable application — handy for
shipping a worker fleet to another machine or running it outside Nano.

The downloaded `nano-workers-app.zip` unpacks to a project with a `main.ts`
entrypoint, a `deno.json` (import map + `deno task start`), a `README.md`, the
worker SDK, each selected worker's source, and a `resources/` folder.

On startup the app **deploys every `.bpmn` file in `resources/`** to the target
engine, then runs the bundled workers. Drop your own models into `resources/`
before starting. Run it with Deno:

```bash
cd nano-workers-app
# Point at your Nano gateway (defaults to http://127.0.0.1:8080):
export NANOBPMN_BASE_URL=http://127.0.0.1:8080
deno task start            # runs with --allow-net --allow-read --allow-env
```

**Dependencies travel with the app.** The exporter scans the selected workers'
imports and writes a dependency manifest into the app's `deno.json`, so a worker
that imports `lodash` or `@camunda8/orchestration-cluster-api` works out of the
box — no manual dependency wrangling.

## Connect your application

Nano serves the Camunda 8 Orchestration Cluster **v2 REST API**, so existing
Camunda 8 clients and tooling work against it unchanged. Browse the full,
interactive API at **`/swagger`** (served offline). For the precise supported
subset — which BPMN elements execute, which endpoints are served, and the
behavioural differences to expect — see the
[**Camunda compatibility subset**](docs/camunda-compatibility.md).

You have two ways to drive the engine:

- **REST API** (`/v2`) — the standard Camunda 8 v2 surface: deployments, process
  instances, jobs, incidents, messages, and search. See
  [Deploy processes and run instances](#deploy-processes-and-run-instances).
- **Falcon protocol** (`/falcon`) — a single bidirectional WebSocket that
  multiplexes process creation *and* the full job lifecycle onto one persistent,
  flow-controlled socket. It hits the same engine path as REST but avoids
  per-request setup and long-polling, and uses **credits** for backpressure
  instead of `429`/`503` + retry. It's the high-throughput ingress.

### Node / TypeScript SDK

The **`@nanobpm/sdk`** package is a drop-in replacement for
`@camunda8/orchestration-cluster-api`: **change the import and nothing
else**. The same client API, the same method signatures, the same
request/response types. What changes is the transport underneath:

- Against a **Nano** server, the SDK detects the Falcon advertisement
  in `/v2/topology` and transparently routes `createProcessInstance`
  through the credit-metered Falcon stream and switches job workers
  from long-polling to Falcon's pushed subscription.
- Against a stock **Camunda 8** server, the same code stays on the
  byte-identical REST path — no `nano` field, no upgrade, no branching
  in your code.

```diff
- import { createCamundaClient } from "@camunda8/orchestration-cluster-api";
+ import { createCamundaClient } from "@nanobpm/sdk";

  const camunda = createCamundaClient({ /* same config */ });
  await camunda.createProcessInstance({ processDefinitionId: "demo" });
```

The Falcon upgrade is on by default and can be disabled with
`CAMUNDA_FALCON=off` if you want to force REST against a Nano server.

> **Throughput tip.** A single connection's commands are processed in arrival
> order at journal-commit latency. Throughput comes from **concurrency across
> connections**. The practical worker topology is **one stream per job-type
> worker**, plus a small **pool of separate sockets for `createInstance`** so a
> burst of process creates can't block job completions.

### Rust SDK

The **`camunda-orchestration-sdk`** crate on crates.io is the same story
with **one import for both backends** — no Nano-specific crate, no
feature flag, no code change to switch between Camunda 8 and Nano:

```toml
[dependencies]
camunda-orchestration-sdk = "0.2"
```

```rust
use camunda_orchestration_sdk::CamundaClient;

let client = CamundaClient::from_env()?;                  // reads CAMUNDA_REST_ADDRESS, ...
let _ = client.create_process_instance(instruction).await?;
```

`CamundaClient` probes `GET /v2/topology` once per client. When the
response advertises Nano (Falcon), `create_process_instance` routes
through the shared, credit-metered Falcon producer and `JobWorker`
subscribes to the pushed Falcon stream. Against stock Camunda the same
client stays on plain REST. Toggle with `CAMUNDA_FALCON=off` to force
REST against a Nano server.


## Deploy processes and run instances

These are the everyday operations against the v2 REST API (all also available over
the Falcon protocol).

- **Deploy a model** — `POST /v2/deployments` with the BPMN 2.0 XML. Deployment is
  **idempotent**: a byte-for-byte-identical redeploy reuses the current version; a
  changed model becomes the next version.
- **Start an instance** — `POST /v2/process-instances` by `processDefinitionId` or
  `processDefinitionKey`. Variables on the request seed the instance. With
  `awaitCompletion: true` the call blocks (up to `requestTimeout`, default 5 s) and
  returns the final variables when `processCompleted` is `true`. *Note:* on timeout
  Nano returns `200` with `processCompleted: false` and the `processInstanceKey`
  (so you can poll), rather than Camunda's `504`.
- **Work jobs** — `POST /v2/jobs/activation` activates jobs of a type (with
  Camunda-style long-polling), `…/completion` completes a job and merges its output
  into the instance (driving downstream routing), `…/failure` sets remaining
  retries, and `…/error` raises a BPMN business error (routing to a matching error
  boundary event if there is one).
- **Recover from incidents** — when a job runs out of retries (or a condition or
  uncaught error fails), an **incident** is raised and the work parks. The recovery
  loop is: give the job retries again with `PATCH /v2/jobs/{key}` (or correct the
  data behind a gateway incident via
  `PUT /v2/element-instances/{key}/variables`), then
  `POST /v2/incidents/{incidentKey}/resolution`, which **retries the failed work**
  rather than just clearing the record.
- **Correlate messages** — `POST /v2/messages/publication` and
  `…/correlation` deliver a message to any matching open subscription, releasing a
  catch event, interrupting an activity via a boundary event, or **creating a new
  instance** for a message start event. Messages are not buffered: with no match,
  the message is dropped.
- **Observe state** — the `search`/`get` endpoints for process instances, jobs,
  incidents, and variables expose engine state. They support the full v2 query
  contract: operator filters (`$eq`, `$neq`, `$in`, `$notIn`, `$exists`, `$like`
  with `*`/`?` wildcards), multi-field sort, and all four pagination shapes
  (`limit`, offset, forward cursor, backward cursor).

> **Reads are eventually consistent.** A `search`/`get` issued in the instant
> after a write may briefly not observe it (typically sub-millisecond), exactly
> like Camunda 8's Operate read channel. The read side is a durable projection, so
> queries are served from it, not from live engine memory.

## Capture traces

Nano can record every instance's inputs so runs can be **replayed and analysed
later**. With the c8ctl plugin, add `--capture` when you start:

```bash
c8ctl nano start 3 --capture
c8ctl nano status            # shows "trace capture: on"
```

Read a trace back from any node:

```text
GET /console/api/traces/{instanceKey}
  → { creationVariables, stimuli[], <per-incident variables> }
```

Under the hood `--capture` sets `NANOBPMN_TRACE_STIMULI=1` on every node, which
records the inputs needed for deterministic replay. The capture limits are
adjustable:

| Variable | Default | Purpose |
|---|---|---|
| `NANOBPMN_TRACE_STIMULI=1` | off | Enable recorded-input replay capture. |
| `NANOBPMN_TRACE_VARIABLES_MAX_BYTES` | 16384 | Max captured variable payload bytes. |
| `NANOBPMN_TRACE_STIMULI_MAX` | 1024 | Max recorded inputs per instance. |
| `NANOBPMN_TRACE_CAPACITY` | 2000 | Max traced instances retained in memory. |
| `NANOBPMN_TRACE_FILE` | *(unset)* | Append each **finished** trace to this file as one NDJSON line, then drop it from memory. Durable across restart; memory stays bounded by the active set, so capture can stay on. Unset keeps the in-memory-only behaviour. |
| `NANOBPMN_TRACE_FILE_MAX_BYTES` | *(unset)* | Rotate the trace file at this size (`<path>.1` … `<path>.<keep>`). Unset leaves rotation to `logrotate`. |
| `NANOBPMN_TRACE_FILE_KEEP` | 5 | Rotated trace files to keep. |
| `NANOBPMN_TRACE_FILE_TAIL` | 0 | Recently-finished traces to keep in memory for the console after writing them to the file. |
| `NANOBPMN_TRACE_FILE_QUEUE` | 4096 | Bounded depth of the writer channel between the engine and the sink thread. A full queue **drops** finished traces (counted in `sink_stats().dropped`) rather than back-pressuring the engine — raise it if drops appear under load. |
| `NANOBPMN_TRACE_FILE_FLUSH_MS` | 1000 | How often the writer thread flushes its buffer to the file. A crash can lose at most one flush interval of traces. |

## Run a cluster

For fault tolerance and higher throughput, run multiple nodes. The c8ctl plugin
handles the wiring — `c8ctl nano start 3 --rf 3` brings up a 3-node,
Raft-replicated cluster and configures every node for you. Use
`c8ctl nano status`, `logs`, `pause`, and `resume` to operate it.

A few rules of thumb if you configure topology yourself:

- **Partitions ≥ nodes, always.** Each partition has exactly one writer node; the
  clean choice is **one partition led per node**. The partition count can't be
  changed in place — start fresh to change it.
- **Replication factor = copies per partition.** `RF=1` (default) is no
  replication; **`RF=3` survives one node loss**. Keep `RF` ≤ node count and use an
  odd voter count.
- **Throughput scales with led partitions, not nodes alone** — each partition is a
  single writer, so aggregate write throughput tracks the number of led
  partitions.

> Durability and replication tiers are chosen at startup from the on-disk log;
> switching tiers on an existing data directory is unsupported. Start each
> reconfigured cluster from a fresh data directory.

## Tune durability and performance

Single-node defaults are tuned for correctness and need no thought: one partition,
synchronous durability, backpressure on. The knobs below only matter once you
cluster or push a node toward saturation — pick each axis independently for your
workload.

| If you want… | Set | Effect |
|---|---|---|
| **Zero-data-loss durability** (the default) | leave `NANOBPMN_DURABILITY=sync`, `NANOBPMN_REPLICATION=quorum` | A `2xx` means fsync'd locally **and** majority-committed. Strongest guarantee, highest write latency. |
| **Lowest write latency** | `NANOBPMN_DURABILITY=async` + `NANOBPMN_REPLICATION=leader-durable` | Ack on the leader's local durable append — no fsync-before-ack wait, no follower round-trip. A just-acked tail can be lost on ungraceful leader loss (bounded, never divergent). |
| **High throughput with many workers** | Automatic (default `NANOBPMN_REPLICATE_ACTIVATION=auto`); force with `=0` or `=digest` | `auto` (zero-config) keeps the activation lease off the replication log and broadcasts a soft digest — identical to `=digest`, validated healthy at both extremes (2,400/s @ 50 KB, ~36k/s @ negligible payload). Force `=0` for plain leader-local (no digest). Avoid `=1`/`quorum` at scale: the strict replicated lease adds a per-activation quorum commit that collapses throughput. Still at-least-once. |
| **Even job drain across nodes** | `NANOBPMN_ACTIVATION_FAIRNESS=1` or `=2` | Spread the activation budget across nodes; `2` also drains the deepest backlog fastest. |
| **A producer that outpaces workers** | leave backpressure on (default), or pin `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT=<n>` | The engine sizes the in-flight watermark from measured latency and sheds excess creates with `503`, so the producer converges to the drain rate. |
| **Behaviour at the saturation ceiling** | `NANOBPMN_SLA_MODE=latency` (default) or `=admission` | `latency` keeps accepted instances fast by shedding admission (time-to-complete SLA); `admission` keeps admitting and lets latency grow (start-every-process SLA). OOM-safety rails apply in both. |
| **Bounded memory after bursts** | `NANOBPMN_IDLE_PURGE_MS`, `NANOBPMN_HISTORY_MAX_INSTANCES` | Idle-purge returns freed memory to the OS; cap retained completed instances to bound read-model growth. |

**Recommended profiles:**

- **Strong durability (default, money-movement workloads):** `RF=3`, leave
  durability/replication unset. Every ack is fsync'd and quorum-committed. Use a
  persistent data dir per node.
- **Low latency (interactive workflows, modest concurrency):** `RF=3`,
  `NANOBPMN_DURABILITY=async`, `NANOBPMN_REPLICATION=leader-durable`,
  `NANOBPMN_REPLICATE_ACTIVATION=digest`.
- **Max throughput / benchmarking:** one partition led per node,
  `NANOBPMN_REPLICATE_ACTIVATION=0`, `NANOBPMN_ACTIVATION_FAIRNESS=2`. Always
  benchmark the release binary.

### What an acknowledgement means — and what a node crash costs

In a cluster (`RF=3` recommended), the durability tier changes what a `2xx`
promises when hardware fails. Both tiers preserve ordering and lose nothing they
have already replicated; they differ only in *when* the ack is returned.

- **Strict (default: `quorum` + `sync`).** A create/complete is acked only after
  a **majority** of nodes have replicated and applied it *and* the leader has
  fsync'd it to disk. If any single node is lost — leader or follower — everything
  the client saw acknowledged is already durable on the surviving majority, so the
  promoted leader has it. **Nothing you observed is lost.** The price is one
  cross-node quorum round-trip on the critical path (a ~10 ms/job floor at low
  concurrency; group commit amortizes it to ~33k jobs/s under many workers).

- **Relaxed (`leader-durable` + `async`).** A create/complete is acked as soon as
  the **leader alone** has applied it and written it to the OS page cache (fsync
  deferred, bounded to a ~10 ms / 8 MiB window); followers replicate in the
  background. If the leader process merely crashes and restarts, it recovers from
  its own disk — no loss. Only if the leader is lost **permanently and
  simultaneously** (disk failure, or the VM destroyed) *before* followers catch up
  is the un-replicated tail — acks the client already saw — lost, and a follower
  is promoted from the most complete log it holds.

**Why relaxed is a latency trade, not a correctness hole.** Nano is at-least-once
end to end. A dropped completion just means the job's lease expires and it is
redelivered — and completion is keyed, so an idempotent worker already tolerates
it. A dropped create means the instance was never durably admitted, and the (also
at-least-once) producer retries. Relaxed never reorders and never loses anything
already replicated: the leader's local log stays the single ordered source of
truth per partition. So the trade is exact — a rare
simultaneous-permanent-leader-loss turns from *no loss* into *a bounded tail of
millisecond-scale redeliveries*, in exchange for taking the quorum round-trip off
every ack.

> ⚠️ **Two-node clusters are a trap.** An `RF=2` group has quorum 2, so losing
> *either* node halts writes — durability without availability. Use **3+** nodes
> for fault tolerance.

## Data, durability, and recovery

Nano's engine runs in memory but is **event-sourced and crash-durable**. When a
data directory is configured, every durable command is written to an append-only
journal and flushed to disk *before the response returns* — so a `200`/`204`
means "persisted and will survive a crash". On restart the journal replays to
reconstruct state exactly.

- **What persists:** deployments, instances, element progress, jobs, incidents,
  variables, completed history, and armed timers.
- **What doesn't:** job **activation locks** are deliberately volatile. A restart
  forfeits every lock, returning in-progress jobs to the activatable pool, so a
  worker simply re-activates after recovery. (Worker handlers should be idempotent
  — the standard at-least-once BPMN contract.)
- **Where it lives:** set `NANOBPMN_DATA_DIR` to a stable path. With neither a data
  dir nor a journal configured, Nano runs **fully in-memory and persists nothing**.
- **Your authoring assets are separate:** models and workers live in the
  `NANOBPMN_WORKSPACE_DIR` workspace, which survives deleting the engine data dir.

## Troubleshooting

**"Running workers requires Deno" / a worker won't start.** Install
[Deno](https://deno.com) so the `deno` binary is on `PATH`, or set
`NANOBPMN_DENO_BIN` to its full path, then reload the Workers tab. You can author
and export workers without Deno; only running them needs it.

**Everything was lost after a restart.** The server runs in-memory unless you set
`NANOBPMN_DATA_DIR` (or use `c8ctl nano`, which configures persistence for you).
Point it at a stable directory to keep data across restarts.

**A query doesn't show a write I just made.** Reads are eventually consistent
(typically sub-millisecond lag). Re-query, or use `awaitCompletion` /
`awaitInstance` when you need to act on a terminal result.

**Work is stuck and an instance "has an incident".** A job ran out of retries or a
gateway/error failed. Inspect it in the **Explorer** tab or via
`POST /v2/incidents/search`, fix the cause (give retries with `PATCH /v2/jobs/{key}`
or correct variables), then resolve it with
`POST /v2/incidents/{incidentKey}/resolution` to retry the failed work.

**Can't reach the console at localhost:8080.** Check the URLs printed at startup —
you may have set a different `PORT`, or another process may hold the port. In a
cluster, use `c8ctl nano status` to see each node's health.
