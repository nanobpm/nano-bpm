# Nano BPM

**A Rust research engine exploring high-performance BPMN execution and Camunda 8
compatibility.**

Nano BPM (`nanobpmn`) is a single self-contained binary that runs BPMN processes
behind a **Camunda 8-compatible v2 REST API**. It embeds a deterministic,
event-sourced BPMN engine (`engine-core`), an append-only journal for crash
durability, an SQLite-backed read model, optional multi-node Raft replication,
and a built-in web console — all in one executable with no runtime dependencies.

It is an advanced research prototype: a place to explore what a faster, smaller,
faster-to-iterate-on process engine can do, while staying API-compatible with
existing Camunda 8 clients and tooling.

> Looking to **build from source** or understand the code-generation pipeline?
> See [DEVELOPMENT.md](DEVELOPMENT.md). This README is for running and operating
> the distributed binary.

> Nano spans three repositories (this engine, the console **extension packs**, and
> the **c8ctl plugin** that runs it). For the map of what lives where and how they
> relate, see [`docs/nano-repositories.md`](docs/nano-repositories.md).

## Self-optimizing by design: one decision, not a hundred knobs

Nano is built on a single design principle:

> **If we can tell you *how* to do it, and *when* to do it — why don't we just do
> it?**

Anything the engine can decide correctly on its own, it decides on its own. Tuning
that a human would only ever set by watching a metric and applying a rule is instead
done by the engine, continuously, from that same metric. What's left for you is the
one thing the engine genuinely *cannot* know: a business value judgement about how
the service should behave at its limit.

### What the engine tunes for you (no configuration)

These subsystems are automatic and on by default. Each has an environment variable
**only to override or disable** the self-tuning behaviour — you never need to set one
to get the right behaviour:

- **Backpressure & the throughput ceiling.** An adaptive AIMD limiter measures live
  latency, sizes the in-flight watermark itself, and sheds excess creates
  (`503 RESOURCE_EXHAUSTED`) so a producer converges to the real drain rate. It finds
  the ceiling; you don't measure it.
- **Memory.** Tiered hot-state **variable spill** and **cold spill** move the live
  working set to disk under pressure; an **idle-purge** tick compacts hot maps and
  returns freed arenas to the OS; completed-instance history is bounded automatically.
- **Durable var-store footprint.** The var-store WAL is periodically truncated so it
  can't ratchet up on disk under a sustained large-payload load.
- **Cluster job drain** *(multi-node)*. Backlog-weighted activation fairness steers
  each worker's lease budget toward the nodes that actually have jobs — the deepest
  node drains fastest — instead of strict local-first. On by default (`stage2`).
- **Cluster create placement** *(multi-node)*. Load-aware placement (smooth weighted
  round-robin over gossiped per-node load) steers new instances toward nodes with
  real spare capacity, and every node self-protects: a saturated owner sheds a
  forwarded create back to the ingress node, which reroutes it to a node with
  headroom. The client only sees backpressure under **genuine cluster-wide**
  saturation. On by default (`balanced`). See
  [ADR 0014](docs/adr/0014-create-placement-protection-and-load-awareness.md).

All of the above are **no-ops on a single node** and where load is evenly balanced,
so the defaults are byte-identical to the historical behaviour exactly where they
can't help — and strictly better where they can.

### The one decision you make: behaviour at the edge of the envelope

There is exactly **one** knob that encodes a business decision, because it is the one
thing the engine cannot infer — it depends on what *your* service promises. When the
system reaches its capacity ceiling, which SLA do you want?

> *Do we admit all-comers to the restaurant and let them know the meal-serving time
> is getting longer — or ask newcomers to come back later so we can guarantee the
> patrons already seated get their meals as fast as possible?*

That is `NANOBPMN_SLA_MODE`:

- **`latency`** (default) — *"seat fewer, serve fast."* Preserve end-to-end speed by
  shedding admission at the ceiling. A **time-to-complete** SLA.
- **`admission`** — *"seat everyone; the backlog and the wait grow with demand."*
  Drop the proactive active-backlog governor and run the engine at its true drain
  ceiling (~48% more throughput in test). The AIMD concurrency limiter stays armed as
  an engine-overload guard — it measurably tightens the tail at no throughput cost —
  but it does **not** bound the accumulated backlog: under sustained overload the
  backlog grows until the **memory-safety rails** shed. You get higher throughput than
  `latency` mode and a much looser tail. A **start-every-process** SLA.

In both modes the memory-safety rails still guard against OOM, and the AIMD limiter
stays armed as an engine-overload guard. What differs is the **backlog bound**:
`latency` mode's active-backlog governor holds the backlog tight (so the tail stays
short), whereas `admission` mode drops it and lets the backlog grow with demand up to
the memory-safety rails — trading a much longer tail for higher throughput.
`admission` accepts looser latency and a larger backlog, never a crash. This is the
only behavioural policy you choose, and it is
[**switchable at runtime, cluster-wide**](docs/adr/0013-sla-modes-and-varstore-wal-bounding.md)
(flip it on one node and it propagates to the rest).

### Everything else is a deployment fact, not a tuning knob

The remaining environment variables don't tune *behaviour under load* — they declare
the **shape and durability** of your deployment, which are inherent choices for any
distributed, durable system:

- **Topology** — how many `NANOBPMN_NODES`, `NANOBPMN_PARTITIONS`, and the
  replication factor `NANOBPMN_RF`. This is *how big and how fault-tolerant*, not
  *how to behave*.
- **Durability tier** — `NANOBPMN_DURABILITY` / `NANOBPMN_REPLICATION`: how much of a
  just-acked tail you're willing to lose on an ungraceful leader loss (zero, by
  default). Every durable distributed engine makes you state this; it's a data-safety
  guarantee, not an envelope-edge tuning. See
  [ADR 0003](docs/adr/0003-write-path-durability-tiers.md).

So: the engine self-optimizes with **zero technical configuration**, the deployment
knobs describe *what you're running*, and the single business decision —
`NANOBPMN_SLA_MODE` — describes *what you promise*. Full reference in
[Cluster configuration](#cluster-configuration) and [Cluster tuning](#cluster-tuning).


## Quick start with c8ctl

The easiest way to run and manage Nano BPM — single node or a whole cluster — is
the [**c8ctl**](https://github.com/camunda/c8ctl) CLI with the
[`c8ctl-plugin-nano`](https://github.com/jwulf/c8ctl-plugin-nano) plugin. The
plugin ships a prebuilt Nano BPM binary for your platform (installed
automatically as an npm `optionalDependency`), so there is nothing to compile.

```bash
# Load the plugin (installs the matching prebuilt binary for your OS/arch)
c8ctl load plugin c8ctl-plugin-nano

# Start a single-node cluster on port 8080
c8ctl nano start

# Start a 3-node cluster (ports 8080, 8081, 8082)
c8ctl nano start 3

# Start a 3-node Raft-replicated cluster (RF=3 enables Raft automatically)
c8ctl nano start 3 --rf 3

# Show cluster status and per-node health (queries each node's /v2/topology)
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
shared workspace that survives `stop`/`clean`, separate from the ephemeral
per-node engine data. It also wires every node's environment for you (ports,
node ids, partitions, replication, data dirs). See the
[plugin README](https://github.com/jwulf/c8ctl-plugin-nano) for the full command
and flag reference, including [trace capture](#trace-capture) (`--capture`).

Once a cluster is up, point any Camunda 8 v2 REST client (or the
[`@nanobpmn/sdk`](#nodetypescript-sdk) Falcon client) at
`http://127.0.0.1:8080/v2`, and open the console at
`http://127.0.0.1:8080/console`.

## Running the binary

You can also run the binary directly (the same one the plugin installs),
configuring it entirely through environment variables:

```bash
NANOBPMN_DATA_DIR=/var/lib/nanobpmn PORT=8080 \
  ./nanobpm-gateway-rest-server
```

When started, the gateway prints the human-facing URLs:

```text
Nano BPM is up:
  Landing page   http://127.0.0.1:8080/
  Web console    http://127.0.0.1:8080/console
  API reference  http://127.0.0.1:8080/swagger
  REST API       http://127.0.0.1:8080/v2
  Metrics        http://127.0.0.1:8080/metrics
```

Common configuration:

| Variable | Effect |
| --- | --- |
| `PORT=<n>` | HTTP listen port (default `8080`). |
| `NANOBPMN_DATA_DIR=<dir>` | Durable event-log + read-model directory. Without it the server runs fully in-memory (ephemeral). |
| `DEBUG_REST=1` | Log every REST request/response (method, URI, status, latency, body preview). Leave off in production. |
| `NANO_CONSOLE_TERMINAL` | Controls the IDE's integrated terminal (a full shell in the project directory). **Enable it in the console** (Config → Server → Integrated terminal); the setting persists in `<NANOBPMN_DATA_DIR>/console-settings.json`. This env var is a **hard lock**: set it to `0`/`false`/`off` to force the terminal off and prevent the console from enabling it (for shared / network-exposed servers). Set to `1`/`true`/`on` to default it on. Only ever reachable from **loopback** (local peer IP + loopback `Host`), since a shell is arbitrary code execution. Requires the `console` feature. |

```text
INFO rest: --> POST /v2/process-instances [53 bytes] {"processDefinitionId":"demo","tenantId":"<default>"}
INFO rest: <-- POST /v2/process-instances 200 OK (39.7ms) [180 bytes] {"processInstanceKey":"3", …}
```

The full set of tuning variables (durability, memory, partitions, clustering) is
documented in the sections below.

## REST API

Nano BPM serves the Camunda 8 Orchestration Cluster **v2 REST API**. The whole
API surface is routable; operations not yet wired into the engine respond with
`501 Not Implemented`. For the honest, element-by-element boundary of what
executes and what is served — and where the edges are — see the
[**Camunda compatibility subset**](docs/camunda-compatibility.md). The following
are backed by the embedded `engine-core` BPMN engine:

- `POST /v2/deployments` (`createDeployment`) parses the uploaded BPMN 2.0 XML
  resources and deploys them, assigning each process a key and a per-id version.
  Deployment is **idempotent**: redeploying a process that is byte-for-byte
  identical to the current latest version of the same id reuses that version
  (no new key, no version bump, nothing journaled), mirroring Zeebe; a changed
  model deploys as the next version.
- `POST /v2/process-instances` (`createProcessInstance`, by `processDefinitionId`
  or `processDefinitionKey`) starts a real instance and returns its
  engine-assigned key. Variables supplied on the request seed the root scope.
  With `awaitCompletion: true` the request blocks (off the engine write lock)
  until the instance reaches a terminal state or `requestTimeout` ms elapse
  (default 5s). The response carries a `processCompleted` flag: when `true`, the
  returned `variables` (optionally narrowed by `fetchVariables`) are the
  authoritative final result; when `false`, the instance is still running.
  **Deviation from Camunda:** on timeout nanobpmn returns `200` with
  `processCompleted: false` and the `processInstanceKey` (so the caller can poll)
  rather than Camunda's `504`.
- `POST /v2/jobs/activation` (`activateJobs`) activates available jobs of a type,
  locking each to the worker until `now + timeout`; supports Camunda-style
  long-polling via `requestTimeout` (a waiting request wakes as soon as a job
  becomes available).
- `POST /v2/jobs/{jobKey}/completion` (`completeJob`) completes a job and resumes
  the token, merging any returned variables into the instance (so worker output
  drives downstream gateway routing). A job must have been activated first;
  completing an un-activated job returns `409`. Completion is by key alone (no
  worker check), so a slow worker whose lock expired can still complete the job —
  first completion wins.
- `POST /v2/jobs/{jobKey}/failure` (`failJob`) sets a job's remaining retries.
  With retries left the job returns to the activatable pool; with none an
  incident is raised and the job parks (no longer activatable or completable).
- `POST /v2/jobs/{jobKey}/error` (`throwError`) raises a business error from a
  job. If the job's service task has an error boundary event with a matching
  `errorCode`, the task is interrupted and the boundary's outgoing path runs
  (e.g. a refund/compensation flow); otherwise the error propagates up any
  enclosing **embedded sub-process** to a sub-process error boundary (which
  terminates the whole sub-process scope), and if still uncaught an incident is
  raised. The job is consumed either way. Returns `404` for an unknown or
  un-activated job and `409` if the job is no longer active.
- `PATCH /v2/jobs/{jobKey}` (`updateJob`) applies the changeset's `retries` to a
  job — used to recover a job parked on a no-retries incident (timeout updates
  are not modelled).
- `POST /v2/incidents/{incidentKey}/resolution` (`resolveIncident`) resolves an
  incident by **retrying the failed work**, not just clearing the record. A
  job-incident returns the parked job (which must have retries again) to the
  activatable pool — recovery loop: `failJob`(0) → `updateJob`(retries) →
  `resolveIncident` → re-activate → `completeJob`. A gateway incident
  re-evaluates the gateway; an uncaught-error incident re-creates the
  service-task job. If the retry fails again, a fresh incident is raised.
  Incidents now carry a real `creationTime` (the server feeds the engine its
  clock at command time). Resolving accepts an optional `operationReference`
  (recorded on the retained record for audit) and returns `409` if the incident
  is already resolved.
- `PUT /v2/element-instances/{elementInstanceKey}/variables`
  (`createElementInstanceVariables`) merges variables into a scope (the key may
  be a process instance or an active element instance). Use it to correct the
  data behind a gateway/condition incident, then `resolveIncident` re-evaluates
  and the token proceeds. `local` is accepted but has no effect (single scope).
- `POST /v2/messages/publication` (`publishMessage`) and
  `POST /v2/messages/correlation` (`correlateMessage`) deliver a message to any
  open subscription whose name and correlation key match, releasing a **message
  intermediate catch event**'s token, interrupting an activity via an
  **interrupting message boundary event**, or spawning a parallel token via a
  **non-interrupting message boundary event** while the activity keeps running
  (such boundary events attach to a service task **or an embedded sub-process** —
  an interrupting one on a sub-process tears down its whole inner scope)
  (merging the message's variables into
  the instance first). Messages are **not buffered** (no TTL/dedup): with no
  match the message is dropped. `publishMessage` always returns `200` with the
  minted `messageKey`; `correlateMessage` returns `404` when nothing correlates
  (and otherwise `200` with the first correlated `processInstanceKey`). A
  **message start event** also correlates here: a matching `correlateMessage`
  **creates a new instance** (seeded with the message's variables) and returns its
  `processInstanceKey`.
- **Event-triggered instance creation.** Deploying a process whose start event is
  a **message start event** opens a process-level subscription (a matching
  `correlateMessage` creates an instance), and a **timer start event** arms a
  process-level timer fired by the background tick — a one-shot `timeDuration` runs
  once, a recurring `timeCycle` (`R/PT…`) creates an instance every interval. Both
  are journaled, so the subscription/schedule survives a restart.

Read endpoints make engine state observable:

- `GET /v2/process-instances/{processInstanceKey}` (`getProcessInstance`) —
  reports state (`ACTIVE`/`COMPLETED`) and `hasIncident`.
- `GET /v2/incidents/{incidentKey}` (`getIncident`) and
  `POST /v2/incidents/search` (`searchIncidents`) — expose incidents (active
  **and** resolved, since resolved records are retained as an audit trail; each
  reports its `state`, `ACTIVE` or `RESOLVED`), including the `incidentKey`
  needed to resolve them. `errorType` reflects the cause (`JOB_NO_RETRIES`,
  `CONDITION_ERROR`, `UNHANDLED_ERROR_EVENT`).
- `POST /v2/process-instances/search` (`searchProcessInstances`) and
  `POST /v2/jobs/search` (`searchJobs`) round out the read surface.
- `POST /v2/variables/search` (`searchVariables`) and
  `GET /v2/variables/{variableKey}` (`getVariable`) — expose instance variables.
  nano keeps a single instance-level scope, so every variable's `scopeKey`
  equals its `processInstanceKey`. Values are reported as serialized JSON
  (a string `text` as `"text"`, numbers/booleans bare); `searchVariables`
  truncates long values unless `truncateValues=false` and flags `isTruncated`.
  Variable keys are assigned by the read model (the engine does not mint them).

All search endpoints implement the full v2 query contract:

- **Filters** use the advanced operator algebra — `$eq`, `$neq`, `$exists`,
  `$in`, `$notIn`, and `$like` (with `*`/`?` wildcards) — in addition to plain
  scalar equality. Examples: `state: {$in: ["FAILED", "ERROR_THROWN"]}`,
  `type: {$like: "pay*"}`, `processInstanceKey: {$exists: true}`.
- **Sort** accepts multiple `{field, order}` clauses applied in order, with the
  entity key as a deterministic final tiebreak.
- **Pagination** supports all four request shapes: `{limit}`, offset
  `{from, limit}`, forward cursor `{after, limit}`, and backward cursor
  `{before, limit}`. Responses carry `totalItems` plus `startCursor`/`endCursor`
  (opaque, padding-free base64 of the entity key) for stable cursor walks.

Timestamps are reported as the Unix epoch since the engine is clock-free.

A demo process (`processDefinitionId: "demo"`, a single service task) is
pre-deployed at server startup, but you can also deploy your own `.bpmn` files
through the deployment endpoint. Engine and parse errors map to real status
codes (`400`/`404`/`409`); everything else is still `501`.

## Durability and recovery

### Event-log replay

The engine is in-memory but event-sourced: every command returns the complete,
ordered list of events it produced, and replaying those events over a fresh
state reconstructs it exactly. The server turns that into crash durability with
an **append-only journal**. Set `NANOBPMN_JOURNAL` to a file path and every
durable command's events are appended (newline-delimited JSON) and flushed
before the response returns; on startup the log is replayed through
`Engine::replay`, which also advances the key generator past every key the log
assigned so post-recovery commands never collide with replayed ones. Without the
env var the server runs purely in memory (ephemeral).

```bash
NANOBPMN_JOURNAL=./nanobpmn.journal PORT=8099 ./nanobpm-gateway-rest-server
```

Job **activation locks are intentionally not journaled** — they are volatile
lease state. A restart forfeits every lock, returning uncompleted jobs to the
activatable pool, so a worker simply re-activates after recovery. Everything
durable (deployments, instances, element progress, jobs, incidents, variables,
completion, **armed timers**) survives.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_DATA_DIR=<dir>` | Co-locates both under one directory: `<dir>/journal.jsonl` + `<dir>/read-model.sqlite` (created if absent). |
| `NANOBPMN_JOURNAL=<file>` | Back-compat: selects the journal file; the database is `NANOBPMN_READ_DB` if set, else a sibling `read-model.sqlite`. |
| *(neither set)* | Fully in-memory: an ephemeral journal and a `:memory:` read store. Nothing is persisted. |

### Background tick (timers and lock expiry)

The engine reads no wall clock; the host drives time in. The server runs a
single background task (every 500 ms) that feeds `now` into the engine via two
ticks: `TriggerTimers` fires every due **timer intermediate catch event**,
**timer boundary event** (interrupting or non-interrupting, on a service task or
embedded sub-process; a `timeCycle` non-interrupting boundary **re-arms** for the
next interval on each fire) and **timer start
event** (durable — journaled), and `ExpireJobs`
releases activation locks past their deadline (volatile — not journaled). When a timer fires it may unblock
downstream work, so the tick wakes any long-polling `activateJobs`. A timer
parked before a restart is recovered by replay and fired by the first due tick
afterwards.

### Write path: concurrency, group-commit, and fsync

The server runs on a multi-threaded Tokio runtime (one worker per core), so
connection handling, HTTP parsing and JSON (de)serialization already spread
across cores. The engine itself is a **single writer**, so it sits behind a
`RwLock`: mutating operations (create instance, complete/fail job, deploy, job
activation, the background tick) take the **write** lock and are serialized,
while the read-only `search*`/`get*`/`topology` projections take the **read**
lock and therefore run **concurrently across cores**. The lock is never held
across an `.await`, and critical sections are short. A write enqueues its events
to the journal writer thread *under* the write lock (preserving command order),
then releases the lock and `.await`s durability outside it, so disk I/O no longer
serializes behind the engine lock; read-heavy load scales with the number of
cores.

The journal is owned by a dedicated `nanobpmn-journal-writer` thread. Each
mutating command serializes its events and hands them to the writer over an
ordered channel, receiving a `Commit` handle. The writer batches every queued
request into a single `write_all` followed by one `sync_all` (**fsync**), then
acks each batched command — amortizing the fsync cost across concurrent writes
(**group commit**). A handler only returns its success status *after* its
`Commit` resolves, i.e. after the events are durably on disk, so a `200`/`204`
means "persisted and will survive a crash". If a journal write ever fails the
writer aborts the process rather than serve state that outran the durable log;
on restart the log replays to re-derive consistent state.

## Read model and consistency

The engine's hot state holds only what execution needs. If every completed
process instance, job and incident stayed there forever, resident memory would
grow without bound under sustained load (and never come back down). nanobpmn
follows the Camunda 8 / Operate split — a **command side** and a separate
**read side** — collapsed into a single binary:

- **Command side** — the engine + journal. It runs in-memory and is the single
  writer. Once an instance's history is durably recorded in the read model, the
  engine **evicts** the completed instance and everything it owns (jobs, timers,
  subscriptions, incidents) from hot state, so the footprint tracks *in-flight*
  work rather than all history.
- **Read side** — an embedded **SQLite read store**. A background
  `nanobpmn-exporter` thread streams the journal's event log into it (in command
  order), projecting events into denormalized `process_definitions` /
  `process_instances` / `jobs` / `incidents` / `variables` tables. **Every
  `search*`/`get*` query is served from SQLite**, never from hot engine state —
  which is what makes eviction possible.

The read model is a pure **derived projection** of the journal, so it needs no
durability of its own: on boot the server replays any journal events the store
has not yet projected (tracked by an `exported_position` cursor; a fresh or
schema-mismatched store rebuilds from scratch), then evicts completed instances
from the recovered hot state.

Because the exporter is asynchronous, reads are **eventually consistent**: a
`search`/`get` issued in the instant after a write may briefly not observe it
(typically sub-millisecond). This mirrors Camunda 8's Operate read channel.

> Activation locks are volatile and **not journaled**, so a `searchJobs` result
> reflects durable state: an activated job shows as `CREATED` without a worker or
> deadline. This is deliberate — the read model reports what survives a crash.

## Memory management

Load arrives in bursts: a flood of `createProcessInstance`s grows the hot-state
maps and 50 KB-class variable payloads, which are then freed as instances
complete and are evicted. Nano BPM keeps that freed memory from being pinned, and
bounds the live peak of large backlogs.

### Allocator and idle reclamation

Rust maps never shrink on removal, and the default system allocator hoards freed
pages, so an idle server would otherwise pin its peak resident footprint long
after the work is gone. Nano BPM addresses both:

- It uses **jemalloc** as the global allocator (vendored, built from source — the
  binary stays self-contained). jemalloc returns unused pages to the OS on a
  **decay** schedule, driven by a background thread on Linux. (On Windows/MSVC the
  system allocator is used instead.)
- An **idle-purge tick** closes the gap on platforms with no jemalloc background
  thread (macOS) and makes reclamation prompt everywhere: when the engine goes
  quiescent after a burst, it `shrink_to_fit`s the hot-state maps and forces
  jemalloc to purge every arena, returning the freed memory to the OS
  **immediately**. It fires once per active→idle transition, never while work is
  flowing, so it adds no steady-state cost. (In a local run, a 4 000-instance
  create+complete burst's idle tick logged `returned 156.9 MiB to the OS
  (214.4 -> 57.5 MiB resident)`.)

| Variable | Effect |
| --- | --- |
| `NANOBPMN_IDLE_PURGE_MS=<n>` | Quiescence (ms) the server must be idle before it compacts hot state and returns freed memory to the OS. Default `5000`; `0` disables the idle-purge tick. |
| `NANOBPMN_HISTORY_MAX_INSTANCES=<n>` | Caps how many *completed/terminated* instances the read model retains (with their variables, jobs and incidents); the oldest beyond the cap are evicted continuously on the exporter thread. Active instances are never evicted. Default `0` = unbounded history. Set it to bound read-model memory to the working set. |

### Tiered hot state: variable spill and cold spill

The engine's hot state lives in RAM for speed, but two workloads make an unbounded
resident footprint a problem: a large *active* backlog (many instances parked on a
job, each carrying a 50 KB-class variable payload) and a large *dormant* backlog
(many long-lived instances parked on a timer or message, idle for minutes to days).
nanobpmn pages both classes out to disk and rehydrates them on demand, mirroring how
Camunda 8 / Zeebe back hot state with RocksDB. Both tiers reuse one SQLite store
(`<data-dir>/var-spill.sqlite`) and are **derived caches** of the durable journal — a
lost spill blob is reconstructable, so the store runs `synchronous=NORMAL`.

- **Variable spill** sheds just the *variables* of instances parked on a job once the
  resident parked-backlog exceeds a hot budget, and rehydrates them on `activateJobs`
  (the moment a worker needs them). It targets the high-throughput flood case and
  leaves the small control-state maps resident. It deliberately skips instances
  holding a timer or open message subscription, since those resume without an
  activation seam.
- **Cold spill** evicts whole *dormant* instances — control state, jobs, timers,
  subscriptions, variables — when resident RAM crosses a high-water mark, keeping only
  a slim resident **routing index** (job keys, message correlation keys, timer
  due-times) so an off-heap instance can still be found. It rehydrates an instance the
  instant an event targets it: a worker poll for its job type, a command addressing it
  by key, a correlating message, or its timer falling due.

Both tiers are **on by default in persistent mode** (a data directory is configured)
and off for fully in-memory runs.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_VAR_SPILL=<on\|off>` | Force variable spill on/off. Unset: on iff a persistent data path exists. |
| `NANOBPMN_VAR_SPILL_BUDGET=<n>` | Resident parked-instance budget before variables spill. Default `512`. |
| `NANOBPMN_COLD_SPILL=<on\|off>` | Force cold spill on/off. Unset: on iff a persistent data path exists. |
| `NANOBPMN_COLD_SPILL_MB=<n>` | High-water resident RAM (MiB) above which dormant instances are evicted to disk. Default `384`; low-water = 7/8 of high. |

## Partitions

Following Zeebe, a node may run several **partitions**, each its own
single-writer engine thread + journal. Partitioning multiplies the single-writer
throughput ceiling (each partition fsyncs and applies independently) while
keeping every command serialized within its partition.

- **Default is one partition** (`NANOBPMN_PARTITIONS=1`), which preserves the
  historical behaviour exactly: one engine thread, one `journal.jsonl`, keys
  `1, 2, 3, …`.
- Set `NANOBPMN_PARTITIONS=<n>` to run `n` partitions (0-based ids `0…n-1`).
  Keys embed their owning partition in their high bits, so a command targeting an
  existing key routes to exactly one partition, while a fresh
  `createProcessInstance` is balanced **round-robin** across partitions. An
  instance lives on its creating partition for life.
- **Queries, `awaitCompletion`, and the read model are global** — answered from a
  single shared projection fed by all partitions, so multi-partition is
  transparent to clients.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_PARTITIONS=<n>` | Number of single-writer partitions. Default `1`. Clamped to `[1, 8192]`. Changing this requires fresh data directories (the journal layout differs). |

## Clustering, replication, and availability

A deployment is one or more **nodes** (processes). Every node is both a **broker**
(owning a subset of partitions) and a **gateway** (accepts client connections for
the *whole* cluster, forwarding operations it does not own to the node that does).
Partition→node placement is deterministic (`partition_id % num_nodes`), so every
node computes the same ownership map from static config with no coordinator.
Single-node (the default, `NANOBPMN_NODES` unset) is byte-for-byte the historical
behaviour.

> The [c8ctl plugin](https://github.com/jwulf/c8ctl-plugin-nano) wires all of the
> variables below for you — `c8ctl nano start 3 --rf 3` brings up a 3-node,
> Raft-replicated cluster on localhost with no manual env-var configuration.

Two independent axes:

- **Distribution** (`NANOBPMN_NODES` / `NANOBPMN_NODE_ID`): spread partitions across
  nodes for throughput. Each partition still lives on exactly one node.
- **Replication** (`NANOBPMN_RAFT=on` + `NANOBPMN_RF=<k>`): replicate each partition
  across `k` nodes as a per-partition **Raft** group, for durability and failover.

### Replication factor and quorum

With `NANOBPMN_RF=k`, each partition's replica set is `k` consecutive nodes
(`[owner, owner+1, …]`), the first being its initial leader, and writes commit
through Raft. `RF` is clamped to `[1, num_nodes]`. **A `k`-voter Raft group
commits only with a quorum of ⌊k/2⌋+1 replicas and therefore tolerates ⌊(k−1)/2⌋
failures:**

| Nodes (RF = nodes) | Quorum | Faults tolerated | On one node loss |
| --- | --- | --- | --- |
| 1 | 1 | 0 | total outage |
| **2** | **2** | **0** | **total write outage** (see warning) |
| 3 | 2 | 1 | stays up — sub-second leadership blip |
| 5 | 3 | 2 | stays up |

Raft deployments use **odd** node counts. The first size that survives a failure
is **3** (quorum 2, tolerates 1).

> ⚠️ **Two-node clusters are a trap.** A 2-node `RF=2` group has quorum 2 — *both*
> nodes are required to commit — so it tolerates **zero** faults, the *same* as a
> single node, while *doubling* the number of machines whose failure halts all
> writes. It buys **durability only**, **not availability**. Use 3+ for fault
> tolerance.

### What happens when a node is lost

nanobpmn is **CP** (consistency over availability): a partition without a quorum
refuses to commit rather than diverge — so there is **no split-brain and no data
loss**, ever. Behaviour depends on which side of the cut you are on:

- **Majority side (e.g. lose 1 of 3, `RF=3`).** The survivors hold quorum (2/3),
  **re-elect a new leader for the lost node's partitions within the election
  timeout (~sub-second)**, and keep serving. In-flight writes forwarded to the
  now-dead leader fail fast and retry on the new leader. Measured: throughput
  through a node loss holds at ~98% of baseline with a sub-second p99 blip, then
  full recovery on rejoin — no data loss.
- **Minority side (a single node taken off the network).** That node can reach no
  quorum for *any* partition, so it becomes **write-unavailable**: every
  create/complete/activate fails fast. Reads are still served from its **local
  applied state — consistent but frozen/stale**. **No writes it attempted while
  isolated are ever acknowledged or retained.**
- **RF=1 (no replication, the default).** Each partition is single-homed. Losing a
  node keeps the **survivor fully serving its own partitions** (partition-level
  fault isolation), but the **dead node's partitions go offline** until it returns.

A returning node reconnects, catches up via Raft `AppendEntries` (or an
`InstallSnapshot` if it fell far behind), **discards any uncommitted entries it
proposed while isolated**, and resumes as a follower. Membership is not changed on
a transient outage, so no operator action is needed.

### Cluster configuration

| Variable | Effect |
| --- | --- |
| `NANOBPMN_NODES=<url,url,…>` | Comma-separated node base URLs, **index = node id** (e.g. `http://10.0.0.1:8080,http://10.0.0.2:8080`). Unset (or one entry) ⇒ single node. |
| `NANOBPMN_NODE_ID=<i>` | This node's id (index into `NANOBPMN_NODES`). Default `0`. |
| `NANOBPMN_RAFT=on` | Enable per-partition Raft replication. Off ⇒ the single-homed, byte-identical path. |
| `NANOBPMN_RF=<k>` | Replication factor: nodes per partition. Default `1`. Clamped to `[1, num_nodes]`. Use an odd node count with `RF=num_nodes` for fault tolerance. |
| `NANOBPMN_REPLICATE_ACTIVATION=<mode>` | How the job-activation lock is handled under Raft (RF>1). Default under `quorum` ⇒ **`auto`**: a zero-config alias for **leader-local + soft digest** (identical to `digest`) — validated healthy across the whole payload/throughput range (2,400/s @ 50 KB, ~36k/s @ negligible payload; see PERFORMANCE.md). `1`/`quorum` ⇒ always **replicated** (strict lease; a per-activation quorum commit that collapses throughput at scale — unsafe at any non-trivial load, kept only for parity/testing). `0`/`off` ⇒ **leader-local** (lease in the leader's RAM only; immediate redelivery on failover). `digest` ⇒ leader-local **plus** a best-effort lease broadcast that narrows the failover redelivery window. All modes are at-least-once. See [`docs/adr/0002-leader-local-activation-and-lease-digest.md`](docs/adr/0002-leader-local-activation-and-lease-digest.md). |
| `NANOBPMN_REPLICATION=<tier>` | Replication durability tier under Raft (RF>1). Default ⇒ **quorum** (acks after a majority commits + applies; zero data loss on node loss). `leader-durable` (`acks=1`) ⇒ the leader acks after its own local durable append+apply and ships to learners asynchronously (lowest latency; a just-acked tail can be lost on ungraceful leader loss, bounded — never divergent). See [`docs/adr/0003-write-path-durability-tiers.md`](docs/adr/0003-write-path-durability-tiers.md). |
| `NANOBPMN_RAFT_HEARTBEAT_MS` / `_ELECTION_MIN_MS` / `_ELECTION_MAX_MS` | Leader heartbeat (default `250`) and randomized election window (defaults `500`/`1000`). |
| `NANOBPMN_RAFT_SNAPSHOT_LOGS=<n>` | Snapshot every `n` applied entries (log compaction). Default `5000`. |
| `NANOBPMN_ACTIVATION_FAIRNESS=off\|1\|2` | Fairness-aware job-activation routing across nodes. **Default `2`** (self-optimizing: caps each source by its live backlog, steering a worker's lease budget toward where the jobs are). `1` ⇒ rotation+quota only; `off` ⇒ strict local-first (historical). No-op on a single node. See [`docs/adr/0001-cluster-job-activation-fairness.md`](docs/adr/0001-cluster-job-activation-fairness.md). |
| `NANOBPMN_CREATE_PLACEMENT=off\|protect\|balanced` | Cluster create-placement protection & load-awareness (RF>1). **Default `balanced`** (self-optimizing): load-aware weighted placement (smooth weighted round-robin over peer-gossiped load) steers creates toward nodes with real spare capacity, **plus** `protect` — a saturated owner sheds a forwarded create back to the ingress node, which reroutes to an owner with headroom (only 503s the client under **global** saturation). `protect` ⇒ self-protection without load-weighting; `off` ⇒ blind round-robin (historical). Never risks a duplicate instance (reroute fires only on a proven-not-applied shed). **No-op on a single node** (byte-identical). See [`docs/adr/0014-create-placement-protection-and-load-awareness.md`](docs/adr/0014-create-placement-protection-and-load-awareness.md). |
| `NANOBPMN_CREATE_PLACEMENT_GOSSIP_MS=<ms>` | Peer create-load gossip interval for `NANOBPMN_CREATE_PLACEMENT=balanced`. Default `500`. No effect in `off`/`protect` or single-node. |

See [`docs/distributed-scaling-design.md`](docs/distributed-scaling-design.md) for
the full design rationale.

## Falcon protocol (WebSocket)

Alongside the REST API, the server exposes a single **bidirectional WebSocket**
at `GET /falcon` that multiplexes the whole client lifecycle — process
creation *and* the full job lifecycle — onto one persistent, credit-coordinated
socket. It funnels to the same engine command path as the REST handlers, so it is
purely a more efficient ingress: no per-request connection setup, no long-poll for
jobs, and flow control by **credits** instead of `429`/`503` + client retry.

Connect with an optional `worker` query parameter
(`/falcon?worker=my-worker`). Frames are **JSON text frames**, each a
tagged union with a camelCase `"type"`. On connect the server sends a `welcome`
(initial submission window + heartbeat cadence) followed by a `submissionCredits`
grant. Idle sockets exchange `heartbeat` frames every 15 s.

The stream carries **two credit lanes** over the one engine thread:

- **Job push (demand/pull).** A client `subscribe`s to a job type with a credit
  count; a single server-side dispatcher leases jobs **round-robin across all
  subscribers** and pushes `job` frames while credits remain, topping demand back
  up via `jobCredits`. **Lease expiry is the at-least-once guarantee:** a job
  pushed to a worker that never completes it is reclaimed by the periodic
  lock-expiry tick, so a dropped socket needs no special handling.
- **Submission (request/response).** `createInstance` is metered by a
  **submission-credit window** fed from the engine's processing headroom: under
  saturation the server simply **withholds credits** and the client stalls intake
  — no `503`, no retry storm. Job completions (`completeJob` / `failJob` /
  `throwError`) flow **unmetered** — draining backlog must never be throttled.

Client → server frames: `subscribe`, `jobCredits`, `createInstance`,
`completeJob`, `failJob`, `throwError`, `awaitInstance`, `heartbeat`.
Server → client frames: `welcome`, `job`, `commandResult`, `instanceCompleted`,
`submissionCredits`, `pressure`, `heartbeat`.

**Await-completion and recovery.** `createInstance` may set
`awaitCompletion: true`; rather than holding the request, the server returns the
`commandResult` (with the `processInstanceKey`) immediately and later emits an
async `instanceCompleted` frame, correlated by the create's `corr`. If the socket
drops before that arrives, the client recovers by sending an **`awaitInstance`**
frame on the new connection with the persisted `processInstanceKey`. Because the
read model is durable history, an already-terminal instance resolves
**immediately**, so `awaitInstance` doubles as a completion poll.

> **Per-connection ordering vs. throughput.** A single connection's frames are
> processed in arrival order, each engine command awaited inline — so successive
> `createInstance`s on *one* socket serialize at journal fsync latency. Throughput
> comes from **concurrency across connections**: the group-commit journal writer
> batches many connections' appends into a single fsync. Spread submission load
> over multiple sockets rather than pipelining one.

**Recommended worker topology.** Because ordering is per-socket and throughput
scales with concurrent sockets:

- **One stream per job-type worker** — each gets its own demand-credit lane and
  reader task, so delivery for different types proceeds in parallel.
- **Separate creation from work** — use a distinct socket (or a small pool) for
  `createInstance` submission so a burst of creates can't head-of-line-block job
  completions. For high create rates, fan out across a **pool** of submission
  sockets — that is what lets the group-commit writer batch their appends.
- **Rule of thumb for 10 job types:** ~10 worker sockets (one per type) **plus** a
  small submission pool (e.g. 2–8 sockets sized to your create rate).

| Variable | Effect |
| --- | --- |
| `NANOBPMN_STREAM_SUBMISSION_WINDOW=<n>` | Per-connection create-submission window (default `256`). |

**Stream durability: ack-before-fsync pipelining.** To maximize throughput, job
lifecycle commands (`completeJob` / `failJob` / `throwError`) use **pipelined
commits**: the server applies the command to the engine (establishing order),
then **replies `200` immediately** while fsync completes asynchronously (~5 ms
later). This batches many connections' completions into one group-commit fsync,
measured at **4× higher throughput** (2280 vs 572 writes/s) than awaiting fsync
inline. The trade-off: a crash in the ~5 ms window re-activates the job after
restart (its lock expires) — preserving **at-least-once** semantics (handlers must
be idempotent, the standard BPMN worker contract). The REST API
`/jobs/{key}/completion` endpoint still awaits fsync before replying; only the
Falcon pipelines.

See [`docs/falcon-design.md`](docs/falcon-design.md) for the full
design rationale,
[`docs/falcon.asyncapi.yaml`](docs/falcon.asyncapi.yaml) for the
AsyncAPI 3.1 description of every frame, and
`server/tests/falcon_e2e.rs` for runnable examples.

### Node/TypeScript SDK

[`clients/node-stream`](clients/node-stream) publishes **`@nanobpmn/sdk`**, a
companion to `@camunda8/orchestration-cluster-api` that adds a typed
Falcon client and a streaming job worker. The worker auto-detects the
backend: it uses the Falcon protocol against nanobpmn and **falls back to Camunda
REST polling** against a Camunda gateway, so the same handler code serves both.
See [`clients/node-stream/README.md`](clients/node-stream/README.md).

## Web console

The distributed binary includes a built-in **web console** (behind the `console`
Cargo feature, which the c8ctl-shipped binaries enable). Start a cluster and open
`http://127.0.0.1:8080/console`. It serves a single-page app at `/console` and a
JSON API under `/console/api/*`. The root `/` serves a small self-contained
landing page, and `/swagger` serves an **offline** Swagger UI with the OpenAPI
spec bundled in (nothing fetched from a CDN).

The console ships in two **profiles** and the navigation adapts to each:

- **Studio** (the default authoring build) shows **Projects**, **Extensions**,
  **Topology**, **Metrics**, **Explorer**, **Traces**, and **Workers**.
- **Observe** (the operator build) drops the two authoring tabs and shows only
  **Topology**, **Metrics**, **Explorer**, **Traces**, and **Workers**.

Two more views — **Config** (appearance, theme packs, and server settings) and
**Credits** — are reachable by route but do not live on the navigation rail. Each
tab is described below.


### Topology

Cluster/partition/Raft overview with **live per-node health**: each peer's
`GET /v2/topology` is probed (concurrently, on a 5 s cadence) to show
reachability, gateway version, and round-trip latency.

### Metrics

A live performance dashboard (process starts/s, jobs/s, active processes,
connected clients, commit pipeline depth, journal/fsync means, writer duty cycle,
resident memory) with inline sparklines, derived client-side from the gateway's
Prometheus surface (`/metrics`). In a multi-node cluster it also shows a
**per-node breakdown** by probing each peer's `GET /console/api/metrics`.

**Ceiling LEDs and worker-provisioning hints.** Like the LED on a rack
compressor/limiter that lights when the limiter engages (see
[ADR 0013](docs/adr/0013-sla-modes-and-varstore-wal-bounding.md)), `/metrics`
exposes when a node is pressed against a capacity ceiling:

- `nanobpm_ceiling_active{ceiling="throughput"|"memory"}` — `1` while at that
  ceiling, else `0`. `throughput` = create-processing concurrency / active-backlog
  limit; `memory` = the always-in-circuit memory-safety rails (create-queue depth,
  exporter saturation, in-flight pipeline bytes, resident-memory watermark).
- `nanobpm_ceiling_hits_total{ceiling}` — rising-edge count of how often each
  ceiling engaged (a peak-hold on the gain-reduction meter).
- `nanobpm_job_type_activatable{job_type}` / `nanobpm_job_type_workers{job_type}`
  / `nanobpm_job_type_starved{job_type}` — waiting jobs, subscribed workers, and a
  starvation hint (`1` when jobs wait with no worker to drain them) so
  under-provisioned job types are visible at a glance.

All are published ~1 Hz off the hot path (relaxed atomic reads + cheap
low-priority scans), so scraping them never touches the create/complete path.

### Nano Studio (RAD IDE)

**Nano Studio** (the **Studio** tab) is the home of Nano's **Rapid Application
Development** environment and replaces the old standalone "Modeler" tab. A
project is a self-contained, runnable Deno application directory (see [Workspace
vs cluster data](#workspace-vs-cluster-data)); the Studio view is a tile gallery
with a **New project** template picker, and opening a project drops you into its
workspace. First-party project templates and example apps are themselves shipped
as extension packs.

Inside a project you author four first-class file kinds, each with a dedicated
editor:

- **Models** (`.bpmn`, in `resources/processes/`) — a bpmn-js editor.
- **Decisions** (`.dmn`, in `resources/decisions/`) — a dmn-js editor. DMN is
  executed **natively** by the engine's Rust decision engine (no JVM), so a
  `businessRuleTask` with a `zeebe:calledDecision` evaluates on the cluster just
  like any other element.
- **Forms** (`.form`, in `resources/forms/`) — a form editor with live preview.
- **Pages** (`.page.json`, in `pages/`) — a **Page Composer** for assembling data
  grids and page layouts over the project's domain.

From a BPMN file you can **Deploy** the model to the engine (idempotent), **Start
instance** (optionally supplying initial variables as JSON) once the saved XML
matches what is deployed, and **Test** it entirely in the browser. At the project
level a **Run / Stop / Compile / Configure / Export** toolbar spawns
`deno run main.ts`, cross-compiles a binary, or zips the whole project — none of
which touches the engine data dir.

**Test** executes a model entirely in the browser, with no cluster round-trip and
fully offline. It is powered by **μ-nano** ("micro-nano") — a compact WebAssembly
build of the very same Rust `engine-core` that the cluster runs, aggressively
size-optimized (`opt-level="z"`, fat LTO, single codegen unit, `panic="abort"`,
`wasm-opt -Oz`) to about **0.5 MB**, and served gzip-compressed so it lands in
**~0.2 MB** over the wire. It is compiled for `wasm32` and loaded straight into
the page. Because it is the production engine (not a re-implementation), token
flow, gateways, timers, DMN, and FEEL expressions behave exactly as they will on
the server.

To run a test and inspect the trace:

1. Open a model in a project and click **Test**.
2. Start an instance (optionally supplying initial variables as JSON).
3. As each job activates, **complete** it with mock result variables, **fail** it,
   or throw a BPMN **error** — μ-nano advances the tokens accordingly.
4. **Fast-forward** the virtual clock to fire timers without waiting in real time.
5. Watch tokens move on the diagram, and read the **execution trace** — the
   ordered list of elements visited, jobs created/completed, and variable
   snapshots — to confirm the model behaves as intended before deploying anything.

### Build with an agent (`/agent`)

Every node serves a **live agent brief** at **`/agent`** (also `/agent.md`) — the
"point your agent here" surface (ADR 0051). Hand the URL to any coding agent
(Claude Code, Copilot CLI, or any MCP-driven assistant) and tell it *"read
`<node>/agent` and build me an app"*. The brief teaches the agent, addressed to
it directly, how to **author a Nano App on disk** and **link it into this node**
(import by reference, ADR 0041) — and how Nano works, so it can also just
*explain the product to you*. It is rendered per node, so it prints this node's
real base URL, on-disk projects root, import endpoint, installed packs and
scaffold templates: an agent needs no other context. A companion **`/llms.txt`**
points agent tooling at the brief plus the OpenAPI, AsyncAPI and JSON-Schema
specs. The Studio view exposes it as a **Build with an agent** button.

### Extensions

The console is **extensible**: an agentic SDLC pack, language support, app
templates, example apps, event triggers, and themes are shipped as **extension
packs** — plain npm packages discovered by the `nano-ide-ext` keyword and
installed from the console UI. The console can install any pack carrying that
keyword; the **first-party** packs live in the
[`nano-ide`](https://github.com/nanobpm/nano-ide) repo, and their manifest schema is
defined here in `server/src/console/extensions.rs`. See
[`docs/nano-repositories.md`](docs/nano-repositories.md) for how the pieces
relate. (This is a Studio-profile authoring tab.)

**Authoring a pack.** See the [**Extensions authoring & publishing
guide**](docs/extensions.md) for the `nano-ide.ext.json` manifest, the
marketplace, and every pack kind with a minimal example — language, app template,
example, trigger, and theme — plus agentic-SDLC apps and outbound connectors
(both capabilities layered on those kinds), the install/trust model, and how to
publish to npm.

### Explorer

A live process-instance explorer (variables, jobs, incidents) with BPMN XML for
each running or completed instance.

### Traces

**Execution traces** folded from the engine event stream: a searchable list of
process instances (active, completed, terminated) with a per-instance **timeline**
of the elements visited, jobs created/completed, and variable snapshots. Where
Explorer shows live instance state, Traces reconstructs the ordered history of how
each instance got there.

### Workers

Author TypeScript job workers in the browser and run them as sandboxed **Deno**
subprocesses over the Falcon protocol, with a live "Running" fleet view (status,
throughput, completed/failed, uptime, restarts) and streamed logs. **Running
workers requires [Deno](https://deno.com) installed on the host** (see below); the
tab still authors code without it. Selected workers can also be **exported as a
standalone application** you run yourself — see
[Exporting workers as an application](#exporting-workers-as-an-application).

#### A 90s IDE, on purpose

The worker editor is deliberately styled after the **integrated development
environments of the early-to-mid 1990s** — Borland Delphi, Microsoft Visual
Basic — where the wiring was invisible and you focused only on what *can't* be
automatically connected. The BPMN model is the form; the engine binds tasks to
job workers by type; the runtime, the Falcon transport, the retry/timeout
plumbing, and dependency resolution are all handled for you. You write only the
handler body. That ethos drives the code intelligence:

- **Full IntelliSense for the worker SDK, offline.** The embedded
  `@nanobpm/worker` SDK is registered with the editor's TypeScript language
  service as a local library, so `defineWorker`, the `job` object
  (`job.variables`, `job.complete`, `job.fail`, `job.error`, `job.jobKey`,
  `job.processInstanceKey`, …) and every option auto-complete and type-check with
  no network access whatsoever.
- **Automatic Type Acquisition (ATA) for any npm package.** Import *any* package
  — `import _ from "lodash"`, `import { ... } from "@camunda8/orchestration-cluster-api"`
  — and the editor fetches its type definitions on the fly (from the jsdelivr
  CDN) and lights up completion, hovers, and signature help for it. This is a
  progressive enhancement: it needs network access, and degrades silently to
  SDK-only IntelliSense when offline. The TypeScript compiler is loaded lazily,
  only the first time you edit a worker file, so the editor stays lightweight.
- **Cross-file IntelliSense — sibling helpers and a shared library.** Reusable
  logic doesn't have to live in one file. Split a worker into multiple files and
  `import { helper } from "./helper.ts"`; the editor resolves siblings with full
  types. For logic shared across *every* worker, drop a module into the
  workspace **Shared library** (the `📚 Shared library` entry below the worker
  list) and import it from anywhere with the `@lib/` alias —
  `import { fmtMoney } from "@lib/money.ts"`. In keeping with the invisible-wiring
  ethos, there is nothing to configure: the `@lib/` import map alias is injected
  into every worker's `deno.json` automatically, so the same import resolves in
  the editor (IntelliSense), at runtime (the Deno supervisor), and in an exported
  app (the bundled `lib/` folder).

### Workspace vs cluster data

The console keeps the user's **authoring source of truth** in a *workspace*
directory, deliberately separate from the engine's data dir so that deleting
cluster data leaves your projects and workers intact. The workspace holds the
standalone **Workers** tab's files at the top level, and every RAD **project**
under `projects/` as a self-contained, runnable Deno application:

```text
<workspace>/
├── workers/<name>/              # standalone Workers-tab workers (worker.ts, deno.json, …)
├── lib/                         # shared library modules for those workers (@lib/…)
├── nano-generated/worker-sdk.ts # embedded Deno worker SDK (auto-written)
├── .deno-cache/                 # DENO_DIR for worker dependency caching
└── projects/<project>/          # one directory per RAD project (a runnable Deno app)
    ├── nanobpm.project.json      # project config (name, deploy target, platforms)
    ├── deno.json                 # import map (@nanobpm/worker, @lib/) + start task
    ├── tsconfig.json             # standard-tooling type resolution
    ├── package.json              # npm identity + `npm install` deps
    ├── main.ts                   # entrypoint: deploys processes + starts workers
    ├── resources/
    │   ├── processes/<name>.bpmn # BPMN models (deployed to the engine on run)
    │   ├── decisions/<name>.dmn  # DMN decisions
    │   └── forms/<name>.form     # forms
    ├── pages/<name>.page.json    # Page Composer pages
    ├── workers/<name>/           # the project's own workers (worker.ts, deno.json, …)
    ├── lib/                      # the project's shared library modules (@lib/…)
    ├── nano-generated/worker-sdk.ts  # embedded Deno worker SDK (auto-written)
    └── .deno-cache/              # DENO_DIR for dependency caching
```

| Variable | Meaning |
| --- | --- |
| `NANOBPMN_WORKSPACE_DIR=<dir>` | Console workspace root (holds the standalone `workers/` plus, by default, `projects/` — unless `NANOBPMN_PROJECTS_DIR` moves the projects root). Default `./nanobpm-workspace`. Survives deletion of `NANOBPMN_DATA_DIR`. |
| `NANOBPMN_PROJECTS_DIR=<dir>` | Override the projects root directly. Default `<workspace>/projects`. |
| `NANOBPMN_DENO_BIN=<path>` | Explicit path to the Deno binary used to run workers. Default: `deno` on `PATH`, else `~/.deno/bin/deno`. |

### Embedded workers (Deno)

Each worker is a directory under `workers/<name>/` whose `worker.ts` imports the
embedded SDK and declares a handler:

```ts
import { defineWorker } from "@nanobpm/worker";

defineWorker({
  type: "my-job",
  maxParallelJobs: 10,
  async handle(job) {
    // job.variables holds the activated job's variables; npm libraries are
    // available via `npm:` specifiers.
    return { result: 42 };          // resolves -> completeJob({ result: 42 })
    // or: job.fail("boom") / job.error("CODE", "msg")
  },
});
```

The supervisor runs one sandboxed `deno run` subprocess per enabled worker that
speaks the Falcon protocol directly. **Deno is required to *run* workers** (in the
console or as an exported app): install it from <https://deno.com> so the `deno`
binary is on `PATH`, or point `NANOBPMN_DENO_BIN` at it. If Deno is not installed
the Workers tab still authors and exports code, but starting a worker reports the
runtime as unavailable.

### Exporting workers as an application

Workers don't have to run inside the console. From the **Workers** tab, click
**Export app**, tick the workers you want (all are selected by default), and
**Download zip** to get a self-contained, runnable application — handy for
shipping a worker fleet to another machine or running it outside Nano.

The downloaded `nano-workers-app.zip` unpacks to:

```text
nano-workers-app/
├── main.ts                 # entrypoint: deploys models, then starts the workers
├── deno.json               # import map (@nanobpm/worker) + `deno task start`
├── README.md               # how to configure and run it
├── sdk/worker-sdk.ts       # the embedded worker SDK (same source as the console)
├── workers/<name>/…        # each selected worker's source
└── resources/              # drop your .bpmn models here (auto-deployed on startup)
```

On startup the app **deploys every `.bpmn` file in `resources/`** to the target
engine, then runs the bundled workers. Add your own models by dropping their
`.bpmn` files into `resources/` before starting. Run it with Deno:

```bash
cd nano-workers-app
# Point at your Nano gateway (defaults to http://127.0.0.1:8080):
export NANOBPMN_BASE_URL=http://127.0.0.1:8080
deno task start            # runs with --allow-net --allow-read --allow-env
```

The bundled `README.md` documents the same steps and the required permissions.

**Dependencies travel with the app.** The exporter scans the selected workers'
source for `import`/`import(...)` specifiers, resolves each to its npm package
(handling scoped names, subpath imports, and `npm:` specifiers), and writes a
dependency manifest into the app's `deno.json` import map. So a worker that
imports `lodash` or `@camunda8/orchestration-cluster-api` produces an export
whose `npm:` resolution works out of the box — no manual dependency wrangling.

## Cluster tuning

Single-node defaults are tuned for correctness and need no thought: one partition,
`sync` durability, backpressure on. The knobs below only matter once you cluster
(`NANOBPMN_RAFT=on`, `NANOBPMN_NODES=…`) or push a node toward saturation. They are
deliberately orthogonal — pick each axis independently for your workload.

### Topology: nodes, partitions, replication factor

- **Partitions ≥ nodes, always.** Each partition is owned by exactly one node
  (`owner = partition % num_nodes`), and a clustered node that owns *zero*
  partitions aborts on startup. The clean, balanced choice is **one partition led
  per node**. Use a higher multiple only if you want finer rebalancing granularity
  — `NANOBPMN_PARTITIONS` cannot be changed in place (the journal layout differs).
- **`NANOBPMN_RF=3` for fault tolerance.** RF is the number of copies per
  partition; `RF=1` (default) is no replication. `RF=3` survives one node loss.
  Keep `RF` ≤ node count; use an **odd** voter count per group.
- **Throughput scales with partitions, not nodes alone.** Each partition is a
  single writer (one fsync + apply loop), so aggregate write throughput tracks the
  number of *led* partitions.

### What to tune, by situation

| If you want… | Tune | To |
| --- | --- | --- |
| **Lowest write latency** (small/medium concurrency) | `NANOBPMN_DURABILITY=async` + `NANOBPMN_REPLICATION=leader-durable` | Ack on the leader's local durable append+apply — no fsync-before-ack wait and no follower round-trip. Cost: a just-acked tail can be lost on an ungraceful leader loss (bounded, never divergent). |
| **Zero-data-loss durability** (the default) | leave `NANOBPMN_DURABILITY=sync`, `NANOBPMN_REPLICATION=quorum` | `200`/`204` means fsync'd locally **and** majority-committed. Strongest guarantee; highest per-write latency. |
| **High throughput under worker over-provisioning** | Handled automatically by the default `NANOBPMN_REPLICATE_ACTIVATION=auto`; force with `=0` (leader-local) or `=digest` | `auto` already keeps activation off the Raft log for large-payload partitions (avoiding the ~125× collapse) while keeping strict leases for small ones. Force `=0`/`=digest` to keep the lease off the log unconditionally (~3× activation throughput). All modes stay at-least-once. |
| **Even job drain across nodes** | on by default (`NANOBPMN_ACTIVATION_FAIRNESS=2`); set `=off` to disable | Default `2` caps each source by its live backlog so the deepest node drains fastest; `1` is rotation+quota only; `off` restores strict local-first. |
| **A producer that outpaces the workers** | leave backpressure on (default **Adaptive**), or pin `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT=<n>` | Adaptive (AIMD) sizes the in-flight watermark from measured latency and sheds excess creates with `503 RESOURCE_EXHAUSTED`, so the producer converges to the drain rate. |
| **Behaviour at the saturation ceiling** | `NANOBPMN_SLA_MODE=latency` (default) or `=admission` | `latency` holds a tight tail and a bounded backlog via the active-backlog governor, sacrificing peak throughput (**time-to-complete SLA**). `admission` drops the governor to run at the true drain ceiling (~+48% throughput in test), accepting a much longer tail and a backlog that grows to the memory-safety rails (**start-every-process SLA**). The AIMD engine-overload guard and the memory-safety rails stay armed in both modes; arming AIMD in `admission` tightens its tail (~40% lower p90) at no throughput cost, but does not by itself bound the backlog. |
| **Bounded memory after bursts** | `NANOBPMN_IDLE_PURGE_MS`, `NANOBPMN_HISTORY_MAX_INSTANCES`, `NANOBPMN_VAR_SPILL*` | Idle-purge compacts hot state and returns freed arenas to the OS. Cap retained completed instances to bound read-model growth. |
| **Bounded var-store WAL on disk** | `NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS` (default `30`, `0`/`off` disables) | Periodically runs `wal_checkpoint(TRUNCATE)` on the durable var-store so its `-wal` file can't grow without bound under a sustained large-payload write load. |

### Recommended profiles

- **Strong durability (default, money-movement workloads):** `RF=3`,
  `NANOBPMN_RAFT=on`, leave durability/replication/activation unset. Every ack is
  fsync'd and quorum-committed; activation defaults to `auto` (strict replicated
  leases for small payloads, digest for large so throughput never collapses). Pair
  with a persistent `NANOBPMN_DATA_DIR` per node.
- **Low latency (interactive workflows, modest concurrency):** `RF=3`,
  `NANOBPMN_DURABILITY=async`, `NANOBPMN_REPLICATION=leader-durable`,
  `NANOBPMN_REPLICATE_ACTIVATION=digest`. Bounded-loss, self-healing failover.
- **Max throughput / benchmarking:** one partition led per node,
  `NANOBPMN_REPLICATE_ACTIVATION=0`. Job-drain fairness (`=2`) and create placement
  (`balanced`) are already on by default; set `NANOBPMN_CREATE_PLACEMENT=off` only
  for a strict apples-to-apples blind-round-robin baseline. Always benchmark the
  **release** binary.

> Durability/replication tiers are chosen at startup from the on-disk log; switching
> tiers on an existing data directory is unsupported. Start each reconfigured cluster
> from a fresh `NANOBPMN_DATA_DIR`.

## Trace capture

Nano BPM can record every instance's inputs so runs can be **replayed and analysed
later** (the basis for the runtime process-optimization research direction below).
With the c8ctl plugin:

```bash
c8ctl nano start 3 --capture
c8ctl nano status            # shows "trace capture: on"
```

`--capture` sets `NANOBPMN_TRACE_STIMULI=1` on **every** node, enabling the Tier 2
recorded-input (stimuli) log and auto-enabling Tier 1 variable capture. Read a
trace back from any node:

```text
GET /console/api/traces/{instanceKey}
  → { creationVariables, stimuli[], <per-incident variables> }
```

| Env var | Default | Purpose |
| --- | --- | --- |
| `NANOBPMN_TRACE_STIMULI=1` | off | Enable Tier 2 recorded-input replay (also enables Tier 1). |
| `NANOBPMN_TRACE_VARIABLES_MAX_BYTES` | 16384 | Max captured variable payload bytes. |
| `NANOBPMN_TRACE_STIMULI_MAX` | 1024 | Max recorded stimuli per instance. |
| `NANOBPMN_TRACE_CAPACITY` | 2000 | Max traced instances retained in memory. |
| `NANOBPMN_TRACE_FILE` | *(unset)* | Append each **finished** trace to this file as one NDJSON line, then drop it from memory. Durable across restart and bounds memory to the active set, so recorded-input capture can stay on in production. Unset keeps the in-memory-only behaviour. Each line is the same JSON as `GET /console/api/traces/{key}`. |
| `NANOBPMN_TRACE_FILE_MAX_BYTES` | *(unset)* | Rotate the trace file at this size (`<path>.1` … `<path>.<keep>`). Unset leaves rotation to `logrotate` — which **must** use `copytruncate`, because the writer opens the file once and never reopens it (rename/create rotation would leave it writing to the renamed file). |
| `NANOBPMN_TRACE_FILE_KEEP` | 5 | Rotated trace files to keep. |
| `NANOBPMN_TRACE_FILE_TAIL` | 0 | Recently-finished traces to keep in memory for the console after writing them to the file. |
| `NANOBPMN_TRACE_FILE_QUEUE_BYTES` | 16777216 | Byte budget of the writer channel between the engine and the sink thread. A full budget **drops** finished traces (the `dropped` counter returned by `sink_stats()`) rather than back-pressuring the engine — raise it if drops appear under load. Bounded in bytes (not trace count) so a stalled disk can never retain more than this much serialized trace. The budget bounds *backlog*, not one trace's size: a single trace larger than the whole budget is still admitted to an **empty** queue (a healthy writer drains it immediately), so it is a backlog ceiling, not an absolute memory ceiling. |
| `NANOBPMN_TRACE_FILE_FLUSH_MS` | 1000 | How often the writer thread flushes its buffer to the file, on an absolute deadline that does not move when new traces arrive. A crash can lose at most one flush interval of traces. |

> **Roadmap:** beyond the Camunda-compatible engine, see
> [`docs/process-optimization-design.md`](docs/process-optimization-design.md) for
> the closed-loop **runtime process optimization** direction — execution trace
> export, deterministic recorded-input replay, WASM-powered simulation, a cost/SLA
> model, canary experiments, and an LLM-in-the-loop reasoning plane built on the
> engine's event-sourced core.

## Debugging BPMN processes

An **in-engine process debugger** (proof-of-concept) lets you set a breakpoint on
a flow node, pause a process instance *inside the engine* mid-command, inspect its
variables, and step or resume — with the paused element highlighted on the diagram
in VS Code. It is a four-piece stack: the engine-core stepping executor →
`engine-wasm` debug surface → the standalone [`dap-adapter`](dap-adapter/) (Debug
Adapter Protocol) → the [`vscode-bpmn-debug`](vscode-bpmn-debug/) extension.

For a copy-pasteable runbook — a 30-second **headless proof** (no VS Code) and the
full **F5 click-the-diagram** walkthrough — see
[**`docs/debugger-getting-started.md`**](docs/debugger-getting-started.md).

## Building from source

End users do not need to build anything — the [c8ctl plugin](#quick-start-with-c8ctl)
ships a prebuilt binary. To build from source, run the engine tests, or work on
the code-generation pipeline, see **[DEVELOPMENT.md](DEVELOPMENT.md)**.
