# ProcessOS — Design Proposal

> Status: draft / direction. Companion to `docs/process-optimization-design.md`
> (the "what" and "why" of runtime process optimization). This document is the
> "where it lives": ProcessOS is the **separate component** that hosts the
> optimization loop, so that **Nano handles production and ProcessOS handles
> optimization** — a clean separation of concerns.

## 0. Thesis

Nano is a lean, low-resource, WASM-able BPMN engine on the production hot path. The
optimization loop (reasoning, simulation, experiment statistics, an LLM, static
analysis, cost/feedback ledgers) is heavyweight, network-egressing, and key-holding.
Bolting it into the gateway would forfeit the very property that makes Nano
interesting. So it lives in its own crate with its own webserver:

```
                 public contracts only (HTTP)
   ┌──────────┐  ───────────────────────────▶  ┌──────────────┐
   │   Nano   │   read:  trace + metrics export │  ProcessOS   │
   │ (engine, │   write: deploy + routing table │ (optimizer,  │
   │ gateway) │  ◀───────────────────────────   │  webserver)  │
   └──────────┘                                  └──────────────┘
   handles PRODUCTION                            handles OPTIMIZATION
```

**The dependency is one-way and enforced as a build rule.** ProcessOS depends on
Nano's public surfaces; Nano never depends on ProcessOS and builds/runs with it
absent. The single engine-side concession is the **policy-free routing primitive**
(see `process-optimization-design.md` §7) — cohort→version selection at
`CreateInstance`, configured by a routing table ProcessOS edits over the public
control endpoint. Nano stores and applies a table; it has no concept of
"experiment", "candidate", "guardrail", or "rollback".

## 1. Boundary — the contracts (design these first)

Everything ProcessOS does crosses one of three contracts. Pin them, version them
(`X-ProcessOS-Contract: 1`), and treat them as the API the two components evolve
against independently.

### 1.1 Read contract (Nano → ProcessOS)

Already exists or is the natural extension of Stage T1:

- **Trace export** — `GET /console/api/traces` (list), `/console/api/traces/{key}`
  (the §3 canonical trace), `/console/api/traces/{key}/otel` (OTLP/JSON spans).
  Plus a **streaming** form for ingest (SSE/WS tail of the exporter, or an OTLP
  push endpoint ProcessOS exposes and Nano's exporter is pointed at). ProcessOS
  prefers *push* so it never polls the hot path.
- **Metrics** — `GET /console/api/metrics`, `/console/api/cluster/metrics`
  (throughput, p99, active instances, per-node).
- **Process catalogue** — the existing Camunda `process-definitions` + the
  console models API (versions, deployed XML via `/v2/process-definitions/{k}/xml`).
- **(T4) Realized cost** — the `__cost` lineage carried in the trace (token spend,
  external-API $), per element/version.

ProcessOS treats these as **read-only and authoritative**. It never reaches into
Nano's journal, read DB, or engine state directly.

### 1.2 Control contract (ProcessOS → Nano)

Only two verbs, both already-public or thin nanobpmn extensions:

- **Deploy** — the standard Camunda `POST /v2/deployments` (idempotent; a changed
  model is a new version). This is how a candidate process gets onto the cluster —
  *exactly the path a normal client uses*, no backdoor.
- **Routing table** — a new policy-free control endpoint (nanobpmn extension under
  `/console/api/routing` or via `spec-patches`). Body is plain data:
  `{ processId, rules: [{ cohort: <predicate|weight>, version }], default }`.
  Setting 5% → candidate is a `PUT`; auto-rollback is a `PUT` back to 100%
  incumbent. Journaled, durable, failover-safe. **This is the only new write
  surface the optimization story adds to Nano.**

### 1.3 No fourth contract

ProcessOS does **not** get privileged access. If it needs something Nano can't yet
express (e.g. shadow-execution dry-run worker semantics, idempotency keys), that is
a *Nano public-API* addition designed on its own merits, not a ProcessOS hook.

### 1.4 Two engines — the client's target vs. ProcessOS's own

The three contracts above bind ProcessOS to the **client's production Nano — the
read-only analysis target**. But ProcessOS's own optimization loop is *itself* a Nano
process (§7.9, §10): hypothesise / evaluate are tasks, the engine is the verifier, and
a human user task makes it a cockpit. That meta-workload **cannot run on the client's
target** — the target is read-only, and the consultants (ProcessOS + human + droid) do
not deploy their own choreography onto the client's engine. So ProcessOS runs its
**own** Nano engine for the pilot loop, distinct from the target it analyses:

- **target** (`NANO_TARGET_URL`) — the client's production engine; read over §1.1,
  written only via the §1.2 deploy/route verbs when promoting a cohort. Never hosts
  ProcessOS's pilot loop.
- **own** (`PROCESSOS_NANO_URL`, or a child engine ProcessOS supervises) — hosts the
  pilot process and the meta-workers that drive it. ProcessOS can **spawn and
  supervise** this engine itself (opt-in): learn its bound port from the gateway's
  stdout, health-check it, **deploy the pilot BPMN on boot**, and reap it on shutdown —
  so a factory-fresh instance comes up self-hosting with no external setup. In a
  single-Nano dev box both resolve to one URL (back-compatible).

This is the physical substrate for "the first target is always itself" (§10): the
self-optimizing loop runs on an engine ProcessOS owns, while the client's engine stays
a pure read-only target.

### 1.5 Workspaces — many engagements over many sources

A single ProcessOS analyses **many** processes across **many** customers, against a mix
of **live** targets and **loaded historical datasets** (a consultant reasoning over a
captured trace export offline). So the single-tenant `target` of §1.4 generalises into a
filesystem-rooted **workspace tree** (`PROCESSOS_WORKSPACES_DIR`, default
`<data_dir>/workspaces`):

```
<root>/<customer-slug>/customer.json
<root>/<customer-slug>/<process-slug>/process.json   # binds a source
<root>/<customer-slug>/<process-slug>/traces/        # optional loaded dataset
```

Each process binds to a **`TraceSource`** — a small enum the read path is generalised
over (`report::build_over(&TraceSource, …)`), so Insights folds identically whether the
source is:

- **live** — a `NanoClient` against `targetUrl` (the §1.1 read contract), or
- **dataset** — a `DatasetSource` that loads a folder of instance-trace JSON (the exact
  `GET /console/api/traces/{key}` shape, eagerly into memory so request-time reads do no
  blocking IO) and derives the trace summaries the report needs.

The tree is **disk-curatable** (list operations scan the directory, so hand-made
customer folders appear without going through the API) and **traversal-safe** (slugs are
single lowercased `[a-z0-9-_]` path segments). This keeps the §1.4 target/own split
intact — a live binding is just a target URL per process — while adding the offline
dataset source the consultant workflow needs. The mutation surface (create customer /
process, bind a source) and reads (`…/insights`) are a thin CRUD layer; `cockpit` /
`prompts` / `pilot` remain global for now, and re-scoping them per selected process is
the natural follow-up.

## 2. Internal architecture (inside the ProcessOS crate)

A pipeline of independently-testable stages, mirroring the optimization design's
Tiers and §6–§9:

| Stage | Module | Responsibility | Leans on |
|------:|--------|----------------|----------|
| Ingest | `ingest` | Subscribe to the trace/metrics push stream; normalize to the §3 schema | read contract |
| Store | `store` | Durable ledgers: traces/exemplars, cost, **feedback ledger** (`hypothesis→evidence→transform→experiment→decision`), routing history | `rusqlite` (bundled), like the gateway |
| Report | `report` | Token-budget-aware **performance report** per version: bottleneck path, incident clusters, queue-vs-service split, var→outcome correlation, worst-case exemplars | store |
| Simulate | `sim` | Ephemeral engines on virtual clock: trace-driven replay (Tier B) + distributional "what-if" | **`engine-core`** (native) / `engine-wasm` |
| Reason | `reason` | LLM **hypothesis generator** over a constrained **typed transform space**; emits candidate model + anchor map | pluggable LLM provider |
| Verify | `verify` | Static soundness (no deadlock, token-safe, bounded) + **replay-backed equivalence** (same inputs → same outcomes, faster/cheaper). Authoritative gate. | `sim`, `engine-core` |
| Experiment | `experiment` | Compile a verified candidate into routing-table edits; run canary/shadow; **sequential tests/CUSUM** guardrails; auto-rollback | control contract |
| Control | `control` | Per-process **policy** (objective + constraints + autonomy + blast radius); orchestrates the loop; writes the feedback ledger | all of the above |
| Web | `web` | axum server: ProcessOS REST API + (optionally) its SPA; serves reports, experiments, ledger | `axum` |

**Reuse, don't re-implement.** `sim` and `verify` embed `engine-core` directly
(path dep, `features=["serde"]`) for *exact* native replay/simulation — the same
pure `(state, command, now) → events` function the production engine runs, which is
what makes counterfactuals trustworthy. The browser modeler's WASM test-run
(`engine-wasm`) is the same engine; ProcessOS can also drive headless WASM for
sandboxed execution of machine-generated processes (the §5.3 safety boundary).

## 3. Tech stack & crate layout

A new top-level crate, sibling to `engine-core` / `server` / `engine-wasm` (no root
workspace — they stay independent, per repo convention):

```
processos/
  Cargo.toml          # axum 0.8, tokio 1, rusqlite 0.32 (bundled), serde,
                      # reqwest (talk to the gateway), an LLM client trait
  src/
    main.rs           # webserver bootstrap, config, the control loop tick
    contracts/        # typed clients/types for the 3 §1 contracts (versioned)
    ingest.rs store.rs report.rs sim.rs reason.rs verify.rs experiment.rs control.rs
    web/              # REST handlers (+ optional embedded SPA, rust-embed)
  Cargo.toml deps:  engine-core = { path = "../engine-core", features=["serde"] }
```

- **Webserver:** `axum` (match the gateway) on its own port (e.g. `PROCESSOS_PORT`).
- **Talking to Nano:** `reqwest` against the gateway base URL (`NANO_BASE_URL`).
- **Storage:** `rusqlite` bundled (self-contained, like the gateway's read DB) for
  the ledgers; trace exemplars + cost + feedback + routing history.
- **LLM:** a `ReasoningProvider` trait with an **OpenAI-compatible** HTTP impl, so it
  works against a **local llama.cpp** server *or* a hosted model — no hard
  dependency on any vendor. Keys/egress live entirely in ProcessOS's boundary.
- **Determinism for sim/verify:** inherited free from `engine-core` (time via
  injected `now`); ProcessOS adds no wall-clock into any replayed run.

## 4. UI / operational surface

Decision (revisitable): **console-proxy primary, standalone optional.**

- ProcessOS serves its own REST API and a small SPA, but the **primary UX is a new
  "Optimization" tab in the existing Nano console**, which proxies to ProcessOS
  (one pane of glass; the console already aggregates cluster views). If ProcessOS is
  not running, the tab shows a "not configured" state — the console never hard-
  depends on it.
- Standalone ProcessOS UI remains available for deployments that run it detached
  from the console.

This keeps operators in one place while preserving the clean process/security
separation underneath.

## 5. Deployment topology

- **Separate process / service**, co-located or remote. Scales independently of the
  gateway (the reasoning/sim plane is bursty and CPU/GPU-heavy; the gateway is the
  steady hot path).
- **Two engines, not one (§1.4).** ProcessOS reads the client's production Nano (the
  read-only *target*) but runs its pilot loop on its **own** Nano engine, which it can
  spawn and supervise as a child process (learn port, health-check, deploy the pilot on
  boot, reap on shutdown). The client's engine never hosts ProcessOS's meta-workload;
  the own engine is where "the first target is itself" (§10) physically runs. The own
  engine is a normal Nano build (console + capture, so its own pilot traces feed back
  through the §1.1 read contract).
- **Read path is push:** Nano's exporter is pointed at ProcessOS's OTLP/stream
  ingest, so optimization never adds latency or polling load to production.
- **Security boundary:** LLM keys, outbound network egress, and business-data-bearing
  Tier-B bundles live only inside ProcessOS — a different blast radius than the
  engine. mTLS / token between the two; ProcessOS authenticates to the gateway as a
  normal API client for deploy + routing writes.
- **Absent-by-default:** a Nano cluster with no ProcessOS behaves exactly as today.

## 6. Mapping to the staged rollout

ProcessOS materializes the optimization design's stages T1–T7 as it grows; each is
useful alone:

| Stage | ProcessOS gains |
|------:|-----------------|
| T1 | `ingest` + `store` + a read-only **Insights** report (surfaces existing traces/metrics) |
| T2/T3 | `sim` (native + WASM replay/what-if) — "explain this instance", "what-if this change" |
| T4 | cost ledger + objective-function dashboards |
| T5 | `experiment`: canary via the **routing-table** control endpoint + guardrails/auto-rollback (needs the §7 routing primitive in Nano) |
| T6 | `verify`: soundness + replay-backed equivalence gate; typed transform space |
| T7 | `reason` + `control`: LLM hypotheses, policy objects, the closed feedback loop |

**Start at T1**: stand up the crate, ingest the existing trace/metrics contracts,
and render a report. It is immediately useful (a richer Insights view) and forces us
to nail the read contract before anything autonomous is built on it.

## 7. MVP — the optimization harness (evaluation rig)

The first thing to build is **not** the production optimizer but the **harness that
proves the method works** — and, conveniently, it is the *same loop* with the data
source swapped. This is where we validate and harden the core value proposition
("can this find better candidates?") and iterate the algorithm under controlled
conditions.

### 7.1 A scenario

The harness is driven by a **Scenario** — a self-contained, deterministic test case:

- **Test model** — the starting BPMN process.
- **Golden model** — a known-better variant we *want exploration to discover*. It is
  the meta-eval oracle: it lets us score not just a candidate but the **search
  method itself** ("did it recover the golden? get within X%? beat it?"). Production
  scenarios have no golden — it exists only to validate the rig.
- **Worker set** — for each service task, a **mock worker** that is a pure,
  **seeded** function `(vars, seed) → (vars', cost, latency, outcome)`. Determinism
  is the whole point: the same input + seed always yields the same result, so base
  and candidate runs are a clean A/B with variance controlled.
- **Latent options** — alternative workers the optimizer may swap in, each with its
  own cost/latency/quality profile (e.g. *a cheaper LLM executor: lower `cost`, but
  higher latency or a higher incident/lower-quality rate*). These options **are** the
  MVP transform space — concrete, typed, individually checkable.
- **Input set with correlated results** — process inputs paired with their
  **expected outputs**, so a candidate is scored on **correctness** (did it preserve
  the business outcome?), not only on latency/cost. This is the §8 verifier
  expressed *empirically*: an optimization that changes outcomes is disqualified, not
  merely cheaper.

### 7.2 The loop

1. **Collect the base dataset.** Run the test model across every input on a Nano
   engine; collect `(output, e2e latency, cost, incidents)` per instance — i.e. the
   §3 trace plus realized cost. This is exactly the **T1 Insights** fold, per run.
2. **Hypothesise.** The LLM proposes candidate processes from the Insights report,
   drawing from the latent options + parameter tweaks (retry/timeout). *MVP may start
   with a **baked** generator — the enumerated worker-swap permutations — to validate
   the measurement+ranking rig before the LLM is in the loop.*
3. **Materialise + validate.** Emit each candidate's BPMN; validate it parses and is
   sound (no deadlock, token-safe) before it is allowed to run.
4. **Evaluate at scale.** Run every candidate over the **same input set + seeds**,
   collecting the same dataset. Replaying identical inputs/seeds is what makes the
   comparison fair.
5. **Rank.** Present each variant's dataset and a ranking across axes — **e2e
   latency, cost, incident rate, and correctness-vs-expected** — plus, in test mode,
   **distance to golden**. Sanity check: feeding the golden in as a candidate must
   rank it at/near the top, or the rig is wrong.

### 7.3 Two execution backends, one `Runner` interface

The loop is identical; only *where instances run* changes:

- **`SimRunner` (native `engine-core`, virtual clock)** — embeds the real engine with
  injected `now`; mock workers advance the clock by their modelled latency and emit
  cost. Fast, deterministic, zero orchestration, CI-friendly. **This is the MVP
  backend** — it isolates the *algorithm* from infrastructure noise, so we can prove
  "can we find better candidates?" cheaply and repeatably.
- **`ClusterRunner` (real Nano engine(s) + embedded Node workers)** — runs the same
  scenario on actual gateways for realistic latency/throughput. It doubles as a
  **Nano stress test** and as the **demo** that shows the live data ProcessOS reasons
  over. (Reuses the existing perf-matrix-style load path.)

**This unifies test and production.** A Scenario's dataset source is either
*synthetic* (mock workers + inputs + golden — test/demo) or *live* (a real Nano
engine's traces — production). In production the harness **skips step 1's generation**
and reads the live dataset through the §1 read contract; steps 2–5 are unchanged.
The MVP builds the synthetic `SimRunner` path; the live path is already half-built
(T1 ingest).

### 7.4 MVP build order

| Slice | Ships | Proves | Status |
|------:|-------|--------|--------|
| **M0** | `Scenario` schema + `SimRunner` (engine-core native, seeded mock workers, virtual clock) collecting `(output, latency, cost)` → base dataset | deterministic capture + cost/latency modelling work | ✅ done |
| **M1** | **Baked** candidate generator (enumerate latent worker-swaps) + BPMN validate + evaluate-all + **ranking** (incl. golden-as-candidate + distance-to-golden) | the measurement + ranking rig is correct end-to-end, *no LLM yet* | ✅ done |
| **M2** | The **LLM hypothesis** step: the model proposes candidates (worker-swaps and optional structural BPMN rewrites), which are validated then run through the same SimRunner + ranking | the search method can discover improvements/the golden | ✅ done |
| **M3** | `ClusterRunner` (real Nano + Node workers) for scale/latency realism, Nano stress-test, demo; wire the **production** (live-source, generation-skipped) path | realism, stress, and the production loop | ⏳ next |

**Start at M0+M1.** They need no cluster orchestration and no LLM, embed the real
engine for trustworthy numbers, and directly answer the core question. Only once the
rig reliably ranks the golden best do we let the LLM (M2) and the cluster (M3) in.

**M2 LLM provider abstraction (implemented).** The hypothesis step reaches the
model through a pluggable client (`harness/llm.rs`) with two providers, so the same
path serves a model anywhere on the spectrum:

* `openai` — the OpenAI **chat-completions** wire shape, which local `llama.cpp`
  (`--api`), Ollama (`/v1`), vLLM, LM Studio, and OpenAI all speak. This is the
  default; point `baseUrl` at a local model on the network.
* `anthropic` — the Anthropic **messages** API.

Provider/baseUrl/model/key/limits are read from the environment
(`PROCESSOS_LLM_*`) and overridable per request, so a local model can be A/B'd
against a hosted one without a restart. The model only *proposes*; the harness
still measures and ranks every proposal with the SimRunner, so a hallucinated
"improvement" that doesn't help is caught by the numbers. Invalid proposals
(unknown worker ids, unparsable rewritten BPMN) are rejected with a reason and
reported in `llm.rejected`. This keeps the load-bearing one-way boundary intact:
the LLM is an *input to ProcessOS's reasoning*, never a path back into Nano.

### 7.5 M3 — what the latent-exploration analysis changes (and doesn't)

`docs/processos-latent-process-exploration.md` widens the *future* optimization
surface, but it **sharpens and constrains M3 rather than redefining it**:

- **Transform space for M3 stays worker-swaps.** The pattern catalogue (§1) and
  the discovery loop (§3) are explicitly later work that "changes no engine code."
  M3 must not pull structural rewrites or pattern discovery forward; it proves
  *realism, scale, and the production loop* on the existing transform space.
- **The `ClusterRunner` is the home of the signals the `SimRunner` structurally
  cannot produce.** The SimRunner runs each input on a *fresh* engine,
  sequentially — it measures clean per-instance latency/cost but **no
  cross-instance throughput, no tail under contention, no queue-vs-service split
  under load**. The exploration doc's §2 names exactly these as missing —
  *distributions not means (p99/p999), queueMs vs serviceMs, contention context
  (concurrency, queue depth)*. So the `ClusterRunner`'s result type must be
  **richer than the SimRunner's** `(avgLatency, avgCost)`: it reports throughput,
  tail latency (p95/p99), and the queue/service decomposition the live T1 contract
  already carries per job (`contracts.rs` `queueMs`/`serviceMs`).
- **Public-surface-only, restated.** Per the exploration doc's footer, every
  Nano-side need is "a ProcessOS-driven extension to Nano's public surfaces, never
  an engine-core change." The `ClusterRunner` therefore *creates instances and
  deploys over the public REST/falcon*, and *reads over the T1 console
  contract* — never engine-core, never the journal/read-DB.
- **No auto-apply in M3.** The doc's safety spectrum (§0) plus its two deferred
  data investments — a **per-task side-effect/purity profile** and a **delayed
  correctness signal** — are the prerequisites for *autonomous* application. M3
  stays in *measure → rank → suggest*; worker swaps are safe to *measure* for
  cost/latency, and quality-affecting swaps remain gated by the harness's existing
  expected-output correctness score, never promoted on their own.

**M3 build slices** (in order; each verifiable on its own):

1. **Production live-source baseline (generation-skipped path).** Read the live
   dataset through the T1 contract and fold it into the *baseline a candidate
   search must beat* — including the doc's tail signals (p95/**p99**) and the
   queue/service split. Verifiable against a mock Nano, no cluster required.
2. **`ClusterRunner` — two halves.** (a) *Measure*: from the live traces a real
   run produced (T1 contract), compute the at-scale signals the SimRunner
   structurally cannot — **throughput**, **e2e tail (p50/p95/p99)**, and the
   **queue/service split under contention** (`harness/cluster.rs`,
   `GET /api/harness/cluster`). Pure + tested; point it at a cluster the
   perf-matrix is driving. (b) *Drive*: generate concurrent load against a real
   gateway — producer over the **v2 REST API** (`POST /v2/process-instances`,
   `/v2/deployments`) + workers over the **v2 job APIs**
   (`POST /v2/jobs/activation`, `POST /v2/jobs/{key}/completion`) or
   `/falcon`, reusing the perf-matrix load path. The science (a)
   lives in ProcessOS; the load-gen plumbing (b) is the documented integration
   boundary the perf-matrix already provides.

   > **Measured fact — trace data is partition-local.** Validated on a live
   > `c8 nano start 3` cluster (RF=1, 3 partitions): each node's
   > `/console/api/traces` returns only the instances **its** partitions own
   > (observed ~102 / 102 / 100 of 304 instances on nodes 0/1/2). A single
   > `NANO_BASE_URL` therefore measures one partition's slice, not the cluster.
   > The ClusterRunner now discovers every node via `GET /v2/topology` and
   > **unions their traces** (`cluster_endpoints` + `build_cluster_summary_over`,
   > deduping by instance key; trace *detail* is fetched from each instance's
   > owning node). End-to-end check: pointed at node 0 it reported all **304**
   > instances (not 102), p99 e2e 1540 ms, and `avgQueueMs 6166 ≫ avgServiceMs 67`
   > — the queue-bound (scale-the-workers) signature, produced from real
   > contention the SimRunner cannot generate.
   >
   > **Per-node sensing (`byNode`).** The same cluster-aware read also surfaces each
   > node's share of the run plus its *live* backlog gauge (`activeInstances`,
   > `completionsTotal` from `/console/api/metrics`). On a deliberately skewed
   > partial-drain it reported node 0 `active=0 / completions=152`, node 1
   > `active=40 / 112`, node 2 `active=50 / 100` — i.e. the cross-node backlog
   > **imbalance** (the fairness signal) a single aggregate throughput number hides.
   > This is the distributed-sensing input distributed *scaling* will act on.
3. **Unify candidate evaluation across backends.** A candidate is evaluated by the
   SimRunner (fast offline what-if) or the ClusterRunner (at-scale realism); the
   ranking step (§7.2 step 5) is unchanged.

### 7.6 M3 in the three-regime / co-optimization frame

`docs/processos-deployment-cooptimization.md` reframes structure / binding /
resourcing as coupled layers of one process→deployment mapping, and adds an
orthogonal **observability/actuation axis**: (a) offline engine sim, (b)
queueing/DES model sim, (c) live act-and-measure. This *locates* the M3 work:

- The **SimRunner is regime (a)** (logic-faithful, isolated, no contention model).
- The **ClusterRunner measurement (slice 2a) is the regime-(c) foundation** —
  act-and-measure on a real cluster — **and** it produces exactly the
  distributions the **regime-(b)** queueing model is *fitted from*:
  throughput, the e2e tail, and the **per-job-type queue-vs-service split**
  (`ClusterRunSummary.byJobType`). That split is the discriminator the resource
  layer turns on: high queue / low service ⇒ *too few workers* (scale); low queue
  / high service ⇒ *slow worker* (bind/substitute).
- The **first regime-(b) increment now exists** (`harness/queueing.rs`): an
  `M/M/c` (Erlang-C) worker-pool model fitted from the measured per-job-type λ
  (`samples` over the run window) and S (`avgServiceMs`), answering the
  co-optimization doc's §4.2 question — *how many workers to hold p99 queue-wait
  < X?* It is exposed as `GET /api/harness/cluster?...&targetP99Ms=`, which
  attaches a per-job-type `staffing` recommendation. It is **advisory only** —
  it actuates nothing. *Verified live:* on a 144 jobs/s run (≈10 Erlangs offered)
  it recommended 11 / 12 / 14 / 18 workers to hold a 500 / 200 / 50 / 10 ms p99
  queue-wait — monotonic in the target, each prediction under it, ρ < 1
  throughout. This closes distributed *sensing* → a grounded distributed *scaling*
  **recommendation**.
- **Calibration — the measure → simulate wire (now exists)** (`harness/calibrate.rs`):
  the SimRunner drives the real `engine-core`, but the per-task service time and
  failure rate it charges came from each `WorkerModel`'s hand-authored numbers. To
  *speculatively execute* a hypothesis against a baseline that reflects reality,
  `calibrate_from_measured(scenario, &[MeasuredJobType])` overrides the **assigned**
  worker of each measured job type with the sample-weighted measured `avgServiceMs`
  and pooled `failures / samples` failure rate — preserving cost (no cost channel in
  the trace contract yet) and leaving **latent candidate** workers (the alternatives
  the LLM may swap in, for which there is no production data) at their modelled
  values. `calibrate_from_cluster` adapts a `ClusterRunSummary.byJobType` onto the
  generic input; `apply` returns a calibrated scenario ready for the ranker /
  hypothesis loop. Exposed as `POST /api/harness/calibrate` (caller supplies the
  measured distributions — no LLM or live cluster needed; returns the calibration
  plus the ranking over the grounded model), and as an optional `measured` field on
  `POST /api/harness/hypothesize` so the LLM reasons over — and every candidate is
  scored against — the bar production actually set. *Verified end-to-end:* feeding the
  bundled example a measured `classify` of 900 ms / 30 % failure moved the baseline
  from the modelled 2100 ms to a calibrated 1350 ms at 0.5 correctness, with
  `calibrated:[classify] uncalibrated:[summarize]` reported. This is the first of the
  three wires that close the loop on **real** data.
- **Production signal → hypothesis prompt — the second wire (now exists)**
  (`harness/hypothesize.rs`): `run_hypothesis` now takes the measured per-job-type
  signal and folds it into the LLM prompt as a *"Measured production signal (per job
  type, observed live — target these)"* block — observed service time and failure
  rate per task, over its sample count. The model therefore reasons about where the
  **real** cost and unreliability live, not only the synthetic baseline; paired with
  calibration (same `measured` payload), the baseline the proposals are *scored*
  against also reflects that reality. Wired through the optional `measured` field on
  `POST /api/harness/hypothesize`. Zero-sample rows carry no signal and are skipped.
  Still synthetic: the *inputs* replayed through the engine remain authored scenario
  inputs — the third wire, **T2 recorded-input replay** of *historical* production
  inputs. **Re-scoped (2026-06):** this does *not* require an engine-core change or a
  journal-reach as previously documented. `Event::ProcessInstanceCreated` already
  carries `variables` (the creation inputs), and those events already flow through the
  same exporter batches the Tier-A `TraceStore` consumes — the projection simply
  *discards* the field today (`server/src/console/trace.rs` folds the create arm with
  `..`). So T2 is a **server/console trace feature**, inside Nano's existing observe
  leg: retain creation variables (and, for faithful replay, the ordered external
  stimuli — job-completion outputs, message/timer vars — also already on the stream)
  on the trace and expose them on `/console/api/traces/{key}`. Must be size-capped +
  opt-in (`NANOBPMN_TRACE_VARIABLES`): payloads can be large (the engine has a
  `variables_spilled` spill path) and variables are a PII surface once shipped to an
  LLM. The same retained creation-variable snapshot also serves modeler **test-mode
  FEEL debugging** (snapshot at the incident + failing expression + error). The
  integration boundary was therefore a *projection gap*, not a contract gap.
- **Variables on the trace — Tier 1 (now exists)** (`server/src/console/trace.rs`):
  the projection now folds, when enabled, each instance's **creation inputs**
  (`ProcessInstanceCreated.variables`) and a **variable snapshot at every incident**
  (the running instance variables — seeded from creation, merged on `VariablesUpdated`
  — that a failing FEEL expression saw). Exposed on `GET /console/api/traces/{key}` as
  `creationVariables` and per-incident `variables`, rendered as natural JSON (via the
  shared `value_to_json`, not the engine `Value`'s tagged form). Opt-in
  (`NANOBPMN_TRACE_VARIABLES`, off by default) and size-capped per snapshot
  (`NANOBPMN_TRACE_VARIABLES_MAX_BYTES`, default 16384 — an oversized map is dropped,
  reporting only `truncated: true` + `bytes`, so the bounded in-memory ring can't be
  ballooned). This hands ProcessOS T2 its replay *inputs* and gives the modeler its
  FEEL-debug snapshot.
- **Recorded-input stimulus log — Tier 2 (now exists)** (`server/src/console/trace.rs`):
  the projection now also folds, when enabled, an **ordered per-instance log of the
  external stimuli the engine consumed** — each job/user-task completion output, message
  correlation payload, timer fire, and standalone set-variables delta, in observed order
  with its observation timestamp. A completion's output is attributed to it by the
  engine's emission order (`JobCompleted`/`MessageCorrelated` is immediately followed by
  its `VariablesUpdated`); the pending attribution is bounded to the element's lifetime
  (cleared on `ElementCompleted`) so a later unrelated delta can't be mis-attributed.
  Exposed on `GET /console/api/traces/{key}` as `stimuli` (`[{seq, at, kind, reference,
  variables}]`, `kind ∈ jobCompleted|userTaskCompleted|message|timer|variablesSet`).
  Replaying `creationVariables` then these deltas in order reproduces a historical
  instance's inputs against a candidate model — the missing piece for **real**
  recorded-input replay (not authored scenario inputs). Opt-in via
  `NANOBPMN_TRACE_STIMULI` (off by default; implies variable capture so the replay has
  both its creation inputs and deltas), per-snapshot byte-capped as Tier 1, and
  per-instance count-capped (`NANOBPMN_TRACE_STIMULI_MAX`, default 1024 — beyond it the
  log flags `stimuliTruncated` rather than growing without bound). Same footprint/PII
  posture: off by default, capped when on.
- **Prompt library — the LLM/prompt as an experimental variable (now exists)**
  (`harness/prompts.rs`): the experimental phase varies not only the candidate
  *models* but the *prompts and LLMs* used to generate them. The LLM was already a
  per-request knob (`LlmOverride`); the system prompt was the one hard-coded lever
  (`DEFAULT_SYSTEM_PROMPT`). It is now an authorable, selectable object. An in-memory
  `PromptLibrary` is seeded with the built-in default, optionally imported from
  `PROCESSOS_PROMPTS_DIR` at startup (`*.json` → full prompt, `*.md/*.txt/*.prompt`
  → `{id=name=stem, system=contents}`), and managed over a small CRUD surface:
  `GET /api/prompts`, `GET /api/prompts/{id}`, `POST /api/prompts` (author/import —
  the server controls the `builtin` flag), `DELETE /api/prompts/{id}` (refuses
  built-ins). `POST /api/harness/hypothesize` selects a prompt by precedence
  **inline `prompt` > `promptId` (library lookup) > built-in default**. The same
  verifier discipline still applies: whatever prompt produced a candidate, the
  SimRunner scores and ranks its output, so a worse prompt is exposed by the numbers,
  not trusted. (UI is deferred — this slice is API-first.)
- **Still deferred (not M3):** the fuller regime-(b) **discrete-event** simulator
  (multi-job-type consolidation what-ifs, cross-process contention — beyond the
  single-pool Erlang-C estimate); the **third (scaling/fleet) control verb** that
  would *apply* a recommendation (actuate-opt-in, its own public-API design); and
  **portfolio scope** (cross-process resource attribution in ingest + an aggregate
  objective). The remaining two closed-loop wires also stay ahead: **T2 recorded-input
  replay** (replay *historical* production inputs, not authored scenario inputs) and
  **production-Insights → prompt** (anchor hypothesis generation on the live baseline
  rather than the synthetic one). M3 stays *measure → rank → suggest* on the
  per-process loop; calibration and an Erlang-C *suggestion* fit squarely in
  "suggest" without crossing into actuation.

### 7.7 Evaluation fidelity & the mocking infra (design rationale)

A BPMN model is a **coordination skeleton**; its semantics live in the workers (FEEL +
external job logic). So a candidate model cannot be evaluated in isolation from the
business logic that materializes it — to evaluate a *re-decomposition* you need a model
of what the new pieces *do*. The tractable framing is a **fidelity ladder**, with the
rung chosen per candidate by how far it diverges from the baseline:

1. **Distributional** (what `SimRunner` does today): each job type carries a calibrated
   service-time + failure distribution; the real engine runs, jobs are served
   statistically. Evaluates timing / cost / structural changes (parallelize, batch,
   reorder, retries, priority). Needs distributions, not per-instance inputs. **No mock
   infra.**
2. **Recorded-input replay** (Tier-2 stimulus log): replay creation inputs + recorded
   job/message outputs. Higher fidelity — routing uses *real* variable values and the
   end-state is checkable. **Works only for input-compatible candidates**: every job the
   candidate issues has a recorded output *of that job type* (hence the stimulus
   `reference` is the job type, not the element position) and routing stays on
   historically-observed paths.
3. **Generative mock**: for candidates that re-draw task boundaries / add job types
   history never exercised, the same LLM lever generates a *mock* `(job_type,
   input_vars) → (output_vars, service_time)`, not just a candidate model.

Key principles that make Level 3 safe and cheap enough to be useful:

- **Reconstruct-history gate.** A generated mock must first reproduce the *original*
  model's historical outputs/latency within tolerance (original model + baseline mock vs
  the recorded inputs). A mock that can't reproduce the past hasn't earned the right to
  score a counterfactual. Its residual is the **noise floor** — candidate differences
  below it aren't real. (This is backtesting; history is the holdout.)
- **Boundary conservation.** However the middle is re-decomposed, the instance-level
  `(creation inputs) → (terminal outputs)` mapping should be preserved for replayed
  historical cases. The stimulus log is thus also the **acceptance test** for any
  higher-level mock: free internally, falsifiable at the process boundary.
- **Rank, don't truth.** Phase 4 (production cohort) is the real arbiter; the simulator
  only has to *shortlist* (pick the 2–3 of N worth a real cohort). That drops the
  fidelity bar enormously and is continuously checkable — every cohort yields ground
  truth, so *sim rank-correlation with production* can be measured and the mocks
  recalibrated. The capability **bootstraps**: early experiments are Level-1-only
  (conservative, high trust); as the mock library earns validation the residual shrinks
  and more divergent candidates unlock. It ratchets — it need not be complete on day one.
- **The customer app stays decoupled.** What's needed is a *contract-level* model of the
  workers, not the live app. The real app is the offline calibration/validation source
  (from the recorded traces) and the Phase-4 runtime (real anyway); evaluation runs
  against the mock. The **plastic layer is the mock**; the engine stays real; Nano stays
  read-only. The deformable "integrated system" = real engine + generated/calibrated
  mock.

Two costs to **surface, not hide**: (a) a candidate that introduces new job types needs
new workers to deploy — flag *requires new workers: [job types]* per candidate (note the
alignment: candidates cheapest to *evaluate* (replay) are cheapest to *deploy* (existing
workers); divergence costs on both ends). (b) Evaluation must never collapse tiers into
one number — **fidelity tier + confidence are first-class outputs** (a Level-1 result is
"measured ±replay-residual"; a Level-3 result is "simulated, mock-validated ±X").

### 7.8 Console UX — the four phases as one experiment's lifecycle

The four phases (production analysis → hypothesis generation → stress-test & ranking →
production cohort + evaluation) are a **pipeline**, not four destinations; making them
top-level nav would feel disjoint. Navigation is **Console → Process → Experiment**, with
a global left rail of **Console · Processes · Prompts · Experiments** (Prompts and
Experiments are global because both are reused across processes — Prompts is the
experimental-variable library).

- **Console**: fleet stats + deployed processes with per-process stats (throughput,
  p50/p99 e2e, cost proxy, incident rate, # running experiments) — Phase 1 at fleet level.
- **Process page**: Phase-1 scoped to one process (calibrated baseline + backing data
  window) on top; its experiments list + *New experiment* below.
- **Experiment page** = the spine: a horizontal **4-step stepper** mirroring observe →
  hypothesize → test → confirm: (1) **Baseline/Analysis** — the calibrated snapshot +
  recorded-input dataset it forked from; (2) **Hypotheses** — pick/author prompt + choose
  LLM → generate candidates (validation status); (3) **Stress-test & rank** — select
  candidates → evaluate → ranked rows showing **fidelity tier, metrics, confidence band,
  reconstruct-history pass/fail, "requires new workers"**; (4) **Production cohort** —
  promote a shortlist, measure live e2e + cost, and show the **predicted-vs-measured
  delta** as the hero number (the trust signal for the whole apparatus). A stepper is
  linearly navigable, one CTA per step, and teaches the mental model by walking it; the
  global **Experiments** list serves the "what's running everywhere" need.


### 7.9 The hypothesis loop as a Nano process (agentic search with the engine as verifier)

The hypothesis→evaluate loop (§7.2 steps 2–5) is best run as **an agentic loop with a fast,
truthful verifier**, the same reason coding agents converge: an LLM with a compiler/test in
the loop beats one-shot reasoning. The verifier already exists in three parts — the embedded
engine (`SimRunner`), recorded-input replay (the Tier-2 stimulus log, switchable via
`c8 nano --capture`), and the golden set. Orchestrate that loop **as a Nano BPMN process**:
LLM-proposal and engine-eval are tasks; the engine is the verifier the LLM iterates against.

- **Process owns the body; LLM owns the judgment.** BPMN is the right skeleton for the
  *durable orchestration* — bounded iteration counter, wall-clock budget (boundary timer),
  cost ceiling, candidate persistence, parallel fan-out, audit. It is the wrong place for the
  *reasoning*. The LLM's decision (iterate / file-as-candidate / give-up) returns as a
  **variable**; a gateway routes on it. Keep reasoning out of the gateways.
- **Gradient, not pass/fail.** "Intersection/disjoint with golden" is a linker error; agents
  need compiler errors. The eval worker returns: **validity** (schema/FEEL/soundness, with
  location), **per-instance replay divergence** (which golden instances failed to reconstruct
  and *how* — which output var, expected vs got), **coverage** (fraction input-compatible;
  unreplayable candidates that need new workers are *flagged*, not penalized — §7.7), and
  **objective metrics** on the replayable subset. That converts random search into
  hill-climbing with a slope.
- **Eval = a pure, reusable capability; then orchestrate it.** Make `validate + replay + score`
  idempotent with a stable `(candidate-hash, input-set-hash)` cache key (agents re-propose
  near-duplicates). Expose it as a Nano job worker *and* keep it directly callable. The BPMN
  loop buys durability, fan-out, and budget enforcement for free; the pure capability keeps it
  testable. This is the same eval engine §7.7's fidelity ladder runs on.
- **Population over single chain.** A lone "iterate until exhausted" trajectory is where LLM
  reasoning gets stuck. BPMN multi-instance lets you **fan out N candidates, evaluate all, keep
  top-k, then ask the LLM to mutate/cross survivors** — evolutionary search with the engine as
  fitness function. The model expresses this naturally and is far more robust.
- **Guardrails.** (a) A cheap **deterministic pre-pass** (schema/FEEL lint) catches
  compiler-class errors before spending an LLM turn or a replay — reserve iterations for
  *semantic* divergence. (b) **Train/holdout split** of the golden set + rank-not-truth (§7.7)
  to resist overfitting / reward-hacking the recorded outputs. (c) **Exhaustion is measured**:
  stop on no improvement beyond the reconstruct-history noise floor for *k* iterations — never
  trust the LLM's self-assessed "out of ideas". (d) Don't hand the LLM the whole golden set
  (cost, PII, overfit) — aggregate divergence + a few representative failing instances; the
  stimulus log already supports sampling.
- **It dogfoods.** Once the optimizer loop is itself a Nano process emitting traces, ProcessOS's
  first customer is *itself*: it can optimize its own search (which prompt / LLM / fan-out shape
  / budget converges fastest and cheapest). The recursive ratchet — and the exit-reason
  distribution (converged / budget / exhausted / invalid) feeds the prompt-experiment loop.

Alternative framing considered: **LLM-as-agent calling the engine as a tool** (function-calling/
MCP) instead of **process-as-agent calling the LLM as a task**. Not exclusive — same pure eval
capability underneath. Choose the BPMN-orchestrated form as the backbone (durability, fan-out,
budget, audit, dogfooding come free); reach for tool-calling only for fast interactive
exploration. The invariant from §8 holds: **the verifier is authoritative; the LLM only
proposes** — nothing becomes a candidate without passing `validate`/replay.

## 8. Invariants ProcessOS must honour

- **One-way dependency, build-enforced.** Nano never imports ProcessOS; ProcessOS
  only touches Nano's three public contracts.
- **No privileged access.** Deploy and routing go through the same public API a
  client uses; reads go through the export endpoints. No reaching into journal/state.
- **The verifier and statistics are authoritative; the LLM only proposes.** Nothing
  reaches a canary without passing `verify`; nothing is promoted without the
  sequential-test guardrail clearing.
- **Determinism preserved.** `sim`/`verify` reuse `engine-core` with injected time;
  no hidden I/O enters a replayed run.
- **Absent-safe.** Removing ProcessOS leaves a fully functional production cluster.

## 9. Open questions

1. **Routing-table endpoint shape** — predicate language for cohorts (tenant /
   business-key hash / %): how expressive before it becomes policy-in-the-engine?
   Lean minimal (weights + a small fixed predicate set).
2. **Ingest transport** — OTLP push vs an SSE/WS tail of the console trace feed.
   OTLP is the standards-friendly choice and decouples volume from the console.
3. **Trace volume/privacy at scale** — sampling + aggregate-by-default + exemplar
   retention live in ProcessOS's `store`; Tier-B bundles need TTL/redaction here,
   not in the engine.
4. **Shadow execution prerequisites** — idempotency-key / dry-run worker contract is
   a *Nano public-API* design (§1.3), gating real-traffic mirroring.
5. **Multi-cluster** — one ProcessOS per cluster vs a fleet view; the contracts are
   per-gateway, so federation is a ProcessOS concern, not Nano's.

## 10. The cybernetic framing — the craft, the pilot, and the droid

"Self-optimizing" usually means an autonomous loop with a human babysitter. The truer
framing is an **X-wing**: a craft flown by a pilot and a droid — cybernetic in the
original sense (*kubernetes* = steersman; feedback-governed steering with the human
*inside* the loop). The observe → hypothesize → verify → deploy machinery of §7 is not
the system; it is the **airframe and instruments**. The system is *human + droid (LLM) +
craft (ProcessOS over Nano)*, coupled into one joint cognitive system. The loop is
human-**steered** self-optimization, not autonomy.

The deepest consequence of **"the first target is always itself"**: operation and
improvement become the *same activity*. You do not learn to fly the craft and then
optimize it — learning to fly it *is* tuning it. Dogfooding stops being a test strategy
and becomes **pedagogy**; the pair's first and continuous subject is its own flight.

**10.1 Empty cockpit, empty droid slot — individuation.** Factory-fresh, every instance
is generic; it individuates through use across **three plastic surfaces**: (a) the **BPM
model that drives ProcessOS itself** (the *pilot process* — the choreography); (b) the
**system prompts** (the droid's skills — *already exists* as the prompt library, §7.6);
(c) **accrued experience** (calibration + trace memory — the logbook). The operator
steers by editing (a) and (b); (c) accrues. Two instances diverge the way two people's
toolchains diverge, and that divergence *is* the value. Honest cost: lock-in / bus-factor
(coevolved instances are idiosyncratic, non-portable); mitigation is that these surfaces
are **explicit, versioned, inspectable artifacts** — the tuning lives in prompts and
BPMN, not in someone's head.

This forces an architectural commitment (previously filed as future work): the §7.9
orchestration must be authored as a **deployable BPMN process living in the instance** —
observable in the same console, optimizable by the same loop — *not hardcoded in Rust*.
The pilot process must be forkable, because it is the first thing the operator reshapes.

**10.2 The user task — conversation, not dispatch.** The §7.9 loop as first framed was
autonomous. Putting a **user task in the pilot process** changes the interaction model
from a *job queue* to a *cockpit*: the human is a participant in the loop, not its
dispatcher. User tasks are the human's turns; LLM tasks are the droid's turns; engine
tasks are the craft responding. **The BPM model literally is the choreography of the
human–droid pairing.** The most generative corollary: **the placement of the user tasks
is the delegation dial.** Early/learning-phase models have many human gates; as the pair
grows fluent and trust accrues, the operator moves user tasks later or removes them,
ceding more to the droid. The model therefore *encodes the current trust boundary*
between human and droid, and reshaping it *is* the coevolution — "learning to fly it with
a droid copilot" is the operator editing where the user tasks sit.

This reshapes §7.8: the 4-step stepper is the right *structure*, but each step becomes a
**conversation surface with a co-present droid**, not a form you submit. The shared
instrument both pilot and droid read is the verifier's **gradient** (§7.7/§7.9) — which
is *why* legible, per-key feedback matters: a copilot you cannot interrogate is not a
copilot.

**10.3 The tension to hold.** Cybernetics and aviation both warn of the **out-of-the-loop
problem**: a droid that does too much atrophies pilot skill and erodes the human's ability
to still judge "better." The user-task dial is the defence, and "first target is itself"
is partly a *training regime* — you keep hands on the craft by continuously tuning it.
Relatedly, **"better" is the pilot's to define**: the objective is a plastic *input* the
human keeps redefining, not a constant — "rank not truth" (§7.7) taken to its conclusion,
where beyond even the Phase-4 production measurement the human is the final arbiter of
rank.

**Architectural commitments that fall out.** (1) Author the §7.9 pilot process as
first-class editable BPMN deployed on Nano (with ≥1 user task), not Rust. (2) Evolve the
§7.8 console into a **cockpit**: a persistent droid-conversation pane alongside the
stepper; user tasks surface as the human's turns. (3) **Per-instance persistence and
individuation** of the three plastic surfaces (cockpit prefs + objective definition;
droid slot = selected/authored prompts + model; experience = calibration) — the prompt
library is the first such surface; the pilot process and objective are next. (4) Give
ProcessOS its **own** engine for the pilot loop (§1.4): the client's Nano is the
read-only target, so the self-optimizing loop, its meta-workers, and the forkable pilot
process live on an engine ProcessOS supervises (spawns on boot, deploys the pilot into,
reaps on shutdown) — never the client's. An instance ships self-hosting: empty cockpit,
empty droid slot, *and its own craft to fly*.

---

*This document proposes where the optimizer lives and the seams it binds to. It
changes no engine code; the only Nano-side addition the loop requires is the
policy-free routing primitive of `process-optimization-design.md` §7. First concrete
step: scaffold the `processos/` crate at Stage T1 — ingest the existing trace/metrics
contracts and serve an Insights report.*
