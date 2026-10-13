# ProcessOS

The runtime **process-optimization plane** for Nano BPM — a separate component, on
the principle that **Nano handles production, ProcessOS handles optimization.**

It runs as its own process with its own webserver and talks to a Nano cluster *only*
over Nano's public HTTP contracts. The dependency is one-way: ProcessOS reads Nano;
**Nano never depends on ProcessOS** and runs unaffected when this binary is absent.

See [`../docs/processos-index.md`](../docs/processos-index.md) for the design-series
reading guide, [`../docs/processos-design.md`](../docs/processos-design.md) for where
the optimizer lives and its contracts, and
[`../docs/process-optimization-design.md`](../docs/process-optimization-design.md) for
the optimization loop it implements.

## Status

The optimization rig and the self-hosting **cockpit** are built and green
(`make processos-test`). The loop is **measure → calibrate → hypothesize → rank →
replay**, and it is **suggest-only**: nothing is actuated automatically — the engine
stays untouched and a human/verifier decides.

Milestones (design §7.4): **M0** (`SimRunner`), **M1** (baked candidate ranking) and
**M2** (LLM hypothesis) are done; **M3** (real-cluster realism + the live production
path) has its measurement/queueing/calibration substrate built. The §10 "cockpit"
build adds the human↔droid framing: an editable, **forkable pilot process**, a
persisted cockpit conversation, a prompt library, and ProcessOS's **own supervised
Nano engine** for the pilot loop.

Deliberately deferred (not gaps — see §7.6): a full discrete-event simulator, the
**actuating** scaling/fleet control verb, portfolio (cross-process) scope.

## Architecture — two engines

ProcessOS distinguishes the two Nano engines it talks to (design §1.4):

- **target** — the client's **production** engine. **Read-only**: ProcessOS folds its
  exported traces + metrics into Insights and a measured baseline. It never writes here.
- **own** — an engine ProcessOS **supervises** for its *pilot loop*: it can spawn the
  gateway as a child process, learn its port, deploy the pilot BPMN on boot, and reap
  it on shutdown. The self-optimizing loop and its meta-workers run here, never on the
  client's engine.

With no env set both roles resolve to the same local gateway, so a single-machine dev
setup "just works".

## The optimization harness (design §7)

The harness embeds the **real `engine-core`** (a one-way `path` dependency) and drives
it with a virtual clock to evaluate process variants. A **scenario** supplies a test
model, seeded **mock workers** (cost / latency / failure models), **latent
worker-swap options** (the MVP transform space), and **inputs carrying expected
outputs**. The `SimRunner` runs every candidate over every input and ranks them across
cost / latency / incident-rate / correctness, reporting whether exploration recovered
the optional **golden** variant. It is deterministic: same scenario + seed ⇒ same
ranking.

Beyond the synthetic baseline, the harness grounds itself on **real** data:

- **ClusterRunner / cluster read** (`harness/cluster.rs`) — cluster-aware
  measurement: discovers every node via `GET /v2/topology` and **unions** their
  partition-local traces, reporting throughput, the e2e tail (p50/p95/p99), the
  per-job-type **queue-vs-service split**, and per-node backlog (`byNode`).
- **Queueing recommender** (`harness/queueing.rs`) — an `M/M/c` (Erlang-C) worker-pool
  model fitted from the measured λ/S, answering *how many workers to hold p99
  queue-wait < X?* Advisory only.
- **Calibration** (`harness/calibrate.rs`) — overrides the SimRunner's hand-authored
  per-task service time / failure rate with **measured** distributions, so candidates
  are scored against the bar production actually set.
- **Recorded-input replay** (`harness/replay.rs`, `replay_rank.rs`, `evolve.rs`) — the
  Level-2 verifier: replay a historical instance's **real** creation inputs + ordered
  stimuli (captured opt-in on Nano's trace, see below) against a candidate model and
  rank by fidelity gradient.

### LLM hypothesis (M2)

Instead of (or alongside) the baked worker-swap grid, an LLM can *propose* candidates.
The model only generates hypotheses — the harness still measures and ranks them with
the SimRunner, so an improvement that doesn't actually help is exposed by the numbers.
The model is reached through a **pluggable client**, so the same path serves a local
model on your network and a hosted API:

- `openai` — the OpenAI **chat-completions** shape, spoken by local `llama.cpp`
  (`--api`), Ollama (`/v1`), vLLM, LM Studio, and OpenAI. **Default.**
- `anthropic` — the Anthropic **messages** API.

The **system prompt** is itself an experimental variable: an authorable, selectable
`PromptLibrary` (`harness/prompts.rs`), seeded with the built-in default and optionally
imported from `PROCESSOS_PROMPTS_DIR`. `POST /api/harness/hypothesize` selects by
precedence **inline `prompt` > `promptId` > built-in default**.

| Env | Default | Meaning |
|-----|---------|---------|
| `PROCESSOS_LLM_PROVIDER` | `openai` | `openai` (local/llama.cpp/ollama/vllm) or `anthropic` |
| `PROCESSOS_LLM_BASE_URL` | `http://127.0.0.1:8080/v1` (openai) / `https://api.anthropic.com` (anthropic) | Endpoint base |
| `PROCESSOS_LLM_MODEL` | _(required)_ | Model name |
| `PROCESSOS_LLM_API_KEY` | _(none)_ | Bearer / `x-api-key`; local models usually need none |
| `PROCESSOS_LLM_MAX_TOKENS` | `2048` | Completion budget |
| `PROCESSOS_LLM_TEMPERATURE` | `0.2` | Sampling temperature |

These are *defaults*; the console **Settings** panel (the gear in the lower-left of the
console and cockpit) manages one or more named **LLM profiles** — switch the *active*
profile, add/delete profiles, and **Fetch** an endpoint's model list to pick a model id
(the fetch also reads each model's **context window** — llama.cpp's `meta.n_ctx`,
`n_ctx_train`, or a `context_length`-style field — and fills the Max-tokens budget from it).
It ships with a `Local (llama.cpp)` profile pointing at `http://localhost:8888/v1`.
Profiles + the active selection + the global Python interpreter persist to
`${XDG_CONFIG_HOME:-~/.config}/processos/settings.json` (overridable with
`PROCESSOS_CONFIG_DIR`; written `0600` as it may hold an API key). The effective config
layers **built-in default → `PROCESSOS_LLM_*` env → active profile → per-request
`llm` override**, each winning when present — so the console is authoritative over the
environment without a relaunch. The panel also configures `PROCESSOS_PYTHON`
(the `run_python` interpreter). See the [settings endpoints](#endpoints).

### Local model sidecar — run llama.cpp from ProcessOS (`src/llama.rs`)

ProcessOS can supervise a local **`llama-server`** (llama.cpp) so the operator never has to
launch one in a separate terminal. Open the **LLM config** panel (the ⚙ cog, or **Configure
LLM profiles** / **Edit profile** from the `LLM sidecars` view). Each profile is one of two
types, chosen with a radio: **Managed sidecar** or **External**.

For a **Managed sidecar** profile, give it a **Model file / HF spec** — either a Hugging Face
`repo[:quant]` spec (e.g. `unsloth/Qwen3-4B-GGUF:UD-Q4_K_XL`, downloaded on demand) or a
`.gguf` path resolved against the **Models directory** — a **Port** (each sidecar needs its
own so two can run at once), and optional **Startup args** (e.g. `-ngl 99 -c 32768 --jinja`,
appended verbatim). An **External** profile instead shows **Provider**, **Base URL**,
**Model** (with a **Fetch** button) and **API key**. **Max tokens**/**Temperature**, a
**Thinking level**, and the editable **Name** apply to both, and the **Save profile** button
sits at the bottom of the form. The **Thinking level** caps reasoning tokens as a fraction of
**Max tokens** — **Fast** 20%, **Medium** 50%, **Max** 100%, or **Default** (unconstrained) —
sent to the endpoint as both `thinking_budget_tokens` and `reasoning_budget` (the latter is
current llama.cpp's per-request `--reasoning-budget` knob; endpoints that recognise neither
ignore the extra fields). The level applies to **both** the investigative primary and any
pairing/monitor partner. The global **Local model server** section sets the shared **Models
directory** (pre-filled with llama.cpp's own cache — `$LLAMA_CACHE`, else macOS
`~/Library/Caches/llama.cpp` / Linux `~/.cache/llama.cpp` — so models are shared with any
separately-run llama.cpp) and an optional **llama-server binary** path (otherwise found on
`PATH`).

**Start sidecar / Stop** spawn and kill the process; sidecars are also stopped on graceful
shutdown. **ProcessOS manages the port automatically** — it assigns a free local port when you
start a sidecar (reusing the previous one when it is still free) and writes it into the profile's
Base URL (`http://127.0.0.1:<port>/v1`), so the profile both *launches* and *talks to* the same
endpoint with nothing to configure. **Logs** opens a streaming viewer that tails the process
output and shows the **equivalent terminal command** (including `LLAMA_CACHE=…`) so you can
run it yourself instead. Fresh installs ship ten sidecar profiles covering a size spread for
two model families — **Gemma 4** (E2B ~4 GB, E4B ~8 GB, 12B ~16 GB, 26B-A4B ~48 GB, 31B ~64 GB)
and **Qwen** (Qwen3 4B ~8 GB, 8B ~16 GB, 32B ~48 GB, **Coder 30B-A3B** ~24 GB, and Qwen 3.6
35B-A3B ~64 GB) — so you can match the model to the machine. The tiny **E2B** suits a cheap loop
**monitor**, and the **Coder** variant helps with BPMN/XML authoring.

**Up to two sidecars run at once.** An investigation can drive a **primary** model plus, optionally,
one **partner** — a sparring partner (Pair AI) or a loop **monitor** — and both can be local. The
supervisor runs at most **two** `llama-server` children, each auto-assigned a distinct free port;
starting a third or re-starting one already up is rejected with a clear message. See the
[llama endpoints](#endpoints).

**MTP — multi-token prediction (`src/gguf.rs`).** Some model families (GLM-4.5+, DeepSeek-V3+,
Qwen3.6+) bake **NextN prediction heads** into the weights, which llama.cpp's beta `--mtp` flag
uses for *self-speculative decoding* — the model drafts several tokens per step and verifies them
itself, often a >1.5× decode speed-up with no second model. ProcessOS **scans the GGUF header**
(`GET /api/llama/scan/{profileId}` — the `{arch}.nextn_predict_layers` metadata key and `nextn`
tensor names; only the header is read, never the weights) and, when the heads are really present,
the model's tile on the **LLMs** page grows an **MTP** toggle. The toggle persists on the profile
(`mtp: true` ⇒ launch with `--mtp`); the start endpoint **re-verifies capability at launch and
refuses** an MTP config whose GGUF carries no MTP heads (e.g. a conversion that stripped them),
so an invalid config cannot start. A running MTP sidecar shows an **mtp** pill.

**Speculator pairings — a fourth pairing mode.** Alongside review/monitor/delegate, a pairing can
be a **speculator**: the sparring-partner slot becomes a **speculative-decoding draft model**. The
pairing's second profile is not started as its own sidecar and writes no prose — when the primary
launches, its GGUF is loaded into the *primary's* `llama-server` via `--model-draft` (with optional
`--draft-max` / `--draft-min` tuning stored on the pairing), where it proposes tokens the primary
verifies. Picking the **same model** for both sides ("loaded twice") is explicitly supported —
self-drafting is a cheap accept-rate win for MoE models. **Invalid configs are disallowed twice**:
saving a speculator pairing validates that the draft is a local sidecar GGUF (and, when both models
are already on disk, that the pair is compatible), and every start re-checks from the GGUF headers —
same tokenizer family and BOS/EOS, vocab sizes within llama.cpp's speculative tolerance (±128) —
refusing the launch otherwise. In the composer, selecting a speculator pairing starts (or offers to
restart) the primary with the draft attached; the roster shows a **⚡ speculator** chip and the LLMs
view a **⚡ speculating** pill while active.

**Drafter pairings — a fifth pairing mode.** Alongside review/monitor/delegate/speculate, a pairing
can be a **drafter**: a small local sidecar paired to **write model IR under grammar constraint**.
It is the *app-level* sibling of the speculator — the drafter contributes no prose and runs no tool
loop. Selecting a drafter pairing in the composer offers the primary a **`draft_ir`** tool: the
primary hands it a plain-language modelling instruction (and, optionally, existing IR to revise via
`base`), and the drafter fires **one grammar-constrained completion** (the checked-in
[`assets/ir.gbnf`](assets/ir.gbnf), served at `GET /api/ir/grammar.gbnf`, loaded as the request's
`grammar` field at temperature 0) that returns well-formed IR. The result is parse-checked before it
comes back, then the primary reviews it and deploys with `write_model_ir`. The point: **the grammar
is a leveller** — a tiny model with GBNF emits valid IR as reliably as a large one, so a small local
drafter is a credible partner to a larger local planner. To try it: create a pairing with mode
**Drafter**, pick a small local model as the IR writer (leave the prompt blank to use the built-in
IR-writer persona), select it in the composer's **Pair AI** picker, then ask the primary to build or
restructure a process — it will call `draft_ir` and propose the drafted model. The roster shows a
**drafter** chip while selected.

**Missing-tool preflight.** ProcessOS shells out to two optional CLI tools it does **not** bundle:
**`llama-server`** (llama.cpp — the local LLM sidecars above) and **Node** (which the supervised
Nano engine uses to run its embedded job workers). On startup the Console checks both via
`GET /api/system/dependencies` and, if either is missing, shows a dismissible banner naming the
tool, what it's for, and a link to the official OS-aware install instructions — so you get a clear
heads-up instead of a cryptic spawn failure later. Trying to start a sidecar when `llama-server`
is absent also reports the same install link inline.

**Wrap-up / loop monitor and reasoning control.** When the loop monitor decides a turn is
spinning (or the operator hits **wrap it up**), ProcessOS ends the model's *current reasoning
block mid-generation* so it stops circling and produces its answer now, rather than only stopping
at the next round boundary. This rides llama.cpp's reasoning-control surface
([PR #23971](https://github.com/ggml-org/llama.cpp/pull/23971), present in build ≥ b9780): the
agent arms each turn by sending `reasoning_control: true` on the completion request, then issues
`POST /v1/chat/completions/control` `{ "id": "<chatcmpl-id>", "action": "reasoning_end" }` keyed on
the in-flight completion id. When the endpoint is absent (older build) or the turn has no
completion id yet, ProcessOS falls back to the per-session cancel flag (round-boundary stop) with
no change in behaviour. `GET /api/llama/reasoning-control` probes whether the active (or a named)
profile's endpoint supports it.

**First-class `LLM sidecars` nav surface.** The left rail — the principal navigation menu shown
on every page after the landing page (cockpit and workspaces share it) — carries a first-class
**LLM sidecars** entry with a live **status dot**: **green** when *any* sidecar is up, **yellow**
when off. The entry opens a dedicated view that lists every sidecar-capable profile, shows each
**running** sidecar with its own **Stop / Logs** (plus **Stop all**), warns when two profiles
**share a port**, and offers **Start / Make active** inline (the same controls as the Settings
cog, promoted to a top-level surface). Other pages deep-link into a named cockpit view via
`/cockpit?view=sidecars` (also `console`, `processes`, `prompts`, `experiments`).

**Active-model roster + partner exclusion.** Beside the investigation's **Send** button a roster
shows which models are active — the **primary** plus any enabled **pair**/**monitor** partner —
each with a dot (green = its sidecar is running, yellow = configured-but-off, blue = remote). A
model can hold only one role: the partner pickers exclude the primary (and each other), and the
primary send-menu **disables** whatever profile is acting as the partner, so you can't send the
investigation message to the model that's meant to be the sparring partner.

**Just-in-time start from chat.** If the primary chat profile **or an enabled partner** is a
sidecar that isn't up yet, sending a cockpit message prompts **"Sidecar(s) not started. Start
them now?"** — *Yes* starts the missing ones, waits for each model to finish loading (polling
`GET /api/llama/ready?profileId=…`, which probes `llama-server`'s `/health`), then sends; *No*
leaves the message in the composer unsent.


## Workspaces — bounded contexts & processes (multi-tenant + datasets)

A consultant manages many engagements, so ProcessOS roots a **workspace tree** of
`workspaces / processes` on disk (`src/workspace.rs`). A **workspace** is a bounded
context for one customer/deployment. Each process **binds** to a trace source —
either a **live** Nano instance (`targetUrl`) or a **loaded dataset** (a folder of
instance-trace JSON, or a sibling `traces/` folder) — and the Insights report is
folded over whichever source is bound (`src/dataset.rs`, `report::build_over`).
Folders are scanned from disk, so a consultant can curate a workspace folder
structure by hand and it shows up; the CRUD API also creates them. The front door
is `/workspace`: browse workspaces → open one → open a process to see its **rendered
BPMN model** (vendored bpmn-js viewer, served from `/assets/bpmn/`) and Insights,
then **Reason in cockpit →** to investigate the bound trace with the droid.

```
<PROCESSOS_WORKSPACES_DIR>/
  <workspace-slug>/
    workspace.json
    <process-slug>/
      process.json          # { displayName, targetUrl? | dataset?, objective?, notes? }
      model.bpmn            # optional process model (rendered in the browser)
      traces/               # optional dataset folder (else `dataset` points elsewhere)
```

A process is `live` when `targetUrl` is set, `dataset` when a `dataset` path or a
`traces/` folder resolves, else `unbound`. Slugs are single, lowercased
`[a-z0-9-_]` path segments (no traversal). Browse it at `/workspace`.

**Load the demo dataset in one click:** `POST /api/workspaces/seed-demo` (or the
"Load demo dataset" button) creates the **Northwind Bank** workspace + **Loan
Approval** process, generates a labelled trace corpus into its `traces/` folder, and
writes `model.bpmn` — ready to view and investigate.

A dataset folder holds per-instance `*.json` files (each a
`GET /console/api/traces/{key}` shape) at top level or under `instances/`/`traces/`,
and/or a single `traces.json` array, plus an optional `metrics.json`.

## Synthetic trace corpus — labelled benchmark for domain inference (`src/corpus.rs`)

To exercise the distributed-sensing goal *without a running server*, ProcessOS can
**synthesise a labelled trace corpus** from a "before" archetype process plus a thin
infra sidecar. It runs the **real** `engine-core` per instance for the logical path
(branching, errors) and overlays timing analytically: lognormal service times + an
M/M/c queue-wait sampled against a **time-varying arrival schedule**. The result is a
`traces.json` array — byte-compatible with a real Nano capture, so it loads straight
into a workspace `DatasetSource` — carrying a **planted pathology** (e.g. an
under-provisioned job whose queue tail spikes weekday 09–12) and a ground-truth label.

A `pack.json` describes the scenario; the bundled
`corpus-packs/loan-approval/` plants a 2-worker `credit-check` bureau pool overloaded
by an 8× weekday-morning arrival spike:

```bash
# generate ~10k instances of traffic over a 14-day horizon
processos gen corpus-packs/loan-approval/pack.json /tmp/loan-corpus
#  -> traces.json (load via a workspace dataset) + metrics.json + expected.json

# run the inference + score it against the planted label
processos infer /tmp/loan-corpus 500   # 500ms p99-wait staffing target
#  -> bottleneckJob credit-check, window weekday-morning, recommendedWorkers 44
#  -> SCORE {"domainOk":true,"jobOk":true,"windowOk":true,"points":3,"maxPoints":3}
```

The inference time-buckets per-`(job, window)` queue means, localises the tail to the
job + temporal window with the largest peak-vs-off-peak inflation, fits the peak arrival
rate to an Erlang-C staffing recommendation, and classifies the domain — then `score`
grades that against `expected.json` (domain + job + window = 3 points). The generator is
the symmetric write-side of `replay.rs`, so a corpus also feeds `replay-rank`/`evolve`.

### Multi-stage orchestrators — call activities (`corpus-packs/cdd-refresh/`)

A pack's `bpmn` may be a **call-activity orchestrator** that invokes other process
definitions; list the BPMN files (or directories) holding the called processes in
`bpmnLibrary`. At load time every `<process>` across the main BPMN and the library forms
a resolution set, and each `callActivity` (Camunda 7 `calledElement` or Camunda 8
`zeebe:calledElement processId`) is **inline-expanded** into an embedded sub-process — a
recursive, id-prefixed copy of the callee — so the real engine walks the whole multi-stage
flow on the existing sub-process machinery (no native call-activity executor).

`corpus-packs/cdd-refresh/` is a corporate **CDD (AML/KYC) refresh**, composed from the
`process-os` `cdd-refresh` example: an orchestrator wiring eight deployable phase processes
(intake → document request → reminders → screening → sanctions gate → risk assessment →
approval → closure) in `phases/`. The planted pathology is a 2-worker
`io.camunda.agenticai:aiagent:1` adverse-media screening agent overloaded by an 8×
weekday-morning refresh-batch spike:

```bash
processos gen corpus-packs/cdd-refresh/pack.json /tmp/cdd-corpus
processos infer /tmp/cdd-corpus 500
#  -> domain kyc-cdd-refresh, bottleneckJob io.camunda.agenticai:aiagent:1,
#     window weekday-morning  ->  SCORE points 3 / 3
```

Phase 4 (document verification) is omitted: it routes on non-standard FEEL list functions
(`any(documents, function(d) …)`) the engine does not evaluate. Parser support added
alongside the call activities — `intermediateThrowEvent`/`receiveTask` (pass-through),
`businessRuleTask`/`scriptTask` (modelled as jobs; a `calledDecision` decisionId becomes the
job type), and a multiple-start-event collapse — lets the remaining eight phases parse and
run; user-task steps are auto-completed (the "user" always acts) but carry no worker-pool
infra.

## Camunda 8 import — analyse existing C8 history offline (`src/camunda_import.rs`)

A consultant often has a customer's **Camunda 8 / Zeebe** history but no Nano engine.
`import-camunda` folds a C8 record export into the same `traces.json` dataset shape, so
that history loads straight into a workspace `DatasetSource` — no engine, no Java.

Camunda brokers stream every state change as an ordered `Record<?>` log; the
Elasticsearch/Opensearch and debug-log exporters persist those as JSON. This is the
offline counterpart of the Nano gateway's in-engine fold (`server/src/console/trace.rs`):
the same projection, run over Camunda's records. The intent vocabularies map ~1:1
(`PROCESS_INSTANCE` element lifecycle → instance/element timing; `JOB`
CREATED/COMPLETED/FAILED + `JOB_BATCH` ACTIVATED → job queue/service split & failures;
`INCIDENT` CREATED → incidents; `PROCESS_INSTANCE_CREATION` → Tier-1 creation variables;
`JOB` COMPLETED variables → Tier-2 `jobCompleted` stimulus log for recorded-input replay).

```bash
# input may be a file or a directory of NDJSON / JSON-array / ES `_search` dumps
processos import-camunda ./zeebe-records.ndjson /tmp/c8-dataset
#  -> traces.json (load via a workspace dataset); prints a fold summary
#  -> { recordsRead, traces, completed, withCreationVariables, withStimuli, processes, … }

processos import-camunda ./zeebe-records.ndjson /tmp/c8-dataset --no-tier2  # skip stimulus capture
```

**Fidelity tiers.** Tier-0 (instance + element durations + jobs + incidents) is always
produced. Tier-1 (`creationVariables`) needs the `PROCESS_INSTANCE_CREATION` record. Tier-2
(`stimuli`) captures `jobCompleted` outputs only — a model that also consumes messages,
timers, or user-task inputs is therefore only partially replayable. The queue-vs-service
split needs `JOB_BATCH ACTIVATED` records in the export; without them the whole job wait is
reported as `serviceMs`.

See **[`docs/processos-camunda-import.md`](../docs/processos-camunda-import.md)** for the
full reference: the complete record→trace mapping, accepted input layouts, getting records
out of Camunda, known limits, and the path from this offline transformer to an in-engine
Zeebe exporter.

## LLM-driven investigation — open-ended sensing over the data (`src/analysis.rs`, `agent.rs`, `investigate.rs`)

The pre-built analyzers above (`corpus::infer`, `queueing`, `cluster`) encode *our*
priors. The research goal is the opposite: can the cockpit droid **reason over the trace
data and form its own hypotheses**? To open the hypothesis space, ProcessOS flattens a
bound trace source into in-memory **DuckDB** tables and gives the model a single
**read-only** `query_traces(sql)` tool — so it writes SQL, sees the result, and reasons,
instead of picking from a fixed menu.

- `analysis.rs` — flattens traces into `instances` / `jobs` / `incidents` tables with the
  cheap temporal primitives pre-derived (`hour`, `dow`, `is_weekend`, a real
  `started_ts`) but **no** pre-bucketed "windows" (that labelling is the inference we want
  the droid to perform). `query()` admits a single `SELECT`/`WITH` only — DDL/DML are
  rejected — over an ephemeral in-memory connection.
- `agent.rs` — a provider-agnostic tool-calling loop (`run_agent`) over an `AgentStep`
  transport (OpenAI/llama.cpp `chat/completions` tool-calls today), unit-testable with a
  deterministic mock. Every query+result is recorded as a reproducible **lab notebook**.
- `investigate.rs` — wires the two: a system prompt that imposes analytical discipline
  (state a hypothesis before querying, report effect size + sample size, replicate on a
  held-out slice before concluding) and emits a gradeable JSON conclusion.
- `pyrunner.rs` — the optional **Python escape hatch** (`run_python`): for hypotheses SQL
  can't express (distribution fitting, changepoint/seasonal decomposition, clustering).
  Off by default; pass `"allowPython": true`. The tables are exported as CSV to a private
  temp workdir and a **dependency-tolerant preamble** loads them as `pandas` DataFrames +
  a `duckdb` connection when those libs are present (`HAVE_PANDAS` / `HAVE_DUCKDB` flags),
  degrading to stdlib `csv` row-dicts otherwise — so it works on a bare interpreter too.
  It runs **arbitrary operator-trusted code** (a trusted-operator hatch, *not* a security
  sandbox) with pragmatic rails: a private workdir, a wall-clock timeout (child killed on
  overrun), and an output cap. Env: `PROCESSOS_PYTHON` (default `python3`),
  `PROCESSOS_PYTHON_TIMEOUT_SECS` (default 20), `PROCESSOS_PYTHON_MAX_OUTPUT` (default
  8000). For the rich path, point `PROCESSOS_PYTHON` at a venv:
  `python3 -m venv .venv && .venv/bin/pip install duckdb pandas numpy scipy` then
  `PROCESSOS_PYTHON=$PWD/.venv/bin/python`.
- `bpmn_model.rs` — **structural BPMN tools** added to the toolbox whenever the process has a
  `model.bpmn`: `read_model` distils the model into a structural graph (process id, start event,
  per-kind counts, and per-node `{id, kind, incoming, outgoing, reachable, gatewayRole}` plus
  kind extras like `jobType`/`attachedTo`/`messageName`), and `analyze_model` returns deterministic
  static findings (missing end events, unreachable / dead-end nodes, exclusive gateways without a
  default flow, unguarded service tasks, parallel-join deadlock hazards, exclusive joins of parallel
  paths, rework loops). Both work with **zero trace data** (design-time review). A flow node's `id`
  and a service task's `job_type` are the same join keys as the trace tables (`jobs.element_id`,
  `jobs.job_type`, `incidents.element_id`), so structural risk can be confirmed against runtime via
  `query_traces`. The **Process Architect** persona is prompted to drive these tools.
- `conformance.rs` — **process-mining tools** that intersect the *designed* model with the
  *actual* trace. `discover_flow` mines the directly-follows graph straight from the `jobs`
  table (task nodes + the A→B transitions between consecutive task executions, ordered by a new
  per-instance `seq` column), with no reference to the BPMN. `conformance_check` replays that
  mined behaviour against the model's permitted task-to-task transitions
  (`bpmn_model::model_task_graph`, which collapses gateways/events so a transition is "permitted"
  when the target task is reachable through only non-task nodes) and reports a transition-fitness
  score plus the concrete divergences: nonconformant transitions, undocumented tasks (executed but
  not modelled), unused model transitions (designed but never taken), and start/end deviations.
  Caveat: parallel branches are linearised in capture, so cross-branch directly-follows edges are
  artefacts. The **Conformance Miner** persona drives both, then characterises the divergences
  with `query_traces`.
- `experiment.rs` — **the Nano Alternate Reality Engine**: speculative execution of variant
  hypotheses. `simulate` forks the current model into a candidate (what-if) BPMN and replays the
  REAL recorded production instances through it on an in-process engine, returning a fidelity
  scorecard (fidelity tier, boundary-conserved count and conserved-rate, replayed latency,
  per-job-type coverage, the job types it would need new workers for, and any output keys it
  failed to reproduce). When a flagged `requiresNewWorkers` / `uncoveredJobTypes` value happens to
  equal a service task's **id**, the scorecard attaches a `jobTypeHints` entry explaining it is the
  `zeebe:taskDefinition`-didn't-bind mistake (the job type defaulted to the element id) — not the
  engine matching on element_id — and points at the `edit_model set_task_job_type` fix, so the model
  stops perceiving it as an engine bug. The scorecard also distinguishes a **genuinely-new worker**
  (a job type with *no* recorded history — the only thing `requiresNewWorkers` now lists, fixable
  with a `mockWorkers` entry) from a **structural divergence**: an *existing* worker (with real
  recorded history) the candidate issued more often than history did. The latter is reported under
  `divergentWorkers` plus a prominent `structuralDivergence` hint that steers the model to fix the
  topology (a broken gateway, a condition on the wrong element, a duplicated branch) instead of
  mocking a real worker. Relatedly, `analyze_model` now emits a `condition-on-non-gateway` warning
  when a flow condition sits on anything other than an exclusive (XOR) gateway — the engine only
  evaluates conditions on a gateway split, so such a condition is silently ignored. `compare_variants` scores a whole *multiverse* of candidates against the
  same recorded dataset — the current model included as the `baseline` — and ranks them
  fidelity-first. Both are built on the deterministic replay harness and need **Tier-2
  recorded-input capture** (`c8 nano --capture` / `NANOBPMN_TRACE_STIMULI`); without it they
  return `replayable:false` with skip accounting rather than fabricating a result. The recorded
  dataset is distilled once per chat turn (bounded to the most recent instances) whenever the
  process has a `model.bpmn`. A new worker can also be mocked as a **failure** — a mock outcome
  with `"throwError": "<ERROR_CODE>"` (instead of `"output"`) makes the worker raise that BPMN
  business error so a candidate's **error boundary** is actually exercised under replay (weight it
  to fail a fraction of the population). The **Experiment Designer** persona drives these tools.
- **Authoring guardrails — runtime errors pulled forward to the authoring boundary.** Hand-writing
  whole-document BPMN is the weakest link for an LLM, so the two highest-frequency mistakes are
  auto-healed and surfaced (IDE-red-squiggle style) instead of looping the model on opaque parse
  errors (`bpmn_model::normalize_authoring` / `lint_task_definition_attribute`): (1) `<…:errorBoundaryEvent>`
  — **not a real BPMN element** — is rewritten to `<…:boundaryEvent>` (an error boundary is a
  `boundaryEvent` carrying a nested `errorEventDefinition`), the single most common cause of the
  `sequence flow … has unknown source element` parse loop; (2) `zeebe:taskDefinition` written as a
  serviceTask **attribute** (which the engine silently ignores, defaulting the job type to the task
  id, so the worker never binds to recorded job types) is flagged as a `task-definition-as-attribute`
  finding with the child-element fix. `validate_model` applies both (reporting them under `autoFixed`
  / findings); `simulate` and `compare_variants` apply the same heal **at the deploy boundary** —
  so the green light is on the same artifact that gets replayed — echoing what changed in
  `authoringFixes`. The opaque engine `deployError` is additionally mapped to a concrete `fix` /
  `fixHint` (`bpmn_model::deploy_fix_hint`) for the recurring boundary-event and unknown-element
  errors. *(Next step on the roadmap: a structured `edit_model` patch tool so the model composes a
  variant from validated operations instead of one-shotting raw XML at all.)*
- **`edit_model` — structured, validated authoring (no more one-shotting raw XML).** The durable
  fix for "hand-writing whole-document BPMN is the LLM's weakest link": instead of emitting raw XML,
  the persona composes a variant from **validated structured operations** applied to the current
  model, and ProcessOS owns the XML correctness end-to-end (`bpmn_model::edit_model` +
  `definition_to_xml`). The base is auto-healed and parsed with the engine's own parser; each op
  mutates the parsed `ProcessDefinition`; the result is re-serialized to engine-parseable XML and
  re-parsed before it is returned — so the model literally **cannot** produce the element/attribute
  syntax mistakes it falls into by hand. Supported ops: `set_task_job_type`, `set_flow_condition`,
  `insert_service_task_after`, `add_error_boundary` (synthesizes the `<bpmn:error>` + `errorRef`
  for you), `reroute_flow`, `remove_node` (reconnects predecessors→successors, drops attached
  boundaries), and `add_exclusive_gateway`. The call returns the new full `model` XML (ready to pass
  straight to `simulate` / `compare_variants`), per-op `appliedOps` notes, and the post-edit
  `analyze_model` findings. The Experiment Designer persona now reaches for `edit_model` as the
  authoring primitive (rung 0 of its iterate-cheaply ladder). *(DI is intentionally omitted — these
  are candidate models for simulation, and the cockpit renders DI-less variants.)*

Agent-marked job workers use canonical IR `serviceTask` with a required `jobType`
and optional `agentType` (`aiAgentTask`, `external`, or `aiAgentSubProcess`). Every
marker is metadata on an ordinary job, not a separate native-agent activation.
Historical `agentTask` IR remains accepted and lowers to this form; omitted legacy
job types retain the element-id fallback. XML emission preserves the marker alongside
priority, task headers, linked resources, retries, I/O mappings, and multi-instance
configuration. XML container tagging follows the retained ad-hoc definition and tool
catalog, not the marker: both `external` and `aiAgentSubProcess` can mark an
`adHocSubProcess`. XML emission restores the catalog's authored tools and their DI,
including retained embedded sub-process bodies. It does not invent tools for empty
containers. The compact IR retains agent classification but does **not** represent
ad-hoc catalogs/bodies or linked resources; it therefore cannot round-trip a complete
ad-hoc BPMN model. Catalog-only leaf tools preserve the fields retained by the engine
(kind, name, job type or user/call configuration, and I/O), not arbitrary source XML
extensions discarded during parsing.

```bash
# Point the configured LLM at a workspace process bound to a dataset:
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/investigate \
  -d '{"llm":{"model":"local-model"},"maxRounds":12,"allowPython":false}'
# -> { dataset:{instances,jobs,incidents},
#      run:{ answer:"{\"bottleneckJob\":\"credit-check\",\"window\":\"weekday-morning\",…}",
#            steps:[{tool:"query_traces", arguments:{sql}, result}], rounds } }
```

The DuckDB connection is `!Send`, so the endpoint runs the whole loop on a dedicated
current-thread runtime via `spawn_blocking`. The labelled corpus is the **eval substrate**:
`investigate.rs` tests drive the loop over a generated corpus and assert it recovers the
planted `credit-check` / `weekday-morning` fault through the full model→tool→DuckDB→model
path (and via `run_python` over the exported CSV with stdlib-only Python).

### Interactive cockpit chat (`src/chat.rs`, `chat_prompts.rs`)

The one-shot `investigate` call is also available as a **multi-turn, scrollable chat** at
`/cockpit?workspace=&process=`. The operator converses with the droid over the bound
dataset; each turn resumes from the persisted transcript, so the model remembers its own
earlier answers (ask *"where is the bottleneck?"* then *"when does **that** happen?"*).

- `chat.rs` — a file-backed `ChatStore` under `PROCESSOS_DATA_DIR` (one JSON file per
  `(workspace, process)`, holding **several named sessions** so the operator can keep
  multiple parallel investigations) plus `render_view`, which projects the raw model
  transcript into operator-facing `user`/`droid` turns (each droid turn carries the SQL/
  Python tool steps it ran as a collapsible **lab notebook**). Each session has an editable
  `name`, `created`/`updated` stamps, and a per-turn timestamp vector (`stamps`) aligned to
  the rendered turns by `render_view_stamped`. The pre-multisession single-conversation file
  shape is migrated on load into one named session.
- **Sessions / tabs** — the cockpit shows a left-hand tab list (newest activity first) with
  each chat's name and last-message time; operators **create**, **rename**, and **delete**
  sessions, and every chat call carries the active `?session=` id. Endpoints live under
  `.../chat/sessions`.
- **Personas** — each chat session runs under a selectable **persona**: a standing *system*
  prompt that sets the droid's lens and discipline. Built-ins ship with **Performance Analyst**
  (the default — the canonical `investigate::CHAT_SYSTEM`), **SRE / Incident Responder**,
  **Capacity Planner**, **Process Architect**, **Conformance Miner**, and **Experiment Designer**;
  operators author their own in the Prompts view. The persona is chosen in
  the cockpit's compose row, sent as `personaId`, and **baked into the session's system message
  on its first turn** — so a session's persona is fixed once the conversation starts (the picker
  locks and the bound id is surfaced on the session/`SessionMeta`). Personas persist to
  `<config_dir>/personas.json` and are served by `GET/POST /api/personas` + `DELETE
  /api/personas/{id}` (built-ins are read-only). This is distinct from the per-message
  *chat-prompt* templates (`/api/chat-prompts`) and the *hypothesis* system prompts
  (`/api/prompts`).
- `agent::run_agent_resumable` drives the loop over a `&mut Vec<Msg>`, appending the
  assistant/tool turns **and** the final answer so the next turn keeps full context. A turn
  can also be **wrapped up early** (`POST .../chat/wrapup`): the loop checks a per-session
  cancel flag between rounds and, when set (or when the round budget is exhausted), tells the
  model to stop investigating and report its findings so far instead of erroring.
- **Chain-of-thought is preserved across rounds**: the reasoning a model emits in a round
  where it decides to call a tool is folded into a `<think>` block on that tool-calling turn
  and carried forward by `render_view` onto the droid bubble it precedes — so the collapsible
  "Thinking" disclosure survives even when the visible answer came from a later round (the
  cockpit renders it as **collapsed, expandable Markdown**, since models think in Markdown too).
- **Live token streaming** (`POST .../chat/stream`, SSE): the canonical agent loop
  (`run_agent_streaming`) threads a sink that emits `round` / `reasoning` / `answer` / `tool` /
  `toolResult` events as the model produces them, terminated by `done` (or `error`). The
  `OpenAiAgent` requests `stream: true` and parses the SSE deltas — accumulating
  `reasoning_content` and `content` tokens and assembling streamed `tool_calls` fragments by
  index — so the cockpit shows the droid's thinking, tool calls, and answer **as they happen**
  (live "Thinking" disclosure shown open, then collapsed once the turn completes). The
  non-streaming `POST .../chat` endpoint still exists (same loop, no-op sink) for curl/API use.
  (`run_agent_streaming`) threads a sink that emits `round` / `reasoning` / `answer` / `tool` /
  `toolResult` events as the model produces them, terminated by `done` (or `error`). The
  `OpenAiAgent` requests `stream: true` and parses the SSE deltas — accumulating
  `reasoning_content` and `content` tokens and assembling streamed `tool_calls` fragments by
  index — so the cockpit shows the droid's thinking, tool calls, and answer **as they happen**
  (live "Thinking" disclosure shown open, then collapsed once the turn completes). The
  non-streaming `POST .../chat` endpoint still exists (same loop, no-op sink) for curl/API use.
- **Pair AI — a second model reviews each turn (N-tier ready).** Optionally, after the primary
  droid answers, one or more **Pair AI** reviewers run in sequence, each a *fresh* sub-conversation
  (its own LLM profile + a **pair-kind persona**) that is handed the user's question and the prior
  answer and re-runs over the **same** data/model tools (`query_traces`, `validate_model`,
  `simulate`, …) to verify, refine, or challenge it. Built-in pair personas: **Skeptic / Red-Team**
  (the default — stress-tests the conclusion), **Synthesizer** (reconciles into one answer), and
  **Refiner** (tightens and corrects). Only the reviewer's final answer is appended to the
  transcript — marked so `render_view` surfaces it as an attributed `pair` turn (purple bubble,
  "🤝 Pair AI · {name}") — keeping the thread a single coherent conversation. The orchestration
  lives in `investigate::run_chat_turn`, which accepts a **chain** of `PairStage`s (`pairs:
  Vec<PairRequest>`; the convenience `pair` field is a 1-stage chain), emitting an `agent`
  SSE/provenance boundary before each reviewer — so the design generalises from one reviewer to an
  N-tier network of cooperating agents. Pair AI is **off by default**; enable it in the compose row,
  pick the reviewer's profile (ideally a *different* model family, so errors decorrelate) and
  persona. Pair personas carry `kind: "pair"` and are offered only in the Pair AI picker, never as
  the primary chat persona. A **carry-forward** button (↪) on any droid/pair answer quotes it into
  the composer, so you can also hand one model's conclusion to another model on the next turn
  (cross-provider refinement) without enabling synchronous Pair AI.
- **Loop monitor — a second model breaks circling (`src/monitor.rs`).** A long investigation can
  *semantically* circle — rephrasing the same dead-end, oscillating between hypotheses, or
  re-attacking a genuinely unreachable path — which the cheap lexical guards in `agent.rs`
  (identical-line / identical-tool-call repetition) don't catch. Optionally enable a **loop
  monitor**: an async watcher that periodically reads the live transcript (made fresh by the
  ~2s incremental persistence), renders a recent window, and asks a *second* model (its own
  profile + a `kind: "monitor"` persona, the built-in **Loop Breaker**) whether the primary is
  circling. If so it injects a `[loop monitor] …` **steer** into the same queue the human
  Steer button uses — primed with the escape hatches (mock workers, failure mocking,
  `edit_model`) so it nudges toward forward progress. After a bounded steer budget
  (`MONITOR_MAX_STEERS`, default 3) of continued circling it escalates to a forced **wrap-up**
  (the same cancel path as the human Wrap-up button). It's **off by default** and biased to
  false negatives (any transport/parse failure ⇒ "not circling" ⇒ no intervention). The
  watcher emits `{type:"monitor"}` SSE events, rendered as a distinct centered note in the
  thread. Enable per-request (`monitor:{enabled, profileId, personaId}`) or globally via the
  `PROCESSOS_MONITOR` env var (`1`/`profileId` ⇒ on). In the cockpit it's a **Monitor** toggle in
  the compose row (a second "pairing mode" alongside Pair AI); its profile picker defaults to the
  local **sidecar** model so it's cheap to keep watching.
- `chat_prompts.rs` — a **prompt library** of reusable compose-box message templates,
  persisted to the user **config dir** (`chat-prompts.json`, alongside `settings.json`).
  Ships four built-ins led by an **open investigation** (the default, pre-loaded into a
  fresh compose box so the unbiased default action is to let the droid profile the data and
  find the problem itself) plus a specific *worker-swap hypothesis*, a temporal angle, and a
  failures angle. Operators author new ones from the compose box ("Save prompt") or the
  console **Prompts** view (which lists both libraries), and load any into the box from a
  dropdown. The CHAT_SYSTEM framing is deliberately open — it asks the analyst to profile
  broadly and form its own hypotheses rather than confirm a preconceived answer — and the
  process's operator-set **objective** (editable on the cockpit surface) is woven in as
  *background context, not a conclusion*, so the operator can optionally steer without
  pre-revealing the planted issue.

```bash
# Send one turn (resumes the persisted transcript); GET to reload it, /reset to forget.
# All chat calls accept ?session={id} (defaults to the most recent / a fresh session).
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat \
  -d '{"message":"Which job type has the worst queue tail?","allowPython":false}'
# -> { sessionId, answer, rounds, dataset:{instances,jobs,incidents}, turns:[{role,text,steps,ts}] }
# Same turn, streamed live as Server-Sent Events (round/reasoning/answer/tool/toolResult/done):
curl -N -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/stream?session={id} \
  -d '{"message":"Which job type has the worst queue tail?","allowPython":false}'
# Pair AI: after the droid answers, a reviewer (its own profile + pair persona) re-checks it.
# Emits an extra `agent` SSE before the reviewer; its answer renders as a `pair` turn.
curl -N -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/stream \
  -d '{"message":"Which job type has the worst queue tail?","pair":{"enabled":true,"profileId":"qwen-8b","personaId":"pair-skeptic"}}'
# Loop monitor: a second model watches the live transcript and steers/wraps-up on circling.
curl -N -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/stream \
  -d '{"message":"Find the bottleneck and propose a fix","monitor":{"enabled":true,"profileId":"qwen-8b","personaId":"monitor-loop-breaker"}}'
curl       .../api/workspaces/{workspace}/processes/{process}/chat        # load transcript
curl       .../api/workspaces/{workspace}/processes/{process}/chat/sessions          # list tabs
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/sessions -d '{"name":"Probe"}' # new
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/rename -d '{"name":"…"}'
curl -XDELETE .../api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}   # delete a tab
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/reset?session={id} # forget it
curl       .../api/chat-prompts                                           # list templates
curl -XPOST .../api/chat-prompts -d '{"id":"my-probe","name":"My probe","text":"…"}'
curl       .../api/personas                                              # list chat personas
curl -XPOST .../api/personas -d '{"id":"cost-hawk","name":"Cost Hawk","summary":"…","system":"You are…"}'
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/wrapup # stop & report now
curl       .../api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/debug # exact payloads sent
curl       .../api/python/status   # { configured, interpreter, interpreterRuns, dataScience, missing }
```

The chat surface adds a few operator conveniences: a left-hand **tab list** of named chat
sessions (each showing its last-message time and turn count) with **+ New**, inline
**rename** (✎), and **delete** (✕); every message and the tab itself is **timestamped**. Each
chat window also has a **Chat / Debug** tab pair — the **Debug** tab shows *exactly what was
sent to the model* on the session's most recent turn (`GET .../chat/sessions/{id}/debug`): one
collapsible section per round with the model, temperature, `max_tokens`, the **full message
array** (system prompt + prior transcript + the latest message, each role-badged), the tool
specs offered, and a **Copy JSON payload** button — so it's clear the model receives far more
than just the operator's prompt. (The same payloads stream live as `request` SSE events.) The
**Send** button is a split control labelled with the active LLM profile (*"Investigate with
&lt;profile&gt; →"*) whose **caret opens a profile picker** so the operator can switch the model
that answers the next message without opening Settings; the button relabels live when the
active profile changes. Droid bubbles are titled with the **profile name** and the model id in
a smaller, dimmer parenthetical (*"Remote Qwen 3.6 (unsloth/qwen3.6-…)"*); they carry a
**copy-to-clipboard** button and render the answer as **Markdown** (headings, lists,
bold/italic, inline + fenced code, **GitHub-style tables**, links — escaped first, only http(s)
links emitted). Any reasoning is shown as a **collapsed, expandable, Markdown** "Thinking"
disclosure whose **tool calls appear inline, exactly where they happened** in the chain of
thought (each its own nested collapsible section). The droid's chain-of-thought is captured
whether the backend emits inline `<think>…</think>` tags or a separate `reasoning_content`
field (as llama.cpp does for Gemma/Qwen), and is preserved even when it happened in an earlier
tool-calling round.
**A−/A+** controls size the chat font (persisted in `localStorage`); a **Wrap it up →** button
appears while a turn is in flight (`POST .../chat/wrapup`); and the Python toggle
self-describes from `GET /api/python/status` — *"Enable Python Data Science tools"* when the
interpreter has pandas/duckdb, *"Enable Python (Optional: Install Data Science tools)"* with an
install popup when it runs but lacks them, or *"Configure Python"* with a setup popup when no
interpreter is usable — and its on/off state is **persisted** in `localStorage`.


## The cockpit & pilot loop (§10)

The optimization loop is itself authored as **editable BPMN** (the *pilot process*,
`pilot/pilot-self-optimize.bpmn`) deployed on the own engine — its **user tasks** are
the human's turns, **LLM tasks** the droid's, **engine tasks** the craft responding.
The pilot is **forkable** and file-backed under the data dir (`src/pilot.rs`), so the
delegation boundary an operator encodes in it is durable and inspectable. The
**cockpit** (`/cockpit`, `src/cockpit.rs` + `conversation.rs`) wraps the experiment
stepper in a persisted droid-conversation surface.

**Theming — dark / light / system.** Every ProcessOS page styles through one set of
semantic design tokens (`/assets/theme.css`, the `--po-*` custom properties: surfaces,
edges, text tiers, a violet+cyan accent family, and ok/warn/danger/info status
families), deliberately mirroring the Nano console's palette so the two read as one
product family. The **appearance switcher** (in the cockpit rail and on each page)
offers **light / dark / follow-system**; the choice persists and `/assets/theme.js`
applies it before first paint (no flash). System mode tracks the OS live. BPMN
canvases stay paper-white in every theme — bpmn-js draws dark ink.

**Customizable workbench.** The cockpit arranges like a workbench: the **nav rail**
and the **chats panel** resize (drag their edge; arrow keys nudge; double-click or
`Home` resets) and **collapse** to slivers, and the **investigation panel's height**
resizes the same way. The arrangement persists per browser (`processos.wb`) and
restores on reload; the rail's **↺** button resets the whole layout.

## Run

### Prerequisites (machine setup)

- **Rust** (stable) — `cargo` builds/tests; the `duckdb` crate vendors its engine (bundled),
  so no system DuckDB is needed. First build compiles the bundled amalgamation (~95s, one-time).
- **Python 3** — only required for the optional `run_python` investigation tool. A bare
  `python3` works (stdlib fallback). For the **rich** analysis path (pandas/numpy/scipy/duckdb)
  create a venv once and point `PROCESSOS_PYTHON` at it:

  ```sh
  # from the processos/ directory
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install duckdb pandas numpy scipy
  export PROCESSOS_PYTHON="$PWD/.venv/bin/python"   # else run_python uses stdlib-only python3
  ```

  The `.venv/` is git-ignored and machine-local. Without it, `run_python` still runs but the
  preamble degrades to stdlib `csv` row-dicts (`HAVE_PANDAS`/`HAVE_DUCKDB` are `False`).

```sh
# Build + test
make processos-build
make processos-test

# Run against a Nano gateway (defaults shown)
PROCESSOS_PORT=8090 NANO_BASE_URL=http://localhost:8080 cargo run

# …or let ProcessOS spawn + supervise its own engine for the pilot loop
PROCESSOS_SPAWN_NANO=1 cargo run
```

| Env | Default | Meaning |
|-----|---------|---------|
| `PROCESSOS_PORT` | `8090` | Port ProcessOS listens on |
| `NANO_BASE_URL` | `http://localhost:8080` | Back-compat alias defaulting **both** the target and own URLs |
| `NANO_TARGET_URL` | _(= `NANO_BASE_URL`)_ | The **read-only production** engine to analyse |
| `PROCESSOS_NANO_URL` | _(= `NANO_BASE_URL`)_ | The **own** engine the pilot loop runs on |
| `PROCESSOS_DATA_DIR` | `./.processos-data` | Persisted forks, conversations, prefs |
| `PROCESSOS_WORKSPACES_DIR` | _(`<data_dir>/workspaces`)_ | Root of the workspaces/processes tree |
| `PROCESSOS_PROMPTS_DIR` | _(none)_ | Directory of prompt files imported into the library on boot |
| `PROCESSOS_SPAWN_NANO` | `false` | Spawn + supervise an own Nano engine as a child process |
| `PROCESSOS_NANO_BIN` | _(built gateway)_ | Path to the own-engine gateway binary |
| `PROCESSOS_NANO_PORT` | `0` (auto) | Port for the spawned own engine |
| `PROCESSOS_NANO_DATA_DIR` | `.processos-nano-data` | Data dir for the spawned own engine |
| `PROCESSOS_NANO_CAPTURE` | `true` | Enable trace/stimulus capture on the own engine |
| `PROCESSOS_PYTHON` | `python3` | Interpreter for the `run_python` tool — point at a venv for the rich (pandas/duckdb) path |
| `PROCESSOS_PYTHON_TIMEOUT_SECS` | `20` | Wall-clock budget for a `run_python` call (child killed past it) |
| `PROCESSOS_PYTHON_MAX_OUTPUT` | `8000` | Max chars of combined stdout+stderr returned to the model |

### Capturing replay inputs on Nano (opt-in, on the engine being read)

Recorded-input replay needs the real inputs on the trace. These are **off by default**
(footprint + PII) and enabled on the *Nano* side:

- `NANOBPMN_TRACE_VARIABLES` — retain creation inputs + per-incident variable snapshots
  (`NANOBPMN_TRACE_VARIABLES_MAX_BYTES`, default 16384, caps each snapshot).
- `NANOBPMN_TRACE_STIMULI` — also retain the ordered external-stimulus log per instance
  (implies variable capture; `NANOBPMN_TRACE_STIMULI_MAX`, default 1024, caps the log).

When ProcessOS spawns its own engine with capture on, it sets `NANOBPMN_TRACE_STIMULI`
for it automatically.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/` | Landing page |
| `GET` | `/features` | Features page |
| `GET` | `/console` | Insights dashboard (fetches `/api/insights`) |
| `GET` | `/cockpit` | The cockpit — Console → Process → Experiment, with the droid conversation; `?workspace=&process=` opens the workspace-dataset investigation surface |
| `GET` | `/harness` | Optimization-harness dashboard (runs the example scenario) |
| `GET` | `/workspace` | Workspace browser — workspaces → processes → BPMN model + Insights + "Reason in cockpit" |
| `GET` | `/health` | Liveness (`ok`) |
| `GET` | `/api/insights?limit=&sample=` | Folded performance report from the **target** traces/metrics |
| `GET` | `/api/cockpit/overview` | Cockpit state: process(es), pilot, recent experiments |
| `GET`/`POST` | `/api/cockpit/experiments` | List / create experiments |
| `GET` | `/api/cockpit/experiments/{key}` | One experiment |
| `POST` | `/api/cockpit/experiments/{key}/decision` | The pilot's turn at `Review` (`accept`\|`iterate`\|`stop`) |
| `GET`/`POST` | `/api/cockpit/experiments/{key}/conversation` | Read / append to the persisted droid conversation |
| `GET` | `/api/harness/example` | The bundled example scenario JSON (a template to copy) |
| `GET` | `/api/harness/example/run` | Run the example scenario, return the ranked report |
| `POST` | `/api/harness/run` | Run a caller-supplied scenario, return the ranked report |
| `POST` | `/api/harness/calibrate` | Recalibrate a scenario from supplied measured distributions, then rank |
| `POST` | `/api/harness/hypothesize` | LLM proposes candidates; harness evaluates + ranks (`{scenario, llm?, includeBaked?, measured?, prompt?/promptId?}`) |
| `POST` | `/api/harness/replay` | Replay one historical instance against a candidate (Level-2 verifier) |
| `POST` | `/api/harness/replay-batch` | Batch replay over many instances |
| `POST` | `/api/harness/replay-rank` | Replay-rank a candidate population by fidelity gradient |
| `POST` | `/api/harness/evolve` | LLM-propose structural candidates, replay-rank them on real traces |
| `GET` | `/api/harness/production` | Live-source baseline a candidate search must beat |
| `GET` | `/api/harness/cluster?...&targetP99Ms=` | Cluster-aware measurement + optional Erlang-C staffing recommendation |
| `GET`/`POST` | `/api/prompts` | List / author (import) hypothesis prompts |
| `GET`/`DELETE` | `/api/prompts/{id}` | Get / delete a prompt (refuses built-ins) |
| `GET`/`PUT` | `/api/pilot` | Read / fork the pilot process BPMN |
| `POST` | `/api/pilot/reset` | Restore the built-in default pilot |
| `GET`/`POST` | `/api/workspaces` | List / create workspaces |
| `POST` | `/api/workspaces/seed-demo` | One-click demo: Northwind Bank + Loan Approval + generated traces + model |
| `GET` | `/api/workspaces/{workspace}` | A workspace + its processes |
| `POST` | `/api/workspaces/{workspace}/processes` | Create a process (bind `targetUrl` or `dataset`) |
| `GET`/`PUT` | `/api/workspaces/{workspace}/processes/{process}` | Read / update a process config |
| `GET`/`PUT` | `/api/workspaces/{workspace}/processes/{process}/model` | Read / set the process's BPMN model |
| `GET` | `/api/workspaces/{workspace}/processes/{process}/insights` | Insights folded over the process's bound source |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/investigate` | One-shot LLM-driven investigation over the bound source via the `query_traces` SQL tool (+ optional `run_python` when `allowPython:true`) |
| `GET`/`POST` | `/api/workspaces/{workspace}/processes/{process}/chat` | Multi-turn cockpit chat: `GET` loads the persisted transcript; `POST {message,allowPython}` sends one turn and resumes from it |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/chat/stream` | Same as `chat`, streamed live as SSE (`round`/`request`/`reasoning`/`answer`/`tool`/`toolResult`/`done`) |
| `GET`/`POST` | `/api/workspaces/{workspace}/processes/{process}/chat/sessions` | List chat sessions (tabs) / create one (`POST {name?}`) |
| `GET`/`DELETE` | `/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}` | Load one session's transcript / delete it |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/rename` | Rename a session (`POST {name}`) |
| `GET` | `/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/debug` | The exact model request payloads sent during this session's most recent turn (one per round) — the Debug tab |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/chat/wrapup` | Ask the in-flight turn to stop and report its findings so far |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/chat/reset` | Forget this dataset's conversation (`?session={id}` clears just that one) |
| `GET`/`POST` | `/api/chat-prompts` | List / author reusable compose-box prompt templates (persisted to the config dir) |
| `DELETE` | `/api/chat-prompts/{id}` | Delete a non-built-in chat prompt |
| `GET`/`POST` | `/api/personas` | List / author chat **personas** (standing system prompts; persisted to the config dir). Built-ins are read-only |
| `DELETE` | `/api/personas/{id}` | Delete a non-built-in persona |
| `GET` | `/assets/bpmn/{file}` | Vendored bpmn-js viewer assets (model rendering) |
| `GET`/`PUT` | `/api/settings` | Read settings / update the **globals** (`activeProfile`, `pythonBin`, `modelsDir`, `llamaBin`). `GET` lists LLM profiles (each redacting its key as `apiKeySet`, plus `sidecar`/`modelFile`/`sidecarArgs`) + the active profile + the Python interpreter + the sidecar models dir / binary + the `defaultModelsDir` prefill |
| `POST` | `/api/settings/profiles` | Create a new LLM profile (optional `name`); returns the new `id` + the updated settings |
| `PUT`/`DELETE` | `/api/settings/profiles/{id}` | Partial-update / delete one profile. On `PUT`, absent fields are unchanged, an empty string (or `0`/negative number) clears a field back to the env default; the API key is set only when a non-empty `apiKey` is sent |
| `POST` | `/api/settings/models` | Query an endpoint for its model list (body: `profileId` + optional `provider`/`baseUrl`/`apiKey` overrides) so the console can pick a model id; each entry includes its `contextWindow` when the endpoint reports one |
| `GET` | `/api/llama/status` | Local llama.cpp sidecar pool: `{running, count, max, sidecars[], error}` where each entry has `profileId`, `model`, `port`, `pid`, `command`, `startedAt`. `running` is true when ≥1 is up |
| `GET` | `/api/llama/ready?profileId=…` | Whether a sidecar is answering its `/health` probe (model loaded). Returns `{running, ready, profileId}`; polled by the cockpit's just-in-time start before sending a queued message |
| `POST` | `/api/llama/start` | Start a sidecar for a profile (`{profileId, pairingId?}`; must have `sidecar:true`). ProcessOS auto-assigns a free port and records it in the profile's Base URL. Up to **two** run at once. A `pairingId` naming a **speculator** pairing loads its draft model via `--model-draft` — after verifying GGUF tokenizer/vocab compatibility; a profile with `mtp:true` is verified to actually carry MTP heads. Invalid configs are refused. Returns the new status |
| `GET` | `/api/llama/scan/{profileId}` | Scan a downloaded sidecar model's GGUF **header** (weights untouched): `{downloaded, arch, tokenizerModel, vocabSize, contextLength, mtpCapable, mtpLayers, nextnTensors, …}`. Powers the MTP toggle and speculator compatibility checks |
| `POST` | `/api/llama/stop` | Stop one sidecar (`{profileId}`) or **all** of them (empty body). Returns the resulting pool state |
| `GET` | `/api/llama/logs?profileId=…&since=N` | Tail a sidecar's combined stdout/stderr from offset `N`; returns `{lines, text, nextOffset, running}` for incremental polling |
| `GET` | `/api/llama/reasoning-control?profileId=…` | Probe whether the active (or named) profile's endpoint exposes the optional reasoning-control surface (`/v1/chat/completions/control`). Returns `{supported, ready, baseUrl}`; wrap-up/monitor use it for a mid-thinking halt when present, else fall back to the round-boundary cancel |
| `GET` | `/api/system/dependencies` | Preflight the external CLI tools ProcessOS shells out to (Node; llama.cpp `llama-server`). Returns `{allPresent, missing[], dependencies[]}` where each entry has `id`, `name`, `purpose`, `bin`, `present`, `version`, `installUrl`, `hint` — drives the Console's missing-tool banner |

## Layout

```
src/
  main.rs         webserver bootstrap, config (target vs own engine), routes, pages
  contracts.rs    typed mirror of Nano's read-contract DTOs + the HTTP client
  report.rs       pure aggregation: traces -> Insights (with unit tests)
  supervisor.rs   spawn + supervise the own Nano engine (learn port, deploy pilot, reap)
  pilot.rs        the forkable pilot process — plastic surface (a) of §10
  workspace.rs    workspaces/processes tree + persistence + CRUD + source binding
  dataset.rs      DatasetSource (loaded trace folder) + TraceSource enum (live | dataset)
  corpus.rs       synthetic labelled trace corpus: generator + inference + scorer
  camunda_import.rs  fold a Camunda 8 / Zeebe record export into a Nano traces.json dataset
  analysis.rs     flatten traces into in-memory DuckDB; read-only query_traces surface
  agent.rs        provider-agnostic tool-calling loop (OpenAI transport) + lab notebook
  investigate.rs  LLM-driven investigation: analysis ToolBox + disciplined system prompt
  pyrunner.rs     optional run_python escape hatch: CSV export + timeout subprocess runner
  settings.rs     operator-editable LLM + Python settings, persisted to ~/.config/processos
  llama.rs        local llama.cpp `llama-server` sidecar supervisor (start/stop/logs)
  cockpit.rs      the cockpit: Console -> Process -> Experiment surface
  chat.rs         interactive cockpit chat: file-backed transcript store + view projection
  chat_prompts.rs reusable compose-box prompt library, persisted to the config dir
  conversation.rs persisted cockpit conversations + data-dir resolution
  harness/
    mod.rs         scenario / worker / variant types + seeded PRNG
    sim.rs         SimRunner: drives engine-core on a virtual clock (M0)
    rank.rs        candidate enumeration + evaluation + ranking (M1)
    llm.rs         pluggable LLM client: openai-compatible + anthropic (M2)
    prompts.rs     authorable, selectable prompt library (M2)
    hypothesize.rs LLM proposes candidates, validate + evaluate + rank (M2)
    calibrate.rs   recalibrate SimRunner worker models from measured production
    cluster.rs     cluster-aware measurement (M3): topology union, tail, queue/service split
    queueing.rs    Erlang-C worker-pool staffing recommendation (advisory)
    production.rs  live-source baseline (generation-skipped path)
    replay.rs      recorded-input replay evaluator (Level-2 verifier)
    replay_rank.rs replay-rank a candidate population by fidelity gradient
    evolve.rs      LLM-propose structural candidates, replay-rank on real traces
    example.rs     the bundled Classify->Summarize worker-swap demo
```

`engine-core` is consumed read-only over a `path` dependency and is never modified —
the harness embeds it for exact native replay without ever adding a back-edge from Nano
to ProcessOS.
