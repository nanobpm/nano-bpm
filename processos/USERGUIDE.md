# Nano ProcessOS — User Guide

**ProcessOS is your process-optimization workbench.** You point it at a business
process, hand it the history of how that process actually ran, and a built-in AI
investigator — *the droid* — helps you find bottlenecks, form hypotheses, and
test changes by replaying them against the real data. Nothing is changed in your
live system: ProcessOS is **suggest-only**. You stay in control and decide what,
if anything, to act on.

This guide is task-focused. It walks you through running ProcessOS, exploring the
bundled demo processes, talking to the droid, choosing and configuring the AI
models it uses, and packaging up a session so someone else can replay your
investigation. You do **not** need source code or a developer setup to follow it.

> **New here?** Start with [Getting started](#getting-started), then
> [Load a demo dataset](#load-a-demo-dataset) and
> [Investigate a process](#investigate-a-process). Everything else you can reach
> for when you need it.

## Getting started

ProcessOS ships as a single binary with its web console built in. There is
nothing to install beyond the binary itself.

1. **Start ProcessOS.** Run the binary you were given (for example
   `./processos`, or launch it with `c8ctl` if you manage Nano that way). It
   starts a local web server and prints the address it is listening on.
2. **Open the console.** In your browser, go to **http://localhost:8090** (this
   is the default; if you set a different port, use that instead). You'll land on
   the ProcessOS home page.
3. **Pick where to go.** From the home page you can jump to **Workspaces** (where
   your processes and data live), the **Cockpit** (where you investigate), the
   **User guide** (this document), and **What it does** (a feature tour).

That's all you need to explore the demos. A couple of optional capabilities have
extra requirements, and ProcessOS tells you about them when they matter:

- **Local AI models** need [llama.cpp](https://github.com/ggml-org/llama.cpp)
  installed. You only need this if you want ProcessOS to run a model on your own
  machine instead of calling a hosted API.
- **Workers** (the code that runs process tasks) need
  [Node](https://nodejs.org/) ≥ 22.6 installed.

If either tool is missing when you try to use it, a banner appears at the top of
the console explaining what's missing and linking to install instructions for
your operating system. You can dismiss the banner and keep working on everything
that doesn't need that tool.

### Changing the port and data location

ProcessOS reads a couple of environment variables when it starts:

| Setting | Default | What it does |
|---|---|---|
| `PROCESSOS_PORT` | `8090` | The port the console is served on. |
| `PROCESSOS_DATA_DIR` | `./.processos-data` | Where your workspaces, datasets, and saved investigations are stored. Point this at a stable folder so your work persists between runs. |

Set them before launching, for example `PROCESSOS_PORT=9000 ./processos`.

## The console at a glance

ProcessOS is organized into a few surfaces. You'll spend most of your time in
Workspaces and the Cockpit.

| Surface | Where | What it's for |
|---|---|---|
| **Home** | `/` | The landing page — your jumping-off point to everything below. |
| **Workspaces** | `/workspace` | Browse and create workspaces and processes, and **load the demo datasets**. A *workspace* is a tenant or business area; a *process* is one BPMN process inside it, together with the trace data recorded for it. |
| **Cockpit** | `/cockpit` | The investigation surface: chat with the droid, run replays, inspect the model, and export your session. This is where the real work happens. |
| **Live instance** | `/console` | A dashboard of insights from a connected Nano engine, for when you're analysing a live production system. |
| **Features** | `/features` | A short tour of what ProcessOS can do. |
| **User guide** | `/guide` | This guide. |

Throughout the console, a **Settings cog** in the lower-left opens the settings
panel, where you manage AI models and a few system options. See
[Configure AI models](#configure-ai-models).

## Make it yours — theme & workbench layout

**Appearance.** Every page has an appearance switcher (in the Cockpit it lives at
the bottom of the left rail) with three modes: **light**, **dark**, and **follow
system** — the last tracks your OS setting live. Your choice is remembered on
this browser and applied before the page paints, so there is no flash. BPMN
diagrams stay on a paper-white canvas in every theme; that's the ink they're
drawn with.

**Arrange the Cockpit like a workbench.** The investigation surface is yours to
shape, and it remembers how you left it:

- **Resize** the left **nav rail** and the **Chats panel** by dragging their
  edges (the handle highlights as you hover). The **investigation panel's
  height** resizes the same way via the handle underneath it.
- **Collapse** the rail (the *⟨ collapse* button at its foot) or the Chats panel
  (the *⟨* button in its header) down to a slim sliver; click the sliver to
  bring it back.
- **Fine-tune with the keyboard**: focus a handle and use the arrow keys to
  nudge, or press `Home` to reset that one dimension. Double-clicking a handle
  resets it too.
- **Reset everything** with the **↺** button next to the rail's collapse button.

The arrangement is saved per browser, so your workbench greets you the way you
configured it.

## Load a demo dataset

The fastest way to learn ProcessOS is to explore one of the bundled demo
processes. Each demo generates a realistic, synthetic history (a *trace corpus*)
so the droid has real data to investigate — no live system required.

1. Go to **Workspaces** (`/workspace`).
2. Click **Load demo dataset**.
3. In the picker — *"Choose a demo scenario to generate and explore"* — pick a
   scenario and click **Load**. ProcessOS shows *"Generating demo corpus…"* while
   it builds the data, then drops you into the seeded process.

Three demos ship with ProcessOS:

| Demo | Workspace · Process | The story |
|---|---|---|
| **Loan Approval** | Northwind Bank · Loan Approval | Loan origination with a credit-check bottleneck that bites under weekday-morning peaks. *"Keep loan approvals fast and reliable as volume grows."* |
| **CDD Refresh** | Meridian Trust · CDD Refresh | A multi-phase KYC/CDD refresh orchestrator with an agentic AI investigation stage. *"Complete periodic refreshes within SLA without overloading analysts."* |
| **Delivery Exception Resolution** | Amazing Retail · Delivery Exception Resolution | Resolving delivery exceptions quickly while keeping AI-agent cost under control. |

Loading a demo creates a workspace, a process, the generated trace dataset, and
the BPMN model — exactly the shape your own data will take later. Once it's
loaded, open the process and start investigating.

> **Bringing your own data later?** The demos mirror how real datasets are
> structured. When you import a customer's process history, it shows up as a
> workspace and process in the same place, and everything in this guide applies
> unchanged.

## Import your Camunda 8 history

You can analyse a **real Camunda 8 / Zeebe deployment's existing history** in
ProcessOS — no live engine required. ProcessOS folds a record export from Camunda
into the same trace dataset shape the demos use, so once imported it investigates
exactly like a demo.

### 1. Export the records from Camunda 8

Get a JSON export of Camunda/Zeebe records (the `Record` stream the
Elasticsearch / OpenSearch / debug-log exporters persist). The importer accepts
the common dump layouts:

- **NDJSON** — one record JSON object per line (debug-log / file exports).
- **JSON array** — `[ {record}, … ]`.
- **Elasticsearch / OpenSearch search response** — `{ "hits": { "hits": [ { "_source": {record} } ] } }` (a raw `_search`/scroll dump works).
- **A directory** of any of the above — every `*.json` / `*.ndjson` file is folded together.

### 2. Transform it into a dataset

Run the ProcessOS binary's `import-camunda` subcommand to turn the export into a
loadable dataset folder:

```sh
processos import-camunda <records.json|dir> <out-dir>
```

This writes `<out-dir>/traces.json` and prints a summary so you can confirm what
came through — records read, traces produced, how many **completed / terminated /
active**, and how many carried **incidents**, **creation variables**, and
recorded **stimuli**, plus the process ids it found.

How much of each instance is reconstructed depends on what the export contains:

- **Always:** the instance, its per-element durations, jobs, and incidents.
- **When present:** each instance's **creation variables**.
- **By default:** an ordered **stimulus log** — job completions plus, when the
  export records them, **message correlations**, **timer fires**, **user-task
  completions**, and **signal broadcasts** — which makes traces *recorded-input
  replayable*, so the droid's simulations can replay them. Pass `--no-tier2` to
  skip this if you only need the timings and structure.

> **What isn't captured.** A stimulus the export doesn't record — or a catch event
> with no resolvable element reference — can't be replayed, so a model that
> consumes it is only *partially* replayable. The import summary flags where
> stimuli were truncated and counts the partially-replayable instances.

### 3. Load it as a process

Point a ProcessOS process at the imported folder:

1. Go to **Workspaces** and open an existing workspace (or **+ New workspace**).
2. Click **+ New process**, give it a **Display name**, and put the import's
   `<out-dir>` path in the **Trace dataset folder** field. Click **Create**.
3. Open the process and investigate it — the droid, its tools, simulations, and
   pairing all work on your imported data exactly as they do on a demo.

(Leave the **Live target URL** blank — that field is for pointing at a running
Nano engine instead of a captured dataset. A process binds to one or the other.)

## Investigate a process

The **Cockpit** is where you and the droid work a process together. Open a
process from Workspaces (or click through after loading a demo) to get there.

1. **Pick a persona.** The **Persona** selector sets the droid's standing
   instructions — its job description for this conversation (for example a
   general investigator, or the *Experiment Designer* who specializes in testing
   changes). The persona is fixed once the conversation starts, so choose it
   first.
2. **Ask a question.** Type into the compose box and click
   **Investigate with the droid →**. The droid reads the process model and the
   recorded traces, runs its own analysis, and answers — showing its reasoning
   and the tools it used along the way.
3. **Test a change by replaying it.** In the Experiment Designer persona, the
   droid can **fork the process model**, apply a change, and **replay it against
   the dataset**. Each run appears in the **Simulations** view with the variant
   it tried and a scorecard, so you can compare options on real history rather
   than guesswork.
4. **See exactly what was sent.** The **Debug** view captures the precise request
   sent to the model for the last turn — useful when you want to understand or
   share what the droid actually saw.

Because ProcessOS is suggest-only, replays and forks never touch your live
process. They're experiments you can keep, discard, or hand to someone else.

## What the droid can do (its tools)

The droid doesn't just chat — it works your data with a toolkit of purpose-built
instruments. You never call these yourself; the droid decides which to use, and
you'll see each call (and its result) in the conversation and the **Debug** view.
Knowing what's in the kit helps you read what it's doing and ask for the right
thing.

**Looking at the recorded history**

- **`query_traces`** — runs read-only SQL queries over the recorded trace data
  (powered by DuckDB). This is how the droid pulls counts, durations, and
  breakdowns out of the history.
- **`discover_flow`** — reconstructs the path instances *actually* took from the
  traces, independent of the diagram.
- **`conformance_check`** — compares what really happened against the BPMN model
  and reports where they diverge.
- **`run_python`** — runs a short Python script over the dataset for deeper
  statistics. This appears only when Python is available; see
  [Set up Python for data-science analysis](#set-up-python-for-data-science-analysis).

**Reading the process model**

- **`read_model`** — a structured, summarized view of the BPMN model.
- **`read_model_xml`** — the raw BPMN XML (optionally for one called sub-process).
- **`analyze_model`** — static structural checks and advisories on the diagram.

**Proposing and checking changes**

- **`edit_model`** — makes targeted, structured edits to the model — for example
  setting a task's job type, renaming a node, setting a flow condition, inserting
  a service task, adding an error-boundary event, rerouting or removing a node, or
  adding an exclusive gateway. The droid patches the model rather than rewriting
  raw XML, so edits stay valid.
- **`validate_model`** — parses and lints a candidate model *before* simulating
  it, catching mistakes early and cheaply.

**Testing changes against reality**

- **`simulate`** — replays one candidate variant against the recorded instances
  and returns a fidelity scorecard.
- **`compare_variants`** — replays and ranks several candidates against the same
  recorded data, best-first.

See the next section for how the simulation tools turn into reviewable
suggestions.

## Suggest and test model changes

ProcessOS's most powerful move is proposing a change to your process and
**proving it on your own history** before you ever touch production. This is the
job of the **Experiment Designer** persona.

**How it works**

1. **Pick the Experiment Designer persona** and tell the droid what you're trying
   to improve (e.g. "reduce end-to-end time for high-value loans").
2. **The droid forks the model and proposes a *variant*.** A variant is just a
   candidate version of your BPMN model with a change applied.
3. **It replays the variant against recorded traces.** Replay feeds each
   candidate the *real recorded inputs* — the original creation data and the
   ordered sequence of events from history — and measures what would have
   happened. Each run appears in the **Simulations** tab with its variant and a
   **scorecard**.
4. **It ranks candidates, best-first.** When several variants are in play, the
   droid scores them together so the strongest rises to the top.

**Reading a scorecard**

Each candidate is scored on how faithfully it reproduces the real outcome, and at
what cost. A scorecard typically shows:

- a **fidelity tier** — how trustworthy the result is (a measured replay is never
  presented as if it were a rougher speculative estimate);
- the **conserved rate** — how much of the real production outcome the variant
  preserves (the headline number);
- **end-to-end latency** (average and p99);
- **requires new workers** — whether the change would need task implementations
  that don't exist yet;
- **divergence hints** — where the variant's behaviour drifted from history.

**Test cheaply, then commit to a full run**

Replaying the whole dataset can take a while, so the droid (and you) can cap the
sample size: it typically starts with a **1-instance smoke test**, moves to a
**~25-instance sample** to see if a change looks promising, and only then does a
**full replay** for a trustworthy score. If you're directing the droid, you can
ask it to "try this on a small sample first."

**You decide — it's suggest-only**

Nothing is ever applied to your live process automatically. In the experiment
detail pane you have the final call:

- **Accept** — take the current result.
- **Ask droid to iterate** — send it back for another round of improvement.
- **Stop loop** — end the exploration.

The result is a vetted suggestion you can adopt on your own terms, or set aside.

## Choose an AI model

Every investigation turn is answered by an **AI model** (an *LLM profile* in
ProcessOS terms). You can switch which model answers without leaving the cockpit.

- The send button is labelled with the **active profile** — e.g.
  *"Investigate with Local (llama.cpp) →"* — so you always know which model is
  about to answer.
- Click the **▾** caret next to the send button to pick a **different profile**
  for the next message. This is handy for comparing how two models reason about
  the same question.
- To change the default for new conversations, open the **Settings cog** and
  click **Set active** on the profile you want.

A *persona* and an *LLM profile* are different things: the persona is *what the
droid is told to do*; the profile is *which model does the thinking*. You can mix
and match them freely.

## Pair the droid with a second model

A single model can talk itself into a corner. ProcessOS lets you put a **second
model alongside the droid** in one of two supporting roles. Both are **off by
default** and switched on from the controls above the compose box. For best
results, give the partner a **different model family** than the primary, so their
mistakes don't correlate.

A given model profile can hold only one role at a time — the primary you send to,
the Pair AI reviewer, or the Monitor — so the pickers won't let you double-book a
model. (If you run local models, ProcessOS can keep two sidecars up at once, so a
local primary plus a local partner works.)

### Pair AI — a reviewer on every turn

Tick **Pair AI** to have a second model review the droid's answer *each turn*,
using the same data and model tools. Pick the reviewer's **model** and its
**persona** (its reviewing style). The reviewer's reply appears as a distinct
🤝 **Pair AI** bubble right after the droid's answer, so you see both the original
and the second opinion.

The persona sets the *pairing mode* — how the reviewer engages:

| Pairing mode | What the reviewer does |
|---|---|
| **Skeptic / Red-Team** (default) | Challenges the droid's conclusion — re-checks the numbers, hunts for an overlooked confound, and validates any proposed model change. |
| **Synthesizer** | Reconciles the droid's findings into one decisive answer, keeping what the data supports and dropping what it doesn't. |
| **Refiner** | Improves the answer — deepens the analysis and fills the gaps — rather than tearing it down. |

### Monitor — a loop-breaker that watches the whole investigation

Tick **Monitor** to have a second model watch the investigation's live transcript
and **step in when the droid starts going in circles** — rephrasing dead-ends,
oscillating between hypotheses, or re-attacking an unreachable path. It nudges the
droid toward the one concrete next action, and forces a graceful wrap-up when
needed. Its interventions appear inline in the conversation.

The default monitor persona is **Loop Breaker**. The monitor defaults to your
local sidecar model so it's cheap to leave watching in the background. Pick its
model and persona next to the **Monitor** toggle.

> **Pair AI vs Monitor.** Pair AI critiques *each answer*; the Monitor watches the
> *whole arc* of the investigation and intervenes only when it detects the droid
> looping. You can run either, both, or neither.

## Configure AI models

Open the **Settings cog** (lower-left in the cockpit or console) to manage your
AI models. Each model you set up is saved as a **profile**. Use **+ Add** to
create one, **Set active** to make it the default, and **Delete** to remove one.

There are two kinds of profile.

### External model (hosted API)

Point ProcessOS at a model served over the network — your own gateway, a hosted
provider, or any OpenAI-compatible endpoint.

| Field | What to enter |
|---|---|
| **Name** | A label you'll recognise, e.g. *"OpenAI GPT-4o"* or *"Team gateway"*. |
| **Provider** | The provider/protocol (e.g. an OpenAI-compatible API). |
| **Base URL** | The API endpoint, e.g. `https://api.openai.com/v1`. |
| **Model** | The model id the endpoint expects. |
| **API key** | Your key. It's stored locally; use **Clear API key** to remove it. |
| **Max tokens** / **Temperature** | Optional generation limits and randomness. |

Click **Save profile** when you're done. Use **Set active** to make it the model
the droid uses by default.

### Local model (managed llama.cpp sidecar)

ProcessOS can run a model **on your own machine** by launching a local
`llama-server` for you — this is a *sidecar*. This needs
[llama.cpp](https://github.com/ggml-org/llama.cpp) installed (ProcessOS will tell
you, with a link, if it isn't).

1. Create a profile and choose **Managed sidecar**.
2. In **Model file / HF spec**, enter either a Hugging Face GGUF reference such
   as `unsloth/Qwen3-4B-GGUF:UD-Q4_K_XL`, or a path to a `.gguf` file you already
   have.
3. Optionally set **Startup args** (advanced llama.cpp flags, e.g.
   `-ngl 99 -c 32768 --jinja`).
4. **Save profile**, then click **Start sidecar**. ProcessOS downloads the model
   if needed and starts it locally. **Download** can pre-fetch a model ahead of
   time, **Stop** shuts the sidecar down, and **Logs** shows the server output if
   you need to troubleshoot.

Once the sidecar is running, select its profile (via the **▾** caret or
**Set active**) and the droid will use your local model — no external API
required.

> **System settings.** The settings panel also lets you set the **llama-server
> binary** location and a **Models directory** (where downloaded models are
> cached), and a **Python interpreter** used by some analysis tools. Defaults
> work for most setups; change these only if ProcessOS can't find a tool or you
> want models stored elsewhere.

## Set up Python for data-science analysis

The droid's **`run_python`** tool lets it run real statistical analysis over your
dataset — the kind of pandas/DuckDB work a data scientist would do by hand. It
works in two modes, and a one-time setup unlocks the powerful one.

**Stdlib-only (works out of the box).** If ProcessOS just finds a plain `python3`,
`run_python` still runs, but the droid only gets the data as basic CSV rows from
Python's standard library. This is enough for simple counting and filtering.

**Rich data-science path (recommended).** If Python has the data-science packages
installed, `run_python` hands the droid the dataset as ready-to-use **pandas**
DataFrames (`jobs`, `instances`, `incidents`) plus a live **DuckDB** connection
(`con`) — so it can do joins, aggregations, percentiles, and statistical tests
quickly and accurately.

### Enable the rich path

Do this once on the machine running ProcessOS:

1. **Create a virtual environment** (in a folder of your choice):

   ```sh
   python3 -m venv .venv
   ```

2. **Install the data-science packages:**

   ```sh
   .venv/bin/pip install duckdb pandas numpy scipy
   ```

3. **Point ProcessOS at that interpreter**, using either method:
   - Open the **Settings cog → Python (analysis escape hatch)**, set the
     **Interpreter** field to the venv's Python (e.g. `.venv/bin/python`, or its
     full path), and click **Save interpreter**; or
   - Set the environment variable before launching ProcessOS:
     `PROCESSOS_PYTHON=/full/path/to/.venv/bin/python`.

That's it — the next time the droid reaches for `run_python`, it gets pandas and
DuckDB instead of the stdlib fallback. If Python is missing entirely, ProcessOS
tells you, and the data-science tool simply isn't offered until it's available.

## Export a psychological trace for debugging

A **psychological trace** is a shareable snapshot of an entire investigation, so
someone else can replay and debug your session exactly as you experienced it —
without needing your machine. This is the way to get help: package up what
happened and send it on.

To export one:

1. In the cockpit, open the **Export ▾** menu above the conversation.
2. Under **Psychological trace**, click **Download trace (.zip)**.

Your browser downloads a `.zip` you can share (for example, drop it into Slack
for the person helping you). The bundle contains:

- The full **conversation transcript** and the droid's debug reasoning.
- The **AI models used**, with their configuration — **API keys are redacted**,
  so it's safe to share.
- The **dataset and model names** the session ran against.

The same **Export ▾** menu can also save just the chat transcript or the debug
log as Markdown or plain text, if that's all you need.

### Importing a shared trace

If someone sends *you* a psychological trace, use **Import** in the cockpit to
load it. It appears in your ProcessOS as an **Investigation** marked with a
special icon, so you can pick up and replay their session locally.

## Troubleshooting

**A banner says Node or llama.cpp is missing.** You tried to use a capability
that needs an external tool. The banner links to install instructions for your
operating system — install the tool, then reload the console. Everything that
doesn't need that tool keeps working in the meantime.

**My work disappeared after restarting.** ProcessOS stores everything under the
folder named by `PROCESSOS_DATA_DIR` (default `./.processos-data`). If you start
it from a different directory, it looks in a different place. Set
`PROCESSOS_DATA_DIR` to a fixed path so your workspaces and investigations
persist.

**The console won't open at localhost:8090.** Check the address ProcessOS printed
when it started — you may have set `PROCESSOS_PORT` to something else, or another
program may be using the port. Restart with a free port, e.g.
`PROCESSOS_PORT=9000 ./processos`.

**A local model won't start.** Make sure llama.cpp is installed and, if needed,
set the **llama-server binary** path in Settings. Use **Logs** on the sidecar to
see why `llama-server` exited.
