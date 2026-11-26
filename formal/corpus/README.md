# Single-source corpus (`formal/corpus/`)

One graph description per process graph is the **single source** for every
downstream artifact of that graph (#1258, #1240 slice 3). It removes the
hand-maintained `MC*.tla` / `ZMC*.tla` twin duplication: the parallel diamond,
inclusive diamond, inclusive divergent path and parallel-join surplus each
existed as two byte-for-byte identical graphs (one `EXTENDS TokenFlow`, one
`EXTENDS ZeebeTokenFlow`), which could silently drift apart. Now there is one
graph, and the generator emits both.

```
formal/corpus/
├── graphs/<Id>.json     # THE SOURCE: nodes, edges, start, and per-family module
├── generate.mjs         # generator: graph -> .tla (+ .bpmn + scenario); --check drift guard
├── generate.test.mjs    # structural + drift tests (node --test, no JVM)
├── bpmn/<Id>.bpmn        # GENERATED: BPMN 2.0 XML with a BPMNDI diagram (DI)
└── scenarios/<Id>.json   # GENERATED: job-completion order, message correlation, timer ticks
```

## The graph source (`graphs/<Id>.json`)

```jsonc
{
  "id": "ParallelDiamond",
  "start": "S",
  "nodes": { "S": "start", "P1": "and", "A": "task", "B": "task", "P2": "and", "E": "end" },
  "edges": [ { "id": "f1", "from": "S", "to": "P1" }, ... ],
  "families": {
    "TokenFlow":      { "module": "MCParallelDiamond",  "comment": ["S -> P1 -> {A, B} -> P2 -> E"] },
    "ZeebeTokenFlow": { "module": "ZMCParallelDiamond", "comment": ["Zeebe reference ..."] }
  }
}
```

- `nodes` maps a node id to its kind: `start`, `end`, `task`, `and`
  (parallelGateway), `or` (inclusiveGateway), `xor` (exclusiveGateway).
- `edges` are the sequence flows, each with its own id — so two distinct flows
  may share endpoints (`ParallelDuplicateFlows`), exactly as in the engine.
- `families` picks the spec families this graph is emitted into, and names the
  module for each. `TokenFlow` modules are `MC*` (the nano spec), `ZeebeTokenFlow`
  modules are `ZMC*` (the Camunda-8 reference). A graph shared by both emits one
  `.tla` per family from the same nodes/edges, so the twins can never diverge.
  The per-family `comment` is the module header prose (the Zeebe citation for a
  `ZMC*`, the shape sketch for an `MC*`).

## Generated artifacts

Per graph, `generate.mjs` writes:

- `formal/tla/<module>.tla` for each family — `EXTENDS` the family base and
  defines `MCNodes`, `MCKind`, `MCStart`, `MCEdges`, `MCFlows`, `MCSrc`, `MCTgt`,
  the exact vocabulary the spec bases expect. These stay registered through the
  #1226 per-spec registry with **no edit to any descriptor**: `specs/TokenFlow.spec`
  globs `MC*.tla`, `specs/ZeebeTokenFlow.spec` globs `ZMC*.tla`, so
  `formal/tla/check.sh` keeps model-checking them.
- `formal/corpus/bpmn/<Id>.bpmn` — BPMN 2.0 XML **with** a generated
  `<bpmndi:BPMNDiagram>` (a `BPMNShape` per node, a `BPMNEdge` with waypoints per
  flow) laid out left-to-right by a layered pass, so it renders for humans. Tasks
  are `serviceTask`s whose job type is the node id. A diverging gateway conditions
  every outgoing flow and declares **no** `default` (which, with every branch
  conditioned, would be provably unreachable): an inclusive (`or`) split makes
  every branch `=true` so all are taken, and an exclusive (`xor`) split makes
  exactly one branch `=true` and the rest `=false` so one deterministic route is
  taken — keeping the model executable while faithfully representing every branch.
- `formal/corpus/scenarios/<Id>.json` — the scenario script: a job per
  `serviceTask`, a deterministic `jobCompletionOrder`, and `messageCorrelation` /
  `timerTicks` slots (empty until a graph introduces message/timer elements).
  This is the input contract for the scenario driver (#1240 slice 5).

## Regenerating and the drift guard

```bash
node formal/corpus/generate.mjs           # rewrite every artifact
node formal/corpus/generate.mjs --check    # fail on drift (the CI guard)
node --test formal/corpus/*.test.mjs       # structural + drift tests
```

The committed `.tla` / `.bpmn` / scenario files are a **derived artifact**. The
generator is the source of truth, so `formal/tla/check.sh` runs
`generate.mjs --check` on a full model-check (when node is present, as on the
`formal (tlc)` CI runner): a forgotten regeneration — an edited graph, or a
hand-edited generated `.tla` — fails CI instead of shipping a silent twin
divergence. This is the same drift discipline as `formal/parity` and
`gen-traces.sh`.

## Adding or changing a corpus graph

1. Edit (or add) `formal/corpus/graphs/<Id>.json`. For a graph both specs must
   agree on, list both families; for a nano-only shape, list only `TokenFlow`.
2. Run `node formal/corpus/generate.mjs` and commit the regenerated artifacts.
3. Add the new model(s) to the matching descriptor's `SPEC_EXPECTED`
   (`specs/TokenFlow.spec` / `specs/ZeebeTokenFlow.spec`) with the expected
   verdict — the generator does not touch the registry, so a new model without an
   `SPEC_EXPECTED` row fails `check.sh` (that is the intended registration gate).
4. Run `formal/tla/check.sh` to model-check and to run the drift guard.

## Bounded-exhaustive enumeration (#1246)

The hand-written graphs above found #1233 and #1241 by luck. `enumerate.mjs`
replaces luck with a bound: it enumerates **every** well-formed graph up to *N*
gateways (exclusive, parallel, inclusive, event-based) and *M* sequence flows,
with tasks (and message catch events) as wait states, reduced by isomorphism
(one graph per class, numbered canonically, id `Gen_<hash of the canonical
key>` — stable across bounds). Well-formedness and the reduction are documented
at the top of `enumerate.mjs`.

Every graph goes through the **same emitters** as the hand-written corpus — one
source, three artifacts:

- `tlaFor` → `MCGen_*` (TokenFlow) and `ZMCGen_*` (ZeebeTokenFlow);
- `bpmnFor(graph, { routing: 'variables' })` → BPMN with DI, whose diverging
  exclusive / inclusive gateways read their route from a start variable
  (`route_<gateway>`), and whose catch events subscribe to
  `<graph>_<node>` keyed by the `correlationKey` start variable. The route
  variable holds the gateway's **remaining choice sequence** (one entry per
  activation still to route — a parallel split can bring a token back to the
  same gateway, and each activation may choose differently); the condition
  reads the head (`route[1]`) and the scenario re-sets the variable to the
  suffix before each activation;
- `scenariosFor` → runtime scenario scripts (formal/parity/runtime format): one
  per **(route × command order) run**. A command-level executor of
  `TokenFlow.tla` (`Reference`) explores, after each settled step, every
  pending routing decision AND every possible next command (complete a job,
  correlate a message), as TLC explores every interleaving; its outcome is the
  scenario's `expect` — including the completed-element multiset, so an
  event-based gateway resumed on the wrong catch fails the oracle even when
  completion and the variables match.

`exhaustive.mjs` runs them:

```bash
node formal/corpus/exhaustive.mjs --bound ci --tlc                 # formal (tlc) job
node formal/corpus/exhaustive.mjs --bound ci --backend nano        # parity-runtime job
node formal/corpus/exhaustive.mjs --bound nightly --tlc --backend both --require-camunda   # nightly
node formal/corpus/exhaustive.mjs --gateways 3 --flows 8 --kinds and,or --tlc   # any bound
node formal/corpus/exhaustive.mjs --bound ci --out /tmp/gen        # just write the artifacts
```

The bound is configurable; the named bounds live in `bounds.json` (`ci` for the
PR gate, `nightly` for `.github/workflows/parity-exhaustive-nightly.yml`, which
also takes a manual dispatch with overrides). Checks per graph:

| phase | divergence |
| --- | --- |
| `tlc` | `RefinesZeebe` violated, or the nano spec's verdict ≠ the Zeebe reference's verdict (`tlc-refines`, `tlc-verdict`); a reference run that gets stuck where TLC reports no stuck state (`reference-vs-tlc`) |
| `nano` | the engine-wasm TestEngine's outcome (completion, jobs created, variables) ≠ the reference outcome |
| `spec-oracle` | the nano observation is NOT a behaviour of `ZeebeTokenFlow` (#1261): its token-flow milestone multiset (taken flows, task wait states, completion) ≠ the generating run's. See below |
| `camunda` | live Camunda 8 ≠ the reference, or ≠ nano on the fields both expose (`differential`) — completing scenarios only (the bare-gateway backend observes an instance through completion) |

### The spec oracle (#1261)

Beyond the *differential* oracle (nano ≡ Camunda on the shared fields) and the
`expect` block (nano ≡ the reference outcome), the `spec-oracle` phase asserts
the **spec** half of #1261: the trace nano emits is a **behaviour of
`ZeebeTokenFlow.tla`**. `spec-oracle.mjs` projects both the generating run and
the nano observation onto the spec's observable milestone vocabulary
(`taken` → `from->to` flows, `active` → task wait states, `done` → completion)
and asserts the multisets are equal. The run a scenario is generated from is by
construction a spec behaviour (the `Reference` executor's `joinFires` is Zeebe's
arrival-time join guard), so an observation that differs from it is a trace that
is not a `ZeebeTokenFlow` behaviour — a parity defect on the token-flow surface.
The `fired` (join-firing) milestone is anchored separately: the `Reference`
records it (`joinsFired`) and the Rust trace-validation harness
(`engine-core/tests/trace_validation/zeebe_token_flow.rs`) ties it to both the
engine and the captured Zeebe goldens. The Nano-only surface that is *out* of
this oracle's scope is the extension register (`formal/parity/extensions.json`).


Command orders are driven one job at a time: `completeOneJob` completes exactly
one job of a type, so a run that holds two same-type jobs (a parallel split
through an exclusive merge) is still deterministic — multi-token runs are kept,
not skipped. Message steps use the driver's `publishMessage` op (a buffered
correlation: Camunda publishes with a TTL, so no race with the subscription the
previous step opens; nano's RTC makes it immediate). The publication endpoint
does not wait for correlation, so when resuming the catch activates exactly one
follow-on task — and no job of that type was already waiting — the step carries
a `settled.jobType` barrier: Camunda long-polls one activation of that type (the
job exists only once the message correlated) and fails it back for the driver's
own completion step, so the driver never advances past a still-buffered message.
A type barrier cannot acknowledge a correlation when a job of that type was
ALREADY waiting (Camunda's type-only activation could return the older job first)
or when the catch activates no job at all; then, if the instance survives, the
step carries `settled.ack` instead and the backend acknowledges the correlation
(a synchronous `/messages/correlation`, retried on a not-yet-open subscription)
before advancing.

**Any divergence fails the run and is shrunk.** Graphs are enumerated
smallest-first (flows, then gateways), so the first graph showing a divergence
class (its `signature`) is a minimal one within the bound; of its failing
scenarios the shortest is kept. `--record` appends these minimal cases to
`divergences.json`, the committed corpus of known divergences. Each committed
case needs a filed `issue` (an unfiled case fails the run and the unit tests);
a committed case that stops reproducing inside the bound fails as stale, so a
fix deletes it. A committed case is validated against **its own** graph and
scenario, not just its signature: field-based signatures can group unrelated
defects, so a case whose own reproducer stopped failing while another graph
still trips the signature fails as **obsolete** (re-record or delete it) rather
than ratcheting on a dead reproduction. Nano divergences are filed as their own
issues and fixed there, never inside the generator.

The generated scenarios are deterministic command scripts (fixed ids, fixed
order, unique correlation keys), so the same `--out` corpus can feed the
determinism CI command corpus (#1665).

## Differential fuzzing with shrinking (#1250)

The bounded-exhaustive run stops at a small bound. `fuzz.mjs` reaches beyond
it: **seeded random** well-formed graphs (the same skeleton vocabulary and
`wellFormed` rules as `enumerate.mjs` — exclusive, parallel, inclusive and
event-based gateways, service tasks, message catch events — but with more
gateways and flows than the nightly bound), each driven by a **seeded random
command order** (`randomRun`: a random walk through the reference's
(routing choice × command) tree — job completions in any order, message
correlations, per-activation gateway routing). Every scenario goes through the
same oracle as the exhaustive run (`exhaustive.mjs` `checkScenario`): the
reference outcome, nano, and live Camunda 8 when reachable.

```bash
node formal/corpus/fuzz.mjs --seed 42 --cases 200                        # nano only
node formal/corpus/fuzz.mjs --seed 42 --backend both --require-camunda   # + live Camunda 8
node formal/corpus/fuzz.mjs --seed 42 --max-gateways 7 --kinds and,or     # shape the generator
node formal/corpus/fuzz.mjs --replay-only                                # the committed corpus only
node formal/corpus/fuzz.mjs --seed 42 --labels task,timer --cancel-rate 0  # narrow the wait-state vocabulary
```

- **Extended vocabulary (#1673).** Beyond the exhaustive run's labels, a random
  wait-state edge may be a **timer** catch (`timer`, resumed by an `advanceTime`
  tick), an embedded **subprocess** wrapping one inner service task (`sub`,
  drawn collapsed with its own DI plane), or a service task carrying an
  interrupting **timer** / **message** boundary event (`btimer` / `bmsg`, whose
  outgoing flow joins the task's at a merge-capable target). A random walk also
  **cancels** the instance with probability `--cancel-rate` (default 0.05) per
  step (`cancelInstance`; a cancelled scenario is nano-only, like a stuck one).
  A scenario that **ticks** is nano-only too: the bare-gateway Camunda backend
  has no timer-readiness barrier, so a tick could overtake the timer it fires.
  A tick that re-activates a routed gateway is preceded by a `setVariables`
  step carrying that gateway's route suffix, and a message **boundary**
  correlation takes the same settlement barrier (`settled`) as a catch's.
  `--labels` picks the wait-state labels drawn (default: all). The Reference
  models each: a tick fires every due timer within the one command, one TRIGGER
  at a time in timer-key order, each drained before the next (#2024), an interrupting boundary
  terminates its task (never completed), the container completes when its inner
  end does. The bounded-exhaustive enumeration keeps its legacy vocabulary by
  default (`enumerateSkeletons({ labels })` opts in).

- **Deterministic from its seed.** Case *i* draws from its own PRNG seeded by
  `caseSeed(seed, i)`; the same seed produces byte-identical graphs, BPMN (with
  DI) and scenarios. No retries anywhere.
- **Shrinking.** A divergence is greedily reduced (`reductions`: drop a gateway,
  drop a flow, turn a boundary flow into a task one and any wait-state flow into a
  direct one — always staying well-formed)
  while some command order of the smaller graph (`shrinkRuns`: the first DFS
  orders plus fixed random walks) still shows the **same signature**; the result
  is 1-minimal, and its fewest-step reproducing scenario is kept.
- **Permanent corpus.** `--record` appends each minimal reproducer to
  `fuzz-corpus.json` (`--file-issues` also files a parity issue per case with
  `gh` and stamps it in). Every run — and the PR gate's `parity-runtime` job via
  `--replay-only` — replays the corpus first: a case with `"fixed": false` must
  still reproduce its signature (otherwise it fails as stale: flip it to
  `"fixed": true`), a case with `"fixed": true` must show no divergence at all,
  so a fixed reproducer is a regression guard forever. A case without a filed
  `issue` fails.
- **Nightly, never required.** `.github/workflows/parity-fuzz-nightly.yml` runs
  it against nano and a throwaway Camunda 8 container (seed = run number, or a
  dispatched seed); on a new divergence it files the issue and opens a PR
  committing the reproducer.
