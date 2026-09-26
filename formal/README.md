# Formal verification

Machine-checked models of nanobpm's semantics. This is epic #1224: TLA+ for
concurrency, distribution and state-machine invariants, and Lean 4 for proofs
about pure semantics (both land slice by slice).

```
formal/
├── tla/
│   ├── TokenFlow.tla         # single-instance token flow: gateways + join bookkeeping
│   ├── TokenFlow.tla         # single-instance token flow: gateways + join bookkeeping
│   ├── ZeebeTokenFlow.tla    # Zeebe reference token flow (#1240): correct Camunda-8 gateway/join semantics
│   ├── MC*.tla               # GENERATED process graphs for TokenFlow (from formal/corpus)
│   ├── ZMC*.tla              # GENERATED process graphs for ZeebeTokenFlow (from formal/corpus)
│   ├── raft/                 # RaftHandoff spec family (#1228): leadership handoff + reclaim
│   │   ├── RaftHandoff.tla   # fence-epoch register (ADR 0019), anchored to nano-server-raft::fence
│   │   └── RH*.tla           # models: fenced pass + a no-fencing split-brain violation
│   ├── snapshot/             # SnapshotReplay family: snapshot/compaction/replay-migration recovery
│   │   ├── SnapshotReplay.tla  # durable-world model of recovery (NoSilentRewind + FailClosed, #1229)
│   │   └── MCSnapReplay*.tla   # the shipped fail-closed recovery vs. the historical naive one
│   ├── specs/                # one <Name>.spec descriptor per spec family (the registry)
│   │   ├── TokenFlow.spec    # TokenFlow's constants/invariants/properties/expected/trace models
│   │   ├── ZeebeTokenFlow.spec # the Zeebe reference spec's registration
│   │   └── SnapshotReplay.spec # SnapshotReplay's invariants + expected verdicts (models in snapshot/)
│   ├── check.sh              # discovers specs/*.spec, runs TLC per model, compares with each spec's EXPECTED
│   ├── gen-traces.sh         # dumps TLC behaviours of the trace models to committed JSON fixtures
│   ├── trace/parse.mjs       # TLC -tool output -> trace fixture JSON
│   └── traces/<Spec>/*.json  # committed trace fixtures (replayed by engine-core/tests/trace_validation)
├── corpus/                   # single-source corpus generator (#1258): one graph -> .tla + BPMN+DI + scenario
│   ├── graphs/<Id>.json      # THE SOURCE: nodes/edges/start + which spec families (MC*/ZMC*) it emits
│   ├── generate.mjs          # generator; --check is the drift guard run by check.sh
│   ├── bpmn/<Id>.bpmn         # GENERATED BPMN 2.0 XML with a BPMNDI diagram (DI)
│   └── scenarios/<Id>.json    # GENERATED scenario (job-completion order, message correlation, timer ticks)
├── lean/                     # Lean 4 reference semantics + differential fuzz (see below)
│   ├── lean-toolchain        # pinned Lean version (elan reads this)
│   ├── lakefile.lean         # Lake project: one lib target per slice + the feelfuzz exe
│   ├── feel-diff.sh          # build Lean, generate a corpus, check Rust against it
│   ├── Feel/                 # FEEL reference semantics + generator (this slice, #1230)
│   ├── Replay/               # replay determinism (#1231) — adds files here only
│   └── Roundtrip/            # processos IR ⇄ BPMN round-trip (#1232) — adds files here only
└── parity/                   # Zeebe parity coverage matrix (see below)
    ├── zeebe-pin.json        # the Zeebe commit the matrix is derived from
    ├── fetch-zeebe.sh        # sparse-fetches the pinned sources
    ├── extract.mjs           # Zeebe sources -> zeebe-surface.json (derived, never hand-edited)
    ├── coverage.json         # maps every cell to Nano evidence, a gap, or out-of-scope
    ├── gaps.json             # the gap ratchet baseline: the only cells a gap rule may claim
    └── check.mjs             # the CI guard + coverage report
```

## Running

You need Java 11+ (CI uses Temurin 21). Nothing else to install: on first run,
`check.sh` fetches the pinned `tla2tools.jar` into
`~/.cache/nanobpm-formal/` and verifies its SHA-256.

```bash
formal/tla/check.sh                      # every model (a few seconds)
formal/tla/check.sh MCChainedInclusive   # one model
```

CI runs the `formal (tlc)` job whenever `formal/**`, `engine-core/src/**`,
`engine-core/tests/**`, `engine-core/examples/**` or `engine-core/Cargo.toml`
changes. The job runs the
TLA+ model check, the parity matrix steps described below, and the Lean build +
FEEL differential fuzz described under "Lean 4 layer". The job is skip-tolerant:
it is required only when it runs, so a PR that touches none of those paths skips
it cleanly.

To read a counterexample trace, keep the logs:

```bash
FORMAL_LOG_DIR=/tmp/tlc formal/tla/check.sh MCParallelJoinMultiArrival
# /tmp/tlc/MCParallelJoinMultiArrival.{cfg,log}
```

A model that does not match its expectation prints its full TLC log anyway.

## `TokenFlow.tla`

This models the engine-core drain loop (`engine-core/src/engine/mod.rs`) for a
single process instance in a single scope:

| Spec | Engine |
|---|---|
| `pending` (a bag of flows being taken) | the `Step::Activate` queue, drained to empty per command |
| `waiting` | tokens parked on wait-state tasks (job completion is `CompleteTask`) |
| `TakeFlows` | `take_flow`: a flow into a join is counted when it is taken (`ParallelJoinTokenArrived`), as in Zeebe |
| `joinTokens` | `join_flow_arrivals` (per incoming flow, parallel and inclusive) |
| `ArriveJoin` | `activate_join`, the guard for both join kinds (`ParallelJoinOpened` / `Fired`) |
| `HasActivePathTo`, `LiveSources`, `PathReaches` | `has_active_path_to`, `path_reaches_join` (Zeebe's `hasActivePathToTheGateway`) |
| `CompleteInstance` | `complete_finished_instances` |

The model deliberately covers more behaviours than the engine can produce. It
drains the queue in any order and picks gateway branches freely instead of evaluating conditions. Each *safety* property (the
invariants and deadlock freedom) checked against this superset therefore also
holds for the engine's deterministic choices. `Termination` does not transfer
that way: it assumes fair routing choices, and real condition data can keep
choosing a loop branch forever. Read it as "the graph allows every instance to
finish", not "the engine always finishes".

The properties checked:

- `JoinBookkeepingCoherent`: an open join holds tokens, a join holding tokens
  is open unless an activation into it is still queued, tokens sit only on its
  own incoming flows, and a parallel join never has every incoming flow fed
  without an activation on its way to fire it. This is the class behind the
  missing-`ParallelJoinReset` bugs.
- `ParallelJoinWaitsForEveryFlow`: a parallel join fires only after every
  incoming flow has delivered, matching BPMN and Zeebe.
- `NoStuckInstance`: a settled instance with no runnable task has no open join.
- TLC's deadlock check, which reports any state where nothing can happen and
  the instance has not completed. `violates:` models run without it (see
  below); in this spec such a state is always a `NoStuckInstance` and
  `Termination` violation, so nothing is lost.
- `JoinFiresAtMostOnce`: a join fires at most once per instance. This is
  guarded by `Acyclic`, which the spec derives from the graph, since a rework
  loop legitimately re-fires a join. An acyclic graph that piles several
  tokens onto a join's inputs (not 1-safe, a BPMN lack-of-synchronization)
  also violates it, and that is intended: it is a finding about the model.
- `Termination`: every instance eventually completes, under the fairness
  in `TokenFlow.tla`'s `Fairness`. Every drain action is strongly fair *per
  routing choice*, so a gateway reached infinitely often
  eventually takes each branch. That is the "fair data" assumption of
  workflow-net soundness. Task completion is strongly fair too, because a task
  can only complete in a settled state, which is intermittent. Instance
  completion is weakly fair. A loop with an exit therefore terminates, and a
  loop without one is reported as a livelock.

The following are out of scope for now: sub-process scopes, boundary and
intermediate events, incidents, listeners and multi-instance. Each one is a
future extension of this spec.

## `ZeebeTokenFlow.tla` — the Camunda 8 reference, and the Nano ⇔ Zeebe refinement

Nano is a **strict superset of Camunda 8**: everything Camunda supports must
behave *exactly* as Zeebe does — on Camunda's surface there is no tolerated
divergence, a difference is a Nano defect. `ZeebeTokenFlow.tla` (#1240) is the
reference that pins that claim down. It re-derives the gateway/join token flow
**directly from `zeebe/engine`**, not from Nano, so a Nano-vs-Zeebe divergence
surfaces as a failed refinement rather than being defined away. Each rule cites
its Zeebe source: `canActivateParallelGateway` (a parallel join waits for every
*distinct* incoming flow, #1233), `canActivateInclusiveGateway` +
`hasActivePathToTheGateway` (an inclusive join is evaluated only on arrival,
#1241), `cleanupSequenceFlowsTaken` (one taken record consumed per incoming
flow on firing, surplus kept — the "Tetris" principle). Its **observable
projection** is drawn only from the shared vocabulary — each observable variable
(`taken`, `active`, `done`) maps to a Zeebe exporter record (`SEQUENCE_FLOW_TAKEN`,
`ELEMENT_ACTIVATED`, `ELEMENT_COMPLETED`), since Zeebe's internal bookkeeping is
visible only through what it exports. Alongside these the module carries a small,
explicitly marked set of **auxiliary** variables — control state derivable from
those records (`queue`, `open`) and pure ghost bookkeeping (`fired`, `earlyPar`) —
that exist only to drive the transition relation and state invariants and are
**not** part of the observable projection (see the module's vocabulary-rule
header for the per-variable justification).

It registers through the multi-spec harness like any other family
(`specs/ZeebeTokenFlow.spec`, model glob `ZMC*.tla`, its own
invariants/properties/expected). The `ZMC*` corpus is a representative sample —
parallel sync, inclusive sync, the arrival-time inclusive-join guard (#1241),
and a not-1-safe surplus — that TLC confirms the reference reproduces with the
same verdicts (and the same state counts) as the matching `MC*` models. Each
`ZMC*` and its `MC*` twin are **generated from one shared graph source**
(`formal/corpus/`, #1258), so the twins can no longer drift apart.

**The refinement (slice 2).** `TokenFlow.tla` must be trace-equivalent to
`ZeebeTokenFlow` on every Camunda-expressible model. Every action other than a
join arrival (pass-through, task activation, end, split routing) is structurally
identical between the two specs, so the join activation decision is the *sole*
refinement obligation. `TokenFlow.tla` discharges it with the `RefinesZeebe`
invariant, checked over the **whole `MC*` corpus**: at every reachable state it
asserts this spec's join-firing decision equals the Zeebe reference decision,
which it obtains by instancing `ZeebeTokenFlow`'s pure guard predicates
(`ZeebeJoinReady`). Because the Zeebe guard lives *only* in `ZeebeTokenFlow.tla`,
an edit that drifts `TokenFlow`'s guard away from Zeebe fails `RefinesZeebe`
until the reference — and hence the parity claim — is updated too. Every
historical divergence is a filed parity issue; the first, the arrival-time
inclusive-join guard, is #1241 (closed). With it landed, `RefinesZeebe` holds on
the entire corpus, so there is no open divergence to file.

The differential *runtime* half of #1240 builds on this static formal proof. Its
first piece has landed: a **single-source corpus generator** (`formal/corpus/`,
#1258) — one graph description per model produces its `MC*`/`ZMC*` `.tla`, its
BPMN XML with a DI diagram, and its scenario script (job-completion order,
message correlation, timer ticks), so the previously hand-maintained MC/ZMC
twins are now generated from one source (see
[Single-source corpus](corpus/README.md)). The remaining pieces — a
Zeebe-exporter trace normaliser, the one-driver/two-backend scenario runner
(Camunda v2 REST against Nano and a live Camunda 8), and the executable
differential oracle + extension register — extend it to executable traces and
land in their own follow-up slices (they need a Zeebe/Camunda-8 runtime this
proof does not).

## Expected outcomes and known defects

The `SPEC_EXPECTED` table in a spec's descriptor (`formal/tla/specs/<Name>.spec`)
records the expected outcome for each model:
`pass`, or `violates:<P1>,<P2>,...`, the **exact** set of invariants and
properties TLC must report as violated. Every property not listed is thereby
proven to hold for that model. A `violates:` model runs with TLC's `-continue
-deadlock`, so TLC explores the whole state space whatever order it reaches
violations in (a stuck state still shows up, as `NoStuckInstance` and
`Termination`). A `violates:` entry records one of two things:

- **A known engine defect** the model reproduces, linked to its issue. The spec
  models the engine **as it is**, defects included. A PR that fixes the defect
  in Rust must also update the spec to model the fixed behaviour; the model's
  verdict then changes, and `check.sh` fails until the entry is updated. The
  fixed behaviour therefore stays guarded.
- **A deliberately unsound graph**, where the violation is the correct verdict.
  `MCParallelJoinMultiArrival` puts two tokens on one incoming flow of a
  parallel join and one on the other. The join fires once, and, as in Zeebe,
  the surplus token waits forever (`NoStuckInstance,Termination`). It no
  longer fires early: before #1233 the engine counted arrivals rather than
  distinct incoming flows, and the entry also listed
  `ParallelJoinWaitsForEveryFlow`. `MCParallelJoinSurplus` takes every
  incoming flow twice; the join keeps the surplus between firings (Zeebe's
  "Tetris" principle), fires twice (`JoinFiresAtMostOnce`), and the instance
  completes. `MCInclusiveJoinSurplus` is the inclusive-join version. As in
  Zeebe, an inclusive join is only evaluated when a token arrives (#1241): when
  the surplus arrives last it fires the join again (`JoinFiresAtMostOnce`), but
  when it arrives first it waits forever (`NoStuckInstance,Termination`).
  `MCInclusiveDivergentPath` is the same verdict for a sound-looking graph: the
  competing branch leaves through an exclusive gateway, so the join is never
  re-evaluated and waits, as in Zeebe. Before #1241 a quiescence sweep
  re-evaluated waiting joins, and both models completed.

TLC cannot see the Rust code by itself. Trace validation (#1226) now links the
two for the parallel-only corpus: TLC-generated behaviours are replayed against
`Engine::apply_command` in `engine-core/tests/trace_validation` (see [Trace
validation](#trace-validation) below). For the model families it does not yet
anchor (condition-routed graphs, and the specs still to land), forcing the spec
update when the engine changes stays a review responsibility.

`check.sh` also fails if a model file matched by a spec's `SPEC_MODELS_GLOB` has
no `SPEC_EXPECTED` entry, if an entry has no model, if a model is claimed by more
than one spec, if a committed `*.tla` that `EXTENDS` a registered spec base is
claimed by none, or if TLC prints a warning.

## Adding a spec family

Each spec family (TokenFlow, ZeebeTokenFlow and SnapshotReplay, and the future
JobLease and RaftHandoff) registers itself through a self-contained
descriptor, so a new spec is added by **creating files in its own path** — never
by editing `check.sh` or another spec's descriptor.

1. Write the base module `formal/tla/<Name>.tla` (or under a subdirectory of
   your choosing) and its concrete model files.
2. Create `formal/tla/specs/<Name>.spec` — a shell fragment `check.sh` sources
   in a fresh subshell, so its `SPEC_*` variables are private to your spec. Set:
   `SPEC_NAME` (the base module every model `EXTENDS`), `SPEC_MODELS_DIR`
   (directory of the models, relative to `formal/tla`, `.` for the root),
   `SPEC_MODELS_GLOB` (glob selecting your models — must not overlap another
   spec's), `SPEC_CONSTANTS` (the `.cfg` CONSTANTS lines), `SPEC_INVARIANTS`,
   `SPEC_PROPERTIES`, and `SPEC_EXPECTED` (one `"<Model> pass"` /
   `"<Model> violates:<P1>,..."` row per model). Copy `TokenFlow.spec` as the
   reference. Optionally set `SPEC_TRACE_MODELS` (see below).
3. Run `formal/tla/check.sh` — your spec is discovered and checked with its own
   constants/invariants/properties; the drift guard is scoped to your spec, so
   it never forces your models into TokenFlow's table or vice versa.

`check.sh MCFoo` still runs a single model by name across all specs.

## Trace validation

Trace validation anchors a spec to the real engine (#1226, the epic's anti-drift
rule): TLC emits a spec behaviour, and a Rust test replays it against
`Engine::apply_command`, failing on any divergence.

- `formal/tla/gen-traces.sh` runs TLC over each model listed in a descriptor's
  `SPEC_TRACE_MODELS`, dumping the shortest completing behaviour (a witness
  invariant `~(SPEC_TRACE_DONE)` forces TLC to emit it) plus the TLC-evaluated
  process graph (`SPEC_TRACE_GRAPH`, in the spec's own graph vocabulary) to a
  committed fixture `formal/tla/traces/<Spec>/<Model>.json`. `SPEC_TRACE_DONE`
  and `SPEC_TRACE_GRAPH` are descriptor-supplied (required whenever
  `SPEC_TRACE_MODELS` is non-empty), so the generator is spec-agnostic — a
  sibling family with different state/graph vocabulary supplies its own. The
  fixtures are a derived artifact: `gen-traces.sh --check` regenerates them and
  fails on drift, and `check.sh` runs it on a full model-check (when node is
  available) so the `formal (tlc)` CI job enforces it.
- `engine-core/tests/trace_validation.rs` reads those fixtures, rebuilds each
  model as a real engine process, drives it to quiescence, and asserts the
  engine's observable **milestone multiset** (tokens on flows, task wait states,
  join firings, completion) equals the spec's. A divergence fails the test — no
  tolerated mismatch, no retries. The corpus is restricted to parallel-only,
  routing-deterministic models whose milestone multiset is invariant under
  interleaving, so multiset equality is an exact check. Models with two distinct
  flows sharing endpoints (`MCParallelDuplicateFlows`) are model-checked but
  excluded from the anchored corpus: the engine's `SequenceFlowTaken` event has
  no per-flow identity, so their milestone multiset cannot distinguish the two
  same-endpoint flows and the anchor would be unsound.

**Reuse entry point (for sibling specs #1227, #1240, …).** The replay driver is
spec-agnostic and lives in `engine-core/tests/trace_validation/harness.rs`. A
new spec anchors its own models by implementing the `TraceMapping` trait and
calling `harness::validate`:

```rust
#[path = "trace_validation/harness.rs"]
mod harness;
use harness::{Fixture, Milestone, TraceMapping, validate};

// build the engine process from the spec graph; project engine Events onto
// the shared Milestone vocabulary:
pub trait TraceMapping {
    fn process_id(&self, fixture: &Fixture) -> String;
    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition;
    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone>;
}
// validate(&mapping, &fixture) -> Result<(), String>   // Err on divergence
```

`engine-core/tests/trace_validation/token_flow.rs` is the reference
`TraceMapping` implementation.

### Anchoring a non-engine subsystem (SnapshotReplay)

Not every spec models the token engine. `SnapshotReplay` (#1229) models the
durable snapshot / compaction / cold-archive + replay-migration recovery
protocol behind incidents #1065–#1071, which lives in
`server/crates/nano-server-storage/src/seglog.rs`, not in `Engine`. TLC trace
fixtures replayed through `Engine::apply_command` cannot reach that subsystem, so
this spec omits `SPEC_TRACE_MODELS` and anchors on the storage side instead: the
`seglog::tests::snapshot_replay_conformance` module builds each durable-world
class the spec's `RecoverOutcome` classifies and drives it through the real
`Journal::open_segmented` → `recover` path, asserting exactly the spec's two
safety invariants — `NoSilentRewind` (a successful recovery reconstructs the full
`[0, total_events)` history: no lost instance, no rewound key generator) and
`FailClosed` (a genuinely unreconstructable world — a pruned gap or an
unreadable / `UnknownVariant` frame — rejects with a typed error). That test is
the anti-drift anchor; keep it and `SnapshotReplay.tla` in sync in the same PR.

## Adding a model to TokenFlow

The `MC*.tla` (and `ZMC*.tla`) models are **generated** from a single graph
source (`formal/corpus/`, #1258) — do not hand-write or hand-edit them (a
hand-edit fails the corpus drift guard in `check.sh`). To add a model:

1. Add a graph source `formal/corpus/graphs/<Id>.json` (nodes, edges, start,
   and a `families` entry naming the `MC*` module and, if the graph is also a
   Zeebe reference, the `ZMC*` module). Flows have their own ids, as in the
   engine, so two distinct flows may share endpoints (`ParallelDuplicateFlows`).
   Such a duplicate-endpoint model is model-checked but **not** trace-anchored:
   the engine's `SequenceFlowTaken` event carries no per-flow identity, so its
   observable milestone multiset cannot distinguish the two same-endpoint flows
   (see `SPEC_TRACE_MODELS` in `TokenFlow.spec`). Run
   `node formal/corpus/generate.mjs` and commit the generated `.tla` (+ BPMN +
   scenario). See [Single-source corpus](corpus/README.md).
2. Add a row to `SPEC_EXPECTED` in `formal/tla/specs/TokenFlow.spec` (and
   `specs/ZeebeTokenFlow.spec` for a `ZMC*`) with its expected outcome. There
   are no hand-written `.cfg` files. `check.sh` generates the same config, with
   every property, for every model, so no model can skip a property.

If the model finds a violation, confirm it against the real engine with a red
Rust test before recording it. The model may simply be wrong.

## Keeping the spec honest

The spec is hand-written, so it can drift from the Rust code. Trace validation
(#1226) ties the two together for the parallel-only corpus by replaying
TLC-generated behaviours against `Engine::apply_command` (see [Trace
validation](#trace-validation)). Where a model is not yet trace-anchored, any
change to the drain loop, the join functions or `path_reaches_join` should
update `TokenFlow.tla` in the same PR.

## Lean 4 layer

`formal/lean/` is a single [Lake](https://github.com/leanprover/lean4) project
holding the Lean 4 reference semantics and proofs. It is the wave-0 seam for the
Lean slices of epic #1224: the shared `lakefile.lean` and `lean-toolchain` are
authored once (#1230) and **pre-declare one library target per slice**, so each
sibling only ADDS files inside its own subdirectory and never edits the shared
lakefile or a shared barrel file.

| Lib target | Directory | Slice |
|---|---|---|
| `Feel` | `formal/lean/Feel/` | FEEL reference semantics + differential fuzz (#1230) |
| `Replay` | `formal/lean/Replay/` | replay determinism (#1231) |
| `Roundtrip` | `formal/lean/Roundtrip/` | processos IR ⇄ BPMN round-trip (#1232) |

`lakefile.lean` also declares the `feelfuzz` executable (`Feel.Fuzz`). Every
target is a `@[default_target]`, so `lake build` builds them all. The package
sets `warningAsError := true`, so any Lean warning fails the build — keep new
code warning-clean.

**Per-slice convention.** Add your `.lean` files under your own directory
(`Feel/`, `Replay/`, `Roundtrip/`); the matching lib target globs its
submodules automatically. Do not edit `lakefile.lean`, `lean-toolchain`, or
another slice's directory.

### Running

`elan` manages the Lean toolchain and installs the pinned version from
`lean-toolchain` on first use:

```bash
# Pin the elan installer to a specific commit (v4.2.4) and verify its SHA-256
# before executing it — never pipe an unpinned `master` script straight into a
# shell (mirrors the CI installer in .github/workflows/ci.yml).
ELAN_INIT_SHA256=a620ff1641616222c8d37c54845492004bb84d6877cdbc944dd65c1aa685bf53
curl -fsSL https://raw.githubusercontent.com/leanprover/elan/227caca133724d5516bee25c2aeb3e609478f2d8/elan-init.sh -o /tmp/elan-init.sh
echo "${ELAN_INIT_SHA256}  /tmp/elan-init.sh" | sha256sum -c -
sh /tmp/elan-init.sh -y --default-toolchain none
export PATH="$HOME/.elan/bin:$PATH"
cd formal/lean && lake build          # build every Lean target
```

### FEEL differential fuzz (#1230)

The FEEL slice (`formal/lean/Feel/`) is a total reference evaluator for the FEEL
subset that `engine-core/src/feel` implements. Per the epic's anti-drift rule,
the reference is tied to the Rust implementation by a **differential fuzz**:

- `Feel/Semantics.lean` is the reference evaluator, faithful to
  `engine-core/src/feel/eval.rs` (three-valued and/or with short-circuit,
  `=`/`!=` that always yield a boolean — cross-type is `false`, never null —
  ordering (`<`/`<=`/`>`/`>=`) that is a type *error* on incomparable operands
  (including `null`), `if` on a non-bool condition → null, etc.).
- `Feel/Gen.lean` is a type-directed generator that emits FEEL expressions with
  bound contexts, keeping all numeric values exact integers within the
  f64-exact range so number formatting cannot drift.
- The `feelfuzz` exe prints a TSV corpus of `expr⇥ctx⇥reference-outcome`.
- `engine-core/examples/feel_diff.rs` re-evaluates each row with the Rust
  evaluator and asserts the canonical outcome is **byte-identical**. Any
  divergence exits non-zero — there is no tolerated mismatch and no retry.

Run the whole loop (build Lean → generate → check Rust) with the driver:

```bash
formal/lean/feel-diff.sh            # 3000 cases (override: FEEL_FUZZ_CASES or first arg)
formal/lean/feel-diff.sh 20000      # more cases
```

To convince yourself the harness actually bites, perturb one arm of
`Feel/Semantics.lean` (e.g. make `add` compute `a - b`), rerun `feel-diff.sh`,
watch it fail with concrete diverging rows, then revert.

## Zeebe parity coverage matrix

Nano must behave exactly like Camunda 8 wherever Camunda defines behaviour.
The parity suite (#1240) needs a definition of "complete" that nobody writes
by hand. This matrix provides it (#1245). `extract.mjs` reads the Zeebe
sources at the commit pinned in `zeebe-pin.json` and lists every behaviour
Zeebe declares, one **cell** per behaviour, in `zeebe-surface.json`:

| Family | One cell per | Read from |
|---|---|---|
| `element:<Class>` | supported BPMN element | `FlowElementValidator.SUPPORTED_ELEMENT_TYPES` |
| `event:<position>:<definition>` | supported event definition per position | `SUPPORTED_*` lists in the boundary, intermediate-catch and sub-process validators; `*Behavior` classes in the end and intermediate-throw event processors |
| `lifecycle:<BpmnElementType>:<command>` | each lifecycle command (activate, complete, terminate, continue-terminating, complete-execution-listener), plus any `child-*` hook the processor implements | commands from `BpmnStreamProcessor.processEvent`; element types from `BpmnElementProcessors`; hooks from the processor interfaces (and the interfaces they extend), following each processor's `extends` chain |
| `guard:<method>:<message>` | rejection branch of the state-transition guard | `Either.left` in `ProcessInstanceStateTransitionGuard` |
| `incident:<ErrorType>` | incident type | `ErrorType` |
| `intent:<Record>:<INTENT>` | record intent | every `*Intent` enum in `protocol/record/intent` |
| `validation:<Validator>:<message>` | deploy-time rejection message | `addError` calls in the bpmn-model and engine deployment validators |
| `rejection:<Class>:<RejectionType>` | command rejection a processing-layer class produces: a processor, or a validator/helper that builds the rejection a processor writes | `RejectionType.X` uses under `engine/processing` (comparisons excluded), checked against the SBE schema |

Each cell records the source lines it was read from. The extractor fails
loudly when an anchor it relies on moves or changes shape. It also fails when
an anchor appears in a form it does not read (for example
`SUPPORTED_ELEMENT_TYPES.addAll`, or a processor hook missing from
`HOOK_TRANSITIONS`), and when two different messages would collapse into one
cell id. An upstream refactor therefore stops extraction; it never silently
shrinks the matrix.

Known extraction gap: Zeebe declares no list of supported process-level start
event types (`StartEventValidator` only checks their count and form), so those
have no cells yet.

### Mapping cells to evidence

`coverage.json` holds ordered rules. The first rule whose `match` fits a cell
claims it, and `*` in a `match` matches any run of characters. Each rule has
one status:

| Status | Required | Meaning |
|---|---|---|
| `parity` | `evidence`: fixtures in `engine-core/tests/conformance/corpus/` | Nano's verdict is asserted equal to a Zeebe verdict captured in the fixture (`accept` or `reject`; a `diverge` fixture is not parity). Every claimed cell must be listed in one of the fixtures' `<!-- zeebe-cells: … -->` comment |
| `nano-tested` | `evidence`: `path::test_fn` | a Nano `#[test]` (not `#[ignore]`d) exercises the behaviour, but not against a Zeebe oracle. Every claimed cell must be listed in a `// zeebe-cells: …` line among the comments and attributes directly above the test fn |
| `gap` | `issue`, `note` | no evidence yet; the issue closes it. Only cells in the `gaps.json` baseline may be gaps (see below) |
| `out-of-scope` | `issue`, `note` | the cell has no Nano meaning (for example, partition-internal records). Use sparingly |

`check.mjs` fails on any of these:

- an unmapped cell, including new cells from a Zeebe bump
- a rule that claims no cell
- evidence that does not resolve (a renamed test, a missing fixture, a
  fixture or test that does not declare the cell, or one that declares a cell
  not in the surface)
- a malformed rule
- a surface extracted at a different commit than the pin
- a `coverage.json` whose `reviewedAt` is not the pinned commit, so every bump
  is an explicit review of the surface diff
- a gap cell that is not in the `gaps.json` baseline, a baseline entry that is
  no longer a gap (it gained evidence or left the surface), or an unsorted
  baseline

`gaps.json` is a ratchet. It freezes the cells that were gaps when the matrix
landed, so a broad `gap` rule such as `intent:*` cannot absorb a cell that a
Zeebe bump adds: that cell needs evidence or an `out-of-scope` rule. When cells
gain evidence, `node formal/parity/check.mjs --update-gaps` drops them from the
baseline. It only ever removes entries, and CI runs the guard with
`--base-gaps` against the target branch's `gaps.json`, rejecting any id the PR
adds, so the baseline shrinks toward empty.

It prints the per-family counts and, in CI, adds them to the job summary.
Moving cells up the ladder, from `gap` to `nano-tested` to `parity`, is the
work of the parity suite.

```bash
node formal/parity/check.mjs                 # guard + report
node --test formal/parity/*.test.mjs         # extractor and guard tests
```

CI regenerates `zeebe-surface.json` from the pinned sources and fails if the
result differs from the committed file, or if the surface or `gaps.json` is not
tracked.

### Bumping the Zeebe pin

1. Update `ref` and `sha` in `zeebe-pin.json`. Also update `paths` if the
   sources moved.
2. `node formal/parity/extract.mjs "$(formal/parity/fetch-zeebe.sh)"`. The
   extractor refuses to run on a checkout that is not at the pinned commit.
3. If extraction fails, a Zeebe refactor moved an anchor. Update
   `extract.mjs` and its tests.
4. `node formal/parity/check.mjs`. Map every unmapped cell and every new gap
   cell it reports to evidence or `out-of-scope`, and delete any rule it
   reports as dead. Run `--update-gaps` to drop cells the bump removed. Then
   set `reviewedAt` in `coverage.json` to the new commit.
5. Review the `zeebe-surface.json` diff. Removed or renamed cells are Zeebe
   behaviour changes that Nano may need to follow.
