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
