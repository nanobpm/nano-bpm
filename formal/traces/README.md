# Zeebe differential traces (`formal/traces/`)

Part of the Zeebe 1:1 parity strategy (#1391), step 1. Each scenario here runs
on two engines:

- **the pinned Zeebe engine**: offline, in-memory `EngineRule`, no cluster, run
  by hand.
- **nano engine-core**: in CI, as part of the required
  `engine-core (clippy + test)` job.

Both runs are reduced to a **normalised record stream** using one shared spec,
and the two streams must be equal.

```
scenarios/<name>/process.bpmn   the model (MUST carry BPMN DI)
scenarios/<name>/script.json    the command script
normaliser.json                 THE normaliser spec (shared by Java and Rust)
goldens/PIN                     Zeebe commit the goldens were captured at
goldens/<name>.trace.json       normalised Zeebe record stream, per step
goldens/<name>.cells.json       touched cells + their raising sites
allowlist.json                  known divergences (issue-referenced)
capture.sh                      capture / refresh goldens (offline)
capture/src/...                 the JVM capture harness
../../engine-core/tests/zeebe_traces.rs   the nano runner (CI)
```

## Scenario format

`process.bpmn` is BPMN 2.0 XML with Zeebe extensions. It **must** include a
`BPMNDiagram`/`BPMNPlane` with a `BPMNShape` for every flow node and a
`BPMNEdge` for every sequence flow. Both loaders reject a model that lacks
these: the Rust loader in `load_scenario`, tested by
`scenario_without_di_is_rejected`, and the Java loader in `Scenarios.requireDi`.

`script.json` holds `{ "description": "...", "steps": [ ... ] }`. Each step is
one of the following:

| `op` | fields | Zeebe command |
| --- | --- | --- |
| `createInstance` | `bpmnProcessId`, `variables?` | PROCESS_INSTANCE_CREATION CREATE |
| `completeJob` | `jobType`, `variables?` | JOB COMPLETE (the job is activated first) |
| `failJob` | `jobType`, `retries`, `errorMessage?` | JOB FAIL |
| `completeUserTask` | `elementId`, `variables?` | USER_TASK COMPLETE |
| `publishMessage` | `name`, `correlationKey`, `variables?` | MESSAGE PUBLISH |
| `advanceClock` | `millis` | clock advance, then fire any due timers |
| `resolveIncident` | `elementId` | INCIDENT RESOLVE |
| `updateVariables` | `variables` | VARIABLE_DOCUMENT UPDATE on the process instance |

Any step can set `"expectRejection": true`. That step's command must then be
rejected.

## Normalisation

The spec lives only in `normaliser.json`, and its `doc` field defines the
algorithm. `Normaliser.java` and `normalise()` in `zeebe_traces.rs` implement
that same algorithm over raw Zeebe-shaped records.

- **Kept records:** each lane keeps the EVENT records of its `valueType` whose
  intent it lists, projected onto the lane's fields. A missing field becomes
  `null`.
- **Rejections:** command rejections go into a trailing lane as
  `{recordType, valueType, intent, rejectionType}`.
- **Ordering:** lanes are concatenated in the order the spec lists them. Order
  *within* a lane is compared; interleaving *across* lanes is not.
- **Runtime identity is kept, canonically:** every `*Key` field of a kept
  record (record keys and the value's `processInstanceKey` / `flowScopeKey` /
  `scopeKey` / …) is rewritten to a canonical id — the first distinct raw key
  the scenario's records touch (in step order, then lane order) becomes 1, the
  next 2, and so on. The key map is kept for the **whole scenario** (a per-step
  reset would hide a cross-step target swap), and within one record the fields
  are visited in sorted name order so both normalisers assign the same ids.
  Raw key values are engine-specific, but the identity pattern is behavioural:
  completing a task in instance A must not read as completing it in instance B.
- **Dropped:** everything else, including timestamps and positions.

On the nano side, the runner renders engine events as raw Zeebe-shaped records.
For example, `JobCreated` becomes a JOB CREATED record, and an `EngineError`
becomes a COMMAND_REJECTION with the matching Zeebe `RejectionType`. Element
types come from the BPMN tag (`serviceTask` becomes `SERVICE_TASK`), as Zeebe
does.

## Touched cells and raising sites

`<name>.cells.json` contains:

- `cells`: the sorted cell ids the Zeebe trace touched.
- `sites`: for each `failure:`/`rejection:` cell, the
  `fqcn.method:line` that raised it.

The capture harness records these at capture time. A ByteBuddy agent
(`Sites.java`) intercepts `Failure` construction and every engine method with a
`RejectionType` parameter. It walks the stack to the raising frame, and only
the outermost site counts.

Cell ids follow the grammar shared with `formal/parity/extract.mjs`:

- `failure:<Class>.<method>:<ErrorType>`
- `rejection:<Class>.<method>:<RejectionType>`

The rules for the parts:

- `<Class>` is the simple name of the enclosing **top-level** class.
- `<method>` is the enclosing named method. A lambda is attributed to the
  method that contains it. A field initialiser or static block gives an empty
  method.
- A message-only `Failure` gives an empty `<ErrorType>`. The static extractor
  reports a re-wrap via `f.getErrorType()` as `forwarded`; at runtime the
  harness sees the original construction, so it attributes the cell to that
  original site and drops the forwarding plumbing.

For example: `failure:ExclusiveGatewayProcessor.findSequenceFlowToTake:CONDITION_ERROR`.

The coarser cells are `element:<Type>` (for example `element:ServiceTask`),
`intent:<ValueType>:<INTENT>` (for example
`intent:ProcessInstance:ELEMENT_ACTIVATED`) and `incident:<ErrorType>`.
`CellIdsTest.java` unit-tests the mapping from site to id, including lambdas,
inner classes and initialisers. Run it with
`capture.sh test`.

## Capturing and refreshing goldens (offline, not in CI)

```bash
ZEEBE_TRACES_JAVA_HOME=/path/to/jdk21 formal/traces/capture.sh            # all scenarios
ZEEBE_TRACES_JAVA_HOME=/path/to/jdk21 formal/traces/capture.sh <name>...  # some
ZEEBE_TRACES_JAVA_HOME=/path/to/jdk21 formal/traces/capture.sh test       # CellIdsTest
```

`capture.sh` reads the pin from `formal/parity/zeebe-pin.json` (the pin is not
duplicated anywhere here). On first use it checks out `camunda/camunda` at the
pinned sha into `~/.cache/nano-zeebe-traces/` and builds `zeebe/engine`. That
build takes several minutes, and the cache lives outside the repo because the
camunda build breaks on paths that contain spaces. The script then rewrites
the goldens and `goldens/PIN`.

**Pin bump:** update `zeebe-pin.json`, run `capture.sh`, and commit the
resulting diff. Every changed Zeebe trace or raising site then shows up as a
reviewable golden diff. CI fails until this is done: the
`goldens_match_the_zeebe_pin_and_the_scenarios` test compares `goldens/PIN`
with the pin.

## Harvesting Zeebe engine tests (#1391 step 2, #1249)

```bash
ZEEBE_TRACES_JAVA_HOME=/path/to/jdk21 formal/traces/capture.sh harvest [ClassName...]
```

The harvester (`Harvest.java`) is **dynamic**: it runs the pinned Zeebe engine
tests themselves — the compiled `zeebe/engine` test classes of the packages
listed in `harvest/harvest.json` (`processing/incident`, `processing/bpmn`,
`processing/message`, `processing/timer`), exactly as Zeebe's CI runs them —
and records, per passing test method, the process models it deployed and the
client commands it issued, read back from the `RecordingExporter` record
stream. That captures models built through helper methods, lambdas and
parameterised builders (e.g. `JobWorkerElementIncidentTest`) which a static
source parser cannot reproduce.

Each harvested test whose deployment + commands map onto the scenario script
vocabulary becomes `scenarios/h-<Class>-<method>/` (`process.bpmn` +
`script.json`); the `h-` prefix marks harvested scenarios. **DI:** the
`Bpmn.createExecutableProcess` builders emit no DI, so the harvester runs the
harvested XML through processos' layout/DI emitter (`processos layout`, the
`PROCESSOS_BIN` binary — built automatically when unset). A model that still
has no complete DI is **not** committed: the test is counted out-of-scope with
the reason in `harvest/out-of-scope.json`. Goldens are then captured by
`Capture` from the SAME committed DI-bearing XML, so Zeebe and nano run
identical models.

`harvest/summary.json` carries the counts (tests seen, harvested, laid out by
processos, out-of-scope, per class). The nano runner's `corpus_report` test
prints them in CI —

```
Zeebe harvested corpus: harvested=N runnable=N diverging=N out-of-scope=N
```

— and fails if the committed `h-*` scenarios and `out-of-scope.json` disagree
with the summary (a forgotten re-harvest). A harvested scenario that diverges
on the nano runner needs an allow-list entry with an issue reference, exactly
like a hand-written one.

## The nano runner (CI)

```bash
cd engine-core && cargo test --features serde --test zeebe_traces
```

`corpus_matches_zeebe_goldens` runs every scenario and fails in any of these
cases:

- the scenario diverges from its golden and is **not** in `allowlist.json`.
- an allow-list entry is **stale**: it is listed but no longer diverges, or its
  scenario does not exist.
- an allow-list entry lacks an issue reference (`#N` or an issues URL).

**Join rejections.** Zeebe takes every flow into a join by writing an
`ACTIVATE_ELEMENT` command, and `ProcessInstanceStateTransitionGuard` rejects it
with `INVALID_STATE` until the join is ready, so each early arrival leaves a
command rejection in the trace. Nano has no command records. The runner derives
these rejections from nano's events instead (`join_activation_rejections`):
per step, one rejection for each join arrival (`ParallelJoinTokenArrived`)
that did not fire a join (`ParallelJoinFired`) (#1244).

Allow-list entry shape:
`{ "scenario": "<name>", "issue": "#N", "reason": "...", "ignoreRecords": [ {...} ] }`.

`ignoreRecords` is optional and narrows the entry: each matcher is a non-empty
object of record fields (e.g. `{"bpmnElementType": "BOUNDARY_EVENT",
"elementId": "compA"}`). With the matched records masked out of both traces,
every step must still equal Zeebe exactly — a divergence anywhere else fails
the gate. Prefer it over a whole-scenario entry whenever the divergence is
confined to identifiable records.

## Adding a scenario

1. Create `scenarios/<name>/process.bpmn` (with DI) and
   `scenarios/<name>/script.json`.
2. Run `capture.sh <name>` and review `goldens/<name>.*`.
3. Run the nano runner. If nano diverges, fix nano (red/green), or file an
   issue and add an allow-list entry for it.

## Divergences this harness found but did not seed

The seeded corpus is all-passing except for the narrowly masked #1424 entry (compensation boundary-event lifecycle records). Two real
divergences were found while building the harness. They are tracked
separately, and their scenarios should be added once the fixes land:

- #1401: completing an already-completed job or user task. Zeebe rejects it
  with `NOT_FOUND`; nano rejects it with `INVALID_STATE`.
- #1402: an exclusive gateway with no matching flow. Nano emits
  `ELEMENT_ACTIVATED` before the `CONDITION_ERROR` incident; Zeebe keeps the
  gateway in `ACTIVATING`.
