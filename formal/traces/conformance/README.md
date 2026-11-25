# Zeebe exporter-trace validation (#1962)

Closes the Zeebe-side link of the parity proof:

```
Zeebe Java ──(A: this directory)──> ZeebeTokenFlow.tla <──(B: RefinesZeebe)── TokenFlow.tla
```

`formal/tla/ZeebeTokenFlow.tla` is hand-transcribed from `zeebe/engine`. Link B
(TLC's `RefinesZeebe`) only proves Nano's spec agrees with that transcription;
link A checks the transcription against the **real** engine, so a mistranscribed
reference model can't hide divergences on both sides.

## What it does

`zeebe-conformance.mjs`:

1. **Scenarios**: takes every graph of the hand-written corpus
   (`formal/corpus/graphs`) and of the bounded-exhaustive enumeration
   (`formal/corpus/enumerate.mjs`, a bound named in `formal/corpus/bounds.json`)
   whose node kinds `ZeebeTokenFlow` models. For each one it derives one
   scenario per completion order and routing, from the same source the runtime
   parity runner uses (`exploreOrders` → `scenariosFor`, `bpmnFor` with variable
   routing). Cyclic graphs (`ExclusiveLoop`) are unrolled to
   `CYCLIC_MAX_STEPS` commands. A run cut at the bound is a settled prefix, so
   it's still a valid behaviour to check.
2. **Capture**: runs every scenario on the pinned Zeebe
   (`formal/parity/zeebe-pin.json`), in-JVM via `EngineRule` with the recording
   exporter (`formal/traces/capture.sh`), and normalises the records with the
   shared normaliser (`formal/traces/normaliser.json`).
3. **Soundness**: projects each record stream onto the spec vocabulary and asks
   TLC whether it's a behaviour of `ZeebeTokenFlow`
   (`formal/tla/conformance/ZeebeTokenFlowTrace.tla`):

   | Milestone   | Exporter record                                       |
   |-------------|-------------------------------------------------------|
   | `flow:<f>`  | `SEQUENCE_FLOW_TAKEN` of flow `f`                     |
   | `act:<n>`   | `ELEMENT_ACTIVATED` of a task or event-based gateway  |
   | `fire:<j>`  | `ELEMENT_ACTIVATED` of a join (and/or, >1 incoming)   |
   | `done`      | `ELEMENT_COMPLETED` of the process instance           |

   Each model action consumes exactly the milestones it emits, in any order
   within that action. The trace has to end with the model at rest: settled,
   and not owing the instance completion. When a trace is rejected, the runner
   binary-searches for the longest prefix the model accepts and reports the
   first milestone it can't produce.
4. **Coverage**: for each graph with a join, runs the *unconstrained* model and
   collects every join decision (`fire:<j>` / `defer:<j>`) it can take. Each one
   has to be observed in some Zeebe trace of that graph. Every flow taken into a
   join is one activation attempt, decided within its step, so a step with `k`
   arrivals and `m` fires observed `m` fires and `k - m` defers.

Any rejected trace or uncovered decision is a **divergence**: the reference model
is wrong (or too permissive). Fix the model; `RefinesZeebe` then surfaces any
matching Nano divergence, which gets filed separately.

## Abstraction and its soundness

This section states exactly what link A checks, so it can be reviewed without reading the code.

**Action labels.** `ZeebeTokenFlowTrace.tla` doesn't restate Zeebe's semantics. It
`INSTANCE`s `ZeebeTokenFlow` and labels each model action with the exporter
records that action emits (`stepMilestones` projects the records onto the same labels):

| Model action (`ZeebeTokenFlow`)             | Label (milestones)                       | Exporter records                                                |
|---------------------------------------------|------------------------------------------|-----------------------------------------------------------------|
| `DrainVia(f, S)` into a task / event gateway | `act:<n>`                                | `ELEMENT_ACTIVATED` of `n`                                      |
| `DrainVia(f, S)` into a join that fires      | `fire:<j>` + `flow:<g>` for `g ∈ S`      | `ELEMENT_ACTIVATED` of `j`, `SEQUENCE_FLOW_TAKEN` of its outflows |
| `DrainVia(f, S)` into a join that defers     | nothing (silent)                         | nothing (the activation command is rejected, no event)          |
| `DrainVia(f, S)` into a non-join gateway    | `flow:<g>` for `g ∈ S`                   | `SEQUENCE_FLOW_TAKEN` of each chosen outflow                    |
| `DrainVia(f, S)` into an end event           | nothing (silent)                         | end-event lifecycle (dropped, see below)                        |
| `CompleteTask(t)`                           | `flow:<g>` for `g ∈ Out(t)`              | `SEQUENCE_FLOW_TAKEN` after the job completes                   |
| `CorrelateEvent(n, g)`                      | nothing (silent)                         | the catch event is activated directly; no flow is taken         |
| `CompleteInstance`                          | `done`                                   | `ELEMENT_COMPLETED` of the `PROCESS`                            |

**What is dropped, and why that is sound.** The projection keeps only
`PROCESS_INSTANCE` records. It drops:

- every non-`ELEMENT_ACTIVATED` lifecycle intent (`ACTIVATING`, `COMPLETING`,
  `COMPLETED`, …) of an element other than the process;
- `ELEMENT_ACTIVATED` of splits, non-join gateways, catch and end events;
- the `JOB`, `VARIABLE`, `MESSAGE_SUBSCRIPTION`, … value types.

None of these is a state change of `ZeebeTokenFlow`: they're wait-state
bookkeeping or lifecycle steps that follow, in the same command, from a kept
record. The model has no variables for them. Dropping them only makes the
check *weaker*, never wrong: link A proves "every observed projected trace is a
behaviour of the model". It doesn't claim the model predicts the dropped
records. Those are compared record-for-record by the differential harness
(`formal/traces`, `engine-core/tests/zeebe_traces.rs`).

Order inside one action's label is not checked (`Match` compares sets), because
Zeebe writes one command's follow-up records contiguously in an order the model
abstracts. Order *between* actions is checked. Each label set is consumed whole,
so a step's records can't interleave with another step's.

**The join-inference rule** (coverage, `observedDecisions`). A deferred join
arrival is silent, so a trace can't show a `defer:<j>` directly. The runner infers it:
within one captured step, `k` flows taken into join `j` and `m` `fire:<j>`
records mean `m` fires and `k − m` defers.

*Assumption:* every flow taken into a join produces exactly one activation
attempt, and that attempt is decided (fired or rejected) **within the same
step** (the same client command's processing). This holds for Zeebe because
`SEQUENCE_FLOW_TAKEN` writes one `ACTIVATE_ELEMENT` command for the target,
and the engine processes all follow-up commands of a client command before it
returns. The model makes the same assumption: one queued activation per
arrival, drained by `DrainVia`. Same-step settlement is a **precondition
supplied by the capture harness**, not something TLC verifies:
`traceMilestones` flattens captured command boundaries before validation, so
moving a join firing into a later captured step can leave the validated trace
unchanged while changing the inferred fire/defer coverage. What TLC *does*
check is the label-level shape: it only accepts a trace where each `fire:<j>`
is produced by the drain of an arrival, so `fire:<j>` with no arrival, more
fires than arrivals, or a fire before its arrivals are all **rejected** (see
the negative tests in `zeebe-conformance.test.mjs`). The inference is only
used for *coverage*
(which decisions were observed), never to accept a trace.

## Running it

```bash
node formal/traces/conformance/zeebe-conformance.mjs --bound ci          # corpus + ci bound (~6 min)
node formal/traces/conformance/zeebe-conformance.mjs --graph ParallelDiamond
node formal/traces/conformance/zeebe-conformance.mjs --bound ci --captured /tmp/zc/traces  # reuse a capture
```

Every finding (`REJECTED` / `UNCOVERED`, `divergences.json`, a filed issue) carries a
complete `repro:` command — every verdict-shaping option, the graph and `--pin <sha>`
(the Zeebe commit it was captured on; a mismatched pin refuses to run) — to paste
verbatim.

It needs JDK 21 (`ZEEBE_TRACES_JAVA_HOME`, as for `capture.sh`) and the pinned
`tla2tools.jar`, which `formal/tla/check.sh` fetches. The first capture builds
the pinned Zeebe engine, which takes a while; after that it's cached.

On macOS sandboxes that forbid JVM self-attach, preload the byte-buddy agent:
`JAVA_TOOL_OPTIONS="-javaagent:<byte-buddy-agent.jar> -Djava.io.tmpdir=<writable dir>"`.

## CI

`.github/workflows/zeebe-conformance-nightly.yml` runs it nightly on the
`nightly` bound and files each new divergence class as a
`parity(zeebe-conformance): …` issue, deduplicated by a signature marker. It is
**not** a required PR check. Pull requests that bump the Zeebe pin or edit the
reference model, the trace module or this runner re-run it on the `ci` bound.
The formal (tlc) job runs the unit tests (`zeebe-conformance.test.mjs`), which
check the projection and TLC acceptance on fixed Zeebe-shaped traces.

## Findings

- **Event-based gateway correlation is silent.** Zeebe activates the triggered
  catch event directly (`BpmnEventSubscriptionBehavior.activateTriggeredEvent`)
  and writes no `SEQUENCE_FLOW_TAKEN` for the gateway→catch flow. The spec
  vocabulary (`ZeebeTokenFlow.CorrelateEvent`'s documentation,
  `formal/corpus/spec-oracle.mjs`) previously claimed the selected flow was
  taken. The matching Nano divergence (Nano takes every gateway→catch flow) is
  https://github.com/nanobpm/nano-bpm/issues/1963.
