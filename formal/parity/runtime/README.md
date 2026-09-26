# Two-backend runtime parity runner

Slice 5 of the differential runtime for [#1240] (part of [#1224]). Implements
[#1260].

One scenario **driver**, two **backends**:

- **nano** — the `engine-wasm` read-model `TestEngine`, in-process (no Docker).
  It runs every command to BPMN run-to-completion (RTC) quiescence
  synchronously, so nano needs no timing serialisation and is deterministic by
  construction. It exposes the full event stream, so it authoritatively reports
  every observation field and doubles as the always-on **reference oracle**
  (each scenario's `expect` block is checked against nano).
- **camunda** — a live **Camunda 8** reached over the **Camunda v2 REST API**
  (c8run, Testcontainers, or a service container). A real client, never a stub.

The driver plays an identical scenario against a backend and normalises the
outcome into a shared observation vocabulary. When both backends run, the
**differential oracle** asserts they are identical on the fields both expose.

## Product rule (why "identical", not "close")

Nano is a strict superset of Camunda 8: on Camunda's surface there is **no
tolerated divergence** — a difference is a Nano defect. And there are **no
retries**: a nondeterministic scenario is a *driver* defect to root-cause, not to
paper over.

## Layout

```
formal/parity/runtime/
  observation.mjs      shared observation vocabulary + differential/expect oracles
  nano-backend.mjs     engine-wasm TestEngine adapter (reference, always on)
  camunda-backend.mjs  Camunda v2 REST adapter (live C8; real, skip-tolerant)
  driver.mjs           scenario loader + step dispatcher (serialised, no retries)
  run.mjs              CLI: --backend nano|camunda|both  --corpus DIR
  driver.test.mjs      node --test: nano corpus + oracle logic (no Docker)
  corpus/<name>/        seed scenarios: process.bpmn (+DI) + scenario.json
```

## Scenario format

`corpus/<name>/scenario.json`:

```json
{
  "name": "linear-service-task",
  "bpmn": "process.bpmn",
  "processId": "parity-linear",
  "variables": {},
  "steps": [
    { "op": "activateAndComplete", "jobType": "work", "variables": { "result": "done" } }
  ],
  "expect": { "completed": true, "variables": { "result": "done" } }
}
```

Step ops (each fully settles before the next — this is the timing
serialisation): `activateAndComplete`, `correlateMessage`, `broadcastSignal`,
`advanceTime`. Every BPMN carries DI (repo rule: all BPMN models need DI for
human rendering).

The seed corpus is a small MC-derived sample — a linear service task, an
exclusive-gateway route, and a parallel diamond — enough to exercise the spine.
It is wired to the **single-source corpus generator** ([#1258], built in
parallel) once that lands: point `--corpus` at the generated directory; the
scenario schema above is the contract.

## Observation vocabulary

The spec vocabulary may only use what **both** engines expose. A bare Camunda 8
gateway (no Elasticsearch / secondary storage) exposes, over v2 REST, only
process **completion** and the final process **variables** (via
`createProcessInstance` with `awaitCompletion`). Nano additionally exposes the
full element-instance history (`completedElements`), taken `sequenceFlows`,
`jobsCreated`, and `incidents` through its event stream.

Each backend declares which fields it can `provide`. The differential oracle
compares only the **intersection** — the honest Camunda-surface contract
(`completed` + `variables` today) — while the per-scenario `expect` block pins
the richer element/flow multiset against nano. Extending the Camunda side to the
richer fields needs the v2 **query** API (secondary storage) and is future work
(see below).

## Running locally

```sh
# nano only (no runtime needed):
node formal/parity/runtime/run.mjs --backend nano

# both backends against a live Camunda 8:
CAMUNDA_REST_ADDRESS=http://localhost:8080/v2 \
  node formal/parity/runtime/run.mjs --backend both

# unit tests (nano + oracle logic):
node --test formal/parity/runtime/*.test.mjs
```

Env for the Camunda backend: `CAMUNDA_REST_ADDRESS` (required to run it — unset
⇒ skip-tolerant skip), optional `CAMUNDA_AUTH_TOKEN` (Bearer) or
`CAMUNDA_BASIC_AUTH` (`user:pass`).

## CI

The `parity-runtime (two-backend)` job in `.github/workflows/ci.yml` is a
**distinct** block from the `formal (tlc)` job and is **change-gated** (the
`parityruntime` paths filter) and **non-required** — it is deliberately *not* in
branch protection or `.mergify.yml`, so a skip or an absent Camunda runtime never
blocks a sibling PR. It always runs the nano half and the oracle unit tests; the
live Camunda 8 differential runs only when the `RUN_CAMUNDA_PARITY` repo variable
is `true`.

## Pending environment decision (maintainer action)

Standing up a live Camunda 8 in CI is a real infrastructure decision that a
maintainer must make — deliberately **not** faked or stubbed here. To enable the
differential:

1. Provision a Camunda 8 runtime reachable over v2 REST — a GitHub-hosted-runner
   **service container** / Testcontainers, **c8run**, or a **self-hosted
   runner**. A bare Zeebe gateway is enough for the `completed` + `variables`
   differential (no Elasticsearch required, because `awaitCompletion` reads the
   final state from the broker directly).
2. Set the repo variable `RUN_CAMUNDA_PARITY=true` and `CAMUNDA_REST_ADDRESS`
   (and any auth secret) so the CI step points at it.
3. To extend the differential to element-instance history / sequence flows /
   incidents, add secondary storage (Elasticsearch + the exporter) and wire the
   v2 query API into `camunda-backend.mjs`'s `provides` + `observe`.

Until then the job runs nano-only and stays green, which is the intended
skip-tolerant behaviour.

[#1224]: https://github.com/nanobpm/nano-bpm/issues/1224
[#1240]: https://github.com/nanobpm/nano-bpm/issues/1240
[#1258]: https://github.com/nanobpm/nano-bpm/issues/1258
[#1260]: https://github.com/nanobpm/nano-bpm/issues/1260
