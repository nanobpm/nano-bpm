# Runtime parity environment spike (#1280) — go/no-go

**Status: PENDING ENVIRONMENT DECISION (maintainer action).**

This is the dedicated infrastructure spike that gates the *runtime* half of the
nano ↔ Camunda 8 parity suite ([#1240]). It answers one **environment go/no-go**:

> Can our CI runners host **Docker + a live Camunda 8** (reachable over v2 REST)
> **and** a **JVM + JaCoCo agent** — deterministically, retry-free, within
> budget?

Everything downstream ([#1270] flip-on, [#1247] JVM/JaCoCo coverage) presupposes
"yes"; nothing had proven it on the actual runners. This directory is the
**self-contained harness** that proves it, plus the place the decision is
recorded.

It does **not** depend on [#1260]/PR #1271's not-yet-merged two-backend driver:
the probes here stand alone so the spike can run before/independently of that
landing. Once #1271 lands on `main`, the live corpus run is
`node formal/parity/runtime/run.mjs --backend both` pointed at the same gateway
this harness stands up.

## What runs the spike

`parity-runtime-spike.workflow.yml` (in this directory) — a
**`workflow_dispatch`-only, NON-REQUIRED** workflow. It never runs on
push/PR/merge_group, so it can never block a sibling PR, and it is deliberately
**not** in branch protection or `.mergify.yml`. Two jobs map onto the spike
tasks:

| job | spike task | proves |
| --- | --- | --- |
| `spike / Docker + live Camunda 8 (v2 REST)` | 1 + 2 | Docker-in-runner works; image pull cost + startup time; a bare gateway (no Elasticsearch) answers `deploy` + `createProcessInstance(awaitCompletion)` + final `variables` over v2 REST |
| `spike / JVM + JaCoCo agent` | 3 | the runner can fetch the JaCoCo agent, attach it via `-javaagent`, and produce a coverage exec + report artifact |

The self-contained probes:

```
formal/parity/runtime/spike/
  process.bpmn      trivial start -> end (DI included); no job worker needed
  rest-probe.mjs    v2 REST: topology -> deploy -> create+awaitCompletion -> assert
  jacoco-probe.sh   -javaagent attach + exec dump + CLI report, on a throwaway JVM
```

The `process.bpmn` is intentionally worker-free (start → end): it exercises
exactly the `completed` + `variables` surface a **bare gateway** exposes, with no
external moving parts. Task 2's full corpus (service tasks + a v2 REST job
worker) is #1271's `run.mjs`, wired in once it lands.

## How a maintainer runs it

0. **Install the workflow.** This harness ships the workflow as
   `parity-runtime-spike.workflow.yml` in this directory rather than under
   `.github/workflows/`, because the bot that opened the PR lacks the GitHub
   `workflow` OAuth scope needed to push into `.github/workflows/`. A maintainer
   with that scope moves it into place (it is otherwise a ready-to-run workflow):

   ```sh
   git mv formal/parity/runtime/spike/parity-runtime-spike.workflow.yml \
          .github/workflows/parity-runtime-spike.yml
   ```

1. **Dispatch the workflow** (Actions → *parity-runtime-spike* → *Run
   workflow*). Inputs let you iterate without editing the file:
   - `camunda_image` — the bare gateway image (default pinned to the 8.10 line,
     matching `formal/parity/zeebe-pin.json`). Resolving the exact
     image/tag/flags is itself part of the spike.
   - `extra_docker_env` — extra `-e KEY=VAL` docker flags (auth/config knobs).
   - `rest_port`, `ready_timeout_ms` — port + readiness budget.
2. **Read the job summary** — each job prints a go/no-go table: image size, pull
   seconds, gateway readiness ms, probe outcome, JaCoCo agent/exec sizes.
3. **Decide GitHub-hosted vs. self-hosted** from those numbers (runtime, resource
   ceilings, retry-free determinism, cost).
4. **Record the decision below**, with the run link and the numbers.

There are **no retries** anywhere: a nondeterministic probe is a defect to
root-cause (a driver/environment defect), never to paper over — per this repo's
"No Such Thing as Flaky Tests" rule.

## Decision (fill in — this is the acceptance deliverable)

> **GO / NO-GO:** _pending_
>
> - Workflow run: _link_
> - Docker + Camunda: pull `_s`, image `_ MB`, ready `_ ms`, probe `pass/fail`
> - JaCoCo: agent `_ KB`, exec `_ KB`, report `yes/no`
> - Runner choice: GitHub-hosted / self-hosted — _rationale, cost_

### If GO

- Flip [#1270] on: set repo variable `RUN_CAMUNDA_PARITY=true` and
  `CAMUNDA_REST_ADDRESS` (+ any auth secret) so #1271's `parity-runtime` CI job
  runs the live differential. Keep it **non-required / skip-tolerant**.
- For [#1247]: attach the JaCoCo agent to `zeebe/engine` in this same runtime
  using the approach the `jacoco-probe.sh` validated; keep the coverage job
  non-required.

### If NO-GO

- Record the self-hosted-runner (or alternative) recommendation and its cost.

## Promotion guard

This harness must stay **non-required**. Promoting any of it to a merge gate
requires the full procedure in [`AGENTS.md` → "Merging PRs"][merge] (the
merge-protocol block + `.mergify.yml` + live branch protection +
`console/scripts/merge-gates.*`). Do not add it to a required check without that.

[#1240]: https://github.com/nanobpm/nano-bpm/issues/1240
[#1247]: https://github.com/nanobpm/nano-bpm/issues/1247
[#1260]: https://github.com/nanobpm/nano-bpm/issues/1260
[#1270]: https://github.com/nanobpm/nano-bpm/issues/1270
[merge]: ../../../../AGENTS.md
