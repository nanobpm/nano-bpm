# Tier-2 differential isolation scenario (ADR-0020)

Proves the **Tier-2 per-process-definition** admission compressor throttles only
the *sick* definition and leaves a healthy sibling that **shares a job type**
fully admitted.

## The two definitions

Both emit the SHARED job type `common-job` (one well-provisioned worker pool
serves both). Only `orders-slow` adds a second, deliberately under-provisioned
task on `slow-job`:

- `orders-fast.bpmn` — `start → [common-job] → end`
- `orders-slow.bpmn` — `start → [common-job] → [slow-job] → end`

Because `slow-job` is starved, **only** `orders-slow`'s in-flight instance
backlog `L_P` climbs. Tier-2 keys on the process definition, so it must raise
`nanobpm_tier2_pressure{proc=orders-slow}` and shed its creates while
`orders-fast` stays at zero pressure and full admission. A per-*job-type*
detector on the shared `common-job` would see it healthy and fail to isolate —
which is exactly why Tier-2 keys per definition.

## Driver

`load-testing/rust-worker/src/diffgen.rs` (`cargo build --release --bin diffgen`)
drives two independent lanes (each with its own producers/clients) at equal
create rates and two worker pools:

- `common-job`: `COMMON_WORKERS` (fast, keeps up for both lanes)
- `slow-job`: `SLOW_WORKERS` (few) × `SLOW_MAXPAR` with `SLOW_DELAY_MS` per job

It reports, per lane, `producedRate` / `acceptedRate` / `shedRate` (the Tier-2
shed signal) / `tput` / e2e `p50`/`p99`, recording e2e on each lane's terminal
task (`common-job` for FAST, `slow-job` for SLOW).

## Run (GCP loadbox)

Requires `NANOBPMN_SLA_MODE=latency` and `NANOBPMN_TIER2=on` (opt-in; the default
is observe-only, see issue #1532) on the gateway.

```bash
GW=http://10.128.0.19:8080 \
  load-testing/scripts/tier2-diff-soak.sh
```

The orchestrator deploys both definitions, launches diffgen, samples
`nanobpm_tier2_pressure{proc}` over the run, prints the per-lane results and
asserts the isolation invariant:

    PASS = slow pressure>0 AND slow shedRate>0  AND  fast pressure==0 AND fast sheds ~0
