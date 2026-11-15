# ADR 0020 — Two-tier admission compression: a global engine-saturation guard + per-process-definition backlog compressors

Status: **Proposed — accepted for implementation.** Superseded in part: the Tier-2
per-definition compressor must not ship as a customer latency-SLA control until the
latency-causal shed predicate is added — see ADR 0021 "Blocking constraint: shedding
must be latency-causal" (2026-07-17).
Date: 2026-07-17. Revised 2026-07-18 (Tier-2 keyed on per-process-definition
in-flight backlog directly, replacing the per-job-type detect + per-process
fan-in design; Tier-1 signal resolved to raft-log fsync latency).
Relates to: ADR 0013 (SLA modes & the compressor/limiter model), ADR 0017 (worker-concurrency governor), ADR 0012 (decoupling terminal state from exporter lag), `server/src/backpressure.rs`, `server/src/drain_guard.rs`, `server/src/deepthi.rs`, `server/src/main.rs`, `docs/distributed-scaling-design.md`.

## Context

In `NANOBPMN_SLA_MODE=latency` the engine must **shed/pace producer intake to
preserve end-to-end (create→complete) latency**. The actuator is the admission
gate on `createProcessInstance`; the question this ADR settles is *what signal
drives it*.

A long line of designs each keyed the compressor on a single **proxy** for "are
we the bottleneck" and each proxy missed the real constraint:

- **Internal command latency vs a learned baseline** (commit `b259685`): the
  baseline pinned to the first idle window (~5 µs) and froze whenever load pushed
  latency past `baseline × CONGESTION_RATIO`, so under any real load the relative
  threshold was unreachable and the cap ratcheted monotonically to the floor and
  never released — the observed *"90k/2000 clipping"*.
- **Engine-actor saturation ρ** (commit `4d60bfc`, the busy fraction of the
  single-writer engine actor + backlog-trend): correct, absolute, cold-start-proof,
  and immune to external strain — but a **GCP soak proved ρ never approaches
  saturation** for real workloads. The apply loop is microsecond-cheap; under a 3×
  create-flood (150k offered, ~44k landed, ~105k/s backpressured) ρ peaked at 0.58
  and typically sat at 0.2–0.35. The throughput wall is **upstream of the apply
  loop** (the raft propose/batch/log-fsync/commit path), which ρ-of-the-apply-loop
  does not measure. So the ρ compressor stayed inert on genuine overload.

Two structural facts fell out of that soak and the analysis around it:

1. **Little's law is the frame.** Latency `W = L / λ` (backlog ÷ throughput). The
   *absolute* backlog is **not** a latency proxy on its own — the same backlog is
   fine at high throughput and catastrophic at low throughput. The scale-free
   signals are the **backlog trend `dL/dt`** ("is latency growing?", threshold-free)
   and, to bound *absolute* latency, a **throughput-scaled target** `L* = W_target ·
   λ` (so a latency target becomes a backlog band that auto-scales with capacity —
   dissolving the "90k/2000" magic-number problem).

2. **Choosing the metric is choosing the plant boundary (control volume).** ρ draws
   the box around *our engine only* — worker slowness is an external disturbance to
   *ignore* (fault-attribution). Backlog/latency draws the box around the *whole
   create→complete pipeline* — the workers are *inside* it, so their slowness is
   internal dynamics to *regulate* by throttling the one input we own (intake).
   `SLA_MODE=latency` is, by definition, the second contract: preserve e2e latency
   regardless of which component is slow.

The engine is a **general product deployed into heterogeneous environments** with
many process definitions and many independently-scaled worker pools. A single
*global* backlog signal cannot express that heterogeneity: if one workload's
backlog explodes, a global compressor throttles admission of **every** process —
punishing healthy workloads for one sick one. Heterogeneity therefore forces
**per-process-definition decomposition** (regulate each user workload by its own
accumulating backlog).

Finally, the create-flood soak showed a bottleneck class that is **not**
per-definition: a **shared internal write path** (raft commit / disk fsync / apply)
that saturates *all* definitions at once. Per-definition decomposition cannot see
it; it needs a **global**
guard. The two bottleneck classes are orthogonal.

## Decision

Adopt a **two-tier admission compressor**, gated on in latency mode. The final
admission decision is the **intersection** of the two tiers:

```
admit(createProcessInstance P)  ⟺  global_engine_headroom > 0   (Tier 1)
                                AND  p_P < 1                     (Tier 2, P's own backlog)
```

### Tier 1 — Global engine-saturation guard (the shared write path is *ours*)

A single node-level guard that throttles **all** intake when the engine's own
**shared write path** is the bottleneck — the class ρ was reaching for but
mis-located. A read-only GCP diagnostic (150k-offered create-flood, binsha
`5e2e280c`) localized the wall to the **raft-log fsync**: `raft_fsync` ran
~737–790/s at ~2.4 ms latency (aggregate ~1.78 fsync-seconds/s per node, spread
concurrently across the node's partition logs), dwarfing the varstore
`journal_fsync` (~260/s, 0.79 s/s), while **commit-wait stayed ~75 µs**
(batch-amortized — a *poor* signal). So Tier-1 keys on the **raft-log fsync
latency knee** (~2.4–3 ms under load vs sub-ms idle), *not* the apply-loop busy
fraction and *not* commit-wait. The signal path already exists:
`metrics::raft_fsync_sum_count()` (the windowed mean `raft_fsync_avg_us` the
recovery throttle already consumes). This is the correct home for the
*isolated-server / fault-attribution* frame: it fires only when **we** (the
engine's shared write machinery) are saturated, independent of any worker pool.
It is a backstop, not the primary actuator.

**Implementation (`GlobalGuard` in `backpressure.rs`).** A single node-level
compressor on the smoothed raft-fsync latency. It calibrates a healthy baseline
(snap-down-to-min, slow creep-up), *floored* at `baseline_floor_us` (default
0.5 ms) so a near-zero idle baseline can't make the knee trivially crossable;
the knee is `max(baseline, floor) · congestion_ratio` (default 2×). Above the
knee it freezes the baseline and **attacks** (pressure `p ∈ [0,1]` rises,
steepening with the fractional overshoot past the knee); below it, it releases
slowly (attack/release asymmetry damps oscillation). The monitor steps it each
~1 Hz tick with the windowed `fsync_avg_us` (reusing the delta already computed
for the recovery throttle) and only actuates in latency mode; admission sheds a
paced `p·1000` per-mille fraction of **all** creates via a deterministic
accumulator (no RNG), at all three create entry points, checked before Tier-2.
Env: `NANOBPMN_TIER1` (default on) + `NANOBPMN_TIER1_{RATIO,ATTACK,ATTACK_GAIN,
RELEASE,EWMA,CREEP,FLOOR_US}`. Published as `nanobpm_tier1_pressure` (per-mille,
node-level gauge).

**Recovery relaxation — defer the soft knee to the recovery throttle (2026-07-18).**
A GCP bounce soak exposed a failure mode where Tier-1 and the recovery admission
throttle (`recovery_throttle.rs`) *stack* destructively on a returning owner. Both
key on the same raft-fsync signal: the recovery throttle paces intake under the
fsync knee via a backlog cap (it *is* the durability-preserving recovery actuator),
and Tier-1 *also* sheds on the fsync knee. On a node rejoining near the cluster
ceiling, its just-reclaimed partitions carry an elevated/oscillating fsync latency
(the "depth ratchet"), so Tier-1 shed the returning owner's **own** creates to ~0
(`tier1_pressure` pinned near 1000‰) for the whole recovery window — while the node
had *near-zero* backlog to justify it. The shed instances simply displaced onto the
already-loaded survivors, so it bought **no aggregate-latency benefit**: exactly the
"punish admission, deliver no latency gain" anti-pattern ADR-0021 warns against. The
result was the old node-bounce **oscillation collapse** (recovered throughput
swinging 77 ↔ 24 k/s, never converging).

The fix: while the node is in a recovery window (`recovering` = failover incumbent
or returning owner, the same signal the recovery throttle consumes), Tier-1 defers
its **soft knee** to the recovery throttle and only sheds past a **wider
hard-ceiling backstop** (`baseline · recovery_ratio`, default 4× vs the 2× soft
knee), and it does **not** calibrate its baseline from the congested recovery
latency (which would inflate the steady-state knee afterward). The recovery throttle
remains the primary disk-protection actuator during the window; Tier-1 becomes a
pure backstop that still clamps a *genuinely* drowning node (fsync past the hard
ceiling). Env: `NANOBPMN_TIER1_RECOVERY_RELAX` (default on),
`NANOBPMN_TIER1_RECOVERY_RATIO` (default 4.0). Outside a recovery window Tier-1 is
byte-for-byte unchanged. This closes the oscillation regression but **not** the
residual recovered-throughput deficit, which is completion-latency-bound on the
returning owner's deep partitions (a distinct, still-open lever — see ADR-0019).

### Tier 2 — Per-process-definition backlog compressors

The primary latency-preservation loop. An earlier draft detected congestion per
**job type** and reconstructed a per-process pressure by fanning in
(`max_{t∈types(P)} p_t`) over a statically-derived process→job-type incidence
map. That indirection existed only to answer *"given a shared job type is
backing up, which `createProcessInstance` calls do I throttle?"* — a question
that **dissolves** once we measure a process definition's own backlog directly.
Keying Tier-2 on **per-definition in-flight instance backlog** makes the
detection unit equal the actuation unit (`createProcessInstance(P)` ↔ P's own
backlog), and is strictly *more precise*: if processes `A` and `B` share a
congested job type but only `B` is accumulating, per-definition backlog throttles
only `B`, whereas fan-in would over-throttle healthy `A`.

- **Signal.** For each process definition `P`, let `L_P` = **in-flight instance
  count** (created but not yet terminally completed/terminated), maintained as an
  O(1) increment-on-CreateInstance / decrement-on-terminal-completion counter in
  the engine `State` (hooked at the *logical* lifecycle, **not** the resident
  insert/remove — a spilled instance is still in-flight). `λ_P` = P's create rate
  (monitor differences a cumulative per-definition created counter).
- **Law.** One delay-gradient compressor per definition on `L_P`:
  - fast term: drive `dL_P/dt → 0` (match intake to P's own drain — scale-free,
    no magic number, finds capacity wherever it is);
  - slow term: bias toward the throughput-scaled band `L*_P = W_target · λ_P` to
    bound *absolute* e2e **instance** latency `W_P = L_P/λ_P` (exactly the
    process latency users feel), bleeding a deep-but-stable backlog down.
  Each governor emits a **pressure** `p_P ∈ [0,1]` (0 = healthy, 1 = fully shed);
  EWMA-smoothed with a deadband as a fraction of `λ_P` (raw `dL/dt` is noisy).
- **Actuation.** `admit(createProcessInstance P) ⟺ p_P < 1` (throttled in
  proportion to `p_P`). No incidence map, no fan-in.
- **Default: observe-only (2026-11, issue #1532).** The compressors always run and
  publish `nanobpm_tier2_pressure{proc}`, but they do **not** shed creates unless
  the operator opts in with `NANOBPMN_TIER2=on`. Two reasons: (1) ADR-0021's
  latency-causal release blocker (below) — a shed that does not relieve a *locally
  contended* resource buys no latency; (2) a backlog with **no workers draining**
  (batch pre-creation, workers deployed later, a worker outage) cannot meet any
  latency budget however slowly instances arrive, so an on-by-default Tier-2 shed
  legitimate intake and never recovered — and Camunda 8 does not reject on this
  pattern, making the opt-out default a C8-compatibility gap as well. Tier-1
  (engine-saturation) and the memory-safety rails still shed real overload in
  every mode; only the per-definition latency-SLA shed is opt-in.

Two behaviours are **correct by design**, not bugs:
- a definition whose instances drain healthily is **never** throttled (the entire
  win over a global compressor), even if it shares a job type with a sick one;
- throttling `P` suppresses *all* of `P` (you cannot create "just the healthy
  parts" of an instance) — inherent to instance-granular admission.

Per-job-type congestion remains visible for **operators** via the existing
`nanobpm_job_sojourn_seconds` histogram (which worker pool is the constraint),
but it is **reporting only** — no longer a control input.

### What this replaces / retains

- The ρ + backlog-trend `BacklogGovernor` (commit `4d60bfc`) is **superseded** as the
  primary control. ρ survives as **observability**; the raft-fsync latency signal is
  now the Tier-1 global guard (see Tier 1).
- The **drain-guard credit servo** (`drain_guard.rs`, ADR-0013 lineage) remains the
  **inner** liveness rail. Tiers 1–2 are the **outer** loop; the cascade discipline
  holds (outer must move slower than inner to avoid the historical oscillation).
- Memory-safety rails (create-queue, exporter-queue, resident-byte watermark) are
  unchanged and continue to fire in **all** SLA modes for OOM safety.

## Consequences

- **Behaviour change vs ρ (a deliberate contract flip):** in the external-strain
  regime (slow workers, deep-but-stable backlog) the ρ design stayed *inert*; the
  Tier-2 design *will* throttle intake to bleed that backlog to the latency band.
  This is the correct `SLA_MODE=latency` behaviour (match admission to capacity to
  hold latency) and the opposite of ρ's fault-attribution. `SLA_MODE=admission`
  users are unaffected — Tier 1/2 do not actuate there (memory rails only).
  - **⚠ CORRECTION / RELEASE BLOCKER (2026-07-17):** this "correct behaviour" claim
    is only valid when the backlog reflects a *locally relievable* queue. When the
    elevated sojourn comes from a **slow external service the worker calls**,
    throttling intake does **not** speed up in-flight instances and (with ample
    worker concurrency) buys *no* latency at all — it sheds customer instances for
    nothing, inverting the SLA trade. Tier-2 keys on total in-flight `L_P` and
    cannot tell relievable queue-wait from external in-service waiting. **Tier-2
    must not ship as a customer latency-SLA control until a latency-causal shed
    predicate is added.** See ADR 0021 → "Blocking constraint: shedding must be
    latency-causal." (Observe-only / monitoring mode, which does not shed, is
    unaffected.)
- **Per-definition collateral throttling:** throttling a definition suppresses all
  of its instances (instance-granular admission); a deep-but-stable backlog is bled
  to the latency band. Both inherent and accepted.
- **Cardinality:** per-definition governors are a few atomics each — cheap even at
  hundreds of definitions. `L_P`/`λ_P` are O(1) counters in the engine `State`; no
  BPMN static analysis, no incidence map, no per-edge flow measurement.
- **The one absolute knob is `W_target`** (target e2e latency), which is meaningful
  and portable — unlike an internal-latency µs threshold or an absolute backlog
  count. Everything else (`dL/dt → 0`, `L* = W_target · λ`) is scale-free.
- **Tier-1 signal (resolved):** the read-only diagnostic localized the shared-write
  wall to the **raft-log fsync latency** (~2.4–3 ms under load vs sub-ms idle);
  commit-wait (~75 µs, amortized) and apply-loop ρ (never saturates) were both
  rejected. Tier-1 reuses `metrics::raft_fsync_sum_count()` — no new instrumentation.

## Validation

GCP 3-node RF=3 P=12, clean journal, latency mode. All four scenarios below **PASS**
on binsha `0620df4683d8b881` (commit `8042ae5`).

1. **Heterogeneous per-definition isolation:** two process definitions sharing a
   job type; sicken that type's worker pool via a definition that hammers it.
   EXPECT only the *accumulating* definition gets throttled; a sibling definition
   whose instances still drain healthily keeps full admission. (The scenario no
   prior design could pass — and per-definition backlog is *more* precise here than
   the earlier per-type fan-in, which would have over-throttled the healthy one.)

   **PASS** (binsha `0620df4683d8b881` = commit `8042ae5`, `load-testing/scenarios/tier2-diff`
   driven by `diffgen`, both lanes offered 2000 creates/s; `orders-slow` adds a
   starved `slow-job` pool of 4 workers × 250 ms ≈ 16 jobs/s while the shared
   `common-job` pool of 128 keeps up). Measured over the sickened window:

   | definition | Tier-2 pressure | shed | admitted | tput | e2e p50 |
   |---|---|---|---|---|---|
   | `orders-slow` (sick) | **1000‰** | 946/s | 669/s | 15/s | 28.4 s |
   | `orders-fast` (healthy, shares `common-job`) | **0‰** | 0/s | 1195/s | 1196/s | **16 ms** |

   The compressor drove `orders-slow` to full shed while `orders-fast` — sharing the
   *same* `common-job` type — stayed at zero pressure with full admission and a
   16 ms p50. A per-job-type detector on the shared `common-job` (never itself
   congested) could not have isolated this; per-definition backlog does.
2. **External-strain latency bound — run as an explicit OFF-vs-ON counterfactual.**
   The question this arm answers is *causal*: does throttling the sick definition
   **cause** its backlog (hence sojourn) to stay bounded, or are we merely shedding
   *because* latency is already high (correlation)? To separate the two we hold the
   offered load fixed and toggle only the actuator: one arm with `NANOBPMN_TIER2=off`
   (the governor still **computes and publishes** `nanobpm_tier2_pressure` but does
   not shed — "observe-only"), one arm with `NANOBPMN_TIER2=on`. Single sick
   definition `orders-slow`, offered 150 creates/s into a starved `slow-job` pool
   (4 × 250 ms ≈ 15 jobs/s drain), `W_target = 15 s`, 120 s window, binsha
   `0620df4683d8b881`. Server-side backlog `L(t) = nanobpm_active_backlog` summed
   across the three nodes.

   **PASS — the counterfactual is decisive on the backlog trajectory:**

   | arm | shed | backlog `L(t)` behaviour | net slope after t=21 s |
   |---|---|---|---|
   | **Tier-2 OFF** (observe-only) | 0/s | diverges monotonically 0 → **16,800 and still climbing** | **+132 /s** (unbounded) |
   | **Tier-2 ON** | 128/s | rises to the band (≈ `W_target·λ` = 2 250) then **arrests and holds ≈ 2 000**, gently bleeding | **−3.6 /s** (draining) |

   Same offered load; the *only* changed variable is whether the compressor may
   actuate — so the bounded backlog in the ON arm is **caused** by the throttling,
   not merely coincident with it. The tell is visible in the OFF arm: the governor
   *computed* pressure → 1000‰ by t = 21 s (it "wanted" to act) yet, unable to shed,
   the backlog diverged anyway. Bounded `L` ⇒ bounded sojourn by Little's law
   (`W = L/μ`): OFF → `W` tracks `L` to 1 100 s+ and rising; ON → `W ≈ L/μ ≈ 133 s`,
   bounded. The ON compressor holds a stable operating point (pressure oscillates
   850–1000‰ around the setpoint), not a monotonic clamp.

   **Two honest findings from running it properly:**
   - *Completed-only e2e percentiles cannot discriminate here* — both arms reported
     p50 ≈ 61 s / p90 ≈ 104 s / max ≈ 116 s, near-identical. This is **window
     censoring / survivorship bias**: in the divergent (OFF) arm the deeply-queued
     instances never finish inside the measurement window, so they never enter the
     percentile sample; the max simply pins to ≈ the window length in *both* arms.
     The server-side backlog `L(t)` is therefore the honest discriminator, and the
     admitted sojourn must be read as `L/μ`, not from completed-e2e percentiles.
   - *The realized bound is looser than nominal `W_target`.* The law arrests growth
     and then **holds** `L` near the level at which it caught it (≈ `W_target · λ_offered`)
     rather than actively driving `L` down to `W_target · μ` (≈ 225). Because the band
     keys on the **create rate** `λ` and attack fires only while `L` is *rising*, the
     steady state is "growth arrested, slow bleed" (−3.6 /s here). Tightening the
     realized sojourn to `W_target` is a tuning follow-up: key the band on the
     **drain/throughput** `μ` rather than the create rate, or add an active
     drive-down term below the band. The load-bounding (divergent → bounded) claim
     is proven regardless.
3. **Shared-write-path overload — Tier-1 engages on the fsync knee.**
   The premise is that the shared bottleneck is *our* raft write path. A plain
   create-flood, however, does **not** reach that regime on this hardware: at
   ~54k creates/s the **single-writer engine actor** (CPU) is the ceiling and the
   disk stays healthy (fsync flat ≈ 2.8 ms, commit-inflight < 600), so Tier-1
   correctly stays **dormant** — actor-CPU saturation is the *ρ-governor's* domain,
   not Tier-1's. To actually move the bottleneck onto the disk write path we inflate
   the per-instance journal payload (`VAR_BYTES=8192`) so disk *bandwidth* — hence
   fsync latency — is what saturates. (binsha `0620df4683d8b881`, `NANOBPMN_TIER2=off`
   to isolate Tier-1, offered ~60k creates/s of 8 KB instances across 3 nodes.)

   **PASS:**

   | phase | `nanobpm_tier1_pressure` | fsync | Tier-1 shed | behaviour |
   |---|---|---|---|---|
   | engage (t=6→9 s) | 0 → **1000‰** | ~1.9 → 2.6 ms (crosses 2× knee) | → 62,928/s peak | detects the knee, clamps |
   | hold (t=9→50 s) | 750–1000‰ | held ~2.0–2.9 ms (bounded) | 40–60k/s | protects the write path |
   | release (t=50→81 s) | **1000 → 850 → 650 → 500 → 350 → 231 → 126 → 32 → 0** | settles ~3.5–4.2 ms | → 0 | smooth glide, no sawtooth |
   | equilibrium (t>81 s) | 0 | stable ~3.6–4.5 ms | 0 | admitted ~22–27k/s sustained |

   Tier-1 engaged on the raft-fsync knee, held a **stable operating point** (no
   monotonic clamp — pressure decayed smoothly back to 0 as the adaptive baseline
   recalibrated to the sustained level; no sawtooth), and kept create-accept latency
   bounded throughout (p50 27 ms, p99 415 ms, max 826 ms). The requirement that the
   guard distinguish a *transient spike* (attack) from a *sustained new normal*
   (recalibrate + release) is exactly what the baseline creep delivered here.
4. **Cold-start into a storm — correct throttle on the first ticks.**
   Same 8 KB flood, but launched into a **freshly-started, clean-journal cluster
   with no warmup** (`WARM=0`, 1 s sampling) so the guard has *no* calibrated
   baseline. **PASS:** the very first fsync knee-crossing — t=5 s, fsync spiking to
   4326 µs as the cold commit pipeline fills — triggered Tier-1 on the **same tick**
   (pressure 194‰, shedding began), ramping to 1000‰ by t=10 s and then holding a
   controlled band (670–1000‰) with fsync bounded ~2.0–3.9 ms for the rest of the
   run. No learning window was needed: the floored baseline (`baseline_floor_us`,
   500 µs) makes the knee meaningful from t=0, so the scale-free signal throttles
   correctly on the first ticks. This is the property no learned-threshold governor
   could offer — it would have admitted the storm while still calibrating.

**Status: all four ADR-0020 validation scenarios PASS** on binsha `0620df4683d8b881`
(commit `8042ae5`), GCP 3-node RF=3 P=12, latency mode.

**Scope note surfaced by #3.** Tier-1 guards the **disk write path** (raft fsync)
specifically. When the bottleneck is instead the single-writer **engine actor**
(CPU-bound, as under a small-payload create-flood), fsync stays healthy and Tier-1
does not — and should not — fire; that regime is covered by the ρ-based
`AdmissionGovernor` (engine-actor saturation). The two are both "global" guards but
watch different shared resources; a complete deployment wants both live.

**Recovery-relax A/B (2026-07-18, binsha `578efd7248d52a23`).** Node-bounce soak,
RATE=14000, 90 s outage, node18 killed near the cluster ceiling and restored;
recovered throughput sampled t=155–300 after restore. Identical script and params,
guard relax OFF vs ON:

| arm | recovered mean/s | recovered min/s | node18 creates/s (median) |
|---|---|---|---|
| soft-knee shed in recovery (OFF) | **4,324** | **77** (oscillation collapse) | **0** |
| recovery-relax (ON) | **28,923** | **20,701** (stable band) | **1,107** |

Baseline steady ≈ 38,500/s. Relaxing Tier-1 during recovery converts an oscillating,
collapse-to-~80/s recovery into a **stable ≈75%-of-baseline** band and lets the
returning owner participate (median 0 → 1,107 creates/s) instead of being shed to
zero for no aggregate-latency benefit. The returning node stayed healthy while
admitting (drained its backlog; `rho` never pinned). The residual ~25% vs baseline
is **completion-latency-bound** on node18's deep just-reclaimed partitions — not
admission — and is the still-open lever tracked in ADR-0019.

## Alternatives considered

- **Single global backlog compressor (aggregate whole-system).** Simpler, but blind
  to *where* the backlog is — one sick workload throttles all admission. Rejected for
  a general/heterogeneous product.
- **Isolated-server ρ as primary (fault-attribution).** Rejected: proven inert on
  real workloads (apply loop never saturates) and it declines to preserve latency
  under external strain, which violates the `SLA_MODE=latency` contract. Retained
  only as observability.
- **Per-job-type detection + per-process fan-in (the earlier draft of this ADR).**
  Detect congestion at each job-type queue, then throttle `createProcessInstance(P)`
  by `max_{t∈types(P)} p_t` over a statically-derived incidence map. Rejected in
  favour of per-definition backlog: it needs BPMN static analysis + an incidence map,
  and it *over-throttles* — a definition sharing a congested type with a sick sibling
  is throttled even when its own instances drain fine. Per-definition backlog makes
  detection unit == actuation unit and only throttles the actually-accumulating
  workload. Per-job-type sojourn is retained as **reporting**.
- **Per-`(process,type)` decomposition.** Even finer attribution of a type's inflow
  across processes, but higher cardinality and needs per-edge flow measurement.
  Unnecessary once control is per-definition.
- **Direct e2e-sojourn probe (perturb-and-observe).** The original operator intuition;
  correct in spirit but e2e sojourn has a long transport dead-time that makes a fast
  probe oscillate. Resolved by using **backlog depth as the fast, dead-time-free
  causal proxy** (Little's law) with sojourn as slow confirmation — which is exactly
  the Tier-2 signal.
