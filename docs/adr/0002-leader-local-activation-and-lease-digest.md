# ADR 0002 — Leader-local job activation & best-effort lease digest

Status: **Leader-local activation Accepted (shipped, opt-in). Lease digest Accepted (implemented, opt-in; live failover A/B pending). Zero-config `auto` policy Accepted (shipped, DEFAULT under quorum) — see Part C (2026-07-11); resolves to leader-local + digest.**
Date: 2026-06-23.
Relates to: ADR 0001 (cluster job-activation fairness), `docs/distributed-scaling-design.md`.

## Context

A job in nanobpmn moves create → activate → complete. Under Raft (RF > 1) **all
three** were proposed through the partition log, so each job cost **3 quorum
commits**. Activation is the odd one out: unlike create/complete it does not
record durable *progress*, it records a **lease** — `JobState::Activated` plus a
`worker` and a `deadline` (`engine-core/src/state.rs`). The only cross-replica
effect of that lease is the `activated` latch read by the completion guard
(`if !job.activated → JobNotActivated`, `engine.rs`); the comment there already
notes completion is *by key alone, lock-holder-irrelevant*.

Two problems followed from replicating the lease:

1. **Throughput ceiling / over-provisioning dip.** Measured with the real
   `ch2-workers` driver (one producer Falcon connection, 96 in-flight
   fire-and-forget creates, worker ramp) on a 3-node RF=3 cluster: throughput
   peaked ~1425 jobs/s at 8 workers then **collapsed to ~196 jobs/s at 16
   workers** with a multi-second p99. More workers fragment activation into more
   small per-pull commits, all competing for the fixed per-partition commit
   budget against the create/complete traffic that actually carries progress.
2. **Activation is on the critical path for no durability benefit.** The system
   is **at-least-once** by construction — a worker that overruns its `deadline`
   has its lease reclaimed (`ExpireJobs`) and the job redelivered, so jobs must be
   idempotent regardless. The replicated lease buys only one thing: on **leader
   failover**, the new leader knows the in-flight lease + deadline and waits for
   the deadline before redelivering, rather than redelivering immediately. That
   narrows *wasteful duplicate execution* on a rare event; it is **not** a
   correctness property.

The question this ADR answers: **where should the activation lease live** — Raft
log, leader RAM, or somewhere in between — and what does each choice cost?

## Decision

### Part A — Leader-local activation (shipped, this ADR)

Add an opt-in mode, `NANOBPMN_REPLICATE_ACTIVATION` (default **true** = the
original fully-replicated lifecycle; single-node / RF=1 unaffected). When set to
`0`/`false`/`off`/`no` the activation lease becomes **leader-local**:

- `ActivateJobs` is **not** proposed through the Raft log — the leader locks jobs
  in its own single-writer engine actor only (`try_activate` →
  `activate_on_local` instead of `activate_on_raft`).
- Lock expiry (`ExpireJobs`) is likewise **leader-local** — the lock exists only
  on the leader, so proposing expiry through Raft would emit `JobLockExpired` on
  the leader (job is `Activated`) but nothing on followers (job is still
  `Created`), diverging the replicated event stream. `TriggerTimers` **stays
  logged** (it mints state / drives follower key allocation).
- Replicas run with **lenient completion** — since #1661 (ADR 0066 slice 2) a
  piece of *replicated engine state* (`State::lenient_completion`), set by the
  journaled `Command::SetLenientCompletion` → `Event::LenientCompletionSet`
  that the partition leader proposes through the Raft log, so every replica
  flips it at the same log index (it was a per-node host setter before):
  the `!job.activated` completion guard is relaxed so a replicated `CompleteJob`
  applies on a follower's `Created` job (the follower never saw the activation).
  This is gated — default-strict preserves single-node semantics and the
  `JobNotActivated` unit tests.

Each job then costs **2 quorum commits instead of 3**, and per-worker activation
no longer competes for the commit budget. **Only durable progress
(create / complete / fail / throw / timers) is replicated.** Possession of the
job key *is* the capability — keys are only ever handed out by activation — so
relaxing the latch does not let an arbitrary client complete a job it never held.

**Durability trade-off (the cost of Part A).** The lease is leader-RAM-only and
**does not survive failover**. Followers always see an activated job as
`Created`, with no `deadline`. So on leader failover the new leader re-dispatches
in-flight jobs **immediately**, versus the default mode which waits for the
replicated deadline. Both modes remain at-least-once; Part A merely **widens the
failover redelivery window** from "after deadline" to "immediate". On a *stable*
leader there is no behavioural change — the single-writer engine still enforces
exclusive leases, so no double-activation. Documented at
`replicate_activation_from_env` and the `ServerImpl.replicate_activation` field.

### Part B — Best-effort lease digest (implemented; opt-in)

Part A trades the failover redelivery window for ~3× throughput. **Part B buys most
of that window back without re-incurring the Raft cost and without an external
dependency**, as a *third* setting: `NANOBPMN_REPLICATE_ACTIVATION=digest`.

Idea: the leader periodically broadcasts a **lease digest** to its followers as a
**fire-and-forget async peer push** (no Raft consensus, no per-job round-trip) over
the existing app-lane peer Falcon. Followers keep a **soft lease table**. On
becoming leader, a node recovers any job it has heard a lease for — transitioning it
`Created → Activated` until that lease's (digest-reported) deadline — so the normal
leader-local expiry tick re-dispatches it only *after* the deadline rather than
immediately.

**As built (the spike's resolved choices):**

- **Digest shape: full `Vec<(job_key, deadline)>` per partition.** The richest of the
  options below was chosen for the first cut — it is exact, self-trimming (bounded by
  concurrent in-flight activated jobs), and needs no key reconstruction (full keys
  embed their partition). A roaring-bitmap encoding remains a future optimisation if
  payload size ever matters; pairs are fine at realistic in-flight counts.
- **Recovery is applied on every tick for every led partition, not on an explicit
  leadership-transition event.** This is idempotent and self-targeting: a long-stable
  leader holds *no* received digest for a partition it leads (it only stores digests it
  *receives*, and receives none for partitions it sends for), so recovery is a no-op
  except just after a promotion. `recover_lease` is also a no-op on an already-Activated
  job and on an expired deadline. The stored digest is evicted once its max deadline has
  passed.
- **Transport: the app-lane peer WebSocket** (`PeerLink::send_oneway` →
  `ClientFrame::LeaseDigest`, no `corr`, no reply), *not* the dedicated Raft lane and
  *not* (yet) the ADR-0001 backlog-piggyback channel. A standalone fire-and-forget frame
  keeps the digest fully decoupled from both consensus and the fairness signal; folding
  it into the piggyback envelope remains a possible future consolidation.
- **Cadence: the existing 500 ms server tick** drives both the broadcast and the
  recovery pass (`run_lease_digest`), gated on `lease_digest && !raft.is_empty()`.
- **Soft state only.** `recover_lease`/`activated_leases` (engine-core) mutate engine
  state without emitting or journaling any event — the lease was never replicated, so
  there is nothing durable to write. Default and leader-local code paths are
  byte-identical; the digest is purely additive.

**Verification:** engine-core unit tests cover `recover_lease` (soft Created→Activated,
idempotent, expired-deadline no-op) and `activated_leases`. A 3-node RF=3 server test
(`the_lease_digest_holds_failover_redelivery_until_the_deadline`) proves the end-to-end
behaviour: a job leased on the leader is broadcast, the leader is killed, the new leader
recovers the digest and an immediate `activateJobs` returns **nothing** (redelivery is
held), and after the deadline the expiry tick reclaims the lease so the job remains
available (at-least-once preserved). Still pending: a **live failover A/B** measuring the
duplicate-execution / redelivery-window reduction vs. plain leader-local (`=0`) under
load, and a throughput parity check (the digest adds only a periodic fire-and-forget
broadcast, so throughput should match leader-local).

Open design questions considered during the spike (choices resolved above):

- **Digest shape.** Options, cheapest → richest:
  - a per-partition **max-deadline / grace timestamp** (one `u64` per led
    partition): a new leader simply waits `max(observed deadline) − now` before
    re-dispatching *any* pre-existing `Created` job that has ever been
    activatable. Coarse but O(partitions) and trivially bounded.
  - a **roaring bitmap of leased job-locals** (+ a single grace deadline): the
    new leader holds only the specific jobs it heard were leased. O(in-flight)
    bits, still compact (~in-flight/8 bytes), self-trimming as leases clear.
  - a full `{job_key → deadline}` map: exact, but O(in-flight) entries and the
    largest payload. **Chosen** for the first cut (see above).
- **Cadence vs staleness.** Push every tick. A digest is a *hint*, never
  authoritative: a missed/stale digest degrades to the Part-A behaviour (immediate
  redelivery) — never to incorrectness, since leases stay exclusive on the live
  leader's single-writer actor and completion is key-alone.
- **Consistency class.** This is intentionally **best-effort / lossy async**,
  i.e. the *same* consistency class as an external Redis/ElastiCache lease
  (async-replicated, can drop the last writes on its own failover), but **with
  zero external infrastructure and no synchronous hop on the activate/complete
  hot path**. It is strictly *weaker* than the Raft-replicated lease of the
  default mode and strictly *stronger* (in the common case) than Part A's
  no-replication.
- **Memory.** Bounded by concurrent in-flight (activated-not-completed) jobs, not
  total jobs; self-trimming as leases clear/expire. ~in-flight/8 bytes (bitmap)
  to ~hundreds of bytes/lease (map). Backlog itself is already gated by
  admission/backpressure, so the digest reflects bound load, it does not create
  it.
- **Interaction with fairness (ADR 0001).** The digest currently rides its own
  fire-and-forget frame; folding it into the ADR-0001 peer piggyback envelope (so the
  two payloads compose without extra round-trips) remains a future consolidation.

## Alternatives considered

- **External shared lease store (Redis / ElastiCache, `SET … EX <deadline> NX`).**
  Works and does **not** explode memory (O(in-flight), TTL auto-expires orphans).
  Rejected as the *primary* path because it breaks nanobpmn's self-contained
  "no external infra" value prop, adds a synchronous network hop to every
  activate/complete, splits the source of truth, and provides a guarantee
  *weaker* than the Raft lease we already have by default (Redis replication is
  async/lossy on its own failover). Part B delivers the same consistency class
  embedded. Kept on record as a possible deployment-specific option for operators
  who already run a low-latency Redis and want cross-node lease visibility without
  the embedded digest.
- **Keep everything in Raft (status quo / default).** Strongest lease durability,
  but the 3-commits-per-job ceiling and over-provisioning dip Part A removes.
  Retained as the default for workloads that need failover to honor in-flight
  lease deadlines.

## Measurement methodology

A/B with the **authoritative** `ch2-workers` driver
(`~/workspace/nano-demo/scripts/ch2-workers.sh` → `driver/.../ch2_workers.rs`):
one producer Falcon connection, 96 in-flight fire-and-forget creates, a
worker ramp (`WS_STAGES`). Cluster: 3 nodes / 3 partitions / RF=3, `NANOBPMN_RAFT=1`,
`NANOBPMN_DURABILITY=async`, the **release** binary (debug is ~3.5× slower).
Validate cluster perf with **this** driver, not a REST-create + pipelined-complete
harness — the latter bypasses the reader_loop serialization and masks the
collapse. Reproduce:

```
# bring up a 3-node RF=3 cluster on :8080-8082, then for each arm:
#   default (replicate):           (unset NANOBPMN_REPLICATE_ACTIVATION)
#   leader-local (Part A):         NANOBPMN_REPLICATE_ACTIVATION=0
WS_STAGES=8,16,32,64 WS_STAGE_SECS=10 TARGET=http://127.0.0.1:8080 ./scripts/ch2-workers.sh
```

## Part A — performance: replicate vs leader-local

3-node RF=3, async durability, ramp 8/16/32/64 workers, 10 s/stage, M-series box
(co-resident servers + load, so absolute numbers are contention-limited; the
**A/B delta on the same box** is the signal).

| workers | replicate (default) job/s | leader-local job/s | replicate p99 | leader-local p99 |
| --- | --- | --- | --- | --- |
| 8  | 102.6 | 408.4  | 1527 ms   | 621 ms |
| 16 | 229.5 | 412.2  | 1300 ms   | 548 ms |
| 32 | 353.9 | 1052.7 | 897 ms    | 379 ms |
| 64 | 265.1 | 1119.7 | **30,535 ms** (collapse) | **279 ms** |
| **total completed** | **11,305** | **33,773 (~3×)** | | |

An earlier 1/4/8/16 ramp on the same setup showed the dip directly: replicate
peaked 1433 job/s @8w then collapsed to 196 @16w; leader-local climbed
314 → 1095 with p99 bounded < 700 ms throughout. **Leader-local removes the
over-provisioning collapse and the multi-second tail**, at the cost of the
failover redelivery window described above. (1 stray error at 64w under
leader-local = expected at-least-once lease redelivery.)

## Part B — performance

_Not yet implemented._ Spike to be measured against **Part A (leader-local) as
the baseline**, on the failover dimension specifically: inject a leader kill
mid-load and measure (a) duplicate-execution count and (b) redelivery latency —
expect digest to cut duplicate executions toward the default-mode level while
keeping Part A's steady-state throughput. Record here.

## Consequences

- **Default-off** keeps every existing benchmark, the single-node path, and CI
  byte-identical (codebase convention: admission, backpressure, and ADR-0001
  fairness are all opt-in).
- **At-least-once unchanged.** Jobs must be idempotent in every mode; only the
  failover redelivery window differs (default: after deadline; leader-local:
  immediate; digest: after digest-reported deadline, best-effort).
- **No double-activation on a stable leader** in any mode — leases stay exclusive
  on each partition's single-writer actor; a stale digest/hint costs at most an
  occasional premature redelivery, never a double-lease at the source.
- **Operator guidance.** Use the default for failover-deadline-sensitive
  workloads; `=0` for throughput-bound idempotent workloads; `=digest` (once
  shipped) for throughput-bound workloads that also want a narrowed failover
  redelivery window without external infra.

## Part C — zero-config `auto` policy (2026-07-11, DEFAULT under quorum)

### Context

Parts A & B were **opt-in**: `NANOBPMN_REPLICATE_ACTIVATION` defaulted to fully
replicated (`true`) under `quorum`. The 2026-07-11 GCP A/B (see PERFORMANCE.md, "Quorum
mode at 50KB") showed that default is a **throughput cliff**: with 50 KB variable
payloads, quorum + replicated activation collapsed completion throughput ~125×
(2,400 → 19/s, backlog 42k→85k). The activation *command* is small, but under `quorum`
every activation is an extra majority commit; that per-activation commit tips an already
loaded commit pipeline over. Switching to `digest` restored full parity with
leader-durable (2,398/s, p99 91 ms). Requiring the operator to *know* they must set
`=digest` is a foot-gun.

An earlier revision of this part tried to be clever: an **adaptive per-partition policy**
that kept the strict replicated lease at small payloads (using a payload-byte EWMA) and
flipped to leader-local + digest only at large payloads. The 2026-07-11 validation soak
**disproved that premise**: a *negligible*-payload workload at ~61k jobs/s **wedged**
(creates and completions both froze, producers hit `MAX_INFLIGHT` and stopped) precisely
because `auto` kept the strict replicated lease there. The cost of the strict lease is a
**per-activation quorum commit**, which is unsafe at *any* non-trivial throughput —
driven by commit COUNT at small payloads and by BYTE volume at large payloads. A signal
keyed on payload bytes cannot see the count-pressure case. The strict lease is only
affordable at genuinely low throughput, where its marginal benefit (no failover
redelivery) does not justify the wedge risk.

### Decision

`NANOBPMN_REPLICATE_ACTIVATION=auto` is the **default under `quorum`** (`leader-durable`
keeps its Part-A leader-local default). `auto` is a **zero-config alias for leader-local
activation + the soft lease digest** — behaviourally identical to `=digest` (Part B). It
never keeps the strict replicated lease. This is validated healthy across the whole
payload/throughput range:

- **50 KB payload:** 2,400/s, p99 91 ms (parity with leader-durable).
- **Negligible payload:** ~36k/s aggregate, p50 28 ms / p99 88 ms, backlog ~0.

The operator sets nothing; the strict replicated lease remains available via `=1`/`quorum`
for parity/testing but is not recommended at scale.

### Parse / precedence

`1`/`true`/`on`/`yes`/`quorum`/`replicate` → Always (Part-A `true`); `digest` → Digest
(Part B); `auto` → Auto (this part, == Digest behaviour); `0`/`false`/`off`/`no`/`leader-local`/`local`
(and anything unrecognised) → LeaderLocal (Part-A `false`). Unset → `auto` under quorum,
leader-local under leader-durable.



- Code (Part A): `engine-core/src/engine.rs` (`lenient_completion`, relaxed
  completion guards; the flag is replicated state since #1661 —
  `Command::SetLenientCompletion`),
  `server/src/main.rs` (`replicate_activation_from_env`,
  `ServerImpl.replicate_activation`, `try_activate`, `activate_on_local`,
  `tick_partition_via_raft`). Env: `NANOBPMN_REPLICATE_ACTIVATION`.
- Code (Part C, zero-config `auto`): `server/src/main.rs`
  (`ActivationPolicy`, `parse_activation_policy`/`activation_policy_from_env`,
  `ServerImpl.replicate_activation_for`). Env: `NANOBPMN_REPLICATE_ACTIVATION=auto`
  (default under quorum).
- Driver: `~/workspace/nano-demo/scripts/ch2-workers.sh`,
  `driver/src/bin/ch2_workers.rs`.
- Related: ADR 0001 (the peer piggyback channel Part B reuses),
  `docs/distributed-scaling-design.md`, `docs/falcon-design.md`.
