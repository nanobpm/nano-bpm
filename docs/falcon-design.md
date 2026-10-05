# The Falcon Protocol — Job Streaming + Credit Dispatch — Design Proposal

> **The Falcon Protocol** is Nano's command stream. It is named for **Falko Menge**,
> whose work on system optimisation is the genesis and inspiration for this
> subsystem. Nano is a distillation of Camunda Engineering's expertise; its
> subsystems are named for the engineers who created them. Artists sign their work.

> NOTE: §1–12 describe the **job-only** SSE design (first iteration). §13 supersedes
> the endpoint with a **unified bidirectional Falcon protocol** that also carries
> `createProcessInstance` and the full job lifecycle. Read §13 for the current target;
> §1–12 remain as the foundational dispatch/credit/lease reasoning it builds on.

> Status: analysis / proposal only. No code changes made.
> Grounded in: `server/src/main.rs` (activate_jobs_impl, try_activate, jobs_available,
> backpressure), `engine-core/src/engine.rs` (activate_jobs / expire_jobs / completion-by-key),
> `engine-core/src/state.rs` (Job lease: worker, deadline), `scripts/generate.sh` +
> `spec-patches/patches.yaml` (spec generation pipeline).

## 1. Goals
- Replace busy long-poll fan-out with server-initiated push on a persistent SSE connection.
- Make backpressure intrinsic (credit flow control) and coordinated (the server is the single
  dispatcher with a global view), eliminating worker retry/thundering-herd behavior.
- Keep `activateJobs` polling as a coexisting fallback; reuse the existing lease/expiry safety
  net unchanged (no new delivery-guarantee machinery).

## 2. Why it fits this codebase
The hard parts already exist:
- `jobs_available` is a fan-out broadcast (`Notify::notify_waiters()`), already the wake source
  for long-pollers (main.rs:524, 2315).
- `try_activate` already leases on the single engine thread while encoding 50 KB variable
  payloads off it, in parallel across cores (main.rs:2328).
- Job leases carry `worker` + `deadline`, and **completion is by key alone** — a lease can
  safely expire and be reclaimed by `expire_jobs`, and a slow/dead worker's job returns to the
  activatable pool with no special handling (engine.rs:431, 472, 733; state.rs:55-77).

A streaming endpoint is essentially the existing long-poll loop with `return` replaced by a
`yield` into an SSE body — the same wait/wake/lease cycle, never torn down between jobs.

## 3. Core data structures (server-side, outside the engine thread)
```
StreamRegistry {
  streams: DashMap<StreamId, Arc<WorkerStream>>,
  by_type: DashMap<JobType, Vec<StreamId>>,   // dispatch index, round-robin cursor
}

WorkerStream {
  id: StreamId,                    // server-minted (uuid)
  job_type: String,
  worker: String,                  // engine lease owner name
  fetch_variable: Option<Vec<String>>,
  timeout_ms: u64,                 // lease duration requested at activation
  credits: AtomicI64,              // demand; push only while > 0
  tx: mpsc::Sender<StreamEvent>,   // to the SSE response body task (bounded)
  last_seen: AtomicU64,            // heartbeat / liveness
}
```
The registry lives next to `jobs_available` / `backpressure` on `ServerImpl`. The engine core
stays untouched — streaming is purely a new *consumer* of `activate_jobs`.

## 4. Endpoint contract
`GET /jobs/stream?type=<t>&worker=<w>&timeout=<ms>&fetchVariable=...` → `text/event-stream`.

SSE event types:
| event       | direction       | payload                                            |
|-------------|-----------------|----------------------------------------------------|
| `job`       | server→worker   | one `ActivatedJobResult` (same shape as activateJobs) |
| `pressure`  | server→worker   | `{ level, retryAfterMs? }` — fleet coordination signal |
| `heartbeat` | server→worker   | periodic keepalive, carries server time            |

SSE is one-directional, so credits and acks need a back-channel. Two options:
- **(A) Pure-SSE:** credits implied — server pushes ≤ maxJobs, then treats each
  `POST /jobs/{key}/completion` (already exists) as the implicit credit replenish. Simple, but
  couples credit to completion latency.
- **(B) SSE + control POST (recommended):** lightweight `POST /jobs/stream/{id}/credits {n}`
  adds demand explicitly (true reactive-streams). Decouples demand from completion; matches
  Zeebe's `StreamActivatedJobs` model.

## 5. Lifecycle
- **Connect:** worker opens stream → registry inserts `WorkerStream` (`credits = maxJobs`),
  indexes under `by_type`. Immediately attempt a drain (backlog may exist).
- **Dispatch (new loop, replaces "every poller races on try_activate"):** a dispatcher reacts to
  `jobs_available.notified()` and, per job_type, selects eligible streams (`credits > 0`, alive),
  calls `activate_jobs(type, worker, n, timeout, now)` on the engine on behalf of chosen workers.
  Per returned job: `credits -= 1`, encode variables off the engine thread (as `try_activate`
  does), `tx.send(job)`.
- **Replenish:** option B → `credits += n` on control POST; option A → `credits += 1` on
  completion of a pushed job.
- **Disconnect / death:** `tx` closed or heartbeat stale → remove from registry, drop credits.
  Jobs already pushed-but-not-completed are **not** specially handled — their engine lease
  `deadline` expires via `expire_jobs`, returning them to the activatable pool (state.rs:472).
  The existing lease *is* the at-least-once guarantee; no distributed ack protocol needed.

## 6. Dispatch fairness
**Decision: uniform random consumer selection** (Camunda parity — Zeebe's
`RemoteStreamImpl.pickInitialConsumer()` uses `ThreadLocalRandom`). Among the streams of a type
with credits available, pick one at random; on push failure, shuffle the rest and try them in
order. Per tick, cap jobs leased per stream at its credit count. Selection is partition-local
(see §14.5); fairness is statistical (law-of-large-numbers at throughput) rather than strict.
Random is stateless (no shared cursor to contend), trivially correct under concurrent
connect/disconnect, and — because each partition selects independently — round-robin would not
buy global fairness anyway. Round-robin is noted only as an optional low-throughput-evenness
alternative.

## 7. Backpressure integration (the payoff)
- **Per-worker:** a saturated worker stops issuing credits → server naturally stops pushing to
  it. No signal, no 503, no retry.
- **System:** gate the dispatcher on the existing `processing` gauge / `AdaptiveController`. Over
  watermark → dispatcher throttles/pauses leasing and emits one `pressure{level:red}` to all
  streams; recovery emits `pressure{green}`. Edge-triggered, O(streams) once, zero steady-state
  cost.
- **Herd is gone by construction:** there is no per-client failure to synchronize on; "backoff"
  is the server choosing not to push.

## 8. Coexistence with polling
- `activateJobs` stays as-is. Both paths call the same engine `activate_jobs`; the engine's lease
  index serializes who gets a job, so no double-lease across paths.
- Periodic **sweep**: streaming handles hot, newly-created jobs (woken by `jobs_available`); a
  low-frequency timer re-drains `by_type` to catch jobs predating a stream or freed by lease
  expiry. Mirrors Camunda's "stream + poll backstop."

## 9. Concurrency model fit
- Engine remains a single-threaded actor (`engine.with(...)`); the dispatcher calls in exactly
  like `try_activate`, so leasing stays serialized and correct.
- Registry mutations (connect/disconnect/credits) are off-thread on concurrent maps; only the
  lease step touches the engine.
- Variable encoding stays off the engine thread, preserving current parallelism.

## 10. Failure modes & safety
| failure                         | handling                                                  |
|---------------------------------|-----------------------------------------------------------|
| worker dies mid-job             | lease `deadline` expires → `expire_jobs` re-activates      |
| slow consumer                   | bounded `tx`; if full, stop dispatching (credits idle)     |
| pushed-but-unacked on disconnect| same as death: lease expiry reclaims                       |
| duplicate delivery              | at-least-once by design; completion by key, idempotent     |
| stream starvation               | random selection + per-tick credit cap                     |

## 11. API specification patching (generation pipeline)
The Rust REST layer is generated from the bundled OpenAPI spec by `scripts/generate.sh`
(openapi-generator-cli, `-g rust-axum`). The upstream files under `spec/` are kept **byte-for-byte
identical** to the Camunda Orchestration Cluster release they track, so they are never edited
directly. nanobpmn-specific additions are applied as overlays in `spec-patches/patches.yaml` by
`scripts/preprocess-spec.py`, which sanitizes `spec/` into `build/spec/` before generation.

**This feature requires patching the spec during generation** to introduce the new job-stream
endpoint(s), because they do not exist in the upstream Camunda spec. Concretely:
- Add a `spec-patches/patches.yaml` overlay introducing `GET /jobs/stream` (and, for credit model
  B, `POST /jobs/stream/{id}/credits`) under `jobs.yaml` paths, plus any new request/response
  schemas (e.g. a `JobStreamEvent` / `pressure` payload schema).
- The existing patch mechanism supports only `merge` (deep-merge a mapping, e.g. add a property)
  and `append` (append to a list, e.g. a `required` array), per the header of `patches.yaml`.
  Adding a **whole new path item / operation** likely exceeds those two primitives, so
  `preprocess-spec.py` will probably need a small extension (e.g. a `set`/`add-path` directive) to
  inject a new path node. Flag this as a prerequisite task.
- **SSE / `text/event-stream` is not well modeled by openapi-generator's rust-axum target.** The
  generator emits unary handlers; a streaming response body is not something it produces natively.
  Expect to either (a) describe the endpoint in the spec only enough to mint the route + types and
  then hand-implement the streaming body in the server (bypassing the generated unary signature),
  or (b) add a `postprocess-generated.py` fix-up. The control-POST and schemas generate cleanly;
  the streaming GET is the part that needs manual wiring. Keep the upstream `spec/` untouched —
  all of this lives in `spec-patches/` + the post-processors.

## 12. Open questions
1. Credit model **A vs B** (implicit completion-driven vs explicit control POST). Recommend B.
2. What pressure workers should see — engine `processing` saturation only, or also
   activatable-job starvation? Defines `pressure` event semantics.
3. Lease timeout ownership — server-chosen per stream vs worker-supplied `timeout`. Affects
   reclaim latency on death.
4. Heartbeat interval / liveness threshold vs SSE proxy idle timeouts.
5. Create-side backpressure stays a separate rail (current `createProcessInstance` 503); decide
   whether `pressure` events should also advise creators or only workers.
6. Generator extension scope — how much of the SSE endpoint to model in-spec vs hand-wire.

---

## 13. Unified bidirectional Falcon protocol (supersedes the job-only endpoint)

### 13.1 Rationale
Putting only job push on a stream leaves `createProcessInstance` backpressure on a separate
rail (the `503 RESOURCE_EXHAUSTED` in main.rs:361-373). Moving **create + the job lifecycle**
onto one stream unifies all engine-bound write traffic under a single credit window and a single
fair-share scheduler over the one engine thread — the strongest version of "coordinate
distributed workers, no thundering herd." Reads (`search*`/`get*`, answered from the
eventually-consistent read model) and low-rate control ops (deploy, admin) **stay on REST** to
preserve Camunda drop-in compatibility, statelessness, cacheability, and the generate-from-spec
workflow.

### 13.2 Transport consequence (important)
The job-only design used SSE (server→client) + a control POST for credits. Once the client must
*send* high-frequency commands (create/complete/fail), a one-directional SSE channel is no longer
enough — doing each as a separate POST defeats the purpose. The unified stream therefore requires
a genuinely **bidirectional** transport:
- **WebSocket (recommended):** first-class axum support, stays on the HTTP stack/port, easy
  framing of a tagged-union message type.
- **gRPC bidi:** Zeebe-native (Camunda's original gateway was exactly this), but adds a second
  server stack alongside the REST/axum layer.

Either way the endpoint is **not expressible in the OpenAPI REST spec** and lives entirely
outside the generated rust-axum surface (see §13.8).

### 13.3 Endpoint
Rename `/jobs/stream` → `/falcon` (single bidirectional connection per client). It
multiplexes two interaction patterns over one socket: **demand/push** (jobs) and
**request/response** (create, complete, fail), correlated by `corr` id.

### 13.4 Frame protocol (tagged union)
Client→server:
- `Subscribe { jobType, jobCredits, fetchVariable, timeout }` — opt into job push for a type
  (`timeout` is the per-job activation lock in ms; null/non-positive applies a 60 s server
  default so a job is never leased with a zero lock, which would re-dispatch it instantly)
- `CreateInstance { corr, processDefinitionId|key, version, variables, awaitCompletion }`
- `CompleteJob { corr, jobKey, variables }`
- `FailJob { corr, jobKey, retries, retryBackoff, errorMessage }`
- `ThrowError { corr, jobKey, errorCode, errorMessage, variables }`
- `JobCredits { jobType, n }` — replenish job-push demand
- `AwaitCompletion { corr, processInstanceKey }` — (re-)assert interest in an instance's terminal
  result; used on reconnect to resume a lost await (see §14.8)
- `Heartbeat`

Server→client:
- `Job { jobKey, ... }` — pushed activated job (consumes one job-delivery credit)
- `CommandResult { corr, status, body }` — ack/result for create/complete/fail/throwError
- `InstanceCompleted { corr, processInstanceKey, variables }` — async await-completion event
- `SubmissionCredits { n }` — grants client intake capacity (create-side backpressure lever)
- `Pressure { level, retryAfterMs? }` — coarse fleet signal
- `Heartbeat`

### 13.5 Two credit lanes
| lane                  | granted by      | gates                              | purpose                          |
|-----------------------|-----------------|------------------------------------|----------------------------------|
| Job-delivery credits  | worker → server | server *pushing* jobs              | worker-side flow control (§4-7)  |
| Submission credits    | server → client | client *sending* engine-bound writes | **unified create-side backpressure** |

- Client must hold a submission credit to send a `CreateInstance`. The server replenishes
  submission credits from engine `processing` headroom via the existing `AdaptiveController`.
  Under saturation it **withholds** credits → the client stalls intake → no 503, no client retry,
  no herd. This is how `createProcessInstance` joins the same coordinated window as activation.
- **Asymmetry — meter intake, not drain:** `CreateInstance` (and message publish) consume the
  scarce submission lane; `CompleteJob`/`FailJob`/`ThrowError` flow on generous/unmetered credit
  because completing jobs *reduces* backlog — throttling drain would worsen overload. (Mirrors
  Zeebe: backpressure applies to user commands, not job completion.)

### 13.6 await-completion becomes async (and cheaper)
Today a REST create with `awaitCompletion` holds the HTTP request open until the exporter signals
a terminal state (`instances_changed` Notify, main.rs:80) or it times out, returning
`processCompleted` (the spec-patch field, see patches.yaml). On the stream:
1. `CreateInstance{corr, awaitCompletion:true}` → immediate `CommandResult{corr, key,
   processCompleted:false}`.
2. Later `InstanceCompleted{corr, variables}` emitted when the exporter projects the terminal
   event, routed back by `corr`.

This removes the long-held request future per awaiting instance — strictly cheaper under load.
The stream keeps a `pending_completions: Map<corr, processInstanceKey>` so the exporter wake
(`instances_changed`) can fan out completion frames to the right connections. Because completion is
sourced from the durable read model (not a parked request), the await is **resumable**: on
reconnect a client re-asserts `AwaitCompletion{corr, processInstanceKey}` and is answered
immediately if the read model already shows terminal, else re-registered. This makes
await-completion durable across reconnect/failover — a parity advantage over Zeebe (see §14.8).

### 13.7 Engine integration & ordering
- All engine-bound writes (create/complete/fail/throwError) already funnel to the single
  command/journal thread today; the stream demultiplexer is just a different *ingress* to that
  same path — no new engine semantics. The journal continues to serialize commands.
- Per-client ordering is preserved by processing one connection's frames in arrival order;
  independent connections interleave freely at the journal.
- The `ClientStream` registry entry from §3 grows: per-type `job_credits`, a `submission_credits`
  AtomicI64, and the `pending_completions` correlation map. Job dispatch (§5-6) and lease/expiry
  safety (§10) are unchanged — a pushed job that is never completed still reclaims via lease
  `deadline` expiry.

### 13.8 Spec / codegen impact (updates §11)
- WebSocket/gRPC-bidi cannot be modeled in the OpenAPI REST spec, so `/falcon` is
  **hand-wired** in the server and lives entirely outside the generated rust-axum surface. At most
  add a documentation-only stub path via `spec-patches/` for discoverability; the framing types,
  the socket handler, and the demux/credit scheduler are all hand-written.
- The REST `createProcessInstance`, `activateJobs`, `completeJob`, `failJob` endpoints **remain**
  (generated, Camunda-compatible) as the fallback/compat path; the stream is an additive hot path.
  Both ingresses share the same engine command path, so a command is never double-applied.
- Net spec-patch work is therefore smaller than the job-only plan assumed for the *streaming*
  part (nothing meaningful to express in OpenAPI), but the hand-written server surface is larger.

### 13.9 Revised open questions
- Transport: **WebSocket vs gRPC bidi** (recommend WebSocket to stay on one stack).
- Should message publish / `setVariables` also be metered on the submission lane, or only create?
- Submission-credit policy: fixed window vs AIMD off `processing`; per-connection vs global fair
  share across connections.
- Reconnect/resumption: on socket drop, how are in-flight `corr`s and pending completions
  recovered (replay by key over REST? client re-create?).
- Back-compat: do we publish a nanobpmn SDK for the stream, or document the frame protocol only?

---

## 14. Distributed load-balancing & fault tolerance — Camunda parity analysis

> Compares nanobpmn's current single-node model against Zeebe/Camunda 8's distributed
> architecture, to decide what parity is worth chasing alongside the Falcon work.
> Camunda mechanisms below are cited from the `camunda/camunda` monorepo.

### 14.0 Where nanobpmn stands today
- **One partition, no replication.** Topology is hardcoded: `clusterSize 1, partitionsCount 1,
  replicationFactor 1`, a single broker (node 0) leading partition 1 (main.rs:1130-1152).
- **Gateway and broker are fused** in one process; the engine actor is a single-writer command
  queue that explicitly "mirrors Zeebe's per-partition `StreamProcessor`" (deepthi.rs).
- **Durability without fault tolerance:** append-only journal, group-commit + fsync, replay on
  restart (journal.rs). RF=1 means a node loss is an outage, not data loss-on-quorum.
- **Volatile leases:** activation locks are *not* journaled; a crash forfeits all locks and
  workers re-activate; completion is by key (idempotent) (journal.rs, engine.rs:733).

In Zeebe terms, **nanobpmn ≈ a single Zeebe partition with RF=1 and no cluster.** That makes the
parity question tractable: most of the per-partition semantics already match; the gaps are
*multi-partition routing*, *replication/consensus*, and *gateway/broker separation*.

### 14.1 Existing parity wins (keep — do not regress)
These already match Zeebe by design and the Falcon work must preserve them:
- **Engine actor = StreamProcessor.** Single serial writer per partition; the journal serializes
  commands. Identical concurrency model.
- **At-least-once via lease/deadline reclaim.** Zeebe stores `deadline = now + timeout` in the
  JOB ACTIVATED record and a timeout scan returns expired jobs to ACTIVATABLE; nanobpmn does the
  same with `expire_jobs` + completion-by-key (state.rs:472, engine.rs:733). **This is exactly
  the streaming safety net** (§10): a job pushed to a dead/slow worker reclaims on lease expiry.
- **Stream-on-failover semantics.** In Zeebe, stream registry is in-memory (not Raft-replicated);
  on leader change jobs stay ACTIVATED until their deadline, then reclaim. nanobpmn's volatile
  leases give the identical behavior for free.
- **Separate eventually-consistent read model.** Matches Zeebe's exporter/Operate split; reads
  never touch hot engine state (main.rs `store`).

### 14.2 Gap 1 — Partitioning (highest value, achievable single-node)
Zeebe shards into N partitions; an instance lives on one partition for its lifetime. Routing
(docs + `RoundRobinDispatchStrategy`, `PartitionUtil`):
| command | routing |
|---|---|
| `createProcessInstance` (plain) | round-robin across leaderful partitions (random initial offset 0-127 to de-sync gateways) |
| create-with-result / businessId, message correlation | `abs(hash(key) % partitionCount) + 1` |
| timer-start, deployments | always partition 1 (deployment then replicated to all) |
Keys embed the partition: 13-bit partitionId + 51-bit local key (`Protocol.java`).

**nanobpmn path:** run N independent engine actors + journals (N single-writer threads) behind
the gateway — *partitioning is achievable on one node first*, then across nodes later. Adopt the
partition-embedded key encoding now (cheap, future-proofs distribution). Route at the
gateway/falcon demux: round-robin creates, hash messages/businessId, pin deploy+timer to
partition 1. This is the single biggest parity lever and composes cleanly with the command
stream (the demux already exists; it just gains a partition selector).

### 14.3 Gap 2 — Replication & consensus (highest cost)
Zeebe runs multi-Raft (one Raft group per partition, modified Atomix); commit = quorum
(`floor(RF/2)+1`) durable-append; the StreamProcessor only applies committed entries; failover is
Raft election on heartbeat loss. RF=3 tolerates one broker loss with no data loss.

nanobpmn has *no* replication today. **HA is in scope (decision §14.7)**, so option 3 (real Raft)
is the committed target; options 1–2 are interim stages, not end states:
1. **RF=1 + fast replay** (status quo, *interim*): durable on a single disk, node loss = downtime
   until restart/replay. Acceptable only as the pre-HA baseline.
2. **Async log shipping / read replicas** (*interim*): stream the journal to a follower for warm
   standby; not true consensus (small window of loss on failover) but cheap fault tolerance to
   bridge toward Raft.
3. **Real Raft per partition** (e.g. the `openraft` crate) wrapping the journal as the replicated
   log, applying only committed entries to the engine actor — **the committed HA target**. Sequence
   it *after* partitioning, and keep the journal's commit/ack boundary as the integration seam (the
   `Commit` future already models "durable before we 200" — Raft replaces fsync-only with
   quorum-append). RF=3 tolerates one node loss with no data loss, matching Zeebe.

### 14.4 Gap 3 — Gateway / broker separation
Zeebe's gateway is **stateless**: it holds open client (job) streams, learns topology via Atomix
gossip (`BrokerTopologyManager.getLeaderForPartition`), routes each request to the partition
leader, and retries the next partition on `PARTITION_LEADER_MISMATCH`/`RESOURCE_EXHAUSTED`. The
`getTopology` endpoint already exists in nanobpmn's spec but returns the hardcoded single broker.

**nanobpmn path:** the Falcon protocol makes this natural — the gateway terminates the worker/
client connections and fans frames to partition leaders. Single-node: gateway + N partitions in
one process (no network). Multi-node later: gateway becomes a thin router + topology client. Make
`getTopology` report real partition/leader state once partitioning lands, so standard Camunda
SDKs route correctly.

### 14.5 Gap 4 — Distributed job streaming
Zeebe registers each worker's stream with **all** brokers (`ClientStreamer.add` → AddStreamRequest
to every node); each partition leader pushes its own jobs; the gateway aggregates; consumer
selection is **uniform random** within an `AggregatedRemoteStream` of identical-metadata streams;
on failover streams re-register within ~1s (`ClientStreamServiceImpl.onServerJoined`).

**Parity for nanobpmn's Falcon protocol:** when partitioned, a worker's `Subscribe` must fan out
to **every partition**, and each partition pushes independently into the one client connection.
**Decision: uniform random selection (matches Zeebe, see §6)** — partition-local, no global
coordinator. Credit accounting must be **per-connection, not per-partition**, so a worker's
`jobCredits` cap is honored across all partitions pushing to it (otherwise N partitions each push
up to the full credit = N× overrun). Because the partitions push concurrently into one shared
credit budget, the per-connection counter must be atomic (a partition decrements-then-pushes,
rolling back on a full `tx`). This is a concrete design constraint the implementer must handle.

### 14.6 Gap 5 — Fault-tolerance boundaries (mostly already aligned)
| concern | Zeebe | nanobpmn today | parity gap |
|---|---|---|---|
| in-flight job on worker death | deadline scan → re-activatable | `expire_jobs` → re-activatable | **none** |
| pushed job, leader fails mid-push | stays ACTIVATED until timeout | volatile lease forfeited on crash | none (equivalent) |
| committed command durability | quorum-replicated | fsync on one node | **needs Raft (14.3)** |
| uncommitted command on failover | client retries next partition | n/a (single node) | needs gateway retry (14.4) |
| completeJob idempotency | stale key rejected | completion-by-key | **none** |
| delivery guarantee | at-least-once | at-least-once | **none** |

The delivery/idempotency/lease story is **already at parity**; only *command durability under node
failure* (Raft) and *cross-partition retry* (gateway) are missing.

### 14.7 Recommended sequencing
**Target decision: true multi-node HA is in scope** — per-partition Raft is committed work, not
optional. Sequencing still front-loads the cheaper levers so throughput/load-balancing land first
and multi-node arrives incrementally:
1. **Partition-embedded key encoding** now (cheap, unblocks everything; do it before keys leak
   into clients). Adopt Zeebe's 13-bit partition / 51-bit local split for wire-compatibility.
2. **N-partition single-node** (N engine actors + journals) with gateway-side routing + per-
   connection credit accounting; make `getTopology` truthful.
3. **Distributed job-stream fan-out** (subscribe-to-all-partitions, per-connection credits,
   random selection).
4. **Replication / HA** — async log-ship for warm standby as an interim, then real per-partition
   Raft (`openraft`) at the journal's commit seam. The `Commit` future already models "durable
   before we 200"; Raft replaces fsync-only with quorum-append, applying only committed entries to
   the engine actor. Gateway gains `PARTITION_LEADER_MISMATCH`-style retry-next-leader on
   failover.

This orders parity by value/cost: partitioning + gateway routing get most of the
load-balancing/throughput story on one node and set up multi-node cleanly; Raft is the final,
larger HA investment but is **in scope**. Crucially, nanobpmn's volatile-lease design already gives
Zeebe's at-least-once + stream-failover semantics for free, so the streaming work does not need to
invent a delivery-guarantee layer.

### 14.8 Durable, resumable await-completion (a parity *advantage* over Zeebe)
**Zeebe's limitation:** `CreateProcessInstanceWithResult` delivers its result through an
**in-memory, connection-bound** subscription on the partition leader/gateway. A leader failover or
gateway/client disconnect during the await **loses the result delivery** even though the instance
completes durably — the caller just times out, and the result isn't retained for keyed lookup
(recovery means querying the exporter/Operate). Await completion is best-effort and not resumable.

**nanobpmn mitigation via the streaming architecture.** Await is already sourced from the
exporter's `instances_changed` Notify (main.rs:80) — *the single durable point through which all
completion/termination events flow* — i.e. from the **durable, replayable read-model projection of
the terminal event**, not from a thread parked on the request future. This makes await completion
durable and resumable:
1. **Decoupled** (§13.6): create returns an immediate `CommandResult{key}`; completion arrives
   later as an async `InstanceCompleted{corr}` frame. No held request to lose.
2. **Resumable across reconnect:** on reconnect the client re-asserts
   `AwaitCompletion{corr, processInstanceKey}`; the gateway answers **immediately if the read model
   already shows terminal**, else registers for the next `instances_changed` wake. The result is
   never lost — at worst delayed until reconnect.
3. **Survives failover (with HA):** after a leader change the new leader's exporter still projects
   the terminal event; delivery keyed by `corr`/instanceKey against durable state is at-least-once,
   not best-effort.

**Requirement:** the pending await must be recoverable. Simplest is **client re-asserts on
reconnect** (the client remembers its outstanding instance keys) — robust and stateless on the
server; optionally persist the pending-await set server-side. Either way this is strictly better
than Zeebe's ephemeral result delivery and should be called out as a differentiator.

### 14.9 Remaining open questions
- Per-connection credit enforcement across partitions — central atomic counter on the gateway vs
  per-partition sub-allocation of a connection's budget.
- await-completion recovery: client-re-assert only, or also server-persisted pending set (needed
  if a client can crash *and* must still be notified via some other channel)?
- Raft: embed `openraft` vs port Atomix semantics; flush/quorum tuning to match Zeebe's
  durability knobs.

---

## 15. Storage & durability architecture vs Zeebe

> Grounded in: `server/src/journal.rs` (JSONL WAL + group-commit fsync, apply/export/commit
> ordering), `server/src/readstore.rs` (SQLite read model / export target), `server/src/varspill.rs`
> (SQLite cold-variable spill cache), and the in-memory event-sourced engine in `engine-core/`.

### 15.1 The three stores (none is RocksDB)
nanobpmn has **three on-disk artifacts** — one JSONL log and **two separate** SQLite databases —
plus RAM-resident engine state. They are frequently conflated; they are distinct:

| layer | nanobpmn | role | Zeebe equivalent |
|---|---|---|---|
| **Journal** | **JSONL file** on disk, group-commit + fsync (journal.rs) | source of truth / WAL | Raft-replicated **log (ledger)** — but nanobpmn's is **local, not replicated** |
| **Engine state** | **in-memory** HashMaps, event-sourced (engine-core) | hot execution state | **RocksDB** — but Zeebe's is **on-disk LSM**, nanobpmn's is **RAM** |
| **VarSpill** | optional **SQLite on disk** (varspill.rs) | offload of *cold variable payloads only*, to bound RAM | partial analog of RocksDB disk-residence; a **derived cache**, not the state store |
| **ReadStore** | **SQLite on disk** or `:memory:` (readstore.rs) | CQRS read model — the **export target**; `search*`/`get*` read here | the **exporter → Elasticsearch/Operate** store |

Disambiguation of the common confusion:
- **Exporting goes to SQLite on disk** — the ReadStore, a CQRS projection, eventually consistent.
- That is **not** "RocksDB with a spill": the engine's RocksDB-equivalent is **in-memory + the
  optional varspill SQLite** (cold variables only); the export is a **separate** SQLite DB.
- "RocksDB-in-memory + a JSONL file" describes the **engine state (RAM) + journal (WAL)** — the
  execution/durability side — **not** the export, which is a third store.
- **varspill ≠ RocksDB:** it offloads one expensive field and is `synchronous=NORMAL`. It is a
  cache *relative to snapshots*: every classic snapshot folds spilled payloads and cold instances
  back in (`Journal::engine_snapshot`), so a snapshot never depends on it. Its rows are not wiped
  on open, because snapshots written before that fold (#1331) still reference them.

### 15.2 Command ordering: apply-then-commit (gap vs Zeebe's commit-then-apply)
Current path (journal.rs:271-280, 235-265):
1. `engine.apply_command_at` — **in-memory mutation**, produces events.
2. `persist` — **forwards events to the exporter/read model** (journal.rs:244), then queues bytes
   to the durable writer.
3. caller awaits `Commit` = **local `write`+`fsync`**, *then* returns 200.

So nanobpmn **applies (and exports) before durability is confirmed**, and "commit" is a single-node
fsync. Zeebe is the inverse: a record is **Raft-committed (quorum) before** the StreamProcessor
applies it. Two distinct gaps:
- **No distributed commit** — single-disk fsync, not quorum-append.
- **Ordering is unsafe for HA** — effects (ack *and* read-model export) are exposed before the
  events are durably/quorum-committed. On one node a crash loses both, so it is currently correct;
  under replication it is not.

### 15.3 HA-critical changes (feed into §14.3 Raft work)
The Raft work in §14.3 is **not** just "add replication under the journal." It must also:
1. **Reorder to commit-then-apply (or commit-then-expose).** Replicate the events to the Raft log
   and gate **both** the request ack **and** the exporter handoff on quorum commit. Concretely, move
   the `exporter.send` (journal.rs:244) to *after* the `Commit` resolves, so the read model never
   reflects uncommitted events. The `Commit` future is the right seam — it already means "durable
   before we 200"; HA redefines "durable" from fsync to quorum-append.
2. **Replicate events, not commands.** nanobpmn journals *outputs* (the events a command produced),
   so followers apply committed events deterministically to rebuild state — they do not re-run
   command logic. This keeps replicas consistent without requiring deterministic re-execution.
3. **Add state snapshotting.** Recovery today is a **full journal replay** (O(history)); there is no
   snapshot. Zeebe periodically snapshots RocksDB and replicates the snapshot so restart/failover
   skips replaying the whole log. HA needs periodic engine-state snapshots (+ snapshot transfer to
   new/lagging replicas) to bound restart and catch-up time. The read model already persists
   `exported_position` for warm restart (readstore.rs), but the **engine state** has no equivalent.

### 15.4 Memory model trade-off (not a durability concern, but parity-relevant)
- Engine state is **RAM-resident** — fast for the working set, but bounded by RAM unless varspill is
  enabled, and even then **only variables spill**, not full state (instances, jobs, tokens stay in
  RAM). Zeebe keeps *all* execution state on disk (RocksDB) by default, trading latency for
  unbounded-by-RAM capacity.
- For very large *active* backlogs this is the relevant scaling axis; varspill (and the variable
  spill memory the benchmark notes reference) is nanobpmn's answer, but a full RocksDB-style
  on-disk state tier is the deeper parity item if RAM-bound capacity becomes a constraint.

### 15.5 Open questions
- Snapshot format & cadence: serialize engine state how (and how often), and how to transfer to a
  joining/lagging Raft follower?
- Replicate the JSONL events as-is over Raft, or a more compact framed encoding for the log?
- Read-model placement under HA: per-node local SQLite projection (each replica exports
  independently from the committed log) vs a single shared/external read store?
