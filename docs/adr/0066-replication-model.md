# ADR 0066 — Replication model: replicate events, not commands

Status: **Proposed.** (A human accepts it; implementation slices follow acceptance.)
Date: 2026-11-18.
Relates to:
ADR 0001 (`0001-cluster-job-activation-fairness.md`),
ADR 0002 (`0002-leader-local-activation-and-lease-digest.md`),
ADR 0003 (`0003-write-path-durability-tiers.md`),
`docs/falcon-design.md` §15.3,
issues #1655 (this decision), #1657 (cluster variables unreplicated), #1642 / #1649 (FEEL
non-determinism), #1071 (migration-by-replay), #1069 (golden serde drift guard).

## Context

### Current state: command replication

The Raft log carries **commands**, and every replica re-runs the engine on them:

- `server/crates/nano-server-raft/src/raft.rs:106` — `pub struct ReplicatedCommand { command: Command,
  now: u64, operation_reference: Option<i64> }`. `ReplicatedBatch { items: Vec<ReplicatedCommand> }`
  (`raft.rs:286`) is the entry payload. The leader's clock reading (`now`) is the only
  non-command input carried in the entry.
- `raft.rs:1906` — `RaftStateMachine::apply` for `PartitionStateMachine` runs on **every** replica.
  For each committed batch it calls `journal.apply_command_ref_at(command, now, operation_reference)`
  (`raft.rs:1997`): FEEL evaluation, key allocation, gateway branching and variable merging all
  happen again on each follower. The journal then persists the events the replica itself produced.
- Leader-driven ticks are commands too: `Command::TriggerTimers { now }` and `Command::ExpireJobs
  { now }` (`engine-core/src/command.rs:273-282`), proposed by the leader's pump
  (`server/src/main.rs` around `has_due_timers`, line ~20726) and re-executed everywhere.
  Orphan detection inside apply is already special-cased as "leader-proposed with a replicated
  timestamp" (test `orphan_detected_in_apply_is_leader_proposed_with_replicated_timestamp`,
  `raft.rs:7166`) — a symptom of the model: anything a replica could compute differently has to be
  hoisted to the leader by hand.
- **Node-local input read during apply (#1657).** `ClusterVariableStore` (`server/src/main.rs:397`)
  is an in-memory, unjournaled store mutated directly by REST handlers. Its live handle is
  installed into every journal/engine (`main.rs:3436` →
  `nano-server-storage/src/journal.rs:3881` `set_cluster_variables`) and read by
  `overlay_cluster_variables` (`engine-core/src/engine/resolve.rs:444`) **while applying replicated
  commands**. Two replicas holding different cluster variables evaluate the same command to
  different events: split-brain, and replay re-evaluates history against today's values.

### The drift

`docs/falcon-design.md` §15.3 item 2 says: *"Replicate events, not commands … followers apply
committed events deterministically to rebuild state — they do not re-run command logic."* The
implementation went the other way and no ADR recorded that. This ADR closes the drift.

### Why command replication is fragile

Command replication makes replica consistency equivalent to **engine determinism over every input
an apply can observe**. That surface is open-ended: FEEL built-ins (`now()`, `today()`,
`random number()`, `uuid()` — #1642, fixed by #1649's deterministic evaluation context), hash-map
iteration order, float formatting, any new built-in, any dependency upgrade (`engine-core/Cargo.lock`
already triggers the FEEL differential gate for this reason), and any host-injected state (#1657).
#1649's replica-agreement test catches only the paths it exercises; each uncovered source is a
latent split-brain found in production.

### How Zeebe does it (camunda/camunda @ bb0521e)

- `zeebe/stream-platform/src/main/java/io/camunda/zeebe/stream/impl/StreamProcessorMode.java` —
  two modes. `PROCESSING` (leader): replay existing events, then process commands. `REPLAY`
  (followers): *"all events are replayed and commands are never processed"*.
- `.../stream/impl/ProcessingStateMachine.java` — on the leader, a command is processed **once**:
  FEEL is evaluated, keys are allocated, branches are taken, and the results are written as
  **follow-up records** (events, plus follow-up commands) in the same batch as the command.
- `.../stream/impl/ReplayStateMachine.java` — on followers (and on leader recovery) only events are
  applied, through `EventAppliers`; there is no command logic on this path.
- `.../stream/impl/state/DbKeyGenerator.java` — keys are generated from leader state and carried in
  the records; replay advances the key generator from the keys it sees, it never allocates.
- Consequence: FEEL non-determinism cannot diverge replicas. `ZeebeFeelEngineClock` wraps the
  controllable stream clock for testing/clock control, not replica agreement, and
  `random number()` is simply random.

### Measured: log size and bandwidth

`engine-core/examples/replication_size.rs` (added with this ADR) runs representative lifecycles
through the engine and serializes, with the same `serde_json` encoding the Raft log store uses
(`raft_logstore.rs:457`), the commands each step replicates today versus the events those commands
produced. Activation is excluded (leader-local per ADR 0002 in both models).
`cargo run --release --features serde --example replication_size`:

| workload (one instance lifecycle) | commands (B) | events (B) | events/commands | events |
|---|---:|---:|---:|---:|
| 1 task, no vars | 125 | 1758 | 14.06x | 18 |
| 1 task, 1 KB create | 1166 | 2799 | 2.40x | 18 |
| 1 task, 50 KB create (the existing benchmark payload) | 51342 | 52975 | 1.03x | 18 |
| 1 task, 50 KB create + 50 KB complete | 102559 | 104247 | 1.02x | 19 |
| 5 tasks, no vars | 304 | 4514 | 14.85x | 46 |
| 5 tasks, 1 KB create + 1 KB per complete | 6550 | 11035 | 1.68x | 51 |

| deploy | command (B) | events (B) |
|---|---:|---:|
| 1-task process | 1002 | 1119 |
| 5-task process | 2450 | 2567 |

Reading: the event form costs a fixed ~1.6 KB of structural events per lifecycle (~95 B/event at
~18 events for a one-task instance) plus the payload once. Payload-dominated workloads (the 50 KB
benchmark) are within **3 %**; deploys within **~12 %**. Tiny-payload workloads grow **~14x** — but
from ~125 B to ~1.8 KB per lifecycle, i.e. ~18 MB/s of replication per 10k lifecycles/s per
follower, well inside a LAN link; the per-entry overhead (one entry per batch, ADR 0003's group
commit) is unchanged. Notably, **the journal already persists exactly these events on every
replica today**, so local disk write volume does not change; only the Raft log entry (and the
network) grows. Structural compaction of the event encoding (dropping redundant fields, binary
framing) is an optimization available later and is not needed for the decision.

## Options

1. **Event replication (Zeebe model).** The leader applies a command batch once, buffering the
   produced events (the existing `begin_raft_entry` buffering), and proposes **the events** (with
   the per-command responses) as the Raft entry. Followers fold committed events into state.
2. **Formally adopt command replication.** Keep the model, rewrite `falcon-design.md` §15.3, and
   declare engine determinism a hard invariant guarded by CI (replica-agreement test of #1649,
   `replay_determinism_differential`, FEEL differential gate), banning host-injected state during
   apply.

| dimension | 1. events | 2. commands |
|---|---|---|
| Determinism surface | Followers are a pure fold over events; FEEL, iteration order, floats, dependency upgrades cannot diverge replicas. Leader-only non-determinism is acceptable (it is recorded). | Every apply code path, forever. Guards catch only exercised paths; host state (#1657) is a design-level violation that tests did not catch. |
| Log size / bandwidth | Measured above: +2–3 % at benchmark payloads, ~14x on tiny instances (~1.6 KB/lifecycle absolute). | Smallest entries. |
| Leader-only processing / activation (ADR 0001, 0002) | Natural fit: activation and lease digest are already leader-local; `JobActivated` becomes an event like any other. | Works today; activation commands are re-executed on followers. |
| Leases | Lease grant/expiry is decided on the leader and recorded; followers never evaluate a deadline. | Followers must agree on `now` (carried in the entry) — fine until a new clock read slips in. |
| Timers, `ExpireJobs`, routing pump | Remain leader-proposed *commands to the leader's processor*; only their resulting events replicate. Removes the hand-hoisting needed for orphans. | Leader proposes ticks; every replica re-evaluates them. |
| Durability tiers (ADR 0003) | Unchanged semantics: `quorum` = events committed before ack; `leader-durable` (`acks=1`) = leader journal fsync before ack, events then replicate asynchronously — a failover can lose the unreplicated tail exactly as today. Simpler to reason about: the leader's journal *is* the entry. | Unchanged. |
| Snapshot / replay / #1071 | Aligns: migration-by-replay already replays **event** journals (`decode_event_json`); the Raft log and the journal become one shape. `golden_serde_drift` / `golden_replay` now guard the replication frame too (one guard, not two). | Two persisted shapes to keep compatible: the command wire (`ReplicatedCommandWire` legacy promotion) and the event journal. |
| Rolling upgrade / cutover | Requires an entry-format version (see plan). Mixed old/new cluster cannot share a log in both directions; needs a gated cutover. | None. |
| Effort | Medium-large: entry type, leader/follower apply split, response plumbing, event-apply path in engine-core (most exists: replay already folds events), cutover. | Small doc change plus #1657 fix, then perpetual vigilance. |

## Decision (proposed)

**Adopt event replication (option 1)**, staged so each slice ships independently and the cluster
stays correct at every step. `falcon-design.md` §15.3 item 2 is kept as the target and annotated
with this ADR as the decision of record.

### Staged migration plan (each slice is fileable as an issue)

1. **Event-apply path parity.** Ensure `engine-core` exposes a single canonical "apply events to
   state" fold used by replay *and* (later) by followers, and add a differential test: for any
   command sequence, `state(process commands)` == `state(fold produced events)`. This test is the
   invariant that makes the switch safe. No wire change.
2. **Versioned entry payload.** Introduce `ReplicatedEntry::{Commands(ReplicatedBatch) /* v1 */,
   Events(EventBatch) /* v2 */}` with decode support for both; still only *emit* v1. Ship this to
   every node first (rolling-upgrade-safe: new nodes read both). Golden corpus covers the new
   frame (additive variant — no `SNAPSHOT_FORMAT_VERSION` bump).
3. **Leader processes, proposes events.** Behind a cluster-wide feature flag: the leader applies
   the command batch once against its live engine (buffered, as `begin_raft_entry` does today),
   proposes `Events` carrying the events and per-command responses; followers fold. Leader
   rollback on a failed proposal reuses the existing fail-closed path (rebuild from journal). Keys
   come only from the leader; the fold advances the key generator from observed keys.
4. **Cutover.** Flip the flag once every voter runs ≥ slice 2 (membership/version check refuses
   the flip otherwise). Old v1 entries still in the log are applied by command replay; nothing
   is rewritten.
5. **Remove follower command execution** after one release: v1 decode stays (historical logs),
   follower command processing and the `now`-hoisting special cases (orphan detection) are deleted.
6. **Measure** replication bandwidth and p99 latency on the existing cluster benchmarks before/after
   slice 4; optional encoding compaction if tiny-payload overhead matters.

Until slice 4 lands, the determinism guards from #1649 remain required and must not be weakened.

### How #1657 (cluster variables) is solved

Independently of, and **before**, the cutover: cluster-variable mutations become replicated engine
commands producing new additive events (`ClusterVariableCreated/Updated/Deleted`, carrying `kind`),
and cluster-variable state moves into engine state (snapshotted, rebuilt by replay). The host-side
live overlay handle (`set_cluster_variables`) is removed. This is required under *both* models —
it fixes data loss on restart and historical replay — and under event replication it additionally
means a read during processing happens only on the leader at a log position, with the result
recorded in events, so a follower can never re-evaluate it against different values.

## Consequences

- Replica consistency no longer depends on engine determinism; FEEL may stay naturally
  non-deterministic on the leader (matching Zeebe). Determinism is still wanted for *replay
  parity tests*, not for correctness of replication.
- Raft entries grow (measured above); disk volume is unchanged since the journal already stores
  these events.
- The command wire format (`ReplicatedCommandWire`) becomes legacy-read-only after slice 5.
- One persisted, replay-guarded shape (events) for journal, Raft log and migration-by-replay.

## Open questions

- Should the Raft log and the event journal be merged into one store (Zeebe's single log) after
  cutover, removing the duplicate write? Out of scope; follow-up ADR if pursued.
- Exact per-command response encoding inside `EventBatch` (needed for the SDK reply on the leader
  only — possibly not replicated at all).
- Whether `ActivateJobs` commands that still flow through the log should be retired entirely in
  favour of the ADR 0002 leader-local path plus `JobActivated` events.
