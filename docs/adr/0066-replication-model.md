# ADR 0066 — Replication model: event replication vs. command replication with enforced determinism

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

The options compared are **A** event replication (Zeebe), **B** today's command replication as-is,
and **C** command replication with *enforced* determinism (TigerBeetle / FoundationDB style). The
apply-input audit below is the evidence that decides between A and C.

### Current state: command replication

The Raft log carries **commands**, and every replica re-runs the engine on them:

- `server/crates/nano-server-raft/src/raft.rs:106` — `pub struct ReplicatedCommand { command: Command,
  now: u64, operation_reference: Option<i64> }`. `ReplicatedBatch { items: Vec<ReplicatedCommand> }`
  (`raft.rs:286`) is the entry payload. The leader's clock reading (`now`) and the terminal
  attribution the leader stamps at proposal time (`stamp_for_proposal`, `raft.rs:2836` — worker
  and lock deadline resolved from leader state) are the only non-command inputs carried in the
  entry.
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

### Measured: payload size (uncompressed, pre-batching)

`engine-core/examples/replication_size.rs` (added with this ADR) runs representative lifecycles
through the engine and serializes, with the same `serde_json` encoding the Raft log store uses
(`raft_logstore.rs:457`), the command frame each step replicates today versus the events those
commands produced. The command side counts the full `ReplicatedCommand` frame (`{command, now,
operation_reference}`, `raft.rs:106`) including the terminal attribution the leader stamps at
proposal time (`stamp_for_proposal`, `raft.rs:2836`) — the worker/deadline every replica carries.
Activation is excluded (leader-local per ADR 0002 in both models; see the options table).
`cargo run --release --features serde --example replication_size`:

| workload (one instance lifecycle) | commands (B) | events (B) | events/commands | events |
|---|---:|---:|---:|---:|
| 1 task, no vars | 217 | 1775 | 8.18x | 18 |
| 1 task, 1 KB create | 1258 | 2816 | 2.24x | 18 |
| 1 task, 50 KB create (the existing benchmark payload) | 51434 | 52992 | 1.03x | 18 |
| 1 task, 50 KB create + 50 KB complete | 102651 | 104264 | 1.02x | 19 |
| 5 tasks, no vars | 672 | 4599 | 6.84x | 46 |
| 5 tasks, 1 KB create + 1 KB per complete | 6918 | 11120 | 1.61x | 51 |

| deploy | command (B) | events (B) |
|---|---:|---:|
| 1-task process | 1025 | 1119 |
| 5-task process | 2473 | 2567 |

Scope of the measurement: this is an **uncompressed, payload-only** comparison. The per-entry
`ReplicatedBatch` wrapper and the OpenRaft entry framing (`raft_logstore.rs:783`) are not
counted, and the log store's optional disk compression is not applied. Fixed framing would dilute
the ratios, and compression savings depend on entry contents (repetitive event JSON may compress
differently from command JSON), so the ratios above hold **only for uncompressed payload bytes**,
not physical log or wire bytes. An absolute
bandwidth conclusion needs a full-frame measurement with an explicit batching/compression
policy; none is claimed here.

Reading: the event form costs a fixed ~1.6 KB of structural events per lifecycle (~95 B/event at
~18 events for a one-task instance) plus the payload once. Payload-dominated workloads (the 50 KB
benchmark) are within **3 %**; deploys within **~12 %**. Tiny-payload workloads grow **~7–8x** —
but from ~217 B to ~1.8 KB per lifecycle, i.e. ~18 MB/s of replication per 10k lifecycles/s per
follower, well inside a LAN link. Notably, **the journal already persists exactly these events on
every replica today**, so *journal* write volume does not change. Total local disk volume does
grow, however: `RaftLogStore::append_entries` (`raft_logstore.rs:774`) persists its own copy of
each entry, so the larger Raft entries are written to disk a second time alongside the journal
(and sent over the network) until the two stores are merged — which is explicitly deferred.
Structural compaction of the event encoding (dropping redundant fields, binary
framing) is an optimization available later and is not needed for the decision.

## Apply-input audit

Every input that engine apply (`Engine::apply_command_records_at`, reached from
`Journal::apply_command_ref_at`, `nano-server-storage/src/journal.rs:3032`) reads that is **not**
in `(engine state, command)`. Method: a grep of `engine-core/src` for clocks, RNG, environment,
hashers and float intrinsics, plus every `set_*` host hook on `Engine` and `Journal`, then reading
each hit. Line numbers are at this ADR's commit.

| # | input | where | status today | A (events) | C (enforced determinism) |
|---|---|---|---|---|---|
| 1 | **Cluster-variable host handle** (#1657): `Arc<RwLock<ClusterVariableSnapshot>>` | type `cluster_vars.rs:63`; installed `engine/mod.rs:772` via `journal.rs:1334/1379` from `server/src/main.rs:3436`; read `engine/resolve.rs:444` | **Open leak**: node-local, unjournaled, mutated by REST | Read only on the leader, but still unjournaled (lost on restart, replay sees today's values), so it has to move into engine state anyway | Becomes engine state changed by replicated commands; the handle type is deleted from the apply signature |
| 2 | **FEEL wall clock / entropy fallback** | `feel/builtins.rs:1096` (`SystemTime::now` in `now()`/`today()`), `feel/builtins.rs:1330` (`random number()`/`uuid()`) | Closed by #1649 when `feel::env::with_env` is installed (`feel/env.rs:62`), but **fails open**: without an installed env it silently reads the host clock | Harmless on followers (no evaluation there) | Fail closed: under the apply build the fallback is a hard error, and `SystemTime`/`Instant` are banned in the apply crates by lint (below) |
| 3 | **`lenient_completion` flag** (host config) | `engine/mod.rs:741`; set from activation policy at `server/src/main.rs:3078` and **changed at runtime** at `main.rs:43375` | Leak during a policy flip: each node flips at its own wall-clock moment, not at a log index, so one committed `CompleteJob` can be accepted on one replica and rejected on another | Leader-only decision, recorded as events | Becomes engine state set by a replicated command (flips at a log index on every replica) |
| 4 | **`num_partitions`** (host config, `NANOBPMN_PARTITIONS`) | `engine/mod.rs:731`; set at `main.rs:3071` | Documented "must be identical on every node" but not checked | Leader-only routing decision, recorded | Engine state from a replicated command, or verified at join (refuse a mismatching member) |
| 5 | **Leader-local activation state** (ADR 0002) | soft grants `main.rs` `prepare_activation`; apply reads job state (with #3 papering over it) | By design followers do not hold the leader's locks | Unchanged (leader-local in both) | Unchanged; apply must not read it: the apply-input type excludes it, and #3 stops being a toggle |
| 6 | **`HashMap` iteration order** (std `RandomState`, per-process random seed) | engine state maps `state/types.rs:2352-2407` (`instances`, `jobs`, `timers`, `message_subscriptions`, …) | Kept safe **by hand**: event-emitting walks sort first (`engine/mod.rs:17337`, `:17351`; `engine/event_subprocess.rs:460`; 97 `sort*` calls in `engine/`); unique-key lookups (`engine/mod.rs:971`) are order-independent. Nothing enforces it, and the JSON journal encoding of these maps is **not canonical** (bytes differ between replicas holding equal state) | Cannot diverge followers; leader order only has to be self-consistent | Replace the hasher with a fixed-seed `BuildHasher` alias (or `BTreeMap`) in engine state; `clippy::disallowed_types` bans `std::collections::HashMap/HashSet` in the apply crates. Also makes the encoding canonical, which the digest below relies on |
| 7 | **Platform `libm` transcendental floats** | `feel/builtins.rs:525-526` (`ln`, `exp`), `feel/eval.rs:542` (`powf`) | Can differ by an ulp across OS/arch (glibc vs. Apple libm); same-binary clusters agree. `sqrt` is IEEE-exact; float formatting is Rust's own shortest-repr (deterministic) | Cannot diverge followers | Route through a pure-Rust `libm` (or the FEEL decimal type) so the result is platform-independent |
| 8 | **Node-local residency / cold-store I/O** | `journal.rs:2327` `ensure_resident_for_command` | An unreadable local row **fails closed** under replicated apply (#1470), so it fail-stops rather than diverges | Folding events still needs residency; same behaviour | Same; the digest (below) is a second line of defence |
| 9 | **Orphan sink / leadership** | `journal.rs:1703`, `:1719` | Decides which node *proposes* a stamped quarantine; the stamp travels in the command | Unchanged | Unchanged (already in the envelope) |
| 10 | **Clock and seed** | `ReplicatedCommand.now` (`raft.rs:106`); `TriggerTimers/ExpireJobs { now }` (`command.rs:273-282`); FEEL seed from replicated state (#1649) | Closed: carried in the envelope | n/a | Already compliant |
| 11 | **Environment variables** | none in `engine-core/src` (grep `std::env`: no hits) | Closed | n/a | Lint keeps it closed |
| 12 | **Dependencies** | `engine-core/Cargo.toml`: only optional `serde`/`serde_json`; no regex crate, no time-zone DB (`feel/temporal.rs:20`, offsets only) | Small surface; the Rust toolchain/std is the remaining one | n/a | Covered by the determinism CI across the previous release and build profiles |

**What the audit shows: the leaks are few and closable.** Two are live defects (#1 cluster
variables, #3 the runtime `lenient_completion` flip); three are latent and closable mechanically
(#2 fail-open fallback, #6 hasher, #7 libm); #4 needs a check. The FEEL surface that motivated A is
already closed by #1649, engine-core has no non-std dependency on the apply path, and the hardest
class (#6) is already handled by convention — the gap is enforcement, not design. That is the
case in which C keeps today's efficiency and simplicity and adds a safety net, rather than the
pervasive case in which A's "cannot diverge by construction" earns its migration cost.

## Options

- **A. Event replication (Zeebe model).** The leader applies a command batch once and proposes
  **the events** (plus per-command record metadata and responses) as the Raft entry. Followers fold
  committed events into state.
- **B. Command replication as-is.** Keep today's model, rewrite `falcon-design.md` §15.3, rely on
  the existing tests (#1649 replica agreement, `replay_determinism_differential`, FEEL differential
  gate).
- **C. Command replication with enforced determinism.** Keep replicating commands; make
  determinism an enforced and *detected* invariant:
  1. **Pure apply boundary.** Apply is a function of `(state, command)`. The engine's apply entry
     takes an `ApplyInput { command, now, seed, operation_reference }` and `&mut EngineState`; the
     host-side hooks that feed apply today (`set_cluster_variables`, `set_lenient_completion`,
     `set_num_partitions`) are removed from `Engine` and become state changed by replicated
     commands (#1, #3, #4). No `Arc<RwLock<…>>`, no `dyn` callback reaches apply. A
     `clippy.toml` `disallowed-methods`/`disallowed-types` list, enabled at `deny` in `engine-core`,
     bans `SystemTime::now`, `Instant::now`, `std::env::*`, `std::collections::HashMap`/`HashSet`
     (with `RandomState`) and `f64::{ln, exp, powf, …}` outside a deterministic wrapper module;
     the FEEL clock/RNG fallback becomes unreachable in the apply build (#2).
  2. **Every non-deterministic input travels in the envelope.** Today: `now`, the FEEL seed (from
     replicated state), `operation_reference`, and the terminal attribution `stamp_for_proposal`
     resolves on the leader (worker, lock deadline), plus orphan stamps. Nothing else the leader
     samples reaches apply once #1/#3/#4 become state; the lint keeps it that way.
  3. **Runtime divergence detection.** See the digest specification below.
  4. **Determinism CI.** A job replays the same recorded journals (the `golden_replay` corpus plus a
     generated randomized workload) under (a) several `HashMap` seeds — run before #6 lands, it
     flushes out any unsorted walk; after it, a guard — (b) debug and release profiles and
     Linux/macOS × x86_64/aarch64 (#7), and (c) the previous release's binary, then compares
     digests at every checkpoint. This exercises paths no unit test targets, which is where B's
     tests are blind.

### Divergence digest (option C)

- **Incremental chain, every entry.** Each replica folds `h_i = H(h_{i-1} || encoded records of
  entry i)` over the journal bytes it already writes. Equal chains imply equal state, because state
  is the fold of its events (#1071). The leader proposes its `h` for index `i - k` inside entry `i`
  (so the digest never delays the entry it covers); followers compare at that index. `H` is a
  stable, versioned hash (xxh3-128 or BLAKE3) pinned in the server, not std's `DefaultHasher`.
- **Precondition: canonical bytes.** Today's journal JSON is **not** canonical — `HashMap` fields
  serialize in `RandomState` order — so the chain needs audit item #6 first. Without it, a replica
  would have to re-encode canonically, which is far more expensive (measured below).
- **Periodic full hash, at snapshot time only.** The snapshot already serializes the whole state;
  its digest (over the canonical snapshot bytes) is recorded with the snapshot and checked on
  install. It is too expensive for the apply path (measured below).
- **On mismatch:** the follower fails stop (stops applying, like the #1405 fenced
  journal), reports both digests and the index, and resyncs by installing the leader's snapshot.
  The divergent journal segment is kept for forensics. A leader never adopts a follower's state.
- **Snapshots and #1071.** A snapshot records `(last_applied, chain digest, full digest)`;
  a restored replica resumes the chain from the recorded value. Migration-by-replay re-derives the
  chain from the replayed journal and must reproduce the recorded digest at each snapshot index —
  a migration that changes the fold now fails loudly instead of silently. A deliberate encoding
  change (a `SNAPSHOT_FORMAT_VERSION` bump) re-bases the chain at the migration point.

Measured cost (`engine-core/examples/divergence_digest.rs`, added with this ADR;
`cargo run --release --features serde --example divergence_digest`; Apple M1 Max; each step is a
create with 4 variables or a job completion with 1; `DefaultHasher` stands in for the production
hash):

| live instances | apply / cmd | canonical re-encode + hash / cmd | overhead | hash of already-encoded journal bytes / cmd | full digest | snapshot bytes |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 39 µs | 30 µs | 77 % | 1.2 µs | 64 ms | 1.3 MB |
| 10,000 | 63 µs | 31 µs | 49 % | 1.3 µs | 1.19 s | 13.3 MB |
| 50,000 | 265 µs | 36 µs | 13 % | 1.4 µs | 6.9 s | 67 MB |

Reading: once item #6 makes the journal bytes canonical, the per-entry chain costs **~1–1.4 µs per
command (~2–3 % of apply)**, because it only hashes bytes that are written anyway. Re-encoding
canonically through a sorted DOM instead costs 30–36 µs (13–77 %), which is why #6 is a
precondition rather than an option. A full-state hash is O(state) (≈ 140 ms per 1,000 live
instances), so it belongs at snapshot time only.

## Comparison

| dimension | A. events | B. commands as-is | C. commands, enforced determinism |
|---|---|---|---|
| Determinism surface | Followers are a pure fold over events; FEEL, iteration order, floats and upgrades cannot diverge them. Leader-only non-determinism is acceptable (it is recorded). | Every apply path, forever; guards see only exercised paths; #1657 shipped undetected. | The audited inputs (12 items), closed by type boundary + lint; anything missed is **detected** at the next digest instead of silently persisting. |
| Failure mode on a missed input | None for replica agreement (leader just records its result). | Silent split-brain. | Fail-stop of the divergent follower + snapshot resync; alert. |
| Log size / bandwidth | Measured above (payload-only, uncompressed): +2–3 % at benchmark payloads, ~7–8x on tiny instances (~1.6 KB/lifecycle). Raft log store keeps its own copy, so disk volume grows until the stores merge. | Smallest entries. | Smallest entries + 16–32 B digest every k entries. |
| CPU | Followers fold (cheaper than processing); leader unchanged. | Every replica processes. | Every replica processes + ~1–1.4 µs/cmd chain hash. |
| Leader-side complexity | **Substantial and new.** Speculative processing ahead of commit against a copy-on-write overlay of committed state (engine-core has no such overlay today); discard the overlay on failed proposal or failover; hold every client response until commit; activation, queries, lease digests and snapshots must read committed state only; key allocation is leader-only and the fold must advance the generator from observed keys; per-command record metadata (`operation_reference` lives on `Record`) has to ride in the entry. | None new. | None new: leader and followers run the same code path. |
| Activation / leases (ADR 0001, 0002) | ADR 0002 split preserved: soft grants stay leader-local; durable activations replicate as `JobActivated` events. | Activation commands re-executed on followers. | As B; the runtime `lenient_completion` toggle (#3) becomes replicated state. |
| Timers, `ExpireJobs`, pump | Leader-proposed commands to the leader's processor; only results replicate. | Re-evaluated everywhere with replicated `now`. | As B (`now` already in the envelope). |
| Durability tiers (ADR 0003) | Semantics unchanged; replication acknowledgment stays separate from local durability (`NANOBPMN_DURABILITY`, `journal.rs:129–132`; `raft_logstore.rs:853–861`). | Unchanged. | Unchanged. |
| Snapshot / replay / #1071 | One persisted, replay-guarded shape (events) for log and journal. | Two shapes: command wire (`ReplicatedCommandWire`) and event journal. | Two shapes as B, but migration-by-replay is now *checked* against recorded digests. |
| Rolling upgrade | Entry-format version; gated cutover; mixed old/new clusters cannot share a log in both directions. | None. | None for the log format. A determinism-affecting change (e.g. #7's libm swap, a hasher change) is gated on the previous-release replay CI. The digest entry field is additive. |
| Migration + effort | **Large**: 6 slices (below); about 2–3 engineer-months, with a running-system Raft log format change and a cutover that cannot be cleanly rolled back once events are emitted. | Doc change + #1657. | **Medium**: #1657 + #3/#4 to state (needed anyway), hasher swap (#6, mechanical but touches every state map), libm wrapper (#7), lint, digest field + fail-stop/resync path, determinism CI. About 3–5 engineer-weeks; no log format break, each slice independently revertible. |

## Decision (proposed)

**Adopt option C** — command replication with enforced determinism — because the audit shows the
apply-input leaks are few and closable, and C closes them for ~2–3 % apply CPU with no Raft log
format change. B is rejected: #1657 is the proof that hoping is not enough. A is **not**
rejected on principle; it is deferred with an explicit trigger: if, after C ships, the divergence
digest fires in production for a cause the type boundary and lint cannot express, or the audit
has to grow without bound, re-open this ADR for A (its staged plan is kept below so it is not lost).

`falcon-design.md` §15.3 item 2 is annotated: the decision of record is command replication with
enforced determinism, per this ADR.

### Staged plan for C (each slice is fileable as an issue)

1. **#1657: cluster variables become engine state** (see below). Removes audit item #1.
2. **Host config into state.** `lenient_completion` and `num_partitions` become engine state set by
   replicated commands (or verified at join); `set_*` hooks deleted from `Engine` (#3, #4).
3. **Deterministic hasher.** Type alias over a fixed-seed `BuildHasher` for every engine-state map;
   `disallowed-types` lint for std `HashMap`/`HashSet`. Journal encoding becomes canonical (#6).
   Not a format change: the same JSON shape, only key order becomes stable.
4. **Purity lint + fail-closed FEEL env.** `disallowed-methods` for clocks, env, std float
   transcendental intrinsics; pure-Rust `libm` wrapper (#7); FEEL fallback unreachable in apply (#2).
5. **Divergence digest.** Chain digest per entry, leader digest carried additively in the entry,
   fail-stop + snapshot resync, digests recorded in snapshots and checked by #1071 replay.
6. **Determinism CI.** Seed / profile / platform / previous-release replay matrix comparing digests.

### Staged plan for A (deferred; kept for the re-open trigger)

1. Event-apply path parity: one canonical "apply events to state" fold shared by replay and
   followers; differential test `state(process commands) == state(fold produced events)`.
2. Versioned entry payload `ReplicatedEntry::{Commands /* v1 */, Events /* v2 */}`, decode both,
   emit v1; golden corpus covers the new frame.
3. Leader processes against an isolated copy-on-write speculative state and proposes `Events` with
   per-command record metadata (`operation_reference`) and responses; after commit it folds the
   committed events through the follower fold exactly once; failed proposals discard the overlay;
   activation/queries/lease digests/snapshots read committed state only; keys leader-only.
4. Cutover behind a cluster-wide flag, refused unless every voter **and learner** runs ≥ slice 3,
   and refusing later joins below slice 3 while on.
5. Remove follower command execution only after a verified boundary: every node has a snapshot at
   or beyond the last v1 entry and compaction removed all v1 entries; until then #1649 guards stay.
6. Measure bandwidth and p99 before/after cutover.

### How #1657 (cluster variables) is solved

Under **every** option the first step is the same: cluster-variable mutations become replicated
engine commands producing new additive events (`ClusterVariableCreated/Updated/Deleted`, carrying
`kind`); cluster-variable state moves into engine state (snapshotted, rebuilt by replay); the
host-side `Arc<RwLock<…>>` handle and `set_cluster_variables` are removed. This fixes the split-brain,
the loss on restart, and replay against today's values.

- **Under A**, a read during processing additionally happens only on the leader at a log
  position, and its result is recorded in events, so followers never re-evaluate it.
- **Under C**, every replica reads the same engine state at the same log index, because the
  mutation is itself a log entry; the removed handle can no longer be reintroduced because apply's
  input type has no place for it and the lint bans the lock types in the apply crates. If a host
  input ever slips in anyway, the divergence digest fail-stops the affected follower.

## Consequences

- (C) Replica consistency still depends on engine determinism, but determinism is now enforced
  (type boundary + lint), tested beyond exercised paths (determinism CI), and **detected** at
  runtime (digest) with a fail-stop rather than silent split-brain.
- (C) Raft entries stay small; apply cost grows ~2–3 % for the digest chain; snapshotting gains a
  full-state hash over bytes it already produces.
- (C) Two persisted shapes remain (command wire + event journal); both stay under the golden drift
  guards.
- (C) A non-Rust or cross-platform follower (e.g. a wasm replica) must reproduce the same digest;
  the libm and hasher slices are what make that possible.
- If the re-open trigger fires, A's plan above applies unchanged.

## Open questions

- Digest cadence `k` (every entry vs. every N entries) and whether the leader's digest rides in
  every entry or in a periodic marker entry.
- Whether the full-state snapshot hash should be computed incrementally (e.g. a sum of per-key
  hashes maintained on write) to make it cheap enough for periodic checks between snapshots.
- Whether the Raft log and the event journal should be merged into one store (relevant to both
  options; follow-up ADR if pursued).
