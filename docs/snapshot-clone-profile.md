# On-actor snapshot clone: profile and options

Refs [#50](https://github.com/nanobpm/nano-bpm/issues/50). This is an evaluation only. `State` is unchanged.

## What blocks the actor

`PartitionStateMachine::get_snapshot_builder` (`server/src/raft.rs`) runs
`Engine::snapshot()` (`engine-core/src/engine/memory.rs`) on the partition's
single-threaded engine actor. `Engine::snapshot()` is essentially `self.state.clone()`.
JSON serialization already runs off the actor (`PartitionSnapshotBuilder::build_snapshot`).
The time this clone takes is the time the actor cannot serve any other command.

## Benchmark

```
cd engine-core
cargo run --release --features serde --example snapshot_clone [max_n] [payload_kb] [reps]
```

`examples/snapshot_clone.rs` creates `N` resident one-service-task instances. Each
instance has a live job and a 10 KB variable payload. At each working-set size it
times `Engine::snapshot()` and `snapshot_control_only()` 5 times, then reports the
median and the maximum. "Creates stalled" is the median clone time divided by the
measured per-create cost.

Results on an Apple M4 Pro (release build, 10 KB payload):

| resident instances | snapshot median / max | ns per instance | creates stalled | control_only median |
|---:|---:|---:|---:|---:|
| 1,000 | 0.23 / 0.42 ms | 228 | ~33 | 0.25 ms |
| 10,000 | 3.2 / 5.0 ms | 316 | ~480 | 3.5 ms |
| 50,000 | 22 / 27 ms | 440 | ~3,200 | 18 ms |
| 100,000 | 35 / 58 ms | 353 | ~5,000 | 37 ms |
| 200,000 | 64 / 138 ms | 322 | ~9,200 | 73 ms |
| 500,000 | 159 / 396 ms | 318 | ~22,000 | 185 ms |
| 1,000,000 | 318 / 934 ms | 318 | ~44,000 | 370 ms |

Findings:

- The clone is linear: about 0.3 µs per resident instance, including its job and
  indexes. The payload size does not matter, because `variables` is an `Arc` and
  cloning it only bumps a refcount. `control_only` costs the same as a full snapshot.
- At realistic per-partition working sets (≤ 10k–50k resident instances), the stall
  is roughly 3–25 ms per snapshot. Jitter from #49 already staggers these across
  partitions.
- At large working sets (≥ 200k resident instances on one partition), the stall
  reaches 60–300 ms median, with tail spikes up to about 1 s. Those spikes come from
  allocator and page-fault pressure on the first touch of the new nodes. A 1 s
  actor pause would be visible as p99 latency.

## Options

The maps in `State` are `BTreeMap`s: `instances`, `jobs`, `user_tasks`, `incidents`,
`timers`, subscriptions, ledgers, and indexes such as `jobs_by_instance`.

| Option | On-actor snapshot cost | Write-path cost | Memory | Touched sites | Risk |
|---|---|---|---|---|---|
| **Persistent maps** (`im::OrdMap` / `rpds::RedBlackTreeMapSync`) | O(1) (root `Arc` bump) | O(log n) path copy per insert/remove, roughly 2–5× a `BTreeMap` op. Every `get_mut` needs `Arc::make_mut` on the node path. | Node sharing while a snapshot is alive. Higher constant per entry. | Every `State` map type plus every `get_mut`/`entry`/`values_mut`/`range` mutation site in `engine-core` (hundreds). serde derives need the crates' serde features. Iteration-order determinism must be re-proven against the determinism corpus. | High: engine-wide, and it touches the replay/determinism gates |
| **Double buffering** (a second `State` replica kept in step by replaying applied events on another thread, then snapshotted there) | ~0 on the actor | Every event is applied twice, on another core. The handoff (a channel) costs a little on the actor. | 2× the working set | New replica-applier plumbing in `server/src/raft.rs`. engine-core is unchanged, apart from possibly exposing an event-apply entry point. The replica must stay bit-identical (it is the same deterministic apply). | Medium: memory doubling. The replica can lag, so the snapshot must be taken at a known applied index. |
| **COW `Arc<State>` + `Arc::make_mut`** | O(1) handoff | The **first write after a snapshot deep-clones the whole `State` on the actor**, which is the same cost moved to a different moment | Same | Small (`Engine.state` field) | Low risk, but **no net gain** |
| **Per-map COW** (`Arc<BTreeMap>` per field) | O(#maps) | The first write to each map after a snapshot clones that map. Large maps (`instances`, `jobs`) are written by almost every command, so this is close to the full clone. | Same | Moderate | Low risk, little gain |
| **Status quo + jitter (#49)** | 0.3 µs × resident | none | none | none | none |

## Recommendation

Keep the status quo for now. At realistic working sets the stall is a few
milliseconds and staggered across partitions. Plain COW does not help. If
deployments routinely hold more than ~200k resident instances per partition, the
lower-risk next step is **double buffering**, which is confined to the server
snapshot path, rather than a persistent-map migration of `State`.
Re-run this benchmark against a captured production working set before deciding.
