//! Replay-determinism differential guard (issue #1279; anti-drift anchor for the
//! #1231 Lean proof, part of epic #1224).
//!
//! # What this guards, and why it exists
//!
//! The Lean proof in `formal/lean/Replay/Determinism.lean` establishes
//!
//! > `snapshot ∘ replay(tail) = full replay`
//!
//! — reconstructing the engine from a snapshot taken after the first `k` events
//! and replaying the surviving tail equals a full replay of the whole journal —
//! for **every** compaction split point `k`. That is the structural negation of
//! the #1065 silent-rewind incident class (recovery that yields a state, or a key
//! high-water, other than a full replay silently loses or re-mints history).
//!
//! But that theorem is proved over an **abstract** left-fold applier
//! `apply : σ → Event → σ` on an abstract state `σ` (see the file's Scope note).
//! Epic #1224's anti-drift rule requires every spec to be *tied to the Rust
//! implementation* — an unanchored spec is itself a drift surface. This test is
//! that tie: it is the **concrete instance** of `recover_snapshotAt` on the real
//! [`Engine`], making the abstract `apply` non-abstract and failing CI the moment
//! the real recovery path diverges from the proven equality.
//!
//! # The concrete correspondence
//!
//! | Lean (`Replay.Determinism`)                       | Rust (`engine-core`)                              |
//! |---------------------------------------------------|---------------------------------------------------|
//! | `replayFrom pid apply init evs` (full replay)     | [`Engine::replay_partition`]                      |
//! | `snapshotAt pid apply init evs k` (serialize)     | [`Engine::snapshot`] + serde JSON round-trip      |
//! | `recover` = deserialize ∘ `replayFrom(_, tail)`   | [`Engine::from_snapshot`] + [`Engine::apply_replayed_events`] |
//! | projection `(state, nextLocal)`                    | [`Engine::state`] + `snapshot().next_local`       |
//! | `deserialize_serialize` (round-trip identity)     | real `serde_json` serialize/deserialize below     |
//!
//! The Lean theorems are scoped to the replay-relevant projection
//! `(state, nextLocal)` — the two fold-threaded values a silent rewind (#1065)
//! would corrupt — **not** to a bitwise-identical engine (the snapshot-carried
//! scalars `partition_id`, `num_partitions`, `now`, `start_dispatch_rr` are
//! restored verbatim by `from_snapshot` while a full `replay_partition`
//! re-defaults them, so they are outside the fold by construction). This guard
//! asserts equality of **exactly** that projection, so the Rust boundary matches
//! the proven boundary — no more, no less.
//!
//! # CRITICAL: `--features serde`
//!
//! [`EngineSnapshot`]'s serde derives are feature-gated, so the snapshot JSON
//! round-trip (the faithful analogue of Lean's `deserialize_serialize` axiom)
//! only compiles under `--features serde`. The whole file is therefore
//! `#![cfg(feature = "serde")]`; the `engine-core (clippy + test)` CI job already
//! passes the flag (this test reuses that job — it does not re-add it).

#![cfg(feature = "serde")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::{
    Command, Engine, EngineSnapshot, Event, ProcessBuilder, ProcessDefinition, Value,
    SNAPSHOT_FORMAT_VERSION,
};

/// One named journal to drive the differential over: an ordered, replayable
/// `Vec<Event>` and the partition id whose local key-counter the fold advances.
struct NamedJournal {
    name: &'static str,
    partition_id: u64,
    events: Vec<Event>,
}

/// Serialize a snapshot and deserialize it straight back — the concrete,
/// on-disk-shape analogue of the Lean model's `deserialize_serialize` identity
/// axiom. Recovery in production reads the snapshot back through serde JSON, so
/// anchoring the proof to the *real* path means round-tripping here rather than
/// handing the in-memory snapshot straight to `from_snapshot`.
fn round_trip(snapshot: &EngineSnapshot) -> EngineSnapshot {
    let bytes = serde_json::to_vec(snapshot).expect("snapshot serializes");
    serde_json::from_slice(&bytes).expect("snapshot deserializes")
}

/// The core differential: for **every** split point `k` in `0..=len`, assert that
/// reconstructing from the snapshot taken after the first `k` events and
/// replaying the surviving tail reproduces exactly the full-replay projection
/// `(state, nextLocal)`. This is `recover_snapshotAt` (and, across the range of
/// `k`, `recover_split_invariant`) evaluated on the real engine.
fn assert_snapshot_replay_tail_equals_full(journal: &NamedJournal) {
    let pid = journal.partition_id;
    let evs = &journal.events;

    // Full-replay reference — Lean `replayFrom pid apply init evs`.
    let full = Engine::replay_partition(pid, evs.iter().cloned());
    let full_next_local = full.snapshot().next_local;

    for k in 0..=evs.len() {
        // Snapshot after the first `k` events — Lean `snapshotAt … k` (a replay
        // of `take k`, then serialize). Round-tripped through serde JSON so the
        // recovered engine is rebuilt from the real on-disk shape.
        let head = Engine::replay_partition(pid, evs[..k].iter().cloned());
        let snap = round_trip(&head.snapshot());

        // Recover from the snapshot and replay the surviving tail `drop k` —
        // Lean `recover pid apply (snapshotAt … k) (evs.drop k)`.
        let mut recovered = Engine::from_snapshot(snap);
        recovered.apply_replayed_events(evs[k..].iter().cloned());

        // Projection equality #1 (domain state): the user-visible datum a silent
        // rewind would corrupt. `State: Eq`.
        assert_eq!(
            recovered.state(),
            full.state(),
            "journal `{}`, split k={k}/{}: recovered State must equal full-replay State \
             (a mismatch here is exactly the #1065 replay-divergence class the Lean \
             `recover_state_eq` proof rules out)",
            journal.name,
            evs.len(),
        );

        // Projection equality #2 (key high-water): a `next_local` that rewinds on
        // recovery re-issues live keys. This is the second half of the #1065
        // guarantee and the Lean `recover_nextLocal_eq` corollary.
        assert_eq!(
            recovered.snapshot().next_local,
            full_next_local,
            "journal `{}`, split k={k}/{}: recovered next_local (key high-water) must equal \
             full-replay next_local — the #1065 counter-rewind guard (`recover_nextLocal_eq`)",
            journal.name,
            evs.len(),
        );
    }
}

// --- Journal builders (multiple distinct journals, as the acceptance requires) ---

/// A linear service-task process instantiated several times, with jobs activated
/// and completed, on the default partition `0`. Exercises the full instance
/// lifecycle (deploy → create → activate → complete) so the fold threads real
/// state mutation *and* key minting.
fn journal_service_task_lifecycle() -> NamedJournal {
    const T0: u64 = 1_700_000_000_000;
    let mut engine = Engine::new();
    let mut events: Vec<Event> = Vec::new();

    let order: ProcessDefinition = ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .build()
        .expect("build order process");
    events.extend(
        engine
            .apply_command_at(Command::DeployProcess(order), T0)
            .expect("deploy order"),
    );

    for i in 0..4u64 {
        let mut vars = HashMap::new();
        vars.insert("amount".to_string(), Value::Int(1000 + i as i64));
        events.extend(
            engine
                .apply_command_at(Command::create_instance_with("order", vars), T0 + 1 + i)
                .expect("create instance"),
        );
        let job_keys = engine.select_activatable_job_keys("payment", 1, T0 + 100 + i, false);
        let activated = engine
            .apply_command_at(
                Command::activate_jobs_by_key(
                    job_keys,
                    "worker",
                    30_000,
                    T0 + 100 + i,
                    Default::default(),
                ),
                T0 + 100 + i,
            )
            .expect("activate job");
        events.extend(activated.iter().cloned());
        // Complete every job the activation emitted, so the instance walks to a
        // retained-terminal state.
        for event in activated {
            if let Event::JobActivated { job_key, .. } = event {
                events.extend(
                    engine
                        .apply_command_at(Command::complete_job(job_key), T0 + 200 + i)
                        .expect("complete job"),
                );
            }
        }
    }

    NamedJournal {
        name: "service_task_lifecycle",
        partition_id: 0,
        events,
    }
}

/// The same lifecycle but minted in a **non-zero** partition (`3`). This drives
/// the `partition_of(max_key) == partition_id` branch of the fold — the
/// per-partition local-counter reconstruction — which a pid-0-only corpus would
/// never exercise.
fn journal_nonzero_partition() -> NamedJournal {
    const T0: u64 = 1_700_000_000_000;
    let pid = 3u64;
    let mut engine = Engine::with_partition(pid);
    let mut events: Vec<Event> = Vec::new();

    let flow: ProcessDefinition = ProcessBuilder::new("flow")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("a", "task-a")
        .service_task("b", "task-b")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .build()
        .expect("build flow process");
    events.extend(
        engine
            .apply_command_at(Command::DeployProcess(flow), T0)
            .expect("deploy flow"),
    );

    for i in 0..3u64 {
        events.extend(
            engine
                .apply_command_at(Command::create_instance("flow"), T0 + 1 + i)
                .expect("create instance"),
        );
    }

    NamedJournal {
        name: "nonzero_partition",
        partition_id: pid,
        events,
    }
}

/// The checked-in golden journal (owned by #1069, reused by #1070/#1071). Loading
/// it here folds the real cross-version replay corpus into the differential, so
/// the proof is anchored to the same journal shape production recovery replays —
/// not only to test-authored ones. Reusing the corpus (rather than forking it)
/// keeps a single source of truth per the fixtures README.
fn journal_golden_corpus() -> NamedJournal {
    let path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden")
        .join(format!("event_corpus.v{SNAPSHOT_FORMAT_VERSION}.json"));
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read golden event corpus {}: {e}", path.display()));
    let events: Vec<Event> =
        serde_json::from_str(&raw).expect("golden event corpus deserializes under current build");

    // The golden corpus is produced on the single-partition (id 0) namespace by
    // `build_golden_corpus()` in `golden_serde_drift.rs`.
    NamedJournal {
        name: "golden_corpus",
        partition_id: 0,
        events,
    }
}

/// Every journal the differential runs over. Kept as a function so each test
/// gets a fresh set (journals own `Vec<Event>`).
fn journals() -> Vec<NamedJournal> {
    vec![
        journal_service_task_lifecycle(),
        journal_nonzero_partition(),
        journal_golden_corpus(),
    ]
}

/// **The anchor.** For every journal and every split point, snapshot∘replay-tail
/// equals full-replay on the real [`Engine`] projection `(state, next_local)`.
/// This is the concrete instance of the Lean `recover_snapshotAt` theorem; it
/// fails CI the moment the real recovery path drifts from the proven equality,
/// satisfying epic #1224's anti-drift rule for the replay-determinism slice.
#[test]
fn snapshot_replay_tail_equals_full_replay_for_every_split() {
    let journals = journals();
    // Guard the guard: an empty corpus would make the loop vacuously pass.
    assert!(
        journals.iter().any(|j| j.events.len() > 1),
        "differential must run over non-trivial journals"
    );
    for journal in &journals {
        assert_snapshot_replay_tail_equals_full(journal);
    }
}

/// Recovery is independent of *where* the journal was compacted: recovering at
/// split `j` yields the same projection as at split `k`, for all `j, k`. This is
/// the real-engine instance of the Lean `recover_split_invariant` theorem — a
/// compaction boundary that could shift the recovered state (the #1065 failure
/// mode) is impossible.
#[test]
fn recovery_is_independent_of_compaction_split() {
    for journal in &journals() {
        let pid = journal.partition_id;
        let evs = &journal.events;
        // Reference recovery at split 0 (recover from the empty snapshot, replay
        // everything). Every other split must reproduce its projection.
        let base_snap = round_trip(&Engine::replay_partition(pid, std::iter::empty()).snapshot());
        let mut base = Engine::from_snapshot(base_snap);
        base.apply_replayed_events(evs.iter().cloned());
        let base_next_local = base.snapshot().next_local;

        for k in 1..=evs.len() {
            let head = Engine::replay_partition(pid, evs[..k].iter().cloned());
            let mut recovered = Engine::from_snapshot(round_trip(&head.snapshot()));
            recovered.apply_replayed_events(evs[k..].iter().cloned());
            assert_eq!(
                recovered.state(),
                base.state(),
                "journal `{}`, split k={k}: recovered State must be split-invariant",
                journal.name,
            );
            assert_eq!(
                recovered.snapshot().next_local,
                base_next_local,
                "journal `{}`, split k={k}: recovered next_local must be split-invariant",
                journal.name,
            );
        }
    }
}
