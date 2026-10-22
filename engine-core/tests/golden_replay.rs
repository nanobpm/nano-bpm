//! Golden old-journal replay regression tests (issue #1070, L4 of the
//! snapshot-durability epic #1067; incident #1065).
//!
//! # What this guards
//!
//! Migration-by-replay rebuilds the engine by **replaying the event journal**
//! under the current binary. That only works if OLD events still deserialize and
//! replay into the SAME engine state a live run produced — the property that
//! broke in incident #1065 (a replay that silently rewound the key high-water,
//! re-minting `instance 41`). This file locks that property in as a regression
//! guard by replaying the checked-in golden journal and asserting it
//! reconstructs the golden snapshot's state.
//!
//! It **reuses** the golden corpus owned by #1069 (L3) — it does not fork or
//! duplicate it. See `engine-core/tests/fixtures/golden/README.md`:
//!
//! * `event_corpus.v{N}.json` — an ordered, replayable `Vec<Event>` journal.
//! * `engine_snapshot.v{N}.json` — the `EngineSnapshot` that same scenario
//!   produced (`build_golden_corpus()` in `golden_serde_drift.rs`).
//!
//! Both are pinned to [`SNAPSHOT_FORMAT_VERSION`], so a version bump repoints
//! these tests at the fresh `v{N}` corpus while the prior `v{N-1}` files stay
//! checked in for #1071's cross-version replay-migrator.
//!
//! # Three replay risks covered
//!
//! 1. **Whole-journal replay parity** — the golden journal replays into exactly
//!    the golden state (key high-water included).
//! 2. **Forward-compat of additive fields** — a legacy record missing a
//!    now-`#[serde(default)]` field still decodes and replays (the contract that
//!    makes additive changes safe).
//! 3. **Unknown/removed variant rejection** — an unrecognized event variant is a
//!    typed [`EventDecodeError::UnknownVariant`], never a silent drop or panic.
//!
//! # CRITICAL: `--features serde`
//!
//! The `Event`/`EngineSnapshot` serde derives are feature-gated, so this whole
//! file is `#![cfg(feature = "serde")]` and must be built/run with
//! `--features serde` (the `engine-core (clippy + test)` CI job #1069 wired
//! already passes it; this task reuses that job and does not re-add the flag).

#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::{
    decode_event_json, Command, Engine, EngineSnapshot, Event, EventDecodeError, ProcessBuilder,
    SNAPSHOT_FORMAT_VERSION,
};

/// Directory holding the checked-in golden corpus (owned by #1069), relative to
/// the crate root.
const GOLDEN_SUBDIR: &str = "tests/fixtures/golden";

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(GOLDEN_SUBDIR)
        .join(name)
}

/// The version-pinned golden filenames (same scheme as the #1069 drift guard).
fn event_corpus_name() -> String {
    format!("event_corpus.v{SNAPSHOT_FORMAT_VERSION}.json")
}
fn snapshot_name() -> String {
    format!("engine_snapshot.v{SNAPSHOT_FORMAT_VERSION}.json")
}

/// Load the golden journal (`Vec<Event>`) #1069 landed.
fn load_golden_journal() -> Vec<Event> {
    let raw = std::fs::read_to_string(golden_path(&event_corpus_name()))
        .unwrap_or_else(|e| panic!("read golden event corpus: {e}"));
    serde_json::from_str(&raw).expect("golden event corpus deserializes under current build")
}

/// Load the golden `EngineSnapshot` #1069 landed (the expected replay target).
fn load_golden_snapshot() -> EngineSnapshot {
    let raw = std::fs::read_to_string(golden_path(&snapshot_name()))
        .unwrap_or_else(|e| panic!("read golden snapshot: {e}"));
    serde_json::from_str(&raw).expect("golden snapshot deserializes under current build")
}

/// **The core #1065 regression guard.** Replaying the golden old-format journal
/// under current code must reconstruct exactly the golden engine state — same
/// materialized `State` and, critically, the same **key high-water**
/// (`next_local`), so a replay-migrated engine never re-mints a key it already
/// handed out (the `instance 41` rewind of #1065).
#[test]
fn golden_journal_replays_into_expected_state() {
    let journal = load_golden_journal();
    let golden = load_golden_snapshot();

    let engine = Engine::replay_partition(golden.partition_id, journal);

    // Materialized state must be identical (State: PartialEq/Eq).
    assert_eq!(
        engine.state(),
        &golden.state,
        "replaying the golden journal must reconstruct the golden snapshot's State \
         (a drift here is exactly the #1065 replay-divergence class)"
    );

    // The generator/placement scalars a resumed engine relies on must match.
    // `now` is deliberately excluded: replay resets the clock to 0 (timestamps
    // ride on the events themselves), which is the documented replay contract.
    let replayed = engine.snapshot();
    assert_eq!(
        replayed.next_local, golden.next_local,
        "key high-water (next_local) must survive replay — the #1065 rewind guard"
    );
    assert_eq!(replayed.partition_id, golden.partition_id);
    assert_eq!(replayed.num_partitions, golden.num_partitions);
}

/// Every record in the golden journal must decode through the typed
/// [`decode_event_json`] replay-boundary helper (as the storage layer reads it,
/// one compact line per event) and round-trip back to the same `Event`. This
/// confirms the typed decoder accepts the real corpus (no false rejections).
#[test]
fn golden_journal_decodes_through_typed_decoder() {
    for event in load_golden_journal() {
        let line = serde_json::to_string(&event).expect("event serializes to a journal line");
        let decoded =
            decode_event_json(&line).expect("golden journal record decodes via typed decoder");
        assert_eq!(decoded, event, "typed decode must round-trip the record");
    }
}

#[test]
fn archived_format_journals_remain_replayable_after_version_bumps() {
    for version in 1..SNAPSHOT_FORMAT_VERSION {
        let raw =
            std::fs::read_to_string(golden_path(&format!("event_corpus.v{version}.json"))).unwrap();
        let frames: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap();
        let journal = frames
            .into_iter()
            .map(|frame| decode_event_json(&frame.to_string()).unwrap());
        let raw = std::fs::read_to_string(golden_path(&format!("engine_snapshot.v{version}.json")))
            .unwrap();
        let expected: EngineSnapshot = serde_json::from_str(&raw).unwrap();
        let replayed = Engine::replay_partition(expected.partition_id, journal);
        assert_eq!(
            replayed.state(),
            &expected.state,
            "archived format {version}"
        );
        assert_eq!(
            replayed.snapshot().next_local,
            expected.next_local,
            "archived allocator {version}"
        );
    }
}

/// **Forward-compat of additive fields.** An OLD record written before a field
/// gained `#[serde(default)]` has no such key; it must still decode (defaulting
/// the field) and replay, confirming the `serde(default)` contract that makes
/// additive event changes replay-safe. Uses the `JobFailed.worker` field (#959),
/// added after journals already existed.
#[test]
fn legacy_record_missing_a_defaulted_field_replays() {
    // A pre-`worker` `JobFailed`, plus the deploy+create+activate prefix needed
    // for the applier to accept the completion path around it.
    let legacy_line = r#"{"JobFailed":{"job_key":7,"instance_key":1,"retries":0}}"#;
    let decoded =
        decode_event_json(legacy_line).expect("legacy record missing a defaulted field decodes");
    match &decoded {
        Event::JobFailed { worker, .. } => {
            assert!(worker.is_none(), "missing field must default to None")
        }
        other => panic!("expected JobFailed, got {other:?}"),
    }
    // Replaying a journal that contains it must not error/panic (the applier
    // tolerates an unmatched job-failed as a no-op; the point is that decode +
    // replay of the legacy shape succeed).
    let _engine = Engine::replay(std::iter::once(decoded));
}

/// **Pre-#1233 parallel join, upgraded mid-flight.** Before #1233 a
/// `ParallelJoinTokenArrived` carried no `flow`. A journal that left a join
/// half-open must still decode, replay, and fire the join when the missing
/// branch arrives under the new build: an unidentified arrival counts as one
/// taken incoming flow.
#[test]
fn pre_1233_half_open_parallel_join_fires_after_upgrade() {
    let def = ProcessBuilder::new("sync")
        .start_event("s")
        .parallel_gateway("fork")
        .service_task("t", "sync-task")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "t")
        .connect("fork", "join")
        .connect("t", "join")
        .connect("join", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    let mut journal = engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sync"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    journal.extend(created);

    // Re-encode the journal as a pre-#1233 build wrote it: no `flow` key.
    let mut stripped = 0;
    let legacy: Vec<Event> = journal
        .iter()
        .map(|event| {
            let mut json = serde_json::to_value(event).unwrap();
            if let Some(arrived) = json.get_mut("ParallelJoinTokenArrived") {
                arrived.as_object_mut().unwrap().remove("flow");
                stripped += 1;
            }
            decode_event_json(&json.to_string()).expect("pre-#1233 record decodes")
        })
        .collect();
    assert_eq!(stripped, 1, "the half-open join journaled one arrival");

    let mut upgraded = Engine::replay(legacy);
    let instance = upgraded.instance(instance_key).unwrap();
    assert!(instance.join_flow_arrivals.is_empty());
    assert_eq!(instance.join_flows_taken("join"), 1);

    let job = upgraded.activate_jobs("sync-task", "w", 1, 60_000, 0)[0].key;
    upgraded.apply_command(Command::complete_job(job)).unwrap();
    assert!(
        upgraded.is_completed(instance_key),
        "the upgraded join fires once the missing branch arrives"
    );
}

/// **Unknown/removed variant rejection.** A record naming a variant this build
/// does not define (a renamed/removed event kind, or a newer/foreign journal
/// format) must surface as the typed [`EventDecodeError::UnknownVariant`] —
/// never a silent skip, never an ambiguous/anonymous error.
#[test]
fn unknown_variant_is_typed_rejection() {
    let line = r#"{"NoSuchEventFromTheFuture":{"instance_key":42}}"#;
    match decode_event_json(line) {
        Err(EventDecodeError::UnknownVariant { variant, detail }) => {
            assert_eq!(variant, "NoSuchEventFromTheFuture");
            assert!(
                detail.contains("unknown variant"),
                "detail should carry the underlying serde diagnosis, got: {detail}"
            );
        }
        other => panic!("expected a typed UnknownVariant rejection, got {other:?}"),
    }
}

/// An unknown variant embedded **among valid records** of a real journal is
/// rejected at its record, not silently dropped while the rest replay — the
/// exact "silent skip" failure mode #1070 exists to prevent.
#[test]
fn unknown_variant_among_valid_records_is_not_skipped() {
    let mut lines: Vec<String> = load_golden_journal()
        .iter()
        .map(|e| serde_json::to_string(e).expect("serialize"))
        .collect();
    // Splice a removed/foreign variant into the middle of the journal.
    let inject_at = lines.len() / 2;
    lines.insert(
        inject_at,
        r#"{"RetiredLegacyEvent":{"instance_key":1}}"#.to_string(),
    );

    let mut unknown_hits = 0;
    for (idx, line) in lines.iter().enumerate() {
        match decode_event_json(line) {
            Ok(_) => {}
            Err(EventDecodeError::UnknownVariant { variant, .. }) => {
                unknown_hits += 1;
                assert_eq!(idx, inject_at, "only the injected record should be unknown");
                assert_eq!(variant, "RetiredLegacyEvent");
            }
            Err(other) => panic!("valid golden record failed to decode: {other}"),
        }
    }
    assert_eq!(
        unknown_hits, 1,
        "the injected unknown variant must be surfaced exactly once, not skipped"
    );
}

/// A record naming a KNOWN variant but with a malformed payload (wrong field
/// type) is [`EventDecodeError::Malformed`], NOT `UnknownVariant` — the two
/// failure classes must stay distinct so a journal reader can tolerate a torn
/// trailing write (`Malformed`) while still hard-rejecting a real unknown frame.
#[test]
fn malformed_payload_is_typed_malformed_not_unknown() {
    // Known variant, but `deployment_key` should be an integer key.
    let bad_type = r#"{"DeploymentCreated":{"deployment_key":"not-a-number"}}"#;
    assert!(
        matches!(
            decode_event_json(bad_type),
            Err(EventDecodeError::Malformed { .. })
        ),
        "a type-mismatched known variant is Malformed, not UnknownVariant"
    );

    // A truncated line (interrupted write) is also Malformed — the torn-tail class.
    let torn = r#"{"DeploymentCreated":{"deployment_ke"#;
    assert!(
        matches!(
            decode_event_json(torn),
            Err(EventDecodeError::Malformed { .. })
        ),
        "a truncated record is Malformed (a journal reader may tolerate it only as a torn tail)"
    );
}
