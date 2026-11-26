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
    decode_event_json, Command, Engine, EngineSnapshot, Event, EventDecodeError, JobState, Key,
    ProcessBuilder, SNAPSHOT_FORMAT_VERSION,
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
        let journal: Vec<Event> = frames
            .into_iter()
            .map(|frame| decode_event_json(&frame.to_string()).unwrap())
            .collect();
        let raw = std::fs::read_to_string(golden_path(&format!("engine_snapshot.v{version}.json")))
            .unwrap();
        let expected: EngineSnapshot = serde_json::from_str(&raw).unwrap();
        let terminal_keys: std::collections::VecDeque<_> = journal
            .iter()
            .filter_map(|e| match e {
                Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key } => Some(*instance_key),
                _ => None,
            })
            .collect();
        // #1574 review (review 5899722557) stamps each terminal instance's own
        // `terminated_epoch` with the epoch its terminal transition produced
        // (the target-specific lifecycle-validity record). Derive it from the
        // journal BEFORE the journal is consumed by replay: the Nth terminal
        // event (1-based) sets its instance's `terminated_epoch = N`. Live
        // instances keep `0`.
        let mut terminal_epoch_by_instance: std::collections::BTreeMap<Key, u64> =
            std::collections::BTreeMap::new();
        {
            let mut epoch = 0u64;
            for event in &journal {
                if let Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key } = event
                {
                    epoch += 1;
                    terminal_epoch_by_instance.insert(*instance_key, epoch);
                }
            }
        }
        let replayed = Engine::replay_partition(expected.partition_id, journal.clone());
        // Load the archived snapshot the way a host does (`from_snapshot`), so
        // its load-time normalizations apply — e.g. legacy element-keyed join
        // bookkeeping is keyed by flow-scope instance (#1580).
        let next_local = expected.next_local;
        let expected = Engine::from_snapshot(expected);

        // Replay-compatibility is a NORMALIZED equality, not a byte-identical
        // one: later builds legitimately derive some fields under semantics the
        // archived snapshot predates. Each normalization below names the change
        // whose replay-only divergence it absorbs; anything ELSE drifting still
        // fails this assert, which is what keeps the #1065 class guarded.
        let replayed_state = replayed.state().clone();
        let mut expected_state = expected.state().clone();
        if version < 3 {
            // #1440 (landed at format v3) retains a job's first terminal
            // instant/deadline; v1/v2 journals predate the event field that
            // carries it, so replaying them yields `terminal_end: None` where a
            // v3 live run would record a stamp. Normalize the pre-field
            // snapshots to the replayable floor.
            for job in expected_state.jobs.values_mut() {
                job.terminal_end = None;
            }
        }
        // #1551 clears `has_failed_with_retries_left` when a job goes terminal
        // (a completed/canceled job is not retry-eligible). Archived snapshots
        // recorded the pre-#1551 semantics — the flag surviving on a terminal
        // job — and the v1/v2 journals do not carry the information to
        // reconstruct the pre-clear value, so replay under current semantics
        // converges on the cleared flag. Normalize the archived expectation to
        // current terminal semantics.
        for job in expected_state.jobs.values_mut() {
            if matches!(job.state, JobState::Completed | JobState::Canceled) {
                job.has_failed_with_retries_left = false;
            }
        }
        // #1574 review adds the replicated `recently_terminated` ring, derived
        // purely from terminal events. Archived snapshots predate it (they load
        // it empty via `serde(default)`), so normalize it to what the archived
        // journal's own terminal events derive — computed independently here
        // from the journal, not copied from the replayed state. The same
        // change adds the replicated `terminal_epoch` counter (one increment
        // per terminal event), normalized the same way.
        expected_state.terminal_epoch = terminal_keys.len() as u64;
        expected_state.recently_terminated = terminal_keys;
        // #1193 adds `DeployedProcess::deployment_key`, derived purely from the
        // journal's `ProcessDeployed` frames. Archived snapshots predate the
        // field and load it `0` via `serde(default)`, so normalize them to the
        // deployment key the archived journal itself carries for each
        // definition (computed from the journal, not the replayed state).
        let deployment_key_by_definition: std::collections::BTreeMap<Key, Key> = journal
            .iter()
            .filter_map(|e| match e {
                Event::ProcessDeployed {
                    deployment_key,
                    process_definition_key,
                    ..
                } => Some((*process_definition_key, *deployment_key)),
                _ => None,
            })
            .collect();
        for deployed in expected_state
            .processes
            .values_mut()
            .chain(expected_state.process_versions.values_mut())
        {
            if let Some(deployment_key) = deployment_key_by_definition.get(&deployed.key) {
                deployed.deployment_key = *deployment_key;
            }
        }
        // Normalize each terminal instance's `terminated_epoch` (derived above
        // from the journal's own terminal events): archived snapshots predate
        // the field and load it `0`, but replay under current semantics records
        // it. Live instances (absent from the map) keep `0`.
        for (key, instance) in expected_state.instances.iter_mut() {
            if let Some(epoch) = terminal_epoch_by_instance.get(key) {
                instance.terminated_epoch = *epoch;
            }
        }
        assert_eq!(
            &replayed_state, &expected_state,
            "archived format {version}"
        );
        assert_eq!(
            replayed.snapshot().next_local,
            next_local,
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

/// **Message buffering (#1874).** A journal that buffers a message with a TTL,
/// correlates it to a later-opened subscription and expires another must
/// round-trip through the typed journal decoder and replay into exactly the
/// live engine's state (buffer included) and key high-water.
#[test]
fn message_buffering_journal_replays_through_typed_decoder() {
    let process = ProcessBuilder::new("await")
        .start_event("start")
        .message_intermediate_catch_event("catch", "paid", "orderId")
        .end_event("end")
        .connect("start", "catch")
        .connect("catch", "end")
        .build()
        .unwrap();
    let publish = |key: &str, ttl: u64| Command::CorrelateMessage {
        message_name: "paid".into(),
        correlation_key: key.into(),
        variables: Default::default(),
        business_id: None,
        message_key: None,
        time_to_live: Some(ttl),
        message_id: Some(format!("id-{key}")),
    };
    let mut engine = Engine::new();
    let mut journal = Vec::new();
    for (command, now) in [
        (Command::DeployProcess(process), 0),
        (publish("a", 10_000), 1_000),
        (publish("b", 500), 1_000),
        (
            Command::create_instance_with(
                "await",
                [(
                    "orderId".to_string(),
                    nanobpmn_engine_core::Value::Str("a".into()),
                )]
                .into(),
            ),
            2_000,
        ),
        (Command::TriggerTimers { now: 2_000 }, 2_000),
    ] {
        journal.extend(engine.apply_command_at(command, now).unwrap());
    }
    for kind in [
        "MessageBuffered",
        "BufferedMessageCorrelated",
        "MessageExpired",
    ] {
        assert!(
            journal
                .iter()
                .any(|e| serde_json::to_string(e).unwrap().contains(kind)),
            "the scenario must journal a {kind}"
        );
    }
    let decoded: Vec<Event> = journal
        .iter()
        .map(|e| decode_event_json(&serde_json::to_string(e).unwrap()).unwrap())
        .collect();
    assert_eq!(decoded, journal);
    let replayed = Engine::replay_partition(0, decoded);
    assert_eq!(replayed.state(), engine.state());
    assert_eq!(replayed.state().buffered_messages.len(), 1);
    assert_eq!(replayed.snapshot().next_local, engine.snapshot().next_local);
}

/// **Pre-#1676 `ElementActivating`.** An activation journaled before the
/// event-based-gateway race identity existed carries no `race` key: it must
/// decode with `race: None` (the additive `serde(default)` contract). A
/// replayed legacy catch is never retroactively enrolled into a race — the
/// applier only inserts a `races` membership when the frame carries one.
#[test]
fn pre_1676_element_activating_decodes_with_no_race() {
    let legacy_line = r#"{"ElementActivating":{"instance_key":1,"element_instance_key":2,"element_id":"onM1","scope":0}}"#;
    let decoded =
        decode_event_json(legacy_line).expect("pre-#1676 ElementActivating (no race) decodes");
    match &decoded {
        Event::ElementActivating { race, .. } => {
            assert!(race.is_none(), "a missing race must default to None")
        }
        other => panic!("expected ElementActivating, got {other:?}"),
    }
    // Replaying a journal that contains it must not error/panic, and the
    // replayed instance carries no race membership for the legacy catch (the
    // applier inserts one only from a `Some(race)` frame).
    let engine = Engine::replay(std::iter::once(decoded));
    let enrolled = engine
        .snapshot()
        .state
        .instances
        .values()
        .flat_map(|i| i.races.keys())
        .any(|k| *k == 2);
    assert!(
        !enrolled,
        "a pre-#1676 activation must not be retroactively enrolled into a race"
    );
}

/// **Pre-#1640 `JobCreated`.** A job journaled before mapping-time secret
/// provenance existed has no `secret_provenance` key: it must decode to EMPTY
/// provenance and replay into a job that resolves nothing (fail closed) — an
/// old job is never retroactively trusted.
#[test]
fn pre_1640_job_created_replays_with_empty_secret_provenance() {
    let legacy_line = r#"{"JobCreated":{"job_key":7,"instance_key":1,"element_instance_key":2,"element_id":"call","job_type":"call-api"}}"#;
    let decoded = decode_event_json(legacy_line).expect("pre-#1640 JobCreated decodes");
    match &decoded {
        Event::JobCreated {
            secret_provenance, ..
        } => assert!(secret_provenance.is_empty()),
        other => panic!("expected JobCreated, got {other:?}"),
    }
    let engine = Engine::replay(std::iter::once(decoded));
    let job = engine
        .snapshot()
        .state
        .jobs
        .get(&7)
        .cloned()
        .expect("the replayed job exists");
    assert!(
        job.secret_provenance.is_empty(),
        "a pre-#1640 job must carry no provenance (resolve nothing)"
    );
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
    assert_eq!(
        instance.join_flows_taken(&nanobpmn_engine_core::JoinKey::root("join")),
        1
    );

    let job = upgraded.activate_jobs("sync-task", "w", 1, 60_000, 0)[0].key;
    upgraded.apply_command(Command::complete_job(job)).unwrap();
    assert!(
        upgraded.is_completed(instance_key),
        "the upgraded join fires once the missing branch arrives"
    );
}

/// **Pre-#1608 `OrphanStampSuperseded`.** A supersede journaled before the
/// event carried the report's off-heap tier has no `kind` key: it must decode
/// with `kind: None` (the additive `serde(default)` contract) and replay as a
/// state no-op — the storage layer's backlog rebuild derives the tier at
/// re-stamp time for such a legacy frame.
#[test]
fn pre_1608_orphan_stamp_superseded_decodes_with_no_kind() {
    let legacy_line = r#"{"OrphanStampSuperseded":{"instance_key":27,"episode":26}}"#;
    let decoded =
        decode_event_json(legacy_line).expect("pre-#1608 OrphanStampSuperseded (no kind) decodes");
    match &decoded {
        Event::OrphanStampSuperseded { kind, .. } => {
            assert!(kind.is_none(), "a missing kind must default to None")
        }
        other => panic!("expected OrphanStampSuperseded, got {other:?}"),
    }
    // Re-serializes byte-identically (#1608 review): the storage chain digest
    // folds serialized events, so an extra `"kind":null` would fail
    // `replay_verified` on an existing digest-bearing journal.
    assert_eq!(
        serde_json::to_string(&decoded).expect("serialize"),
        legacy_line,
        "a legacy supersede must round-trip byte-identically"
    );
    // Replaying a journal that contains it must not error/panic (the reducer
    // records it pending; the point is decode + replay of the legacy shape succeed).
    let _engine = Engine::replay(std::iter::once(decoded));
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
