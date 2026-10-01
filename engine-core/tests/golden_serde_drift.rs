//! CI serde-drift guard for the snapshot/event on-disk shape (issue #1069, L3
//! of the snapshot-durability epic #1067; incident #1065).
//!
//! # What this guards
//!
//! The engine persists two serde-derived shapes that a later build must be able
//! to read back off disk:
//!
//! * [`EngineSnapshot`] / [`State`] — the snapshot graph
//!   (`engine-core/src/engine/mod.rs`), written into the versioned snapshot
//!   envelope introduced by L2 (#1068).
//! * [`Event`] — the event frame (`engine-core/src/event.rs`). Migration-by-replay
//!   (#1071) replays the journal under new code, so a silent drift in the event
//!   frame breaks replay exactly as badly as a snapshot drift.
//!
//! This is the engine-core analogue of the read-model's
//! `schema_edit_requires_version_bump` (`read-model/src/store.rs`), which pins
//! `SCHEMA` to a checked-in fingerprint tied to `SCHEMA_VERSION`. Here we use the
//! **golden-sample** approach instead of a hash: a representative snapshot and a
//! representative event journal are serialized and checked in byte-for-byte. The
//! samples double as the cross-version corpus that #1070 (replay tests) and
//! #1071 (replay-migrator) consume — see [`README`](fixtures/golden/README.md).
//!
//! # How it ties to the version
//!
//! The golden filenames embed [`SNAPSHOT_FORMAT_VERSION`] (from L2, #1068):
//! `engine_snapshot.v{N}.json` / `event_corpus.v{N}.json`. So the guard asserts
//! the *current* build's serialized shape still matches the golden recorded **at
//! the current version**. Any shape change to `EngineSnapshot`, `State` or
//! `Event` changes the serialized bytes and fails this test, forcing a human
//! decision at the drift point (the guard does not — and cannot — *prove*
//! serde-json compatibility; an added `#[serde(default)]` field is compatible, a
//! rename is not. Its job is to make the change impossible to land silently).
//!
//! # CRITICAL: `--features serde`
//!
//! The `Event`/`EngineSnapshot` serde derives are feature-gated
//! (`cfg_attr(feature = "serde", ...)`). Without the feature the types do not
//! derive `Serialize`/`Deserialize`, this whole file compiles to nothing, and
//! the guard is **inert** (silently passing). The `engine-core (clippy + test)`
//! CI job therefore runs with `--features serde`; downstream tasks (#1070/#1071)
//! reuse that job and do not re-add the flag.
//!
//! # Regenerating the goldens
//!
//! When you make a *deliberate, compatible* change to the shape (or you have
//! bumped [`SNAPSHOT_FORMAT_VERSION`] and need the new `vN` corpus), regenerate
//! the fixtures deterministically:
//!
//! ```sh
//! UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift
//! ```
//!
//! then review the diff and commit it. Never edit the JSON by hand.

#![cfg(feature = "serde")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::{
    Command, Engine, EngineSnapshot, Event, ProcessBuilder, ProcessDefinition, Value,
    SNAPSHOT_FORMAT_VERSION,
};

/// Directory holding the checked-in golden corpus, relative to the crate root.
const GOLDEN_SUBDIR: &str = "tests/fixtures/golden";

/// Absolute path to a golden fixture file by name.
fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(GOLDEN_SUBDIR)
        .join(name)
}

/// The version-pinned golden filenames. Bumping [`SNAPSHOT_FORMAT_VERSION`]
/// (declaring a breaking payload change, per L2 #1068) points the guard at a
/// fresh `vN` file — the author must regenerate it, while the prior `vN-1`
/// corpus stays checked in for #1071's cross-version replay-migrator.
fn snapshot_golden_name() -> String {
    format!("engine_snapshot.v{SNAPSHOT_FORMAT_VERSION}.json")
}
fn event_corpus_golden_name() -> String {
    format!("event_corpus.v{SNAPSHOT_FORMAT_VERSION}.json")
}

/// Serialize `value` to a **canonical**, deterministic, human-readable JSON
/// string: routed through [`serde_json::Value`] (whose object is a `BTreeMap`,
/// so every object's keys are emitted in sorted order regardless of the source
/// `HashMap`'s per-process iteration order) and pretty-printed with a trailing
/// newline. The scenario in [`build_golden_corpus`] is deliberately kept so that
/// every set-valued (`HashSet`) field holds at most one element, so the only
/// remaining source of non-determinism — array order — cannot bite.
fn canonical_json<T: serde::Serialize>(value: &T) -> String {
    let canonical: serde_json::Value =
        serde_json::to_value(value).expect("value serializes to serde_json::Value");
    let mut s = serde_json::to_string_pretty(&canonical).expect("canonical value pretty-prints");
    s.push('\n');
    s
}

/// Assert `actual` matches the checked-in golden at `name`, OR (when
/// `UPDATE_GOLDEN` is set in the environment) overwrite the golden with `actual`
/// so the author can review + commit the refresh. The failure message spells out
/// the author's two options, mirroring the SQL drift guard's ethos (#831).
fn assert_or_update_golden(name: &str, actual: &str) {
    let path = golden_path(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create golden dir");
        }
        std::fs::write(&path, actual).unwrap_or_else(|e| panic!("write golden {name}: {e}"));
        eprintln!("UPDATE_GOLDEN: refreshed {}", path.display());
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read golden fixture {}: {e}\n\
             If you are establishing a NEW version's corpus (you just bumped \
             SNAPSHOT_FORMAT_VERSION to {SNAPSHOT_FORMAT_VERSION}), generate it with:\n    \
             UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift",
            path.display()
        )
    });

    assert_eq!(
        actual, expected,
        "\n\n\
         ============================================================================\n\
         SERDE DRIFT DETECTED in the persisted snapshot/event shape ({name}).\n\
         ============================================================================\n\
         The serialized form of EngineSnapshot / State / Event no longer matches the\n\
         golden recorded at SNAPSHOT_FORMAT_VERSION = {SNAPSHOT_FORMAT_VERSION}. An\n\
         incompatible on-disk format must never reach a production restart, so this\n\
         change cannot land until you make an explicit decision:\n\
         \n\
         (1) BREAKING change (a field/variant rename, retag, type change or reorder —\n\
             anything serde's additive `#[serde(default)]` forward-compat does NOT\n\
             rescue): bump SNAPSHOT_FORMAT_VERSION in engine-core/src/engine/mod.rs,\n\
             then regenerate the new-version corpus with\n\
                 UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift\n\
             and ensure #1071's replay-migrator handles the old->new transition.\n\
         \n\
         (2) ADDITIVE / COMPATIBLE change (e.g. a new field carrying\n\
             `#[serde(default)]`, so old on-disk data still deserializes): keep the\n\
             version, refresh the golden in place with\n\
                 UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift\n\
             and confirm in review the change really is backward-compatible.\n\
         ============================================================================\n"
    );
}

/// Build the deterministic golden corpus: a representative engine driven through
/// a realistic-but-controlled journal, returning the resulting snapshot and the
/// full ordered event journal.
///
/// Determinism contract (so the serialized bytes are stable across processes):
/// the scenario keeps every `HashSet`-valued `State` field to at most one
/// element — at snapshot time no job is left in the `Activated` set and each
/// instance owns at most one job — so the only non-`HashMap` collections that
/// serialize to JSON arrays are trivially ordered. All `HashMap`s are canonically
/// key-sorted by [`canonical_json`]. Do not add multi-job instances or leave
/// several jobs activated here without re-checking cross-process stability.
fn build_golden_corpus() -> (EngineSnapshot, Vec<Event>) {
    // Fixed logical clock so `now`, timers and any timestamped events are stable.
    const T0: u64 = 1_700_000_000_000;

    let mut journal: Vec<Event> = Vec::new();
    let mut engine = Engine::new();

    // A linear process with a single service task emitting `payment` jobs.
    let payment: ProcessDefinition = ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .build()
        .expect("build order process");

    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(payment), T0)
            .expect("deploy order"),
    );

    // An inclusive-gateway (OR split/join) witness process. Deployed but never
    // instantiated: its only purpose is to embed an `ElementKind::InclusiveGateway`
    // — plus a condition-routed default flow — in the snapshot's persisted process
    // definitions, so the checked-in golden pins that variant's serialized bytes.
    // Without a witness here the #1069 drift guard would stay green if a field were
    // added to (or the tag of) `InclusiveGateway` changed, exactly the variant-
    // exhaustiveness blind spot `snapshot_forward_compat.rs` documents.
    let review: ProcessDefinition = ProcessBuilder::new("review")
        .start_event("s")
        .inclusive_gateway("split")
        .service_task("audit", "audit")
        .service_task("notify", "notify")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "split")
        .connect_when("split", "audit", "= risk > 10")
        .connect_default("split", "notify")
        .connect("audit", "join")
        .connect("notify", "join")
        .connect("join", "e")
        .build()
        .expect("build review process");

    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(review), T0)
            .expect("deploy review"),
    );

    // An escalation witness process. Deployed but never instantiated: its only
    // purpose is to embed `ElementKind::EscalationThrowEvent` and BOTH flavours
    // of `ElementKind::EscalationBoundaryEvent` — interrupting (`true`) and the
    // non-interrupting default (`false`) — in the snapshot's persisted process
    // definitions, so the checked-in golden pins their serialized tags/fields,
    // including the `interrupting` bool. Without a witness here the #1069 drift
    // guard would stay green if a field were added to (or the tag of) either
    // escalation variant changed — exactly the variant-exhaustiveness blind spot
    // `snapshot_forward_compat.rs` documents (#1173).
    let escalation: ProcessDefinition = ProcessBuilder::new("escalate")
        .start_event("s")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .escalation_boundary_event("b_int", "sub", "OVERLOAD")
        .non_interrupting_escalation_boundary_event("b_non", "sub", "")
        .end_event("e")
        .end_event("h_int")
        .end_event("h_non")
        .connect("s", "sub")
        .connect("sub_start", "throw")
        .connect("throw", "sub_end")
        .connect("sub", "e")
        .connect("b_int", "h_int")
        .connect("b_non", "h_non")
        .build()
        .expect("build escalation process");

    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(escalation), T0)
            .expect("deploy escalation"),
    );

    // Instance A: created with variables, its one job activated + completed, so
    // it walks the full lifecycle to a retained-terminal instance. Leaves the
    // `Activated` set empty again (the job completed).
    let mut vars_a = HashMap::new();
    vars_a.insert("amount".to_string(), Value::Int(4200));
    vars_a.insert("currency".to_string(), Value::Str("EUR".to_string()));
    journal.extend(
        engine
            .apply_command_at(Command::create_instance_with("order", vars_a), T0 + 1)
            .expect("create instance A"),
    );
    // Drive activation through the Command (not the `activate_jobs` helper, which
    // discards the journal events) so the emitted `Event::JobActivated` lands in
    // the corpus — keeping it a faithful, replayable journal for #1070/#1071.
    let activated_a = engine
        .apply_command_at(
            Command::activate_jobs_by_key(
                engine.select_activatable_job_keys("payment", 1, T0 + 2, false),
                "worker-1",
                30_000,
                T0 + 2,
                Default::default(),
            ),
            T0 + 2,
        )
        .expect("activate job A");
    let job_a = activated_a
        .iter()
        .find_map(|e| match e {
            Event::JobActivated { job_key, .. } => Some(*job_key),
            _ => None,
        })
        .expect("instance A job was activated");
    journal.extend(activated_a);
    let mut result_a = HashMap::new();
    result_a.insert("approved".to_string(), Value::Bool(true));
    // The completion assigns a Camunda 8.10 business id to root instance A,
    // pinning the additive `ProcessInstanceBusinessIdAssigned` frame and the
    // snapshot's assigned `business_id`.
    journal.extend(
        engine
            .apply_command_at(
                Command::complete_job_with(job_a, result_a)
                    .with_business_id(Some("order-A".to_string())),
                T0 + 3,
            )
            .expect("complete job A"),
    );

    // Instance B: created and left parked on its (created, NOT activated) job —
    // the live working set. One job, not in the `Activated` set.
    let mut vars_b = HashMap::new();
    vars_b.insert("amount".to_string(), Value::Int(999));
    let created_b = engine
        .apply_command_at(Command::create_instance_with("order", vars_b), T0 + 4)
        .expect("create instance B");
    let instance_b = created_b
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .expect("instance B was created");
    journal.extend(created_b);

    // Instance B is suspended, resumed, then suspended again (Camunda-parity
    // suspend/resume). This exercises both the additive `ProcessInstanceSuspended`
    // and `ProcessInstanceResumed` event frames and leaves a currently-suspended
    // instance in the snapshot (its `suspended_at` carries the last instant), so
    // the golden corpus pins the replay shape of both.
    journal.extend(
        engine
            .apply_command_at(Command::suspend_instance(instance_b), T0 + 5)
            .expect("suspend instance B"),
    );
    journal.extend(
        engine
            .apply_command_at(Command::resume_instance(instance_b), T0 + 6)
            .expect("resume instance B"),
    );
    journal.extend(
        engine
            .apply_command_at(Command::suspend_instance(instance_b), T0 + 7)
            .expect("re-suspend instance B"),
    );

    // Parallel-join witnesses (#1233). `dup` joins two distinct flows between
    // the same pair of elements and completes at once, pinning
    // `ParallelJoinTokenArrived.flow` (ordinals 0 and 1) and
    // `ParallelJoinFired`. `sync` parks half-open on its one job, pinning a
    // non-empty `join_flow_arrivals` in the snapshot.
    let dup: ProcessDefinition = ProcessBuilder::new("dup")
        .start_event("s")
        .parallel_gateway("fork")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "join")
        .connect("fork", "join")
        .connect("join", "e")
        .build()
        .expect("build dup process");
    let sync: ProcessDefinition = ProcessBuilder::new("sync")
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
        .expect("build sync process");
    for (def, at) in [(dup, T0 + 8), (sync, T0 + 8)] {
        journal.extend(
            engine
                .apply_command_at(Command::DeployProcess(def), at)
                .expect("deploy join witness"),
        );
    }
    for (id, at) in [("dup", T0 + 9), ("sync", T0 + 10)] {
        journal.extend(
            engine
                .apply_command_at(Command::create_instance(id), at)
                .expect("create join witness instance"),
        );
    }

    // Decision-evaluation witnesses (#1292): pin `DecisionEvaluated` (its
    // `decision_evaluation_key`, `failure`, and each evaluated decision's
    // `decision_key`/`decision_version`) across a successful businessRuleTask,
    // a failed one (FAILED evaluation ahead of its incident) and a standalone
    // `EvaluateDecision`, plus the deployed-decision snapshot state.
    let drg = nanobpmn_engine_core::dmn::parse_dmn(
        r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="greet" name="greet">
      <decision id="base" name="Base">
        <literalExpression id="bl"><text>"b"</text></literalExpression>
      </decision>
      <decision id="root" name="Root">
        <informationRequirement id="ir"><requiredDecision href="#base" /></informationRequirement>
        <decisionTable hitPolicy="UNIQUE">
          <input id="ri"><inputExpression id="re" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="ro" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="rie1"><text>"xx"</text></inputEntry>
            <outputEntry id="roe1"><text>"a"</text></outputEntry></rule>
          <rule id="r2"><inputEntry id="rie2"><text>"xx"</text></inputEntry>
            <outputEntry id="roe2"><text>"b"</text></outputEntry></rule>
          <rule id="r3"><inputEntry id="rie3"><text>not("xx")</text></inputEntry>
            <outputEntry id="roe3"><text>base</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##,
    )
    .expect("parse decision witness DRG");
    let decide: ProcessDefinition = ProcessBuilder::new("decide")
        .start_event("s")
        .business_rule_task("brt", "root", Some("out".to_string()))
        .end_event("e")
        .connect("s", "brt")
        .connect("brt", "e")
        .build()
        .expect("build decide process");
    journal.extend(
        engine
            .apply_command_at(Command::DeployDecisionRequirements(vec![drg]), T0 + 11)
            .expect("deploy decision witness DRG"),
    );
    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(decide), T0 + 11)
            .expect("deploy decide process"),
    );
    for (lang, at) in [("de", T0 + 12), ("xx", T0 + 13)] {
        let mut vars = HashMap::new();
        vars.insert("lang".to_string(), Value::Str(lang.to_string()));
        journal.extend(
            engine
                .apply_command_at(Command::create_instance_with("decide", vars), at)
                .expect("create decide instance"),
        );
    }
    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("en".to_string()));
    journal.extend(
        engine
            .apply_command_at(
                Command::EvaluateDecision {
                    decision: nanobpmn_engine_core::DecisionReference::Id("root".to_string()),
                    variables: vars,
                },
                T0 + 14,
            )
            .expect("standalone evaluation"),
    );

    let snapshot = engine.snapshot();
    (snapshot, journal)
}

/// Guards the `EngineSnapshot` / `State` serialized shape against silent drift.
#[test]
fn engine_snapshot_shape_is_pinned_to_version() {
    let (snapshot, _journal) = build_golden_corpus();
    let actual = canonical_json(&snapshot);
    assert_or_update_golden(&snapshot_golden_name(), &actual);
}

/// Guards the `Event` serialized frame against silent drift, and lands the
/// replay corpus #1070/#1071 consume.
#[test]
fn event_corpus_shape_is_pinned_to_version() {
    let (_snapshot, journal) = build_golden_corpus();
    let actual = canonical_json(&journal);
    assert_or_update_golden(&event_corpus_golden_name(), &actual);
}

/// The corpus must round-trip: the checked-in golden snapshot deserializes back
/// into an `EngineSnapshot` a current build can load (so #1070/#1071, and any
/// real restart, can consume it). This also catches a golden that was
/// hand-edited into something the current types can no longer parse.
#[test]
fn golden_snapshot_deserializes_under_current_build() {
    // Skip the round-trip when regenerating (the file may not exist yet).
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        return;
    }
    let raw = std::fs::read_to_string(golden_path(&snapshot_golden_name()))
        .expect("read golden snapshot");
    let decoded: EngineSnapshot =
        serde_json::from_str(&raw).expect("golden snapshot deserializes under current build");
    // Rebuilding an engine from it must not panic (validates invariants like the
    // partition-id bound in `from_snapshot`).
    let _engine = Engine::from_snapshot(decoded);

    let raw_events = std::fs::read_to_string(golden_path(&event_corpus_golden_name()))
        .expect("read golden event corpus");
    let _decoded_events: Vec<Event> =
        serde_json::from_str(&raw_events).expect("golden event corpus deserializes");
}

/// Serialization is deterministic within a single build — a cheap self-check
/// that `canonical_json` really is stable (the read-model precedent has the
/// analogous `schema_fingerprint_is_stable_within_a_build`).
#[test]
fn golden_serialization_is_deterministic() {
    let (snapshot, journal) = build_golden_corpus();
    assert_eq!(canonical_json(&snapshot), canonical_json(&snapshot));
    assert_eq!(canonical_json(&journal), canonical_json(&journal));

    // And a fresh, independently-built corpus serializes identically — catches
    // any accidental dependence on per-process HashMap/HashSet iteration order.
    let (snapshot2, journal2) = build_golden_corpus();
    assert_eq!(canonical_json(&snapshot), canonical_json(&snapshot2));
    assert_eq!(canonical_json(&journal), canonical_json(&journal2));
}

/// Directly exercises the property the whole corpus relies on: `canonical_json`
/// must be **insensitive to HashMap insertion/iteration order**, emitting object
/// keys in sorted order. The `_is_deterministic` test above rebuilds the *same*
/// corpus, so within one process its HashMaps share a RandomState seed and can
/// end up in identical iteration order regardless of insertion — meaning it would
/// still pass even if `canonical_json` stopped sorting keys. This test defeats
/// that by inserting the *same* entries in two different orders and asserting the
/// canonical bytes match AND are sorted, so it fails the moment canonicalization
/// regresses (e.g. stops routing through serde_json's key-sorted `Value`).
#[test]
fn canonical_json_is_insensitive_to_map_insertion_order() {
    let entries = [
        ("zeta", Value::Int(1)),
        ("alpha", Value::Int(2)),
        ("mu", Value::Str("m".to_string())),
        ("beta", Value::Bool(true)),
    ];

    let mut forward: HashMap<String, Value> = HashMap::new();
    for (k, v) in entries.iter() {
        forward.insert((*k).to_string(), v.clone());
    }
    let mut reverse: HashMap<String, Value> = HashMap::new();
    for (k, v) in entries.iter().rev() {
        reverse.insert((*k).to_string(), v.clone());
    }

    let forward_json = canonical_json(&forward);
    let reverse_json = canonical_json(&reverse);
    assert_eq!(
        forward_json, reverse_json,
        "canonical_json must not depend on HashMap insertion/iteration order"
    );

    // The bytes are equal *because* keys are emitted in sorted order — assert
    // that directly so a regression to raw (unsorted) iteration order is caught
    // even in the unlucky case where two orders happen to iterate identically.
    let alpha = forward_json.find("\"alpha\"").expect("alpha key present");
    let beta = forward_json.find("\"beta\"").expect("beta key present");
    let mu = forward_json.find("\"mu\"").expect("mu key present");
    let zeta = forward_json.find("\"zeta\"").expect("zeta key present");
    assert!(
        alpha < beta && beta < mu && mu < zeta,
        "canonical_json must emit object keys in sorted order:\n{forward_json}"
    );
}
