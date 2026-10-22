//! Forward-compatibility guard for the persisted **snapshot** shape
//! (`EngineSnapshot` / `ElementKind`) — the snapshot-side analogue of the
//! event-frame test `legacy_record_missing_a_defaulted_field_replays` in
//! `golden_replay.rs`.
//!
//! # Why this exists (incident: the pre-#1057 upgrade-boot crash)
//!
//! `ElementKind` is part of the persisted `EngineSnapshot`. When #1057 added
//! `propagate_all_parent_variables` / `propagate_all_child_variables` to
//! `ElementKind::CallActivity` **without `#[serde(default)]`**, every snapshot
//! written before #1057 stopped deserializing under the new binary, and a node
//! restarting on an existing data dir panicked during recovery with:
//!
//! ```text
//! snapshot payload deserialize failed: missing field `propagate_all_parent_variables`
//! ```
//!
//! Migration-by-replay (#1071) could not rescue it: on a data dir whose journal
//! was already compacted away (no cold archive), there are no events left to
//! replay, so the only durable copy of the model lived in the now-unreadable
//! snapshot.
//!
//! # Why the #1069 golden drift guard did not catch it
//!
//! Two blind spots this file closes:
//!
//! 1. **The golden corpus is not variant-exhaustive.** `build_golden_corpus()`
//!    (`golden_serde_drift.rs`) contains no `CallActivity`, so a field added to
//!    that variant changed no golden bytes and the drift guard stayed green.
//!    [`callactivity_variant_witness`] below is a compile-time forcing function:
//!    a new `ElementKind` variant will not compile until its author has read the
//!    forward-compat checklist.
//! 2. **Nothing proved an OLD snapshot still loads.** The golden is regenerated
//!    at the current shape, so it always contains the newest fields and never
//!    exercises the "field absent" path. Additive changes do not bump
//!    `SNAPSHOT_FORMAT_VERSION`, so no prior-shape fixture is retained either.
//!    The tests here deserialize an explicitly older shape (fields stripped) and
//!    assert it still loads — going **RED at the moment of a #1057-style
//!    change**, pointing straight at the missing `serde(default)`.
//!
//! # CRITICAL: `--features serde`
//!
//! The `EngineSnapshot` / `ElementKind` serde derives are feature-gated, so this
//! whole file is `#![cfg(feature = "serde")]` and is built/run by the
//! `engine-core (clippy + test)` CI job (which passes `--features serde`).

#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use nanobpmn_engine_core::{Command, ElementKind, Engine, EngineSnapshot, ProcessBuilder};

/// The two `CallActivity` fields #1057 added. An older snapshot predates them;
/// stripping them from a current snapshot reproduces exactly that on-disk shape.
const CALL_ACTIVITY_ADDED_FIELDS: &[&str] = &[
    "propagate_all_parent_variables",
    "propagate_all_child_variables",
];

/// Recursively removes every object member named in `keys` from a JSON value —
/// used to synthesise an OLDER serialized shape from a current snapshot (a field
/// that did not exist in the old shape is simply absent).
fn strip_keys(value: &mut serde_json::Value, keys: &[&str]) {
    match value {
        serde_json::Value::Object(map) => {
            for k in keys {
                map.remove(*k);
            }
            for (_, v) in map.iter_mut() {
                strip_keys(v, keys);
            }
        }
        serde_json::Value::Array(items) => {
            for v in items.iter_mut() {
                strip_keys(v, keys);
            }
        }
        _ => {}
    }
}

#[test]
fn agent_job_type_survives_snapshot_and_legacy_snapshots_default_to_element_id() {
    let model = include_str!("fixtures/external-agent-job-type.bpmn");
    for legacy in [false, true] {
        let def = nanobpmn_engine_core::bpmn::parse_bpmn(model)
            .unwrap()
            .remove(0);
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let mut json = serde_json::to_value(engine.snapshot()).unwrap();
        assert!(json.to_string().contains("\"job_type\":\"senior:rebase\""));
        if legacy {
            for index in ["processes", "process_versions"] {
                for deployed in json["state"][index].as_object_mut().unwrap().values_mut() {
                    deployed["definition"]["elements"]["agent"]["kind"] = serde_json::json!({
                        "AgentTask": { "agent_type": "External" }
                    });
                }
            }
        }
        let snapshot: EngineSnapshot = serde_json::from_value(json).unwrap();
        let mut engine = Engine::from_snapshot(snapshot);
        engine
            .apply_command(Command::create_instance_with(
                "external-agent-routing",
                std::collections::HashMap::from([(
                    "route".into(),
                    nanobpmn_engine_core::Value::Str("senior:rebase".into()),
                )]),
            ))
            .unwrap();
        let expected = if legacy { "agent" } else { "senior:rebase" };
        assert_eq!(engine.activate_jobs(expected, "W", 1, 1_000, 0).len(), 1);
    }
}

#[test]
fn historical_agent_deployment_frames_replay_through_the_canonical_job_model() {
    use nanobpmn_engine_core::{bpmn::parse_bpmn, decode_event_json, Event, Value};
    for marker in ["External", "AiAgentTask"] {
        for job_type in [None, Some("senior:rebase")] {
            let def = parse_bpmn(include_str!("fixtures/external-agent-job-type.bpmn"))
                .unwrap()
                .remove(0);
            let mut engine = Engine::new();
            let events = engine.apply_command(Command::DeployProcess(def)).unwrap();
            let mut frames = Vec::new();
            for event in events {
                let mut json = serde_json::to_value(event).unwrap();
                if let Some(deployment) = json.get_mut("ProcessDeployed") {
                    let mut legacy = serde_json::json!({"agent_type": marker});
                    if let Some(job_type) = job_type {
                        legacy["job_type"] = serde_json::json!(job_type);
                    }
                    deployment["process"]["elements"]["agent"]["kind"] =
                        serde_json::json!({"AgentTask": legacy});
                }
                frames.push(decode_event_json(&serde_json::to_string(&json).unwrap()).unwrap());
            }
            let mut restored = Engine::replay(frames);
            assert!(matches!(
                restored.state().processes["external-agent-routing"]
                    .definition
                    .elements["agent"]
                    .kind,
                ElementKind::ServiceTask {
                    agent_type: Some(_),
                    ..
                }
            ));
            let events = restored
                .apply_command(Command::create_instance_with(
                    "external-agent-routing",
                    std::collections::HashMap::from([(
                        "route".into(),
                        Value::Str("senior:rebase".into()),
                    )]),
                ))
                .unwrap();
            assert!(!events
                .iter()
                .any(|e| matches!(e, Event::AgentInstanceCreated { .. })));
            let jobs = restored.activate_jobs(job_type.unwrap_or("agent"), "W", 1, 1_000, 0);
            assert_eq!(jobs.len(), 1);
            assert!(
                jobs[0].lease_token.is_none(),
                "legacy markers do not implicitly request a lease"
            );
        }
    }
}

#[test]
fn legacy_jobless_agent_snapshot_preserves_live_state_without_inventing_jobs() {
    use nanobpmn_engine_core::{AgentDefinition, AgentType, Event};

    let def = ProcessBuilder::new("legacy-live")
        .start_event("start")
        .agent_task("agent", "worker", AgentType::AiAgentTask)
        .end_event("end")
        .connect("start", "agent")
        .connect("agent", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("legacy-live"))
        .unwrap();
    let element_instance_key = events
        .iter()
        .find_map(|event| match event {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .unwrap();
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition {
                model: Some("legacy-model".into()),
                ..Default::default()
            },
            limits: None,
            history: Vec::new(),
        })
        .unwrap();
    let mut json = serde_json::to_value(engine.snapshot()).unwrap();
    let live_instances = json["state"]["instances"].clone();
    json["state"]["jobs"] = serde_json::json!({});
    for index in ["processes", "process_versions"] {
        for deployed in json["state"][index].as_object_mut().unwrap().values_mut() {
            deployed["definition"]["elements"]["agent"]["kind"] =
                serde_json::json!({"AgentTask": {"agent_type": "AiAgentTask"}});
        }
    }
    let restored = Engine::from_snapshot(serde_json::from_value(json).unwrap());
    assert!(restored.state().jobs.is_empty());
    assert_eq!(
        serde_json::to_value(&restored.state().instances).unwrap(),
        live_instances,
    );
    assert!(matches!(
        restored.state().processes["legacy-live"]
            .definition
            .elements["agent"]
            .kind,
        ElementKind::ServiceTask {
            agent_type: Some(AgentType::AiAgentTask),
            ..
        }
    ));
}

/// Builds a one-element process whose only flow node is a `CallActivity`, deploys
/// it, and returns the resulting `EngineSnapshot` (which embeds the deployed
/// model, and therefore the `ElementKind::CallActivity`).
fn snapshot_with_call_activity() -> EngineSnapshot {
    let def = ProcessBuilder::new("parent")
        .start_event("start")
        .call_activity("call", "child")
        .end_event("end")
        .connect("start", "call")
        .connect("call", "end")
        .build()
        .expect("build parent process with a call activity");

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(def))
        .expect("deploy parent process");
    engine.snapshot()
}

/// The core forward-compat guard, exercised through the **real snapshot
/// deserialize path**: a snapshot serialized before #1057 (its `CallActivity`
/// carrying neither propagation flag) must still deserialize under the current
/// build, defaulting both flags to `true` (the Zeebe / parse-time default a
/// fresh redeploy of the same BPMN would produce).
///
/// Before the `#[serde(default = "default_propagate")]` fix this panics with
/// `missing field propagate_all_parent_variables` — the exact upgrade-boot crash.
#[test]
fn pre_1057_snapshot_without_callactivity_propagation_flags_still_loads() {
    let snapshot = snapshot_with_call_activity();

    // Serialize, then drop the fields to synthesise the pre-#1057 on-disk shape.
    let mut json = serde_json::to_value(&snapshot).expect("serialize snapshot");
    // Sanity: the current shape really does carry the fields we are about to
    // strip (otherwise this test would vacuously pass forever).
    let before = json.to_string();
    assert!(
        CALL_ACTIVITY_ADDED_FIELDS
            .iter()
            .all(|k| before.contains(k)),
        "current snapshot must contain the CallActivity propagation fields; \
         did a variant/field rename make this fixture stale?"
    );
    strip_keys(&mut json, CALL_ACTIVITY_ADDED_FIELDS);

    // The real path: deserialize the old shape back into an EngineSnapshot and
    // rebuild an engine from it (as recovery does).
    let decoded: EngineSnapshot = serde_json::from_value(json)
        .expect("pre-#1057 snapshot (no CallActivity propagation flags) must still deserialize");
    let _engine = Engine::from_snapshot(decoded);
}

/// The same property at the `ElementKind` granularity, against a hand-authored
/// literal (human-readable, independent of the builder): the pre-#1057
/// `CallActivity` serialized only `called_process_id`.
#[test]
fn legacy_call_activity_element_kind_deserializes_to_true_defaults() {
    let legacy = r#"{"CallActivity":{"called_process_id":"child"}}"#;
    let kind: ElementKind =
        serde_json::from_str(legacy).expect("legacy CallActivity ElementKind must deserialize");
    assert_eq!(
        kind,
        ElementKind::CallActivity {
            called_process_id: "child".to_string(),
            propagate_all_parent_variables: true,
            propagate_all_child_variables: true,
        }
    );
}

/// The inverse property: an explicitly-persisted `false` must survive the
/// round-trip untouched — the `serde(default)` must default only an *absent*
/// field, never override a present one.
#[test]
fn explicit_call_activity_flags_round_trip_verbatim() {
    let original = ElementKind::CallActivity {
        called_process_id: "child".to_string(),
        propagate_all_parent_variables: false,
        propagate_all_child_variables: false,
    };
    let json = serde_json::to_string(&original).expect("serialize CallActivity");
    let back: ElementKind = serde_json::from_str(&json).expect("deserialize CallActivity");
    assert_eq!(back, original);
}

/// Issue #1159 forward-compat guard, at the `AdHocToolKind` granularity. The
/// call-activity **tool** variant (`AdHocToolKind::CallActivity`) gained the same
/// two propagation flags, each `#[serde(default = "default_true")]`. This is a
/// distinct persisted enum from `ElementKind` (it lives in the ad-hoc tool
/// catalog carried in the snapshot), so it needs its own old-shape guard: a
/// catalog serialized before #1159's flags — its tool carrying only
/// `process_id` — must still deserialize under the current build, defaulting
/// both flags to `true` (the Zeebe / parse-time default a redeploy produces).
/// Before the `serde(default)` this would panic with a missing-field error on an
/// upgrade boot, exactly like the pre-#1057 `ElementKind::CallActivity` crash.
#[test]
fn legacy_adhoc_call_activity_tool_deserializes_to_true_defaults() {
    use nanobpmn_engine_core::AdHocToolKind;
    let legacy = r#"{"CallActivity":{"process_id":"child"}}"#;
    let kind: AdHocToolKind = serde_json::from_str(legacy)
        .expect("legacy AdHocToolKind::CallActivity (no propagation flags) must deserialize");
    assert_eq!(
        kind,
        AdHocToolKind::CallActivity {
            process_id: Some("child".to_string()),
            propagate_all_parent_variables: true,
            propagate_all_child_variables: true,
        }
    );
}

/// The inverse for the tool variant: an explicitly-persisted `false` on either
/// propagation flag must survive the round-trip verbatim — `serde(default)` may
/// only fill an *absent* field, never override a present one.
#[test]
fn explicit_adhoc_call_activity_tool_flags_round_trip_verbatim() {
    use nanobpmn_engine_core::AdHocToolKind;
    let original = AdHocToolKind::CallActivity {
        process_id: Some("child".to_string()),
        propagate_all_parent_variables: false,
        propagate_all_child_variables: false,
    };
    let json = serde_json::to_string(&original).expect("serialize AdHocToolKind::CallActivity");
    let back: AdHocToolKind =
        serde_json::from_str(&json).expect("deserialize AdHocToolKind::CallActivity");
    assert_eq!(back, original);
}

/// Compile-time forcing function (mirrors `processos`'s `variant_witness`): an
/// exhaustive `match` over `ElementKind` that does nothing at runtime. Its sole
/// job is to **fail to compile when a new variant is added**, dropping the author
/// into this file — next to the forward-compat corpus — with the checklist below.
///
/// NEW VARIANT CHECKLIST (read before adding an arm):
///
/// 1. If your change adds a field to an **existing** persisted variant, that
///    field MUST carry `#[serde(default)]` (or `serde(default = "..")` for a
///    non-`Default` default), and you MUST add an old-shape fixture/assertion
///    above so an older snapshot still loads. This is the rule whose absence
///    caused the pre-#1057 upgrade-boot crash.
/// 2. A brand-new variant is additive-safe (old snapshots never contain it), so
///    it needs only an arm here — but still refresh the #1069 golden corpus.
/// 3. A rename/removal/retag is a BREAKING change: bump `SNAPSHOT_FORMAT_VERSION`
///    and follow the migrator contract in AGENTS.md ("Adding or Changing an
///    Event").
#[test]
fn callactivity_variant_witness_is_exhaustive() {
    fn witness(k: &ElementKind) -> &'static str {
        use ElementKind::*;
        match k {
            StartEvent => "startEvent",
            EndEvent => "endEvent",
            TerminateEndEvent => "terminateEndEvent",
            ServiceTask { .. } => "serviceTask",
            BusinessRuleTask { .. } => "businessRuleTask",
            UserTask(_) => "userTask",
            ExclusiveGateway => "exclusiveGateway",
            ParallelGateway => "parallelGateway",
            InclusiveGateway => "inclusiveGateway",
            EventBasedGateway => "eventBasedGateway",
            ErrorBoundaryEvent { .. } => "errorBoundaryEvent",
            TimerIntermediateCatchEvent { .. } => "timerIntermediateCatchEvent",
            TimerBoundaryEvent { .. } => "timerBoundaryEvent",
            MessageIntermediateCatchEvent { .. } => "messageIntermediateCatchEvent",
            MessageBoundaryEvent { .. } => "messageBoundaryEvent",
            MessageStartEvent { .. } => "messageStartEvent",
            TimerStartEvent { .. } => "timerStartEvent",
            SubProcess { .. } => "subProcess",
            IntermediateThrowEvent => "intermediateThrowEvent",
            EscalationThrowEvent { .. } => "escalationThrowEvent",
            EscalationBoundaryEvent { .. } => "escalationBoundaryEvent",
            LinkIntermediateThrowEvent { .. } => "linkIntermediateThrowEvent",
            LinkIntermediateCatchEvent { .. } => "linkIntermediateCatchEvent",
            Task => "task",
            ScriptTask { .. } => "scriptTask",
            CallActivity { .. } => "callActivity",
            SignalIntermediateCatchEvent { .. } => "signalIntermediateCatchEvent",
            SignalBoundaryEvent { .. } => "signalBoundaryEvent",
            ConditionalIntermediateCatchEvent { .. } => "conditionalIntermediateCatchEvent",
            ConditionalBoundaryEvent { .. } => "conditionalBoundaryEvent",
            CompensationBoundaryEvent { .. } => "compensationBoundaryEvent",
            CompensationThrowEvent => "compensationThrowEvent",
            AgentTask { .. } => "agentTask",
        }
    }

    // A representative instance run through the witness — the assertion is
    // incidental; the point is the exhaustive match above compiling at all.
    let k = ElementKind::CallActivity {
        called_process_id: "child".to_string(),
        propagate_all_parent_variables: true,
        propagate_all_child_variables: true,
    };
    assert_eq!(witness(&k), "callActivity");
}
