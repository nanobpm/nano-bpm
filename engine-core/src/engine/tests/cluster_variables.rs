//! Cluster variables as replicated engine state (#1657, ADR 0066 option C).
//!
//! Cluster variables used to be a node-local, host-injected handle read while
//! applying replicated commands: two replicas holding different sets took
//! different gateway branches (split-brain), a restart lost them, and replay
//! re-evaluated history against today's (empty) values. They are now engine
//! state changed only by replicated commands, so these tests pin the defect
//! class: same journal => same events on every replica; state survives
//! replay and snapshot restore (kinds included); the command semantics
//! (409 / 404, idempotent distributed copies) are deterministic.
use std::collections::BTreeMap;

use super::*;
use crate::cluster_vars::{ClusterVariableKind, DEFAULT_TENANT};

const XML: &str = r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="p">
      <bpmn:startEvent id="s" />
      <bpmn:exclusiveGateway id="gw" default="toOther" />
      <bpmn:serviceTask id="Emea">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="emea" />
          <zeebe:ioMapping>
            <zeebe:input source="=limit" target="limit" />
          </zeebe:ioMapping>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:serviceTask id="Other">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="other" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="gw" />
      <bpmn:sequenceFlow id="toEmea" sourceRef="gw" targetRef="Emea">
        <bpmn:conditionExpression>=region = "EMEA"</bpmn:conditionExpression>
      </bpmn:sequenceFlow>
      <bpmn:sequenceFlow id="toOther" sourceRef="gw" targetRef="Other" />
    </bpmn:process>
  </bpmn:definitions>"#;

const T0: u64 = 1_700_000_000_000;

fn create(tenant: Option<&str>, name: &str, value: Value, distributed: bool) -> Command {
    Command::CreateClusterVariable {
        tenant_id: tenant.map(str::to_string),
        name: name.to_string(),
        value,
        kind: ClusterVariableKind::Json,
        metadata: BTreeMap::new(),
        distributed,
    }
}

/// The replicated command journal: deploy, define cluster variables, create
/// an instance whose gateway + input mapping read them, then retarget one.
fn journal() -> Vec<Command> {
    let def = crate::bpmn::parse_bpmn(XML).unwrap().remove(0);
    vec![
        Command::DeployProcess(def),
        create(None, "region", Value::Str("EMEA".into()), false),
        create(Some(DEFAULT_TENANT), "limit", Value::Int(7), false),
        Command::create_instance("p"),
        Command::UpdateClusterVariable {
            tenant_id: None,
            name: "region".into(),
            value: Value::Str("APAC".into()),
            metadata: BTreeMap::from([("owner".to_string(), Value::Str("ops".into()))]),
        },
        Command::create_instance("p"),
    ]
}

/// Applies the journal on a fresh engine (what every Raft replica does),
/// returning the engine and every emitted event.
fn apply_journal() -> (Engine, Vec<Event>) {
    let mut engine = Engine::new();
    let mut all = Vec::new();
    for (i, cmd) in journal().into_iter().enumerate() {
        all.extend(engine.apply_command_at(cmd, T0 + i as u64).unwrap());
    }
    (engine, all)
}

fn job_types(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::JobCreated { job_type, .. } => Some(job_type.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn replicas_applying_the_same_journal_produce_the_same_events() {
    let (_, a) = apply_journal();
    let (_, b) = apply_journal();
    assert_eq!(a, b, "replicas diverged");
    // The cluster variables are actually read at their log position: the
    // first instance routes on EMEA, the second (after the update) does not.
    assert_eq!(job_types(&a), vec!["emea", "other"]);
}

#[test]
fn cluster_variables_are_read_at_the_log_position() {
    let (_, events) = apply_journal();
    // The first instance's input mapping resolved the tenant cluster variable
    // into the journaled event stream (the value is part of history, so a
    // replay can never re-read a different one).
    let journaled = format!("{events:?}");
    assert!(
        journaled.contains("\"limit\", Int(7)") || journaled.contains("\"limit\": Int(7)"),
        "{journaled}"
    );
}

#[test]
fn replay_rebuilds_cluster_variables_from_the_log() {
    let (engine, events) = apply_journal();
    let replayed = Engine::replay(events);
    assert_eq!(
        replayed.state.cluster_variables,
        engine.state.cluster_variables
    );
    let region = replayed.cluster_variables().get(None, "region").unwrap();
    assert_eq!(region.value, Value::Str("APAC".into()));
    assert_eq!(
        region.metadata.get("owner"),
        Some(&Value::Str("ops".into()))
    );
}

#[cfg(feature = "serde")]
#[test]
fn snapshot_and_tail_recovery_preserves_values_and_kinds() {
    let (mut engine, _) = apply_journal();
    engine
        .apply_command(Command::CreateClusterVariable {
            tenant_id: None,
            name: "apiCfg".into(),
            value: Value::Str("camunda.secrets.TOKEN".into()),
            kind: ClusterVariableKind::SecretReference,
            metadata: BTreeMap::new(),
            distributed: false,
        })
        .unwrap();
    let json = serde_json::to_string(&engine.snapshot_control_only()).unwrap();
    assert!(json.contains("SECRET_REFERENCE"), "{json}");
    let mut restored =
        Engine::from_snapshot(serde_json::from_str::<EngineSnapshot>(&json).unwrap());
    assert_eq!(
        restored.state.cluster_variables,
        engine.state.cluster_variables
    );
    // A tail command after restore observes the restored state identically.
    let tail = |e: &mut Engine| {
        e.apply_command_at(Command::create_instance("p"), T0 + 100)
            .unwrap()
    };
    assert_eq!(tail(&mut restored), tail(&mut engine));
}

#[test]
fn command_semantics_are_deterministic_rejections() {
    let mut engine = Engine::new();
    engine
        .apply_command(create(None, "x", Value::Int(1), false))
        .unwrap();
    assert_eq!(
        engine.apply_command(create(None, "x", Value::Int(2), false)),
        Err(EngineError::ClusterVariableAlreadyExists {
            tenant_id: None,
            name: "x".into()
        })
    );
    // Same name, different scope: no clash.
    engine
        .apply_command(create(Some("t"), "x", Value::Int(3), false))
        .unwrap();
    let missing = || EngineError::ClusterVariableNotFound {
        tenant_id: Some("t".into()),
        name: "y".into(),
    };
    assert_eq!(
        engine.apply_command(Command::UpdateClusterVariable {
            tenant_id: Some("t".into()),
            name: "y".into(),
            value: Value::Null,
            metadata: BTreeMap::new(),
        }),
        Err(missing())
    );
    assert_eq!(
        engine.apply_command(Command::DeleteClusterVariable {
            tenant_id: Some("t".into()),
            name: "y".into(),
            distributed: false,
        }),
        Err(missing())
    );
    for e in [
        EngineError::ClusterVariableAlreadyExists {
            tenant_id: None,
            name: "x".into(),
        },
        missing(),
    ] {
        assert!(!e.is_environmental());
    }
}

#[test]
fn distributed_copies_are_idempotent_upserts() {
    let mut engine = Engine::new();
    engine
        .apply_command(create(None, "x", Value::Int(1), true))
        .unwrap();
    let id = engine.cluster_variables().get(None, "x").unwrap().id;
    // Re-delivery of the same (or a later) distributed upsert keeps the id.
    engine
        .apply_command(create(None, "x", Value::Int(2), true))
        .unwrap();
    let x = engine.cluster_variables().get(None, "x").unwrap();
    assert_eq!((x.id, &x.value), (id, &Value::Int(2)));
    let delete = || Command::DeleteClusterVariable {
        tenant_id: None,
        name: "x".into(),
        distributed: true,
    };
    assert_eq!(engine.apply_command(delete()).unwrap().len(), 1);
    assert!(
        engine.apply_command(delete()).unwrap().is_empty(),
        "a re-delivered distributed delete is a no-op"
    );
}

/// The REST funnel builds its response (and the fan-out copy) from the
/// applying command's OWN events — never a re-read of live state, which a
/// concurrent mutation could change in between (PR #1670 review). Pin the
/// contract that makes that possible: a create/update emits exactly one
/// lifecycle event carrying the full post-mutation record, and a delete
/// emits exactly one carrying none.
#[test]
fn lifecycle_events_carry_the_full_post_mutation_record() {
    let mut engine = Engine::new();
    let events = engine
        .apply_command(create(None, "x", Value::Int(1), false))
        .unwrap();
    assert_eq!(events.len(), 1);
    let Event::ClusterVariableCreated {
        value,
        kind,
        metadata,
        ..
    } = &events[0]
    else {
        panic!("a create emits exactly its ClusterVariableCreated record");
    };
    assert_eq!(
        (value, kind, metadata.is_empty()),
        (&Value::Int(1), &ClusterVariableKind::Json, true)
    );

    let events = engine
        .apply_command(Command::UpdateClusterVariable {
            tenant_id: None,
            name: "x".into(),
            value: Value::Int(2),
            metadata: BTreeMap::from([("owner".to_string(), Value::Str("ops".into()))]),
        })
        .unwrap();
    assert_eq!(events.len(), 1);
    let Event::ClusterVariableUpdated {
        value,
        kind,
        metadata,
        ..
    } = &events[0]
    else {
        panic!("an update emits exactly its ClusterVariableUpdated record");
    };
    assert_eq!(value, &Value::Int(2));
    assert_eq!(metadata.get("owner"), Some(&Value::Str("ops".into())));
    assert_eq!(
        kind,
        &ClusterVariableKind::Json,
        "an update event carries the variable's preserved kind, captured in the committing turn"
    );

    let events = engine
        .apply_command(Command::DeleteClusterVariable {
            tenant_id: None,
            name: "x".into(),
            distributed: false,
        })
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], Event::ClusterVariableDeleted { .. }),
        "a delete emits exactly its ClusterVariableDeleted record"
    );
}

/// The update event's `kind` is the variable's PRESERVED kind (fixed at
/// creation), captured in the same turn that validates and commits the
/// update — never a placeholder. A SECRET_REFERENCE variable updated with a
/// new value must emit `kind: SecretReference`, so a gateway deriving the
/// response/fan-out record from the proposal's own events never mixes the
/// new value with a wrong (e.g. re-created replacement's) kind (PR #1670
/// review).
#[test]
fn update_event_carries_the_preserved_kind() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::CreateClusterVariable {
            tenant_id: None,
            name: "api_token".into(),
            value: Value::Str("camunda.secrets.TOKEN".into()),
            kind: ClusterVariableKind::SecretReference,
            metadata: BTreeMap::new(),
            distributed: false,
        })
        .unwrap();
    let events = engine
        .apply_command(Command::UpdateClusterVariable {
            tenant_id: None,
            name: "api_token".into(),
            value: Value::Str("camunda.secrets.ROTATED".into()),
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let Event::ClusterVariableUpdated { kind, value, .. } = &events[0] else {
        panic!("an update emits exactly its ClusterVariableUpdated record");
    };
    assert_eq!(kind, &ClusterVariableKind::SecretReference);
    assert_eq!(value, &Value::Str("camunda.secrets.ROTATED".into()));
    // The stored record keeps its kind and id across the update.
    let stored = engine
        .state()
        .cluster_variables
        .get(None, "api_token")
        .expect("stored");
    assert_eq!(stored.kind, ClusterVariableKind::SecretReference);
}

/// PR #1670 review: the peer-feature requirement derived from recovered state
/// must be one-way — once a partition applied a cluster-variable create, the
/// retained log/snapshot depends on `cluster-variables-v1` even after every
/// variable was deleted, and across snapshot restore.
#[test]
fn cluster_variables_ever_used_is_sticky_across_delete_and_snapshot() {
    let mut engine = Engine::new();
    assert!(!engine.cluster_variables_ever_used());
    engine
        .apply_command(create(None, "x", Value::Int(1), false))
        .unwrap();
    assert!(engine.cluster_variables_ever_used());
    engine
        .apply_command(Command::DeleteClusterVariable {
            tenant_id: None,
            name: "x".into(),
            distributed: false,
        })
        .unwrap();
    assert!(engine.state.cluster_variables.is_empty());
    assert!(engine.cluster_variables_ever_used(), "sticky after delete");
    let json = serde_json::to_string(&engine.snapshot_control_only()).unwrap();
    let restored = Engine::from_snapshot(serde_json::from_str::<EngineSnapshot>(&json).unwrap());
    assert!(
        restored.cluster_variables_ever_used(),
        "sticky across snapshot"
    );
}
