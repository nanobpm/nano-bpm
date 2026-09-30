//! The applier: the sole mutator of [`State`].
//!
//! [`apply`] mutates [`State`] purely as a function of an [`Event`]. Keeping
//! every mutation here is what makes the engine deterministic and replayable:
//! replaying the same events over a fresh [`State`] reconstructs it exactly.
//! This half of the `state` module may reference [`crate::event`]; the
//! data-model half ([`super::types`]) must not.
//!
//! [`apply`] itself is a thin dispatcher: it matches each [`Event`] to its
//! family and delegates to a per-family applier, one module per family
//! (`element`, `instance`, `job`, `user_task`, `timer`, `subscription`,
//! `process`, `resource`). Each `Event::Variant` arm lives verbatim in its
//! family's `apply_<family>` function; the dispatcher's match stays exhaustive
//! so a new variant cannot be silently dropped.

mod element;
mod instance;
mod job;
mod process;
mod resource;
mod subscription;
mod timer;
mod user_task;

use crate::event::Event;
use crate::state::types::State;

/// Applies a single [`Event`] to [`State`]. This is the sole mutator of engine
/// state; the processor never mutates [`State`] directly.
pub fn apply(state: &mut State, event: &Event) {
    match event {
        Event::DeploymentCreated { .. } | Event::ProcessDeployed { .. } => {
            process::apply_process(state, event)
        }
        Event::DecisionRequirementsDeployed { .. }
        | Event::DecisionDeployed { .. }
        | Event::FormDeployed { .. }
        | Event::GenericResourceDeployed { .. }
        | Event::DecisionEvaluated { .. }
        | Event::DecisionInstanceDeleted { .. } => resource::apply_resource(state, event),
        Event::ProcessInstanceCreated { .. }
        | Event::VariablesUpdated { .. }
        | Event::ScopedVariablesUpdated { .. }
        | Event::VariableScopeCreated { .. }
        | Event::VariableScopeDestroyed { .. }
        | Event::AgentInstanceCreated { .. }
        | Event::AgentInstanceUpdated { .. }
        | Event::AgentInstanceCompleted { .. }
        | Event::AgentHistoryCreated { .. }
        | Event::AgentHistoryCommitted { .. }
        | Event::AgentHistoryDiscarded { .. }
        | Event::AgentHistoryDeduplicated { .. }
        | Event::ProcessInstanceCompleted { .. }
        | Event::ProcessInstanceTerminating { .. }
        | Event::ProcessInstanceSuspended { .. }
        | Event::ProcessInstanceResumed { .. }
        | Event::ProcessInstanceBusinessIdAssigned { .. }
        | Event::ProcessInstanceMigrated { .. }
        | Event::ProcessInstanceTerminated { .. }
        | Event::StartInstanceDispatched { .. } => instance::apply_instance(state, event),
        Event::ElementActivating { .. }
        | Event::ElementCompleting { .. }
        | Event::ElementActivated { .. }
        | Event::ElementCompleted { .. }
        | Event::SequenceFlowTaken { .. }
        | Event::ParallelJoinOpened { .. }
        | Event::ParallelJoinTokenArrived { .. }
        | Event::ParallelJoinReset { .. }
        | Event::ParallelJoinFired { .. }
        | Event::CompensationSubscriptionCreated { .. }
        | Event::CompensationTriggered { .. }
        | Event::CompensationHandlerCompleted { .. }
        | Event::ScopedCompensationCleared { .. }
        | Event::MultiInstanceActivated { .. }
        | Event::MultiInstanceChildActivated { .. }
        | Event::MultiInstanceChildCompleted { .. }
        | Event::MultiInstanceCompleted { .. }
        | Event::AdHocActivated { .. }
        | Event::AdHocToolActivated { .. }
        | Event::AdHocToolCompleted { .. }
        | Event::AdHocCompletionConditionFulfilled { .. }
        | Event::AdHocIterated { .. }
        | Event::AdHocCompleted { .. } => element::apply_element(state, event),
        Event::JobCreated { .. }
        | Event::ExecutionListenerJobCreated { .. }
        | Event::TaskListenerJobCreated { .. }
        | Event::JobActivated { .. }
        | Event::JobLockExpired { .. }
        | Event::JobFailed { .. }
        | Event::JobErrorThrown { .. }
        | Event::JobCompleted { .. }
        | Event::IncidentRaised { .. }
        | Event::JobRetriesUpdated { .. }
        | Event::JobTimeoutUpdated { .. }
        | Event::IncidentResolved { .. }
        | Event::JobCanceled { .. } => job::apply_job(state, event),
        Event::UserTaskTransitionDeferred { .. }
        | Event::UserTaskCorrectionsApplied { .. }
        | Event::UserTaskTransitionResolved { .. }
        | Event::UserTaskCreated { .. }
        | Event::UserTaskAssigned { .. }
        | Event::UserTaskUpdated { .. }
        | Event::UserTaskCompleted { .. }
        | Event::UserTaskCanceled { .. } => user_task::apply_user_task(state, event),
        Event::TimerCreated { .. }
        | Event::TimerTriggered { .. }
        | Event::TimerCanceled { .. }
        | Event::ProcessStartTimerArmed { .. }
        | Event::ProcessStartTimerFired { .. } => timer::apply_timer(state, event),
        Event::MessagePublished { .. }
        | Event::MessageSubscriptionCreated { .. }
        | Event::MessageSubscriptionOpening { .. }
        | Event::MessageCorrelated { .. }
        | Event::MessageSubscriptionCanceled { .. }
        | Event::SignalSubscriptionCreated { .. }
        | Event::SignalCorrelated { .. }
        | Event::SignalSubscriptionCanceled { .. }
        | Event::ConditionalSubscriptionCreated { .. }
        | Event::ConditionalTriggered { .. }
        | Event::ConditionalSubscriptionCanceled { .. }
        | Event::SignalBroadcast { .. }
        | Event::MessageSubscriptionClosing { .. }
        | Event::RemoteMessageCorrelation { .. }
        | Event::MessageStartSubscriptionCreated { .. } => {
            subscription::apply_subscription(state, event)
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod incident_kind_serde_compat_tests {
    use crate::event::Event;
    use crate::IncidentKind;

    /// The `IoMappingOutput` taxonomy (#939) was collapsed into the single
    /// `IoMapping` kind (#946). JSON journals written by that intermediate code
    /// carry `"kind":"IoMappingOutput"`; without the `serde(alias)` they would
    /// fail to deserialize and prevent the server from booting on replay. Guard
    /// the whole defect class: the legacy name must still deserialize.
    #[test]
    fn legacy_io_mapping_output_kind_deserializes_to_io_mapping() {
        let kind: IncidentKind =
            serde_json::from_str("\"IoMappingOutput\"").expect("legacy kind deserializes");
        assert_eq!(kind, IncidentKind::IoMapping);
    }

    /// The alias is deserialize-only: we must never *emit* the retired name, so
    /// new journals stay on the canonical `IoMapping` taxonomy.
    #[test]
    fn io_mapping_kind_serializes_to_canonical_name() {
        let json = serde_json::to_string(&IncidentKind::IoMapping).expect("serializes");
        assert_eq!(json, "\"IoMapping\"");
    }

    /// A full legacy `IncidentRaised` journal line — retired `IoMappingOutput`
    /// kind and no `redrive` field — must replay, mapping to `IoMapping` with a
    /// defaulted `redrive: None`, so boot never fails on an intermediate-version
    /// journal.
    #[test]
    fn legacy_incident_raised_event_replays() {
        let line = r#"{"IncidentRaised":{"incident_key":7,"instance_key":1,"element_instance_key":2,"element_id":"task","kind":"IoMappingOutput","reason":"boom","job_key":null,"created_at":42}}"#;
        let event: Event = serde_json::from_str(line).expect("legacy event deserializes");
        match event {
            Event::IncidentRaised { kind, redrive, .. } => {
                assert_eq!(kind, IncidentKind::IoMapping);
                assert!(redrive.is_none());
            }
            other => panic!("expected IncidentRaised, got {other:?}"),
        }
    }
}

/// The ad-hoc call-activity redrive payloads (#1176) are **persisted** inside an
/// `IncidentRaised` journal line / snapshot, not just resolved in-memory — an
/// active ioMapping incident sits on the journal until the operator resolves it,
/// and migration-by-replay (#1071) rebuilds the engine by replaying that journal
/// under the current binary. So the single-pass input/output projections they
/// carry (`AdHocCallActivitySpawn.child_seed` /
/// `AdHocToolOutputCollection.precomputed_output`) must survive a serde
/// round-trip **verbatim**, or a recovered incident would respawn the tool with a
/// lost/garbled projection. These guard that replay-safety property directly.
#[cfg(all(test, feature = "serde"))]
mod adhoc_redrive_serde_tests {
    use std::collections::HashMap;

    use crate::event::Event;
    use crate::{IncidentKind, IoMappingRedrive, Value};

    fn projection() -> HashMap<String, Value> {
        // A mix of scalar + nested-container values so a structural
        // serialization/recovery regression (not just a dropped key) is caught.
        HashMap::from([
            ("x".to_string(), Value::Int(1)),
            (
                "chained".to_string(),
                Value::List(vec![Value::Str("z".to_string()), Value::Bool(true)]),
            ),
        ])
    }

    fn raised_with(redrive: IoMappingRedrive) -> Event {
        Event::IncidentRaised {
            incident_key: 7,
            instance_key: 1,
            element_instance_key: 2,
            element_id: "tool".to_string(),
            kind: IncidentKind::IoMapping,
            redrive: Some(redrive),
            reason: "boom".to_string(),
            job_key: None,
            created_at: 42,
        }
    }

    /// An active `AdHocCallActivitySpawn` incident must replay with its input
    /// projection (`child_seed`) preserved verbatim, so a post-recovery respawn
    /// reuses it instead of re-evaluating chained input mappings.
    #[test]
    fn adhoc_call_activity_spawn_projection_survives_replay() {
        let seed = projection();
        let event = raised_with(IoMappingRedrive::AdHocCallActivitySpawn {
            child_seed: seed.clone(),
        });
        let line = serde_json::to_string(&event).expect("serializes");
        let back: Event = serde_json::from_str(&line).expect("replays");
        match back {
            Event::IncidentRaised {
                redrive: Some(IoMappingRedrive::AdHocCallActivitySpawn { child_seed }),
                ..
            } => assert_eq!(child_seed, seed),
            other => panic!("expected AdHocCallActivitySpawn redrive, got {other:?}"),
        }
    }

    /// An active `AdHocToolOutputCollection` incident must replay with its output
    /// projection (`precomputed_output`) preserved verbatim, so a post-recovery
    /// redrive reuses it instead of re-projecting chained output mappings.
    #[test]
    fn adhoc_tool_output_collection_projection_survives_replay() {
        let out = projection();
        let event = raised_with(IoMappingRedrive::AdHocToolOutputCollection {
            precomputed_output: out.clone(),
        });
        let line = serde_json::to_string(&event).expect("serializes");
        let back: Event = serde_json::from_str(&line).expect("replays");
        match back {
            Event::IncidentRaised {
                redrive: Some(IoMappingRedrive::AdHocToolOutputCollection { precomputed_output }),
                ..
            } => assert_eq!(precomputed_output, out),
            other => panic!("expected AdHocToolOutputCollection redrive, got {other:?}"),
        }
    }
}
