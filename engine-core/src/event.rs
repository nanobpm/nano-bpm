//! Events: immutable facts about what happened.
//!
//! Events are the engine's source of truth. They are produced by the processor
//! and consumed by [`crate::state::apply`] (the sole mutator). Because each event
//! carries enough data to rebuild state, the event stream is a replayable log —
//! persist it however you like and replay to recover.

use std::collections::BTreeMap;

use crate::model::{ElementId, IncomingFlow, ProcessDefinition, Value};
use crate::state::types::{
    IncidentKind, IoMappingRedrive, Key, MessageSubscriptionKind, TimerKind,
};

/// `skip_serializing_if` predicate for an optional-by-zero [`Key`] field: a
/// zero key is the legacy/absent sentinel (additive fields default to it), so
/// it is omitted from the serialized frame.
#[cfg(feature = "serde")]
fn is_zero_key(k: &Key) -> bool {
    *k == 0
}

/// One activation instruction of a [`crate::command::Command::ModifyInstance`]: place a new
/// token at `element_id`, first merging `variables` into the instance's root
/// scope. Mirrors Zeebe's `ProcessInstanceModificationActivateInstruction`:
/// `ancestor_element_instance_key` selects the flow-scope instance to activate
/// in and `variable_instructions` set variables on a named scope.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActivateElementInstruction {
    /// The BPMN element id to activate a token at.
    pub element_id: String,
    /// Variables merged into the instance's root scope before the token is
    /// placed (Zeebe's global variable instructions). Empty for none.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables: BTreeMap<String, Value>,
    /// The ancestor element instance (an existing flow-scope instance) to
    /// activate the element under (Zeebe `ancestorElementInstanceKey`). `None`
    /// (Zeebe's `-1`) = use the single existing instance of the flow scope, or
    /// create it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub ancestor_element_instance_key: Option<Key>,
    /// Variable instructions, each optionally targeting a flow scope by element
    /// id (Zeebe `variableInstructions[].scopeId`). Empty for none.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variable_instructions: Vec<VariableInstruction>,
}

/// A variable instruction of a modification (Zeebe
/// `ProcessInstanceModificationVariableInstruction`): set `variables` on the
/// flow scope identified by `scope_id` — an element id of the activated
/// element or one of its flow scopes — or on the process root scope when
/// `scope_id` is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VariableInstruction {
    /// The variables to set.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables: BTreeMap<String, Value>,
    /// The element id of the scope to set them on; `None` = process root.
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope_id: Option<ElementId>,
}

/// Which element instance(s) a [`MoveInstruction`] moves (Camunda 8.9
/// `sourceElementInstruction`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MoveSource {
    /// Every active instance of this element id (`sourceType: byId`).
    ById(ElementId),
    /// Exactly this element instance (`sourceType: byKey`).
    ByKey(Key),
}

impl Default for MoveSource {
    fn default() -> Self {
        MoveSource::ById(String::new())
    }
}

/// How a [`MoveInstruction`] picks the flow-scope instance its target is
/// activated in (Camunda 8.9 `ancestorScopeInstruction`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AncestorScopeInstruction {
    /// Infer the ancestor from the source instance's hierarchy
    /// (`ancestorScopeType: inferred`; Zeebe
    /// `inferAncestorScopeFromSourceHierarchy`). The default.
    #[default]
    Inferred,
    /// Use this explicit ancestor element instance (`ancestorScopeType:
    /// direct`; Zeebe `ancestorScopeKey`).
    Direct(Key),
    /// Use the source instance's direct flow-scope instance
    /// (`ancestorScopeType: sourceParent`; Zeebe
    /// `useSourceParentKeyAsAncestorScopeKey`).
    SourceParent,
}

/// One move instruction of a [`crate::command::Command::ModifyInstance`] (Camunda 8.9
/// `ProcessInstanceModificationMoveInstruction`): terminate the source element
/// instance(s) and activate `target_element_id` in their place.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MoveInstruction {
    /// The element instance(s) to move.
    pub source: MoveSource,
    /// The element to activate in place of each moved source instance.
    pub target_element_id: ElementId,
    /// How the target's flow-scope instance is chosen.
    #[cfg_attr(feature = "serde", serde(default))]
    pub ancestor_scope: AncestorScopeInstruction,
    /// Variable instructions applied for each activated target.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variable_instructions: Vec<VariableInstruction>,
}

impl ActivateElementInstruction {
    /// Activate `element_id` in the process root scope with no variables.
    pub fn new(element_id: impl Into<String>) -> Self {
        Self {
            element_id: element_id.into(),
            ..Self::default()
        }
    }
}

/// A flow-scope element instance re-pointed by a migration (#1374).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MigratedElementInstance {
    pub element_instance_key: Key,
    pub source_element_id: ElementId,
    pub target_element_id: ElementId,
}

/// What a migration did with a catch-event / boundary subscription (#1374).
/// Zeebe retains a subscription whose trigger is unchanged and re-subscribes
/// when the message name, correlation key or timer changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SubscriptionMigrationAction {
    /// Kept as is, re-pointed at the target element.
    #[default]
    Retained,
    /// Closed and opened again because its trigger changed.
    Recreated,
    /// Opened for a catch event that only exists in the target.
    Created,
    /// Closed because its catch event has no counterpart in the target.
    Deleted,
}

/// The trigger of a migrated subscription (#1374).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MigratedSubscriptionTrigger {
    Message {
        subscription_key: Key,
        message_name: String,
        correlation_key: String,
    },
    Timer {
        timer_key: Key,
        due_at: u64,
    },
    Signal {
        subscription_key: Key,
        signal_name: String,
    },
}

/// A catch-event / boundary subscription touched by a migration (#1374).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MigratedSubscription {
    /// The element instance owning the subscription (the catch event itself,
    /// or the activity a boundary event is attached to).
    pub element_instance_key: Key,
    /// The target catch-event element id the subscription now belongs to.
    pub element_id: ElementId,
    pub action: SubscriptionMigrationAction,
    pub trigger: MigratedSubscriptionTrigger,
}

/// A call-activity child instance kept attached across a migration (#1374).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MigratedChildInstance {
    pub call_activity_element_instance_key: Key,
    pub child_instance_key: Key,
    pub target_element_id: ElementId,
}

/// A completed compensable activity's compensation subscription re-pointed by
/// a migration (#1631, Zeebe `CompensationSubscriptionIntent.MIGRATED`): every
/// subscription recorded for `source_element_id` now names
/// `target_element_id` and runs `target_handler` when compensation is thrown.
/// Scope subscriptions (#901, completed sub-processes) carry an empty handler.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MigratedCompensationSubscription {
    pub source_element_id: ElementId,
    /// The migrated subscription's element instance key, so each plan entry
    /// names one subscription occurrence (a task with several compensation
    /// boundaries holds one subscription per boundary).
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_element_instance_key: Key,
    /// The migrated subscription's source handler (empty for a scope
    /// subscription), identifying which boundary's subscription this is.
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_handler: ElementId,
    /// The migrated subscription's source compensation boundary (empty for a
    /// scope subscription, or a pre-#1635 subscription recorded without one).
    #[cfg_attr(feature = "serde", serde(default))]
    pub source_boundary_id: ElementId,
    pub target_element_id: ElementId,
    pub target_handler: ElementId,
    /// The mapped target compensation boundary the subscription now belongs
    /// to (empty for a scope subscription), persisted so successive
    /// migrations and throws keep the boundary identity.
    #[cfg_attr(feature = "serde", serde(default))]
    pub target_boundary_id: ElementId,
}

/// The message that triggered a **message start event** (#1300), carried on a
/// cross-partition [`Event::StartInstanceDispatched`] /
/// [`crate::Command::DispatchStartInstance`] so the partition that mints the
/// instance can record the [`Event::MessageStartEventCorrelated`] (it is the
/// only partition that knows the new instance key).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MessageStartCorrelation {
    pub message_key: Key,
    /// The message start subscription (keyed by its process definition).
    pub process_definition_key: Key,
    pub message_name: String,
    pub correlation_key: String,
}

/// A fact emitted by the engine. The ordering of a command's returned events is
/// the order in which they occurred.
///
/// # Adding or changing an event (event-frame replay-compatibility)
///
/// This enum is a **persisted, replayed on-disk shape**: migration-by-replay
/// (#1071) rebuilds the engine by replaying an old journal under new code, so
/// every historical record must still deserialize under the current binary
/// (incident #1065). The serde derives are feature-gated
/// (`cfg_attr(feature = "serde", …)`); everything replay-related is built/tested
/// with `--features serde`. Two rules keep the frame replay-compatible — the
/// #1069 CI drift guard (`engine-core/tests/golden_serde_drift.rs`) enforces
/// them, failing the build on any un-versioned shape change:
///
/// * **Additive change (safe, no version bump).** Adding a new field to an
///   existing variant is forward-compatible *only if* it carries
///   `#[serde(default)]` — an older record without the field then decodes with
///   the default (`None`/`0`). This crate already relies on that contract
///   pervasively; a new field without `#[serde(default)]` breaks replay of every
///   older record of that variant. Adding a brand-new variant is likewise
///   additive (old journals simply never contain it). After an additive change,
///   refresh the golden corpus:
///   `UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift`.
/// * **Breaking change (requires a version bump + migrator handling).** Renaming
///   or removing a variant, retagging it, changing a field's type, or reordering
///   in a way that changes the serialized form is NOT rescued by serde defaults.
///   It requires bumping [`SNAPSHOT_FORMAT_VERSION`](crate::SNAPSHOT_FORMAT_VERSION)
///   (#1068) — which the loader surfaces as a typed format mismatch and #1071's
///   replay-migrator branches on — and the migrator must handle the old→new
///   transition. A **removed/renamed** variant also means old journals may carry
///   a variant this build no longer knows; replay rejects it explicitly as
///   [`EventDecodeError::UnknownVariant`] (via [`decode_event_json`]) rather than
///   dropping it silently.
///
/// See `AGENTS.md` ("Adding or Changing an Event") for the full checklist.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Event {
    /// A `POST /v2/deployments` command was accepted. Emitted **once per deploy
    /// call**, unconditionally, before any [`Event::ProcessDeployed`] events
    /// that may follow — mirroring Zeebe's `DeploymentIntent.CREATED` (see
    /// `DeploymentCreateProcessor.java:224`). The key identifies the API call
    /// (not the artefact), is what the client sees in the response envelope,
    /// and is the handle used by audit-log filters. The applier is a strict
    /// no-op today; state persistence for later `GET /deployments/{key}`
    /// retrieval is tracked separately (issue #47, Option A).
    ///
    /// Emitting on every call — even a pure-duplicate deploy that mints no
    /// [`Event::ProcessDeployed`] — is what keeps the spec's
    /// `DeploymentKey: LongKey` (pattern `^-?[0-9]+$`) contract satisfied. An
    /// empty deploymentKey trips `Long.parseLong` in every stock C8 client.
    DeploymentCreated { deployment_key: Key },

    /// A process definition was registered as part of a deployment. The engine
    /// assigns the `deployment_key` (shared by every resource in the same
    /// deployment), a unique `process_definition_key`, and a `version` that
    /// increments per process id across deployments.
    ProcessDeployed {
        deployment_key: Key,
        process_definition_key: Key,
        version: i32,
        process: ProcessDefinition,
    },

    /// A decision requirements graph (DRG, one parsed `.dmn` resource) was
    /// registered as part of a deployment. Carries the parsed graph so the engine
    /// can evaluate its decisions natively (`businessRuleTask`, EvaluateDecision).
    DecisionRequirementsDeployed {
        deployment_key: Key,
        decision_requirements_key: Key,
        version: i32,
        drg: crate::dmn::DecisionRequirementsGraph,
    },

    /// A single decision inside a deployed DRG was registered, indexed for lookup
    /// by id. One [`Event::DecisionRequirementsDeployed`] emits one of these per
    /// decision it contains.
    DecisionDeployed {
        deployment_key: Key,
        decision_requirements_key: Key,
        decision_key: Key,
        decision_id: String,
        decision_name: String,
        version: i32,
    },

    /// A form (`form-js` `.form` JSON) was registered as part of a deployment.
    /// The engine assigns a unique `form_key` and a `version` that increments per
    /// `form_id` across deployments. The engine does not execute forms; it stores
    /// them so they can be served by `GetFormByKey` (Zeebe parity).
    FormDeployed {
        deployment_key: Key,
        form_key: Key,
        version: i32,
        /// The user-provided form identifier (the form-js document's `id`).
        form_id: String,
        /// The deploy resource name (e.g. `greeting.form`).
        resource_name: String,
        /// The verbatim form-js JSON document.
        schema: String,
        /// The optional version tag this form version carries (form-js
        /// `versionTag`), matched by a `versionTag`-bound user-task form
        /// (#1193). Additive: `serde(default)` so journals written before the
        /// field existed decode with `None` (no tag).
        #[cfg_attr(feature = "serde", serde(default))]
        version_tag: Option<String>,
    },

    /// A generic resource (any deployed file that is not a BPMN/DMN/form — e.g. a
    /// Markdown agent prompt) was registered as part of a deployment. The engine
    /// assigns a unique `resource_key` and a `version` that increments per
    /// `resource_id` across deployments. The engine does not execute generic
    /// resources; it stores them so they can be served by `GetResourceByKey` and
    /// searched by `resourceId` (Zeebe parity).
    GenericResourceDeployed {
        deployment_key: Key,
        resource_key: Key,
        version: i32,
        /// The resource identifier (its filename, for a generic resource).
        resource_id: String,
        /// The deploy resource name (the filename).
        resource_name: String,
        /// The verbatim resource content.
        content: String,
        /// The optional version tag this resource version carries (Zeebe
        /// `versionTag` deploy metadata), matched by a `versionTag`-bound
        /// `zeebe:linkedResource` (#1193). Additive: `serde(default)` so
        /// journals written before the field existed decode with `None`.
        #[cfg_attr(feature = "serde", serde(default))]
        version_tag: Option<String>,
    },

    /// A decision was evaluated — by a `businessRuleTask` (with `instance_key` /
    /// `element_id` set) or by the standalone EvaluateDecision API (both `0` /
    /// empty — Zeebe's `-1`). Carries the root output and the per-decision audit
    /// trail for exporter parity with Zeebe's decision-evaluation records; a
    /// failed evaluation is recorded too (`failure` set).
    DecisionEvaluated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        decision_key: Key,
        decision_id: String,
        decision_output: Value,
        evaluated_decisions: Vec<crate::dmn::EvaluatedDecision>,
        /// Logical instant the decision was evaluated (the engine's clock
        /// reading for the command). Defaulted to `0` so journals written before
        /// this field existed still replay.
        #[cfg_attr(feature = "serde", serde(default))]
        evaluated_at: u64,
        /// The evaluation's own key, minted per evaluation from the partition
        /// key generator (Zeebe's `decisionEvaluationKey`, issue #1292). `0` in
        /// journals written before this field existed: read it through
        /// [`Event::decision_evaluation_key`], which derives a stable
        /// per-evaluation key for those legacy records.
        #[cfg_attr(feature = "serde", serde(default))]
        decision_evaluation_key: Key,
        /// Why the evaluation failed, if it did (`None` = EVALUATED; Zeebe's
        /// `DecisionEvaluation:FAILED`, whose audit trail ends with the failed
        /// decision). Defaulted so older journals replay.
        #[cfg_attr(feature = "serde", serde(default))]
        failure: Option<crate::dmn::EvaluationFailure>,
        /// Key and id of the decision requirements graph (DRG) version that was
        /// evaluated — every evaluated decision belongs to it (Zeebe's
        /// `decisionRequirementsKey`/`Id`). Carried on the event because the
        /// read model retains only the latest version of a DRG. `0` / empty in
        /// journals written before #1292.
        #[cfg_attr(feature = "serde", serde(default))]
        decision_requirements_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        decision_requirements_id: String,
    },

    /// A decision instance (all rows of one [`Event::DecisionEvaluated`], sharing
    /// its [`Event::decision_evaluation_key`]) was marked for deletion via the
    /// DeleteDecisionInstance management API. `instance_key` is the owning
    /// process instance (`0` for a standalone evaluation), carried so this event
    /// is journaled and projected on the same partition/shard as the evaluation
    /// it retracts. Journals written before #1292 carry the evaluation's *root
    /// decision definition key* here (the old row key) — the read model honours
    /// that legacy meaning. Audit/projection-only: no core engine state to mutate.
    DecisionInstanceDeleted {
        instance_key: Key,
        decision_evaluation_key: Key,
    },

    /// An ENDED process instance's history was deleted via the
    /// `DeleteProcessInstanceHistory` management API (#1948). Journaled on the
    /// instance's own partition, so the read-model shard that projected the
    /// instance removes it — with every descendant (call-activity child)
    /// instance and all their instance-scoped rows.
    ///
    /// Event-sourced deletion: the event carries only the root key, because
    /// which ended descendants are still resident is replica-local (eviction,
    /// cold spill) and a journaled set would differ between replicas. Each
    /// consumer resolves the tree from its own state: engine apply REMOVES the
    /// root and every resident call-activity descendant (so snapshots never
    /// carry them and a reseed cannot resurrect them); the read model walks
    /// its parent edges across the live store and the archive.
    ProcessInstanceHistoryDeleted { instance_key: Key },

    /// A cluster variable was created (#1657; Zeebe `ClusterVariableIntent.CREATED`)
    /// — or, for a distributed fan-out copy, upserted (an existing variable keeps
    /// its id). `tenant_id` `None` is the global scope. Partition-agnostic
    /// configuration state: carries no key; a shared journal tags it with the
    /// partition that wrote it.
    ClusterVariableCreated {
        tenant_id: Option<String>,
        name: String,
        value: Value,
        #[cfg_attr(feature = "serde", serde(default))]
        kind: crate::cluster_vars::ClusterVariableKind,
        #[cfg_attr(feature = "serde", serde(default))]
        metadata: std::collections::BTreeMap<String, Value>,
    },

    /// A cluster variable's value and metadata were replaced (#1657; Zeebe
    /// `ClusterVariableIntent.UPDATED`). Its id and kind are preserved.
    ///
    /// `kind` is the variable's preserved kind, captured by the applying
    /// engine in the same turn that commits the update. Carrying it on the
    /// event lets a gateway derive the response/fan-out record entirely from
    /// the proposal's own events — a live-state re-read runs in a separate
    /// actor turn, so a concurrent delete/recreate could otherwise combine
    /// the new value with a replacement's kind (PR #1670 review). Additive
    /// (`serde(default)`): records written before this field decode with the
    /// `Json` default, which only the (unused) response projection ever read.
    ClusterVariableUpdated {
        tenant_id: Option<String>,
        name: String,
        value: Value,
        #[cfg_attr(feature = "serde", serde(default))]
        metadata: std::collections::BTreeMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        kind: crate::cluster_vars::ClusterVariableKind,
    },

    /// A cluster variable was deleted (#1657; Zeebe `ClusterVariableIntent.DELETED`).
    ClusterVariableDeleted {
        tenant_id: Option<String>,
        name: String,
    },

    /// A new process instance was created (carries a single token at its start
    /// event) with its initial variables. `created_at` is the logical instant
    /// the instance was started, carried on the command (the engine never reads
    /// a wall clock); it is the instance's start date. Defaulted to `0` so
    /// journals written before this field existed still replay. `tags` and
    /// `business_id` are user-supplied metadata, defaulted for older journals.
    ProcessInstanceCreated {
        instance_key: Key,
        process_id: String,
        variables: BTreeMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(feature = "serde", serde(default))]
        tags: Vec<String>,
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
        /// The unique key of the process **definition version** this instance was
        /// created on. Pins the instance to that version for its whole life
        /// (Zeebe parity). `0` (the `serde(default)`) for events written before
        /// version pinning; the applier then resolves the latest version by id.
        #[cfg_attr(feature = "serde", serde(default))]
        process_definition_key: Key,
        /// The version number of that definition, surfaced by the read model.
        /// `0` (the `serde(default)`) for events written before version pinning.
        #[cfg_attr(feature = "serde", serde(default))]
        version: i32,
        /// When this instance is a **child process instance** spawned by a call
        /// activity, the `processInstanceKey` of the calling (parent) instance;
        /// `None` for a top-level instance (created via the API, a message/timer
        /// start, or a signal). Populated so tooling can draw the parent↔child
        /// process tree (C8 `parentProcessInstanceKey`). `None` (the
        /// `serde(default)`) for events written before native call activities.
        #[cfg_attr(feature = "serde", serde(default))]
        parent_process_instance_key: Option<Key>,
        /// When this instance is a child spawned by a call activity, the element
        /// instance key of the call-activity element in the parent instance that
        /// spawned it (C8 `parentElementInstanceKey`); `None` for a top-level
        /// instance. `None` (the `serde(default)`) for pre-native-call events.
        #[cfg_attr(feature = "serde", serde(default))]
        parent_element_instance_key: Option<Key>,
    },

    /// Variables were merged into a process instance.
    VariablesUpdated {
        instance_key: Key,
        variables: BTreeMap<String, Value>,
    },

    /// Variables were merged into a specific variable scope (Part C hierarchical
    /// scoping). `scope_key` names the scope-owning element instance the values
    /// land in; when it equals `instance_key` (or `0`) they land in the root
    /// scope, matching [`Event::VariablesUpdated`]. The engine resolves Zeebe
    /// variable propagation *before* emitting, so each event targets exactly one
    /// scope and the applier is a plain merge.
    ScopedVariablesUpdated {
        instance_key: Key,
        scope_key: Key,
        variables: BTreeMap<String, Value>,
    },
    /// A non-root variable scope was opened (Part C). Registers `scope_key`
    /// (a scope-owning element instance: sub-process, multi-instance body or
    /// child) with its `parent_scope_key` in the instance's scope tree, so reads
    /// resolve upward and local variables can be held against it.
    VariableScopeCreated {
        instance_key: Key,
        scope_key: Key,
        parent_scope_key: Key,
    },
    /// A non-root variable scope was closed (Part C): its local variables are
    /// dropped and its tree entry removed. Emitted as the owning element instance
    /// completes or is terminated.
    VariableScopeDestroyed { instance_key: Key, scope_key: Key },
    /// A correlated message / signal payload is held for an event
    /// sub-process whose start event carries output mappings (#1555, Zeebe
    /// parity): Zeebe creates such a payload LOCAL to the start event
    /// instance, so it is parked against the event sub-process container
    /// instance `scope_key` until that container's start event activates
    /// (possibly in a later command, e.g. after a container `start`
    /// execution listener completes). Journaled so the hold survives replay;
    /// dropped with the container's scope (`VariableScopeDestroyed`).
    EventSubProcessPayloadHeld {
        instance_key: Key,
        scope_key: Key,
        variables: BTreeMap<String, Value>,
    },

    /// An element instance entered `ACTIVATING`. It registers the element
    /// instance as a live token (so a token parked in `ACTIVATING` on an
    /// activation-time incident is still owned by its instance, Zeebe parity:
    /// `JobWorkerTaskProcessor.onActivate` raises the incident before
    /// `transitionToActivated`). `ElementActivated` follows once activation
    /// succeeds.
    ElementActivating {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        /// The enclosing embedded sub-process element instance, `0` for the
        /// root scope — as on [`Event::ElementActivated`]. `serde(default)`:
        /// journals written before it existed always carry the matching
        /// `ElementActivated`, which records the scope.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Key,
        /// LEGACY (#1676, retired by #2004): the event-based gateway element
        /// instance whose deferred choice armed this (catch) element instance.
        /// This build never emits it (`None`); it is kept so old journals
        /// decode, and a replayed `Some` lands in `ProcessInstance::races`,
        /// which the loader refuses as an in-flight legacy race.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        race: Option<Key>,
    },
    /// An element instance reached `ACTIVATED`.
    ElementActivated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        /// The element instance of the enclosing embedded sub-process this
        /// instance lives in, or `0` for the process-level (root) scope. Used to
        /// track token scopes for sub-process completion and interruption.
        /// Defaulted to `0` so journals written before sub-processes existed
        /// still replay.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Key,
    },
    /// An element instance entered `COMPLETING`.
    ElementCompleting {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An element instance reached `COMPLETED`.
    ElementCompleted {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An element instance entered `TERMINATING`: it is being torn down
    /// without completing — interrupted by an interrupting boundary event or
    /// event sub-process (#1535, Zeebe parity). Unlike completion, termination
    /// takes no outgoing flow and opens no compensation subscription.
    ElementTerminating {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An element instance reached `TERMINATED`. Clears the token exactly as
    /// [`Event::ElementCompleted`] does (one shared reducer).
    ElementTerminated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },

    /// A split gateway (exclusive, or an inclusive fork) selected its outgoing
    /// flow(s) while ACTIVATING (Zeebe `onActivate`, #1402). The selection is
    /// journaled so completion takes exactly these flows instead of
    /// re-evaluating the conditions: FEEL expressions are not required to be
    /// deterministic (and a listener may rewrite a condition variable), so a
    /// completion-time re-evaluation could pick a different route — or raise a
    /// routing incident after `ElementActivated`. Cleared when the gateway
    /// completes. Indices are into the element's outgoing-flow list (document
    /// order), exactly as `Engine::take_flow` consumes them.
    GatewayRouteSelected {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        /// The selected outgoing-flow indices (exactly one for an exclusive
        /// gateway, one or more for an inclusive fork).
        flows: Vec<usize>,
    },

    /// An event-based gateway opened its gateway-owned subscriptions (#1963
    /// slice i, #2003): one per outgoing catch target, owned by the gateway's
    /// element instance and tagged with the target catch element id (Zeebe
    /// `EventBasedGatewayProcessor.onActivate`).
    EventGatewaySubscriptionsOpened {
        instance_key: Key,
        /// The owning event-based gateway's element instance key.
        gateway_element_instance_key: Key,
        gateway_element_id: ElementId,
        /// The target catch element ids, in outgoing-flow (document) order.
        catch_element_ids: Vec<ElementId>,
    },

    /// A gateway-owned subscription fired (#1963, #2004): the gateway
    /// recorded its event trigger for `catch_element_id` and every other
    /// subscription it owns is deleted (Zeebe `EventTrigger` on the gateway's
    /// event scope; first trigger in log order wins, arbitrated per batch;
    /// losing catches are cancelled with no trigger record). The gateway then
    /// completes and activates the catch directly under
    /// `catch_element_instance_key`. Replay contract (legacy before the
    /// `event-gateway-runtime-v1` latch, new runtime after):
    /// `docs/adr/0067-event-gateway-runtime-latch.md`.
    EventGatewayTriggered {
        instance_key: Key,
        gateway_element_instance_key: Key,
        /// The catch element whose event fired.
        catch_element_id: ElementId,
        /// The element instance key minted for the triggered catch's direct
        /// activation. Additive (`serde(default)`; `0` on a record written
        /// before #2004, which no build ever emitted).
        #[cfg_attr(feature = "serde", serde(default))]
        catch_element_instance_key: Key,
        /// The winning event's payload (#2047 review), snapshotted at the
        /// trigger and written LOCAL to the catch element instance when it
        /// activates, so its listeners, output mappings and propagation read
        /// it even when a variable changes while the gateway completes (e.g.
        /// on an `end` listener). Additive and written only when non-empty.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "BTreeMap::is_empty")
        )]
        variables: BTreeMap<String, Value>,
    },

    /// A token moved along a sequence flow from one element to another.
    SequenceFlowTaken {
        instance_key: Key,
        from: ElementId,
        to: ElementId,
    },

    /// A parallel- or inclusive-gateway join was opened on the first arriving
    /// token that did not make it ready; later tokens accumulate against it.
    ///
    /// Since #1244 the join is **parked** (`scope` is `Some`): its key holds the
    /// waiting tokens in its flow scope without activating, so no
    /// `ElementActivating`/`ElementActivated` precedes the join's readiness,
    /// as in Zeebe, where a not-ready join only rejects the `ACTIVATE_ELEMENT`
    /// command. A journal written before #1244 has `scope: None` and activated
    /// the join visibly right before this event.
    ParallelJoinOpened {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        /// The flow scope the parked join waits in (`0` = process root);
        /// `None` for a join opened (and activated) before #1244.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Option<Key>,
    },
    /// A token arrived at an open join. `flow` is the incoming sequence flow it
    /// arrived over, so a parallel join can require every incoming flow (not just
    /// enough arrivals) before it fires (#1233), and a firing join can consume one
    /// token per flow. It is `None` for journals written before #1233 (and, for
    /// inclusive joins, before #1237) and for an activation that did not come
    /// over a flow.
    ParallelJoinTokenArrived {
        instance_key: Key,
        element_id: ElementId,
        /// The flow-scope instance whose join this is (`0` = process root,
        /// #1580): joins count per flow-scope instance. `None` for a journal
        /// written before #1580, when joins were keyed by element id alone;
        /// the applier then addresses the element's single join slot.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Option<Key>,
        #[cfg_attr(feature = "serde", serde(default))]
        flow: Option<IncomingFlow>,
    },
    /// A join's bookkeeping was cleared outright: a join fired under a build
    /// older than #1233 (parallel) / #1237 (inclusive), or the scope holding the
    /// join was torn down.
    ParallelJoinReset {
        instance_key: Key,
        element_id: ElementId,
        /// The flow-scope instance whose join this is (`0` = process root,
        /// #1580): joins count per flow-scope instance. `None` for a journal
        /// written before #1580, when joins were keyed by element id alone;
        /// the applier then addresses the element's single join slot.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Option<Key>,
    },
    /// A join fired: a parallel join because every incoming flow has been taken,
    /// an inclusive join because every incoming flow has been taken or no active
    /// path can still reach it over an untaken flow (#1241). It consumes
    /// ONE arrival per incoming flow and keeps any surplus for the next
    /// activation (Zeebe's "Tetris principle",
    /// `ProcessInstanceElementActivatingV3Applier`, #1233, #1237).
    ParallelJoinFired {
        instance_key: Key,
        element_id: ElementId,
        /// The flow-scope instance whose join this is (`0` = process root,
        /// #1580): joins count per flow-scope instance. `None` for a journal
        /// written before #1580, when joins were keyed by element id alone;
        /// the applier then addresses the element's single join slot.
        #[cfg_attr(feature = "serde", serde(default))]
        scope: Option<Key>,
    },

    /// A job was created for a service task; the token now rests until the job
    /// is completed. `created_at` is the logical instant it was created (carried
    /// on the event so replay reconstructs the same timestamp). `priority` is the
    /// *resolved* job-activation priority (FEEL evaluated against the instance
    /// variables, or a literal; default 50) — higher priority is activated first.
    JobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        job_type: String,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::types::default_job_priority")
        )]
        priority: i32,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::types::default_job_retries")
        )]
        retries: i32,
        /// Mapping-time secret-reference provenance (#1640): the input-mapping
        /// target paths holding trusted `camunda.secrets.<name>` references,
        /// captured when the mappings ran (never re-derived later). Paths and
        /// secret NAMES only — never plaintext. Additive: a pre-#1640 record
        /// decodes to empty provenance, which resolves nothing (fail closed).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        secret_provenance: Vec<crate::state::SecretProvenance>,
    },
    /// A job was created for one execution listener in an element's sequential
    /// listener chain (ADR 0037). Distinct from [`Event::JobCreated`] so that
    /// listener-free models never produce it and their log stays byte-identical.
    /// `event_type` is the transition it fires on, `listener_index` its 0-based
    /// position in the element's listener list for that transition, and `scope`
    /// the element's enclosing scope (carried so completing the job can drive the
    /// next listener or resume the lifecycle transition). The token rests until
    /// the job completes.
    ExecutionListenerJobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        job_type: String,
        event_type: crate::model::ListenerEventType,
        listener_index: usize,
        scope: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::types::default_job_retries")
        )]
        retries: i32,
    },
    /// A job was activated by a worker and locked until `deadline` (a logical
    /// instant supplied by the caller). Another worker cannot activate it until
    /// the lock expires, but any holder of the key may complete it.
    JobActivated {
        job_key: Key,
        instance_key: Key,
        /// An authoritative activation must never become a leader-local lock,
        /// including when no lease token was requested.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "std::ops::Not::not")
        )]
        durable: bool,
        worker: String,
        deadline: u64,
        /// The logical instant the lock was acquired (the activating command's
        /// `now`). `None` for events serialized before this field existed, so a
        /// replayed historical activation yields no (bogus epoch-0) timestamp.
        #[cfg_attr(feature = "serde", serde(default))]
        activated_at: Option<u64>,
        /// The **declared read-set**: the `fetchVariables` names the worker asked
        /// for on this activation (Zeebe `ACTIVATED`-record parity — the variable
        /// set the worker was handed). This is the engine-native provenance signal
        /// for post-hoc reification / data-dependency analysis: reconstructing
        /// "which step read a variable a prior step wrote" from the log alone,
        /// with no application instrumentation (values are recoverable by folding
        /// `ScopedVariablesUpdated`/`VariablesUpdated` writes, so the lean names
        /// alone reconstruct the DAG at minimal log cost).
        ///
        /// **Empty** when the activation declared no `fetchVariables` (fetch-all):
        /// the read-set is then "all in-scope / undeclared", which a reader must
        /// treat as *unknown reads* rather than "reads everything". Serialized
        /// only when non-empty (`skip_serializing_if`), so declaration-free
        /// activations stay byte-identical in the journal.
        ///
        /// This is the enabler for engine-native reification: it lets the reifier
        /// in `camunda/web-demo-framework` (PR #101) move off its sandbox
        /// read-set proxy and onto the engine trace, since writes are already
        /// log-native (`ScopedVariablesUpdated`/`VariablesUpdated`, `JobCompleted`
        /// outputs) — this closes the missing read side on the generic
        /// service-task job path.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        fetch_variables: Vec<String>,
        /// The opaque **per-activation lease token**, minted only when the worker
        /// requests `withLease`, for any job kind. Distinct from `deadline`
        /// (Camunda `JobRecord.leaseToken`, ADR 0005-810-job-lease).
        /// A staleness handle (backed by a monotonic key, not a
        /// cryptographically unguessable secret) that only fences a command
        /// against a superseded activation; caller forgery-resistance is the
        /// gateway auth layer's job (ADR-0028). Generated exactly once at
        /// command-processing and carried here so replay restores it verbatim
        /// rather than regenerating it (D2). `None` for a *lease-less* activation,
        /// which the agent lease gate
        /// (`validate_agent_job_context`) treats as Camunda's `!hasLeaseToken()`
        /// — the lease comparison is skipped. Serialized only when present, so
        /// lease-less activations stay byte-identical in the journal.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        #[cfg_attr(
            feature = "serde",
            serde(deserialize_with = "crate::lease::deserialize_optional")
        )]
        lease_token: Option<String>,
    },
    /// A job's activation lock expired (its `deadline` passed); it becomes
    /// activatable again. Emitted by an `ExpireJobs` tick.
    JobLockExpired { job_key: Key, instance_key: Key },
    /// A worker reported that a job failed, setting its remaining `retries`. With
    /// retries left the job becomes activatable again; with none it parks and an
    /// [`Event::IncidentRaised`] follows.
    JobFailed {
        job_key: Key,
        instance_key: Key,
        retries: i32,
        /// The worker that was holding the activation lock at fail time. Carried
        /// on the event so a terminal (retries == 0) park durably retains it for
        /// incident attribution across engine restart *and* on the read-model
        /// leader-local path (where `JobActivated` is never exported, so the row
        /// would otherwise have `worker = NULL`). `None` for events serialized
        /// before this field existed, and when the job had no activating worker.
        #[cfg_attr(feature = "serde", serde(default))]
        worker: Option<String>,
        /// The worker-reported failure message, limited like Zeebe's
        /// `JobFailProcessor` (see [`crate::limit_error_message`]).
        /// Recorded on *every* fail, retries left or not, so the job's last
        /// failure reason is queryable (Zeebe job `errorMessage`, #1327). `None`
        /// for events serialized before this field existed.
        #[cfg_attr(feature = "serde", serde(default))]
        error_message: Option<String>,
        /// Camunda 8 v2 `retryBackOff` (#933): the absolute logical instant
        /// (ms) before which the job, returned to the pool with retries left,
        /// must not be re-activated (`now + retryBackOff` at fail time).
        /// `None` when no backoff was requested (retryable immediately), when
        /// no retries remain, and for events serialized before this field
        /// existed. Skipped when `None`, so backoff-less fails stay
        /// byte-identical.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        retry_backoff_until: Option<u64>,
    },
    /// A worker threw a business error from a job. The job is consumed; either a
    /// matching error boundary event interrupts the activity, or an
    /// [`Event::IncidentRaised`] follows when no boundary catches `error_code`.
    JobErrorThrown {
        job_key: Key,
        instance_key: Key,
        error_code: String,
        /// The worker that was holding the activation lock when the error was
        /// thrown. Carried on the event so the terminal `Errored` park durably
        /// retains it for incident attribution across engine restart *and* on the
        /// read-model leader-local path (see [`Event::JobFailed::worker`]). `None`
        /// for events serialized before this field existed, and when the job had
        /// no activating worker.
        #[cfg_attr(feature = "serde", serde(default))]
        worker: Option<String>,
        /// The worker-reported error message, limited like Zeebe's
        /// `JobThrowErrorProcessor` (Zeebe job `errorMessage`, #1327). `None` for
        /// events serialized before this field existed.
        #[cfg_attr(feature = "serde", serde(default))]
        error_message: Option<String>,
    },
    /// A job was completed. `created_at` is the logical instant the job was
    /// created (carried through from job state) so the server can observe the
    /// job's end-to-end sojourn (create→complete) at the completion site. `job_type`
    /// is carried so that sojourn can be reported *per job type* — the reporting
    /// surface that lets an operator localize external/worker strain to a specific
    /// process/job type (one type's sojourn stretching while the engine's internal
    /// command latency stays flat = a slow downstream for that type, not our
    /// congestion). Both are observational: `created_at` is `0` and `job_type` is
    /// empty for jobs created before the engine carried the fields; neither is used
    /// by replay.
    JobCompleted {
        job_key: Key,
        instance_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(feature = "serde", serde(default))]
        job_type: String,
        /// The worker that was holding the activation lock at completion time.
        /// Carried on the event so a successful completion durably retains it for
        /// attribution across engine restart *and* on the read-model leader-local
        /// path (where `JobActivated` is never exported, so the row would
        /// otherwise have `worker = NULL`) — symmetric with
        /// [`Event::JobFailed::worker`] / [`Event::JobErrorThrown::worker`]. This
        /// is what lets a *successful* job (including a husked agent round — a
        /// `COMPLETED` job that minted no `AgentInstance`) be attributed to the
        /// worker that ran it, via the Camunda-parity join
        /// `AgentInstance.jobKey → completed Job.worker`. `None` for events
        /// serialized before this field existed, and when the job had no
        /// activating worker. Observational: not used by replay.
        #[cfg_attr(feature = "serde", serde(default))]
        worker: Option<String>,
        /// The logical instant of the completion (the completing command's
        /// `now`, #1440). Retained on the job (`Job::terminal_end`) so a
        /// snapshot-seeded read model recovers `endTime`/`lastUpdateTime`.
        /// `0` for events serialized before this field existed (unknown).
        #[cfg_attr(feature = "serde", serde(default))]
        completed_at: u64,
        /// The activation-lock deadline the job held at completion (#1440).
        /// Carried on the replicated event (rather than read from replica job
        /// state) because a leader-local soft activation is never journaled:
        /// replicas, the event-driven read model and a snapshot reseed must all
        /// see the SAME terminal deadline. `None` when unlocked, and for events
        /// serialized before this field existed.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        deadline: Option<u64>,
    },
    /// A **leased** job's settlement was recorded in the settlement ledger
    /// (#1181): the lease token (per-activation fence) that settled it and the
    /// outcome. Emitted immediately before the terminal `JobCompleted` /
    /// `JobFailed` / `JobErrorThrown` of a leased job (never for a lease-less
    /// one, whose journal stays byte-identical). Carrying the token on a
    /// replicated event — rather than reading it from replica job state — keeps
    /// the ledger identical on every replica even when the activation itself was
    /// leader-local and never replicated, so a re-issued settlement is answered
    /// idempotently by whichever host the worker reaches.
    JobSettlementRecorded {
        job_key: Key,
        instance_key: Key,
        outcome: crate::state::JobSettlementOutcome,
        lease_token: String,
    },
    /// A **routed cross-partition delivery was applied** (#1497): the recipient
    /// partition records the source-minted `delivery_key` of a routed follow-up
    /// (`OpenMessageSubscription` / `CorrelateMessageSubscription` /
    /// `CloseMessageSubscription` / `DispatchStartInstance`) it applied, so a
    /// re-delivery of the same `delivery_key` — a host re-delivery
    /// after a lost acknowledgement — is acknowledged WITHOUT being re-applied
    /// (at-least-once transport ⇒ exactly-once effect, Zeebe's command
    /// distribution dedup).
    ///
    /// For a `CloseMessageSubscription` the record doubles as the **close
    /// tombstone**: it additionally carries the `subscription_key` it closed,
    /// so a delayed `OpenMessageSubscription` for that subscription — which
    /// bears its OWN, different `delivery_key` — that lands after its close is
    /// rejected instead of resurrecting a subscription for a cancelled
    /// instance. (`None` for the non-subscription `DispatchStartInstance`.)
    /// Replicated through the partition's log, so every replica holds the same
    /// ledger; bounded by [`crate::state::ROUTED_DELIVERY_LEDGER_CAP`].
    /// Additive variant: a journal written before #1497 never contains it.
    RoutedDeliveryRecorded {
        /// The source partition's minted delivery identity (see the routed
        /// source events' `delivery_key`).
        delivery_key: Key,
        /// The RECIPIENT partition that applied (and owns) this ledger record.
        /// `delivery_key` is minted on the SOURCE partition, so a key-derived
        /// split (`partition_of(max_key())`, the legacy shared-journal
        /// recovery) would file it under the sender; recovery routes the record
        /// by this field instead (see [`Event::owning_partition`]).
        #[cfg_attr(feature = "serde", serde(default))]
        recipient_partition: u64,
        /// The subscription this delivery concerned, recorded so a close
        /// tombstones it against a late open. `None` for a start-instance
        /// dispatch (no subscription).
        #[cfg_attr(feature = "serde", serde(default))]
        subscription_key: Option<Key>,
        /// `true` for a `CloseMessageSubscription` delivery: the record is the
        /// close tombstone for `subscription_key`.
        #[cfg_attr(feature = "serde", serde(default))]
        close_tombstone: bool,
    },
    /// A job's remaining retries were updated (e.g. by an operator recovering a
    /// parked job before resolving its incident). Does not change job state.
    JobRetriesUpdated {
        job_key: Key,
        instance_key: Key,
        retries: i32,
        /// Caller audit correlation id (Camunda `operationReference`). Derived
        /// at `Engine::emit` from the command's generic reference — the same
        /// value its [`Record`] envelope carries (#1556); never set per-site.
        #[cfg_attr(feature = "serde", serde(default))]
        operation_reference: Option<i64>,
    },

    /// A job's activation lock was extended: its `deadline` was reset to a later
    /// logical instant while it stayed `Activated` (e.g. a worker holding a
    /// long-running job open). Does not change job state; the holder keeps the
    /// lock until this new `deadline` passes.
    JobTimeoutUpdated {
        job_key: Key,
        instance_key: Key,
        deadline: u64,
        /// Caller audit correlation id (Camunda `operationReference`). Derived
        /// at `Engine::emit` from the command's generic reference — the same
        /// value its [`Record`] envelope carries (#1556); never set per-site.
        #[cfg_attr(feature = "serde", serde(default))]
        operation_reference: Option<i64>,
    },

    /// A user task was created for a `userTask` element; the token now rests
    /// until the task is completed. `created_at` is the logical instant it was
    /// created, carried on the event so replay reconstructs the same timestamp.
    /// The assignment/scheduling/priority attributes are the *resolved* values
    /// (FEEL evaluated against the instance variables, or literals) declared on
    /// the BPMN element. `assignee` is `None` when no assignee was declared.
    UserTaskCreated {
        user_task_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        created_at: u64,
        assignee: Option<String>,
        candidate_groups: Vec<String>,
        candidate_users: Vec<String>,
        due_date: Option<String>,
        follow_up_date: Option<String>,
        priority: i32,
        /// The resolved numeric form key, if the task's `zeebe:formDefinition`
        /// declared a `formId` that resolved against a currently-deployed form
        /// (latest version) at creation time. `None` when the task declares no
        /// embedded form, or its `formId` matched no deployed form. Serialized
        /// records written before form linkage load as `None`.
        #[cfg_attr(feature = "serde", serde(default))]
        form_key: Option<Key>,
        /// The external form reference declared via `zeebe:formDefinition
        /// externalReference`, surfaced verbatim. `None` when the task declares
        /// no external form. Serialized records written before form linkage load
        /// as `None`.
        #[cfg_attr(feature = "serde", serde(default))]
        external_form_reference: Option<String>,
    },
    /// A user task's assignee was set (or cleared, when `assignee` is `None`).
    UserTaskAssigned {
        user_task_key: Key,
        instance_key: Key,
        assignee: Option<String>,
    },
    /// A user task's attributes were changed via the update endpoint. Each field
    /// is `Some` only when that attribute was part of the changeset; an empty
    /// list or empty/`None` date resets the attribute.
    UserTaskUpdated {
        user_task_key: Key,
        instance_key: Key,
        candidate_groups: Option<Vec<String>>,
        candidate_users: Option<Vec<String>>,
        due_date: Option<Option<String>>,
        follow_up_date: Option<Option<String>>,
        priority: Option<i32>,
    },
    /// A user task was completed; the parked token resumes along the task's
    /// outgoing flow.
    UserTaskCompleted {
        user_task_key: Key,
        instance_key: Key,
    },
    /// A user task was cancelled because its activity/instance was terminated.
    UserTaskCanceled {
        user_task_key: Key,
        instance_key: Key,
    },

    /// A task-listener job was created for one listener in a user task's
    /// sequential chain (ADR 0037 §6). Mirrors [`Event::ExecutionListenerJobCreated`]
    /// but gates a *user-task* transition rather than an element lifecycle
    /// transition. Only emitted for user tasks that declare task listeners, so
    /// listener-free user tasks are byte-identical to the pre-task-listener
    /// engine.
    TaskListenerJobCreated {
        job_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        user_task_key: Key,
        job_type: String,
        event_type: crate::model::TaskListenerEventType,
        listener_index: usize,
        #[cfg_attr(feature = "serde", serde(default))]
        created_at: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::types::default_job_retries")
        )]
        retries: i32,
    },
    /// A user task began deferring a lifecycle transition behind its task
    /// listeners (ADR 0037 §6). Records the in-flight transition on the task
    /// until its listener chain drains (commit) or a listener denies it.
    UserTaskTransitionDeferred {
        user_task_key: Key,
        instance_key: Key,
        pending: crate::state::types::PendingUserTaskTransition,
    },
    /// A task listener returned corrections to user-task data; they merge into
    /// the deferred transition's accumulated corrections (ADR 0037 §6).
    UserTaskCorrectionsApplied {
        user_task_key: Key,
        instance_key: Key,
        corrections: crate::model::UserTaskCorrections,
    },
    /// A user task's deferred transition was resolved (committed after its
    /// listener chain drained, or denied by a listener); the pending transition
    /// is cleared. The actual state change (assign/update/complete/cancel) is
    /// carried by the accompanying lifecycle event on commit. `denied` is
    /// `Some(reason)` when a task listener denied the transition (the task
    /// returns to its prior available state) and `None` on a normal commit.
    UserTaskTransitionResolved {
        user_task_key: Key,
        instance_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        denied: Option<String>,
    },

    /// An incident was raised (e.g. an exclusive gateway found no matching flow,
    /// or a job exhausted its retries); the token is parked until the incident
    /// is resolved. `job_key` is `Some` only for recoverable job-incidents.
    /// `created_at` is the logical instant the incident was raised, carried on
    /// the event so replay reconstructs the same timestamp.
    IncidentRaised {
        incident_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: IncidentKind,
        /// The lifecycle phase / projection to replay on resolution
        /// (phase-driven recovery). Present for every `IncidentKind::IoMapping`
        /// incident, and also for the ad-hoc call-activity tool recovery paths
        /// (#1176) that park on an `IncidentKind::ExpressionEvaluation`
        /// (output-collection type) or `IncidentKind::CalledElementError`
        /// (child-spawn) incident while carrying a preserved single-pass
        /// projection to reuse verbatim on redrive. `None` for every other
        /// incident, whose recovery is derived from the kind. `serde(default)`
        /// so events journaled before phase-driven recovery replay as `None`.
        #[cfg_attr(feature = "serde", serde(default))]
        redrive: Option<IoMappingRedrive>,
        reason: String,
        job_key: Option<Key>,
        created_at: u64,
    },
    /// An incident was resolved; the engine then retries the work that failed
    /// (see [`crate::Command::ResolveIncident`]). For a job-incident (`job_key`
    /// is `Some`) the parked job returns to the activatable pool. The record is
    /// retained (transitioned to `Resolved`) with `resolved_at` and any
    /// `operation_reference` for audit.
    IncidentResolved {
        incident_key: Key,
        instance_key: Key,
        job_key: Option<Key>,
        resolved_at: u64,
        operation_reference: Option<i64>,
    },

    /// The last token of a process instance was consumed; the instance is done.
    ProcessInstanceCompleted { instance_key: Key },

    /// A process instance was cancelled by an operator: every token was
    /// discarded and the instance transitions to `Terminated` (it did not
    /// complete normally). The resource-cancellation events this command
    /// produced (`JobCanceled`, `TimerCanceled`, `MessageSubscriptionCanceled`)
    /// precede it; this event closes any still-active incident on the instance.
    ProcessInstanceTerminated { instance_key: Key },

    /// Cancellation of a process instance began, but one or more user tasks are
    /// running their `canceling` task listeners (ADR 0037 §6). The instance's
    /// other tokens are already discarded (their cancellation events precede
    /// this one); it moves to `Terminating` and finishes with
    /// [`Event::ProcessInstanceTerminated`] once the last canceling chain drains.
    ProcessInstanceTerminating { instance_key: Key },

    /// A process instance was suspended by an operator
    /// ([`crate::Command::SuspendInstance`]). It stops making progress (its jobs
    /// become non-activatable and its timers/other triggers do not fire) while
    /// retaining all runtime state, and its `state` becomes `Suspended`. Only
    /// emitted for an instance currently `Active`. `at` is the suspension instant
    /// (ms since epoch) — the `suspendedDate` surfaced downstream. Additive,
    /// replay-safe new variant (AGENTS.md §"Adding or Changing an Event").
    ProcessInstanceSuspended { instance_key: Key, at: u64 },

    /// A suspended process instance was resumed by an operator
    /// ([`crate::Command::ResumeInstance`]). Its `state` returns to `Active` with
    /// its exact prior running state, and its `suspended_at`/`suspendedDate`
    /// clears. Only emitted for an instance currently `Suspended`. Additive,
    /// replay-safe new variant.
    ProcessInstanceResumed { instance_key: Key },

    /// A business id was assigned to a root process instance as part of a job
    /// completion (Camunda 8.10 `JobCompletionRequest.businessId`). Emitted at
    /// most once per instance, immediately before the completing
    /// [`Event::JobCompleted`]; only artifacts created afterwards carry it.
    /// Additive, replay-safe new variant.
    ProcessInstanceBusinessIdAssigned {
        instance_key: Key,
        business_id: String,
    },

    /// A process instance was migrated to a target process definition (Zeebe
    /// process-instance migration). Carries the full remapping the applier needs
    /// to rewrite state deterministically on replay: `target_process_id` is the
    /// BPMN process id the instance now belongs to, `target_process_definition_key`
    /// its deployment key, and `element_mappings` the accepted
    /// `(source_element_id, target_element_id)` pairs. [`crate::state::apply`]
    /// re-points every active element instance carrying a mapped `source` id (and
    /// its attached jobs, user tasks, timers, subscriptions and incidents) at the
    /// corresponding `target`, and sets the instance's `process_id`.
    ProcessInstanceMigrated {
        instance_key: Key,
        target_process_id: String,
        target_process_definition_key: Key,
        element_mappings: Vec<(ElementId, ElementId)>,
        /// The flow-scope element instances (embedded / event sub-processes)
        /// re-pointed by this migration (#1374). Additive; empty when none.
        #[cfg_attr(feature = "serde", serde(default))]
        migrated_flow_scopes: Vec<MigratedElementInstance>,
        /// Catch-event / boundary subscriptions retained, re-created, created
        /// or deleted by this migration (#1374). Additive; empty when none.
        #[cfg_attr(feature = "serde", serde(default))]
        subscriptions: Vec<MigratedSubscription>,
        /// Call-activity child instances kept attached to their (migrated)
        /// call-activity element instance (#1374). Additive; empty when none.
        #[cfg_attr(feature = "serde", serde(default))]
        child_instances: Vec<MigratedChildInstance>,
        /// Whether the join bookkeeping was migrated per flow-scope instance
        /// (#1580). Additive; a legacy record decodes `false` and replays the
        /// pre-#1580 element-wide merge (one slot per target join element).
        #[cfg_attr(feature = "serde", serde(default))]
        join_scoped: bool,
        /// The compensation subscriptions re-pointed by this migration
        /// (#1631). Only applied when `compensation_migrated` is set: such a
        /// migration also deletes every subscription NOT listed (its
        /// compensation boundary was left unmapped, Zeebe parity) and remaps
        /// in-flight compensation waits. Additive; empty when none.
        #[cfg_attr(feature = "serde", serde(default))]
        compensation_subscriptions: Vec<MigratedCompensationSubscription>,
        /// Whether this migration carries the compensation plan (#1631).
        /// Additive; a legacy record decodes `false` (pre-#1631 migrations
        /// rejected every instance holding compensation state).
        #[cfg_attr(feature = "serde", serde(default))]
        compensation_migrated: bool,
    },

    /// A process instance was modified (Zeebe `ProcessInstanceModification
    /// MODIFIED`): the accepted instructions of a
    /// [`crate::Command::ModifyInstance`], recorded as an audit fact ahead of
    /// the element activation / termination events the modification produces
    /// (those carry the state change; this record's applier is a no-op).
    /// Additive, replay-safe new variant (#1374).
    ProcessInstanceModified {
        instance_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        activate_instructions: Vec<ActivateElementInstruction>,
        #[cfg_attr(feature = "serde", serde(default))]
        terminate_instructions: Vec<Key>,
        #[cfg_attr(feature = "serde", serde(default))]
        terminate_element_ids: Vec<ElementId>,
        #[cfg_attr(feature = "serde", serde(default))]
        move_instructions: Vec<MoveInstruction>,
        /// Element instances (incl. created flow scopes) activated by the
        /// modification (Zeebe `activatedElementInstanceKeys`).
        #[cfg_attr(feature = "serde", serde(default))]
        activated_element_instance_keys: Vec<Key>,
    },

    /// A timer was armed: either on a timer intermediate catch event (the token
    /// rests on it) or as an interrupting boundary timer on an activity (the
    /// activity runs as normal until the timer fires). `due_at` is the logical
    /// instant it fires and `kind` records what it guards, both carried on the
    /// event so replay reconstructs the timer exactly.
    TimerCreated {
        timer_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        due_at: u64,
        kind: TimerKind,
        /// Remaining fires of a bounded cycle (`Rn/…`) INCLUDING this one;
        /// the timer is not re-armed after the fire that consumes the last
        /// (#1596). `None` for an unbounded cycle, a one-shot, and records
        /// serialized before this field existed.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        repetitions: Option<u32>,
    },
    /// A due timer fired. For an intermediate catch event its token is released
    /// along the event's outgoing flow; for an interrupting boundary timer the
    /// attached activity is interrupted and the boundary's outgoing flow runs
    /// (the job-cancellation, element-completion and sequence-flow events follow).
    TimerTriggered {
        timer_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An armed timer was cancelled before firing because the element it guarded
    /// left the flow first (e.g. a boundary timer whose activity completed
    /// normally, or a sibling boundary timer when another fired).
    TimerCanceled {
        timer_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A job was cancelled because its activity was interrupted by a boundary
    /// event firing.
    JobCanceled {
        job_key: Key,
        instance_key: Key,
        /// The logical instant of the cancellation (the canceling command's
        /// `now`, #1440); see `Event::JobCompleted::completed_at`. `0` for
        /// events serialized before this field existed (unknown).
        #[cfg_attr(feature = "serde", serde(default))]
        canceled_at: u64,
        /// The activation-lock deadline the job held at cancellation; see
        /// `Event::JobCompleted::deadline` (#1440).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        deadline: Option<u64>,
    },

    /// A message was published. This records no durable state itself; it
    /// carries the minted `message_key` (returned to the host and used to
    /// restore the key generator on replay) and heads the
    /// [`Event::MessageCorrelated`] events the same command produced. A message
    /// published with a positive time-to-live is additionally buffered by the
    /// [`Event::MessageBuffered`] that follows.
    MessagePublished {
        message_key: Key,
        message_name: String,
        correlation_key: String,
    },
    /// A published message was **buffered** until `deadline` (#1874, Zeebe
    /// `MessageRecord.deadline = timestamp + timeToLive`) on `partition` — the
    /// partition owning `hash(correlation_key)`. While buffered, a newly opened
    /// subscription for `(message_name, correlation_key)` correlates it
    /// ([`Event::BufferedMessageCorrelated`]) and a publish carrying the same
    /// `message_id` is rejected. `correlated_instances` are the instances the
    /// publish already correlated (Zeebe `existMessageCorrelation`): a message
    /// correlates to an instance at most once. Removed by
    /// [`Event::MessageExpired`].
    MessageBuffered {
        message_key: Key,
        partition: u64,
        message_name: String,
        correlation_key: String,
        deadline: u64,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        message_id: Option<String>,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "BTreeMap::is_empty")
        )]
        variables: BTreeMap<String, Value>,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        correlated_instances: Vec<Key>,
    },
    /// A buffered message passed its deadline and was removed from the buffer
    /// (#1874, Zeebe `MessageIntent.EXPIRED`). Emitted by `TriggerTimers`.
    MessageExpired { message_key: Key, partition: u64 },
    /// A buffered message was correlated to a subscription opened after it was
    /// published (#1874, Zeebe `MessageSubscriptionCreateProcessor` →
    /// `correlateNextMessage`). Records `instance_key` against the message so
    /// it never correlates to that instance again; the
    /// [`Event::MessageCorrelated`] that follows carries the actual
    /// correlation. Buffered correlation is LOCAL-ONLY: an instance on another
    /// partition never takes a buffered message (remote delivery is tracked in
    /// https://github.com/nanobpm/nano-bpm/issues/1949).
    BufferedMessageCorrelated {
        message_key: Key,
        partition: u64,
        subscription_key: Key,
        instance_key: Key,
    },
    /// A message subscription was opened: either on a message intermediate catch
    /// event (the token rests on it) or as an interrupting message boundary on an
    /// activity (the activity runs as normal until a message is correlated).
    /// `correlation_key` is the resolved correlation value captured at open time,
    /// carried on the event so replay reconstructs the subscription exactly.
    MessageSubscriptionCreated {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
        kind: MessageSubscriptionKind,
    },
    /// The **instance** partition parked a token on a message catch element whose
    /// canonical subscription lives on another partition (`hash(correlation_key)`,
    /// see [`crate::subscription_partition`]). This records the instance
    /// partition's pending view; the host routes an
    /// [`crate::Command::OpenMessageSubscription`] to the message partition (which
    /// records the canonical [`Event::MessageSubscriptionCreated`]) and later a
    /// [`crate::Command::CorrelateMessageSubscription`] continuation back here to
    /// advance the token. Only emitted when `num_partitions > 1` and the key
    /// hashes off-partition — a single-partition host never produces it, so its
    /// log is byte-identical to before.
    ///
    /// `delivery_key` is the **stable delivery identity** (#1497) the host's
    /// bounded re-drive carries to the message partition: minted on this (the
    /// source) partition when the event is emitted, it lets the recipient
    /// deduplicate a re-delivered `OpenMessageSubscription` after a lost
    /// acknowledgement (at-least-once ⇒ exactly-once). `0` on a record written
    /// before #1497, which the recipient treats as "no identity" (no dedup —
    /// the legacy best-effort path).
    MessageSubscriptionOpening {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
        kind: MessageSubscriptionKind,
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
    },
    /// A published message correlated to an open subscription. For an
    /// intermediate catch event its token is released along the event's outgoing
    /// flow; for an interrupting boundary the attached activity is interrupted and
    /// the boundary's outgoing flow runs (the job-cancellation, element-completion
    /// and sequence-flow events follow). `message_key` ties it back to the
    /// [`Event::MessagePublished`] that produced it.
    ///
    /// `variables` is the correlated message's payload, recorded **on the event**
    /// (#1739) so consumers (the trace store's recorded-input log) read it
    /// directly instead of inferring it from the variable writes that follow —
    /// those writes cannot distinguish a payload from a mapping write. Empty
    /// for a payload-less message and on a record written before #1739.
    ///
    /// `boundary_element_id` is the boundary event the correlation triggers
    /// (interrupting or non-interrupting message boundary), `None` for an
    /// intermediate catch / event sub-process start and on a pre-#1739 record.
    /// Here `element_instance_key` / `element_id` name the subscription's host
    /// (the attached activity for a boundary); the boundary event's own element
    /// instance is minted by the `ElementActivating` this correlation enqueues,
    /// which names `boundary_element_id` in the same command.
    MessageCorrelated {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "BTreeMap::is_empty")
        )]
        variables: BTreeMap<String, Value>,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        boundary_element_id: Option<ElementId>,
    },
    /// A published message matched a subscription on the **message** partition
    /// whose instance lives on another partition. Settles the canonical
    /// subscription (exactly like [`Event::MessageCorrelated`]) but does **not**
    /// advance the token here; it carries the full continuation payload (kind +
    /// the message's variables) so the host can route a
    /// [`crate::Command::CorrelateMessageSubscription`] to the instance partition,
    /// where the token actually advances. Only produced when `num_partitions > 1`.
    ///
    /// `delivery_key` is the stable delivery identity (#1497) a host re-delivery
    /// carries with the continuation: minted on this (the message) partition, it
    /// lets the instance partition deduplicate a re-delivered
    /// `CorrelateMessageSubscription` after a lost acknowledgement — the guard
    /// that keeps a retried **non-interrupting** boundary correlation (whose
    /// `Opening` record intentionally stays open) from spawning a second token.
    /// `0` on a pre-#1497 record: no dedup (the legacy path).
    RemoteMessageCorrelation {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: MessageSubscriptionKind,
        variables: BTreeMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
        /// Ack-paced buffered delivery (#1949, Zeebe `CORRELATING`): when
        /// `Some`, this delivery came from the message buffer (to a one-shot
        /// or a persistent subscription alike), and the instance partition
        /// must answer with an [`Event::MessageCorrelationAcknowledging`]
        /// routed back to this (message) partition, which settles the
        /// reservation. The applier marks the canonical subscription
        /// `awaiting_ack`. `None` on every other correlation and on a
        /// pre-#1949 record (additive).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        ack_partition: Option<u64>,
    },
    /// The instance partition's answer (#1949) to an ack-paced
    /// [`Event::RemoteMessageCorrelation`]. `consumed` says whether the token
    /// actually took the message (an unconsumed reservation is released on
    /// the message partition); `accepted` whether the subscription still
    /// waits for more (consumed AND its persistent scope survived) — `false`
    /// closes it there. A suspended instance whose scope is alive answers
    /// `deferred` instead (see the field). Derived from durable state, so a
    /// re-driven duplicate delivery is answered identically. The
    /// host routes it to `target_partition` (the message partition) as a
    /// [`crate::Command::AcknowledgeMessageCorrelation`]. `partition` is the
    /// emitting (instance) partition, recorded explicitly because the message
    /// key may be foreign. State-neutral on the emitting partition.
    MessageCorrelationAcknowledging {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        accepted: bool,
        /// Whether the parked token took the message (see the variant doc).
        consumed: bool,
        /// The instance is SUSPENDED but its subscription scope is alive
        /// (#1874): the message partition keeps the subscription open and the
        /// message buffered for it, instead of closing it. Additive.
        #[cfg_attr(feature = "serde", serde(default))]
        deferred: bool,
        partition: u64,
        target_partition: u64,
        /// The answered delivery's key (#1949). Applying this record stores
        /// the answer as that delivery's durable receipt on the emitting
        /// (instance) partition, so a re-driven duplicate is answered with
        /// the ORIGINAL outcome — never one recomputed from later state.
        /// Additive: `0` on a pre-receipt record (no receipt stored).
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
        /// The RESOLVED consumption element (#2037): the element the
        /// delivery was consumed at, read from the instance partition's durable state when the
        /// answer is derived — so an instance migrated between the message
        /// partition's reservation and the consumption reports the
        /// post-migration element, not the stale one the delivery was
        /// routed with (Zeebe re-points the message-partition
        /// subscription on migration). `None` when unresolvable and on a
        /// pre-#2037 record (additive). No process definition key is
        /// carried: it is derivable from the instance, whose migration
        /// already re-points it, so a copy here would be a drift surface.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        element_id: Option<ElementId>,
    },
    /// The message partition applied an instance partition's acknowledgement
    /// (#1949): clears the canonical subscription's `awaiting_ack`; an
    /// unconsumed delivery releases the message's reservation (it is
    /// available again, to this instance too); a rejected delivery also
    /// closes the subscription, so the untaken buffered messages stay
    /// buffered for other subscriptions. `partition` is this (message)
    /// partition: the subscription key is foreign.
    MessageCorrelationAcknowledged {
        subscription_key: Key,
        message_key: Key,
        accepted: bool,
        /// See [`Event::MessageCorrelationAcknowledging`]'s `consumed`.
        consumed: bool,
        /// See [`Event::MessageCorrelationAcknowledging`]'s `deferred`.
        #[cfg_attr(feature = "serde", serde(default))]
        deferred: bool,
        partition: u64,
        /// See [`Event::MessageCorrelationAcknowledging`]'s `element_id`
        /// (#2037): applying it re-points the canonical subscription.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        element_id: Option<ElementId>,
    },
    /// A published message correlated to a process-level **message start
    /// event** and created instance `instance_key` (#1300; Zeebe records this as
    /// a correlated message subscription). Emitted right after the instance's
    /// [`Event::ProcessInstanceCreated`], on the partition that minted it.
    /// `subscription_key` is the start subscription's key — nano keys message
    /// start subscriptions by their `process_definition_key`. Purely a history
    /// record: it changes no engine state.
    MessageStartEventCorrelated {
        message_key: Key,
        subscription_key: Key,
        process_definition_key: Key,
        process_id: String,
        instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
    },
    /// A Raft applied-entry record (#1405): the LAST record of the single
    /// atomic journal frame that carries every event of one committed Raft log
    /// entry, so the journal knows — in the same all-or-nothing write as the
    /// entry's effects — which Raft entry it last applied. `index` is the Raft
    /// log index; `raft_state` is the replication layer's opaque serialization
    /// of the applied log id and membership (engine-core never interprets it).
    /// On a Raft bootstrap without a snapshot the state machine seeds its
    /// `last_applied` from the journal-recovered record, so entries already in
    /// the journal are never re-applied. State-neutral: the applier ignores it
    /// and it carries no keys, so the read model is unaffected.
    ///
    /// `start_dispatch_rr` is the engine's round-robin start-dispatch cursor
    /// after the entry applied. It is the one piece of engine state that is not
    /// derived from events, so a replica that seeds `last_applied` from this
    /// record (skipping command re-execution) restores it from here and routes
    /// the next round-robin start identically to a peer that never restarted.
    /// Additive: a pre-existing record decodes with `0`.
    /// The partition's lenient-completion mode was set (#1661, ADR 0066 slice
    /// 2) by a replicated [`crate::Command::SetLenientCompletion`]. When
    /// `lenient` is `true`, `CompleteJob` / `FailJob` / `ThrowJobError` no longer
    /// require the job to have been activated on this replica (activation is
    /// leader-local). Engine state, not host config: every replica flips it at
    /// the same log index, a snapshot carries it and a journal replay restores
    /// it. Additive variant (old journals never contain it).
    LenientCompletionSet { lenient: bool },
    RaftEntryApplied {
        index: u64,
        raft_state: String,
        #[cfg_attr(feature = "serde", serde(default))]
        start_dispatch_rr: u64,
    },
    /// An open message subscription was cancelled before correlating because the
    /// element it guarded left the flow first (e.g. a boundary subscription whose
    /// activity completed normally, or a sibling boundary subscription when
    /// another boundary on the same activity fired).
    MessageSubscriptionCanceled {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },

    /// A signal was broadcast. Signals are not buffered, so this records no
    /// durable state; it carries the minted `signal_key` (returned to the host
    /// and used to restore the key generator on replay) and heads the
    /// [`Event::SignalCorrelated`] events the same command produced.
    SignalBroadcast {
        signal_key: Key,
        signal_name: String,
    },
    /// A signal subscription was opened: on a signal intermediate catch event
    /// (the token rests on it) or as a signal boundary on an activity. Signals
    /// correlate by **name only**, so there is no correlation key.
    SignalSubscriptionCreated {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        signal_name: String,
        kind: MessageSubscriptionKind,
    },
    /// A broadcast signal correlated to an open subscription. For an intermediate
    /// catch its token is released along the event's outgoing flow; for an
    /// interrupting boundary the attached activity is interrupted and the
    /// boundary's outgoing flow runs. `signal_key` ties it back to the
    /// [`Event::SignalBroadcast`] that produced it.
    SignalCorrelated {
        subscription_key: Key,
        signal_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An open signal subscription was cancelled before correlating because the
    /// element it guarded left the flow first (mirrors
    /// [`Event::MessageSubscriptionCanceled`]).
    SignalSubscriptionCanceled {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A conditional subscription was opened: on a conditional intermediate catch
    /// event (the token rests on it) or as a conditional boundary on an activity.
    /// It carries the FEEL `condition` and the set of root variable names the
    /// condition references (`referenced_vars`), so the engine re-evaluates it
    /// only when one of those variables changes. Unlike message/signal
    /// subscriptions it has no external trigger command: the engine evaluates it
    /// on open and on each change to a referenced variable, firing when the
    /// condition becomes `true` (see [`Event::ConditionalTriggered`]).
    ConditionalSubscriptionCreated {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        condition: String,
        referenced_vars: Vec<String>,
        kind: MessageSubscriptionKind,
    },
    /// A conditional subscription's condition evaluated `true`. For an
    /// intermediate catch or an interrupting boundary this settles the
    /// subscription (it fires once); for a non-interrupting boundary it stays
    /// open (each satisfying variable change spawns another token). The token
    /// advance (catch completion / boundary interrupt / parallel spawn) is
    /// carried by the surrounding events the same command produced.
    ConditionalTriggered {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An open conditional subscription was cancelled before firing because the
    /// element it guarded left the flow first (mirrors
    /// [`Event::SignalSubscriptionCanceled`]).
    ConditionalSubscriptionCanceled {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// An activity carrying a [`CompensationBoundaryEvent`] completed
    /// successfully and became *compensable*: a later
    /// [`CompensationThrowEvent`] can run its `handler` to compensate it.
    /// Records the completed activity's element instance, its compensation
    /// `handler` activity id, and the scope it completed in.
    ///
    /// `scope_subscription` (#901) marks a **flow-scope** subscription: a
    /// completed sub-process (or other scope-owning activity) that contains at
    /// least one compensable subscription records its own handlerless
    /// subscription (Zeebe
    /// `BpmnCompensationSubscriptionBehaviour.isFlowScopeWithSubscriptions`),
    /// so a whole-scope throw can trigger the nested subscriptions through it
    /// (`triggerCompensationFromTopToBottom`). A scope subscription has an
    /// empty `handler` and runs no handler itself; it completes
    /// bottom-to-top once every subscription inside its scope has completed.
    /// Additive: `serde(default)` decodes pre-#901 records as `false` (a
    /// boundary subscription), exactly what every pre-#901 record was.
    ///
    /// [`CompensationBoundaryEvent`]: crate::model::ElementKind::CompensationBoundaryEvent
    /// [`CompensationThrowEvent`]: crate::model::ElementKind::CompensationThrowEvent
    CompensationSubscriptionCreated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        handler: ElementId,
        scope: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        scope_subscription: bool,
        /// The compensation boundary event this subscription was recorded for
        /// (#1635), so a throw and a later migration resolve the subscription's
        /// boundary by identity rather than by occurrence order. Empty for a
        /// scope subscription. Additive: `serde(default)` decodes older records
        /// as empty, and resolution falls back to boundary-id occurrence order.
        #[cfg_attr(feature = "serde", serde(default))]
        boundary_id: ElementId,
    },
    /// A compensation throw event fired: it consumed the compensable
    /// subscriptions in its `scope` and triggered their handlers. `handlers`
    /// lists the handler activity ids activated and `consumed` the compensable
    /// element-instance keys removed. The throw's token
    /// (`throw_element_instance_key`) rests until every triggered handler
    /// completes.
    ///
    /// `handler_instance_keys` (#897) is the element-instance key minted for each
    /// activated handler, positionally aligned with `handlers`: it binds every
    /// pending handler to the throw that activated it, so a handler's completion
    /// routes back to ITS throw — never to another throw in the same scope that
    /// happens to wait on the same handler element id (the `element_id` + `min()`
    /// misattribution). Additive: `serde(default)` decodes pre-#897 records with
    /// an empty list, and the reducer falls back to id-only waits for them.
    ///
    /// `scope_subscriptions` (#901) lists the compensable element-instance keys
    /// of the triggered **flow-scope** subscriptions (completed sub-processes
    /// whose own compensation is the fan-out into their nested subscriptions,
    /// Zeebe `triggerCompensationFromTopToBottom`). A scope subscription runs
    /// no handler: it retires bottom-to-top once every subscription inside its
    /// scope has completed, and the throw rests until both its handler
    /// instances AND its scope subscriptions have drained. Additive:
    /// `serde(default)` decodes pre-#901 records with an empty list.
    CompensationTriggered {
        instance_key: Key,
        throw_element_instance_key: Key,
        throw_element_id: ElementId,
        scope: Key,
        handlers: Vec<ElementId>,
        consumed: Vec<Key>,
        #[cfg_attr(feature = "serde", serde(default))]
        handler_instance_keys: Vec<Key>,
        #[cfg_attr(feature = "serde", serde(default))]
        scope_subscriptions: Vec<Key>,
        /// Aligned with `handler_instance_keys` (#901): the scope each
        /// triggered handler's subscription was recorded in — the durable
        /// handler -> scope relationship that decides when an enclosing scope
        /// subscription has drained. Additive (`serde(default)`).
        #[cfg_attr(feature = "serde", serde(default))]
        handler_scopes: Vec<Key>,
        /// Aligned with `scope_subscriptions` (#901): the scope each triggered
        /// scope subscription was recorded in (its parent scope subscription,
        /// or the throw's scope). Additive (`serde(default)`).
        #[cfg_attr(feature = "serde", serde(default))]
        scope_subscription_parents: Vec<Key>,
    },
    /// One compensation handler triggered by a compensation throw event
    /// completed. Removes the handler from the throw's outstanding set; when it
    /// empties the throw event completes and routes onward.
    ///
    /// `handler_element_instance_key` (#897) is the completing handler's element
    /// instance key: the reducer matches the outstanding entry by instance key
    /// (not merely by element id), so two throws in one scope waiting on the same
    /// handler element id can never consume each other's completions. `None` only
    /// on pre-#897 replay paths that route by element id.
    CompensationHandlerCompleted {
        instance_key: Key,
        throw_element_instance_key: Key,
        handler_element_id: ElementId,
        #[cfg_attr(feature = "serde", serde(default))]
        handler_element_instance_key: Option<Key>,
    },
    /// A triggered **scope subscription** (#901) completed: the compensation
    /// fan-out inside the completed sub-process it stands for has fully
    /// drained (every nested handler done, every nested scope subscription
    /// completed). Retires the scope subscription from the throw's outstanding
    /// set — bottom-to-top (Zeebe
    /// `completeFlowScopeSubscriptionFromBottomToTop`); when the throw's
    /// handlers AND scope subscriptions are all drained the throw event
    /// completes and routes onward. `scope_subscription_key` is the
    /// compensable element-instance key of the completed scope subscription
    /// (the sub-process's element instance).
    CompensationScopeSubscriptionCompleted {
        instance_key: Key,
        throw_element_instance_key: Key,
        scope_subscription_key: Key,
    },
    /// A sub-process scope was torn down (by a scoped terminate end or an
    /// interrupting boundary), so the compensation state scoped to it must be
    /// dropped. `scopes` is the terminated scope element instance plus every
    /// descendant scope; the reducer removes each `compensable` subscription and
    /// `compensation_waits` entry whose `scope` is in this set. Carries the scope
    /// set explicitly (rather than recomputing it) because the descendant tokens
    /// are torn down in the same batch, so the scope tree no longer exists by
    /// replay. Emitted only when the scope actually holds compensation state.
    ScopedCompensationCleared { instance_key: Key, scopes: Vec<Key> },
    /// A multi-instance body activated: its `input_collection` was evaluated to
    /// `items` and one child of `element_id` will run per item (all at once when
    /// `sequential` is `false`, one after another when `true`). Carries the
    /// resolved loop configuration so the body's runtime state is fully
    /// reconstructable from the log. `body_key` is the body element instance (the
    /// scope the children run in).
    MultiInstanceActivated {
        instance_key: Key,
        body_key: Key,
        element_id: ElementId,
        sequential: bool,
        items: Vec<Value>,
        input_element: Option<String>,
        output_collection: Option<String>,
        output_element: Option<String>,
        completion_condition: Option<String>,
        /// The BPMN `cancelRemainingInstances` attribute resolved at parse
        /// (default `true`): whether a fired completion condition cancels the
        /// children still running (`true`) or latches and defers the body's
        /// completion until they drain (`false`). Additive with a serde default
        /// of `true` — a body recorded before this field existed completes
        /// exactly as the pre-field engine did (cancel remaining).
        #[cfg_attr(
            feature = "serde",
            serde(default = "crate::state::types::default_true")
        )]
        cancel_remaining_instances: bool,
    },
    /// A multi-instance body's `<completionCondition>` fired with
    /// `cancelRemainingInstances=false`: the fulfilment is latched and the body
    /// completes once its outstanding children drain — even if a variable the
    /// condition reads later changes or becomes unreadable (Zeebe
    /// `ElementInstance#isCompletionConditionFulfilled`). Durable so the latch
    /// survives replay.
    MultiInstanceCompletionConditionFulfilled { instance_key: Key, body_key: Key },
    /// A multi-instance child instance was activated at `index` (0-based) into the
    /// input collection. `local_variables` are the child's scope bindings (the
    /// `input_element` item, when named, and `loopCounter`), overlaid on the
    /// instance variables when the child's job is activated or its FEEL evaluated.
    MultiInstanceChildActivated {
        instance_key: Key,
        body_key: Key,
        child_key: Key,
        index: usize,
        local_variables: BTreeMap<String, Value>,
    },
    /// A multi-instance child completed: its `output` (the evaluated
    /// `output_element`, if any) is recorded at `index` in the body's collected
    /// results, the child leaves the body's active set, and its local variable
    /// overlay is dropped.
    MultiInstanceChildCompleted {
        instance_key: Key,
        body_key: Key,
        child_key: Key,
        index: usize,
        output: Option<Value>,
    },
    /// A multi-instance body completed (all children finished, or a completion
    /// condition fired). Its runtime state is dropped; the aggregated
    /// `output_collection` (when named) is written by a surrounding
    /// [`Event::VariablesUpdated`] and its outgoing flow taken by the surrounding
    /// element-completion events.
    MultiInstanceCompleted { instance_key: Key, body_key: Key },
    /// An ad-hoc sub-process container was activated (ADR 0023 seam 2). The
    /// container element instance is the ad-hoc token scope; its agent job is
    /// created by a surrounding [`Event::JobCreated`]. Carries the resolved
    /// `output_collection`/`output_element` so the runtime state reconstructs
    /// from the log. `container_key` is the container element instance.
    AdHocActivated {
        instance_key: Key,
        container_key: Key,
        element_id: ElementId,
        output_collection: Option<String>,
        output_element: Option<String>,
    },
    /// An ad-hoc tool child instance was activated (by an agent activate-element
    /// instruction) inside `container_key`'s scope. `local_variables` are the
    /// child's scope bindings (the instruction's seed variables), overlaid when
    /// the child's job is activated or its FEEL evaluated. The child joins the
    /// container's active set.
    AdHocToolActivated {
        instance_key: Key,
        container_key: Key,
        child_key: Key,
        local_variables: BTreeMap<String, Value>,
    },
    /// An ad-hoc tool child completed: its `output` (the container's evaluated
    /// `output_element`, if any) is appended to the container's accumulated
    /// results and the child leaves the active set. Its local variable overlay
    /// is dropped by the child's surrounding `ElementCompleted`.
    AdHocToolCompleted {
        instance_key: Key,
        container_key: Key,
        child_key: Key,
        output: Option<Value>,
    },
    /// An ad-hoc container's agent job re-emitted for the next turn (a new
    /// activate-element cycle). Bumps the container's iteration counter. The new
    /// job itself is carried by a surrounding [`Event::JobCreated`].
    AdHocIterated {
        instance_key: Key,
        container_key: Key,
    },
    /// The declared `<completionCondition>` was satisfied while the container's
    /// `cancelRemainingInstances` attribute is `false`: rather than cancel the
    /// tools still running, the container latches the fulfilment and defers its
    /// completion until they drain (Zeebe
    /// `BpmnAdHocSubProcessBehavior#completionConditionFulfilled`). Durable so
    /// the latch survives replay.
    AdHocCompletionConditionFulfilled {
        instance_key: Key,
        container_key: Key,
    },
    /// An ad-hoc container completed (the agent signalled completion, or no tools
    /// remained and none were requested, or a cancel was requested). Its runtime
    /// state is dropped; the aggregated `output_collection` (when named) is
    /// written by a surrounding [`Event::VariablesUpdated`] and its outgoing flow
    /// taken by the surrounding element-completion events. `cancelled` records
    /// whether completion was a `cancel_remaining_instances` request (which tears
    /// down any still-active tools) versus a normal agent-signalled finish, so the
    /// distinction is durable on the journal for the trace read model and metrics.
    AdHocCompleted {
        instance_key: Key,
        container_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        cancelled: bool,
    },
    /// The **instance** partition tore down a cross-partition parked
    /// subscription (state [`crate::state::MessageSubscriptionState::Opening`],
    /// recorded by [`Event::MessageSubscriptionOpening`]) because the element it
    /// guarded left the flow (cancel, normal completion or a sibling boundary
    /// firing). Marks the local placeholder cancelled and carries
    /// `message_name` + `correlation_key` so the host routes a
    /// [`crate::Command::CloseMessageSubscription`] to the message partition
    /// (`hash(correlation_key)`) to disarm the canonical record there. Mirrors
    /// [`Event::MessageSubscriptionOpening`]: only emitted when the canonical
    /// subscription lives off-partition, so a single-partition log never
    /// produces it and stays byte-identical.
    ///
    /// `delivery_key` is the stable delivery identity (#1497) a host re-delivery
    /// carries with the routed close: minted on this (the instance) partition,
    /// it lets the message partition record a **close tombstone** so an
    /// `OpenMessageSubscription` that arrives *after* its close (an instance
    /// cancelled while the open was still backing off) is rejected instead of
    /// resurrecting a live subscription for the dead instance. `0` on a
    /// pre-#1497 record: no tombstone (the legacy path).
    MessageSubscriptionClosing {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
    },
    /// The resume handshake (#1949): a SUSPENDED instance with a
    /// cross-partition subscription was resumed, so the message partition
    /// (`subscription_partition(correlation_key)`) is told to re-offer that
    /// subscription its buffered backlog — the ack-paced delivery it
    /// deferred while the instance was suspended. The host routes it as a
    /// [`crate::Command::ResumeMessageSubscription`]. Idempotent on the
    /// receiver (it only re-offers an open, non-awaiting subscription).
    /// State-neutral on the emitting (instance) partition.
    MessageSubscriptionResuming {
        subscription_key: Key,
        instance_key: Key,
        correlation_key: String,
        /// The `Opening` record's [`crate::state::MessageSubscription::resume_generation`]
        /// this handshake answers, echoed back through the confirmation so a
        /// delayed confirmation of an OLDER resume never clears a newer
        /// obligation. Additive.
        #[cfg_attr(feature = "serde", serde(default))]
        generation: u64,
    },
    /// The message partition processed a resume handshake (#1949): from
    /// here on its own durable state (`awaiting_ack` of any re-offered
    /// delivery) drives the subscription, so the instance partition can stop
    /// re-driving the handshake even when the backlog turned out empty. The
    /// host routes it as a
    /// [`crate::Command::ConfirmMessageSubscriptionResume`] to the instance's
    /// partition. State-neutral here; `partition` is this (message) partition:
    /// the subscription and instance keys are foreign.
    MessageSubscriptionResumed {
        subscription_key: Key,
        instance_key: Key,
        partition: u64,
        /// The handshake's generation, echoed (see
        /// [`Event::MessageSubscriptionResuming`]). Additive.
        #[cfg_attr(feature = "serde", serde(default))]
        generation: u64,
    },
    /// The instance partition received the message partition's
    /// [`Event::MessageSubscriptionResumed`] confirmation (#1949): clears the
    /// `Opening` record's `resume_pending`, ending the handshake re-drive.
    MessageSubscriptionResumeConfirmed {
        subscription_key: Key,
        instance_key: Key,
        /// The confirmed generation; it matched the record's current one.
        /// Additive.
        #[cfg_attr(feature = "serde", serde(default))]
        generation: u64,
    },
    /// The message partition processed an ack of `delivery_key` (#1949) —
    /// applied it, or found the delivery no longer awaited (a duplicate or
    /// stale ack): either way no re-drive of that delivery can change its
    /// reservation any more. The host routes it as a
    /// [`crate::Command::SettleMessageCorrelationReceipt`] to the instance's
    /// partition (`partition_of(subscription_key)`), which unpins the
    /// delivery's receipt. State-neutral here; `partition` is this (message)
    /// partition: the subscription key is foreign.
    MessageCorrelationSettled {
        subscription_key: Key,
        delivery_key: Key,
        partition: u64,
    },
    /// The instance partition received [`Event::MessageCorrelationSettled`]
    /// (#1949): the delivery's receipt is unpinned and its ledger entry
    /// becomes an ordinary cap-bounded dedup entry.
    MessageCorrelationReceiptSettled {
        subscription_key: Key,
        delivery_key: Key,
    },
    /// A replicated envelope enabled remote buffered delivery on this
    /// partition for the first time (#1949). Recorded in the journal so event
    /// replay (not only a snapshot) restores the one-way activation latch:
    /// a replica recovered from the journal then ignores a later disabled
    /// envelope exactly like one that applied the enabling envelope live.
    /// Partition-level; names no instance.
    RemoteBufferedDeliveryActivated { partition: u64 },
    /// The event-based gateway runtime of #2004 (`event-gateway-runtime-v1`)
    /// was activated on this partition: from this log position on, an
    /// event-based gateway waits on gateway-owned subscriptions and its
    /// trigger activates only the winning catch. Before it, the legacy #1676
    /// race path (arm every catch, withdraw the losing siblings) runs, so a
    /// journal without this record replays under legacy semantics. Recorded
    /// once, when the replicated envelope first enables the capability and no
    /// legacy race state is left. Partition-level; names no instance.
    EventGatewayRuntimeActivated { partition: u64 },
    /// A replicated envelope enabled `message-correlation-ack-v2` on this
    /// partition for the first time (#2037): every cluster member advertises
    /// v2, so from here on the ack output carries the resolved consumption
    /// element. One-way latch, recorded in the journal so replay restores it.
    /// Partition-level; names no instance.
    MessageCorrelationAckV2Activated { partition: u64 },
    /// A process-level message start subscription was opened at deploy time: a
    /// later [`Event::MessagePublished`] with a matching `message_name` creates a
    /// new instance of `process_id`. Carried on the log so replay reconstructs
    /// the subscription.
    MessageStartSubscriptionCreated {
        process_definition_key: Key,
        process_id: String,
        message_name: String,
        start_element_id: ElementId,
        /// The MCP tool name declared on the start event via the
        /// `io.camunda.tool:name` zeebe:property (Camunda 8.10 MCP gateway,
        /// #1499), when present. Additive: `None` for events serialized before
        /// this field existed.
        #[cfg_attr(feature = "serde", serde(default))]
        tool_name: Option<String>,
        /// The start event's `zeebe:properties` subset whose keys carry the
        /// `io.camunda.tool:` prefix (`:purpose`, `:input_N_*`, …), projected so
        /// the `/mcp/processes` server and the REST `toolProperties` field can
        /// assemble the tool description/input schema without re-parsing the
        /// BPMN XML. Empty when the start event declares no tool properties and
        /// for events serialized before this field existed.
        #[cfg_attr(feature = "serde", serde(default))]
        tool_properties: std::collections::BTreeMap<String, String>,
    },
    /// A process-level timer start event was armed at deploy time, first due at
    /// `due_at`. When it fires a new instance of `process_id` is created.
    ProcessStartTimerArmed {
        timer_key: Key,
        process_definition_key: Key,
        process_id: String,
        start_element_id: ElementId,
        due_at: u64,
        interval_millis: u64,
        repeating: bool,
        /// Remaining fires of a bounded cycle (`Rn/…`) INCLUDING this one;
        /// the timer is not re-armed after the fire that consumes the last
        /// (#1596). `None` for an unbounded cycle, a one-shot, and records
        /// serialized before this field existed.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        repetitions: Option<u32>,
    },
    /// A process-level timer start event fired (a `ProcessInstanceCreated` and
    /// the new instance's flow events follow). `next_due_at` is `Some` for a
    /// cycle (the timer re-arms for that instant) and `None` for a one-shot (it
    /// is retained, never to fire again).
    ProcessStartTimerFired {
        timer_key: Key,
        next_due_at: Option<u64>,
    },
    /// A start event (message-start or timer-start, both hosted on the deploy
    /// partition) fired but, in a multi-partition cluster, the new instance is
    /// placed on `target_partition` instead of being created locally — so
    /// start-triggered instances spread across the cluster rather than piling
    /// onto partition 0. Carries the full creation payload; the host routes a
    /// [`crate::Command::DispatchStartInstance`] to `target_partition`, which
    /// mints the instance in its own namespace. Only emitted when
    /// `num_partitions > 1` and the chosen target is not this partition, so a
    /// single-partition log never produces it and stays byte-identical.
    StartInstanceDispatched {
        process_id: String,
        start_element_id: ElementId,
        variables: BTreeMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        tags: Vec<String>,
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
        target_partition: u64,
        /// Set when a message start event fired (#1300): the target records the
        /// correlation once it mints the instance. `None` for timer starts.
        #[cfg_attr(feature = "serde", serde(default))]
        message_start: Option<MessageStartCorrelation>,
        /// The stable delivery identity (#1497) a host re-delivery carries with
        /// the routed `DispatchStartInstance`: minted on this (the deploy)
        /// partition, it lets the target partition deduplicate a re-delivered
        /// dispatch after a lost acknowledgement — the guard that keeps a retry
        /// from minting a SECOND process instance. `0` on a pre-#1497 record:
        /// no dedup (the legacy path).
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
    },

    /// An engine-native AgentInstance was created (Camunda `AgentInstanceIntent.CREATED`,
    /// stable/8.10). Emitted when an [`crate::model::ElementKind::AgentTask`]
    /// element activates: the engine mints a dedicated `agent_instance_key`,
    /// links it to the activating `element_instance_key`, and records the full
    /// instance in status `INITIALIZING`. The applier inserts it into the owning
    /// process instance's `agent_instances` map. Carries the whole record value
    /// so state rebuilds by replay.
    AgentInstanceCreated {
        instance_key: Key,
        agent_instance: crate::agent::AgentInstance,
    },

    /// An engine-native AgentInstance was updated (Camunda
    /// `AgentInstanceIntent.UPDATED`, stable/8.10). Emitted by the UPDATE
    /// processor after it validated ownership, advanced the status to one of the
    /// *active* states, accumulated the metric deltas (within the configured
    /// limits) and replaced the tool set. The associated history batch is
    /// carried by separate `AgentHistoryCreated`/`AgentHistoryCommitted` events.
    /// Carries the whole post-update record so state rebuilds by replay (the
    /// applier upserts it).
    AgentInstanceUpdated {
        instance_key: Key,
        agent_instance: crate::agent::AgentInstance,
    },

    /// An engine-native AgentInstance was completed (Camunda
    /// `AgentInstanceIntent.COMPLETED`, stable/8.10). Emitted by the COMPLETE
    /// processor: the record moves to the terminal `COMPLETED` status (the only
    /// path to it). Carries the whole post-completion record so state rebuilds
    /// by replay (the applier upserts it).
    AgentInstanceCompleted {
        instance_key: Key,
        agent_instance: crate::agent::AgentInstance,
    },

    /// One AgentHistory turn was appended to an agent instance's append-only
    /// turn log (Camunda `AgentHistoryIntent.CREATED`, stable/8.10). Emitted
    /// once per turn by the batch-append behavior; the record is materialised
    /// with a monotonic `agent_history_key` and `commit_status` PENDING. The
    /// applier inserts it into the owning process instance's `agent_history`
    /// map (keyed by `agent_instance_key`), ordered by `(loop_iteration,
    /// produced_at, agent_history_key)`.
    AgentHistoryCreated {
        /// The owning process instance key (locates the `agent_history` store).
        instance_key: Key,
        /// The materialised, PENDING history record.
        record: crate::agent::AgentHistoryRecord,
    },

    /// The pending AgentHistory turns of an agent instance were committed
    /// (Camunda `AgentHistoryIntent.COMMITTED`, stable/8.10): each listed turn
    /// moves PENDING -> COMMITTED. Committed turns are immutable.
    AgentHistoryCommitted {
        /// The owning process instance key.
        instance_key: Key,
        /// The agent instance whose pending turns were committed.
        agent_instance_key: Key,
        /// The keys of the turns that transitioned to COMMITTED.
        agent_history_keys: Vec<Key>,
    },

    /// The pending AgentHistory turns of an agent instance were discarded
    /// (Camunda `AgentHistoryIntent.DISCARDED`, stable/8.10): each listed turn
    /// moves PENDING -> DISCARDED. Discarded turns are immutable.
    AgentHistoryDiscarded {
        /// The owning process instance key.
        instance_key: Key,
        /// The agent instance whose pending turns were discarded.
        agent_instance_key: Key,
        /// The keys of the turns that transitioned to DISCARDED.
        agent_history_keys: Vec<Key>,
    },

    /// A submitted AgentHistory turn was detected as an idempotent retry of an
    /// already-recorded turn (same `historyItemId`) for the agent instance and
    /// was therefore **not** materialised into a new record (Camunda 8.10
    /// AgentHistory dedup, slice S2). No new AGENT_HISTORY record is created —
    /// this event only records the dedup outcome so the API can echo back
    /// `isDuplicate=true` with the original turn's `agent_history_key`. It is a
    /// state and read-model no-op (the append-only log is left untouched).
    AgentHistoryDeduplicated {
        /// The owning process instance key.
        instance_key: Key,
        /// The agent instance the duplicate turn targeted.
        agent_instance_key: Key,
        /// The `historyItemId` that matched an already-recorded turn.
        history_item_id: String,
        /// The `agent_history_key` of the original (already-recorded) turn the
        /// duplicate resolves to.
        original_agent_history_key: Key,
    },

    /// An instance's off-heap variable payload was quarantined as lost (#1574).
    /// Emitted by the stamped [`crate::Command::QuarantineOffheapOrphan`]
    /// alongside its `IncidentRaised`, its reducer records the instance in
    /// [`crate::state::State::variables_lost_quarantined`], the replicated
    /// dedup tombstone the quarantine idempotency guard reads (lifted when
    /// `incident_key` is resolved). It carries **no** variable payload
    /// and mutates no instance state: the payload-carrying repair was dropped
    /// because a leader-snapshotted payload is not safe under Raft ordering
    /// (it can roll back an intervening `SetVariables`). Deterministic
    /// apply-time repair is the #1447 follow-up.
    VariablesLostQuarantined {
        instance_key: Key,
        /// The `VariablesLost` incident this quarantine raised: the loss
        /// episode. Resolving exactly this incident lifts the tombstone, keyed
        /// on the replicated `IncidentResolved.incident_key` so the lift does
        /// not depend on whether the incident record is resident.
        incident_key: Key,
        /// The replicated loss-episode id the quarantine's stamp carried
        /// (#1587), recorded into the tombstone so a delayed duplicate of the
        /// SAME episode (applying after the incident was resolved and the
        /// tombstone lifted) is a deterministic no-op instead of a fresh
        /// incident. `0` for a frame written before this field existed
        /// (episode-blind, pre-#1587 behaviour). Additive: `serde(default)`.
        #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "is_zero_key"))]
        episode: Key,
    },

    /// A stamped orphan-quarantine proposal could not apply because its
    /// loss-episode id no longer matches the incident key a fresh proposal
    /// would mint (#1587 review): unrelated key-minting work advanced the
    /// replicated allocator between the leader stamping the report and the
    /// proposal committing. The report itself is still pending in the leader's
    /// journal backlog, so this event is the **replicated** signal every
    /// replica (and the leader's journal, which watches applied events)
    /// observes identically: the leader re-stamps the still-pending report at
    /// its now-current allocator position and re-proposes it, instead of
    /// silently clearing the pending report without raising `VariablesLost`.
    /// Its only state change records the report in
    /// `State::pending_orphan_supersedes` (#1608) until a
    /// `VariablesLostQuarantined` or terminal event settles it, so the pending
    /// report survives snapshots and adoption. Additive variant: no
    /// snapshot-format bump.
    OrphanStampSuperseded {
        instance_key: Key,
        /// The superseded loss-episode id (the stamp's `episode`), recorded so
        /// the journal can correlate the reissue with the report it parked.
        episode: Key,
        /// The off-heap tier the pending report was detected against (#1608).
        /// The supersede is the DURABLE record that a loss report is pending,
        /// so recovery and snapshot-adoption rebuild the in-memory backlog
        /// from the surviving event history — a crash or failover between the
        /// supersede and the re-stamped retry otherwise loses the report and
        /// `VariablesLost` is never raised for a dormant lost instance.
        /// `None` for a frame written before this field existed: the rebuild
        /// then derives the tier at re-stamp time from the recovered routing
        /// state. Additive: `serde(default)`, no snapshot-format bump.
        ///
        /// Omitted when `None` so a legacy frame re-serializes byte-identically
        /// (the chain digest folds serialized events).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        kind: Option<crate::command::OrphanKind>,
    },
}

impl Event {
    /// Whether this event is one of the loss-lifecycle variants the #1608
    /// settlement backfill folds (`OrphanStampSuperseded`,
    /// `VariablesLostQuarantined`, `ProcessInstanceCompleted`,
    /// `ProcessInstanceTerminated`). The ONE canonical filter, shared by the
    /// backfill fold and the storage readers that feed it (so retained-history
    /// reads keep only these, not every archived payload).
    pub fn is_loss_lifecycle(&self) -> bool {
        matches!(
            self,
            Event::OrphanStampSuperseded { .. }
                | Event::VariablesLostQuarantined { .. }
                | Event::ProcessInstanceCompleted { .. }
                | Event::ProcessInstanceTerminated { .. }
        )
    }

    /// The process-instance key of a **terminal** transition (completed or
    /// terminated), if this event is one. Used by the follower-replica apply
    /// path to reclaim the hot-state shell of an instance the moment it reaches
    /// a terminal state — a replica has no read-model exporter to drive that
    /// eviction, so without this its terminal shells would accumulate unbounded.
    pub fn terminal_instance_key(&self) -> Option<Key> {
        match self {
            Event::ProcessInstanceCompleted { instance_key }
            | Event::ProcessInstanceTerminated { instance_key } => Some(*instance_key),
            _ => None,
        }
    }

    /// The process-instance key this event relates to, if any.
    ///
    /// Used by the engine to decide which instances to check for completion
    /// after a command settles.
    pub fn instance_key(&self) -> Option<Key> {
        match self {
            Event::ProcessInstanceCreated { instance_key, .. }
            | Event::VariablesUpdated { instance_key, .. }
            | Event::ScopedVariablesUpdated { instance_key, .. }
            | Event::VariableScopeCreated { instance_key, .. }
            | Event::VariableScopeDestroyed { instance_key, .. }
            | Event::EventSubProcessPayloadHeld { instance_key, .. }
            | Event::ElementActivating { instance_key, .. }
            | Event::ElementActivated { instance_key, .. }
            | Event::ElementCompleting { instance_key, .. }
            | Event::ElementCompleted { instance_key, .. }
            | Event::ElementTerminating { instance_key, .. }
            | Event::ElementTerminated { instance_key, .. }
            | Event::GatewayRouteSelected { instance_key, .. }
            | Event::EventGatewaySubscriptionsOpened { instance_key, .. }
            | Event::EventGatewayTriggered { instance_key, .. }
            | Event::SequenceFlowTaken { instance_key, .. }
            | Event::ParallelJoinOpened { instance_key, .. }
            | Event::ParallelJoinTokenArrived { instance_key, .. }
            | Event::ParallelJoinReset { instance_key, .. }
            | Event::ParallelJoinFired { instance_key, .. }
            | Event::JobCreated { instance_key, .. }
            | Event::ExecutionListenerJobCreated { instance_key, .. }
            | Event::JobActivated { instance_key, .. }
            | Event::JobLockExpired { instance_key, .. }
            | Event::JobFailed { instance_key, .. }
            | Event::JobErrorThrown { instance_key, .. }
            | Event::JobCompleted { instance_key, .. }
            | Event::JobSettlementRecorded { instance_key, .. }
            | Event::JobRetriesUpdated { instance_key, .. }
            | Event::JobTimeoutUpdated { instance_key, .. }
            | Event::UserTaskCreated { instance_key, .. }
            | Event::UserTaskAssigned { instance_key, .. }
            | Event::UserTaskUpdated { instance_key, .. }
            | Event::UserTaskCompleted { instance_key, .. }
            | Event::UserTaskCanceled { instance_key, .. }
            | Event::TaskListenerJobCreated { instance_key, .. }
            | Event::UserTaskTransitionDeferred { instance_key, .. }
            | Event::UserTaskCorrectionsApplied { instance_key, .. }
            | Event::UserTaskTransitionResolved { instance_key, .. }
            | Event::IncidentRaised { instance_key, .. }
            | Event::IncidentResolved { instance_key, .. }
            | Event::TimerCreated { instance_key, .. }
            | Event::TimerTriggered { instance_key, .. }
            | Event::TimerCanceled { instance_key, .. }
            | Event::JobCanceled { instance_key, .. }
            | Event::MessageSubscriptionCreated { instance_key, .. }
            | Event::MessageSubscriptionOpening { instance_key, .. }
            | Event::MessageCorrelated { instance_key, .. }
            | Event::MessageStartEventCorrelated { instance_key, .. }
            | Event::RemoteMessageCorrelation { instance_key, .. }
            | Event::MessageSubscriptionCanceled { instance_key, .. }
            | Event::SignalSubscriptionCreated { instance_key, .. }
            | Event::SignalCorrelated { instance_key, .. }
            | Event::SignalSubscriptionCanceled { instance_key, .. }
            | Event::ConditionalSubscriptionCreated { instance_key, .. }
            | Event::ConditionalTriggered { instance_key, .. }
            | Event::ConditionalSubscriptionCanceled { instance_key, .. }
            | Event::CompensationSubscriptionCreated { instance_key, .. }
            | Event::CompensationTriggered { instance_key, .. }
            | Event::CompensationHandlerCompleted { instance_key, .. }
            | Event::CompensationScopeSubscriptionCompleted { instance_key, .. }
            | Event::ScopedCompensationCleared { instance_key, .. }
            | Event::MultiInstanceActivated { instance_key, .. }
            | Event::MultiInstanceCompletionConditionFulfilled { instance_key, .. }
            | Event::MultiInstanceChildActivated { instance_key, .. }
            | Event::MultiInstanceChildCompleted { instance_key, .. }
            | Event::MultiInstanceCompleted { instance_key, .. }
            | Event::AdHocActivated { instance_key, .. }
            | Event::AdHocToolActivated { instance_key, .. }
            | Event::AdHocToolCompleted { instance_key, .. }
            | Event::AdHocIterated { instance_key, .. }
            | Event::AdHocCompletionConditionFulfilled { instance_key, .. }
            | Event::AdHocCompleted { instance_key, .. }
            | Event::MessageSubscriptionClosing { instance_key, .. }
            | Event::MessageCorrelationAcknowledging { instance_key, .. }
            | Event::MessageSubscriptionResuming { instance_key, .. }
            | Event::MessageSubscriptionResumeConfirmed { instance_key, .. }
            | Event::VariablesLostQuarantined { instance_key, .. }
            | Event::OrphanStampSuperseded { instance_key, .. }
            | Event::ProcessInstanceCompleted { instance_key }
            | Event::ProcessInstanceTerminated { instance_key }
            | Event::ProcessInstanceHistoryDeleted { instance_key, .. } => Some(*instance_key),
            // A standalone decision evaluation (and its deletion) belongs to no
            // process instance: `0` is the "none" sentinel (Zeebe's `-1`).
            Event::DecisionEvaluated { instance_key, .. }
            | Event::DecisionInstanceDeleted { instance_key, .. } => {
                (*instance_key != 0).then_some(*instance_key)
            }
            Event::ProcessInstanceTerminating { instance_key } => Some(*instance_key),
            Event::ProcessInstanceSuspended { instance_key, .. } => Some(*instance_key),
            Event::ProcessInstanceResumed { instance_key } => Some(*instance_key),
            Event::ProcessInstanceBusinessIdAssigned { instance_key, .. } => Some(*instance_key),
            Event::ProcessInstanceMigrated { instance_key, .. } => Some(*instance_key),
            Event::ProcessInstanceModified { instance_key, .. } => Some(*instance_key),
            Event::AgentInstanceCreated { instance_key, .. } => Some(*instance_key),
            Event::AgentInstanceUpdated { instance_key, .. }
            | Event::AgentInstanceCompleted { instance_key, .. } => Some(*instance_key),
            Event::AgentHistoryCreated { instance_key, .. }
            | Event::AgentHistoryCommitted { instance_key, .. }
            | Event::AgentHistoryDiscarded { instance_key, .. }
            | Event::AgentHistoryDeduplicated { instance_key, .. } => Some(*instance_key),
            Event::ProcessDeployed { .. }
            | Event::DecisionRequirementsDeployed { .. }
            | Event::DecisionDeployed { .. }
            | Event::FormDeployed { .. }
            | Event::GenericResourceDeployed { .. }
            | Event::DeploymentCreated { .. }
            | Event::MessagePublished { .. }
            // Buffer bookkeeping (#1874) lives on the message partition; the
            // correlation record that follows names the instance.
            | Event::MessageBuffered { .. }
            | Event::MessageExpired { .. }
            | Event::BufferedMessageCorrelated { .. }
            | Event::MessageCorrelationAcknowledged { .. }
            | Event::MessageSubscriptionResumed { .. }
            | Event::MessageCorrelationSettled { .. }
            | Event::MessageCorrelationReceiptSettled { .. }
            | Event::RemoteBufferedDeliveryActivated { .. }
            | Event::EventGatewayRuntimeActivated { .. }
            | Event::MessageCorrelationAckV2Activated { .. }
            | Event::SignalBroadcast { .. }
            | Event::MessageStartSubscriptionCreated { .. }
            | Event::ProcessStartTimerArmed { .. }
            | Event::ProcessStartTimerFired { .. }
            | Event::RaftEntryApplied { .. }
            | Event::LenientCompletionSet { .. }
            | Event::RoutedDeliveryRecorded { .. }
            | Event::ClusterVariableCreated { .. }
            | Event::ClusterVariableUpdated { .. }
            | Event::ClusterVariableDeleted { .. }
            | Event::StartInstanceDispatched { .. } => None,
        }
    }

    /// `true` for the cluster-variable lifecycle events (#1657): keyless and
    /// partition-agnostic, replicated to EVERY partition by the fan-out. Any
    /// recovery path reading an untagged (legacy) journal must broadcast them
    /// to every partition rather than route them by [`Event::owning_partition`]
    /// (which assigns the keyless record to partition 0). The single canonical
    /// predicate shared by the legacy demux and segmented recovery.
    pub fn is_cluster_variable_lifecycle(&self) -> bool {
        matches!(
            self,
            Event::ClusterVariableCreated { .. }
                | Event::ClusterVariableUpdated { .. }
                | Event::ClusterVariableDeleted { .. }
        )
    }

    /// The decision evaluation an [`Event::DecisionEvaluated`] records — Zeebe's
    /// `decisionEvaluationKey`, which every decision-instance row of the
    /// evaluation shares — or `None` for any other event.
    ///
    /// This is the single canonical reading of the key (issue #1292): records
    /// written before the key was minted carry `0`, and fall back to the
    /// `businessRuleTask`'s `element_instance_key`. Those journals record only
    /// successful evaluations — at most one per businessRuleTask activation —
    /// so it is unique per evaluation, and it lives on the same partition, so
    /// legacy rows get a stable, collision-free key that routes like a minted
    /// one.
    pub fn decision_evaluation_key(&self) -> Option<Key> {
        match self {
            Event::DecisionEvaluated {
                decision_evaluation_key,
                element_instance_key,
                ..
            } => Some(if *decision_evaluation_key != 0 {
                *decision_evaluation_key
            } else {
                *element_instance_key
            }),
            _ => None,
        }
    }

    /// For a decision-instance event (`DecisionEvaluated` /
    /// `DecisionInstanceDeleted`), the key whose partition owns the decision
    /// instance ([`crate::dmn::decision_instance_owner_key`]); `None` for every
    /// other event.
    pub fn decision_instance_owner_key(&self) -> Option<Key> {
        match self {
            Event::DecisionEvaluated { instance_key, .. } => {
                Some(crate::dmn::decision_instance_owner_key(
                    *instance_key,
                    self.decision_evaluation_key()?,
                ))
            }
            Event::DecisionInstanceDeleted {
                instance_key,
                decision_evaluation_key,
            } => Some(crate::dmn::decision_instance_owner_key(
                *instance_key,
                *decision_evaluation_key,
            )),
            _ => None,
        }
    }

    /// Whether this is a cross-partition follow-up the host must route to
    /// another partition as a command (the single canonical routable set):
    /// a subscription open / close, a remote correlation, its ack (#1874), or
    /// a start-instance dispatch.
    pub fn is_routable(&self) -> bool {
        matches!(
            self,
            Event::MessageSubscriptionOpening { .. }
                | Event::RemoteMessageCorrelation { .. }
                | Event::MessageCorrelationAcknowledging { .. }
                | Event::MessageSubscriptionResuming { .. }
                | Event::MessageSubscriptionResumed { .. }
                | Event::MessageCorrelationSettled { .. }
                | Event::MessageSubscriptionClosing { .. }
                | Event::StartInstanceDispatched { .. }
        )
    }

    /// The source-minted routed-delivery identity (#1497) this event carries,
    /// if it is one of the four routable source events
    /// (`MessageSubscriptionOpening` / `RemoteMessageCorrelation` /
    /// `MessageSubscriptionClosing` / `StartInstanceDispatched`) with a non-zero
    /// key. These keys are minted by THIS partition's allocator, so recovery
    /// must advance past them ([`Event::advance_allocator`]) or a restarted
    /// node re-issues an already-delivered identity and the recipient drops a
    /// genuinely new delivery as a duplicate.
    pub fn source_delivery_key(&self) -> Option<Key> {
        let k = match self {
            Event::MessageSubscriptionOpening { delivery_key, .. }
            | Event::RemoteMessageCorrelation { delivery_key, .. }
            | Event::MessageSubscriptionClosing { delivery_key, .. }
            | Event::StartInstanceDispatched { delivery_key, .. } => *delivery_key,
            _ => return None,
        };
        (k != 0).then_some(k)
    }

    /// Advance a partition's local key allocator (`next_local`) past every key
    /// this event proves `partition_id` already minted — the single canonical
    /// replay/recovery fold (#1497). Considers [`Event::max_key`] and,
    /// independently, [`Event::source_delivery_key`]: the latter is checked on
    /// its own because a routed event can also carry a higher FOREIGN key (e.g.
    /// a `RemoteMessageCorrelation` naming an instance on a higher partition),
    /// which would otherwise mask the local delivery key behind the
    /// `partition_of(max) == partition_id` guard.
    ///
    /// A non-minting deletion record ([`Event::ProcessInstanceHistoryDeleted`],
    /// [`Event::DecisionInstanceDeleted`]) only NAMES a key the caller supplied
    /// — possibly one this engine never minted (an evicted or absent key) — so
    /// it proves nothing about the allocator and must not advance it: the live
    /// apply leaves `next_local` unchanged, and replay must agree (#1948).
    pub fn advance_allocator(&self, partition_id: u64, next_local: &mut u64) {
        if self.is_non_minting_deletion() {
            return;
        }
        for k in [Some(self.max_key()), self.source_delivery_key()]
            .into_iter()
            .flatten()
        {
            if crate::state::partition_of(k) == partition_id {
                *next_local = (*next_local).max(crate::state::local_of(k));
            }
        }
    }

    /// Whether this record deletes caller-named history without minting any key
    /// (see [`Event::advance_allocator`]).
    pub fn is_non_minting_deletion(&self) -> bool {
        matches!(
            self,
            Event::ProcessInstanceHistoryDeleted { .. } | Event::DecisionInstanceDeleted { .. }
        )
    }

    /// Whether this is a partition-agnostic deployment event (a process, DRG,
    /// decision, form or generic resource definition). The single canonical
    /// predicate for the deployment broadcast set: the engine's
    /// `install_deployment*`, the journal's durable replicated copy, and every
    /// recovery demux (segmented hot tail, cold archive, legacy) must route
    /// through this so no resource kind is silently dropped on one path (#1500).
    pub fn is_deployment(&self) -> bool {
        matches!(
            self,
            Event::ProcessDeployed { .. }
                | Event::DecisionRequirementsDeployed { .. }
                | Event::DecisionDeployed { .. }
                | Event::FormDeployed { .. }
                | Event::GenericResourceDeployed { .. }
        )
    }

    /// The partition that owns this event when a shared (untagged) journal is
    /// split per partition on recovery. Normally the partition encoded in
    /// [`Event::max_key`]; a [`Event::RoutedDeliveryRecorded`] record instead
    /// belongs to its `recipient_partition`, because its only key was minted on
    /// the SOURCE partition (#1497), and a message-correlation record routes by
    /// its own keys, never its (possibly foreign, #1434) shared message key.
    /// Every key-derived split must go through this, never
    /// `partition_of(max_key())` directly.
    pub fn owning_partition(&self) -> u64 {
        match self {
            Event::RoutedDeliveryRecorded {
                recipient_partition,
                ..
            } => *recipient_partition,
            // Message-buffer records (#1874) belong to the buffering (message)
            // partition, recorded explicitly: the shared publication key
            // (#1434) and a buffered correlation's subscription / instance keys
            // may all be foreign.
            Event::MessageBuffered { partition, .. }
            | Event::MessageExpired { partition, .. }
            | Event::BufferedMessageCorrelated { partition, .. }
            | Event::MessageCorrelationAcknowledging { partition, .. }
            | Event::MessageCorrelationAcknowledged { partition, .. }
            | Event::MessageSubscriptionResumed { partition, .. }
            | Event::MessageCorrelationSettled { partition, .. }
            | Event::RemoteBufferedDeliveryActivated { partition }
            | Event::EventGatewayRuntimeActivated { partition }
            | Event::MessageCorrelationAckV2Activated { partition } => *partition,
            // A message key is a PUBLICATION identity shared by every partition
            // the publish fans out to (#1434): it may have been minted on another
            // partition, so it never identifies the record's owner. Route by the
            // record's own (locally-minted) keys instead.
            // A remote correlation is recorded on the MESSAGE partition, while
            // its subscription / instance / element-instance keys belong to the
            // (foreign) instance partition. Its own key is the locally-minted
            // `delivery_key` (#1497); a pre-#1497 record (`0`) predates the
            // shared fan-out key (#1434), so its `message_key` was local.
            Event::RemoteMessageCorrelation {
                delivery_key,
                message_key,
                ..
            } => crate::state::partition_of(if *delivery_key != 0 {
                *delivery_key
            } else {
                *message_key
            }),
            Event::MessageCorrelated {
                subscription_key,
                instance_key,
                element_instance_key,
                ..
            } => crate::state::partition_of(
                (*subscription_key)
                    .max(*instance_key)
                    .max(*element_instance_key),
            ),
            Event::MessageStartEventCorrelated { instance_key, .. } => {
                crate::state::partition_of(*instance_key)
            }
            _ => crate::state::partition_of(self.max_key()),
        }
    }

    /// The highest [`Key`] this event references in any field.
    ///
    /// Replay uses the maximum across the whole log to restore the engine's key
    /// generator past every key the original run assigned, so newly minted keys
    /// never collide with replayed ones. (Keys are only ever minted by the
    /// engine and stamped onto events, so the log is an exact record of them —
    /// including transient ones like completed element-instance keys that no
    /// longer appear in final state.)
    pub fn max_key(&self) -> Key {
        let mut m = self.instance_key().unwrap_or(0);
        match self {
            Event::ProcessDeployed {
                deployment_key,
                process_definition_key,
                ..
            } => m = m.max(*deployment_key).max(*process_definition_key),
            Event::DecisionRequirementsDeployed {
                deployment_key,
                decision_requirements_key,
                ..
            } => m = m.max(*deployment_key).max(*decision_requirements_key),
            Event::DecisionDeployed {
                deployment_key,
                decision_requirements_key,
                decision_key,
                ..
            } => {
                m = m
                    .max(*deployment_key)
                    .max(*decision_requirements_key)
                    .max(*decision_key)
            }
            Event::FormDeployed {
                deployment_key,
                form_key,
                ..
            } => m = m.max(*deployment_key).max(*form_key),
            Event::GenericResourceDeployed {
                deployment_key,
                resource_key,
                ..
            } => m = m.max(*deployment_key).max(*resource_key),
            Event::DecisionEvaluated {
                element_instance_key,
                decision_key,
                decision_evaluation_key,
                ..
            } => {
                m = m
                    .max(*element_instance_key)
                    .max(*decision_key)
                    .max(*decision_evaluation_key)
            }
            Event::DecisionInstanceDeleted {
                decision_evaluation_key,
                ..
            } => m = m.max(*decision_evaluation_key),
            Event::DeploymentCreated { deployment_key } => m = m.max(*deployment_key),
            Event::ElementActivating {
                element_instance_key,
                ..
            }
            | Event::ElementActivated {
                element_instance_key,
                ..
            }
            | Event::ElementCompleting {
                element_instance_key,
                ..
            }
            | Event::ElementCompleted {
                element_instance_key,
                ..
            }
            | Event::ElementTerminating {
                element_instance_key,
                ..
            }
            | Event::ElementTerminated {
                element_instance_key,
                ..
            }
            | Event::ParallelJoinOpened {
                element_instance_key,
                ..
            } => m = m.max(*element_instance_key),
            Event::EventGatewaySubscriptionsOpened {
                gateway_element_instance_key,
                ..
            } => m = m.max(*gateway_element_instance_key),
            // The catch key is MINTED here (#2004): if the catch's activation
            // never follows (scope terminated first), only this record
            // carries it, so replay must still advance the allocator past it.
            Event::EventGatewayTriggered {
                gateway_element_instance_key,
                catch_element_instance_key,
                ..
            } => {
                m = m
                    .max(*gateway_element_instance_key)
                    .max(*catch_element_instance_key)
            }
            Event::JobCreated {
                job_key,
                element_instance_key,
                ..
            } => m = m.max(*job_key).max(*element_instance_key),
            Event::ExecutionListenerJobCreated {
                job_key,
                element_instance_key,
                scope,
                ..
            } => m = m.max(*job_key).max(*element_instance_key).max(*scope),
            Event::TaskListenerJobCreated {
                job_key,
                element_instance_key,
                user_task_key,
                ..
            } => {
                m = m
                    .max(*job_key)
                    .max(*element_instance_key)
                    .max(*user_task_key)
            }
            Event::JobActivated {
                job_key,
                lease_token,
                ..
            } => {
                m = m.max(*job_key).max(
                    lease_token
                        .as_deref()
                        .map(crate::lease::issued_key)
                        .unwrap_or(0),
                );
            }
            // The settlement frame carries the lease token that settled the job
            // (#1181) — a key minted by the lease allocator at activation time.
            // Under leader-local activation the `JobActivated` event may never
            // have been replicated, so this frame can be the ONLY record of that
            // minted key in the log: advance the allocator past it, exactly as
            // `JobActivated` does, or a reactivation after replay re-mints the
            // same token and the job's next legitimate settlement is swallowed
            // as a replay of the old one.
            Event::JobSettlementRecorded {
                job_key,
                lease_token,
                ..
            } => {
                m = m.max(*job_key).max(crate::lease::issued_key(lease_token));
            }
            // The recorded key was minted on the SOURCE partition; folding it
            // in is harmless because the allocator fold is partition-scoped
            // (`partition_of(k) == partition_id`), so a foreign key never
            // advances this partition's counter.
            Event::RoutedDeliveryRecorded { delivery_key, .. } => {
                m = m.max(*delivery_key);
            }
            Event::JobLockExpired { job_key, .. }
            | Event::JobFailed { job_key, .. }
            | Event::JobErrorThrown { job_key, .. }
            | Event::JobCompleted { job_key, .. }
            | Event::JobCanceled { job_key, .. }
            | Event::JobRetriesUpdated { job_key, .. }
            | Event::JobTimeoutUpdated { job_key, .. } => m = m.max(*job_key),
            Event::IncidentRaised {
                incident_key,
                element_instance_key,
                job_key,
                ..
            } => {
                m = m.max(*incident_key).max(*element_instance_key);
                if let Some(j) = job_key {
                    m = m.max(*j);
                }
            }
            Event::IncidentResolved {
                incident_key,
                job_key,
                ..
            } => {
                m = m.max(*incident_key);
                if let Some(j) = job_key {
                    m = m.max(*j);
                }
            }
            Event::TimerCreated {
                timer_key,
                element_instance_key,
                ..
            } => m = m.max(*timer_key).max(*element_instance_key),
            Event::TimerTriggered {
                timer_key,
                element_instance_key,
                ..
            } => m = m.max(*timer_key).max(*element_instance_key),
            Event::TimerCanceled {
                timer_key,
                element_instance_key,
                ..
            } => m = m.max(*timer_key).max(*element_instance_key),
            Event::MessagePublished { message_key, .. }
            | Event::MessageBuffered { message_key, .. }
            | Event::MessageExpired { message_key, .. }
            | Event::BufferedMessageCorrelated { message_key, .. } => m = m.max(*message_key),
            Event::MessageCorrelationAcknowledging {
                subscription_key,
                message_key,
                ..
            }
            | Event::MessageCorrelationAcknowledged {
                subscription_key,
                message_key,
                ..
            } => m = m.max(*subscription_key).max(*message_key),
            Event::MessageSubscriptionResuming {
                subscription_key, ..
            }
            | Event::MessageSubscriptionResumeConfirmed {
                subscription_key, ..
            }
            | Event::MessageCorrelationSettled {
                subscription_key, ..
            }
            | Event::MessageCorrelationReceiptSettled {
                subscription_key, ..
            } => m = m.max(*subscription_key),
            Event::MessageSubscriptionResumed {
                subscription_key,
                instance_key,
                ..
            } => m = m.max(*subscription_key).max(*instance_key),
            Event::SignalBroadcast { signal_key, .. } => m = m.max(*signal_key),
            Event::SignalSubscriptionCreated {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::SignalSubscriptionCanceled {
                subscription_key,
                element_instance_key,
                ..
            } => m = m.max(*subscription_key).max(*element_instance_key),
            Event::ConditionalSubscriptionCreated {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::ConditionalTriggered {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::ConditionalSubscriptionCanceled {
                subscription_key,
                element_instance_key,
                ..
            } => m = m.max(*subscription_key).max(*element_instance_key),
            Event::CompensationSubscriptionCreated {
                element_instance_key,
                scope,
                ..
            } => m = m.max(*element_instance_key).max(*scope),
            Event::CompensationTriggered {
                throw_element_instance_key,
                scope,
                consumed,
                handler_instance_keys,
                scope_subscriptions,
                handler_scopes,
                scope_subscription_parents,
                ..
            } => {
                m = m.max(*throw_element_instance_key).max(*scope);
                for key in consumed
                    .iter()
                    .chain(handler_instance_keys)
                    .chain(scope_subscriptions)
                    .chain(handler_scopes)
                    .chain(scope_subscription_parents)
                {
                    m = m.max(*key);
                }
            }
            Event::CompensationHandlerCompleted {
                throw_element_instance_key,
                ..
            } => m = m.max(*throw_element_instance_key),
            Event::CompensationScopeSubscriptionCompleted {
                throw_element_instance_key,
                scope_subscription_key,
                ..
            } => {
                m = m
                    .max(*throw_element_instance_key)
                    .max(*scope_subscription_key)
            }
            Event::MultiInstanceActivated { body_key, .. }
            | Event::MultiInstanceCompletionConditionFulfilled { body_key, .. }
            | Event::MultiInstanceCompleted { body_key, .. } => m = m.max(*body_key),
            Event::MultiInstanceChildActivated {
                body_key,
                child_key,
                ..
            }
            | Event::MultiInstanceChildCompleted {
                body_key,
                child_key,
                ..
            } => m = m.max(*body_key).max(*child_key),
            Event::AdHocActivated { container_key, .. }
            | Event::AdHocIterated { container_key, .. }
            | Event::AdHocCompletionConditionFulfilled { container_key, .. }
            | Event::AdHocCompleted { container_key, .. } => m = m.max(*container_key),
            Event::AdHocToolActivated {
                container_key,
                child_key,
                ..
            }
            | Event::AdHocToolCompleted {
                container_key,
                child_key,
                ..
            } => m = m.max(*container_key).max(*child_key),
            Event::SignalCorrelated {
                subscription_key,
                signal_key,
                element_instance_key,
                ..
            } => {
                m = m
                    .max(*subscription_key)
                    .max(*signal_key)
                    .max(*element_instance_key)
            }
            Event::MessageSubscriptionCreated {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::MessageSubscriptionOpening {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::MessageSubscriptionCanceled {
                subscription_key,
                element_instance_key,
                ..
            }
            | Event::MessageSubscriptionClosing {
                subscription_key,
                element_instance_key,
                ..
            } => m = m.max(*subscription_key).max(*element_instance_key),
            Event::MessageCorrelated {
                subscription_key,
                message_key,
                element_instance_key,
                ..
            }
            | Event::RemoteMessageCorrelation {
                subscription_key,
                message_key,
                element_instance_key,
                ..
            } => {
                m = m
                    .max(*subscription_key)
                    .max(*message_key)
                    .max(*element_instance_key)
            }
            Event::MessageStartSubscriptionCreated {
                process_definition_key,
                ..
            } => m = m.max(*process_definition_key),
            Event::MessageStartEventCorrelated {
                message_key,
                instance_key,
                ..
            } => m = m.max(*message_key).max(*instance_key),
            Event::ProcessStartTimerArmed {
                timer_key,
                process_definition_key,
                ..
            } => m = m.max(*timer_key).max(*process_definition_key),
            Event::ProcessStartTimerFired { timer_key, .. } => m = m.max(*timer_key),
            Event::UserTaskCreated {
                user_task_key,
                element_instance_key,
                ..
            } => m = m.max(*user_task_key).max(*element_instance_key),
            Event::UserTaskAssigned { user_task_key, .. }
            | Event::UserTaskUpdated { user_task_key, .. }
            | Event::UserTaskCompleted { user_task_key, .. }
            | Event::UserTaskCanceled { user_task_key, .. }
            | Event::UserTaskTransitionDeferred { user_task_key, .. }
            | Event::UserTaskCorrectionsApplied { user_task_key, .. }
            | Event::UserTaskTransitionResolved { user_task_key, .. } => m = m.max(*user_task_key),
            Event::AgentInstanceCreated { agent_instance, .. }
            | Event::AgentInstanceUpdated { agent_instance, .. }
            | Event::AgentInstanceCompleted { agent_instance, .. } => {
                m = m
                    .max(agent_instance.agent_instance_key)
                    .max(agent_instance.element_instance_key)
            }
            Event::AgentHistoryCreated { record, .. } => {
                m = m
                    .max(record.agent_history_key)
                    .max(record.agent_instance_key)
                    .max(record.element_instance_key)
            }
            Event::AgentHistoryCommitted {
                agent_instance_key,
                agent_history_keys,
                ..
            }
            | Event::AgentHistoryDiscarded {
                agent_instance_key,
                agent_history_keys,
                ..
            } => {
                m = m.max(*agent_instance_key);
                if let Some(max_key) = agent_history_keys.iter().copied().max() {
                    m = m.max(max_key);
                }
            }
            Event::AgentHistoryDeduplicated {
                agent_instance_key,
                original_agent_history_key,
                ..
            } => {
                m = m.max(*agent_instance_key).max(*original_agent_history_key);
            }
            Event::ScopedCompensationCleared { scopes, .. } => {
                if let Some(max_scope) = scopes.iter().copied().max() {
                    m = m.max(max_scope);
                }
            }
            _ => {}
        }
        m
    }
}

/// A typed, FATAL failure to decode a persisted journal line back into an
/// [`Event`] under the current build.
///
/// Migration-by-replay (#1071) rebuilds the engine by replaying the event
/// journal under new code, which only works if every OLD record still
/// deserializes. Additive field changes are already rescued by the
/// `#[serde(default)]` forward-compat contract on [`Event`] (a missing field
/// decodes as `None`/`0`). What is **not** rescued is a record naming an
/// [`Event`] **variant this build does not know** — an event kind that was
/// renamed or removed, or a record from a newer/foreign journal format. Left to
/// a bare `serde_json::from_str::<Event>`, that is an anonymous, ambiguous
/// deserialize error indistinguishable from a torn/corrupt line, so a caller
/// cannot tell "the write was cut short" (safe to drop as a torn tail) from
/// "this is a real event from a format I cannot replay" (must NEVER be silently
/// dropped — incident #1065).
///
/// [`decode_event_json`] classifies the failure into this typed error so the
/// storage replay path can surface an [`EventDecodeError::UnknownVariant`] as an
/// explicit, operator-actionable rejection — routed to fail-closed (#1066) / the
/// replay-migrator (#1071), exactly like the snapshot loader's typed
/// `SnapshotLoadError`. When the journal reader wraps it in an
/// [`std::io::Error`], recover it via `io::Error::get_ref()` +
/// [`downcast_ref`](std::error::Error::downcast_ref).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventDecodeError {
    /// The record is well-formed JSON naming an externally-tagged variant that
    /// this build's [`Event`] enum does not define — a renamed/removed event
    /// kind, or a record from a newer/foreign journal format. This is a
    /// **breaking** frame change that a [`SNAPSHOT_FORMAT_VERSION`] bump (#1068)
    /// must gate and the migrator (#1071) must handle; it is NEVER a silent drop
    /// (not even for a torn tail — a complete, well-formed unknown record is a
    /// real cross-version frame, not an interrupted write).
    ///
    /// [`SNAPSHOT_FORMAT_VERSION`]: crate::SNAPSHOT_FORMAT_VERSION
    UnknownVariant {
        /// The offending variant name serde named as unknown — recovered from
        /// serde's `unknown variant \`X\`` diagnostic. This is *not* necessarily
        /// the record's top-level event tag: serde raises the same error for an
        /// unknown value of an externally-tagged enum nested inside a *known*
        /// event (e.g. `IncidentRaised.kind`), in which case this is that nested
        /// variant, not the event name. Falls back to the record's top-level
        /// object key, then to `"?"`, if the name cannot be extracted.
        variant: String,
        /// The underlying serde message, for operator diagnostics.
        detail: String,
    },
    /// The record could not be parsed into a known variant for a reason other
    /// than an unknown tag: a type mismatch on a field, malformed JSON, or a
    /// truncated/torn line from an interrupted write. Only this class is safe for
    /// a journal reader to tolerate as a torn trailing record.
    Malformed {
        /// The underlying serde message.
        detail: String,
    },
}

impl std::fmt::Display for EventDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventDecodeError::UnknownVariant { variant, detail } => write!(
                f,
                "unknown variant `{variant}` in journal record: this build cannot \
                 replay it (renamed/removed variant or a newer/foreign journal format). \
                 A breaking event-frame change requires a SNAPSHOT_FORMAT_VERSION bump \
                 (#1068) and replay-migrator handling (#1071); refusing to drop it \
                 silently (fail-closed). serde: {detail}"
            ),
            EventDecodeError::Malformed { detail } => {
                write!(f, "malformed journal record: {detail}")
            }
        }
    }
}

impl std::error::Error for EventDecodeError {}

/// Decode a single persisted journal line into an [`Event`] under the current
/// build, classifying any failure into a typed [`EventDecodeError`].
///
/// This is the replay-boundary decode discipline (#1070): use it instead of a
/// bare `serde_json::from_str::<Event>(line)` wherever a persisted journal is
/// read back for replay, so an unknown/removed event variant becomes an explicit
/// [`EventDecodeError::UnknownVariant`] rejection rather than an anonymous
/// deserialize error a caller might mistake for a torn tail and silently drop.
#[cfg(feature = "serde")]
pub fn decode_event_json(line: &str) -> Result<Event, EventDecodeError> {
    match serde_json::from_str::<Event>(line) {
        Ok(event) => Ok(event),
        Err(err) => {
            let detail = err.to_string();
            // serde emits "unknown variant `X`, expected one of ..." from
            // `serde::de::Error::unknown_variant` when an externally-tagged
            // enum's tag names no known variant. That phrase is part of the
            // serde data model (not json-specific), so it is stable to key on.
            // The offending enum need not be the top-level `Event`: the same
            // error arises for an unknown value of an externally-tagged enum
            // nested inside a *known* event (e.g. `IncidentRaised.kind`), so
            // recover the precise offending name from serde's own message first,
            // and only fall back to the record's top-level object key (then
            // `"?"`) if that fails — reporting the outer event name for a nested
            // failure would mislead the operator.
            if detail.contains("unknown variant") {
                let variant = unknown_variant_from_detail(&detail)
                    .or_else(|| event_tag_of(line))
                    .unwrap_or_else(|| "?".to_string());
                Err(EventDecodeError::UnknownVariant { variant, detail })
            } else {
                Err(EventDecodeError::Malformed { detail })
            }
        }
    }
}

/// One journaled record: an [`Event`] plus the per-record metadata Zeebe
/// carries on every record (#1556). Today that is the caller's
/// `operationReference` — the correlation id a client attaches to a command,
/// propagated **generically** to every record the command produces (Zeebe
/// parity). [`Event`] itself stays untouched: the reference lives once, on this
/// envelope, stamped at the engine's single command seam
/// ([`Engine::apply_command_records_at`](crate::Engine::apply_command_records_at)),
/// so no per-variant copy can drift.
///
/// **Wire form** (`serde` feature): a record WITH a reference serializes as the
/// envelope `{"event":{…},"operation_reference":N}`; a record WITHOUT one
/// serializes as the bare legacy [`Event`] (`{"VariantName":{…}}`). Every
/// pre-#1556 journal line is therefore already a valid record, and
/// [`decode_record_json`] reads both shapes — no `SNAPSHOT_FORMAT_VERSION`
/// bump. The `event` key cannot collide with an event tag (variants are
/// PascalCase).
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    /// The domain event.
    pub event: Event,
    /// The `operationReference` of the command that produced this record.
    pub operation_reference: Option<i64>,
}

impl Record {
    /// A record with no operation reference (the bare-event wire form).
    pub fn bare(event: Event) -> Self {
        Self {
            event,
            operation_reference: None,
        }
    }

    /// Stamps `operation_reference` onto every event of one command's output —
    /// the single, generic propagation seam (#1556).
    pub fn stamp_all(events: Vec<Event>, operation_reference: Option<i64>) -> Vec<Self> {
        events
            .into_iter()
            .map(|event| Self {
                event,
                operation_reference,
            })
            .collect()
    }
}

impl From<Event> for Record {
    fn from(event: Event) -> Self {
        Self::bare(event)
    }
}

/// Borrowed serialization view of a [`Record`] — the ONE implementation of the
/// record wire form, shared by `Record`'s `Serialize` and
/// [`encode_record_json`] (which journals without cloning the event).
#[cfg(feature = "serde")]
struct RecordView<'a> {
    event: &'a Event,
    operation_reference: Option<i64>,
}

#[cfg(feature = "serde")]
impl serde::Serialize for RecordView<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        match self.operation_reference {
            None => self.event.serialize(serializer),
            Some(reference) => {
                let mut s = serializer.serialize_struct("Record", 2)?;
                s.serialize_field("event", self.event)?;
                s.serialize_field("operation_reference", &reference)?;
                s.end()
            }
        }
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Record {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RecordView {
            event: &self.event,
            operation_reference: self.operation_reference,
        }
        .serialize(serializer)
    }
}

/// Encodes one journal line for `event` produced by a command carrying
/// `operation_reference` — the writer-side twin of [`decode_record_json`]
/// (#1556). Without a reference this is exactly the legacy bare-`Event` line.
#[cfg(feature = "serde")]
pub fn encode_record_json(
    event: &Event,
    operation_reference: Option<i64>,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&RecordView {
        event,
        operation_reference,
    })
}

/// Decodes one journal line into a [`Record`] — the replay-boundary reader for
/// the #1556 envelope. Accepts both the envelope and the legacy bare-[`Event`]
/// form (which decodes with `operation_reference: None`). The envelope is
/// recognised by the PARSED top-level `event` member, never by property order,
/// so a producer that reorders keys (`{"operation_reference":N,"event":…}`)
/// still decodes. The inner event is decoded through [`decode_event_json`], so
/// an unknown variant is still the typed [`EventDecodeError::UnknownVariant`]
/// rejection — never a silent drop.
///
/// Two disciplines keep this safe AND cheap on the hot recovery path:
///
/// * **Decode the event FIRST, validate the envelope metadata SECOND.** An
///   envelope whose `event` names a variant this build does not know must
///   surface the typed [`EventDecodeError::UnknownVariant`] even when the
///   surrounding metadata is also invalid (a bad `operation_reference` type or
///   an unrecognised extra field). Validating metadata first would return
///   [`EventDecodeError::Malformed`], which the journal reader
///   (`read_segment_with`) tolerates as a torn tail and DROPS — silently
///   discarding a real cross-version frame (#1065 class). Decoding the event
///   first guarantees the fail-closed `UnknownVariant` wins.
/// * **No double parse of the event payload.** The envelope probe borrows the
///   `event` member as a [`serde_json::value::RawValue`] and hands it straight
///   to [`decode_event_json`] — the payload is parsed exactly once, never
///   materialized into a [`serde_json::Value`] DOM and re-serialized. Bare
///   events (unreferenced commands) skip the probe's payload capture entirely.
#[cfg(feature = "serde")]
pub fn decode_record_json(line: &str) -> Result<Record, EventDecodeError> {
    // A borrowed probe over the raw line: the `event`/`operation_reference`
    // members are captured by reference as a `RawValue` (no DOM, no copy of the
    // payload). Any OTHER top-level key is collected into `extra` so an
    // unrecognised envelope field can be rejected AFTER the event is decoded.
    // `extra` is a `Value` map only because serde's `flatten` cannot deserialize
    // into a borrowed `RawValue`; it stays empty for every well-formed record,
    // so the hot path never builds a DOM.
    #[derive(serde::Deserialize)]
    struct Probe<'a> {
        #[serde(borrow)]
        event: Option<&'a serde_json::value::RawValue>,
        #[serde(borrow)]
        operation_reference: Option<&'a serde_json::value::RawValue>,
        #[serde(flatten)]
        extra: std::collections::BTreeMap<String, serde_json::Value>,
    }

    let probe: Probe<'_> = match serde_json::from_str(line) {
        Ok(probe) => probe,
        // Not an object the probe recognises. Either a legacy bare event (whose
        // top-level key is a PascalCase variant tag, collected into `extra` —
        // but then `event` is absent and we fall through below) or genuinely
        // malformed JSON. The bare-event decode classifies both precisely.
        // Probe failures (e.g. duplicate `operation_reference`/`event` keys,
        // which the derived probe rejects) must never mask the inner event's
        // typed `UnknownVariant`: this cold path re-parses as a DOM (duplicate
        // keys are accepted there) and decodes any `event` member FIRST.
        // Only genuinely unparseable JSON (a torn tail) stays `Malformed`.
        Err(e) => {
            let malformed = || EventDecodeError::Malformed {
                detail: e.to_string(),
            };
            // Walk EVERY top-level member (duplicates included — a DOM would
            // keep only the last) so each `event` occurrence is classified.
            return match top_level_members(line) {
                None => Err(malformed()),
                Some(members) => {
                    // Scan ALL `event` occurrences before failing: a malformed
                    // sibling must never short-circuit past an unknown one, so
                    // any `UnknownVariant` wins regardless of key order.
                    let mut saw_event = false;
                    for (key, raw) in &members {
                        if key == "event" {
                            saw_event = true;
                            if let Err(err @ EventDecodeError::UnknownVariant { .. }) =
                                decode_event_json(raw.get())
                            {
                                return Err(err);
                            }
                        }
                    }
                    if saw_event {
                        Err(malformed())
                    } else {
                        decode_event_json(line).map(Record::bare)
                    }
                }
            };
        }
    };

    let event_raw = match probe.event {
        Some(raw) => raw,
        // No `event` member: a legacy bare event (variant tags are PascalCase,
        // never the lowercase `event` key). Decode it directly.
        None => return decode_event_json(line).map(Record::bare),
    };

    // Decode the inner event FIRST (straight from the borrowed raw payload, no
    // re-serialization) so an unknown variant is the typed fail-closed
    // rejection even when the envelope metadata below is also invalid.
    let event = decode_event_json(event_raw.get())?;

    // Only now validate the envelope metadata. An unrecognised field or a bad
    // `operation_reference` is `Malformed` — but the event is already known to
    // be decodable, so this can never mask an `UnknownVariant`.
    if let Some(extra) = probe.extra.keys().next() {
        return Err(EventDecodeError::Malformed {
            detail: format!("unknown record envelope field `{extra}`"),
        });
    }
    let operation_reference = match probe.operation_reference {
        None => None,
        // `RawValue::get` returns the exact source slice, so an explicit null
        // may carry whitespace (`" null"`); trim before comparing.
        Some(raw) if raw.get().trim() == "null" => None,
        Some(raw) => Some(serde_json::from_str::<i64>(raw.get()).map_err(|e| {
            EventDecodeError::Malformed {
                detail: format!("operation_reference: {e}"),
            }
        })?),
    };
    Ok(Record {
        event,
        operation_reference,
    })
}

/// Every top-level `(key, raw value)` member of a JSON object IN SOURCE ORDER,
/// duplicates preserved (unlike a `Value` map, which keeps only the last).
/// `None` if `line` is not a parseable JSON object.
#[cfg(feature = "serde")]
fn top_level_members(line: &str) -> Option<Vec<(String, Box<serde_json::value::RawValue>)>> {
    struct Members(Vec<(String, Box<serde_json::value::RawValue>)>);
    impl<'de> serde::Deserialize<'de> for Members {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = Members;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> Result<Members, A::Error> {
                    let mut out = Vec::new();
                    while let Some(entry) = map.next_entry()? {
                        out.push(entry);
                    }
                    Ok(Members(out))
                }
            }
            d.deserialize_map(V)
        }
    }
    serde_json::from_str::<Members>(line).ok().map(|m| m.0)
}

/// Extract the offending variant name from serde's
/// `unknown variant \`X\`, expected one of ...` diagnostic — the token between
/// the first pair of backticks. Returns `None` if the message does not carry a
/// backtick-delimited name (so the caller can fall back to another source).
#[cfg(feature = "serde")]
fn unknown_variant_from_detail(detail: &str) -> Option<String> {
    let start = detail.find("unknown variant `")? + "unknown variant `".len();
    let rest = &detail[start..];
    let end = rest.find('`')?;
    Some(rest[..end].to_string())
}

/// Best-effort extraction of the externally-tagged variant name from a journal
/// record: the single top-level object key of `{"VariantName": { … }}`. Every
/// [`Event`] variant is a struct variant, so a valid record is always an object;
/// the string arm is a defensive fallback for a hypothetical unit variant.
#[cfg(feature = "serde")]
fn event_tag_of(line: &str) -> Option<String> {
    match serde_json::from_str::<serde_json::Value>(line).ok()? {
        serde_json::Value::Object(map) => map.into_iter().next().map(|(k, _)| k),
        serde_json::Value::String(s) => Some(s),
        _ => None,
    }
}

#[cfg(all(test, feature = "serde"))]
mod terminal_worker_serde_compat_tests {
    use super::Event;

    /// The `worker` field carried on `JobFailed` (#959) is the durable
    /// attribution contract, but it was added after journals already existed.
    /// A legacy `JobFailed` line — written before the field existed — has no
    /// `worker` key; without the `serde(default)` it would fail to deserialize
    /// and prevent the server from booting on replay. Guard the defect class:
    /// the legacy shape must still deserialize, defaulting `worker` to `None`.
    #[test]
    fn legacy_job_failed_without_worker_defaults_to_none() {
        let line = r#"{"JobFailed":{"job_key":7,"instance_key":1,"retries":0}}"#;
        let event: Event = serde_json::from_str(line).expect("legacy event deserializes");
        match event {
            Event::JobFailed { worker, .. } => assert!(worker.is_none()),
            other => panic!("expected JobFailed, got {other:?}"),
        }
    }

    /// Same compatibility contract for `JobErrorThrown` (#959): a legacy line
    /// without `worker` must replay with a defaulted `worker: None` rather than
    /// failing journal boot.
    #[test]
    fn legacy_job_error_thrown_without_worker_defaults_to_none() {
        let line = r#"{"JobErrorThrown":{"job_key":7,"instance_key":1,"error_code":"BOOM"}}"#;
        let event: Event = serde_json::from_str(line).expect("legacy event deserializes");
        match event {
            Event::JobErrorThrown { worker, .. } => assert!(worker.is_none()),
            other => panic!("expected JobErrorThrown, got {other:?}"),
        }
    }

    /// `JobCompleted.worker` (#1191) is the newest of the three terminal
    /// attributions and, unlike `JobFailed`/`JobErrorThrown`, its `serde(default)`
    /// contract lost its dedicated corpus witness when `event_corpus.v2.json` was
    /// refreshed to *include* the field. Guard the same defect class directly: a
    /// legacy `JobCompleted` line — written before the field existed — has no
    /// `worker` key and must still deserialize, defaulting `worker` to `None`,
    /// so a journal recorded before this PR still replays under the new binary.
    #[test]
    fn legacy_job_completed_without_worker_defaults_to_none() {
        let line = r#"{"JobCompleted":{"job_key":7,"instance_key":1}}"#;
        let event: Event = serde_json::from_str(line).expect("legacy event deserializes");
        match event {
            Event::JobCompleted { worker, .. } => assert!(worker.is_none()),
            other => panic!("expected JobCompleted, got {other:?}"),
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod event_decode_tests {
    use super::{decode_event_json, Event, EventDecodeError};

    /// A well-formed record naming a variant this build does not define is a
    /// typed `UnknownVariant` rejection carrying the offending tag — the
    /// explicit, operator-actionable outcome #1070 requires (never a silent
    /// drop, never an anonymous error).
    #[test]
    fn unknown_variant_classifies_as_typed_rejection() {
        let line = r#"{"SomeRemovedEvent":{"instance_key":1}}"#;
        match decode_event_json(line) {
            Err(EventDecodeError::UnknownVariant { variant, .. }) => {
                assert_eq!(variant, "SomeRemovedEvent")
            }
            other => panic!("expected UnknownVariant, got {other:?}"),
        }
    }

    /// A known variant with a malformed payload (or a torn line) is `Malformed`,
    /// kept distinct from `UnknownVariant` so a journal reader can tolerate a
    /// torn tail without ever tolerating a real unknown frame.
    #[test]
    fn malformed_payload_classifies_as_malformed() {
        let bad_type = r#"{"DeploymentCreated":{"deployment_key":"nope"}}"#;
        assert!(matches!(
            decode_event_json(bad_type),
            Err(EventDecodeError::Malformed { .. })
        ));
        let torn = r#"{"DeploymentCreated":{"deployment_ke"#;
        assert!(matches!(
            decode_event_json(torn),
            Err(EventDecodeError::Malformed { .. })
        ));
    }

    /// A pre-#1292 `DecisionEvaluated` record (no `decision_evaluation_key`,
    /// no `failure`, evaluated decisions without `decision_key` /
    /// `decision_version`) still decodes under this build — the additive-field
    /// replay rule — and its evaluation is identified by the legacy key, its
    /// element instance key, so an old journal replays to the same rows.
    #[test]
    fn legacy_decision_evaluated_record_decodes_with_the_element_instance_key() {
        let line = r#"{"DecisionEvaluated":{"decision_id":"root","decision_key":34,"decision_output":{"Str":"b"},"element_id":"brt","element_instance_key":39,"evaluated_at":1700000000012,"evaluated_decisions":[{"decision_id":"base","decision_name":"Base","decision_output":{"Str":"b"},"decision_type":"LiteralExpression","evaluated_inputs":[],"matched_rules":[]}],"instance_key":37}}"#;
        let event = decode_event_json(line).expect("legacy record decodes");
        let Event::DecisionEvaluated {
            decision_evaluation_key,
            failure,
            evaluated_decisions,
            ..
        } = &event
        else {
            panic!("expected DecisionEvaluated, got {event:?}")
        };
        assert_eq!(*decision_evaluation_key, 0);
        assert!(failure.is_none());
        assert_eq!(
            (
                evaluated_decisions[0].decision_key,
                evaluated_decisions[0].decision_version
            ),
            (0, 0)
        );
        assert_eq!(event.decision_evaluation_key(), Some(39));
        assert_eq!(event.instance_key(), Some(37));
    }

    /// A valid record round-trips through the typed decoder unchanged.
    #[test]
    fn valid_record_decodes_via_typed_decoder() {
        let line = r#"{"DeploymentCreated":{"deployment_key":9}}"#;
        assert_eq!(
            decode_event_json(line).expect("valid record decodes"),
            Event::DeploymentCreated { deployment_key: 9 }
        );
    }

    /// Regression (defect class): serde raises the *same* "unknown variant"
    /// error for an unknown value of an externally-tagged enum nested inside a
    /// *known* event (here `IncidentRaised.kind`) as it does for an unknown
    /// top-level `Event` tag. The reported `variant` must be the offending
    /// nested name (`SomeFutureIncidentKind`), recovered from serde's message —
    /// NOT the outer event tag (`IncidentRaised`), which would mislead the
    /// operator into thinking the whole event type is gone.
    #[test]
    fn nested_unknown_variant_reports_inner_name_not_event_tag() {
        let line = r#"{"IncidentRaised":{"incident_key":1,"instance_key":2,"element_instance_key":3,"element_id":"task","kind":"SomeFutureIncidentKind","reason":"x","job_key":null,"created_at":0}}"#;
        match decode_event_json(line) {
            Err(EventDecodeError::UnknownVariant { variant, .. }) => {
                assert_eq!(variant, "SomeFutureIncidentKind")
            }
            other => panic!("expected UnknownVariant, got {other:?}"),
        }
    }
}
