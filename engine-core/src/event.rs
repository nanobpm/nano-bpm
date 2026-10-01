//! Events: immutable facts about what happened.
//!
//! Events are the engine's source of truth. They are produced by the processor
//! and consumed by [`crate::state::apply`] (the sole mutator). Because each event
//! carries enough data to rebuild state, the event stream is a replayable log —
//! persist it however you like and replay to recover.

use std::collections::HashMap;

use crate::model::{ElementId, IncomingFlow, ProcessDefinition, Value};
use crate::state::types::{
    IncidentKind, IoMappingRedrive, Key, MessageSubscriptionKind, TimerKind,
};

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

    /// A new process instance was created (carries a single token at its start
    /// event) with its initial variables. `created_at` is the logical instant
    /// the instance was started, carried on the command (the engine never reads
    /// a wall clock); it is the instance's start date. Defaulted to `0` so
    /// journals written before this field existed still replay. `tags` and
    /// `business_id` are user-supplied metadata, defaulted for older journals.
    ProcessInstanceCreated {
        instance_key: Key,
        process_id: String,
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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

    /// An element instance entered `ACTIVATING`.
    ElementActivating {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
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

    /// A token moved along a sequence flow from one element to another.
    SequenceFlowTaken {
        instance_key: Key,
        from: ElementId,
        to: ElementId,
    },

    /// A parallel-gateway join element instance was opened on the first arriving
    /// token; subsequent tokens accumulate against it.
    ParallelJoinOpened {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
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
        #[cfg_attr(feature = "serde", serde(default))]
        flow: Option<IncomingFlow>,
    },
    /// A join's bookkeeping was cleared outright: a join fired under a build
    /// older than #1233 (parallel) / #1237 (inclusive), or the scope holding the
    /// join was torn down.
    ParallelJoinReset {
        instance_key: Key,
        element_id: ElementId,
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
    },
    /// A job's remaining retries were updated (e.g. by an operator recovering a
    /// parked job before resolving its incident). Does not change job state.
    JobRetriesUpdated {
        job_key: Key,
        instance_key: Key,
        retries: i32,
        /// Optional caller audit correlation id (Camunda `operationReference`).
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
        /// Optional caller audit correlation id (Camunda `operationReference`).
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
    JobCanceled { job_key: Key, instance_key: Key },

    /// A message was published. In nano messages are not buffered, so this
    /// records no durable state; it carries the minted `message_key` (returned to
    /// the host and used to restore the key generator on replay) and heads the
    /// [`Event::MessageCorrelated`] events the same command produced.
    MessagePublished {
        message_key: Key,
        message_name: String,
        correlation_key: String,
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
    MessageSubscriptionOpening {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
        kind: MessageSubscriptionKind,
    },
    /// A published message correlated to an open subscription. For an
    /// intermediate catch event its token is released along the event's outgoing
    /// flow; for an interrupting boundary the attached activity is interrupted and
    /// the boundary's outgoing flow runs (the job-cancellation, element-completion
    /// and sequence-flow events follow). `message_key` ties it back to the
    /// [`Event::MessagePublished`] that produced it.
    MessageCorrelated {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    },
    /// A published message matched a subscription on the **message** partition
    /// whose instance lives on another partition. Settles the canonical
    /// subscription (exactly like [`Event::MessageCorrelated`]) but does **not**
    /// advance the token here; it carries the full continuation payload (kind +
    /// the message's variables) so the host can route a
    /// [`crate::Command::CorrelateMessageSubscription`] to the instance partition,
    /// where the token actually advances. Only produced when `num_partitions > 1`.
    RemoteMessageCorrelation {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: MessageSubscriptionKind,
        variables: HashMap<String, Value>,
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
    /// [`CompensationBoundaryEvent`]: crate::model::ElementKind::CompensationBoundaryEvent
    /// [`CompensationThrowEvent`]: crate::model::ElementKind::CompensationThrowEvent
    CompensationSubscriptionCreated {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        handler: ElementId,
        scope: Key,
    },
    /// A compensation throw event fired: it consumed the compensable
    /// subscriptions in its `scope` and triggered their handlers. `handlers`
    /// lists the handler activity ids activated and `consumed` the compensable
    /// element-instance keys removed. The throw's token
    /// (`throw_element_instance_key`) rests until every triggered handler
    /// completes.
    CompensationTriggered {
        instance_key: Key,
        throw_element_instance_key: Key,
        throw_element_id: ElementId,
        scope: Key,
        handlers: Vec<ElementId>,
        consumed: Vec<Key>,
    },
    /// One compensation handler triggered by a compensation throw event
    /// completed. Removes the handler from the throw's outstanding set; when it
    /// empties the throw event completes and routes onward.
    CompensationHandlerCompleted {
        instance_key: Key,
        throw_element_instance_key: Key,
        handler_element_id: ElementId,
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
    },
    /// A multi-instance child instance was activated at `index` (0-based) into the
    /// input collection. `local_variables` are the child's scope bindings (the
    /// `input_element` item, when named, and `loopCounter`), overlaid on the
    /// instance variables when the child's job is activated or its FEEL evaluated.
    MultiInstanceChildActivated {
        instance_key: Key,
        body_key: Key,
        child_key: Key,
        index: usize,
        local_variables: HashMap<String, Value>,
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
        local_variables: HashMap<String, Value>,
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
    MessageSubscriptionClosing {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        message_name: String,
        correlation_key: String,
    },
    /// A process-level message start subscription was opened at deploy time: a
    /// later [`Event::MessagePublished`] with a matching `message_name` creates a
    /// new instance of `process_id`. Carried on the log so replay reconstructs
    /// the subscription.
    MessageStartSubscriptionCreated {
        process_definition_key: Key,
        process_id: String,
        message_name: String,
        start_element_id: ElementId,
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
        variables: HashMap<String, Value>,
        #[cfg_attr(feature = "serde", serde(default))]
        tags: Vec<String>,
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
        target_partition: u64,
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
}

impl Event {
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
            | Event::ElementActivating { instance_key, .. }
            | Event::ElementActivated { instance_key, .. }
            | Event::ElementCompleting { instance_key, .. }
            | Event::ElementCompleted { instance_key, .. }
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
            | Event::ScopedCompensationCleared { instance_key, .. }
            | Event::MultiInstanceActivated { instance_key, .. }
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
            | Event::ProcessInstanceCompleted { instance_key }
            | Event::ProcessInstanceTerminated { instance_key } => Some(*instance_key),
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
            | Event::SignalBroadcast { .. }
            | Event::MessageStartSubscriptionCreated { .. }
            | Event::ProcessStartTimerArmed { .. }
            | Event::ProcessStartTimerFired { .. }
            | Event::StartInstanceDispatched { .. } => None,
        }
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
            | Event::ParallelJoinOpened {
                element_instance_key,
                ..
            } => m = m.max(*element_instance_key),
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
            Event::MessagePublished { message_key, .. } => m = m.max(*message_key),
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
                ..
            } => {
                m = m.max(*throw_element_instance_key).max(*scope);
                for key in consumed {
                    m = m.max(*key);
                }
            }
            Event::CompensationHandlerCompleted {
                throw_element_instance_key,
                ..
            } => m = m.max(*throw_element_instance_key),
            Event::MultiInstanceActivated { body_key, .. }
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
