//! Commands: the only way to drive the engine.
//!
//! A [`Command`] expresses *intent*. The engine decides whether and how to honour
//! it, emitting [`crate::Event`]s. Commands never mutate state directly.

use std::collections::BTreeMap;

use crate::model::{
    AdHocActivateElement, AdHocJobResult, ElementId, ProcessDefinition, TaskListenerJobResult,
    Value,
};
use crate::state::{Key, MessageSubscriptionKind};

/// Worker activation options. Leasing is opt-in for every kind of job.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobActivationOptions {
    #[cfg_attr(feature = "serde", serde(default))]
    pub fetch_variables: Vec<String>,
    /// Request a fencing token, not a replication policy. Replicated agent
    /// CREATE/UPDATE require durable activation context even when this is false.
    #[cfg_attr(feature = "serde", serde(default))]
    pub with_lease: bool,
}

/// The authoritative terminal metadata of a job, resolved on the proposing
/// leader and carried on the replicated terminal command (#963).
///
/// Under leader-local activation the activation lock lives only in the
/// leader's RAM, so a replica re-running a replicated terminal command has no
/// activating worker of its own. Reading attribution from replica-local state
/// would make leader and follower persist different terminal events for the
/// same log entry. Stamping it on the command makes the durable outcome a pure
/// function of the replicated log. `worker: None` is an explicit "resolved
/// absent" (the job had no attributable worker when the leader proposed).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobAttribution {
    #[cfg_attr(feature = "serde", serde(default))]
    pub worker: Option<String>,
    /// The activation-lock deadline the job held on the proposing leader
    /// (#1494) — soft (leader-local) or durable. A soft lock lives only in
    /// the leader's RAM, so stamping it here is what lets a soft job's
    /// terminal `JobCompleted.deadline` / `Job::terminal_end` agree on every
    /// replica. `None` when unlocked (or for a stamp written before the field
    /// existed; additive, so an older log entry decodes unchanged).
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub deadline: Option<u64>,
}

/// Which deployed decision a [`Command::EvaluateDecision`] evaluates.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DecisionReference {
    /// The latest deployed version of the decision with this id.
    Id(String),
    /// Exactly the deployed decision (version) with this key.
    Key(Key),
}

/// An instruction submitted to [`crate::Engine::apply_command`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    /// Register a single process definition. Deployed under its own deployment
    /// key; equivalent to a [`Command::DeployResources`] with one process.
    DeployProcess(ProcessDefinition),
    /// Atomically register one or more process definitions as a single
    /// deployment. All processes share one deployment key; each is assigned its
    /// own process-definition key and a per-id version.
    DeployResources(Vec<ProcessDefinition>),
    /// Atomically register one or more decision requirements graphs (parsed
    /// `.dmn` resources) as a deployment. Each DRG is assigned a
    /// decision-requirements key and a per-DRG-id version; each decision it
    /// contains is indexed by id for `businessRuleTask`/EvaluateDecision lookup.
    DeployDecisionRequirements(Vec<crate::dmn::DecisionRequirementsGraph>),
    /// Atomically register one or more forms (`form-js` `.form` resources) as a
    /// deployment. Each form is assigned a form key and a per-form-id version. The
    /// engine does not execute forms; it stores them so they can be served by
    /// `GetFormByKey`.
    DeployForms(Vec<FormResource>),
    /// Atomically register one or more generic resources (any file that is not a
    /// BPMN/DMN/form — e.g. a Markdown agent prompt) as a deployment. Each
    /// resource is assigned a resource key and a per-`resource_id` version. For a
    /// generic resource the `resource_id` is its filename (Zeebe parity: the
    /// default resource transformer uses the resource name as the id). The engine
    /// does not execute generic resources; it stores them verbatim so they can be
    /// served by `GetResourceByKey` / searched by `resourceId`.
    DeployGenericResources(Vec<GenericResource>),
    /// Atomically register one mixed deployment — processes, DRGs, forms and
    /// generic resources submitted together (one REST `POST /deployments`) —
    /// under ONE shared deployment key (#1193). `bindingType: deployment`
    /// lookups (user-task forms, linked resources) resolve by exact
    /// deployment membership, so co-deployed resources must share the key;
    /// splitting a deployment into per-kind commands would mint one key per
    /// kind and strand every co-deployed binding. Duplicate semantics follow
    /// Zeebe: if EVERY resource is identical to its latest version the
    /// deployment is a no-op (each keeps its existing identity); otherwise
    /// every resource — unchanged ones included — is re-versioned under the new
    /// deployment key, so the whole bundle stays mutually resolvable.
    DeployBundle(DeploymentBundle),
    /// Mark a decision instance (all rows sharing a `decision_evaluation_key`) for
    /// deletion in the read model. `instance_key` is the owning process instance
    /// (`0` for a standalone evaluation), carried so the emitted
    /// [`Event::DecisionInstanceDeleted`] is journaled and
    /// projected on the same partition/shard as its originating
    /// [`Event::DecisionEvaluated`]. Audit-only: no core engine state changes.
    DeleteDecisionInstance {
        instance_key: Key,
        decision_evaluation_key: Key,
    },
    /// Delete an ENDED (completed or terminated) process instance's history
    /// from the read model (#1948; Camunda 8.8 `POST
    /// /process-instances/{key}/deletion`). The engine rejects the command with
    /// [`crate::EngineError::InstanceTransitionInvalid`] (`to: "DELETED"`) while
    /// the instance is still live (active, suspended or terminating) in engine
    /// state, so the check is replicated and deterministic. On success it emits
    /// [`Event::ProcessInstanceHistoryDeleted`], which the read-model projection
    /// turns into the removal of the instance, its element instances, variables,
    /// jobs, incidents, user tasks, subscriptions, decision instances and agent
    /// rows — and the same for every descendant (call-activity child) instance.
    /// Only the root's state decides (an ended root has ended its tree), and the
    /// event carries only the root, because ended-instance residency is
    /// replica-local; the event's apply removes the root and every resident
    /// descendant from engine state, so snapshots and reseeds never resurrect it.
    DeleteProcessInstanceHistory { instance_key: Key },
    /// Create a **cluster variable** (#1657; Zeebe `ClusterVariableIntent.CREATE`).
    /// `tenant_id` `None` is the global scope. Rejected with
    /// [`crate::EngineError::ClusterVariableAlreadyExists`] when one already
    /// exists for `(tenant_id, name)` — unless `distributed`, which marks a
    /// fan-out copy of a create/update already validated on the authoritative
    /// partition: it then upserts the full record (value, kind, metadata),
    /// preserving an existing id — idempotent under re-delivery, and a
    /// partition that missed an earlier copy converges.
    CreateClusterVariable {
        tenant_id: Option<String>,
        name: String,
        value: Value,
        kind: crate::cluster_vars::ClusterVariableKind,
        metadata: std::collections::BTreeMap<String, Value>,
        distributed: bool,
    },
    /// Replace an existing cluster variable's value and metadata (its id and
    /// kind are preserved). Rejected with
    /// [`crate::EngineError::ClusterVariableNotFound`] when absent. Only ever
    /// processed on the authoritative partition: the fan-out to the other
    /// partitions carries the resulting full record as a `distributed`
    /// [`Command::CreateClusterVariable`] upsert.
    UpdateClusterVariable {
        tenant_id: Option<String>,
        name: String,
        value: Value,
        metadata: std::collections::BTreeMap<String, Value>,
    },
    /// Delete a cluster variable. Rejected with
    /// [`crate::EngineError::ClusterVariableNotFound`] when absent — unless
    /// `distributed` (a fan-out copy), which is then an idempotent no-op.
    DeleteClusterVariable {
        tenant_id: Option<String>,
        name: String,
        distributed: bool,
    },
    /// Evaluate a deployed decision on demand — the standalone EvaluateDecision
    /// API (Zeebe `DecisionEvaluation:EVALUATE`). Resolves the decision, evaluates
    /// it against `variables`, and records the evaluation as an
    /// [`crate::Event::DecisionEvaluated`] with a freshly minted
    /// `decision_evaluation_key` and no process instance, so it is a queryable
    /// decision instance like a businessRuleTask's. A failed evaluation is
    /// recorded (with its failure), not rejected; an unknown decision is
    /// [`crate::EngineError::DecisionNotFound`].
    EvaluateDecision {
        decision: DecisionReference,
        variables: BTreeMap<String, Value>,
    },
    /// Start a new instance of a previously deployed process, seeding it with the
    /// given variables (used by exclusive-gateway conditions), optional tags, and
    /// an optional business id.
    CreateInstance {
        process_id: String,
        variables: BTreeMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
        /// Selects an exact process **definition version** by its unique key.
        /// `None` (or `Some(0)`) means "not selected by key" — used by the
        /// creation-by-key REST variant, where the key already identifies the
        /// version. Takes precedence over `version` when set. `serde(default)`
        /// so commands written before version selection deserialize unchanged.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        process_definition_key: Option<Key>,
        /// Selects a process version by its version *number* under `process_id`.
        /// `None` (or a non-positive value) means "latest" — the creation-by-id
        /// REST variant's default. Ignored when `process_definition_key` is set.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        version: Option<i32>,
    },
    /// Report that the work for a job has finished, optionally merging variables
    /// into the instance before the token resumes. Leased jobs require the
    /// current activation token; unleased jobs require only the job key.
    CompleteJob {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        variables: BTreeMap<String, Value>,
        /// Optional agentic result for a JOB_WORKER ad-hoc sub-process container
        /// job (Camunda `JobResult`). `None` for every ordinary completion, and
        /// skipped on the wire so plain completions are byte-unchanged. Plumbed
        /// through the transports and acted on by the engine — its
        /// `activate_elements` drive the ad-hoc container's tool activations
        /// (ADR 0023 seam 3).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        adhoc_result: Option<AdHocJobResult>,
        /// Optional result for a *task-listener* job (ADR 0037 §6): may deny the
        /// deferred user-task transition or return corrections. `None` for every
        /// ordinary completion, and skipped on the wire so plain completions are
        /// byte-unchanged.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        task_listener_result: Option<TaskListenerJobResult>,
        /// Camunda 8.10 `JobCompletionRequest.businessId`: assign this business
        /// id to the job's (root) process instance as part of the completion.
        /// A child instance, an empty id, or an id differing from one already
        /// assigned rejects the whole completion; re-sending the assigned id is
        /// a no-op. `None`/skipped for ordinary completions (byte-unchanged).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        business_id: Option<String>,
        /// Authoritative terminal attribution resolved by the PROPOSING leader
        /// (#963). `None` = unstamped (a legacy/locally-applied command: the
        /// engine falls back to the live job). `Some(..)` is authoritative on
        /// every replica, including `Some(JobAttribution { worker: None })` —
        /// "resolved absent", distinct from unstamped. Skipped when unstamped,
        /// so unstamped commands stay byte-identical on the wire.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        attribution: Option<JobAttribution>,
    },
    /// Assign a user task to `assignee`. The task must be in the `Created` state.
    /// When `allow_override` is `false` and the task already has an assignee, the
    /// command is rejected (the task must be unassigned first) — this mirrors
    /// Camunda's group-queue race-prevention semantics.
    AssignUserTask {
        user_task_key: Key,
        assignee: String,
        allow_override: bool,
    },
    /// Clear a user task's assignee. The task must be in the `Created` state.
    UnassignUserTask { user_task_key: Key },
    /// Update a user task's attributes (candidate groups/users, due/follow-up
    /// date, priority). The task must be in the `Created` state. Each field of
    /// the changeset is `Some` only when that attribute is being changed.
    UpdateUserTask {
        user_task_key: Key,
        changeset: UserTaskChangeset,
    },
    /// Complete a user task, optionally merging `variables` into the instance
    /// before the parked token resumes. The task must be in the `Created` state.
    CompleteUserTask {
        user_task_key: Key,
        variables: BTreeMap<String, Value>,
    },
    /// Activate up to `max_jobs` activatable jobs of `job_type` for `worker`,
    /// locking each until `now + timeout`. `now` is a caller-supplied logical
    /// instant — the engine never reads a wall clock.
    ActivateJobs {
        job_type: String,
        worker: String,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        /// The declared read-set (`fetchVariables`) the worker asked for. Carried
        /// from the activate request into the durable [`Event::JobActivated`] as
        /// engine-native read provenance for reification. Empty for a fetch-all
        /// (undeclared) activation, which keeps the declaration-free activation
        /// command byte-identical (`skip_serializing_if`).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        fetch_variables: Vec<String>,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "std::ops::Not::not")
        )]
        with_lease: bool,
    },
    /// Apply an authoritative activation plan without selecting replacement jobs.
    /// The host serializes selection through commit against local activations.
    /// Replica-local soft locks may be replaced; durable activations cannot.
    /// Every selected job stays in the durable domain, even without a lease.
    ActivateJobsByKey {
        job_keys: Vec<Key>,
        worker: String,
        timeout: u64,
        now: u64,
        #[cfg_attr(feature = "serde", serde(default))]
        fetch_variables: Vec<String>,
        #[cfg_attr(feature = "serde", serde(default))]
        with_lease: bool,
    },
    /// Release the activation lock of every job whose `deadline` is at or before
    /// `now`, making it activatable again. A periodic "tick" the host drives;
    /// keeps lock expiry deterministic and out of the engine's clock.
    ExpireJobs { now: u64 },
    /// Expire only durable activations (`true`) or leader-local soft locks
    /// (`false`). Hosts mixing replicated and leader-local activation must keep
    /// these expiry domains separate.
    ExpireJobsByDurability { now: u64, durable: bool },
    /// Fire every armed timer whose `due_at` is at or before `now`, releasing the
    /// token parked on its timer intermediate catch event along the event's
    /// outgoing flow. A periodic "tick" the host drives; keeps timer firing
    /// deterministic and out of the engine's clock.
    TriggerTimers { now: u64 },
    /// Report that a job failed, setting its remaining `retries`. With retries
    /// left the job becomes activatable again; with none an incident is raised
    /// and the job parks. `error_message` is recorded as the incident reason.
    FailJob {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        retries: i32,
        error_message: String,
        /// Camunda 8 v2 `retryBackOff` (ms, #933): with retries left, the job is
        /// not re-activatable before `now + retry_back_off`. `0` (the default,
        /// and any negative request clamped to it) means retryable immediately;
        /// ignored when no retries remain (the job parks with an incident).
        /// Skipped when `0`, so commands without a backoff stay byte-identical.
        #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "is_zero"))]
        retry_back_off: u64,
        /// Authoritative terminal attribution resolved by the PROPOSING leader
        /// (#963). `None` = unstamped (a legacy/locally-applied command: the
        /// engine falls back to the live job). `Some(..)` is authoritative on
        /// every replica, including `Some(JobAttribution { worker: None })` —
        /// "resolved absent", distinct from unstamped. Skipped when unstamped,
        /// so unstamped commands stay byte-identical on the wire.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        attribution: Option<JobAttribution>,
    },
    /// Throw a business error from a job. If the job's activity has a matching
    /// error boundary event it is interrupted and the error-handling path runs;
    /// otherwise an incident is raised. The job is consumed either way.
    ThrowJobError {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        error_code: String,
        error_message: String,
        /// Variables to instantiate at the local scope of the error catch event
        /// that catches the thrown error (Camunda `JobErrorRequest.variables`).
        /// Empty for an error thrown without variables, keeping that path
        /// byte-unchanged. Applied only when a matching boundary catches the
        /// error; ignored when the error is unhandled (parks on an incident).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "BTreeMap::is_empty")
        )]
        variables: BTreeMap<String, Value>,
        /// Authoritative terminal attribution resolved by the PROPOSING leader
        /// (#963). `None` = unstamped (a legacy/locally-applied command: the
        /// engine falls back to the live job). `Some(..)` is authoritative on
        /// every replica, including `Some(JobAttribution { worker: None })` —
        /// "resolved absent", distinct from unstamped. Skipped when unstamped,
        /// so unstamped commands stay byte-identical on the wire.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        attribution: Option<JobAttribution>,
    },
    /// Update a job's remaining retries. Used to recover a job parked on a
    /// no-retries incident before resolving that incident. Does not by itself
    /// unblock the job — the incident must still be resolved. A caller audit
    /// `operationReference` rides the generic record envelope (#1556).
    UpdateJobRetries {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        retries: i32,
    },
    /// Reset the activation lock of a currently-activated job to `now + timeout`.
    /// Signed durations may shorten it; zero or negative values make the lock
    /// due for expiration on the next host-driven expiry sweep.
    /// This is the worker-side mechanism for
    /// legitimately holding a long-running job open past its original lock:
    /// without it the lock simply expires (`JobLockExpired`) and the job is
    /// re-activated elsewhere. Only meaningful for an `Activated` job — a job
    /// that is not currently locked returns `JobUpdateInvalid`. A caller audit
    /// `operationReference` rides the generic record envelope (#1556).
    UpdateJobTimeout {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        timeout: i64,
    },
    /// Atomically validate and update job properties. An empty changeset still
    /// validates job state and a supplied lease token.
    UpdateJob {
        job_key: Key,
        #[cfg_attr(feature = "serde", serde(default))]
        retries: Option<i32>,
        #[cfg_attr(feature = "serde", serde(default))]
        timeout: Option<i64>,
        #[cfg_attr(feature = "serde", serde(default))]
        lease_token: Option<String>,
    },
    /// Resolve an open incident by retrying the work that failed. A job-incident
    /// returns the parked job (which must have retries left) to the activatable
    /// pool; an exclusive-gateway incident re-evaluates the gateway against the
    /// current variables; an uncaught-error incident re-creates the service-task
    /// job. If the retry fails again a fresh incident is raised. The resolved
    /// record is retained for audit, tagged with the command's generic
    /// `operationReference` (#1556) if given.
    ResolveIncident { incident_key: Key },
    /// Merge variables into a scope, typically to correct data before resolving
    /// an incident. `scope_key` may be a process instance key or an element
    /// instance key; it resolves to the owning instance's variable scope. When
    /// `local` is true the values are written strictly into the target scope;
    /// otherwise they propagate upward (each name updates the nearest ancestor
    /// scope that defines it, defaulting to the root scope) — Zeebe's
    /// `SetVariables` semantics.
    SetVariables {
        scope_key: Key,
        variables: BTreeMap<String, Value>,
        local: bool,
    },
    /// Publish a message and correlate it to every open subscription whose
    /// message name and correlation key match. A message intermediate catch
    /// event resumes its parked token; an interrupting message boundary event
    /// interrupts its activity. The message's `variables` are merged into each
    /// correlated instance before its token advances.
    ///
    /// **Buffering (#1874, Zeebe `MessagePublishProcessor` parity).** With a
    /// positive `time_to_live` the message is additionally buffered on the
    /// partition owning `hash(correlation_key)` until `now + time_to_live`
    /// ([`crate::Event::MessageBuffered`]); a subscription opened before that
    /// deadline correlates the oldest still-buffered message its instance has
    /// not already correlated ([`crate::Event::BufferedMessageCorrelated`]).
    /// Buffered delivery is currently **partition-local**: a subscription
    /// whose instance lives on another partition is skipped, so the message
    /// stays buffered until it expires (cross-partition delivery is #1949).
    /// `TriggerTimers` expires buffered messages past their deadline
    /// ([`crate::Event::MessageExpired`]). A `time_to_live` of `None`/`0`
    /// never buffers: the message correlates at publish time only, otherwise
    /// it is dropped. A `message_id` still buffered for the same
    /// `(message_name, correlation_key)` rejects the publish with
    /// [`crate::EngineError::MessageAlreadyExists`].
    CorrelateMessage {
        message_name: String,
        correlation_key: String,
        variables: BTreeMap<String, Value>,
        /// Camunda 8.10 `businessId`: stamped on the instance a **message start
        /// event** creates; no effect on a catch/boundary correlation. Defaulted
        /// so commands serialized before the field existed still decode.
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
        /// A message key already minted for this publication by another
        /// partition (#1434). A multi-partition publish fans one
        /// `CorrelateMessage` out to the correlation-key hash partition AND
        /// partition 0 (message starts); the host carries the key the FIRST
        /// applied partition minted onto the others, so every
        /// `MessagePublished` / correlation record of one publication carries
        /// the single key the client receives. `None` (and every command
        /// serialized before the field existed) mints a fresh key here.
        #[cfg_attr(feature = "serde", serde(default))]
        message_key: Option<Key>,
        /// Zeebe `timeToLive` in milliseconds (#1874). `None`/`0` = correlate
        /// at publish time only (never buffered). Defaulted for old commands.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        time_to_live: Option<u64>,
        /// Zeebe `messageId` (#1874): deduplicates a publish against a message
        /// still buffered for the same name and correlation key.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        message_id: Option<String>,
    },
    /// Broadcast a signal and correlate it to **every** open subscription whose
    /// signal name matches, across all instances. A signal intermediate catch
    /// event resumes its parked token; an interrupting signal boundary event
    /// interrupts its activity. Signals correlate by **name only** (no
    /// correlation key) and are **not buffered**: a broadcast with no matching
    /// open subscription is simply dropped. The `variables` are merged into each
    /// correlated instance before its token advances.
    BroadcastSignal {
        signal_name: String,
        variables: BTreeMap<String, Value>,
    },
    /// Evaluate root-level **conditional start events** (#932, Zeebe
    /// `ConditionalEvaluation:EVALUATE`, `POST /v2/conditionals/evaluation`).
    ///
    /// For every deployed process — the **latest** version of each process id,
    /// or only the exact version `process_definition_key` names when set — each
    /// process-level [`crate::ElementKind::ConditionalStartEvent`] has its FEEL
    /// condition evaluated against `variables`; every one that evaluates to
    /// `true` creates a new instance (seeded with `variables`) on this partition,
    /// starting at that event. Processes and start events are visited in a
    /// deterministic order (process id, then element id) so replay mints the
    /// same keys. A condition that errors or yields a non-boolean is simply not
    /// satisfied (no instance, no incident). The created instances are the
    /// [`crate::Event::ProcessInstanceCreated`] events in the returned log; an
    /// unknown `process_definition_key` is
    /// [`crate::EngineError::ProcessDefinitionNotFound`].
    EvaluateConditionalStartEvents {
        variables: BTreeMap<String, Value>,
        /// Restrict the evaluation to this exact definition version. `None`
        /// evaluates the latest version of every deployed process.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        process_definition_key: Option<Key>,
    },
    /// Open the **canonical** record for a message subscription on the partition
    /// that owns its correlation key (`hash(correlation_key)`). Routed by the host
    /// to the message partition after the instance partition emitted a
    /// [`crate::Event::MessageSubscriptionOpening`]. Records a normal open
    /// [`crate::Event::MessageSubscriptionCreated`]; **idempotent** — re-applying
    /// it for an already-known `subscription_key` is a no-op (at-least-once safe).
    OpenMessageSubscription {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        message_name: String,
        correlation_key: String,
        kind: MessageSubscriptionKind,
        /// The source partition's stable delivery identity (#1497), carried from
        /// the routing [`crate::Event::MessageSubscriptionOpening`]. `0` means
        /// "no identity" (a legacy or hand-issued open): applied without dedup.
        /// Non-zero is deduplicated against the recipient's routed-delivery
        /// ledger and rejected when it matches a close tombstone.
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
    },
    /// Deliver a correlation back to the **instance** partition: advance the token
    /// parked on an `Opening` subscription (the counterpart of a
    /// [`crate::Event::RemoteMessageCorrelation`] settled on the message
    /// partition). Merges the message's `variables` and runs the catch/boundary
    /// outcome exactly as a local correlation would. **Idempotent** — ignored if
    /// the subscription is already settled or the instance has gone.
    CorrelateMessageSubscription {
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        kind: MessageSubscriptionKind,
        variables: BTreeMap<String, Value>,
        /// The message partition's stable delivery identity (#1497), carried
        /// from the routing [`crate::Event::RemoteMessageCorrelation`]. `0`
        /// means "no identity" (legacy): applied without dedup. Non-zero is
        /// deduplicated against the recipient's routed-delivery ledger — the
        /// guard that keeps a retried **non-interrupting** boundary correlation
        /// (whose `Opening` record stays open) from spawning a second token.
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
        /// Ack-paced buffered drain (#1874): when `Some`, answer with an
        /// [`crate::Event::MessageCorrelationAcknowledging`] routed to this
        /// (message) partition, accepted or rejected. Carried from the routing
        /// [`crate::Event::RemoteMessageCorrelation`]. `None` = no ack (legacy).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        ack_partition: Option<u64>,
    },
    /// The instance partition's acknowledgement (#1949) of an ack-paced
    /// buffered delivery, applied on the **message** partition (routed from
    /// [`crate::Event::MessageCorrelationAcknowledging`]). Accepted, with the
    /// subscription still open: the next buffered message is sent. Rejected:
    /// the subscription closes and the remaining messages stay buffered. Not
    /// `consumed`: the message's reservation is released.
    /// **Idempotent** — ignored unless the subscription is awaiting exactly
    /// this delivery (`message_key` AND `delivery_key`), so a duplicate or a
    /// stale ack of an earlier delivery of the same message is a no-op.
    AcknowledgeMessageCorrelation {
        subscription_key: Key,
        message_key: Key,
        /// The acknowledged delivery's key (#1949): the same message may be
        /// re-reserved for this subscription after a deferral, so the message
        /// key alone cannot tell a delayed ack of the old delivery from the
        /// ack of the current one.
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
        accepted: bool,
        /// Whether the instance's token took the message.
        consumed: bool,
        /// The instance is suspended with its scope alive: keep the
        /// subscription open and the message buffered (no next send).
        #[cfg_attr(feature = "serde", serde(default))]
        deferred: bool,
    },
    /// The resume handshake (#1949), applied on the **message** partition
    /// (routed from [`crate::Event::MessageSubscriptionResuming`]): the
    /// subscription's instance was resumed, so an open subscription not
    /// awaiting an ack is offered its buffered backlog again (a delivery it
    /// deferred while the instance was suspended). Idempotent: anything else
    /// is a no-op.
    ResumeMessageSubscription {
        subscription_key: Key,
        /// The handshake generation to echo in the confirmation. Additive.
        #[cfg_attr(feature = "serde", serde(default))]
        generation: u64,
    },
    /// The message partition's confirmation of a resume handshake (#1949),
    /// applied on the **instance** partition (routed from
    /// [`crate::Event::MessageSubscriptionResumed`]): clears the `Opening`
    /// record's `resume_pending` so the host stops re-driving the handshake.
    /// Idempotent: a no-op unless the record is still pending.
    ConfirmMessageSubscriptionResume {
        subscription_key: Key,
        /// Only a confirmation of the record's CURRENT generation clears it.
        /// Additive.
        #[cfg_attr(feature = "serde", serde(default))]
        generation: u64,
    },
    /// The message partition's confirmation (#1949), applied on the
    /// **instance** partition (routed from
    /// [`crate::Event::MessageCorrelationSettled`]), that it applied — or no
    /// longer awaits — the ack of `delivery_key`: no re-drive of that
    /// delivery can change its reservation any more, so the instance
    /// partition unpins the delivery's receipt. Idempotent: a no-op unless
    /// the receipt is still pinned.
    SettleMessageCorrelationReceipt {
        subscription_key: Key,
        delivery_key: Key,
    },
    /// Close a canonical subscription on the message partition because the
    /// instance disarmed it (the guarded activity left the flow, the instance was
    /// cancelled, or a sibling boundary fired). Routed by the host after the
    /// instance partition cancelled its `Opening` record. **Idempotent** — a no-op
    /// for an unknown or already-settled subscription.
    CloseMessageSubscription {
        subscription_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        /// The instance partition's stable delivery identity (#1497), carried
        /// from the routing [`crate::Event::MessageSubscriptionClosing`]. `0`
        /// means "no identity" (legacy): applied without a tombstone. Non-zero
        /// records a **close tombstone** in the recipient's routed-delivery
        /// ledger even when no canonical record exists yet, so a delayed
        /// `OpenMessageSubscription` that lands after this close is rejected
        /// instead of resurrecting a subscription for a cancelled instance.
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
    },
    /// Cancel a running process instance. Every token is discarded: pending jobs
    /// are cancelled, armed timers and open message subscriptions are cancelled,
    /// and any active incident is closed. The instance transitions to
    /// `Terminated` (it does *not* complete). An ACTIVE instance can always be
    /// cancelled; a SUSPENDED one only when `terminate_suspended` is set. An
    /// unknown or already-finished instance is rejected.
    CancelInstance {
        instance_key: Key,
        /// Also terminate a SUSPENDED instance (Camunda parity: "only ACTIVE and
        /// SUSPENDED root instances can be cancelled", #1863). Rolling-upgrade
        /// gated (ADR 0066): a replicated leader clears it unless every member
        /// advertises `cancel-suspended-instance-v1`, so an older replica —
        /// which rejects a SUSPENDED cancel — never applies an entry it would
        /// diverge on. Carried in the command so replay is deterministic.
        /// Additive on the wire: omitted when `false`, defaulted when absent.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "core::ops::Not::not")
        )]
        terminate_suspended: bool,
    },
    /// Suspend a running process instance (Camunda parity). The instance stops
    /// making progress — its jobs become non-activatable and its timers/other
    /// triggers do not fire — while retaining all runtime state, and its state
    /// becomes `Suspended`. A message published to a suspended instance's open
    /// subscription is not correlated to it (a TTL-buffered message stays
    /// buffered, but only a newly opened subscription takes it), and does
    /// not re-correlate on resume — drop-on-suspend is the message suspension
    /// semantics. Only an instance currently `Active` can be suspended
    /// (the sole valid live-transition source); suspending an already
    /// `Suspended` instance is an idempotent no-op, while an unknown or terminal
    /// instance is rejected. Emits [`crate::Event::ProcessInstanceSuspended`].
    SuspendInstance { instance_key: Key },
    /// Resume a suspended process instance (Camunda parity). Its state returns to
    /// `Active` with its exact prior running state and its `suspendedDate`
    /// clears, so jobs and timers become live again. Only an instance currently
    /// `Suspended` can be resumed; resuming an already `Active` instance is an
    /// idempotent no-op, while an unknown or terminal instance is rejected.
    /// Emits [`crate::Event::ProcessInstanceResumed`].
    ResumeInstance { instance_key: Key },
    /// Migrate a running process instance to a target process definition
    /// (Zeebe process-instance migration). Each active element instance whose
    /// element id appears as a `source_element_id` in `mapping_instructions` is
    /// re-pointed at the corresponding `target_element_id` in the target
    /// definition; the instance's `process_id` is rewritten to the target's id.
    /// Runtime attached to migrated element instances (jobs, user tasks, timers,
    /// message/signal/conditional subscriptions, incidents) is remapped in place
    /// — active jobs keep their type and are not re-created.
    ///
    /// Only an active instance can be migrated. The command is rejected (with no
    /// state change) when: the instance is unknown/finished; the target
    /// definition is not deployed; a source element id is duplicated; a mapped
    /// source or target element id does not exist; an active element instance has
    /// no mapping; a mapped pair changes element type; or the instance contains
    /// an element class this phase does not yet support migrating (boundary
    /// events, event subprocesses, multi-instance bodies, call activities,
    /// event-based-gateway catch events) — mirroring Zeebe's "not supported yet"
    /// rejections.
    MigrateInstance {
        instance_key: Key,
        target_process_definition_key: Key,
        mapping_instructions: Vec<(ElementId, ElementId)>,
    },
    /// Modify a running process instance (Zeebe process-instance modification):
    /// spawn fresh tokens at elements and/or terminate specific active element
    /// instances in one atomic command.
    ///
    /// Each *activate* instruction merges its `variables` into the instance's
    /// root scope, then places a new token at `element_id` in the process root
    /// scope — as if a flow had just arrived there. Each *terminate* instruction
    /// (an element-instance key) discards the token resting on that element
    /// instance: its job, user task, timers and subscriptions are cancelled and,
    /// for a sub-process, its inner scope is torn down. If the modification
    /// removes the instance's last token and activates nothing, the instance
    /// terminates.
    ///
    /// Only an active instance can be modified. Activation of an element id that
    /// is not in the process, or termination of a key that is not an active
    /// element instance of the instance, is rejected.
    ///
    /// The Zeebe-parity instruction surface (#1374) — `terminate_element_ids`,
    /// `move_instructions`, per-activation `ancestor_element_instance_key` and
    /// scoped `variable_instructions` — is carried here so every surface (REST,
    /// Falcon, wasm, batch) shares one shape. Until the engine acts on a field
    /// it rejects any non-empty use of it with
    /// [`crate::EngineError::ModificationInstructionNotYetSupported`] (never a
    /// silent ignore).
    ModifyInstance {
        instance_key: Key,
        activate_instructions: Vec<ActivateElementInstruction>,
        terminate_instructions: Vec<Key>,
        /// Terminate every active instance of each listed element id (Zeebe's
        /// terminate-by-`elementId` instruction). Empty for none.
        #[cfg_attr(feature = "serde", serde(default))]
        terminate_element_ids: Vec<ElementId>,
        /// Move instructions (Camunda 8.9 `moveInstructions`): terminate the
        /// source element instance(s) and activate `target_element_id` in their
        /// place. Empty for none.
        #[cfg_attr(feature = "serde", serde(default))]
        move_instructions: Vec<MoveInstruction>,
    },
    /// Create a start-triggered instance on this partition, routed by the host
    /// from the deploy partition's [`crate::Event::StartInstanceDispatched`] so
    /// message-/timer-start instances spread across the cluster instead of all
    /// landing on partition 0. The instance is minted in this partition's
    /// namespace and started at `start_element_id` with the carried payload.
    DispatchStartInstance {
        process_id: String,
        start_element_id: String,
        variables: BTreeMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
        /// The triggering message when a message start event fired (#1300):
        /// the instance's partition records the correlation.
        #[cfg_attr(feature = "serde", serde(default))]
        message_start: Option<crate::MessageStartCorrelation>,
        /// The deploy partition's stable delivery identity (#1497), carried
        /// from the routing [`crate::Event::StartInstanceDispatched`]. `0` means
        /// "no identity" (legacy): applied without dedup. Non-zero is
        /// deduplicated against the recipient's routed-delivery ledger — the
        /// guard that keeps a retried dispatch from minting a SECOND process
        /// instance after a lost acknowledgement.
        #[cfg_attr(feature = "serde", serde(default))]
        delivery_key: Key,
    },
    /// External "activate ad-hoc activities" command (#614 gap 3, Zeebe
    /// `AdHocSubProcessInstructionActivateProcessor` — REST
    /// `POST /element-instances/ad-hoc-activities/{key}/activation`): activate
    /// named tools on an already-running ad-hoc sub-process container WITHOUT
    /// completing its agent job. This is the non-agentic activation seam — it
    /// drives the same tool-activation machinery an agent job's
    /// `activateElements[]` does, and shares the same target validation (an
    /// unknown element id is rejected NOT_FOUND, Zeebe parity).
    ActivateAdHocActivities {
        /// Element-instance key of the ad-hoc sub-process container (the REST
        /// `adHocSubProcessInstanceKey` path parameter).
        ad_hoc_instance_key: Key,
        /// The inner elements (tools) to activate, each with optional seed
        /// variables.
        activate_elements: Vec<AdHocActivateElement>,
        /// Cancel any tools still running and complete the container after this
        /// turn (`cancelRemainingInstances`).
        cancel_remaining: bool,
    },
    /// Create an engine-native AgentInstance (Camunda `AgentInstanceIntent.CREATE`,
    /// stable/8.10; `POST /v2/agent-instances`). The engine infers
    /// `processInstanceKey`, `elementId`, `processDefinitionKey` and `tenantId`
    /// from the referenced `element_instance_key`. The lifecycle *processor* that
    /// validates and applies this command landed in this slice (S3), and its wasm
    /// `TestEngine` driver (`createAgentInstance`) is surfaced in `engine-wasm`
    /// (agent-instance-parity S6).
    CreateAgentInstance {
        /// The key of the AI Agent Sub-process / AI Agent Task element instance.
        element_instance_key: Key,
        /// Job attribution for any agent type. A supplied job must be ACTIVATED,
        /// belong to this element instance, and have the matching opaque lease.
        /// History requires a job; a history-free request may omit it (`0`),
        /// mirroring Camunda's `AgentHistoryBatchBehavior.validateJobContext`.
        #[cfg_attr(feature = "serde", serde(default))]
        job_key: Key,
        /// The opaque activation lease token of `job_key`, not its deadline.
        #[cfg_attr(feature = "serde", serde(default))]
        job_lease: String,
        /// Legacy internal history-free definition. History-bearing CREATE
        /// derives its definition exclusively from CONFIGURATION items.
        definition: crate::agent::AgentDefinition,
        /// Legacy internal history-free limits. The canonical REST path supplies
        /// limits through CONFIGURATION history; unspecified limits default to -1.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        limits: Option<crate::agent::AgentInstanceLimits>,
        /// Initial history, required by the REST contract. Each new item remains
        /// pending until its job resolves. CONFIGURATION establishes the definition.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        history: Vec<crate::agent::AgentHistoryTurn>,
    },
    /// Update an engine-native AgentInstance (Camunda `AgentInstanceIntent.UPDATE`,
    /// stable/8.10; `PATCH /v2/agent-instances/{key}`): advance its status
    /// (to one of the active states) and append attributed, pending history.
    /// Usage is derived from history; configuration changes apply on job commit.
    UpdateAgentInstance {
        agent_instance_key: Key,
        /// The element instance asserting ownership of this update. Must be an
        /// *active* element instance of the same element as the target agent
        /// instance; a new (re-entry) key is appended to the instance's
        /// `element_instance_keys`.
        #[cfg_attr(feature = "serde", serde(default))]
        element_instance_key: Key,
        /// The element id the caller believes this agent instance runs on; must
        /// match the stored instance (guards against a stale/misrouted update).
        #[cfg_attr(feature = "serde", serde(default))]
        element_id: crate::model::ElementId,
        /// The process instance key the caller believes owns this agent
        /// instance; must match the stored instance.
        #[cfg_attr(feature = "serde", serde(default))]
        process_instance_key: Key,
        /// A supplied job is always lease-validated, for every agent type.
        /// History requires an ACTIVATED job belonging to this element instance;
        /// a history-free update may omit job attribution (`0`).
        #[cfg_attr(feature = "serde", serde(default))]
        job_key: Key,
        /// The opaque activation lease token of `job_key`; see
        /// `job_key`.
        #[cfg_attr(feature = "serde", serde(default))]
        job_lease: String,
        /// The target status; must be one of the *active* states (`COMPLETED`
        /// is not settable via UPDATE — it is reached only via COMPLETE).
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        status: Option<crate::agent::AgentInstanceStatus>,
        /// Legacy internal history-free metric patch. Must be default when history
        /// is supplied; canonical REST usage derives metrics from the history items.
        #[cfg_attr(feature = "serde", serde(default))]
        metrics: crate::agent::AgentInstanceMetricsDelta,
        /// Legacy internal history-free tool patch; history-bearing updates use CONFIGURATION.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        tools: Option<Vec<crate::agent::AgentTool>>,
        /// A batch of AgentHistory turns appended pending resolution of their job.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        history: Vec<crate::agent::AgentHistoryTurn>,
    },
    /// Legacy embedded per-agent completion extension, not the Camunda REST
    /// contract. Does not commit history or advance BPMN. Canonical completion
    /// cleans up all agents when their process instance finishes.
    CompleteAgentInstance { agent_instance_key: Key },
    /// Raise a durable [`crate::Event::IncidentRaised`]
    /// ([`crate::IncidentKind::VariablesLost`]) on an instance the host's storage
    /// layer has quarantined as an **off-heap orphan** (#1384): its spill/cold
    /// variable row was definitively absent from a readable store, so the spilled
    /// payload is unrecoverable and the instance keeps only its surviving resident
    /// variables. The host (snapshot fold / boot reconciliation / rehydration)
    /// routes this through the normal command path so the incident is journaled
    /// and Raft-replicated like any other, instead of being only a local log line.
    ///
    /// **Idempotent** — re-applying it for an instance that already carries an
    /// active `VariablesLost` incident is a no-op (at-least-once safe across a
    /// leader retry / replay), and an unknown or terminal instance is a no-op too
    /// (the orphan may have been cancelled between the quarantine note and this
    /// command landing). Additive brand-new variant — old journals never carry it.
    ///
    /// **Replicated form (#1447).** Under Raft the residency-dependent rules
    /// above are NOT replica-independent: whether an instance is resident,
    /// terminal, or already incidented differs per replica (cold tiering,
    /// follower retirement), so the bare command could raise the incident on
    /// one replica and no-op on another, diverging their journals and key
    /// counters. The partition leader therefore proposes it with a
    /// [`OrphanIncidentStamp`] (`stamp: Some`) — the orphan kind plus the
    /// incident metadata it resolved from ITS state — and every replica applies
    /// a stamped command identically: it unconditionally emits exactly one
    /// `IncidentRaised` carrying the stamp's metadata, whatever its local
    /// residency (deduped only by the replicated tombstone). It carries no
    /// variable payload and triggers no replica-local row repair (#1574).
    /// Additive `#[serde(default)]` field: a journal written before #1447
    /// decodes as `stamp: None` (the local form).
    QuarantineOffheapOrphan {
        instance_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        stamp: Option<OrphanIncidentStamp>,
    },
    /// Raise a durable [`crate::Event::IncidentRaised`]
    /// ([`crate::IncidentKind::VariablesLost`]) for a **cold** (wholly off-heap)
    /// instance whose cold row the host's storage layer found definitively
    /// absent (#1481). Unlike [`Command::QuarantineOffheapOrphan`] it does not
    /// require the instance to be resident: a cold instance is absent from
    /// engine state by construction, so the incident is recorded against the
    /// bare `instance_key` (with an empty `element_id` — the instance's model is
    /// lost with its row). This is the only durable record of the loss once the
    /// covered journal prefix compacts away, so it must survive restart.
    ///
    /// If the instance is in fact resident, this behaves exactly like
    /// [`Command::QuarantineOffheapOrphan`]. **Idempotent** — a no-op when an
    /// active `VariablesLost` incident already names the instance. Additive
    /// brand-new variant — old journals never carry it.
    QuarantineColdOrphan { instance_key: Key },
    /// Set this partition's lenient-completion mode (#1661, ADR 0066 slice 2):
    /// see [`crate::state::State::lenient_completion`]. Proposed by the host
    /// (the partition leader) through the replicated log, so every replica
    /// flips the mode at the same log index. ONE-WAY: `lenient: true` emits
    /// [`crate::Event::LenientCompletionSet`] (idempotent); `lenient: false` is
    /// a no-op — lenience can never be cleared, so no replica can strand a
    /// leader-local activation its peers never saw.
    SetLenientCompletion { lenient: bool },
}

/// Which off-heap tier lost an orphan's payload (#1447).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OrphanKind {
    /// A resident instance whose spilled variable row is gone: it keeps its
    /// surviving resident variables.
    Resident,
    /// A cold (wholly off-heap) instance whose cold row is gone: the whole
    /// instance is unrecoverable.
    Cold,
}

/// The leader-resolved incident metadata a replicated
/// [`Command::QuarantineOffheapOrphan`] carries (#1447), so every replica emits
/// a byte-identical `IncidentRaised` whatever its local residency. Built by
/// [`crate::Engine::orphan_incident_stamp`] on the proposing leader.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OrphanIncidentStamp {
    /// The tier that lost the payload.
    pub kind: OrphanKind,
    /// The incident's `element_id` (the process id when the leader had the
    /// instance resident; empty for a cold instance whose model is lost).
    pub element_id: String,
    /// The incident's `reason`.
    pub reason: String,
    /// The leader's replicated terminal epoch
    /// ([`crate::state::State::terminal_epoch`]) at stamp time (#1574 review).
    /// The apply gate compares it against the applying replica's epoch and
    /// terminal ring coverage, so a proposal committing after the ring wrapped
    /// still decides identically on a resident and an evicted replica.
    /// Additive: `0` (a stamp minted before this field existed) disables the
    /// epoch check and keeps the pre-field behaviour (fail open).
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "is_zero"))]
    pub terminal_epoch: u64,
    /// The target instance's own replicated terminal epoch
    /// ([`crate::state::types::ProcessInstance::terminated_epoch`]) observed by
    /// the leader at stamp time (#1574 review, review 5899722557), recovered
    /// from the resident record OR the replicated terminal ring (so a leader
    /// that already evicted its terminal shell still proves it). This is the
    /// target-specific lifecycle-validity record the apply gate reads: the
    /// bounded `recently_terminated` ring cannot distinguish "cold but LIVE"
    /// from "terminated and evicted" once unrelated completions fill it, and the
    /// global epoch distance counts EVERY terminal transition — so it misreads
    /// an unrelated completion as the target's termination (suppressing a live
    /// orphan). `> 0` means the leader observed the target already terminal at
    /// stamp time — a stale proposal the gate no-ops, identically on every
    /// replica. `0` = the target was live (or unprovable) at stamp time.
    /// Additive: `0` for a stamp minted before this field existed (fail open).
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "is_zero"))]
    pub target_terminated_epoch: u64,
    /// The replicated **loss-episode id** (#1587): the leader's replicated
    /// allocator position at stamp time (the next key it would issue). Two
    /// stamped copies of one detection episode carry the SAME episode id, so a
    /// delayed duplicate applying after the episode's incident was resolved is
    /// a deterministic no-op (it matches no live episode) instead of raising a
    /// fresh incident — while a genuinely new loss after resolution stamps a
    /// fresh, distinct episode id and is still reported. The id is a
    /// *replicated* key (never wall-clock, never replica-local), so every
    /// replica compares it identically. Unrelated key-minting work can advance
    /// the allocator between stamping and commitment, so a position mismatch
    /// at apply time is a SUPERSEDED stamp — re-stamped and re-proposed via
    /// [`crate::Event::OrphanStampSuperseded`], never silently dropped (#1587
    /// review). Additive: `0` for a stamp minted before this field existed,
    /// which keeps the pre-#1587 (episode-blind) dedup behaviour.
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "is_zero"))]
    pub episode: u64,
}

#[cfg(feature = "serde")]
fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// Every resource submitted in one deployment, registered atomically by
/// [`Command::DeployBundle`] under a single shared deployment key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeploymentBundle {
    /// BPMN process definitions.
    pub processes: Vec<ProcessDefinition>,
    /// DMN decision requirements graphs.
    pub decisions: Vec<crate::dmn::DecisionRequirementsGraph>,
    /// `form-js` forms.
    pub forms: Vec<FormResource>,
    /// Generic (stored-verbatim) resources.
    pub resources: Vec<GenericResource>,
}

/// A form (`form-js` `.form` resource) to register in a [`Command::DeployForms`].
/// The engine stores it verbatim; it does not parse or execute the form beyond
/// carrying its identity.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FormResource {
    /// The user-provided form identifier (the form-js document's `id`).
    pub id: String,
    /// The deploy resource name (e.g. `greeting.form`).
    pub resource_name: String,
    /// The verbatim form-js JSON document.
    pub schema: String,
    /// The optional version tag this form version carries (form-js
    /// `versionTag`), matched by a `versionTag`-bound user-task form (#1193).
    /// `None` when the form declares no tag.
    #[cfg_attr(feature = "serde", serde(default))]
    pub version_tag: Option<String>,
}

/// Extracts the form-js document `id` from a `.form` resource's JSON — the single
/// canonical definition of "what counts as a valid form id" shared by every deploy
/// boundary (the server's HTTP deploy decomposition and the wasm test-engine entry
/// point), so the rule cannot drift between them. Returns `None` when the body is
/// not a JSON object or lacks a non-empty string `id` (Zeebe requires a form id).
///
/// Gated behind the `serde` feature: JSON parsing is a host concern, so it lives
/// with the other opt-in (de)serialization surface and keeps the default,
/// dependency-free core std-only.
#[cfg(feature = "serde")]
pub fn form_id_of(schema: &str) -> Option<String> {
    let doc: serde_json::Value = serde_json::from_str(schema).ok()?;
    doc.get("id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Extracts the form-js document `versionTag` from a `.form` resource's JSON —
/// the single canonical definition of "what counts as a form version tag"
/// shared by every deploy boundary, mirroring [`form_id_of`]. Returns `None`
/// when the document carries no non-empty string `versionTag` (an untagged
/// form; Zeebe treats the tag as optional metadata matched only by a
/// `versionTag` binding).
///
/// Gated behind the `serde` feature for the same reason as [`form_id_of`].
#[cfg(feature = "serde")]
pub fn form_version_tag_of(schema: &str) -> Option<String> {
    let doc: serde_json::Value = serde_json::from_str(schema).ok()?;
    doc.get("versionTag")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// A generic resource (any deployed file that is not a BPMN process, DMN, or
/// form — e.g. a Markdown agent prompt) to register in a
/// [`Command::DeployGenericResources`]. The engine stores it verbatim; it does
/// not parse or execute it beyond carrying its identity.
///
/// For a plain generic resource the `resource_id` equals `resource_name` (the
/// filename), matching Zeebe's default resource transformer. A structured type
/// could in principle parse a distinct id from the content, so the id is carried
/// explicitly rather than re-derived here.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GenericResource {
    /// The resource identifier used for versioning and lookup. For a generic
    /// resource this is the filename (`resource_name`).
    pub resource_id: String,
    /// The deploy resource name (the filename, e.g. `agent-prompt.md`).
    pub resource_name: String,
    /// The verbatim resource content.
    pub content: String,
    /// The optional version tag this resource version carries (Zeebe
    /// `versionTag` deploy metadata), matched by a `versionTag`-bound
    /// `zeebe:linkedResource` (#1193). `None` when the deploy carried no tag.
    #[cfg_attr(feature = "serde", serde(default))]
    pub version_tag: Option<String>,
}

/// The modification instruction shapes are persisted inside
/// [`crate::event::Event::ProcessInstanceModified`], so they live in the event
/// layer (L3) and are re-exported here for command construction.
pub use crate::event::{
    ActivateElementInstruction, AncestorScopeInstruction, MoveInstruction, MoveSource,
    VariableInstruction,
};

/// The attributes that an [`Command::UpdateUserTask`] may change. Each field is
/// `Some` only when the caller is changing that attribute; `None` leaves it
/// untouched. An empty list or an empty/`None` date *resets* the attribute.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserTaskChangeset {
    /// New candidate groups (empty list clears them).
    pub candidate_groups: Option<Vec<String>>,
    /// New candidate users (empty list clears them).
    pub candidate_users: Option<Vec<String>>,
    /// New due date; `Some(None)` (or `Some("")`-normalised to `None`) clears it.
    pub due_date: Option<Option<String>>,
    /// New follow-up date; `Some(None)` clears it.
    pub follow_up_date: Option<Option<String>>,
    /// New priority (0..=100).
    pub priority: Option<i32>,
}

impl UserTaskChangeset {
    /// Returns `true` when the changeset would not change any attribute.
    pub fn is_empty(&self) -> bool {
        self.candidate_groups.is_none()
            && self.candidate_users.is_none()
            && self.due_date.is_none()
            && self.follow_up_date.is_none()
            && self.priority.is_none()
    }
}

impl Command {
    /// The routed-delivery identity (#1497) this command carries, when it is
    /// one of the four routed recipient commands with a non-zero
    /// `delivery_key`. A deduplicated redelivery of such a command applies
    /// eventlessly, so its acknowledgement must wait on a writer barrier for
    /// the original delivery's write rather than resolve immediately.
    pub fn routed_delivery_key(&self) -> Option<Key> {
        let key = match self {
            Command::OpenMessageSubscription { delivery_key, .. }
            | Command::CorrelateMessageSubscription { delivery_key, .. }
            | Command::CloseMessageSubscription { delivery_key, .. }
            | Command::DispatchStartInstance { delivery_key, .. } => *delivery_key,
            _ => return None,
        };
        (key != 0).then_some(key)
    }

    /// A replica-deterministic fingerprint of this command's identity, mixed
    /// into the FEEL random-stream seed (#1642 / #1649 review) so two commands
    /// planned at the same timestamp and allocator position (neither minting a
    /// key) still derive distinct `random number()` / `uuid()` streams.
    ///
    /// Pure function of the command — which is itself the replicated log
    /// entry — so live apply, full replay, snapshot+tail recovery and every
    /// follower compute the same value with no extra persisted state. Hashes
    /// the [`Command::kind`] plus the command's target key(s) / names with a
    /// fixed FNV-1a (never `RandomState`, never `HashMap` iteration order).
    /// The match is exhaustive so a new variant must decide its identity.
    pub fn seed_fingerprint(&self) -> u64 {
        let mut h = Fnv::new();
        h.str(self.kind());
        match self {
            Command::DeployProcess(d) => h.str(&d.id),
            Command::DeployResources(ds) => {
                for d in ds {
                    h.str(&d.id);
                }
            }
            // Same identity as `DeployResources`: the process ids. The other
            // resource kinds contribute nothing, matching `DeployForms` etc.
            Command::DeployBundle(b) => {
                for d in &b.processes {
                    h.str(&d.id);
                }
            }
            Command::DeployDecisionRequirements(_)
            | Command::DeployForms(_)
            | Command::DeployGenericResources(_)
            | Command::EvaluateConditionalStartEvents { .. } => {}
            Command::DeleteDecisionInstance {
                instance_key,
                decision_evaluation_key,
            } => {
                h.key(*instance_key);
                h.key(*decision_evaluation_key);
            }
            Command::DeleteProcessInstanceHistory { instance_key } => {
                h.key(*instance_key);
            }
            Command::CreateClusterVariable {
                tenant_id, name, ..
            }
            | Command::UpdateClusterVariable {
                tenant_id, name, ..
            }
            | Command::DeleteClusterVariable {
                tenant_id, name, ..
            } => {
                h.str(tenant_id.as_deref().unwrap_or(""));
                h.str(name);
            }
            Command::EvaluateDecision { decision, .. } => match decision {
                DecisionReference::Id(id) => h.str(id),
                DecisionReference::Key(key) => h.key(*key),
            },
            Command::CreateInstance {
                process_id,
                business_id,
                ..
            } => {
                h.str(process_id);
                h.opt(business_id.as_deref());
            }
            Command::CompleteJob { job_key, .. }
            | Command::FailJob { job_key, .. }
            | Command::ThrowJobError { job_key, .. }
            | Command::UpdateJobRetries { job_key, .. }
            | Command::UpdateJobTimeout { job_key, .. }
            | Command::UpdateJob { job_key, .. } => h.key(*job_key),
            Command::AssignUserTask { user_task_key, .. }
            | Command::UnassignUserTask { user_task_key }
            | Command::UpdateUserTask { user_task_key, .. }
            | Command::CompleteUserTask { user_task_key, .. } => h.key(*user_task_key),
            Command::ActivateJobs {
                job_type,
                worker,
                now,
                ..
            } => {
                h.str(job_type);
                h.str(worker);
                h.key(*now);
            }
            Command::ActivateJobsByKey {
                job_keys,
                worker,
                now,
                ..
            } => {
                for k in job_keys {
                    h.key(*k);
                }
                h.str(worker);
                h.key(*now);
            }
            Command::ExpireJobs { now } | Command::TriggerTimers { now } => h.key(*now),
            Command::ExpireJobsByDurability { now, durable } => {
                h.key(*now);
                h.key(u64::from(*durable));
            }
            Command::ResolveIncident { incident_key } => h.key(*incident_key),
            Command::SetVariables { scope_key, .. } => h.key(*scope_key),
            Command::CorrelateMessage {
                message_name,
                correlation_key,
                business_id,
                message_key,
                message_id,
                ..
            } => {
                h.str(message_name);
                h.str(correlation_key);
                h.opt(business_id.as_deref());
                // Only a present id feeds the seed, so every pre-#1874 command
                // keeps its exact fingerprint (determinism vs previous release).
                if let Some(id) = message_id {
                    h.str(id);
                }
                if let Some(k) = message_key {
                    h.key(*k);
                }
            }
            Command::BroadcastSignal { signal_name, .. } => h.str(signal_name),
            Command::OpenMessageSubscription {
                subscription_key,
                element_instance_key,
                ..
            }
            | Command::CloseMessageSubscription {
                subscription_key,
                element_instance_key,
                ..
            } => {
                h.key(*subscription_key);
                h.key(*element_instance_key);
            }
            Command::CorrelateMessageSubscription {
                subscription_key,
                message_key,
                element_instance_key,
                ..
            } => {
                h.key(*subscription_key);
                h.key(*message_key);
                h.key(*element_instance_key);
            }
            Command::AcknowledgeMessageCorrelation {
                subscription_key,
                message_key,
                delivery_key,
                accepted,
                consumed,
                deferred,
            } => {
                h.key(*subscription_key);
                h.key(*message_key);
                h.key(*delivery_key);
                h.str(if *accepted { "1" } else { "0" });
                h.str(if *consumed { "1" } else { "0" });
                h.str(if *deferred { "1" } else { "0" });
            }
            Command::SettleMessageCorrelationReceipt {
                subscription_key,
                delivery_key,
            } => {
                h.key(*subscription_key);
                h.key(*delivery_key);
            }
            Command::ResumeMessageSubscription {
                subscription_key,
                generation,
            }
            | Command::ConfirmMessageSubscriptionResume {
                subscription_key,
                generation,
            } => {
                h.key(*subscription_key);
                h.key(*generation);
            }
            Command::CancelInstance {
                instance_key,
                terminate_suspended,
            } => {
                h.key(*instance_key);
                // Only hashed when set, so a legacy (unset) entry hashes as before.
                if *terminate_suspended {
                    h.str("terminate_suspended");
                }
            }
            Command::SuspendInstance { instance_key }
            | Command::ResumeInstance { instance_key }
            | Command::ModifyInstance { instance_key, .. }
            | Command::QuarantineOffheapOrphan { instance_key, .. }
            | Command::QuarantineColdOrphan { instance_key } => h.key(*instance_key),
            Command::SetLenientCompletion { lenient } => h.str(if *lenient { "1" } else { "0" }),
            Command::MigrateInstance {
                instance_key,
                target_process_definition_key,
                ..
            } => {
                h.key(*instance_key);
                h.key(*target_process_definition_key);
            }
            Command::DispatchStartInstance {
                process_id,
                start_element_id,
                business_id,
                delivery_key,
                ..
            } => {
                h.str(process_id);
                h.str(start_element_id);
                h.opt(business_id.as_deref());
                h.key(*delivery_key);
            }
            Command::ActivateAdHocActivities {
                ad_hoc_instance_key,
                ..
            } => h.key(*ad_hoc_instance_key),
            Command::CreateAgentInstance {
                element_instance_key,
                job_key,
                ..
            } => {
                h.key(*element_instance_key);
                h.key(*job_key);
            }
            Command::UpdateAgentInstance {
                agent_instance_key,
                job_key,
                ..
            } => {
                h.key(*agent_instance_key);
                h.key(*job_key);
            }
            Command::CompleteAgentInstance { agent_instance_key } => h.key(*agent_instance_key),
        }
        h.0
    }

    /// A stable, allocation-free `&'static str` discriminant for this command,
    /// used as a metrics label so per-command actor cost (wall time and
    /// allocated bytes) can be attributed by kind. Kept in lockstep with the
    /// enum variants; a new variant must be added here.
    pub fn kind(&self) -> &'static str {
        match self {
            Command::DeployProcess(_) => "deploy_process",
            Command::DeployResources(_) => "deploy_resources",
            Command::DeployDecisionRequirements(_) => "deploy_decision_requirements",
            Command::DeployForms(_) => "deploy_forms",
            Command::DeployGenericResources(_) => "deploy_generic_resources",
            Command::DeployBundle(_) => "deploy_bundle",
            Command::DeleteDecisionInstance { .. } => "delete_decision_instance",
            Command::DeleteProcessInstanceHistory { .. } => "delete_process_instance_history",
            Command::CreateClusterVariable { .. } => "create_cluster_variable",
            Command::UpdateClusterVariable { .. } => "update_cluster_variable",
            Command::DeleteClusterVariable { .. } => "delete_cluster_variable",
            Command::EvaluateDecision { .. } => "evaluate_decision",
            Command::CreateInstance { .. } => "create_instance",
            Command::CompleteJob { .. } => "complete_job",
            Command::AssignUserTask { .. } => "assign_user_task",
            Command::UnassignUserTask { .. } => "unassign_user_task",
            Command::UpdateUserTask { .. } => "update_user_task",
            Command::CompleteUserTask { .. } => "complete_user_task",
            Command::ActivateJobs { .. } => "activate_jobs",
            Command::ActivateJobsByKey { .. } => "activate_jobs_by_key",
            Command::ExpireJobs { .. } => "expire_jobs",
            Command::ExpireJobsByDurability { .. } => "expire_jobs_by_durability",
            Command::TriggerTimers { .. } => "trigger_timers",
            Command::FailJob { .. } => "fail_job",
            Command::ThrowJobError { .. } => "throw_job_error",
            Command::UpdateJobRetries { .. } => "update_job_retries",
            Command::UpdateJobTimeout { .. } => "update_job_timeout",
            Command::UpdateJob { .. } => "update_job",
            Command::ResolveIncident { .. } => "resolve_incident",
            Command::SetVariables { .. } => "set_variables",
            Command::CorrelateMessage { .. } => "correlate_message",
            Command::BroadcastSignal { .. } => "broadcast_signal",
            Command::EvaluateConditionalStartEvents { .. } => "evaluate_conditional_start_events",
            Command::OpenMessageSubscription { .. } => "open_message_subscription",
            Command::CorrelateMessageSubscription { .. } => "correlate_message_subscription",
            Command::CloseMessageSubscription { .. } => "close_message_subscription",
            Command::AcknowledgeMessageCorrelation { .. } => "acknowledge_message_correlation",
            Command::ResumeMessageSubscription { .. } => "resume_message_subscription",
            Command::ConfirmMessageSubscriptionResume { .. } => "confirm_subscription_resume",
            Command::SettleMessageCorrelationReceipt { .. } => "settle_correlation_receipt",
            Command::CancelInstance { .. } => "cancel_instance",
            Command::SuspendInstance { .. } => "suspend_instance",
            Command::ResumeInstance { .. } => "resume_instance",
            Command::MigrateInstance { .. } => "migrate_instance",
            Command::ModifyInstance { .. } => "modify_instance",
            Command::DispatchStartInstance { .. } => "dispatch_start_instance",
            Command::ActivateAdHocActivities { .. } => "activate_ad_hoc_sub_process_activities",
            Command::CreateAgentInstance { .. } => "create_agent_instance",
            Command::UpdateAgentInstance { .. } => "update_agent_instance",
            Command::CompleteAgentInstance { .. } => "complete_agent_instance",
            Command::QuarantineOffheapOrphan { .. } => "quarantine_offheap_orphan",
            Command::QuarantineColdOrphan { .. } => "quarantine_cold_orphan",
            Command::SetLenientCompletion { .. } => "set_lenient_completion",
        }
    }

    /// A cheap upper-ish estimate of this command's serialized payload size in
    /// bytes, dominated by any carried `variables` map. Used by the Raft propose
    /// batcher to bound a coalesced log entry by bytes (not just command count):
    /// under large variable payloads (e.g. 50 KB/instance) a count-only batch of
    /// 1024 creates would form a ~50 MB entry that cannot replicate within the
    /// AppendEntries RPC timeout, collapsing replication. Non-payload commands
    /// return a small constant — their exact size does not matter for batching.
    pub fn approx_bytes(&self) -> u64 {
        // Small fixed overhead for keys, enum tag, and the fixed scalar fields
        // every command carries; the variable payload dominates when present.
        const BASE: u64 = 64;
        let vars = |variables: &BTreeMap<String, Value>| -> u64 {
            variables
                .iter()
                .map(|(k, v)| k.len() as u64 + v.approx_bytes())
                .sum()
        };
        let payload = match self {
            Command::CreateInstance { variables, .. }
            | Command::CompleteJob { variables, .. }
            | Command::CompleteUserTask { variables, .. }
            | Command::SetVariables { variables, .. }
            | Command::CorrelateMessage { variables, .. }
            | Command::BroadcastSignal { variables, .. }
            | Command::EvaluateConditionalStartEvents { variables, .. }
            | Command::CorrelateMessageSubscription { variables, .. }
            | Command::DispatchStartInstance { variables, .. } => vars(variables),
            Command::ModifyInstance {
                activate_instructions,
                terminate_instructions,
                terminate_element_ids,
                move_instructions,
                ..
            } => {
                let var_instrs = |vi: &[VariableInstruction]| -> u64 {
                    vi.iter()
                        .map(|v| {
                            vars(&v.variables) + v.scope_id.as_ref().map_or(0, |s| s.len() as u64)
                        })
                        .sum()
                };
                let activate: u64 = activate_instructions
                    .iter()
                    .map(|a| {
                        a.element_id.len() as u64
                            + vars(&a.variables)
                            + var_instrs(&a.variable_instructions)
                    })
                    .sum();
                let moves: u64 = move_instructions
                    .iter()
                    .map(|m| {
                        let src = match &m.source {
                            MoveSource::ById(id) => id.len() as u64,
                            MoveSource::ByKey(_) => 8,
                        };
                        src + m.target_element_id.len() as u64
                            + var_instrs(&m.variable_instructions)
                    })
                    .sum();
                let by_id: u64 = terminate_element_ids.iter().map(|e| e.len() as u64).sum();
                // Each terminate instruction is an 8-byte element-instance key.
                activate + moves + by_id + (terminate_instructions.len() as u64 * 8)
            }
            Command::ActivateAdHocActivities {
                activate_elements, ..
            } => activate_elements
                .iter()
                .map(|a| a.element_id.len() as u64 + vars(&a.variables))
                .sum(),
            Command::CreateAgentInstance {
                definition,
                history,
                ..
            } => {
                definition.approx_bytes()
                    + history
                        .iter()
                        .map(crate::agent::AgentHistoryTurn::approx_bytes)
                        .sum::<u64>()
            }
            Command::UpdateAgentInstance { tools, history, .. } => {
                let tools: u64 = tools
                    .iter()
                    .flatten()
                    .map(crate::agent::AgentTool::approx_bytes)
                    .sum();
                let history: u64 = history
                    .iter()
                    .map(crate::agent::AgentHistoryTurn::approx_bytes)
                    .sum();
                tools + history
            }
            // Cluster-variable writes carry an arbitrary value and metadata map
            // (#1657 review): count them so the Raft batcher's byte budget sees
            // the real proposal size instead of the fixed base.
            Command::CreateClusterVariable {
                tenant_id,
                name,
                value,
                metadata,
                ..
            }
            | Command::UpdateClusterVariable {
                tenant_id,
                name,
                value,
                metadata,
            } => {
                tenant_id.as_ref().map_or(0, |t| t.len() as u64)
                    + name.len() as u64
                    + value.approx_bytes()
                    + metadata
                        .iter()
                        .map(|(k, v)| k.len() as u64 + v.approx_bytes())
                        .sum::<u64>()
            }
            Command::DeleteClusterVariable {
                tenant_id, name, ..
            } => tenant_id.as_ref().map_or(0, |t| t.len() as u64) + name.len() as u64,
            // The stamp's strings are leader-resolved metadata of unbounded
            // length (#1574 review): count them so the batcher's byte budget
            // sees the real proposal size.
            Command::QuarantineOffheapOrphan {
                stamp: Some(stamp), ..
            } => stamp.element_id.len() as u64 + stamp.reason.len() as u64,
            _ => 0,
        };
        BASE + payload
    }

    /// Convenience constructor for a `CreateInstance` with no variables, tags, or
    /// business id.
    pub fn create_instance(process_id: impl Into<String>) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables: BTreeMap::new(),
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        }
    }

    /// Convenience constructor for a `CreateInstance` with variables.
    pub fn create_instance_with(
        process_id: impl Into<String>,
        variables: BTreeMap<String, Value>,
    ) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        }
    }

    /// Convenience constructor for a `CreateInstance` with variables, tags, and
    /// optional business id.
    pub fn create_instance_full(
        process_id: impl Into<String>,
        variables: BTreeMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
    ) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables,
            tags,
            business_id,
            process_definition_key: None,
            version: None,
        }
    }

    /// Convenience constructor for a `CreateInstance` that selects a specific
    /// process **definition version** — by its unique key (`process_definition_key`,
    /// the creation-by-key path) or by version *number* under `process_id`
    /// (`version`, the creation-by-id path). A `None`/`0` key and a `None`/
    /// non-positive version together mean "latest".
    #[allow(clippy::too_many_arguments)]
    pub fn create_instance_versioned(
        process_id: impl Into<String>,
        variables: BTreeMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
        process_definition_key: Option<Key>,
        version: Option<i32>,
    ) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables,
            tags,
            business_id,
            process_definition_key,
            version,
        }
    }

    /// Convenience constructor for a `CompleteJob` with no variables.
    pub fn complete_job(job_key: Key) -> Self {
        Command::CompleteJob {
            job_key,
            attribution: None,
            lease_token: None,
            variables: BTreeMap::new(),
            adhoc_result: None,
            task_listener_result: None,
            business_id: None,
        }
    }

    /// Convenience constructor for a `CompleteJob` that carries a task-listener
    /// result (ADR 0037 §6): a denial and/or corrections to user-task data.
    pub fn complete_job_with_task_result(
        job_key: Key,
        task_listener_result: TaskListenerJobResult,
    ) -> Self {
        Command::CompleteJob {
            job_key,
            attribution: None,
            lease_token: None,
            variables: BTreeMap::new(),
            adhoc_result: None,
            task_listener_result: Some(task_listener_result),
            business_id: None,
        }
    }

    /// Convenience constructor for a `CompleteJob` that sets variables.
    pub fn complete_job_with(job_key: Key, variables: BTreeMap<String, Value>) -> Self {
        Command::CompleteJob {
            job_key,
            attribution: None,
            lease_token: None,
            variables,
            adhoc_result: None,
            task_listener_result: None,
            business_id: None,
        }
    }

    /// Convenience constructor for a `CompleteJob` that carries an agentic
    /// ad-hoc sub-process result (Camunda `JobResult`).
    pub fn complete_job_with_result(
        job_key: Key,
        variables: BTreeMap<String, Value>,
        adhoc_result: AdHocJobResult,
    ) -> Self {
        Command::CompleteJob {
            job_key,
            attribution: None,
            lease_token: None,
            variables,
            adhoc_result: Some(adhoc_result),
            task_listener_result: None,
            business_id: None,
        }
    }

    /// Convenience constructor for an `AssignUserTask` (allowing override).
    pub fn assign_user_task(user_task_key: Key, assignee: impl Into<String>) -> Self {
        Command::AssignUserTask {
            user_task_key,
            assignee: assignee.into(),
            allow_override: true,
        }
    }

    /// Convenience constructor for an `UnassignUserTask`.
    pub fn unassign_user_task(user_task_key: Key) -> Self {
        Command::UnassignUserTask { user_task_key }
    }

    /// Convenience constructor for an `UpdateUserTask`.
    pub fn update_user_task(user_task_key: Key, changeset: UserTaskChangeset) -> Self {
        Command::UpdateUserTask {
            user_task_key,
            changeset,
        }
    }

    /// Convenience constructor for a `CompleteUserTask` with no variables.
    pub fn complete_user_task(user_task_key: Key) -> Self {
        Command::CompleteUserTask {
            user_task_key,
            variables: BTreeMap::new(),
        }
    }

    /// Convenience constructor for a `CompleteUserTask` that sets variables.
    pub fn complete_user_task_with(user_task_key: Key, variables: BTreeMap<String, Value>) -> Self {
        Command::CompleteUserTask {
            user_task_key,
            variables,
        }
    }

    /// Convenience constructor for a `FailJob`.
    pub fn fail_job(job_key: Key, retries: i32, error_message: impl Into<String>) -> Self {
        Self::fail_job_with_backoff(job_key, retries, error_message, 0)
    }

    /// Convenience constructor for a `FailJob` carrying a Camunda 8 v2
    /// `retryBackOff` in milliseconds (#933). A negative backoff is clamped to
    /// `0` (retryable immediately), matching Zeebe, which only defers the job
    /// when the backoff is strictly positive.
    pub fn fail_job_with_backoff(
        job_key: Key,
        retries: i32,
        error_message: impl Into<String>,
        retry_back_off: i64,
    ) -> Self {
        Command::FailJob {
            job_key,
            attribution: None,
            lease_token: None,
            retries,
            error_message: error_message.into(),
            retry_back_off: u64::try_from(retry_back_off).unwrap_or(0),
        }
    }

    /// Convenience constructor for a `ThrowJobError`.
    pub fn throw_job_error(
        job_key: Key,
        error_code: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Command::ThrowJobError {
            job_key,
            attribution: None,
            lease_token: None,
            error_code: error_code.into(),
            error_message: error_message.into(),
            variables: BTreeMap::new(),
        }
    }

    /// Convenience constructor for a `ThrowJobError` that seeds `variables` at
    /// the local scope of the catching error boundary event.
    pub fn throw_job_error_with(
        job_key: Key,
        error_code: impl Into<String>,
        error_message: impl Into<String>,
        variables: BTreeMap<String, Value>,
    ) -> Self {
        Command::ThrowJobError {
            job_key,
            attribution: None,
            lease_token: None,
            error_code: error_code.into(),
            error_message: error_message.into(),
            variables,
        }
    }

    /// Convenience constructor for an `UpdateJobRetries`.
    pub fn update_job_retries(job_key: Key, retries: i32) -> Self {
        Command::UpdateJobRetries {
            job_key,
            lease_token: None,
            retries,
        }
    }

    /// Convenience constructor for an `UpdateJobTimeout`.
    pub fn update_job_timeout(job_key: Key, timeout: i64) -> Self {
        Command::UpdateJobTimeout {
            job_key,
            lease_token: None,
            timeout,
        }
    }

    /// Convenience constructor for a `ResolveIncident`.
    pub fn resolve_incident(incident_key: Key) -> Self {
        Command::ResolveIncident { incident_key }
    }

    /// Convenience constructor for a propagating (`local = false`) `SetVariables`.
    pub fn set_variables(scope_key: Key, variables: BTreeMap<String, Value>) -> Self {
        Command::SetVariables {
            scope_key,
            variables,
            local: false,
        }
    }

    /// Convenience constructor for a `SetVariables` with an explicit `local` flag.
    pub fn set_variables_scoped(
        scope_key: Key,
        variables: BTreeMap<String, Value>,
        local: bool,
    ) -> Self {
        Command::SetVariables {
            scope_key,
            variables,
            local,
        }
    }

    /// Convenience constructor for an `ActivateJobs` request.
    pub fn activate_jobs(
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Self {
        Command::ActivateJobs {
            job_type: job_type.into(),
            worker: worker.into(),
            max_jobs,
            timeout,
            now,
            fetch_variables: Vec::new(),
            with_lease: false,
        }
    }

    /// `ActivateJobs` carrying a declared read-set (`fetchVariables`). The names
    /// are stamped onto each resulting [`Event::JobActivated`] as engine-native
    /// read provenance. An empty `fetch_variables` is equivalent to
    /// [`Command::activate_jobs`] (fetch-all, undeclared reads).
    pub fn activate_jobs_with_fetch(
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        fetch_variables: Vec<String>,
    ) -> Self {
        Command::ActivateJobs {
            job_type: job_type.into(),
            worker: worker.into(),
            max_jobs,
            timeout,
            now,
            fetch_variables,
            with_lease: false,
        }
    }

    /// Activate jobs with explicit read-set and leasing options.
    pub fn activate_jobs_with_options(
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        options: JobActivationOptions,
    ) -> Self {
        Command::ActivateJobs {
            job_type: job_type.into(),
            worker: worker.into(),
            max_jobs,
            timeout,
            now,
            fetch_variables: options.fetch_variables,
            with_lease: options.with_lease,
        }
    }

    /// Construct an authoritative, immutable activation plan.
    pub fn activate_jobs_by_key(
        job_keys: Vec<Key>,
        worker: impl Into<String>,
        timeout: u64,
        now: u64,
        options: JobActivationOptions,
    ) -> Self {
        Self::ActivateJobsByKey {
            job_keys,
            worker: worker.into(),
            timeout,
            now,
            fetch_variables: options.fetch_variables,
            with_lease: options.with_lease,
        }
    }

    /// Attach an opaque activation token to a job mutation command.
    pub fn with_job_lease(self, token: impl Into<String>) -> Self {
        self.with_lease_token(Some(token.into()))
    }

    /// Set or clear the optional fencing token of a job mutation command.
    pub fn with_lease_token(mut self, token: Option<String>) -> Self {
        match &mut self {
            Self::CompleteJob { lease_token, .. }
            | Self::FailJob { lease_token, .. }
            | Self::ThrowJobError { lease_token, .. }
            | Self::UpdateJobRetries { lease_token, .. }
            | Self::UpdateJobTimeout { lease_token, .. }
            | Self::UpdateJob { lease_token, .. } => *lease_token = token,
            _ => panic!("with_lease_token requires a job mutation command"),
        }
        self
    }

    /// Set the business id a `CompleteJob` assigns to its process instance
    /// (Camunda 8.10 `JobCompletionRequest.businessId`).
    pub fn with_business_id(mut self, id: Option<String>) -> Self {
        match &mut self {
            Self::CompleteJob { business_id, .. } => *business_id = id,
            _ => panic!("with_business_id requires a CompleteJob command"),
        }
        self
    }

    /// Convenience constructor for a `CorrelateMessage` with no variables.
    pub fn correlate_message(
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        Command::CorrelateMessage {
            message_name: message_name.into(),
            correlation_key: correlation_key.into(),
            variables: BTreeMap::new(),
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
        }
    }

    /// Convenience constructor for a `CorrelateMessage` that carries variables to
    /// merge into each correlated instance.
    pub fn correlate_message_with(
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
        variables: BTreeMap<String, Value>,
    ) -> Self {
        Command::CorrelateMessage {
            message_name: message_name.into(),
            correlation_key: correlation_key.into(),
            variables,
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
        }
    }

    /// Convenience constructor for a legacy `CancelInstance` (rejects a
    /// SUSPENDED instance).
    pub fn cancel_instance(instance_key: Key) -> Self {
        Command::CancelInstance {
            instance_key,
            terminate_suspended: false,
        }
    }

    /// Convenience constructor for a `CancelInstance` that also terminates a
    /// SUSPENDED instance (#1863; rolling-upgrade gated in a cluster).
    pub fn cancel_instance_including_suspended(instance_key: Key) -> Self {
        Command::CancelInstance {
            instance_key,
            terminate_suspended: true,
        }
    }

    /// Convenience constructor for a `SuspendInstance`.
    pub fn suspend_instance(instance_key: Key) -> Self {
        Command::SuspendInstance { instance_key }
    }

    /// Convenience constructor for a `ResumeInstance`.
    pub fn resume_instance(instance_key: Key) -> Self {
        Command::ResumeInstance { instance_key }
    }

    /// Convenience constructor for a `MigrateInstance`.
    pub fn migrate_instance(
        instance_key: Key,
        target_process_definition_key: Key,
        mapping_instructions: Vec<(ElementId, ElementId)>,
    ) -> Self {
        Command::MigrateInstance {
            instance_key,
            target_process_definition_key,
            mapping_instructions,
        }
    }

    /// Convenience constructor for a `ModifyInstance`.
    pub fn modify_instance(
        instance_key: Key,
        activate_instructions: Vec<ActivateElementInstruction>,
        terminate_instructions: Vec<Key>,
    ) -> Self {
        Command::ModifyInstance {
            instance_key,
            activate_instructions,
            terminate_instructions,
            terminate_element_ids: Vec::new(),
            move_instructions: Vec::new(),
        }
    }
}

/// Fixed FNV-1a 64 hasher for [`Command::seed_fingerprint`]: deterministic
/// across processes and replicas (unlike `std`'s randomly-keyed `RandomState`).
struct Fnv(u64);

impl Fnv {
    fn new() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
    fn bytes(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }
    fn str(&mut self, s: &str) {
        // Length-prefixed so adjacent fields cannot alias ("ab","c" vs "a","bc").
        self.key(s.len() as u64);
        self.bytes(s.as_bytes());
    }
    fn key(&mut self, k: u64) {
        self.bytes(&k.to_le_bytes());
    }
    fn opt(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                self.key(1);
                self.str(s);
            }
            None => self.key(0),
        }
    }
}

#[cfg(test)]
mod kind_tests {
    use super::*;

    #[test]
    fn kind_labels_the_hot_commands() {
        assert_eq!(Command::create_instance("p").kind(), "create_instance");
        assert_eq!(
            Command::CompleteJob {
                job_key: 1,
                attribution: None,
                lease_token: None,
                variables: BTreeMap::new(),
                adhoc_result: None,
                task_listener_result: None,
                business_id: None,
            }
            .kind(),
            "complete_job"
        );
        assert_eq!(Command::ExpireJobs { now: 0 }.kind(), "expire_jobs");
        assert_eq!(
            Command::ActivateJobs {
                job_type: "t".into(),
                worker: "w".into(),
                max_jobs: 1,
                timeout: 0,
                now: 0,
                fetch_variables: Vec::new(),
                with_lease: false,
            }
            .kind(),
            "activate_jobs"
        );
    }

    #[test]
    fn approx_bytes_scales_with_variable_payload() {
        // A payload-free command is just the fixed base.
        let empty = Command::create_instance("p").approx_bytes();
        // A create carrying a big string variable is dominated by that payload.
        let big = "x".repeat(50_000);
        let mut vars = BTreeMap::new();
        vars.insert("p".to_string(), Value::Str(big));
        let heavy = Command::create_instance_with("p", vars).approx_bytes();
        assert!(
            heavy >= empty + 50_000,
            "payload must dominate: heavy={heavy} empty={empty}"
        );
        // Non-payload commands stay at the small base regardless of keys.
        let tick = Command::ExpireJobs { now: 123 }.approx_bytes();
        assert!(
            tick <= empty,
            "tick={tick} should be <= empty create={empty}"
        );
    }

    // #1657 review: cluster-variable create/update (incl. distributed fan-out
    // copies) and delete must count their payload, not the fixed base.
    #[test]
    fn approx_bytes_counts_cluster_variable_payloads() {
        use std::collections::BTreeMap;
        let big = "x".repeat(50_000);
        let mut meta = BTreeMap::new();
        meta.insert("m".to_string(), Value::Str("y".repeat(10_000)));
        let tenant = Some("t".repeat(1_000));
        let name = "n".repeat(1_000);
        for distributed in [false, true] {
            let c = Command::CreateClusterVariable {
                tenant_id: tenant.clone(),
                name: name.clone(),
                value: Value::Str(big.clone()),
                kind: crate::cluster_vars::ClusterVariableKind::default(),
                metadata: meta.clone(),
                distributed,
            };
            assert!(
                c.approx_bytes() >= 62_000,
                "create distributed={distributed}"
            );
        }
        let u = Command::UpdateClusterVariable {
            tenant_id: tenant.clone(),
            name: name.clone(),
            value: Value::Str(big),
            metadata: meta,
        };
        assert!(u.approx_bytes() >= 62_000);
        let d = Command::DeleteClusterVariable {
            tenant_id: tenant,
            name,
            distributed: false,
        };
        assert!(d.approx_bytes() >= 2_000);
    }

    // #1574 review: a stamped orphan quarantine carries leader-resolved strings;
    // `approx_bytes` must count them, not treat the command as fixed-size.
    #[test]
    fn approx_bytes_counts_the_orphan_quarantine_stamp() {
        let bare = Command::QuarantineOffheapOrphan {
            instance_key: 1,
            stamp: None,
        }
        .approx_bytes();
        let stamped = Command::QuarantineOffheapOrphan {
            instance_key: 1,
            stamp: Some(OrphanIncidentStamp {
                kind: OrphanKind::Resident,
                element_id: "e".repeat(10_000),
                reason: "r".repeat(20_000),
                terminal_epoch: 0,
                target_terminated_epoch: 0,
                episode: 0,
            }),
        }
        .approx_bytes();
        assert!(
            stamped >= bare + 30_000,
            "stamp strings must count: stamped={stamped} bare={bare}"
        );
    }

    #[test]
    fn approx_bytes_scales_with_agent_instance_payload() {
        use crate::agent::{
            AgentDefinition, AgentHistoryContent, AgentHistoryContentType, AgentHistoryTurn,
            AgentTool,
        };
        let big = "x".repeat(50_000);
        let turn = AgentHistoryTurn {
            content: vec![AgentHistoryContent {
                content_type: AgentHistoryContentType::Text,
                text: Some(big.clone()),
                document_reference: None,
                object: None,
            }],
            ..Default::default()
        };

        // A CREATE carrying a big history turn must be dominated by that payload,
        // not underestimated to the fixed base (which would let the Raft batcher
        // build an oversized log entry that fails to replicate).
        let create = Command::CreateAgentInstance {
            element_instance_key: 1,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![turn.clone()],
        }
        .approx_bytes();
        assert!(
            create >= 50_000,
            "create history payload must dominate: create={create}"
        );

        // An UPDATE is metered across both its tools and history payloads.
        let update = Command::UpdateAgentInstance {
            agent_instance_key: 1,
            element_instance_key: 2,
            element_id: String::new(),
            process_instance_key: 3,
            job_key: 0,
            job_lease: String::new(),
            status: None,
            metrics: Default::default(),
            tools: Some(vec![AgentTool {
                name: big.clone(),
                description: None,
                element_id: None,
            }]),
            history: vec![turn],
        }
        .approx_bytes();
        assert!(
            update >= 100_000,
            "update tools+history payload must dominate: update={update}"
        );
    }
}
