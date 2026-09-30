//! Commands: the only way to drive the engine.
//!
//! A [`Command`] expresses *intent*. The engine decides whether and how to honour
//! it, emitting [`crate::Event`]s. Commands never mutate state directly.

use std::collections::HashMap;

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
    /// Mark a decision instance (all rows sharing a `decision_evaluation_key`) for
    /// deletion in the read model. `instance_key` is the owning process instance,
    /// carried so the emitted [`Event::DecisionInstanceDeleted`] is journaled and
    /// projected on the same partition/shard as its originating
    /// [`Event::DecisionEvaluated`]. Audit-only: no core engine state changes.
    DeleteDecisionInstance {
        instance_key: Key,
        decision_evaluation_key: Key,
    },
    /// Start a new instance of a previously deployed process, seeding it with the
    /// given variables (used by exclusive-gateway conditions), optional tags, and
    /// an optional business id.
    CreateInstance {
        process_id: String,
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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
            serde(default, skip_serializing_if = "HashMap::is_empty")
        )]
        variables: HashMap<String, Value>,
    },
    /// Update a job's remaining retries. Used to recover a job parked on a
    /// no-retries incident before resolving that incident. Does not by itself
    /// unblock the job — the incident must still be resolved. `operation_reference`
    /// is an optional caller-supplied audit correlation id, journaled on the
    /// resulting event.
    UpdateJobRetries {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        retries: i32,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        operation_reference: Option<i64>,
    },
    /// Reset the activation lock of a currently-activated job to `now + timeout`.
    /// Signed durations may shorten it; zero or negative values make the lock
    /// due for expiration on the next host-driven expiry sweep.
    /// This is the worker-side mechanism for
    /// legitimately holding a long-running job open past its original lock:
    /// without it the lock simply expires (`JobLockExpired`) and the job is
    /// re-activated elsewhere. Only meaningful for an `Activated` job — a job
    /// that is not currently locked returns `JobUpdateInvalid`. `operation_reference`
    /// is an optional caller-supplied audit correlation id, journaled on the
    /// resulting event.
    UpdateJobTimeout {
        job_key: Key,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        lease_token: Option<String>,
        timeout: i64,
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        operation_reference: Option<i64>,
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
        operation_reference: Option<i64>,
        #[cfg_attr(feature = "serde", serde(default))]
        lease_token: Option<String>,
    },
    /// Resolve an open incident by retrying the work that failed. A job-incident
    /// returns the parked job (which must have retries left) to the activatable
    /// pool; an exclusive-gateway incident re-evaluates the gateway against the
    /// current variables; an uncaught-error incident re-creates the service-task
    /// job. If the retry fails again a fresh incident is raised. The resolved
    /// record is retained for audit, tagged with `operation_reference` if given.
    ResolveIncident {
        incident_key: Key,
        operation_reference: Option<i64>,
    },
    /// Merge variables into a scope, typically to correct data before resolving
    /// an incident. `scope_key` may be a process instance key or an element
    /// instance key; it resolves to the owning instance's variable scope. When
    /// `local` is true the values are written strictly into the target scope;
    /// otherwise they propagate upward (each name updates the nearest ancestor
    /// scope that defines it, defaulting to the root scope) — Zeebe's
    /// `SetVariables` semantics.
    SetVariables {
        scope_key: Key,
        variables: HashMap<String, Value>,
        local: bool,
    },
    /// Publish a message and correlate it to every open subscription whose
    /// message name and correlation key match. A message intermediate catch
    /// event resumes its parked token; an interrupting message boundary event
    /// interrupts its activity. The message's `variables` are merged into each
    /// correlated instance before its token advances. Messages are **not
    /// buffered**: a message with no matching open subscription is simply
    /// dropped (no TTL, no dedup).
    CorrelateMessage {
        message_name: String,
        correlation_key: String,
        variables: HashMap<String, Value>,
        /// Camunda 8.10 `businessId`: stamped on the instance a **message start
        /// event** creates; no effect on a catch/boundary correlation. Defaulted
        /// so commands serialized before the field existed still decode.
        #[cfg_attr(feature = "serde", serde(default))]
        business_id: Option<String>,
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
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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
    },
    /// Cancel a running process instance. Every token is discarded: pending jobs
    /// are cancelled, armed timers and open message subscriptions are cancelled,
    /// and any active incident is closed. The instance transitions to
    /// `Terminated` (it does *not* complete). Only an active instance can be
    /// cancelled; an unknown or already-finished instance is rejected.
    CancelInstance { instance_key: Key },
    /// Suspend a running process instance (Camunda parity). The instance stops
    /// making progress — its jobs become non-activatable and its timers/other
    /// triggers do not fire — while retaining all runtime state, and its state
    /// becomes `Suspended`. Because the message model is unbuffered, a message
    /// correlated to a suspended instance is *dropped* (not buffered) and does
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
    ModifyInstance {
        instance_key: Key,
        activate_instructions: Vec<ActivateElementInstruction>,
        terminate_instructions: Vec<Key>,
    },
    /// Create a start-triggered instance on this partition, routed by the host
    /// from the deploy partition's [`crate::Event::StartInstanceDispatched`] so
    /// message-/timer-start instances spread across the cluster instead of all
    /// landing on partition 0. The instance is minted in this partition's
    /// namespace and started at `start_element_id` with the carried payload.
    DispatchStartInstance {
        process_id: String,
        start_element_id: String,
        variables: HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
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
}

/// One activation instruction of a [`Command::ModifyInstance`]: place a new
/// token at `element_id`, first merging `variables` into the instance's root
/// scope. (Zeebe's activate instruction also carries an ancestor-scope selector
/// and per-scope variable instructions; this engine activates in the process
/// root scope, which covers the common "start a token here" modeler use.)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActivateElementInstruction {
    /// The BPMN element id to activate a token at.
    pub element_id: String,
    /// Variables merged into the instance's root scope before the token is
    /// placed (Zeebe's global variable instructions). Empty for none.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables: HashMap<String, Value>,
}

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
            Command::DeleteDecisionInstance { .. } => "delete_decision_instance",
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
            Command::OpenMessageSubscription { .. } => "open_message_subscription",
            Command::CorrelateMessageSubscription { .. } => "correlate_message_subscription",
            Command::CloseMessageSubscription { .. } => "close_message_subscription",
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
        let vars = |variables: &HashMap<String, Value>| -> u64 {
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
            | Command::CorrelateMessageSubscription { variables, .. }
            | Command::DispatchStartInstance { variables, .. } => vars(variables),
            Command::ModifyInstance {
                activate_instructions,
                terminate_instructions,
                ..
            } => {
                let activate: u64 = activate_instructions
                    .iter()
                    .map(|a| a.element_id.len() as u64 + vars(&a.variables))
                    .sum();
                // Each terminate instruction is an 8-byte element-instance key.
                activate + (terminate_instructions.len() as u64 * 8)
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
            _ => 0,
        };
        BASE + payload
    }

    /// Convenience constructor for a `CreateInstance` with no variables, tags, or
    /// business id.
    pub fn create_instance(process_id: impl Into<String>) -> Self {
        Command::CreateInstance {
            process_id: process_id.into(),
            variables: HashMap::new(),
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        }
    }

    /// Convenience constructor for a `CreateInstance` with variables.
    pub fn create_instance_with(
        process_id: impl Into<String>,
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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
        variables: HashMap<String, Value>,
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
            lease_token: None,
            variables: HashMap::new(),
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
            lease_token: None,
            variables: HashMap::new(),
            adhoc_result: None,
            task_listener_result: Some(task_listener_result),
            business_id: None,
        }
    }

    /// Convenience constructor for a `CompleteJob` that sets variables.
    pub fn complete_job_with(job_key: Key, variables: HashMap<String, Value>) -> Self {
        Command::CompleteJob {
            job_key,
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
        variables: HashMap<String, Value>,
        adhoc_result: AdHocJobResult,
    ) -> Self {
        Command::CompleteJob {
            job_key,
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
            variables: HashMap::new(),
        }
    }

    /// Convenience constructor for a `CompleteUserTask` that sets variables.
    pub fn complete_user_task_with(user_task_key: Key, variables: HashMap<String, Value>) -> Self {
        Command::CompleteUserTask {
            user_task_key,
            variables,
        }
    }

    /// Convenience constructor for a `FailJob`.
    pub fn fail_job(job_key: Key, retries: i32, error_message: impl Into<String>) -> Self {
        Command::FailJob {
            job_key,
            lease_token: None,
            retries,
            error_message: error_message.into(),
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
            lease_token: None,
            error_code: error_code.into(),
            error_message: error_message.into(),
            variables: HashMap::new(),
        }
    }

    /// Convenience constructor for a `ThrowJobError` that seeds `variables` at
    /// the local scope of the catching error boundary event.
    pub fn throw_job_error_with(
        job_key: Key,
        error_code: impl Into<String>,
        error_message: impl Into<String>,
        variables: HashMap<String, Value>,
    ) -> Self {
        Command::ThrowJobError {
            job_key,
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
            operation_reference: None,
        }
    }

    /// `UpdateJobRetries` tagged with a caller audit `operation_reference`.
    pub fn update_job_retries_with_ref(
        job_key: Key,
        retries: i32,
        operation_reference: Option<i64>,
    ) -> Self {
        Command::UpdateJobRetries {
            job_key,
            lease_token: None,
            retries,
            operation_reference,
        }
    }

    /// Convenience constructor for an `UpdateJobTimeout`.
    pub fn update_job_timeout(job_key: Key, timeout: i64) -> Self {
        Command::UpdateJobTimeout {
            job_key,
            lease_token: None,
            timeout,
            operation_reference: None,
        }
    }

    /// `UpdateJobTimeout` tagged with a caller audit `operation_reference`.
    pub fn update_job_timeout_with_ref(
        job_key: Key,
        timeout: i64,
        operation_reference: Option<i64>,
    ) -> Self {
        Command::UpdateJobTimeout {
            job_key,
            lease_token: None,
            timeout,
            operation_reference,
        }
    }

    /// Convenience constructor for a `ResolveIncident` with no operation
    /// reference.
    pub fn resolve_incident(incident_key: Key) -> Self {
        Command::ResolveIncident {
            incident_key,
            operation_reference: None,
        }
    }

    /// Convenience constructor for a `ResolveIncident` tagged with an operation
    /// reference for audit.
    pub fn resolve_incident_with(incident_key: Key, operation_reference: i64) -> Self {
        Command::ResolveIncident {
            incident_key,
            operation_reference: Some(operation_reference),
        }
    }

    /// Convenience constructor for a propagating (`local = false`) `SetVariables`.
    pub fn set_variables(scope_key: Key, variables: HashMap<String, Value>) -> Self {
        Command::SetVariables {
            scope_key,
            variables,
            local: false,
        }
    }

    /// Convenience constructor for a `SetVariables` with an explicit `local` flag.
    pub fn set_variables_scoped(
        scope_key: Key,
        variables: HashMap<String, Value>,
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
            variables: HashMap::new(),
            business_id: None,
        }
    }

    /// Convenience constructor for a `CorrelateMessage` that carries variables to
    /// merge into each correlated instance.
    pub fn correlate_message_with(
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
        variables: HashMap<String, Value>,
    ) -> Self {
        Command::CorrelateMessage {
            message_name: message_name.into(),
            correlation_key: correlation_key.into(),
            variables,
            business_id: None,
        }
    }

    /// Convenience constructor for a `CancelInstance`.
    pub fn cancel_instance(instance_key: Key) -> Self {
        Command::CancelInstance { instance_key }
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
                lease_token: None,
                variables: HashMap::new(),
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
        let mut vars = HashMap::new();
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
